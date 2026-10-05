"""The endpoints lazer's login path calls that were all 404.

Every request here is made unconditionally by the client while reaching
`APIState.Online`, so a 404 on any one of them shows up in the log as a failed
request and, in `/notifications`' case, kills the socket connector outright.

The assertions check the *field names*, not just the status code: lazer binds
JSON by literal `[JsonProperty]` name and does no case conversion, so a 200 with
the wrong key is still an NRE client-side.

Needs a populated bancho db.

    python tests/test_login_surface.py
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from app import asgi_app  # noqa: E402
from test_ranked_submit import provision_user  # noqa: E402

BASE = "/api/v2"


def md5_hex(value: str) -> str:
    """The digest stable stores as its password (Options_Login.cs:84)."""
    return hashlib.md5(value.encode()).hexdigest()


passed = 0
failed: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    global passed
    if ok:
        passed += 1
        print(f"  ok   {name}")
    else:
        failed.append(f"{name}: {detail}")
        print(f"  FAIL {name} -- {detail}")


async def main() -> int:
    user_id = provision_user()

    transport = httpx.ASGITransport(app=asgi_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t", timeout=30) as c:
        r = await c.post(
            "/oauth/token",
            files={
                "grant_type": (None, "password"),
                "client_id": (None, "5"),
                "client_secret": (None, "devsecret"),
                "scope": (None, "*"),
                "username": (None, "e2e"),
                "password": (None, "e2epass123"),
            },
        )
        token = r.json()["access_token"]
        auth = {"Authorization": f"Bearer {token}"}

        # APINotificationsBundle: the connector dereferences Response!.Endpoint,
        # so a missing field here is a NullReferenceException on every retry.
        print("\nnotifications")
        r = await c.get(f"{BASE}/notifications", headers=auth)
        check("GET /notifications 200", r.status_code == 200, r.text[:120])
        body = r.json() if r.status_code == 200 else {}
        check("has has_more", "has_more" in body, r.text[:120])
        check("has notifications array", isinstance(body.get("notifications"), list), r.text[:120])
        endpoint = body.get("notification_endpoint") or ""
        check("notification_endpoint is a ws url", endpoint.startswith("ws"), repr(endpoint))

        print("\nrelations and favourites")
        for path, label in (
            ("/friends", "friends"),
            ("/blocks", "blocks"),
        ):
            r = await c.get(f"{BASE}{path}", headers=auth)
            check(f"GET {path} 200", r.status_code == 200, r.text[:120])
            check(f"{label} is an array", isinstance(r.json(), list), r.text[:120])

        r = await c.get(f"{BASE}/me/beatmapset-favourites", headers=auth)
        check("GET /me/beatmapset-favourites 200", r.status_code == 200, r.text[:120])
        check("favourites has beatmapset_ids", "beatmapset_ids" in r.json(), r.text[:120])

        r = await c.post(f"{BASE}/chat/ack", headers=auth)
        check("POST /chat/ack 200", r.status_code == 200, r.text[:120])
        check("chat/ack has silences", "silences" in r.json(), r.text[:120])

        print("\nchat updates")
        # GetUpdatesResponse binds PascalCase fields, unlike everything else on
        # this route. Newtonsoft would match snake_case case-insensitively, so
        # the alias is checked exactly as the DTO declares it.
        r = await c.get(
            f"{BASE}/chat/updates",
            params={"since": 0, "includes[]": "presence"},
            headers=auth,
        )
        check("GET /chat/updates 200", r.status_code == 200, r.text[:120])
        body = r.json() if r.status_code == 200 else {}
        check("has Presence", isinstance(body.get("Presence"), list), r.text[:120])
        check("has Messages", isinstance(body.get("Messages"), list), r.text[:120])

        print("\nchat channels")
        # The client re-polls this continuously while the lounge is open, so a
        # 404 here is a permanent retry loop in the log rather than one failure.
        r = await c.get(f"{BASE}/chat/channels", headers=auth)
        check("GET /chat/channels 200", r.status_code == 200, r.text[:120])
        check("channels is an array", isinstance(r.json(), list), r.text[:120])
        r = await c.get(f"{BASE}/chat/channels", headers={"Authorization": "Bearer garbage"})
        check("GET /chat/channels without a token is 401", r.status_code == 401, r.text[:120])

        print("\nseasonal backgrounds")
        r = await c.get(f"{BASE}/seasonal-backgrounds")
        check("GET /seasonal-backgrounds 200", r.status_code == 200, r.text[:120])
        sb = r.json() if r.status_code == 200 else {}
        check("has backgrounds array", isinstance(sb.get("backgrounds"), list), r.text[:120])
        check("has ends_at", isinstance(sb.get("ends_at"), str), r.text[:120])

        print("\npassword grant accepts both credential forms")
        # lazer sends plaintext; stable holds only md5(password) (Options_Login.cs:84
        # stores CryptoHelper.GetMd5String) and sends that. Both must authenticate
        # against the same stored bcrypt(md5(pw)).
        for label, secret in (("plaintext", "e2epass123"), ("pre-md5 hex", md5_hex("e2epass123"))):
            r = await c.post(
                "/oauth/token",
                data={
                    "grant_type": "password",
                    "client_id": "5",
                    "client_secret": "devsecret",
                    "scope": "*",
                    "username": "e2e",
                    "password": secret,
                },
            )
            check(f"oauth password grant accepts {label}", r.status_code == 200, f"{r.status_code} {r.text[:120]}")

        r = await c.post(
            "/oauth/token",
            data={
                "grant_type": "password",
                "client_id": "5",
                "client_secret": "devsecret",
                "scope": "*",
                "username": "e2e",
                "password": "definitely-wrong",
            },
        )
        check("oauth password grant still rejects a wrong password", r.status_code == 401, str(r.status_code))

        print("\nuser search (nested response)")
        r = await c.get(f"{BASE}/search", params={"mode": "user", "query": "e2e"}, headers=auth)
        check("GET /search 200", r.status_code == 200, r.text[:120])
        body = r.json() if r.status_code == 200 else {}
        # nested: SearchUsersResponse.Users reads data.Users where `data` binds to
        # `user`. A flat {"total":..,"users":[..]} binds nothing.
        check("has total", isinstance(body.get("total"), int), r.text[:120])
        check("has nested user.data array", isinstance((body.get("user") or {}).get("data"), list), r.text[:120])

        print("\nbatch endpoints (trailing-slash ids[] family)")
        r = await c.get(f"{BASE}/beatmaps/", params={"ids[]": [5649109]})
        check("GET /beatmaps/ 200", r.status_code == 200, r.text[:120])
        body = r.json() if r.status_code == 200 else {}
        # an object, NOT a bare array -- the opposite of /chat/channels
        check("has beatmaps array", isinstance(body.get("beatmaps"), list), r.text[:120])
        check("has cursor key", "cursor" in body, r.text[:120])

        r = await c.get(f"{BASE}/users/", params={"ids[]": [77]})
        check("GET /users/ 200", r.status_code == 200, r.text[:120])
        body = r.json() if r.status_code == 200 else {}
        check("has users array", isinstance(body.get("users"), list), r.text[:120])
        check("has cursor key", "cursor" in body, r.text[:120])

        print("\npublic user lookup")
        # lazer sends /users/{id}/{ruleset}?key=id for every user it renders
        for ruleset in ("osu", "taiko", "fruits", "mania"):
            r = await c.get(f"{BASE}/users/{user_id}/{ruleset}?key=id")
            ok = r.status_code == 200 and "statistics" in r.json()
            check(f"GET /users/{{id}}/{ruleset} 200", ok, r.text[:120])

        r = await c.get(f"{BASE}/users/{user_id}/osu?key=id")
        stats = r.json().get("statistics", {}) if r.status_code == 200 else {}
        check("statistics is a flat object", isinstance(stats, dict) and "pp" in stats, r.text[:160])

        r = await c.get(f"{BASE}/users/e2e/osu?key=username")
        check("key=username lookup works", r.status_code == 200, r.text[:120])

        r = await c.get(f"{BASE}/users/{user_id}/osu?key=bogus")
        check("unknown lookup key rejected", r.status_code == 400, f"{r.status_code} {r.text[:80]}")

        r = await c.get(f"{BASE}/users/99999999/osu?key=id")
        check("missing user is 404", r.status_code == 404, f"{r.status_code} {r.text[:80]}")

        print("\nunauthenticated")
        for path in ("/notifications", "/friends", "/blocks", "/me/beatmapset-favourites"):
            r = await c.get(f"{BASE}{path}")
            check(f"GET {path} without a token is 401", r.status_code == 401, f"{r.status_code} {r.text[:80]}")

    print(f"\n{passed} passed, {len(failed)} failed")
    for f in failed:
        print(f"  - {f}")
    return 1 if failed else 0


sys.exit(asyncio.run(main()))
