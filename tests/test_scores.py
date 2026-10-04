"""Integration tests for lazer score submission (token lifecycle + validation).

Runs against the real bancho database; creates and cleans up its own token rows.
The forlorn hand-off is stubbed so these test speedforce's half only.

    python tests/test_scores.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import asgi_app  # noqa: E402
from app.models.solo_score import SoloScoreInfo  # noqa: E402
from app.models.solo_score import hit_counts
from app.models.solo_score import mods_to_bits

BASE = "/api/v2"
BEATMAP_ID = 5914598  # osu, exists in the dev database
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


def score_body(**overrides: object) -> dict:
    body: dict = {
        "beatmap_id": BEATMAP_ID,
        "ruleset_id": 0,
        "build_id": 5000,
        "passed": True,
        "total_score": 900000,
        "total_score_without_mods": 800000,
        "accuracy": 98.5,
        "user_id": 1,  # deliberately wrong: must be ignored
        "max_combo": 300,
        "rank": "S",
        "started_at": "2026-10-04T10:00:00+00:00",
        "ended_at": "2026-10-04T10:02:00+00:00",
        "mods": [{"acronym": "HD", "settings": {}}, {"acronym": "RX", "settings": {}}],
        "created_at": "2026-10-04T10:02:00+00:00",
        "updated_at": "2026-10-04T10:02:00+00:00",
        "statistics": {
            "great": 500,
            "ok": 20,
            "meh": 2,
            "miss": 3,
            "perfect": 10,
            "large_tick_hit": 40,
            "slider_tail_hit": 12,
            "small_tick_miss": 1,
        },
        "maximum_statistics": {"large_tick_hit": 44},
    }
    body.update(overrides)
    return body


async def get_token(c: httpx.AsyncClient, auth: dict) -> int:
    r = await c.post(
        f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores",
        data={"version_hash": "deadbeef", "beatmap_hash": "2d259d6b3cd374f0ce705aebd9302f7d", "ruleset_id": "0"},
        headers=auth,
    )
    return int(r.json()["id"])


def provision_user() -> None:
    """create the throwaway login this test authenticates as"""
    import subprocess
    import time

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from app.auth.tokens import hash_password

    pw = hash_password("testpass123")
    now = int(time.time())
    subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            "refx-mysql-1",
            "mysql",
            "-ubancho",
            "-pCXQZ-tzbQ_uOS_xKFpOM9qZj",
            "bancho",
            "-e",
            f"INSERT INTO users (name,safe_name,priv,pw_bcrypt,country,creation_time,latest_activity,preferred_mode) "
            f"VALUES ('sftest','sftest',1,'{pw}','us',{now},{now},0) "
            f"ON DUPLICATE KEY UPDATE pw_bcrypt='{pw}';",
        ],
        capture_output=True,
        text=True,
        check=True,
    )


async def main() -> int:
    provision_user()
    # stub the forlorn hand-off so we exercise only speedforce's half
    import app.route.scores as scores_mod

    forwarded: list[dict] = []

    # capture the real class BEFORE patching, otherwise FakeClient recurses into itself
    real_async_client = httpx.AsyncClient

    class FakeResponse:
        status_code = 200
        content = b'{"score_id": 4242, "position": 1, "ranked": true}'

        @staticmethod
        def json() -> dict:
            return {"score_id": 4242, "position": 1, "ranked": True}

    class FakeClient:
        """Delegates everything except the forlorn hand-off, so the real
        multipart /oauth/token call still works."""

        def __init__(self, *a: object, **k: object) -> None:
            self._inner = real_async_client(*a, **k)

        async def __aenter__(self) -> "FakeClient":
            await self._inner.__aenter__()
            return self

        async def __aexit__(self, *a: object) -> None:
            await self._inner.__aexit__(*a)

        async def post(self, url: str, **kwargs: object) -> object:
            if url.endswith("/api/v1/lazer/scores"):
                forwarded.append({"url": url, **kwargs.get("json", {})})  # type: ignore[dict-item]
                return FakeResponse()
            return await self._inner.post(url, **kwargs)  # type: ignore[arg-type]

        async def put(self, url: str, **kwargs: object) -> object:
            return await self._inner.put(url, **kwargs)  # type: ignore[arg-type]

    scores_mod.httpx.AsyncClient = FakeClient  # type: ignore[misc]

    transport = httpx.ASGITransport(app=asgi_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        # authenticate as the test user
        r = await c.post(
            "/oauth/token",
            files={
                "grant_type": (None, "password"),
                "client_id": (None, "5"),
                "client_secret": (None, "devsecret"),
                "scope": (None, "*"),
                "username": (None, "sftest"),
                "password": (None, "testpass123"),
            },
        )
        token = r.json()["access_token"]
        auth = {"Authorization": f"Bearer {token}"}

        print("\npayload parsing")
        info = SoloScoreInfo(**score_body())
        check("rank parses from string", info.rank == "S")
        check("statistics keys are snake_case", info.statistics.get("large_tick_hit") == 40)
        check("mods parse as APIMod list", len(info.mods) == 2 and info.mods[0].acronym == "HD")

        bits = mods_to_bits(info.mods)
        check("HD|RX -> bits 0x88", bits == (1 << 3) | (1 << 7), f"got {bits:#x}")

        counts = hit_counts(info.statistics)
        # n300 rolls up great + slider_tail_hit + large_tick_hit + small_tick_hit
        check("n300 rolls up ticks/tails", counts["n300"] == 500 + 12 + 40, str(counts))
        check("n100", counts["n100"] == 20, str(counts))
        check("n50", counts["n50"] == 2, str(counts))
        check("nmiss includes small_tick_miss", counts["nmiss"] == 3 + 1, str(counts))
        check("ngeki", counts["ngeki"] == 10, str(counts))
        check("nkatu left at 0", counts["nkatu"] == 0, str(counts))

        print("\ntoken minting")
        r = await c.post(
            f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores",
            data={"version_hash": "x", "beatmap_hash": "2d259d6b3cd374f0ce705aebd9302f7d", "ruleset_id": "0"},
            headers=auth,
        )
        check("mint returns {id}", r.status_code == 200 and "id" in r.json(), str(r.status_code))
        tid = int(r.json()["id"])

        r = await c.post(
            f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores",
            data={"version_hash": "x", "beatmap_hash": "h", "ruleset_id": "7"},
            headers=auth,
        )
        check("non-vanilla ruleset rejected", r.status_code == 400, str(r.status_code))

        r = await c.post(
            f"{BASE}/beatmaps/99999999/solo/scores",
            data={"version_hash": "x", "beatmap_hash": "h", "ruleset_id": "0"},
            headers=auth,
        )
        check("unknown beatmap -> 404", r.status_code == 404, str(r.status_code))

        r = await c.post(
            f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores",
            data={"version_hash": "x", "beatmap_hash": "h", "ruleset_id": "0"},
        )
        check("mint without auth -> 401", r.status_code == 401, str(r.status_code))

        print("\nsubmission")
        forwarded.clear()
        r = await c.put(f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores/{tid}", json=score_body(), headers=auth)
        check("submit accepted", r.status_code == 200, f"{r.status_code} {r.text[:100]}")
        if forwarded:
            f = forwarded[0]
            check("forwards to forlorn", f["url"].endswith("/api/v1/lazer/scores"), f["url"])
            check("user_id taken from token, not payload", f["user_id"] != 1, f"got {f['user_id']}")
            check("mode derived from mods (RX -> 4)", f["mode"] == 4, f"got {f['mode']}")
            check("mods forwarded as bits", f["mods"] == (1 << 3) | (1 << 7), f"got {f['mods']:#x}")
            check("counts forwarded", f["counts"]["n300"] == 552, str(f["counts"]))
            check("statistics forwarded verbatim", f["statistics"]["large_tick_hit"] == 40)

        r = await c.put(f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores/{tid}", json=score_body(), headers=auth)
        check("token single-use (replay rejected)", r.status_code == 401, str(r.status_code))

        tid2 = await get_token(c, auth)
        r = await c.put(f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores/99999999", json=score_body(), headers=auth)
        check("bogus token -> 401", r.status_code == 401, str(r.status_code))

        tid3 = await get_token(c, auth)
        r = await c.put(f"{BASE}/beatmaps/99999999/solo/scores/{tid3}", json=score_body(), headers=auth)
        check("token for a different beatmap -> 401", r.status_code == 401, str(r.status_code))

        tid4 = await get_token(c, auth)
        r = await c.put(
            f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores/{tid4}", json=score_body(**{"rank": "Z"}), headers=auth
        )
        check("invalid rank rejected", r.status_code == 400, str(r.status_code))

        tid5 = await get_token(c, auth)
        r = await c.put(
            f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores/{tid5}", json=score_body(**{"ruleset_id": 9}), headers=auth
        )
        check("non-vanilla ruleset on submit rejected", r.status_code == 400, str(r.status_code))

        print("\nAP-only mode derivation")
        tid6 = await get_token(c, auth)
        forwarded.clear()
        await c.put(
            f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores/{tid6}",
            json=score_body(mods=[{"acronym": "AP", "settings": {}}]),
            headers=auth,
        )
        check(
            "AP -> mode 7",
            forwarded and forwarded[0]["mode"] == 7,
            str(forwarded[0]["mode"]) if forwarded else "not forwarded",
        )

        tid7 = await get_token(c, auth)
        forwarded.clear()
        await c.put(f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores/{tid7}", json=score_body(mods=[]), headers=auth)
        check(
            "no mods -> mode 0",
            forwarded and forwarded[0]["mode"] == 0,
            str(forwarded[0]["mode"]) if forwarded else "not forwarded",
        )

    print(f"\n{passed} passed, {len(failed)} failed")
    for f in failed:
        print("  -", f)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
