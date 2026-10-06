"""In-process integration tests for `GET /api/v2/comments` and the chat
mark-as-read route.

Runs against real rows: the comments are inserted into bancho's `comments`
table and removed again afterwards, because the stable client reads the same
table and the endpoint has nothing else to read.

The assertions that matter are the shape ones. `CommentBundle`'s list properties
dereference each other in their setters, so a null list is a client-side
NullReferenceException rather than an empty comment section -- a 404 or a
half-populated bundle both look like "comments are broken" from the player's
seat, with nothing in the log to say why.

    python tests/test_comments.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_ranked_submit import provision_user  # noqa: E402
from test_ranked_submit import sql  # noqa: E402

from app import asgi_app  # noqa: E402

BASE = "/api/v2"
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


def insert_comments(user_id: int, target_id: int) -> list[int]:
    """Three beatmapset comments and one per-beatmap comment.

    The per-beatmap row is the one that matters: `target_type='map'` must not
    leak into a `commentable_type=beatmapset` response, and the enum value in
    the table (`song`) is not the name the client sends (`beatmapset`).
    """
    sql("DELETE FROM comments WHERE target_type='song' AND target_id=%d;" % target_id)
    ids = []
    for n in range(3):
        out = sql(
            "INSERT INTO comments (target_id,target_type,userid,time,comment) "
            "VALUES (%d,'song',%d,UNIX_TIMESTAMP()+%d,'comment number %d');" % (target_id, user_id, n, n)
        )
        ids.append(out.strip().splitlines()[-1] if out.strip() else "0")
    sql(
        "INSERT INTO comments (target_id,target_type,userid,time,comment) "
        "VALUES (%d,'map',%d,UNIX_TIMESTAMP(),'this is a per-beatmap comment');" % (target_id, user_id)
    )
    return ids


def cleanup(target_id: int) -> None:
    sql("DELETE FROM comments WHERE target_id=%d;" % target_id)


async def main() -> int:
    user_id = provision_user()
    target_id = 999000001
    insert_comments(user_id, target_id)

    transport = httpx.ASGITransport(app=asgi_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test", timeout=60) as c:
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

        print("\ncomments bundle shape")
        r = await c.get(
            f"{BASE}/comments",
            params={"commentable_id": target_id, "commentable_type": "beatmapset", "page": 1, "sort": "new"},
            headers=auth,
        )
        check("GET /comments is 200", r.status_code == 200, f"{r.status_code} {r.text[:200]}")
        d = r.json()

        # every one of these is dereferenced by a CommentBundle setter on the
        # client; a null here throws inside Newtonsoft before the UI is built
        for field in ("comments", "pinned_comments", "included_comments", "user_votes", "users", "commentable_meta"):
            check(f"{field} is a list, not null", isinstance(d.get(field), list), f"{field}={d.get(field)!r}")

        check("comments populated", len(d["comments"]) == 3, f"got {len(d['comments'])}")
        check("per-beatmap comment excluded", all("per-beatmap" not in c0["message"] for c0 in d["comments"]))
        check("total matches", d.get("total") == 3, str(d.get("total")))
        check("has_more false on last page", d.get("has_more") is False, str(d.get("has_more")))

        print("\ncomment fields the client binds")
        if d["comments"]:
            c0 = d["comments"][0]
            for field in (
                "id",
                "parent_id",
                "user_id",
                "message",
                "message_html",
                "replies_count",
                "votes_count",
                "commentable_type",
                "commentable_id",
                "legacy_name",
                "created_at",
                "pinned",
            ):
                check(f"comment has {field}", field in c0, str(sorted(c0)))
            check(
                "commentable_type echoes the client's name",
                c0["commentable_type"] == "beatmapset",
                c0["commentable_type"],
            )
            check("commentable_id echoed", c0["commentable_id"] == target_id, str(c0["commentable_id"]))
            # CreatedAt is a non-nullable DateTimeOffset; a null or unparsable
            # value throws in Newtonsoft rather than rendering.
            check(
                "created_at is a real timestamp",
                isinstance(c0["created_at"], str) and c0["created_at"].startswith("20"),
                str(c0["created_at"]),
            )
            check("top-level has null parent_id", c0["parent_id"] is None, str(c0["parent_id"]))

        print("\nauthor resolution")
        check("author embedded in users[]", len(d["users"]) == 1, str(len(d["users"])))
        if d["users"]:
            u = d["users"][0]
            check("user has id/username", u.get("id") == user_id and u.get("username") == "e2e", str(u)[:120])
            check("user has statistics", "statistics" in u, str(sorted(u))[:120])

        print("\npaging and empty states")
        r = await c.get(
            f"{BASE}/comments",
            params={"commentable_id": target_id, "commentable_type": "beatmapset", "page": 2},
            headers=auth,
        )
        check("page 2 is 200 and empty", r.status_code == 200 and r.json()["comments"] == [], r.text[:120])

        r = await c.get(
            f"{BASE}/comments",
            params={"commentable_id": 999000002, "commentable_type": "beatmapset"},
            headers=auth,
        )
        d2 = r.json()
        check(
            "unknown commentable is an empty bundle",
            r.status_code == 200 and d2["comments"] == [] and d2["total"] == 0,
            r.text[:120],
        )
        check(
            "empty bundle still has all lists",
            isinstance(d2["pinned_comments"], list) and isinstance(d2["users"], list),
        )

        # an unsupported commentable type must not 500 -- the client would retry
        r = await c.get(
            f"{BASE}/comments",
            params={"commentable_id": 1, "commentable_type": "news_post"},
            headers=auth,
        )
        check("unsupported type is an empty bundle, not a 500", r.status_code == 200, f"{r.status_code} {r.text[:120]}")

        r = await c.get(f"{BASE}/comments", params={"commentable_type": "beatmapset"}, headers=auth)
        check("missing commentable_id is a 422", r.status_code == 422, str(r.status_code))

        print("\nchat mark-as-read")
        r = await c.get(f"{BASE}/chat/channels/7/mark-as-read/1234", headers=auth)
        check("mark-as-read is 200", r.status_code == 200, f"{r.status_code} {r.text[:120]}")
        check("mark-as-read body is an object", isinstance(r.json(), dict), r.text[:120])

    cleanup(target_id)

    print(f"\n{passed} passed, {len(failed)} failed")
    for f in failed:
        print("  -", f)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
