"""lazer leaderboard reads: GET /beatmaps/{id}/scores.

Covers the speedforce -> forlorn hand-off, including the auth and validation
paths the client depends on to avoid dropping into APIState.Failing.

    uv run python tests/test_leaderboard.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_pb_progression import submit  # noqa: E402
from test_ranked_submit import BEATMAP_ID  # noqa: E402
from test_ranked_submit import MD5
from test_ranked_submit import provision_user
from test_ranked_submit import reset

from app import asgi_app  # noqa: E402

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
    reset(user_id)
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
        auth = {"Authorization": f"Bearer {r.json()['access_token']}"}

        print("\nvalidation")
        r = await c.get(f"/api/v2/beatmaps/{BEATMAP_ID}/scores")
        check("unauthenticated is a clean 401", r.status_code == 401, str(r.status_code))
        r = await c.get(f"/api/v2/beatmaps/{BEATMAP_ID}/scores?mode=9", headers=auth)
        check("ruleset 9 rejected", r.status_code == 422, str(r.status_code))
        r = await c.get(f"/api/v2/beatmaps/{BEATMAP_ID}/scores?sort=bogus", headers=auth)
        check("unknown sort rejected", r.status_code == 422, str(r.status_code))
        r = await c.get("/api/v2/beatmaps/1/scores", headers=auth)
        check("unknown beatmap is 404", r.status_code == 404, str(r.status_code))

        print("\nempty leaderboard")
        r = await c.get(f"/api/v2/beatmaps/{BEATMAP_ID}/scores?mode=0", headers=auth)
        body = r.json()
        check("empty leaderboard is 200", r.status_code == 200, str(r.status_code))
        check("empty list", body.get("scores") == [], str(body))
        check("count is 0", body.get("total_score_count") == 0, str(body))

        print("\nafter a submission")
        await submit(c, auth, 850_000, 97.25, {"great": 800, "ok": 30, "meh": 3, "miss": 5, "perfect": 40})
        await submit(c, auth, 900_000, 99.1, {"great": 830, "ok": 10, "meh": 1, "miss": 2, "perfect": 55})

        r = await c.get(f"/api/v2/beatmaps/{BEATMAP_ID}/scores?mode=0", headers=auth)
        body = r.json()
        rows = body.get("scores", [])
        check("one personal best listed", len(rows) == 1, f"{len(rows)} rows")
        check("count matches", body.get("total_score_count") == len(rows), str(body))
        if rows:
            s = rows[0]
            check("best (not superseded) score kept", s["total_score"] == 900_000, str(s["total_score"]))
            check("accuracy carried through", abs(s["accuracy"] - 99.1) < 0.01, str(s["accuracy"]))
            check("grade letter present", s["rank"] in ("S", "SS", "A", "B", "C", "D"), s["rank"])
            check("is_hd derived from mods", s["is_hd"] is True, str(s["mods"]))
            check("scorer user block present", s["user"]["username"] == "e2e", str(s["user"]))
            check("scorer's own country", s["user"]["country_code"] == "us", str(s["user"]))
            check("statistics mapped", s["statistics"]["great"] > 0, str(s["statistics"]))
            check("beatmap_id echoed", s["beatmap_id"] == BEATMAP_ID, str(s["beatmap_id"]))

        print("\nsorting")
        r = await c.get(f"/api/v2/beatmaps/{BEATMAP_ID}/scores?mode=0&sort=accuracy", headers=auth)
        check("sort=accuracy accepted", r.status_code == 200, str(r.status_code))

        print("\nwrong ruleset sees a different (empty) board")
        r = await c.get(f"/api/v2/beatmaps/{BEATMAP_ID}/scores?mode=3", headers=auth)
        check("taiko board is empty", r.json().get("scores") == [], r.text[:120])

        reset(user_id)

    print(f"\n{passed} passed, {len(failed)} failed")
    for f in failed:
        print(f"  - {f}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
