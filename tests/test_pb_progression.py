"""PB progression: rscore delta, pp recompute, and leaderboard movement.

Regression cover for `usecases::stats::apply_score`, which is shared by the
stable submission route and the lazer route. The subtlety it guards: the
previous best has to be captured *before* the new score is inserted, otherwise
the new row is itself the best and every delta comes out zero.

Needs forlorn on 3030 and a populated bancho db.

    uv run python tests/test_pb_progression.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_ranked_submit import BEATMAP_ID  # noqa: E402
from test_ranked_submit import MD5
from test_ranked_submit import mysql
from test_ranked_submit import provision_user
from test_ranked_submit import redis
from test_ranked_submit import reset
from test_ranked_submit import sql

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


def stats_row(user_id: int) -> dict[str, int]:
    raw = sql(f"SELECT plays, tscore, rscore, max_combo, pp FROM stats WHERE id={user_id} AND mode=0")
    if not raw:
        return {}
    keys = ("plays", "tscore", "rscore", "max_combo", "pp")
    return dict(zip(keys, (int(v) for v in raw.split("\t"))))


def leaderboard_pp(user_id: int) -> float:
    raw = redis("ZSCORE", "bancho:leaderboard:0", str(user_id))
    try:
        return float(raw)
    except ValueError:
        return 0.0


async def submit(
    client: httpx.AsyncClient,
    auth: dict,
    score: int,
    acc: float,
    stats: dict[str, int],
) -> float:
    """submit one lazer score; returns the pp forlorn computed for it"""
    r = await client.post(
        f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores",
        data={"version_hash": "e2e", "beatmap_hash": MD5, "ruleset_id": "0"},
        headers=auth,
    )
    r.raise_for_status()
    tid = r.json()["id"]

    r = await client.put(
        f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores/{tid}",
        json={
            "beatmap_id": BEATMAP_ID,
            "ruleset_id": 0,
            "build_id": 9999,
            "passed": True,
            "total_score": score,
            "total_score_without_mods": score,
            "accuracy": acc,
            "max_combo": 412,
            "rank": "S",
            "started_at": "2026-10-04T12:00:00+00:00",
            "ended_at": "2026-10-04T12:01:30+00:00",
            "mods": [{"acronym": "HD", "settings": {}}],
            "created_at": "2026-10-04T12:01:30+00:00",
            "updated_at": "2026-10-04T12:01:30+00:00",
            "statistics": stats,
            "maximum_statistics": {"large_tick_hit": 62},
        },
        headers=auth,
    )
    r.raise_for_status()
    sid = r.json()["id"]
    return float(sql(f"SELECT pp FROM scores WHERE id={sid}"))


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

        print("\n1) first play on the map: full score counts toward rscore")
        weaker = {"great": 800, "ok": 30, "meh": 3, "miss": 5, "perfect": 40}
        stronger = {"great": 820, "ok": 15, "meh": 1, "miss": 3, "perfect": 60}

        pp1 = await submit(c, auth, 850_000, 97.25, weaker)
        s1 = stats_row(user_id)
        check("plays = 1", s1.get("plays") == 1, str(s1))
        check("rscore = full first score", s1.get("rscore") == 850_000, str(s1))
        check("tscore = full first score", s1.get("tscore") == 850_000, str(s1))
        check("pp recomputed from the new score", s1.get("pp", 0) > 0, str(s1))
        check("leaderboard reflects pp", leaderboard_pp(user_id) == s1.get("pp"), str(s1))

        print("\n2) better play: rscore gains only the difference")
        pp2 = await submit(c, auth, 900_000, 98.5, stronger)
        s2 = stats_row(user_id)
        check("plays = 2", s2.get("plays") == 2, str(s2))
        check(
            "rscore gained exactly the delta (900000-850000)",
            s2.get("rscore") == 850_000 + 50_000,
            str(s2),
        )
        check("tscore accumulated both plays", s2.get("tscore") == 1_750_000, str(s2))
        check("pp rose with the better play", s2.get("pp", 0) >= s1.get("pp", 0), f"{s1} -> {s2}")
        check("per-score pp rose", pp2 > pp1, f"{pp1} -> {pp2}")
        check("leaderboard followed the new pp", leaderboard_pp(user_id) == s2.get("pp"), str(s2))

        print("\n3) worse play: rejected as a PB, stats must not move")
        before = stats_row(user_id)
        await submit(c, auth, 100_000, 60.0, {"great": 400, "ok": 60, "meh": 10, "miss": 40})
        after = stats_row(user_id)
        check(
            "plays still counts the attempt", after.get("plays") == before.get("plays", 0) + 1, f"{before} -> {after}"
        )
        check("rscore unchanged", after.get("rscore") == before.get("rscore"), f"{before} -> {after}")
        check("pp unchanged", after.get("pp") == before.get("pp"), f"{before} -> {after}")
        check("leaderboard unchanged", leaderboard_pp(user_id) == before.get("pp"), str(after))

        # leave the fixture clean for the other suites
        mysql(f"DELETE FROM score_tokens WHERE user_id={user_id};")
        reset(user_id)

    print(f"\n{passed} passed, {len(failed)} failed")
    for f in failed:
        print(f"  - {f}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
