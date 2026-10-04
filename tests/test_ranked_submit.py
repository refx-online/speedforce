"""End-to-end: lazer score submission against a RANKED beatmap (2548284/5649109).

speedforce runs in-process (ASGITransport) but its hand-off to forlorn is a real
HTTP call, so this exercises the whole write path including usecases::score.

Needs forlorn listening on 3030 and a populated bancho db.

    python tests/test_e2e_submit.py
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import asgi_app  # noqa: E402

BASE = "/api/v2"
BEATMAP_ID = 5649109
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


def sql(query: str) -> str:
    out = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            "refx-mysql-1",
            "mysql",
            "-ubancho",
            "-pCXQZ-tzbQ_uOS_xKFpOM9qZj",
            "bancho",
            "-N",
            "-B",
            "-e",
            query,
        ],
        capture_output=True,
        text=True,
    )
    return out.stdout.strip()


MD5 = "1c72a74f198f72a690164580a5193030"
DB_PW = "CXQZ-tzbQ_uOS_xKFpOM9qZj"


def mysql(stmt: str) -> None:
    subprocess.run(
        ["docker", "exec", "-i", "refx-mysql-1", "mysql", "-ubancho", f"-p{DB_PW}", "bancho", "-e", stmt],
        capture_output=True,
        text=True,
        check=True,
    )


def redis(*args: str) -> str:
    return subprocess.run(
        ["docker", "exec", "refx-redis-1", "redis-cli", *args],
        capture_output=True,
        text=True,
    ).stdout.strip()


def reset(user_id: int) -> None:
    """Make the run repeatable: a re-submit of the same score is not a personal
    best, and only a PB updates the leaderboard -- so previous state has to go
    or the assertions below are testing the wrong thing."""
    mysql(
        f"DELETE FROM lazer_scores WHERE score_id IN "
        f"(SELECT id FROM scores WHERE userid={user_id} AND map_md5='{MD5}');"
        f"DELETE FROM scores WHERE userid={user_id} AND map_md5='{MD5}';"
        f"DELETE FROM score_tokens WHERE user_id={user_id};"
        f"UPDATE stats SET plays=0, tscore=0, rscore=0, max_combo=0, pp=0, xp=0, playtime=0 "
        f"WHERE id={user_id} AND mode=0;"
    )
    redis("DEL", "bancho:leaderboard:0", "bancho:leaderboard:0:us")


def provision_user() -> int:
    """create the throwaway login this test authenticates as"""
    from app.auth.tokens import hash_password

    pw = hash_password("e2epass123")
    now = int(__import__("time").time())
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
            f"VALUES ('e2e','e2e',1,'{pw}','us',{now},{now},0) "
            f"ON DUPLICATE KEY UPDATE pw_bcrypt='{pw}';",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    uid = int(sql("SELECT id FROM users WHERE name='e2e'"))
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
            f"INSERT IGNORE INTO stats (id,mode) VALUES ({uid},0);",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return uid


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
        token = r.json()["access_token"]
        auth = {"Authorization": f"Bearer {token}"}

        print("\nsubmitting a lazer-shaped score end to end")
        r = await c.post(
            f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores",
            data={"version_hash": "e2e", "beatmap_hash": "1c72a74f198f72a690164580a5193030", "ruleset_id": "0"},
            headers=auth,
        )
        tid = int(r.json()["id"])
        check("token minted", r.status_code == 200 and tid > 0, str(r.status_code))

        body = {
            "beatmap_id": BEATMAP_ID,
            "ruleset_id": 0,
            "build_id": 9999,
            "passed": True,
            "total_score": 850000,
            "total_score_without_mods": 800000,
            "accuracy": 97.25,
            "user_id": 999999,  # must be ignored
            "max_combo": 412,
            "rank": "S",
            "started_at": "2026-10-04T12:00:00+00:00",
            "ended_at": "2026-10-04T12:01:30+00:00",
            "mods": [{"acronym": "HD", "settings": {}}],
            "created_at": "2026-10-04T12:01:30+00:00",
            "updated_at": "2026-10-04T12:01:30+00:00",
            "statistics": {"great": 800, "ok": 30, "meh": 3, "miss": 5, "perfect": 40, "large_tick_hit": 60},
            "maximum_statistics": {"large_tick_hit": 62},
        }
        r = await c.put(f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores/{tid}", json=body, headers=auth)
        check("submit accepted", r.status_code == 200, f"{r.status_code} {r.text[:200]}")
        resp = r.json() if r.status_code == 200 else {}
        check("response has an id", resp.get("id", 0) > 0, str(resp))
        score_id = resp.get("id", 0)

        print("\nverifying the database")
        row = sql(
            f"SELECT userid, score, mods, acc, max_combo, n300, nmiss, mode, grade, pp FROM scores WHERE id={score_id}"
        )
        check("score row exists", bool(row), "no row")
        if row:
            f = row.split("\t")
            check("userid from token, not payload", f[0] == str(user_id), f"got {f[0]}")
            check("score value", f[1] == "850000", f[1])
            check("mods bitfield (HD)", f[2] == "8", f[2])
            check("accuracy", f[3].startswith("97.25"), f[3])
            check("max_combo", f[4] == "412", f[4])
            check("n300 rolled up (800+60)", f[5] == "860", f[5])
            check("nmiss", f[6] == "5", f[6])
            check("mode is vanilla osu", f[7] == "0", f[7])
            check("grade", f[8] == "S", f[8])
            check("pp computed (non-zero)", float(f[9]) > 0, f[9])

        lz = sql(
            f"SELECT score_id, build_id, is_legacy_score, statistics_json, mods_json FROM lazer_scores WHERE score_id={score_id}"
        )
        check("lazer_scores row written", bool(lz), "no row -- nothing wrote this table before")
        if lz:
            f = lz.split("\t")
            check("build_id recorded", f[1] == "9999", f[1])
            check("is_legacy_score = 0", f[2] == "0", f[2])
            check("statistics stored verbatim", "large_tick_hit" in f[3], f[3][:80])
            check("mods stored as json", "HD" in f[4], f[4][:80])

        tok = sql(f"SELECT score_id FROM score_tokens WHERE id={tid}")
        check("token bound to the score", tok == str(score_id), f"got {tok}")

        # KNOWN GAP (not a lazer-path bug): forlorn publishes refx:refresh_stats,
        # but bakenohana's handler does `player = PlayerSession.get(id:); return
        # unless player` -- it only recomputes for users *currently connected to
        # bancho*. lazer players never are, so stats and the leaderboard ZSET stay
        # empty for them. Needs a bakenohana change to recompute offline.
        st = sql(f"SELECT tscore, plays, rscore, max_combo FROM stats WHERE id={user_id} AND mode=0")
        check("stats row updated by forlorn", bool(st), "no stats row")
        if st:
            f = st.split("\t")
            check("stats.plays = 1", f[1] == "1", f[1])
            check("stats.tscore advanced", int(f[0]) > 0, f[0])
            check("stats.max_combo recorded", int(f[3]) == 412, f[3])

        zb = subprocess.run(
            ["docker", "exec", "refx-redis-1", "redis-cli", "ZCARD", "bancho:leaderboard:0"],
            capture_output=True,
            text=True,
        ).stdout.strip()
        check("leaderboard ZSET populated", zb not in ("", "0"), f"ZCARD={zb}")

        print("\nreplay is rejected (token already spent)")
        r2 = await c.put(f"{BASE}/beatmaps/{BEATMAP_ID}/solo/scores/{tid}", json=body, headers=auth)
        check("replay rejected", r2.status_code == 401, str(r2.status_code))

    print(f"\n{passed} passed, {len(failed)} failed")
    for f in failed:
        print("  -", f)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
