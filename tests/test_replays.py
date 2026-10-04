"""lazer replay upload + download.

lazer uploads a replay as a separate request after the score
(SubmitScoreRequest only carries SoloScoreInfo), framed as
int32 legacyScoreId + raw .osr bytes. Download is
``scores/{OnlineID}/download`` (DownloadReplayRequest).

    uv run python tests/test_replays.py
"""

from __future__ import annotations

import asyncio
import struct
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_pb_progression import submit  # noqa: E402
from test_ranked_submit import MD5  # noqa: E402
from test_ranked_submit import provision_user
from test_ranked_submit import reset
from test_ranked_submit import sql

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


# a plausible-looking LEE header; the server must not care about content, only
# that the bytes round-trip verbatim
FAKE_OSR = b"osu file format v14\n" + bytes(range(256)) * 4


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

        await submit(c, auth, 850_000, 97.25, {"great": 800, "ok": 30, "meh": 3, "miss": 5, "perfect": 40})
        score_id = int(sql(f"SELECT id FROM scores WHERE userid={user_id} AND map_md5='{MD5}' LIMIT 1"))
        check("score exists to attach a replay to", score_id > 0, str(score_id))

        print("\nvalidation")
        r = await c.post(f"/api/v2/scores/{score_id}/replay", content=b"ab", headers=auth)
        check("short body rejected", r.status_code == 422, str(r.status_code))
        r = await c.post(
            f"/api/v2/scores/{score_id}/replay",
            content=struct.pack("<i", score_id + 999) + FAKE_OSR,
            headers=auth,
        )
        check("mismatched framed id rejected", r.status_code == 422, str(r.status_code))
        r = await c.get("/api/v2/scores/99999999/download", headers=auth)
        check("unknown score download is 404", r.status_code == 404, str(r.status_code))
        r = await c.post(f"/api/v2/scores/{score_id}/replay", content=struct.pack("<i", score_id) + FAKE_OSR)
        check("upload requires auth first", r.status_code == 401, str(r.status_code))

        print("\nupload")
        r = await c.post(
            f"/api/v2/scores/{score_id}/replay",
            content=struct.pack("<i", score_id) + FAKE_OSR,
            headers=auth,
        )
        check("upload accepted", r.status_code == 200, f"{r.status_code} {r.text[:160]}")
        if r.status_code == 200:
            check("framing stripped (4 bytes)", r.json().get("bytes") == len(FAKE_OSR), r.text)

        print("\ndownload")
        r = await c.get(f"/api/v2/scores/{score_id}/download", headers=auth)
        check("download is 200", r.status_code == 200, str(r.status_code))
        check("bytes round-trip verbatim", r.content == FAKE_OSR, f"{len(r.content)} vs {len(FAKE_OSR)}")
        check(
            "filename is .osr",
            ".osr" in r.headers.get("content-disposition", ""),
            r.headers.get("content-disposition", ""),
        )

        print("\nauth required")
        r = await c.get(f"/api/v2/scores/{score_id}/download")
        check("download requires auth", r.status_code == 401, str(r.status_code))

        reset(user_id)

    print(f"\n{passed} passed, {len(failed)} failed")
    for f in failed:
        print(f"  - {f}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
