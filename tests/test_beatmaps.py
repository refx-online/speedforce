"""In-process integration tests for the beatmap endpoints.

Uses httpx's ASGITransport so the full ASGI stack (routing, dependency injection,
serialisation) is exercised without binding a port. Needs a reachable bancho
database -- these run against real rows, not fixtures.

    python tests/test_beatmaps.py
"""

from __future__ import annotations

import asyncio
import sys

import httpx

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

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


async def main() -> int:
    transport = httpx.ASGITransport(app=asgi_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        print("\nsearch")
        r = await c.get(
            f"{BASE}/beatmapsets/search", params={"q": "", "m": 0, "s": "any", "sort": "title_asc", "nsfw": "false"}
        )
        check("search returns 200", r.status_code == 200, f"got {r.status_code}")
        d = r.json()
        check("search has total+sets", "total" in d and "beatmapsets" in d, str(d)[:120])
        first = d["beatmapsets"][0] if d["beatmapsets"] else None
        check("search returns a set", first is not None)
        if first:
            b = first["beatmaps"][0]
            # field mapping the client binds by literal name
            check("beatmap id/beatmapset_id", b["id"] > 0 and b["beatmapset_id"] == first["id"])
            check("od mapped to accuracy", "accuracy" in b and "drain" in b, str(sorted(b)[:8]))
            check("set has covers", "covers" in first and "list" in first["covers"])
            # covers must point at URLs that actually serve an image
            covers = first.get("covers") or {}
            check("no broken header variant advertised", "header" not in covers, str(list(covers)))
            check(
                "cover points at osu's CDN",
                (covers.get("cover") or "").startswith("https://assets.ppy.sh/beatmaps/"),
                str(covers.get("cover")),
            )

        r = await c.get(f"{BASE}/beatmapsets/search", params={"q": "zzzznomatchxyz"})
        check("no-match search returns empty", r.json()["total"] == 0)

        r = await c.get(f"{BASE}/beatmapsets/search", params={"s": "ranked"})
        check("ranked filter (all maps are qualified)", r.json()["total"] == 0)

        r = await c.get(f"{BASE}/beatmapsets/search", params={"s": "qualified"})
        check("qualified filter returns all", r.json()["total"] > 0, str(r.json()["total"]))

        print("\nlookup")
        r = await c.get(f"{BASE}/beatmaps/lookup", params={"id": 5914598})
        check(
            "beatmaps/lookup by id", r.status_code == 200 and r.json()["beatmapset_id"] == 2630196, str(r.status_code)
        )
        beatmap_id = r.json()["id"]
        checksum = r.json()["checksum"]

        # the route-ordering trap: /beatmapsets/lookup must beat /beatmapsets/{id}
        r = await c.get(f"{BASE}/beatmapsets/lookup", params={"beatmap_id": beatmap_id})
        check(
            "beatmapsets/lookup (declared before {id})",
            r.status_code == 200 and r.json()["id"] == 2630196,
            str(r.status_code),
        )

        r = await c.get(f"{BASE}/beatmapsets/2630192")
        check("beatmapsets/{id}", r.status_code == 200 and r.json()["id"] == 2630192, str(r.status_code))

        r = await c.get(f"{BASE}/beatmaps/lookup", params={"id": beatmap_id, "checksum": "deadbeef"})
        check("checksum mismatch -> 404", r.status_code == 404, str(r.status_code))

        r = await c.get(f"{BASE}/beatmapsets/99999999")
        check("unknown set -> 404", r.status_code == 404, str(r.status_code))

        print("\ndownload")
        r = await c.get(f"{BASE}/beatmapsets/2630192/download")
        check("download returns 200", r.status_code == 200, str(r.status_code))
        if r.status_code == 200:
            body = r.content
            check("body is a zip", body[:2] == b"PK", repr(body[:8]))
            import io
            import zipfile

            try:
                z = zipfile.ZipFile(io.BytesIO(body))
                names = z.namelist()
                check("archive contains a .osu", any(n.endswith(".osu") for n in names), str(names[:4]))
                biggest = max(i.file_size for i in z.infolist())
                check("no entry over 100MB (client limit)", biggest < 100 * 1024 * 1024, f"{biggest} bytes")
            except zipfile.BadZipFile as e:
                check("archive parses", False, str(e))

        r = await c.get(f"{BASE}/beatmapsets/99999999/download")
        check("unknown set download -> 404", r.status_code == 404, str(r.status_code))

        r = await c.get(f"{BASE}/beatmapsets/2630192/download", params={"noVideo": 1})
        check("noVideo=1 accepted", r.status_code == 200, str(r.status_code))

        print("\nsingle beatmap detail (lazer GetBeatmapRequest)")
        r = await c.get("/api/v2/beatmaps/5649109")
        check("GET /beatmaps/{id} is 200", r.status_code == 200, str(r.status_code))
        if r.status_code == 200:
            b = r.json()
            check("id echoed", b.get("id") == 5649109, str(b.get("id")))
            check("beatmapset linked", b.get("beatmapset_id") == 2548284, str(b.get("beatmapset_id")))
            check(
                "difficulty_rating real",
                abs((b.get("difficulty_rating") or 0) - 10.52) < 0.1,
                str(b.get("difficulty_rating")),
            )
            check("bpm real", (b.get("bpm") or 0) > 0, str(b.get("bpm")))
            # these are what the difficulty picker bar is built from
            check("count_circles populated", b.get("count_circles") == 1098, str(b.get("count_circles")))
            check("count_sliders populated", b.get("count_sliders") == 121, str(b.get("count_sliders")))
            check("count_spinners populated", b.get("count_spinners") == 1, str(b.get("count_spinners")))
            check("hit_length real (not total_length)", b.get("hit_length") == 214, str(b.get("hit_length")))
        r = await c.get("/api/v2/beatmaps/99999999")
        check("unknown beatmap is 404", r.status_code == 404, str(r.status_code))
        r = await c.get("/api/v2/beatmaps/lookup?id=5649109")
        check("lookup still routes correctly", r.status_code == 200, str(r.status_code))

    print(f"\n{passed} passed, {len(failed)} failed")
    for f in failed:
        print("  -", f)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
