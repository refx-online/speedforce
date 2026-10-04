from __future__ import annotations

import datetime
import os

import httpx
from fastapi import APIRouter
from fastapi import Depends
from fastapi import Query
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.responses import Response

import app.settings as settings
from app.models.beatmap import BeatmapSetsResponse
from app.models.beatmap_mapper import row_to_beatmap
from app.models.beatmap_mapper import rows_to_set
from app.models.beatmap_repo import PAGE_SIZE
from app.models.beatmap_repo import MapRow
from app.models.beatmap_repo import SearchFilters
from app.models.beatmap_repo import get_beatmap_rows_by_ids
from app.models.beatmap_repo import get_beatmapset_rows
from app.models.beatmap_repo import get_set_ids_for_beatmap_ids
from app.models.beatmap_repo import search_beatmapsets
from app.models.repository import UserRow
from app.models.score_repo import get_map
from app.route.me import require_user

router = APIRouter(prefix="/api/v2", tags=["osu! API v2"])


@router.get("/beatmapsets/search")
async def search(
    q: str | None = None,
    m: int | None = Query(None, alias="m"),
    s: str = "any",
    sort: str = "title_asc",
    g: int | None = None,
    language: int | None = Query(None, alias="l"),
    played: str | None = None,
    nsfw: str = "false",
    # cursor: lazer spreads the previous page's last sort value as a bare param
    c: str | None = None,
) -> BeatmapSetsResponse:
    filters = SearchFilters(
        query=q,
        mode=m,
        category=(s or "any").lower(),
        sort=sort,
        genre=g,
        language=language,
        played=played,
        nsfw=nsfw.lower() == "true",
        cursor_sort=c,
    )

    result = await search_beatmapsets(filters)

    # fold rows into sets, preserving order, and cap at PAGE_SIZE sets
    grouped: dict[int, list[MapRow]] = {}
    for row in result.rows:
        grouped.setdefault(row.set_id, []).append(row)

    sets = []
    for rows in list(grouped.values())[:PAGE_SIZE]:
        beatmap_set = rows_to_set(rows)
        if beatmap_set is not None:
            sets.append(beatmap_set)

    cursor = None
    if result.rows:
        last = result.rows[-1]
        cursor = {"sort": getattr(last, sort.partition("_")[0], None)}

    return BeatmapSetsResponse(beatmapsets=sets, total=result.total, cursor=cursor)


# NOTE: must be declared BEFORE /beatmapsets/{beatmapset_id} -- FastAPI matches in
# declaration order, and the int path param would otherwise swallow "lookup".
@router.get("/beatmapsets/lookup")
async def lookup_set(beatmap_id: int = Query(...)) -> JSONResponse:
    mapping = await get_set_ids_for_beatmap_ids([beatmap_id])
    set_id = mapping.get(beatmap_id)
    if set_id is None:
        return JSONResponse(status_code=404, content={"error": "beatmap not found"})
    rows = await get_beatmapset_rows(set_id)
    beatmap_set = rows_to_set(rows)
    if beatmap_set is None:
        return JSONResponse(status_code=404, content={"error": "beatmapset not found"})
    return JSONResponse(content=beatmap_set.model_dump())


@router.get("/beatmapsets/{beatmapset_id}")
async def get_set(beatmapset_id: int) -> JSONResponse:
    rows = await get_beatmapset_rows(beatmapset_id)
    beatmap_set = rows_to_set(rows)
    if beatmap_set is None:
        return JSONResponse(status_code=404, content={"error": "beatmapset not found"})
    return JSONResponse(content=beatmap_set.model_dump())


@router.get("/beatmaps/lookup")
async def lookup_beatmap(id: int = Query(0), checksum: str | None = None) -> JSONResponse:
    if not id:
        return JSONResponse(status_code=404, content={"error": "beatmap not found"})
    rows = await get_beatmap_rows_by_ids([id])
    if not rows:
        return JSONResponse(status_code=404, content={"error": "beatmap not found"})
    if checksum and rows[0].md5.lower() != checksum.lower():
        return JSONResponse(status_code=404, content={"error": "beatmap checksum mismatch"})
    return JSONResponse(content=row_to_beatmap(rows[0]).model_dump())


@router.get("/beatmaps/{beatmap_id}")
async def get_beatmap(beatmap_id: int) -> JSONResponse:
    """Single beatmap detail.

    lazer fetches this for every map it shows in the beatmap info panel and
    before submitting, via `GetBeatmapRequest`. Only `/beatmaps/lookup` existed,
    which lazer never calls.
    """
    rows = await get_beatmap_rows_by_ids([beatmap_id])
    if not rows:
        return JSONResponse(status_code=404, content={"error": "beatmap not found"})
    return JSONResponse(content=row_to_beatmap(rows[0]).model_dump())


# NOTE: lazer streams this to disk and imports it. It must be a real, playable
# .osz -- multiplayer stages block until every client reports the map locally
# available, and the importer needs at least one .osu plus its audio. Entries over
# 100MB inside the archive make the client throw (ZipArchiveReader.MaximumEntrySize).
@router.get("/beatmapsets/{beatmapset_id}/download")
async def download(beatmapset_id: int, noVideo: int | None = Query(None, alias="noVideo")) -> Response:
    """Proxy the .osz from beatmap-service, which owns building them.

    It used to read the file off local disk, but the shared store is a docker
    volume: containers see it at /srv/root/.data while the host sees it under
    /srv/refx-data/shared, so any single configured path is wrong somewhere.
    Proxying also means beatmap-service can build an archive on demand for a set
    we have never downloaded.
    """
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            upstream = await client.get(f"{settings.BEATMAP_SERVICE_URL}/v1/get-osz/{beatmapset_id}")
    except httpx.HTTPError:
        return JSONResponse(status_code=503, content={"error": "beatmap storage unavailable"})

    if upstream.status_code == 404:
        return JSONResponse(status_code=404, content={"error": "beatmapset not found"})
    if upstream.status_code != 200:
        return JSONResponse(status_code=502, content={"error": "beatmap storage unavailable"})

    return Response(
        content=upstream.content,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{beatmapset_id}.osz"',
        },
    )


def _iso(unix: int) -> str:
    return datetime.datetime.fromtimestamp(unix, tz=datetime.timezone.utc).isoformat()


# lazer pages the leaderboard with `sort`, `desc` and a `cursor` rather than
# offsets. Forlorn already returns the whole ranked list for a map (the same
# one stable renders), so sorting and slicing happen here instead of adding
# LIMIT/OFFSET to that shared query.
_SORT_KEYS = {
    "total_score": lambda s: s["score"],
    "accuracy": lambda s: s["accuracy"],
    "max_combo": lambda s: s["max_combo"],
    "pp": lambda s: s["pp"],
    "date": lambda s: s["play_time"],
}


@router.get("/beatmaps/{beatmap_id}/scores")
async def beatmap_scores(
    beatmap_id: int,
    mode: int = Query(0),
    sort: str = Query("total_score"),
    user: UserRow = Depends(require_user),
) -> JSONResponse:
    if not 0 <= mode <= 7:
        return JSONResponse(status_code=422, content={"error": "unsupported ruleset"})

    key = _SORT_KEYS.get(sort)
    if key is None:
        return JSONResponse(status_code=422, content={"error": f"unsupported sort '{sort}'"})

    row = await get_map(beatmap_id)
    if row is None:
        return JSONResponse(status_code=404, content={"error": "beatmap not found"})

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                f"{settings.FORLORN_URL}/api/v1/lazer/beatmaps/{row.md5}/scores",
                params={"mode": mode, "user_id": user.id},
                headers={"X-Internal-Token": settings.LAZER_INTERNAL_TOKEN},
            )
    except httpx.HTTPError:
        return JSONResponse(status_code=503, content={"error": "leaderboard unavailable"})

    if response.status_code != 200:
        return JSONResponse(status_code=502, content={"error": "leaderboard unavailable"})

    scores = sorted(response.json().get("scores", []), key=key, reverse=True)

    # lazer renders the grade letter from these; derive the statistics view it
    # expects from the six legacy counters forlorn stored.
    payload = [
        {
            "id": s["id"],
            "beatmap_id": beatmap_id,
            "user_id": s["user_id"],
            "ruleset_id": mode,
            "accuracy": s["accuracy"],
            "max_combo": s["max_combo"],
            "mods": s["mods"],
            "is_hd": bool(s["mods"] & 8),
            "passed": True,
            "total_score": s["score"],
            # forlorn stores one score value; lazer only uses this for the
            # "without mods" comparison bar
            "total_score_without_mods": s["score"],
            "rank": s.get("grade", "D"),
            "ended_at": _iso(s["play_time"]),
            "started_at": _iso(s["play_time"]),
            "date": _iso(s["play_time"]),
            "statistics": {
                "great": s["n300"],
                "ok": s["n100"],
                "meh": s["n50"],
                "miss": s["nmiss"],
                "perfect": s["ngeki"],
                "good_katu": s["nkatu"],
                "great_katu": s["nkatu"],
            },
            "pp": s["pp"],
            "legacy_score_id": s["id"],
            "user": {
                "id": s["user_id"],
                "username": s["username"],
                "country_code": s["country_code"],
            },
        }
        for s in scores
    ]

    return JSONResponse(content={"scores": payload, "total_score_count": len(payload)})
