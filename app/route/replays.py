from __future__ import annotations

from datetime import datetime

import httpx
from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.responses import Response

import app.settings as settings
from app.models.repository import UserRow
from app.route.me import require_user

router = APIRouter(prefix="/api/v2", tags=["osu! API v2"])

# lazer frames a replay upload as int32 legacyScoreId + raw .osr bytes. We don't
# need the prefix (the score id is already in the path) and forlorn stores
# unframed replays like stable does, so it gets stripped either side.
REPLAY_PREFIX_BYTES = 4


@router.post("/scores/{score_id}/replay")
async def upload_replay(
    score_id: int,
    request: Request,
    user: UserRow = Depends(require_user),
) -> JSONResponse:
    """Accept a replay for a score lazer has just submitted.

    lazer uploads the replay as a separate request after the score itself
    (SubmitScoreRequest only carries SoloScoreInfo), so without this every lazer
    score would have no replay to watch.
    """
    body = await request.body()

    if len(body) <= REPLAY_PREFIX_BYTES:
        return JSONResponse(status_code=422, content={"error": "replay body too short"})

    # the framed legacy score id is the client's own handle; it must agree with
    # the score we are attaching to, otherwise the client is confused about which
    # submission this belongs to.
    framed_id = int.from_bytes(body[:REPLAY_PREFIX_BYTES], "little", signed=True)
    if framed_id != score_id:
        return JSONResponse(
            status_code=422,
            content={"error": f"framed score id {framed_id} does not match {score_id}"},
        )

    replay = body[REPLAY_PREFIX_BYTES:]

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{settings.FORLORN_URL}/api/v1/lazer/scores/{score_id}/replay",
                content=replay,
                headers={"X-Internal-Token": settings.LAZER_INTERNAL_TOKEN},
            )
    except httpx.HTTPError:
        return JSONResponse(status_code=503, content={"error": "replay storage unavailable"})

    if response.status_code == 404:
        return JSONResponse(status_code=404, content={"error": "unknown score"})
    if response.status_code != 200:
        return JSONResponse(status_code=502, content={"error": "replay rejected"})

    return JSONResponse(content=response.json())


@router.get("/scores/{score_id}/download")
async def download_replay(
    score_id: int,
    user: UserRow = Depends(require_user),
) -> Response:
    """Download a stored replay as .osr (DownloadReplayRequest expects this path)."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                f"{settings.FORLORN_URL}/api/v1/lazer/scores/{score_id}/replay",
                headers={"X-Internal-Token": settings.LAZER_INTERNAL_TOKEN},
            )
    except httpx.HTTPError:
        return JSONResponse(status_code=503, content={"error": "replay storage unavailable"})

    if response.status_code == 404:
        return JSONResponse(status_code=404, content={"error": "replay not found"})
    if response.status_code != 200:
        return JSONResponse(status_code=502, content={"error": "replay unavailable"})

    # verbatim bytes: a .osr is LEE-encoded and must not be re-encoded or
    # truncated, and the client checksums what it gets back.
    return Response(
        content=response.content,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{score_id}.osr"'},
    )
