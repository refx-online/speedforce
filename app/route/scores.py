from __future__ import annotations

import httpx
from fastapi import APIRouter
from fastapi import Depends
from fastapi import Form
from fastapi import Request
from fastapi.responses import JSONResponse

import app.settings as settings
from app.models.modes import effective_mode
from app.models.score_repo import MapMeta
from app.models.score_repo import TokenError
from app.models.score_repo import consume_token
from app.models.score_repo import get_map
from app.models.score_repo import mark_token_used
from app.models.score_repo import mint_token
from app.models.solo_score import RANK_NAMES
from app.models.solo_score import SoloScoreInfo
from app.models.solo_score import hit_counts
from app.models.solo_score import mods_to_bits
from app.route.me import require_user

router = APIRouter(prefix="/api/v2", tags=["osu! API v2"])

# The score WRITE itself (personal-best rejection, placement, xp, pp, leaderboard
# ZADD, first-place webhooks, redis events) belongs to forlorn's
# usecases::score -- stable already goes through it, and duplicating it here would
# let the two clients' stats drift apart. speedforce validates and hands off.
FORLORN_SUBMIT_URL = f"{settings.FORLORN_URL}/api/v1/lazer/scores"


def _error(message: str, status: int = 400) -> JSONResponse:
    # The client string-matches a few of these (SubmittingPlayer special-cases
    # "missing token header", "invalid token", "invalid or missing beatmap_hash"),
    # so keep the wording stable.
    return JSONResponse(status_code=status, content={"error": message})


@router.post("/beatmaps/{beatmap_id}/solo/scores", response_model=None)
async def create_score_token(
    beatmap_id: int,
    user=Depends(require_user),
    version_hash: str = Form(...),
    beatmap_hash: str = Form(...),
    ruleset_id: int = Form(...),
) -> JSONResponse:
    meta = await get_map(beatmap_id)
    if meta is None:
        return _error("invalid or missing beatmap_hash", 404)

    # lazer always sends ruleset 0-3; anything else isn't a vanilla request.
    if ruleset_id not in (0, 1, 2, 3):
        return _error("unsupported ruleset")

    token_id = await mint_token(int(user.id), beatmap_id, ruleset_id, beatmap_hash)
    # shape is {"id": <token>} -- the client reads .id and PUTs to .../scores/{id}
    return JSONResponse(content={"id": token_id})


@router.put("/beatmaps/{beatmap_id}/solo/scores/{token_id}", response_model=None)
async def submit_score(
    beatmap_id: int,
    token_id: int,
    payload: SoloScoreInfo,
    user=Depends(require_user),
) -> JSONResponse:
    try:
        token = await consume_token(token_id, int(user.id), beatmap_id)
    except TokenError as e:
        return _error(str(e), 401)

    meta = await get_map(beatmap_id)
    if meta is None:
        return _error("invalid or missing beatmap_hash", 404)

    # guard the obvious mismatch: lazer echoes the ruleset it played
    if payload.ruleset_id not in (0, 1, 2, 3):
        return _error("unsupported ruleset")

    mods = mods_to_bits(payload.mods)
    mode = effective_mode(payload.ruleset_id, mods)
    counts = hit_counts(payload.statistics)

    if payload.rank not in RANK_NAMES:
        return _error("invalid rank")

    # hand the parsed submission to forlorn, which owns the write path
    forward = {
        "user_id": int(user.id),
        "beatmap_id": beatmap_id,
        "map_md5": meta.md5,
        "mode": mode,
        "mods": mods,
        "passed": payload.passed,
        "rank": payload.rank,
        "total_score": payload.total_score,
        "accuracy": payload.accuracy,
        "max_combo": payload.max_combo,
        "build_id": payload.build_id,
        "started_at": payload.started_at.isoformat() if payload.started_at else None,
        "ended_at": payload.ended_at.isoformat() if payload.ended_at else None,
        "counts": counts,
        "statistics": payload.statistics,
        "mods_list": [{"acronym": m.acronym, "settings": m.settings} for m in payload.mods],
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                FORLORN_SUBMIT_URL,
                json=forward,
                headers={"X-Internal-Token": settings.LAZER_INTERNAL_TOKEN},
            )
    except httpx.HTTPError:
        return _error("score processing unavailable", 503)

    if response.status_code >= 400:
        return _error("score rejected", response.status_code)

    # the client reads .ID and .Position off the response
    body = response.json() if response.content else {}

    # only now is the token spent; a failed hand-off above leaves it retryable
    await mark_token_used(token_id, int(body.get("score_id", 0)))

    return JSONResponse(
        content={
            "id": body.get("score_id", 0),
            "position": body.get("position", 0),
            "passed": payload.passed,
            "ranked": body.get("ranked", False),
            "score_id": body.get("score_id", 0),
        }
    )
