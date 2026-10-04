from __future__ import annotations

import math
from datetime import UTC
from datetime import datetime

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request

from app.auth.tokens import decode_token
from app.models.api_user import APIMe
from app.models.api_user import GlobalRank
from app.models.api_user import Grades
from app.models.api_user import LevelInfo
from app.models.api_user import UserStatistics
from app.models.repository import UserRow
from app.models.repository import find_user_by_name
from app.models.repository import get_stats
from app.state.mods import resolve_mode

router = APIRouter(prefix="/api/v2", tags=["osu! API v2"])

# bancho priv bitmask -- src/shared/constants/priv.cr
PRIV_SUPPORTER = 1 << 4
PRIV_MODERATOR = 1 << 12
PRIV_ADMINISTRATOR = 1 << 13
PRIV_DEVELOPER = 1 << 14

# lazer ruleset short names, in mode order. statistics is per-ruleset but the
# response shape is flat, so the path segment selects which mode's row to read.
RULESET_SHORT_NAMES = {0: "osu", 1: "taiko", 2: "fruits", 3: "mania"}


def _iso(timestamp: int) -> str | None:
    if not timestamp:
        return None
    return datetime.fromtimestamp(timestamp, tz=UTC).isoformat()


def _level_for(pp: float) -> LevelInfo:
    # lazer's own curve: level = floor(sqrt(pp) / 1.25) roughly. Not worth
    # matching exactly -- the client only renders it.
    if pp <= 0:
        return LevelInfo()
    return LevelInfo(current=int(math.sqrt(pp) / 1.25) + 1, progress=0)


async def require_user(request: Request) -> UserRow:
    """Bearer auth.

    Raises HTTPException rather than returning a Response: FastAPI does NOT
    short-circuit on a Response returned from a dependency (it just passes the
    value through to the endpoint), which previously turned every unauthenticated
    request into a 500. Repeated 5xx puts the client into APIState.Failing after
    three tries and it stops retrying, so this must be a clean 401.
    """
    header = request.headers.get("Authorization", "")
    scheme, _, token = header.partition(" ")

    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=401, detail="unauthorized")

    claims = decode_token(token)
    if not claims or claims.get("typ") == "refresh":
        raise HTTPException(status_code=401, detail="unauthorized")

    user = await find_user_by_name(str(claims.get("username") or ""))
    if not user or user.id != int(claims["osu_user_id"]):
        raise HTTPException(status_code=401, detail="unauthorized")

    return user


# NOTE the trailing slash: GetMeRequest.Target is `me/{Ruleset?.ShortName}`, and
# lazer sends it with no ruleset, so the literal request path is "/api/v2/me/".
# Route both so a proxy that strips or keeps the slash both work.
@router.get("/me/")
@router.get("/me", include_in_schema=False)
async def get_me(user: UserRow = Depends(require_user)) -> APIMe:
    # lazer sends ruleset 0-3 plus RX/AP as mods, so the effective mode is
    # derived from live mod state when we have it, else preferred_mode.
    mode = resolve_mode(user.id, user.preferred_mode)
    stats = await get_stats(user.id, mode)

    pp = float(stats["pp"] or 0)
    accuracy = float(stats["acc"] or 0.0)

    is_supporter = bool(user.priv & PRIV_SUPPORTER) or user.priv & PRIV_ADMINISTRATOR != 0
    is_admin = bool(user.priv & (PRIV_MODERATOR | PRIV_ADMINISTRATOR | PRIV_DEVELOPER))

    return APIMe(
        id=user.id,
        username=user.name,
        country_code=user.country or "xx",
        avatar_url=f"https://a.041095.xyz/{user.id}",
        # left empty: covers 404 on our CDN *and* on b.ppy.sh, so any URL
        # here would just be a broken image. Needs assets-service work.
        cover_url="",
        playmode=mode,
        is_admin=is_admin,
        is_supporter=is_supporter,
        is_bot=False,
        is_online=True,
        join_date=_iso(user.creation_time),
        last_visit=_iso(user.latest_activity),
        supporter_level=1 if is_supporter else None,
        statistics=UserStatistics(
            level=_level_for(pp),
            is_ranked=pp > 0,
            pp=pp,
            ranked_score=int(stats["rscore"] or 0),
            hit_accuracy=accuracy,
            play_count=int(stats["plays"] or 0),
            play_time=int(stats["playtime"] or 0),
            total_score=int(stats["tscore"] or 0),
            total_hits=int(stats["total_hits"] or 0),
            maximum_combo=int(stats["max_combo"] or 0),
            replays_watched_by_others=int(stats["replay_views"] or 0),
            grade_counts=Grades(
                ssh=int(stats["xh_count"] or 0),
                ss=int(stats["x_count"] or 0),
                sh=int(stats["sh_count"] or 0),
                s=int(stats["s_count"] or 0),
                a=int(stats["a_count"] or 0),
            ),
        ),
        global_rank=GlobalRank(rank=None, ruleset_id=mode),
        # keep null: setting this parks the client in
        # APIState.RequiresSecondFactorAuth and login never completes.
        session_verification_method=None,
        score_processing_notice_url="",
    )
