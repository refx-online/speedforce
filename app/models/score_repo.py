from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast

from sqlalchemy import text
from sqlalchemy.engine import CursorResult

from app.state.services import session_factory

# Token lifetime. The client submits immediately after minting; osu-web allows a
# couple of minutes. Kept short because the token authorises a write.
TOKEN_TTL = timedelta(minutes=10)


@dataclass(slots=True)
class ScoreToken:
    id: int
    user_id: int
    beatmap_id: int
    ruleset_id: int
    expires_at: datetime


@dataclass(slots=True)
class MapMeta:
    id: int
    set_id: int
    mode: int
    status_mask: int
    md5: str
    diff: float
    max_combo: int
    total_length: int


def verification_key(user_id: int, beatmap_id: int, checksum: str, ruleset_id: int) -> str:
    """Standalone check the client echoes back implicitly via the token.

    osu-web derives a per-attempt key; we only need something unforgeable enough
    that a token can't be replayed against a different beatmap.
    """
    raw = f"{user_id}:{beatmap_id}:{checksum}:{ruleset_id}".encode()
    return hashlib.sha1(raw).hexdigest()[:32]  # noqa: S324 - identifier, not a security primitive


async def mint_token(user_id: int, beatmap_id: int, ruleset_id: int, checksum: str) -> int:
    now = datetime.now(UTC).replace(tzinfo=None)
    async with session_factory() as session:
        result = await session.execute(
            text(
                "INSERT INTO score_tokens (user_id, beatmap_id, ruleset_id, verification_key, created_at, expires_at) "
                "VALUES (:uid, :bid, :rid, :vkey, :now, :exp)"
            ),
            {
                "uid": user_id,
                "bid": beatmap_id,
                "rid": ruleset_id,
                "vkey": verification_key(user_id, beatmap_id, checksum, ruleset_id),
                "now": now,
                "exp": now + TOKEN_TTL,
            },
        )
        await session.commit()
        # execute() is typed as the base Result; lastrowid only exists on CursorResult
        return int(cast("CursorResult", result).lastrowid or 0)


class TokenError(Exception):
    """Raised when a score token is missing, expired, or doesn't match the submission."""


async def consume_token(token_id: int, user_id: int, beatmap_id: int) -> ScoreToken:
    now = datetime.now(UTC).replace(tzinfo=None)
    async with session_factory() as session:
        result = await session.execute(
            text(
                "SELECT id, user_id, beatmap_id, ruleset_id, expires_at FROM score_tokens "
                "WHERE id = :id AND score_id IS NULL FOR UPDATE"
            ),
            {"id": token_id},
        )
        row = result.first()
        if row is None:
            raise TokenError("invalid token")

        token = ScoreToken(
            id=int(row[0]), user_id=int(row[1]), beatmap_id=int(row[2]), ruleset_id=int(row[3]), expires_at=row[4]
        )

        if token.user_id != user_id:
            raise TokenError("token does not belong to this user")
        if token.beatmap_id != beatmap_id:
            raise TokenError("token was issued for a different beatmap")
        if token.expires_at < now:
            raise TokenError("token expired")

        return token


async def mark_token_used(token_id: int, score_id: int) -> None:
    """Bind the token to the score it authorised.

    Called only AFTER the write succeeds: score_id is what makes the token
    single-use (consume_token only matches rows where it is still NULL), and it
    is bigint unsigned so there is no room for a sentinel. Leaving it NULL on
    failure means the client can retry the same submission.
    """
    async with session_factory() as session:
        await session.execute(
            text("UPDATE score_tokens SET score_id = :sid WHERE id = :id"),
            {"id": token_id, "sid": score_id},
        )
        await session.commit()


async def get_map(beatmap_id: int) -> MapMeta | None:
    async with session_factory() as session:
        result = await session.execute(
            text(
                "SELECT id, set_id, mode, status_mask, md5, diff, max_combo, total_length "
                "FROM maps WHERE id = :id LIMIT 1"
            ),
            {"id": beatmap_id},
        )
        row = result.first()
    if row is None:
        return None
    return MapMeta(*row)
