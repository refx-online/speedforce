from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text

from app.state.services import session_factory


@dataclass(slots=True)
class UserRow:
    id: int
    name: str
    priv: int
    country: str
    creation_time: int
    latest_activity: int
    preferred_mode: int


async def find_user_by_name(name: str) -> UserRow | None:
    async with session_factory() as session:
        result = await session.execute(
            text(
                "SELECT id, name, priv, country, creation_time, latest_activity, preferred_mode "
                "FROM users WHERE name = :name LIMIT 1"
            ),
            {"name": name},
        )
        row = result.first()
    if row is None:
        return None
    return UserRow(*row)


async def get_password_hash(user_id: int) -> str | None:
    async with session_factory() as session:
        result = await session.execute(text("SELECT pw_bcrypt FROM users WHERE id = :id"), {"id": user_id})
        row = result.first()
    return row[0] if row else None


async def get_stats(user_id: int, mode: int) -> dict:
    """stats.id IS the user id -- there is no user_id column. (id, mode) is the PK."""
    async with session_factory() as session:
        result = await session.execute(
            text(
                "SELECT tscore, rscore, pp, plays, playtime, acc, max_combo, total_hits, replay_views, "
                "xh_count, x_count, sh_count, s_count, a_count "
                "FROM stats WHERE id = :id AND mode = :mode"
            ),
            {"id": user_id, "mode": mode},
        )
        row = result.first()

    if row is None:
        # No row yet for this mode. Returning zeros keeps the client's user page
        # rendering instead of it treating the user as broken.
        return {
            "tscore": 0,
            "rscore": 0,
            "pp": 0,
            "plays": 0,
            "playtime": 0,
            "acc": 0.0,
            "max_combo": 0,
            "total_hits": 0,
            "replay_views": 0,
            "xh_count": 0,
            "x_count": 0,
            "sh_count": 0,
            "s_count": 0,
            "a_count": 0,
        }

    keys = (
        "tscore",
        "rscore",
        "pp",
        "plays",
        "playtime",
        "acc",
        "max_combo",
        "total_hits",
        "replay_views",
        "xh_count",
        "x_count",
        "sh_count",
        "s_count",
        "a_count",
    )
    return dict(zip(keys, row, strict=True))
