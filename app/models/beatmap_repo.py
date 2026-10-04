from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field

from sqlalchemy import text

from app.state.services import session_factory

# Columns selected for a beatmap row. status_mask is required: `status` is the
# legacy single value and lazer needs per-mode approval (see beatmap.py).
_MAP_COLUMNS = """
    id, set_id, mode, status, status_mask, md5, artist, title, version, creator,
    filename, last_update, total_length, hit_length, count_normal, count_slider, count_spinner,
    max_combo, plays, passes, bpm, cs, ar, od, hp, diff
"""


@dataclass(slots=True)
class MapRow:
    id: int
    set_id: int
    mode: int
    status: int
    status_mask: int
    md5: str
    artist: str
    title: str
    version: str
    creator: str
    filename: str
    last_update: object
    total_length: int
    hit_length: int
    count_normal: int
    count_slider: int
    count_spinner: int
    max_combo: int
    plays: int
    passes: int
    bpm: float
    cs: float
    ar: float
    od: float
    hp: float
    diff: float


@dataclass(slots=True)
class SearchFilters:
    query: str | None = None
    mode: int | None = None
    category: str = "any"
    sort: str = "title_asc"
    genre: int | None = None
    language: int | None = None
    played: str | None = None
    nsfw: bool = False
    # cursor: lazer sends the last sort value from the previous page
    cursor_sort: str | None = None
    cursor_id: int | None = None


@dataclass(slots=True)
class SearchResult:
    rows: list[MapRow] = field(default_factory=list)
    total: int = 0


# lazer's `s` category -> the per-mode status codes it maps to.
# rank of preference is preserved: any = no filter.
_CATEGORY_STATUS = {
    "ranked": (3,),  # approved
    "qualified": (4,),
    "loved": (5,),
    "pending": (2,),
    "graveyard": (0, 1),
    "unranked": (2, 4),
}

# `sort` is {criteria}_{asc|desc} on the python side.
_SORT_COLUMNS = {
    "title": "m.title",
    "artist": "m.artist",
    "creator": "m.creator",
    "difficulty": "m.diff",
    "plays": "m.plays",
    "bpm": "m.bpm",
    "length": "m.total_length",
    "ranked": "m.ranked_date",
    "rating": "m.diff",
    "date": "m.last_update",
}

PAGE_SIZE = 25


def _parse_sort(sort: str) -> tuple[str, bool]:
    criteria, _, direction = sort.partition("_")
    column = _SORT_COLUMNS.get(criteria, "m.title")
    desc = direction.lower() == "desc"
    return column, desc


async def search_beatmapsets(f: SearchFilters) -> SearchResult:
    """One query. lazer's search is beatmapset-oriented but our schema stores
    set metadata denormalised onto each difficulty, so we search `maps` and fold
    the rows into sets in Python."""
    where: list[str] = []
    params: dict[str, object] = {}

    if f.mode is not None:
        where.append("m.mode = :mode")
        params["mode"] = f.mode

    if f.query:
        # osu-web matches artist/title/creator/diffname loosely
        like = f"%{f.query}%"
        where.append("(m.title LIKE :q OR m.artist LIKE :q OR m.creator LIKE :q OR m.version LIKE :q)")
        params["q"] = like

    statuses = _CATEGORY_STATUS.get(f.category)
    if statuses:
        # per-mode approval lives in the 3-bit-per-mode status_mask
        clauses = []
        mode = f.mode if f.mode is not None else 0
        for code in statuses:
            clauses.append(f"((m.status_mask >> ({mode} * 3)) & 7) = :code_{code}")
            params[f"code_{code}"] = code
        where.append("(" + " OR ".join(clauses) + ")")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sort_col, desc = _parse_sort(f.sort)
    order = "DESC" if desc else "ASC"

    # keyset pagination: continue after the last row of the previous page
    cursor_sql = ""
    if f.cursor_sort is not None and f.cursor_id is not None:
        cmp = "<" if desc else ">"
        cursor_sql = f" AND ({sort_col} {cmp} :cursor_sort OR ({sort_col} = :cursor_sort AND m.id > :cursor_id))"
        params["cursor_sort"] = f.cursor_sort
        params["cursor_id"] = f.cursor_id

    count_sql = text(f"SELECT COUNT(DISTINCT m.set_id) FROM maps m {where_sql}{cursor_sql}")
    data_sql = text(
        f"SELECT {_MAP_COLUMNS} FROM maps m {where_sql}{cursor_sql} "
        f"ORDER BY {sort_col} {order}, m.id ASC LIMIT :limit"
    )
    params["limit"] = PAGE_SIZE * 3  # over-fetch: sets have multiple difficulties

    async with session_factory() as session:
        total = (await session.execute(count_sql, params)).scalar() or 0
        result = await session.execute(data_sql, params)
        rows = [MapRow(*r) for r in result.all()]

    return SearchResult(rows=rows, total=int(total))


async def get_beatmapset_rows(set_id: int) -> list[MapRow]:
    """All difficulties for a set. Needed because search pages are built from a
    partial row set and must be completed before returning."""
    sql = text(f"SELECT {_MAP_COLUMNS} FROM maps m WHERE m.set_id = :set_id ORDER BY m.mode, m.diff")
    async with session_factory() as session:
        result = await session.execute(sql, {"set_id": set_id})
        return [MapRow(*r) for r in result.all()]


async def get_beatmap_rows_by_ids(ids: list[int]) -> list[MapRow]:
    if not ids:
        return []
    placeholders = ",".join(f":id{i}" for i in range(len(ids)))
    params = {f"id{i}": v for i, v in enumerate(ids)}
    sql = text(f"SELECT {_MAP_COLUMNS} FROM maps m WHERE m.id IN ({placeholders})")
    async with session_factory() as session:
        result = await session.execute(sql, params)
        return [MapRow(*r) for r in result.all()]


async def get_set_ids_for_beatmap_ids(ids: list[int]) -> dict[int, int]:
    """beatmap id -> set id, for /beatmaps/lookup?beatmap_id="""
    if not ids:
        return {}
    placeholders = ",".join(f":id{i}" for i in range(len(ids)))
    params = {f"id{i}": v for i, v in enumerate(ids)}
    sql = text(f"SELECT id, set_id FROM maps WHERE id IN ({placeholders})")
    async with session_factory() as session:
        result = await session.execute(sql, params)
        return {int(r[0]): int(r[1]) for r in result.all()}
