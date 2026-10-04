from __future__ import annotations

from datetime import UTC
from datetime import datetime

from pydantic import BaseModel
from pydantic import Field

# Mirrors of the lazer DTOs in
# osu.Game/Online/API/Requests/Responses/APIBeatmap{,Set}.cs.
#
# The bancho `maps` table is denormalised -- artist/title/creator repeat on every
# difficulty and there is no separate beatmapsets table -- so beatmapset fields
# are folded up from whichever difficulty row we're serialising.
#
# Fields we cannot source from the schema (circle/slider/spinner counts, hit
# length, unicode titles, tags, source, video, storyboard, ratings) are defaulted
# rather than invented. lazer renders them as 0/empty; the ones it actually needs
# in order to play are all present.

# osu-web status ints, which is what lazer expects in `status`.
STATUS_GRAVEYARD = -1
STATUS_PENDING = 0
STATUS_APPROVED = 1
STATUS_QUALIFIED = 2
STATUS_LOVED = 3

# per-mode status_mask codes, from CLAUDE.md: -3->0, -1->1, 0->2, 1->3, 2->4, 3->5, 4->6, 5->7
MASK_CODE_TO_STATUS = {
    0: STATUS_GRAVEYARD,
    1: STATUS_GRAVEYARD,
    2: STATUS_PENDING,
    3: STATUS_APPROVED,
    4: STATUS_QUALIFIED,
    5: STATUS_LOVED,
    6: STATUS_QUALIFIED,
    7: STATUS_LOVED,
}


def status_from_mask(status_mask: int, mode: int) -> int:
    """Per-mode approval. `maps.status` is the legacy single value; lazer needs
    the mode-specific one, which is why CLAUDE.md warns that dropping
    status_mask from a select breaks leaderboards."""
    return MASK_CODE_TO_STATUS.get((status_mask >> (mode * 3)) & 0b111, STATUS_PENDING)


def iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC).isoformat()


class BeatmapSetUser(BaseModel):
    id: int = 0
    username: str = ""
    avatar_url: str = ""


class BeatmapUser(BaseModel):
    id: int = 0
    username: str = ""
    avatar_url: str = ""


class Beatmap(BaseModel):
    id: int
    beatmapset_id: int
    # `status` here is the BEATMAP's per-mode status; the set has its own.
    status: int = STATUS_PENDING
    user_id: int = 0
    mode_int: int = 0
    version: str = ""
    difficulty_rating: float = 0.0
    # bancho `od` is lazer's accuracy, `hp` is drain.
    accuracy: float = 0.0
    drain: float = 0.0
    cs: float = 0.0
    ar: float = 0.0
    total_length: int = 0
    hit_length: int = 0
    count_circles: int = 0
    count_sliders: int = 0
    count_spinners: int = 0
    bpm: float = 0.0
    convert: bool = False
    playcount: int = 0
    passcount: int = 0
    max_combo: int = 0
    checksum: str = ""
    last_updated: str | None = None
    failtimes: dict = Field(default_factory=dict)
    is_scoreable: bool = True
    deleted_at: str | None = None


class BeatmapSet(BaseModel):
    id: int
    artist: str = ""
    # no *_unicode columns in our schema; lazer falls back to the non-unicode
    # value when these are absent, which is correct for ascii titles anyway.
    title: str = ""
    creator: str = ""
    source: str = ""
    tags: str = ""
    # we have no genre/language/tag tables
    genre: str = "Unknown"
    language: str = "Unknown"
    status: int = STATUS_PENDING
    nsfw: bool = False
    has_favourited: bool = False
    favourite_count: int = 0
    play_count: int = 0
    bpm: float = 0.0
    preview_url: str = ""
    video: str = ""
    storyboard: str = ""
    covers: dict = Field(default_factory=dict)
    user_id: int = 0
    beatmaps: list[Beatmap] = Field(default_factory=list)
    ratings: list[dict] = Field(default_factory=list)
    track_id: int = 0
    submitted_date: str | None = None
    ranked_date: str | None = None
    last_updated: str | None = None
    converted_at: str | None = None
    nominated_date: str | None = None
    spotlight: dict = Field(default_factory=dict)
    availability: dict = Field(default_factory=dict)


class BeatmapSetsResponse(BaseModel):
    beatmapsets: list[BeatmapSet] = Field(default_factory=list)
    total: int = 0
    cursor: dict | None = None
