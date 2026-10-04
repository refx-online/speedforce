from __future__ import annotations

from pydantic import BaseModel
from pydantic import Field

# Mirrors of the lazer client's DTOs in
# osu.Game/Online/API/Requests/Responses/APIUser.cs and
# osu.Game/Users/UserStatistics.cs.
#
# lazer binds JSON by literal [JsonProperty] name -- it does no case conversion.
# Only fields the client actually reads are modelled; the rest of APIUser is
# optional and safe to omit.


class LevelInfo(BaseModel):
    current: int = 0
    progress: int = 0


class Grades(BaseModel):
    ssh: int = 0
    ss: int = 0
    sh: int = 0
    s: int = 0
    a: int = 0


class Variant(BaseModel):
    # lazer's Variant is a bitflag (KeyMod / KeyCoop) used for mania key counts.
    # vanilla-only lazer never sets these meaningfully, but the field is an
    # array on the wire and must not be missing.
    mode: str = "0"
    name: str = ""
    count: int = 0


class UserStatistics(BaseModel):
    """`statistics` -- a single flat object for ONE ruleset, not a dict.

    The doc comment on APIUser.Statistics says "for the requested ruleset": the
    ruleset comes from the request path (/api/v2/me/{mode}) or falls back to the
    user's playmode. Keys are the bancho schema's own names, mapped explicitly
    in the repository layer.
    """

    level: LevelInfo = Field(default_factory=LevelInfo)
    is_ranked: bool = False
    global_rank: int | None = None
    global_rank_percent: float | None = None
    country_rank: int | None = None
    pp: float | None = 0
    ranked_score: int = 0
    hit_accuracy: float = 0.0
    play_count: int = 0
    play_time: int | None = 0
    total_score: int = 0
    total_hits: int = 0
    maximum_combo: int = 0
    replays_watched_by_others: int = 0
    grade_counts: Grades = Field(default_factory=Grades)
    variants: list[Variant] = Field(default_factory=list)
    rank_history: dict = Field(default_factory=dict)


class GlobalRank(BaseModel):
    rank: int | None = None
    ruleset_id: int = 0


class APIUser(BaseModel):
    id: int
    username: str
    country_code: str = "xx"
    avatar_url: str = ""
    cover_url: str = ""
    profile_colour: str | None = None
    cover: dict = Field(default_factory=dict)
    playmode: int = 0
    is_admin: bool = False
    is_supporter: bool = False
    is_bot: bool = False
    is_online: bool = False
    is_active: bool = True
    pm_friends_only: bool = False
    supporter_level: int | None = None
    join_date: str | None = None
    last_visit: str | None = None
    follower_count: int = 0
    following_count: int = 0
    favourite_beatmapset_count: int = 0
    statistics: UserStatistics = Field(default_factory=UserStatistics)
    global_rank: GlobalRank | None = None
    groups: list[dict] = Field(default_factory=list)
    active_tournament_banners: list[dict] = Field(default_factory=list)


class APIMe(APIUser):
    """`GET /api/v2/me/`.

    session_verification_method MUST be null unless speedforce also implements
    POST /api/v2/session/verify -- if it carries a value the client parks in
    APIState.RequiresSecondFactorAuth and login never completes.
    """

    session_verification_method: None = None
    score_processing_notice_url: str = ""
