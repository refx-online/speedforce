from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel
from pydantic import Field

# The payload lazer PUTs to /api/v2/beatmaps/{id}/solo/scores/{token}.
# Wire format verified against
# osu.Game/Online/API/Requests/Responses/SoloScoreInfo.cs and lazer's own
# osu.Game.Tests/Online/TestSoloScoreInfoJsonSerialization.cs, which asserts:
#   - statistics keys are snake_case strings ("large_tick_hit"), not ints
#   - rank is a string ("S"), not a number


class APIMod(BaseModel):
    # both carry explicit [JsonProperty] names in APIMod.cs
    acronym: str = ""
    settings: dict[str, Any] = Field(default_factory=dict)


class SoloScoreInfo(BaseModel):
    beatmap_id: int = 0
    ruleset_id: int = 0
    build_id: int | None = None
    passed: bool = False
    total_score: int = 0
    total_score_without_mods: int = 0
    accuracy: float = 0.0
    # NOTE: deliberately NOT trusted. The user is taken from the score token, so
    # a client cannot submit a score as somebody else.
    user_id: int = 0
    max_combo: int = 0
    rank: str = "D"
    started_at: datetime | None = None
    ended_at: datetime | None = None
    mods: list[APIMod] = Field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    deleted_at: datetime | None = None
    # snake_case HitResult keys. Stored verbatim in lazer_scores.statistics_json.
    statistics: dict[str, int] = Field(default_factory=dict)
    maximum_statistics: dict[str, int] = Field(default_factory=dict)
    legacy_total_score: int | None = None
    legacy_score_id: int | None = None
    pauses: dict[str, Any] | None = None


# Full acronym -> bit table from forlorn/forlorn/src/constants/mods.rs. lazer
# sends acronyms; the scores table stores the int bitfield.
MOD_ACRONYM_BITS: dict[str, int] = {
    "NF": 1 << 0,
    "EZ": 1 << 1,
    "TD": 1 << 2,
    "HD": 1 << 3,
    "HR": 1 << 4,
    "SD": 1 << 5,
    "DT": 1 << 6,
    "RX": 1 << 7,
    "HT": 1 << 8,
    "NC": 1 << 9,
    "FL": 1 << 10,
    "AT": 1 << 11,  # lazer's autoplay
    "SO": 1 << 12,
    "AP": 1 << 13,
    "PF": 1 << 14,
    "K4": 1 << 15,
    "K5": 1 << 16,
    "K6": 1 << 17,
    "K7": 1 << 18,
    "K8": 1 << 19,
    "FI": 1 << 20,
    "RN": 1 << 21,
    "CN": 1 << 22,
    "TP": 1 << 23,
    "K9": 1 << 24,
    "CO": 1 << 25,
    "K1": 1 << 26,
    "K3": 1 << 27,
    "K2": 1 << 28,
    "V2": 1 << 29,
    "MR": 1 << 30,
}

# lazer acronyms that have no bancho equivalent; ignored rather than guessed at.
UNKNOWN_ACRONYMS_IGNORED = {"V2", "MR"}

RANK_NAMES = {"D", "C", "B", "A", "S", "SH", "X", "XH"}


def mods_to_bits(mods: list[APIMod]) -> int:
    bits = 0
    for mod in mods:
        bits |= MOD_ACRONYM_BITS.get(mod.acronym.upper(), 0)
    return bits


# statistics -> the six legacy counters on the `scores` row.
# lazer emits far more granular hit types than the legacy six; these roll them up
# the way osu-web does (slider ticks/tails are 300-value hits).
_COUNT_ROLLUP = {
    "n300": ("great", "slider_tail_hit", "large_tick_hit", "small_tick_hit"),
    "n100": ("ok", "good"),
    "n50": ("meh",),
    "nmiss": ("miss", "small_tick_miss", "large_tick_miss", "ignore_miss", "combo_break"),
    "ngeki": ("perfect",),
    # NOTE: nkatu has no lazer equivalent we can justify -- it is a legacy taiko
    # counter. Left at 0 rather than mapped to a guess.
    "nkatu": (),
}


def hit_counts(statistics: dict[str, int]) -> dict[str, int]:
    return {col: sum(statistics.get(key, 0) for key in keys) for col, keys in _COUNT_ROLLUP.items()}
