from __future__ import annotations

from app.models.modes import mods_from_acronyms

# Current lazer mod selection per user, so `/api/v2/me/` can report the stats for
# the ruleset the player is actually on rather than their stored preferred mode.
#
# lazer reports mod state through the metadata hub -- UpdateStatus carries the
# full APIMod list (with settings), UpdateActivity the beatmap/ruleset being
# played. There is no REST equivalent, so this is fed by the hub when it exists.
#
# Until then it is empty, and callers fall back to users.preferred_mode. See
# `resolve_mode` for the precedence rules.

# user_id -> bancho mod bitfield (only mode-affecting bits are tracked; the full
# mod set rides along on the score row at submission time)
_current_mods: dict[int, int] = {}

# user_id -> lazer ruleset_id last seen playing (0-3)
_current_ruleset: dict[int, int] = {}


def set_user_mods(user_id: int, acronyms: list[str] | None) -> None:
    _current_mods[user_id] = mods_from_acronyms(acronyms)


def set_user_ruleset(user_id: int, ruleset_id: int) -> None:
    if ruleset_id in (0, 1, 2, 3):
        _current_ruleset[user_id] = ruleset_id


def clear_user(user_id: int) -> None:
    _current_mods.pop(user_id, None)
    _current_ruleset.pop(user_id, None)


def get_user_mods(user_id: int) -> int | None:
    return _current_mods.get(user_id)


def get_user_ruleset(user_id: int) -> int | None:
    return _current_ruleset.get(user_id)


def resolve_mode(user_id: int, preferred_mode: int) -> int:
    """Effective mode for profile/stat purposes.

    Precedence:
      1. live lazer mod state, when the metadata hub has told us about this user
      2. users.preferred_mode, which the stable side already maintains and which
         may itself be a relax/autopilot id (4-7) from a stable client
      3. vanilla osu

    Case 2 matters because a stable player who prefers relax must see their relax
    stats in lazer too, even though lazer never told us about them.
    """
    from app.models.modes import effective_mode

    ruleset = _current_ruleset.get(user_id)
    mods = _current_mods.get(user_id)

    if ruleset is not None and mods is not None:
        return int(effective_mode(ruleset, mods))

    # No lazer session. preferred_mode is either a vanilla id (0-3) or a
    # stable-side relax/ap id (4-7); effective_mode passes >= 4 straight through,
    # which is what we want here.
    if 0 <= preferred_mode <= 15:
        return int(effective_mode(preferred_mode, 0))

    return 0
