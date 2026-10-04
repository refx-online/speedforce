from __future__ import annotations

from enum import IntEnum

# Port of forlorn/forlorn/src/constants/mode.rs (GameMode::from_params) and the
# mod bits from forlorn/forlorn/src/constants/mods.rs. Keep in lockstep with
# those files -- the dense ids are a wire contract shared with forlorn,
# bakenohana, recalculate and mist.
#
# lazer only ever sends ruleset_id 0-3 (osu/taiko/fruits/mania). Relax and
# autopilot arrive as MODS (ModRelax.Acronym == "RX"), never as a separate
# ruleset, so the effective mode has to be derived from (ruleset, mods). That
# derivation is the whole point of this module.


class Mods(IntEnum):
    """Only the bits that affect mode selection. Not a complete mod list."""

    TOUCHSCREEN = 1 << 2
    RELAX = 1 << 7
    AUTOPILOT = 1 << 13


class GameMode(IntEnum):
    VN_OSU = 0
    VN_TAIKO = 1
    VN_CATCH = 2
    VN_MANIA = 3

    RX_OSU = 4
    RX_TAIKO = 5
    RX_CATCH = 6

    AP_OSU = 7

    CHEAT_OSU = 8
    CHEAT_TAIKO = 9
    CHEAT_CATCH = 10
    CHEAT_MANIA = 11

    CHEAT_RX_OSU = 12
    CHEAT_RX_TAIKO = 13
    CHEAT_RX_CATCH = 14

    CHEAT_AP_OSU = 15


# lazer ruleset_id -> vanilla mode id. lazer's catch ruleset is "fruits" but is
# still ruleset 2.
LAZER_RULESET_TO_MODE = {
    0: GameMode.VN_OSU,
    1: GameMode.VN_TAIKO,
    2: GameMode.VN_CATCH,
    3: GameMode.VN_MANIA,
}


def effective_mode(ruleset_id: int, mods: int = 0) -> int:
    """Map (lazer ruleset, lazer mods) to a dense bancho mode id.

    Faithful to forlorn's `GameMode::from_params`. The lazer-relevant path is the
    tail of that function: lazer never sends ids >= 4, so only the mod-driven
    branches can fire, but the legacy folds are kept so a hand-rolled request
    can't desync the two implementations.

    Mods are the BANCHO int bitfield, not lazer's APIMod acronym list -- callers
    convert. See `mods_from_acronyms`.
    """
    # NB: don't coerce `mods` to the Mods enum -- it's an arbitrary bitfield and
    # will carry bits not listed above (HD, DT, ...), which IntEnum rejects.
    # Plain integer bit tests are all that's needed.
    mod_bits = mods

    # legacy folds, kept for parity with forlorn even though lazer can't send these
    if ruleset_id == 16:
        ruleset_id = 8
    if ruleset_id == 20:  # TD as its own legacy mode
        return GameMode.VN_OSU

    if ruleset_id >= 4:
        # already a dense id from a non-lazer client; pass it through, mirroring
        # forlorn's clamping of unknown values to VN_OSU
        try:
            return GameMode(ruleset_id)
        except ValueError:
            return GameMode.VN_OSU

    base = LAZER_RULESET_TO_MODE.get(ruleset_id, GameMode.VN_OSU)

    if mod_bits is not None:
        # AP is osu-only, and RX excludes mania (there is no relax mania id --
        # 12/13/14 are cheat-rx). lazer enforces this client-side too.
        if mod_bits & Mods.AUTOPILOT and base == GameMode.VN_OSU:
            return GameMode.AP_OSU
        if mod_bits & Mods.RELAX and base != GameMode.VN_MANIA:
            return GameMode(base + int(GameMode.RX_OSU))

    # TD deliberately does NOT change the mode -- the mod bit stays on the score
    # and pp is computed with it. forlorn does the same.

    return base


# lazer's APIMod acronyms -> bancho mod bits. Only the mods that can move the
# effective mode are needed here; the rest ride along on the score row.
ACRONYM_TO_BIT = {
    "TD": int(Mods.TOUCHSCREEN),
    "RX": int(Mods.RELAX),
    "AP": int(Mods.AUTOPILOT),
}


def mods_from_acronyms(acronyms: list[str] | None) -> int:
    bits = 0
    for acronym in acronyms or []:
        bits |= ACRONYM_TO_BIT.get(acronym.upper(), 0)
    return bits
