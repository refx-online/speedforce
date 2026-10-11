"""Project MPv2 rooms down to MPv1 so unmodified stable clients can join them.

Why this exists
---------------
A lazer client speaks MPv2 to this hub. A stable client speaks MPv1 to
bakenohana and has never heard of SignalR. Left alone the two never meet: the
room lives here, the stable lobby reads `MatchSession`, and neither can see the
other. So this translates in one direction only -- an MPv2 room is published to
redis, and bakenohana materialises it as a `Match` for stock stable to consume.

This is the mirror of `bakenohana/src/domain/player/lazer_presence.cr`, which
reads lazer presences out of redis to build bancho packets. Same keyspace idea,
one document, no second source of truth: **this hub stays the owner of every
room** and publishes a read-only projection of it.

Stable -> lazer goes back the other way on `INTENT_CHANNEL`, but it carries
*requests*, not state. A stable player joining publishes `{"action": "join"}`
and this hub does the actual mutation, then republishes. So the room still has
exactly one writer; bakenohana is asking, not telling.

Deliberately *not* relayed: `start`, `settings`, `kick`, `lock`, host transfer.
Those are host-authoritative on MPv2 and the host is a lazer player, so there
is no stable session to authorise them -- bakenohana's `p.same?(m.host)`
correctly rejects them before they get here. Relaying them would mean a second
writer on exactly the fields this design refuses to share.

The wire format is whatever bakenohana's `Match` constructor needs. It is not
mirrored from `bMatch` field-for-field -- only the fields a `Match` consumes
are projected, so this stays small and the two sides cannot drift into a shared
schema that neither actually uses.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import redis.asyncio as aioredis

import app.settings as settings
from app.signalr.rooms import IN_GAME, MultiplayerRoom

logger = logging.getLogger(__name__)

# Namespace matches the `signalr:presence:*` keys the two sides already share,
# so it is obvious in a redis keyspace dump that this belongs to the same bridge.
ROOM_CHANNEL = "refx:mpv2:room"

# The return path. bakenohana publishes what a stable player did in a projected
# room; this hub applies it to the room it owns and republishes the projection.
# Deliberately not the same channel: a pub/sub subscriber cannot tell its own
# echo from a peer's message, so sharing one channel would make every stable
# action bounce back and re-enter the handler that produced it.
INTENT_CHANNEL = "refx:mpv2:intent"

# bakenohana's `Match` has 16 slots (slot_writer.cr) and lazer's room has 16
# too, so slots map one-to-one with no translation. Fixed here rather than
# imported so a client-side constant change cannot silently widen the wire.
MAX_SLOTS = 16

# Created on first use, like `MetadataHub.redis`. One client for the life of the
# process rather than one per room.
_redis: aioredis.Redis | None = None


async def _client() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis


def project(room: MultiplayerRoom) -> dict[str, Any]:
    """Render a room as the fields bakenohana's `Match` consumes.

    Only the *current* beatmap is projected. MPv1 has no playlist -- its `Match`
    carries a single `map_id`/`map_md5` -- so a lazer queue of 16 items has to
    become one map on the stable wire. The host picks which: whoever is host
    sets the current item, and that is the item projected.
    """
    item = room.current_item

    # Slot occupants. Only `user_id` and `ready` are projected: MPv1's slot
    # record is an id, and the client resolves the name from its own lobby --
    # where lazer users already appear via `lazer_presence.cr` on the bakenohana
    # side. A name here would be a second copy of a fact the wire never carries.
    slots: list[dict[str, Any]] = []

    for index in range(MAX_SLOTS):
        occupant = next((u for u in room.users.values() if u.slot == index), None)
        slots.append(
            {
                "user_id": occupant.user_id if occupant else 0,
                "ready": bool(occupant and occupant.ready),
            }
        )

    return {
        "room_id": room.room_id,
        "name": room.settings.name,
        "password": room.settings.password or "",
        "host_id": room.host_id,
        # lazer ruleset ids are 0-3 (osu/taiko/ctb/mania) and so are bakenohana's
        # vanilla `Gamemode` ids, so this passes through unchanged. Mapping it is
        # the receiver's job: it knows its own dense 0-15 enum.
        "ruleset_id": item.ruleset_id if item else room.settings.ruleset_id,
        "map_id": item.beatmap_id if item else 0,
        "map_md5": item.beatmap_md5 if item else "",
        "map_name": item.beatmap_name if item else "",
        "mods": room.settings.mods,
        "slots": slots,
        "in_progress": room.state == IN_GAME,
    }


async def publish(room: MultiplayerRoom) -> None:
    """Publish a room's projection. Never raises.

    A redis failure must not take down a lazer session that is otherwise fine --
    the projection is a convenience for stable clients, not the room's source of
    truth. Same reasoning as every `rescue` in `bakenohana`'s
    `presence_bridge.cr`.
    """
    try:
        await (await _client()).publish(ROOM_CHANNEL, json.dumps(project(room)))
    except Exception:  # noqa: BLE001 - stable losing sight of the room is not fatal
        logger.warning("room projection publish failed", exc_info=True)


async def publish_closed(room_id: int) -> None:
    """Tell bakenohana a projected room is gone so it can dispose the `Match`.

    Without this a closed lazer room leaves a ghost in the stable lobby, and
    stable clients get `Failed to join multiplayer room` from a room that can
    never admit them.
    """
    try:
        await (await _client()).publish(ROOM_CHANNEL, json.dumps({"room_id": room_id, "closed": True}))
    except Exception:  # noqa: BLE001 - see above
        logger.warning("room projection close failed", exc_info=True)
