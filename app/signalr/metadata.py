"""The metadata hub: online presence for lazer clients.

This is the hub ``AuthSession`` must reach for lazer to leave the login screen
(``OnlineMetadataClient.cs:51`` takes ``MetadataUrl``). Spectator and multiplayer
are separate hubs and are not implemented.

Design notes worth knowing before changing anything here:

* **Activity and status are treated as opaque JSON.** They are MessagePack
  ``[Union]`` types (``UserActivity`` has 14 derived shapes) resolved client-side
  by ``SignalRUnionWorkaroundResolver``. Since we only ever *relay* them between
  clients and echo a user's own back, modelling them would buy nothing and risk
  mangling a shape we guessed wrong. We relay verbatim.

* **Presence lives in redis, not in this process**, so it survives reconnects and
  works across instances. Keys are under ``signalr:`` so they cannot collide with
  the ``bancho:*`` keys bakenohana owns.

* **Stable presence comes from bakenohana.** It owns the only record of whether a
  stable player is online -- its in-memory ``PlayerSession`` registry, which
  nothing else can read. ``PresenceBridge`` (bakenohana
  ``src/domain/match/presence_bridge.cr``) therefore writes the same
  ``signalr:presence:*`` keys and publishes ``signalr:presence_changed`` on every
  change; ``StablePresenceListener`` below subscribes and forwards to watchers.
  That is what makes a stable user visible to a lazer client and vice versa.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import Any

import redis.asyncio as aioredis
from redis.exceptions import RedisError

import app.settings as settings
from app.signalr.host import mount
from app.signalr.protocol import HubConnection

PRESENCE_KEY = "signalr:presence:{user_id}"
QUEUE_KEY = "signalr:presence_queue_id"

# bakenohana publishes the changed user id here whenever a stable presence changes
STABLE_CHANNEL = "signalr:presence_changed"

_LOG = logging.getLogger(__name__)

# A presence entry expires this long after its last refresh, so a client that
# vanishes without a clean disconnect stops reading as online.
PRESENCE_TTL_SECONDS = 90


class MetadataHub:
    name = "metadata"

    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None
        # connection_id -> True while it is watching everyone. This is a *flag*,
        # not a set of user ids: BeginWatchingUserPresence takes no arguments and
        # means "send me everyone's presence from now on", so a user who comes
        # online after you subscribed must still reach you. Snapshotting who was
        # online at subscribe time (an earlier bug here) silently dropped them.
        self._watching: set[str] = set()
        # every live connection in this process, for watcher fan-out
        self._live: set[HubConnection] = set()

    async def redis(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        return self._redis

    def _watches(self, connection: HubConnection, user_id: int) -> bool:
        return connection.connection_id in self._watching

    # ---------------------------------------------------------------- lifecycle

    async def on_connect(self, connection: HubConnection) -> None:
        self._live.add(connection)

        r = await self.redis()
        # ChoosingBeatmap is UserActivity's idle-ish default (union 11)
        await r.set(
            PRESENCE_KEY.format(user_id=connection.user_id),
            json.dumps({"type": "ChoosingBeatmap"}),
            ex=PRESENCE_TTL_SECONDS,
        )
        await self.broadcast(connection.user_id)

    async def on_disconnect(self, connection: HubConnection) -> None:
        self._live.discard(connection)
        self._watching.discard(connection.connection_id)

        # only clear presence once the user's *last* connection is gone
        if any(c.user_id == connection.user_id for c in self._live):
            return

        r = await self.redis()
        with contextlib.suppress(RedisError):
            await r.delete(PRESENCE_KEY.format(user_id=connection.user_id))
        await self.broadcast(connection.user_id)

    # ------------------------------------------------------------- hub methods

    async def invoke(self, connection: HubConnection, target: str, args: list[Any]) -> Any:
        if target in ("UpdateActivity", "UpdateStatus"):
            return await self._update_presence(connection, args[0] if args else None)

        if target == "BeginWatchingUserPresence":
            return await self._begin_watching(connection)

        if target == "EndWatchingUserPresence":
            self._watching.discard(connection.connection_id)
            return None

        if target == "GetChangesSince":
            return await self._get_changes_since(int(args[0]) if args else 0)

        if target == "RefreshFriends":
            # no friendship graph in the schema; make the client's refresh a no-op
            # rather than an error
            return None

        if target in ("BeginWatchingMultiplayerRoom", "EndWatchingMultiplayerRoom"):
            # the multiplayer hub is not implemented. Answer successfully so the
            # client does not treat the subscription as a hard failure.
            return []

        raise NotImplementedError(target)

    # ------------------------------------------------------------------ pieces

    async def _update_presence(self, connection: HubConnection, payload: Any) -> None:
        """Store a client's activity/status verbatim, then notify watchers.

        ``null`` is meaningful: it is how the client says it has gone idle, and we
        drop the entry so the user stops reading as online.
        """
        r = await self.redis()
        key = PRESENCE_KEY.format(user_id=connection.user_id)

        if payload is None:
            await r.delete(key)
        else:
            body = payload if isinstance(payload, str) else json.dumps(payload)
            await r.set(key, body, ex=PRESENCE_TTL_SECONDS)

        # advance the change cursor the client resumes from after a reconnect
        await r.incr(QUEUE_KEY)

        await self.broadcast(connection.user_id)
        return None

    async def _begin_watching(self, connection: HubConnection) -> None:
        """Subscribe to everyone else's presence and send a snapshot.

        ``UserPresenceUpdated`` is registered client-side as (userId, presence),
        so a batch is one message per user. Sending the snapshot here means a
        freshly connected client is not blank until someone changes state.
        """
        r = await self.redis()

        # flag first, then send the snapshot: if a presence lands between the read
        # and the flag we would still push it, and the snapshot is just a duplicate
        # the client overwrites with identical data
        self._watching.add(connection.connection_id)

        online: set[int] = set()
        for key in await r.keys(f"{PRESENCE_KEY.format(user_id='')}*"):
            with contextlib.suppress(ValueError, IndexError):
                online.add(int(_text(key).rsplit(":", 1)[1]))

        for user_id in sorted(online - {connection.user_id}):
            stored = _loads(await r.get(PRESENCE_KEY.format(user_id=user_id)))
            presence = presence_to_client(stored)
            if presence is not None:
                await connection.send("UserPresenceUpdated", user_id, presence)

        return None

    async def _get_changes_since(self, queue_id: int) -> dict[str, Any]:
        """Queue cursor for reconnect catch-up.

        Field names must match ``osu.Game/Online/Metadata/BeatmapUpdates.cs``:
        ``BeatmapSetIDs`` and ``LastProcessedQueueID``. Returning anything else
        fails deserialisation during connect, which leaves the client stuck on
        "signing in" forever -- no further hub calls ever happen.

        A full deployment keeps an append-only change log keyed by this id. Here
        the cursor only advances: enough for the reconnect path to complete
        without replaying changes already applied.
        """
        r = await self.redis()
        return {"beatmapSetIDs": [], "lastProcessedQueueID": int(await r.get(QUEUE_KEY) or 0)}

    async def broadcast(self, user_id: int) -> None:
        """Push one user's presence to every connection watching them."""
        r = await self.redis()
        stored = _loads(await r.get(PRESENCE_KEY.format(user_id=user_id)))
        payload = presence_to_client(stored)

        if payload is None:
            return

        for conn in list(self._live):
            if conn is not None and conn.user_id != user_id and self._watches(conn, user_id):
                await conn.send("UserPresenceUpdated", user_id, payload)


def _text(raw: Any) -> str:
    """Redis responses are str or bytes depending on decode_responses."""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    return str(raw)


def _loads(raw: Any) -> Any:
    if raw is None:
        return None
    body = _text(raw)
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return body


# ---------------------------------------------------------------------------
# Presence -> client wire shape.
#
# Stored presence is JSON (`{"Activity": {...}, "Status": ...}`) because that is
# the documented shared contract with bakenohana's presence bridge -- do not
# change the stored form without changing that side too.
#
# What the *client* binds is different, and is a `UserPresence`:
#
#     [MessagePackObject] struct UserPresence { [Key(0)] Activity; [Key(1)] Status; }
#
# so on the wire it is an **integer-keyed** map, and `Activity` is a `[Union]`
# encoded by `UnionFormatter` as a two-element array `[unionKey, payload]`.
# Sending the stored JSON shape verbatim therefore decodes to nothing on the
# client, which is why the players list stayed empty.
#
# Cross-checked against a working implementation:
# `GooGuTeam/g0v0.Server.Realtime` `Objects/States/PlayerState.cs:62`
# (`new UserPresence { Activity = ..., Status = ... }`) and
# `Hubs/MetadataHub.cs:89` -- it hands the real DTO to SignalR, so the integer
# keys come from the client's own attributes.
# ---------------------------------------------------------------------------

# UserActivity union keys, from `[Union(N, ...)]` in osu.Game/Users/UserActivity.cs
_UNION_KEYS = {
    "ChoosingBeatmap": 11,
    "InSoloGame": 12,
    "WatchingReplay": 13,
    "SpectatingUser": 14,
    "SearchingForLobby": 21,
    "InLobby": 22,
    "InMultiplayerGame": 23,
    "SpectatingMultiplayerGame": 24,
    "InPlaylistGame": 31,
    "EditingBeatmap": 41,
    "ModdingBeatmap": 42,
    "TestingBeatmap": 43,
}

# `InGame` subclasses share one [Key] layout (UserActivity.cs:67-78):
# 0 BeatmapID, 1 BeatmapDisplayTitle, 2 RulesetID, 3 RulesetPlayingVerb
_IN_GAME_TYPES = {"InSoloGame", "InMultiplayerGame", "SpectatingMultiplayerGame", "InPlaylistGame"}

# `UserStatus` ordinals (osu.Game/Users/UserStatus.cs): Offline=0,
# DoNotDisturb=1, Online=2. JSON stores the name; the wire wants the ordinal.
_STATUS_ORDINALS = {"Offline": 0, "DoNotDisturb": 1, "Online": 2}


def _activity_payload(kind: str, activity: dict[str, Any]) -> list[Any]:
    """Build the `[key, payload]` array a `UnionFormatter` writes."""
    key = _UNION_KEYS.get(kind)
    if key is None:
        # Unknown activity: relay as an empty ChoosingBeatmap rather than drop the
        # user from the list entirely. Losing the row is worse than a wrong verb.
        return [_UNION_KEYS["ChoosingBeatmap"], {}]

    if kind in _IN_GAME_TYPES:
        return [
            key,
            {0: activity.get("BeatmapID"), 1: activity.get("BeatmapDisplayTitle"), 2: activity.get("RulesetID")},
        ]

    # Every other declared activity carries no [Key] members, so an empty map.
    return [key, {}]


def presence_to_client(stored: Any) -> dict[int, Any] | None:
    """Translate a stored presence document into the client's `UserPresence`.

    Returns None when there is nothing to show, which is the client's signal for
    "this user is not online".
    """
    if not isinstance(stored, dict):
        return None

    activity = stored.get("Activity")
    status = stored.get("Status")

    # Offline / unknown status means offline, as in the reference implementation
    # (PlayerState.cs:63 returns null for Offline).
    ordinal = _STATUS_ORDINALS.get(str(status) if status is not None else "")
    if ordinal is None or ordinal == 0:
        return None

    if not isinstance(activity, dict):
        return {0: None, 1: ordinal}

    return {0: _activity_payload(str(activity.get("type") or ""), activity), 1: ordinal}


hub = MetadataHub()
mount(hub)


class StablePresenceListener:
    """Forwards stable presence changes to lazer watchers.

    bakenohana is the authority on stable sessions, so it publishes the changed
    user id on ``signalr:presence_changed`` and this pushes the (re-read) presence
    document to everyone watching that user.

    One listener per process, started once. Without it a lazer client would only
    learn about a stable user when some *other* event happened to trigger a
    broadcast, which is not the same as presence.
    """

    def __init__(self, hub: MetadataHub) -> None:
        self._hub = hub
        self._task: asyncio.Task[None] | None = None
        self._pubsub: aioredis.client.PubSub | None = None

    async def start(self) -> None:
        if self._task is not None:
            return

        self._task = asyncio.create_task(self._run(), name="stable-presence-listener")
        self._task.add_done_callback(lambda _task: self._forget(_task))

    def _forget(self, task: asyncio.Task[None]) -> None:
        # only clear if this is still the current task; a restart may have
        # already installed a replacement
        if self._task is task:
            self._task = None

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

        if self._pubsub is not None:
            with contextlib.suppress(Exception):
                await self._pubsub.aclose()
            self._pubsub = None

    async def _run(self) -> None:
        # reconnect loop: a redis blip must not silently stop presence updates
        while True:
            try:
                r = await self._hub.redis()
                self._pubsub = r.pubsub(ignore_subscribe_messages=True)
                await self._pubsub.subscribe(STABLE_CHANNEL)

                while True:
                    message = await self._pubsub.get_message(ignore_subscribe_messages=True, timeout=30.0)

                    if message is None:
                        continue

                    raw = message.get("data")
                    if raw is None:
                        continue

                    await self._handle(_text(raw))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - listener must survive anything
                with contextlib.suppress(Exception):
                    if self._pubsub is not None:
                        await self._pubsub.aclose()
                self._pubsub = None
                await asyncio.sleep(2.0)
                _LOG.warning("stable presence listener reconnecting: %s", exc)

    async def _handle(self, payload: str) -> None:
        with contextlib.suppress(ValueError):
            user_id = int(payload)
            await self._hub.broadcast(user_id)


_listener: StablePresenceListener | None = None


async def start_stable_presence() -> None:
    global _listener
    if _listener is None:
        _listener = StablePresenceListener(hub)
    await _listener.start()


async def stop_stable_presence() -> None:
    global _listener
    if _listener is not None:
        await _listener.stop()
        _listener = None
