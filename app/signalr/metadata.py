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

# Must be comfortably *inside* the TTL, for the same reason bakenohana's
# housekeeping was moved off 100s: a refresher slower than the timeout it is
# keeping alive is not a refresher.
PRESENCE_KEEPALIVE_INTERVAL_SECONDS = 30


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
        # Seed a complete presence document in the *stored* shape -- the same
        # shape bakenohana's bridge writes -- because `presence_to_client` reads
        # it. It used to be seeded as a bare activity (`{"type": ...}`), which
        # has no `Status`, so the user read as "no status" and a later
        # UpdateActivity merged on top of a document that still had none.
        #
        # ChoosingBeatmap (union 11) is the idle-ish default the client itself
        # sends on connect. Status defaults to Online: lazer sends `UpdateStatus`
        # immediately after connecting, but until it does, the reference also
        # treats a connected user as visible.
        document = {
            "Activity": {"type": "ChoosingBeatmap"},
            "Status": "Online",
            "rulesetId": 0,
            "client": "lazer",
        }
        await r.set(
            PRESENCE_KEY.format(user_id=connection.user_id),
            json.dumps(document),
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
            return await self._update_presence(connection, target, args[0] if args else None)

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

    async def _update_presence(self, connection: HubConnection, target: str, payload: Any) -> None:
        """Merge one half of a presence, then notify watchers.

        `IMetadataServer` has **two** separate methods, `UpdateActivity` and
        `UpdateStatus`, and lazer calls both right after connecting
        (`OnlineMetadataClient.cs:124-128`). Each carries only its own half: the
        activity is a `UserActivity` union array, the status is a bare
        `UserStatus` ordinal.

        Treating either as the whole document loses the other. Captured with
        SIGNALR_WIRE_LOG while both clients were connected::

            UpdateActivity [[12, [5333124, "Sewerslvt - ...", 0, "Clicking circles"]]]
            UpdateStatus   [2]

        Storing those verbatim left `signalr:presence:77` holding the bare
        string `2`, which `presence_to_client` rejects as not-a-dict -- so the
        lazer user's own presence resolved to offline and they were dropped from
        every list. The reference keeps both halves in one `PlayerState`; so does
        this now.
        """
        r = await self.redis()
        key = PRESENCE_KEY.format(user_id=connection.user_id)

        # keep whatever the other method last wrote, so a status update does not
        # discard the activity (and vice versa)
        current = _loads(await r.get(key))
        document: dict[str, Any] = current if isinstance(current, dict) else {}

        if target == "UpdateStatus":
            # a bare ordinal, but the stored contract is a name -- the bridge
            # writes "Online" and `presence_to_client` resolves by name
            document["Status"] = (
                _STATUS_NAMES.get(_to_ordinal(payload), "Online") if _to_ordinal(payload) else "Offline"
            )
        else:
            document["Activity"] = _activity_document(payload)

        if document.get("Status") == "Offline" or document.get("Activity") is None:
            # Offline, or gone idle: the client reads a missing key as "not
            # online" and removes the row.
            await r.delete(key)
        else:
            document.setdefault("client", "lazer")
            await r.set(key, json.dumps(document), ex=PRESENCE_TTL_SECONDS)

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

    async def _get_changes_since(self, queue_id: int) -> dict[int, Any]:
        """Queue cursor for reconnect catch-up.

        `BeatmapUpdates` is a `[MessagePackObject]`, so the map is **positional**:
        the resolver looks members up by `Key(N)` ordinal and field *names never
        reach the wire*.

            [Key(0)] int[] BeatmapSetIDs
            [Key(1)] int   LastProcessedQueueID

        Returning `{"beatmapSetIDs": ..., "lastProcessedQueueID": ...}` made the
        client report

            Error trying to deserialize result to BeatmapUpdates.
            Deserializing object of the `BeatmapUpdates` type for 'argument' failed.

        during connect, which aborts the catch-up pass. The old docstring here
        asserted the opposite premise ("field names must match"), and the
        resulting error was the one it warned about.

        A full deployment keeps an append-only change log keyed by this id. Here
        the cursor only advances: enough for the reconnect path to complete
        without replaying changes already applied.
        """
        r = await self.redis()
        return {0: [], 1: int(await r.get(QUEUE_KEY) or 0)}

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
    "InDailyChallengeLobby": 51,
    "PlayingDailyChallenge": 52,
}

# Member layouts per activity base, transcribed from
# osu.Game/Users/UserActivity.cs. A `[MessagePackObject]` is sent as an array of
# exactly `len(members)` values, so a wrong count is a hard parse failure in the
# client's binder -- and it fails *silently*, leaving the online list empty.
#
# InGame (UserActivity.cs:59-78)     BeatmapID, BeatmapDisplayTitle, RulesetID,
#                                    RulesetPlayingVerb
# EditingBeatmap (:150-158)          BeatmapID, BeatmapDisplayTitle
# WatchingReplay (:196-209)          ScoreID, PlayerName, BeatmapID, BeatmapDisplayTitle
# InLobby (:260-267)                 RoomID, RoomName
# ChoosingBeatmap, SearchingForLobby, InDailyChallengeLobby declare no members.
_ACTIVITY_MEMBERS: dict[str, tuple[str, ...]] = {
    "InSoloGame": ("BeatmapID", "BeatmapDisplayTitle", "RulesetID", "RulesetPlayingVerb"),
    "InMultiplayerGame": ("BeatmapID", "BeatmapDisplayTitle", "RulesetID", "RulesetPlayingVerb"),
    "SpectatingMultiplayerGame": ("BeatmapID", "BeatmapDisplayTitle", "RulesetID", "RulesetPlayingVerb"),
    "InPlaylistGame": ("BeatmapID", "BeatmapDisplayTitle", "RulesetID", "RulesetPlayingVerb"),
    "PlayingDailyChallenge": ("BeatmapID", "BeatmapDisplayTitle", "RulesetID", "RulesetPlayingVerb"),
    "EditingBeatmap": ("BeatmapID", "BeatmapDisplayTitle"),
    "ModdingBeatmap": ("BeatmapID", "BeatmapDisplayTitle"),
    "TestingBeatmap": ("BeatmapID", "BeatmapDisplayTitle"),
    "WatchingReplay": ("ScoreID", "PlayerName", "BeatmapID", "BeatmapDisplayTitle"),
    "SpectatingUser": ("ScoreID", "PlayerName", "BeatmapID", "BeatmapDisplayTitle"),
    "InLobby": ("RoomID", "RoomName"),
}

# `UserStatus` ordinals (osu.Game/Users/UserStatus.cs): Offline=0,
# DoNotDisturb=1, Online=2. JSON stores the name; the wire wants the ordinal.
_STATUS_ORDINALS = {"Offline": 0, "DoNotDisturb": 1, "Online": 2}
_STATUS_NAMES = {ordinal: name for name, ordinal in _STATUS_ORDINALS.items()}


def _to_ordinal(value: Any) -> int:
    """Coerce a `UserStatus` argument to its ordinal.

    The client sends a bare int over the hub, but a JSON-hub client would send
    the name, so both spellings are accepted.
    """
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return _STATUS_ORDINALS.get(value, 0)

    return 0


def _activity_document(payload: Any) -> dict[str, Any] | None:
    """Turn an inbound `UserActivity` union array into the stored named form.

    Redis holds the same shape bakenohana's bridge writes -- ``{"type": ...}``
    plus named members -- because `presence_to_client` and both writers agree on
    that. The wire form is the opposite: a positional ``[unionKey, [members...]]``
    array, e.g. ``[12, [5333124, "Sewerslvt - ...", 0, "Clicking circles"]]``.

    ``None`` means the client sent nil, which is how it says "idle".
    """
    if payload is None:
        return None

    # a bare {"type": ...} is already in stored form
    if isinstance(payload, dict):
        return payload

    if not isinstance(payload, (list, tuple)) or not payload:
        return {"type": "ChoosingBeatmap"}

    union_key = payload[0]
    members = payload[1] if len(payload) > 1 and isinstance(payload[1], (list, tuple)) else []

    for name, key in _UNION_KEYS.items():
        if key != union_key:
            continue

        names = _ACTIVITY_MEMBERS.get(name, ())
        document: dict[str, Any] = {"type": name}
        document.update({member: members[i] for i, member in enumerate(names) if i < len(members)})
        return document

    return {"type": "ChoosingBeatmap"}


def _activity_payload(kind: str, activity: dict[str, Any]) -> list[Any]:
    """Build the `[key, payload]` array a `UnionFormatter` writes."""
    key = _UNION_KEYS.get(kind)
    if key is None:
        # Unknown activity: relay as an empty ChoosingBeatmap rather than drop the
        # user from the list entirely. Losing the row is worse than a wrong verb.
        return [_UNION_KEYS["ChoosingBeatmap"], {}]

    members = _ACTIVITY_MEMBERS.get(kind)

    if members is None:
        # Declares no members, so the payload is an **empty array**.
        #
        # `[]` explicitly, not `{}`: positionalise decides map-vs-array from the
        # keys, and an empty dict has none, so it would have stayed a fixmap --
        # the exact failure this whole change is fixing, reappearing for the one
        # type that has no members to infer a shape from. The client reads these
        # with `ReadArrayHeader()` like every other object.
        return [key, []]

    # `RulesetPlayingVerb` is what the client renders as the status text
    # (`GetStatus()` returns it), so it is not decoration -- omitting it was one
    # of the reasons an InGame presence failed to bind. Stable's bancho status
    # has no such concept, so derive the standard verb from the ruleset.
    values: dict[int, Any] = {}

    for index, name in enumerate(members):
        if name == "RulesetPlayingVerb":
            values[index] = "playing"
        else:
            values[index] = activity.get(name)

    return [key, values]


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
_keepalive: asyncio.Task[None] | None = None


async def _presence_keepalive() -> None:
    """Keep lazer presences from expiring while their connections are open.

    `PRESENCE_TTL_SECONDS` is a safety net for crashed servers, but lazer only
    sends `UpdateActivity`/`UpdateStatus` on *state changes* -- the ~1/s traffic
    is `Ping` -- so an idle lazer client never rewrites its own document. Without
    this the key expires 90s after connect and the user stops existing for
    everyone: they vanish from their own online list, and from stable's.

    This is the same bug that was fixed on the bakenohana side for stable
    presences, one layer over. Both ends of the bridge need it.

    Deliberately does not publish when the key is merely alive. The document is
    unchanged, so re-broadcasting every connected user to every watcher on every
    tick would be a fan-out storm for no visible difference; only a missing key
    is republished, since that genuinely is a change watchers have not seen.
    """
    while True:
        await asyncio.sleep(PRESENCE_KEEPALIVE_INTERVAL_SECONDS)

        try:
            r = await hub.redis()
            connections = list(hub._live)  # noqa: SLF001 - same module, deliberate
        except Exception:  # noqa: BLE001 - never let this loop kill the process
            continue

        for connection in connections:
            key = PRESENCE_KEY.format(user_id=connection.user_id)

            try:
                if await r.exists(key):
                    await r.expire(key, PRESENCE_TTL_SECONDS)
                    continue

                stored = _loads(await r.get(key))
                if isinstance(stored, dict) and stored:
                    # key expired underneath us; re-establish it and tell watchers
                    await r.set(key, json.dumps(stored), ex=PRESENCE_TTL_SECONDS)
                    await hub.broadcast(connection.user_id)
            except Exception:  # noqa: BLE001 - one bad connection is not fatal
                continue


async def start_stable_presence() -> None:
    global _listener, _keepalive
    if _listener is None:
        _listener = StablePresenceListener(hub)
    await _listener.start()
    if _keepalive is None:
        _keepalive = asyncio.create_task(_presence_keepalive())


async def stop_stable_presence() -> None:
    global _listener, _keepalive
    if _keepalive is not None:
        _keepalive.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _keepalive
        _keepalive = None
    if _listener is not None:
        await _listener.stop()
        _listener = None
