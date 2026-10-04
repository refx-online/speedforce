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

* **bakenohana has no redis presence structure today** -- its online sessions live
  only in its in-memory ``PlayerSession`` registry, which speedforce cannot read.
  So lazer users see each other but are currently invisible to stable. Writing the
  same keys from bakenohana's login/logout path is what closes that gap; see
  notes.md.
"""

from __future__ import annotations

import contextlib
import json
from typing import Any

import redis.asyncio as aioredis
from redis.exceptions import RedisError

import app.settings as settings
from app.signalr.host import mount
from app.signalr.protocol import HubConnection

PRESENCE_KEY = "signalr:presence:{user_id}"
QUEUE_KEY = "signalr:presence_queue_id"

# A presence entry expires this long after its last refresh, so a client that
# vanishes without a clean disconnect stops reading as online.
PRESENCE_TTL_SECONDS = 90


class MetadataHub:
    name = "metadata"

    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None
        # connection_id -> user_ids this connection is watching
        self._watching: dict[str, set[int]] = {}
        # every live connection in this process, for watcher fan-out
        self._live: set[HubConnection] = set()

    async def redis(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        return self._redis

    def _watches(self, connection: HubConnection, user_id: int) -> bool:
        return user_id in self._watching.get(connection.connection_id, set())

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
        await self._broadcast(connection.user_id)

    async def on_disconnect(self, connection: HubConnection) -> None:
        self._live.discard(connection)
        self._watching.pop(connection.connection_id, None)

        # only clear presence once the user's *last* connection is gone
        if any(c.user_id == connection.user_id for c in self._live):
            return

        r = await self.redis()
        with contextlib.suppress(RedisError):
            await r.delete(PRESENCE_KEY.format(user_id=connection.user_id))
        await self._broadcast(connection.user_id)

    # ------------------------------------------------------------- hub methods

    async def invoke(self, connection: HubConnection, target: str, args: list[Any]) -> Any:
        if target in ("UpdateActivity", "UpdateStatus"):
            return await self._update_presence(connection, args[0] if args else None)

        if target == "BeginWatchingUserPresence":
            return await self._begin_watching(connection)

        if target == "EndWatchingUserPresence":
            self._watching.pop(connection.connection_id, None)
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

        await self._broadcast(connection.user_id)
        return None

    async def _begin_watching(self, connection: HubConnection) -> None:
        """Subscribe to everyone else's presence and send a snapshot.

        ``UserPresenceUpdated`` is registered client-side as (userId, presence),
        so a batch is one message per user. Sending the snapshot here means a
        freshly connected client is not blank until someone changes state.
        """
        r = await self.redis()

        online: set[int] = set()
        for key in await r.keys(f"{PRESENCE_KEY.format(user_id='')}*"):
            with contextlib.suppress(ValueError, IndexError):
                online.add(int(_text(key).rsplit(":", 1)[1]))

        watched = online - {connection.user_id}
        self._watching[connection.connection_id] = watched

        for user_id in sorted(watched):
            await connection.send(
                "UserPresenceUpdated", user_id, _loads(await r.get(PRESENCE_KEY.format(user_id=user_id)))
            )

        return None

    async def _get_changes_since(self, queue_id: int) -> dict[str, Any]:
        """Queue cursor for reconnect catch-up.

        A full deployment keeps an append-only change log keyed by this id. Here
        the cursor only advances: enough for the client's reconnect path to
        complete without replaying changes it has already applied.
        """
        r = await self.redis()
        return {"beatmapSets": [], "queueId": int(await r.get(QUEUE_KEY) or 0)}

    async def _broadcast(self, user_id: int) -> None:
        """Push one user's presence to every connection watching them."""
        r = await self.redis()
        payload = _loads(await r.get(PRESENCE_KEY.format(user_id=user_id)))

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


hub = MetadataHub()
mount(hub)
