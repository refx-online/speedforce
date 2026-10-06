"""The multiplayer hub: ``/signalr/multiplayer``.

Lazer speaks only SignalR, so this is the prerequisite for any crossplay -- a
lazer client cannot receive stable's binary ``MatchJoinSuccess`` at all, and a
bridge has to terminate a hub here regardless.

Two surfaces, matching the client:

* **lounge** -- ``CreateRoom``, ``JoinRoom``, ``JoinRoomWithPassword``
  (``IMultiplayerLoungeServer.cs``). The room *list* is not here: it is a REST
  call, ``GET /api/v2/rooms`` (``Rooms/GetRoomsRequest.cs``).
* **in-room** -- the 18 methods on ``IMultiplayerRoomServer``.

Rooms are server-authoritative, which is lazer's own model and the inverse of
stable's host-relayed rooms. See ``rooms.py`` for the state.

**Not implemented: playlists.** Six hub methods and the client's
``AllPlayersRoundRobin`` queue depend on ``MatchSettings``/``Ruleset``
negotiation that needs real beatmap metadata. They raise ``NotImplemented``
rather than half-working -- a playlist that silently accepted items it could not
validate would be worse than one that refuses.
"""

from __future__ import annotations

from typing import Any

from app.signalr.host import mount
from app.signalr.protocol import HubConnection
from app.signalr.rooms import IDLE
from app.signalr.rooms import IN_GAME
from app.signalr.rooms import BadPassword
from app.signalr.rooms import MultiplayerRoom
from app.signalr.rooms import NoSuchRoom
from app.signalr.rooms import NotHost

__all__ = ["MultiplayerHub", "hub", "rooms"]

rooms: dict[int, MultiplayerRoom] = {}

# user_id -> room_id, so a reconnecting client can find its room again
_user_rooms: dict[int, int] = {}


class MultiplayerHub:
    name = "multiplayer"

    def __init__(self) -> None:
        self._connections: dict[str, HubConnection] = {}

    # ---------------------------------------------------------------- lifecycle

    async def on_connect(self, connection: HubConnection) -> None:
        self._connections[connection.connection_id] = connection

    async def on_disconnect(self, connection: HubConnection) -> None:
        self._connections.pop(connection.connection_id, None)

        room_id = _user_rooms.pop(connection.user_id, None)
        if room_id is None:
            return

        room = rooms.get(room_id)
        if room is None:
            return

        # a dropped connection leaves the room but must not strand it: promote
        # someone else rather than leaving a hostless room
        if room.is_host(connection.user_id):
            remaining = list(room.users)
            if remaining:
                # dict values are RoomUser; keys are user ids
                successor = next(iter(room.users.values()))
                room.host_id = successor.user_id
                await self._push(room, "HostChanged", room.host_id)
            else:
                del rooms[room.room_id]
                return

        await self._user_left(room, connection.user_id)

    # ------------------------------------------------------------- dispatching

    async def invoke(self, connection: HubConnection, target: str, args: list[Any]) -> Any:
        handler = getattr(self, f"_{target}", None)

        if handler is None:
            if target in PENDING:
                raise NotImplementedError(f"{target} needs playlist/match-settings support")
            raise NotImplementedError(target)

        return await handler(connection, *args)

    # ------------------------------------------------------------------ lounge

    async def _CreateRoom(self, connection: HubConnection, *args: Any) -> dict[int, Any]:
        """Create a room from `CreateRoom(MultiplayerRoom)`.

        Accepts the room either as one argument or as its fields spread across
        several. The refx-stable client sends the latter -- its frame put the room
        straight into the arguments slot instead of wrapping it, so the room's
        nine fields arrived as nine arguments and this raised

            TypeError: _CreateRoom() takes 3 positional arguments but 10 were given

        which surfaced client-side as a frozen lobby: `OnCreateGameMPv2` sets
        `LobbyStatus.PendingCreate` before the call and only clears it in the
        success/failure callbacks, so a completion it never understood left the
        screen stuck rather than reporting anything.

        Re-wrapping at the boundary is the right place for this: the method
        semantically takes exactly one room, and both spellings name it.
        """
        if len(args) > 1:
            room = list(args)
        elif args:
            room = args[0]
        else:
            raise ValueError("CreateRoom called with no room")

        settings = _parse_settings(room)
        user = _user(connection)

        if connection.user_id in _user_rooms:
            existing = rooms.get(_user_rooms[connection.user_id])
            if existing is not None:
                raise ValueError("already in a room")

        created = MultiplayerRoom(settings, user)

        # `MultiplayerRoomSettings` has no beatmap member at all, so the playlist
        # cannot be seeded from the settings the way it used to be -- lazer sends
        # the playlist explicitly (`Key(6)`) and that is where the beatmap
        # actually lives. Prefer the client's own playlist, falling back to the
        # seeded one for a client that sends none.
        supplied = _playlist_from_room(room)

        if supplied:
            created.playlist = supplied
        await _hydrate_playlist_items(created.playlist)
        rooms[created.room_id] = created
        _user_rooms[connection.user_id] = created.room_id

        return created.to_client()

    async def _JoinRoom(self, connection: HubConnection, room_id: int) -> dict[int, Any]:
        return await self._join(connection, int(room_id), None)

    async def _JoinRoomWithPassword(self, connection: HubConnection, room_id: int, password: str) -> dict[int, Any]:
        return await self._join(connection, int(room_id), password)

    async def _join(self, connection: HubConnection, room_id: int, password: str | None) -> dict[int, Any]:
        room = rooms.get(room_id)
        if room is None:
            raise NoSuchRoom(room_id)

        if room.settings.password:
            if not password:
                raise BadPassword(room_id)
            if password != room.settings.password:
                raise BadPassword(room_id)

        if connection.user_id in _user_rooms:
            raise ValueError("already in a room")

        user = _user(connection)
        try:
            room.add_user(user)
        except Exception as exc:  # RoomFull
            raise ValueError(str(exc)) from exc

        _user_rooms[connection.user_id] = room_id

        await self._push(room, "UserJoined", user.to_client(), skip=connection.connection_id)
        return room.to_client()

    async def _LeaveRoom(self, connection: HubConnection) -> None:
        room = self._room_for(connection)

        _user_rooms.pop(connection.user_id, None)

        if room.is_host(connection.user_id):
            remaining = [u for uid, u in room.users.items() if uid != connection.user_id]
            if remaining:
                room.host_id = remaining[0].user_id
                await self._push(room, "HostChanged", room.host_id, skip=connection.connection_id)
                await self._user_left(room, connection.user_id)
                return
            del rooms[room.room_id]
            return

        await self._user_left(room, connection.user_id, skip=connection.connection_id)

    async def _user_left(self, room: MultiplayerRoom, user_id: int, skip: str | None = None) -> None:
        user = room.remove_user(user_id)
        if user is None:
            return
        await self._push(room, "UserLeft", user.to_client(), skip=skip)

    # ----------------------------------------------------------------- in-room

    async def _KickUser(self, connection: HubConnection, user_id: int) -> None:
        room = self._room_for(connection)
        self._assert_host(room, connection)

        target_id = int(user_id)
        if target_id == room.host_id:
            raise ValueError("cannot kick the host; transfer host first")

        kicked = room.users.get(target_id)
        if kicked is None:
            return

        room.remove_user(target_id)
        _user_rooms.pop(target_id, None)

        for conn in list(self._connections.values()):
            if conn.user_id == target_id:
                await conn.send("UserKicked", kicked.to_client())
                self._connections.pop(conn.connection_id, None)
                _user_rooms.pop(target_id, None)

        await self._push(room, "UserLeft", kicked.to_client())

    async def _TransferHost(self, connection: HubConnection, user_id: int) -> None:
        room = self._room_for(connection)
        self._assert_host(room, connection)

        target_id = int(user_id)
        if target_id not in room.users:
            raise ValueError("user is not in the room")

        room.host_id = target_id
        await self._push(room, "HostChanged", room.host_id)

    async def _InvitePlayer(self, connection: HubConnection, user_id: int) -> None:
        room = self._room_for(connection)
        target_id = int(user_id)
        await self._send_to(target_id, "Invited", connection.user_id, room.room_id, room.settings.password or "")

    async def _ChangeSettings(self, connection: HubConnection, settings: Any) -> None:
        room = self._room_for(connection)
        self._assert_host(room, connection)

        room.settings = _parse_settings(settings)
        await self._push(room, "SettingsChanged", room.settings.to_client())

    async def _ChangeState(self, connection: HubConnection, state: Any) -> None:
        room = self._room_for(connection)
        self._assert_host(room, connection)

        # MultiplayerRoomState is a C# enum: it arrives as its underlying integer
        # (Open=0, WaitingForLoad=1, Playing=2, Closed=3), not as a name.
        new_state = MultiplayerRoom.parse_state(state)
        if new_state is None:
            raise ValueError(f"unknown room state {state!r}")

        room.state = new_state
        await self._push(room, "RoomStateChanged", room.state)

    async def _ChangeUserMods(self, connection: HubConnection, mods: Any) -> None:
        room = self._room_for(connection)
        user = room.users.get(connection.user_id)
        if user is None:
            return
        user.mods = mods if isinstance(mods, list) else []
        await self._push(room, "UserModsChanged", connection.user_id, user.mods)

    async def _StartMatch(self, connection: HubConnection) -> None:
        room = self._room_for(connection)
        self._assert_host(room, connection)

        room.state = IN_GAME

        await self._push(room, "RoomStateChanged", room.state)
        await self._push(room, "GameplayStarted")

    async def _AbortMatch(self, connection: HubConnection) -> None:
        room = self._room_for(connection)
        self._assert_host(room, connection)

        room.state = IDLE
        await self._push(room, "RoomStateChanged", room.state)

    async def _AbortGameplay(self, connection: HubConnection, reason: Any) -> None:
        room = self._room_for(connection)
        await self._push(room, "GameplayAborted", reason if isinstance(reason, str) else "HostAbortedTheMatch")

    async def _ResultsReady(self, connection: HubConnection) -> None:
        room = self._room_for(connection)
        await self._push(room, "RoomStateChanged", room.state)

    async def _VoteToSkipIntro(self, connection: HubConnection, voted: bool) -> None:
        room = self._room_for(connection)
        await self._push(room, "UserVotedToSkipIntro", connection.user_id, bool(voted))

    # ----------------------------------------------------------------- helpers

    def _room_for(self, connection: HubConnection) -> MultiplayerRoom:
        room_id = _user_rooms.get(connection.user_id)
        if room_id is None:
            raise ValueError("not in a room")
        room = rooms.get(room_id)
        if room is None:
            _user_rooms.pop(connection.user_id, None)
            raise NoSuchRoom(room_id)
        return room

    @staticmethod
    def _assert_host(room: MultiplayerRoom, connection: HubConnection) -> None:
        if not room.is_host(connection.user_id):
            raise NotHost(f"user {connection.user_id} is not the host")

    async def _push(self, room: MultiplayerRoom, target: str, *args: Any, skip: str | None = None) -> None:
        for conn in list(self._connections.values()):
            if skip is not None and conn.connection_id == skip:
                continue
            if _user_rooms.get(conn.user_id) == room.room_id:
                await conn.send(target, *args)

    async def _send_to(self, user_id: int, target: str, *args: Any) -> None:
        for conn in list(self._connections.values()):
            if conn.user_id == user_id:
                await conn.send(target, *args)

    async def _AddPlaylistItem(self, connection: HubConnection, item: Any) -> None:
        """Append a playlist item.

        Mirrors `RoomBase.AddPlaylistItemInternal` in the g0v0 reference: append
        with `PlaylistOrder = MaxValue`, push `PlaylistItemAdded`, then reorder
        and repoint the current item. The push happens *before* the reorder so
        the client's add handler sees the item in the order it sent it, matching
        the reference's ordering.
        """
        room = self._room_for(connection)
        parsed = _parse_playlist_item(item, room.next_item_id())

        await _hydrate_playlist_items([parsed])

        room.playlist.append(parsed)
        await self._push(room, "PlaylistItemAdded", parsed.to_client())

        room.reorder_playlist()
        if room.state == IDLE:
            await self._push(room, "PlaylistItemChanged", parsed.to_client())
        if room.sync_current_item():
            await self._push(room, "SettingsChanged", room.settings.to_client())

    async def _EditPlaylistItem(self, connection: HubConnection, item: Any) -> None:
        room = self._room_for(connection)
        item_id = _map_int(item, MPV2_KEY_ITEM_ID)
        existing = next((i for i in room.playlist if i.item_id == item_id), None)

        if existing is None:
            raise ValueError(f"no playlist item {item_id}")

        # Only the mutable fields are taken from the client. `owner_id` and the
        # queue position are server state and are deliberately not client-writable.
        existing.beatmap_id = _map_int(item, MPV2_KEY_BEATMAP_ID, existing.beatmap_id)
        existing.beatmap_md5 = _map_string(item, MPV2_KEY_BEATMAP_MD5, existing.beatmap_md5)
        existing.ruleset_id = _map_int(item, MPV2_KEY_RULESET_ID, existing.ruleset_id)
        existing.expired = bool(_map_int(item, MPV2_KEY_EXPIRED, int(existing.expired)))

        await _hydrate_playlist_items([existing])

        room.reorder_playlist()
        await self._push(room, "PlaylistItemChanged", existing.to_client())

        if room.sync_current_item():
            await self._push(room, "SettingsChanged", room.settings.to_client())

    async def _RemovePlaylistItem(self, connection: HubConnection, playlist_item_id: int) -> None:
        room = self._room_for(connection)
        removed = room.remove_item(int(playlist_item_id))

        if removed is None:
            raise ValueError(f"no playlist item {playlist_item_id}")

        await self._push(room, "PlaylistItemRemoved", removed.item_id)

        if room.sync_current_item():
            await self._push(room, "SettingsChanged", room.settings.to_client())

    async def _ChangeBeatmapAvailability(self, connection: HubConnection, availability: Any) -> None:
        """Record a client's map-download state.

        `BeatmapAvailability` is a `[Key(0)] State, [Key(1)] progress` map; only
        the state is stored, since progress is transient.
        """
        room = self._room_for(connection)
        user = room.users.get(connection.user_id)

        if user is None:
            return

        user.beatmap_availability = _map_int(availability, MPV2_KEY_AVAILABILITY_STATE, user.beatmap_availability)
        await self._push(room, "UserBeatmapAvailabilityChanged", connection.user_id, availability)

    async def _ChangeUserStyle(self, connection: HubConnection, style: Any) -> None:
        """Record in-match style/score state for the calling user.

        Relayed verbatim: the payload is a `[Union]` (`MatchUserState`) and is only
        ever passed between clients, so decoding and re-encoding it here would risk
        reshaping a type we do not model.
        """
        room = self._room_for(connection)
        user = room.users.get(connection.user_id)

        if user is None:
            return

        user.state = style
        # Broadcast to the whole room, sender included. The reference does
        # `Clients.Group(...).MatchUserStateChanged(...)` with no exclusion, and
        # every other player's client needs it while the sender's own is
        # idempotent.
        await self._push(room, "UserStyleChanged", connection.user_id, style)


def _user(connection: HubConnection):
    from app.signalr.rooms import RoomUser

    return RoomUser(user_id=connection.user_id)


def _get(source: Any, int_key: int, str_key: str, default: Any = None) -> Any:
    """Read a field from a hub payload by position or by name.

    A `[MessagePackObject]` with `[Key(N)]` integer members arrives as a
    **positional array** -- confirmed against the real client, which sends
    `CreateRoom [[0, 0, ["name", 0, "", 1, 0, 0, false, nil], ...]`. The same
    payload is authored here as an int-keyed dict and rewritten by
    ``protocol.positionalise`` on the way out, so both spellings have to parse:
    array from a client, dict from our own code and from the JSON hub protocol
    (where keys are strings).
    """
    if isinstance(source, (list, tuple)):
        return source[int_key] if -len(source) <= int_key < len(source) else default

    if not isinstance(source, dict):
        return default

    if int_key in source:
        return source[int_key]

    if str_key in source:
        return source[str_key]

    return default


def _map_int(source: Any, int_key: int, default: int = 0) -> int:
    value = _get(source, int_key, str(int_key), default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _map_string(source: Any, int_key: int, default: str = "") -> str:
    value = _get(source, int_key, str(int_key), default)
    return str(value) if value is not None else default


# Integer keys, transcribed from the lazer declarations. See rooms.py for the
# full per-DTO layouts; only the ones these methods read are named here.
MPV2_KEY_ITEM_ID = 0  # MultiplayerPlaylistItem.ID
MPV2_KEY_BEATMAP_ID = 2  # MultiplayerPlaylistItem.BeatmapID
MPV2_KEY_BEATMAP_MD5 = 3  # MultiplayerPlaylistItem.BeatmapChecksum
MPV2_KEY_RULESET_ID = 4  # MultiplayerPlaylistItem.RulesetID
MPV2_KEY_EXPIRED = 7  # MultiplayerPlaylistItem.Expired
MPV2_KEY_AVAILABILITY_STATE = 0  # BeatmapAvailability.State


def _parse_playlist_item(raw: Any, item_id: int):
    """Build a `PlaylistItem` from the client's payload.

    `OwnerID` is taken from the *authenticated* connection rather than the
    payload: the client sets it, but letting a client claim authorship of
    someone else's queue item is not something to leave open.
    """
    from app.signalr.rooms import PlaylistItem

    return PlaylistItem(
        beatmap_id=_map_int(raw, MPV2_KEY_BEATMAP_ID),
        beatmap_md5=_map_string(raw, MPV2_KEY_BEATMAP_MD5),
        ruleset_id=_map_int(raw, MPV2_KEY_RULESET_ID),
        mods=_get(raw, 5, "RequiredMods") or [],
        # required_mods / allowed_mods are separate members (keys 5 and 6); only
        # required mods are modelled, and the reference stores both.
        star_rating=None,
        player_id=None,
        item_id=item_id,
    )


async def _hydrate_playlist_items(items) -> None:
    """Fill in each item's map title/artist/difficulty from the `maps` table.

    One batched query for the whole room rather than one per item: a 16-item
    playlist would otherwise be 16 round trips, and this runs on every add.

    Uses the existing `beatmap_repo` rather than a new lookup, mirroring how the
    reference resolves `IBeatmapRepository` in `RoomBase.GetStarRating`. Unknown
    beatmaps are left blank rather than failing the add -- a map we have no row
    for is normal (never-downloaded maps) and must not block the playlist.
    """
    from app.models.beatmap_repo import get_beatmap_rows_by_ids

    pending = [i for i in items if not i.beatmap_name]
    if not pending:
        return

    try:
        rows = await get_beatmap_rows_by_ids([i.beatmap_id for i in pending])
    except Exception:  # noqa: BLE001 - metadata is cosmetic; never fail an add for it
        return

    by_id = {r.id: r for r in rows}

    for item in pending:
        row = by_id.get(item.beatmap_id)
        if row is None:
            continue

        item.beatmap_name = row.title
        item.beatmap_artist = row.artist
        # `diff` is the star rating column; star_rating is the hub's field name.
        item.star_rating = float(row.diff)


# `MultiplayerRoomSettings` (osu.Game/Online/Multiplayer/MultiplayerRoomSettings.cs):
#   0 Name  1 PlaylistItemId  2 Password  3 MatchType
#   4 QueueMode  5 AutoStartDuration  6 AutoSkip  7 MaxParticipants
#
# There is no beatmap or ruleset field here at all -- the beatmap lives on the
# playlist item. Reading 1 as a beatmap id and 4 as a ruleset id (which is what
# this did) picked up PlaylistItemId and the QueueMode enum instead.
_SETTINGS_FIELDS = (
    "Name",
    "PlaylistItemId",
    "Password",
    "MatchType",
    "QueueMode",
    "AutoStartDuration",
    "AutoSkip",
    "MaxParticipants",
)


def _playlist_from_room(raw: Any) -> list:
    """Read the playlist a client sent with `CreateRoom`.

    `MultiplayerRoom` `Key(6)` is the playlist, a list of
    `MultiplayerPlaylistItem`. Both spellings are accepted: a positional array
    from a real client, and an int-keyed dict from our own code and the JSON hub
    protocol.

    Each item is `Key(0)` ID, `Key(1)` OwnerID, `Key(2)` BeatmapID,
    `Key(3)` BeatmapChecksum, `Key(4)` RulesetID ...
    """
    raw_playlist = _get(raw, 6, "Playlist")
    if not isinstance(raw_playlist, (list, tuple)):
        return []

    from app.signalr.rooms import PlaylistItem

    items: list[PlaylistItem] = []
    for order, entry in enumerate(raw_playlist):
        beatmap_id = int(_get(entry, 2, "BeatmapID", 0) or 0)
        if beatmap_id <= 0:
            continue

        owner_id = int(_get(entry, 1, "OwnerID", 0) or 0)

        items.append(
            PlaylistItem(
                beatmap_id=beatmap_id,
                beatmap_md5=str(_get(entry, 3, "BeatmapChecksum", "") or ""),
                ruleset_id=int(_get(entry, 4, "RulesetID", 0) or 0),
                player_id=owner_id or None,
                item_id=len(items) + 1,
                playlist_order=order,
            )
        )

    return items


def _parse_settings(raw: Any):
    """Read `MultiplayerRoomSettings` out of whatever the client sent.

    Accepted shapes:
      * a bare settings object, positional or int-keyed
      * a whole `MultiplayerRoom`, where settings is ``Key(2)``
      * the JSON hub protocol's ``{"Settings": {...}}``

    Observed from the real client (SIGNALR_WIRE_LOG):

        [0, 0, ["kaupec2's awesome room", 0, "", 1, 0, 0, false, nil], [], nil, nil, [...], [], 0]
         ^  ^  ^ 0 Name                                                     ^ 7 MaxParticipants
               ^ Key(2) Settings

    Settings is at **index 2**, not 0, which is why reading it positionally as if
    it were the room gave an unnamed room.
    """
    from app.signalr.rooms import RoomSettings

    settings = raw

    if isinstance(settings, (list, tuple)):
        # A bare settings array is 8 long; a room array carries settings at 2.
        candidate = settings[2] if len(settings) > 2 and isinstance(settings[2], (list, dict)) else settings
        settings = candidate
    elif isinstance(settings, dict):
        nested = _get(settings, 2, "Settings")
        if isinstance(nested, (list, dict)):
            settings = nested

    def field(index: int, *names: str, default: Any = "") -> Any:
        value = _get(settings, index, "", None)

        if value is not None:
            return value

        if isinstance(settings, dict):
            for name in names:
                if name in settings:
                    return settings[name]

        return default

    return RoomSettings(
        name=str(field(0, "Name", "name") or "")[:50],
        password=field(2, "Password", "password") or None,
        beatmap_id=int(field(1, "PlaylistItemId", "BeatmapId", "beatmap_id") or 0),
        beatmap_md5="",
        ruleset_id=0,
        mods=[],
        max_players=int(field(7, "MaxParticipants", "MaxPlayers", "max_players") or 16),
    )

    # hub methods that exist on the client but are deliberately not implemented; they
    # all depend on playlist / match-settings support


PENDING = frozenset(
    {
        "SendMatchRequest",
    }
)

hub = MultiplayerHub()
mount(hub)
