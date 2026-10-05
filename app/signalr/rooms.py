"""Multiplayer room model for the lazer multiplayer hub.

Rooms are server-authoritative here, mirroring lazer's own design: the hub owns
the state and pushes changes, and clients only ever *request* changes. That is the
opposite of stable, where the host relays room state.

Deliberately not modelled yet: playlists. ``MultiplayerRoom.Playlist`` exists on
the client and six hub methods operate on it, but a playlist needs the full
``MatchSettings``/``Ruleset`` negotiation to be meaningful. Those methods raise
``NotImplemented`` rather than half-working -- see ``PENDING`` below.

Rooms live in memory for now. A restart drops them, which is acceptable for a
single instance but means the room list is not durable; see notes.md.
"""

from __future__ import annotations

import asyncio

import itertools
import time
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from app.models.repository import find_user_by_id

# Internal room state. Deliberately *not* the wire value: see `_ROOM_STATE`.
IDLE = "Idle"
IN_GAME = "InGame"
PRACTICE = "Practice"

# Slot team, matching MultiplayerTeam
TEAM_SOLO = 0
TEAM_COLOUR_1 = 1
TEAM_COLOUR_2 = 2

# ---------------------------------------------------------------------------
# Wire enum values, transcribed from the client's declarations.
#
# These are plain ints on the wire: the DTO members are C# enums, and
# MessagePack serialises an enum as its underlying integer. Sending the name
# ("Idle") where the client expects the ordinal (0) is the same class of bug as
# sending a string map key where it expects an integer -- it fails at
# deserialisation with nothing legible in the client's log.
#
# Sources, all read from the refx-lazer checkout:
#   MultiplayerRoomState.cs    Open=0 WaitingForLoad=1 Playing=2 Closed=3
#   MultiplayerUserState.cs    Idle=0 Ready=1 WaitingForLoad=2 Loaded=3 ...
#   MatchType.cs               Playlists=0 HeadToHead=1 TeamVersus=2 ...
#   QueueMode.cs               HostOnly=0 AllPlayers=1 AllPlayersRoundRobin=2
#   MultiplayerRoomUserRole.cs Player=0 Referee=1
# ---------------------------------------------------------------------------
_ROOM_STATE = {IDLE: 0, PRACTICE: 1, IN_GAME: 2}
_STATE_ROOM = {v: k for k, v in _ROOM_STATE.items()}
_USER_STATE_IDLE = 0
_MATCH_TYPE_HEAD_TO_HEAD = 1
_QUEUE_MODE_HOST_ONLY = 0
_ROLE_PLAYER = 0

# DownloadState.Unknown, the value `BeatmapAvailability.Unknown()` carries.
_AVAILABILITY_UNKNOWN_STATE = 0
_AVAILABILITY_UNKNOWN_PROGRESS = None

MAX_PLAYERS = 16


async def _public_user(user_id: int) -> dict[str, Any]:
    """Minimal `APIUser` for the REST room list.

    Deliberately a subset of the full APIUser in `app/route/social.py`: a room
    panel renders the host's name, country and avatar, and nothing else. The
    shared helper there is async and pulls a whole statistics block per user,
    which a 16-user room list does not need.
    """
    user = await find_user_by_id(user_id)

    return {
        "id": user.id if user else user_id,
        "username": user.name if user else "",
        "country_code": user.country or "xx" if user else "xx",
        "avatar_url": f"https://a.041095.xyz/{user_id}",
        "cover_url": "",
        "playmode": 0,
        "is_online": True,
    }


def _next_room_id() -> int:
    # monotonic per process; lazer room ids are int64 on the wire but nothing in
    # the client depends on them being large or globally unique
    global _room_ids
    room_id = next(_room_ids)
    return room_id


_room_ids = itertools.count(1)


@dataclass
class PlaylistItem:
    """One playlist entry. Shape follows osu.Game.Online.Rooms.PlaylistItem."""

    beatmap_id: int
    beatmap_md5: str
    ruleset_id: int = 0
    mods: list[dict[str, Any]] = field(default_factory=list)
    star_rating: float | None = None
    player_id: int | None = None
    item_id: int = 0
    # PlaylistOrder / Expired / PlayedAt are separate from `mods` because the
    # queue order is server state, not per-item configuration. Mirrors
    # MultiplayerPlaylistItem keys 7, 8 and 9.
    expired: bool = False
    playlist_order: int = 0
    played_at: float | None = None
    # Resolved from the `maps` table when the item is added. Kept on the item
    # rather than looked up during rendering because `to_client` is sync and the
    # lookup is async -- the reference does the same, awaiting
    # `RoomBase.GetStarRating` in the hub method and storing it on the item.
    beatmap_name: str = ""
    beatmap_artist: str = ""

    def to_client(self) -> dict[int, Any]:
        """Hub shape for `MultiplayerPlaylistItem` (`Online/Rooms/`).

        **Integer keys, in the order the client declares them.** A
        `[MessagePackObject]` with `[Key(N)]` compiles to a formatter that looks
        members up *by integer key*; the member names never reach the wire. So
        this map is positional and a renamed field here is invisible, while
        reordering one silently rebinds it.

        Flat `BeatmapID`/`BeatmapChecksum`/`RulesetID`, not nested objects --
        that is what the client declares, and a nested shape decodes to defaults.
        """
        return {
            0: self.item_id,  # ID
            1: self.player_id or 0,  # OwnerID
            2: self.beatmap_id,  # BeatmapID
            3: self.beatmap_md5,  # BeatmapChecksum
            4: self.ruleset_id,  # RulesetID
            5: [],  # RequiredMods
            6: [],  # AllowedMods
            7: self.expired,  # Expired
            8: self.playlist_order,  # PlaylistOrder
            9: None,  # PlayedAt (DateTimeOffset?) -- only meaningful once played
            10: self.star_rating,  # StarRating
            11: False,  # Freestyle
        }

    def to_rest(self) -> dict[str, Any]:
        """REST shape (`Rooms/PlaylistItem.cs`, snake_case).

        A different DTO field-for-field from `to_client`: the lounge list binds
        these by literal `[JsonProperty]` name, so `beatmap_id`/`ruleset_id`
        cannot be spelled `BeatmapID`/`RulesetID` here.
        """
        return {
            "beatmap_id": self.beatmap_id,
            # The lounge panel renders "artist - title [version]"; without these
            # it shows a blank line, which reads as "broken room" rather than
            # "no metadata".
            "beatmap": {
                "id": self.beatmap_id,
                "md5": self.beatmap_md5,
                "artist": self.beatmap_artist,
                "title": self.beatmap_name,
            },
            "ruleset_id": self.ruleset_id,
            "mods": self.mods,
            "star_rating": self.star_rating,
            "player_id": self.player_id,
        }


@dataclass
class RoomUser:
    """A participant. Mirrors MultiplayerRoomUser plus the slot/team the client
    also carries."""

    user_id: int
    username: str = ""
    country_code: str = ""
    slot: int = -1
    team: int = TEAM_SOLO
    mods: list[dict[str, Any]] = field(default_factory=list)
    beatmap_id: int | None = None
    ruleset_id: int | None = None
    beatmap_availability: int = 0
    state: dict[str, Any] = field(default_factory=dict)

    def to_client(self) -> dict[int, Any]:
        """Hub shape for `MultiplayerRoomUser`.

        **Integer keys, in the client's declared order** -- see
        `PlaylistItem.to_client` for why that is the whole contract. `Slot`,
        `Username`, `CountryCode` and `Team` are *not* in it: this DTO has no
        such members, so anything sent under those names decoded to nothing.
        """
        return {
            0: self.user_id,  # UserID
            1: _USER_STATE_IDLE,  # State
            2: {  # BeatmapAvailability
                0: _AVAILABILITY_UNKNOWN_STATE,  # State
                1: _AVAILABILITY_UNKNOWN_PROGRESS,  # DownloadProgress
            },
            3: self.mods,  # Mods
            4: None,  # MatchState (union; per-match-type, not sent yet)
            5: self.ruleset_id,  # RulesetId
            6: self.beatmap_id,  # BeatmapId
            7: False,  # VotedToSkipIntro
            8: _ROLE_PLAYER,  # Role
        }


@dataclass
class RoomSettings:
    """Mirrors MultiplayerRoomSettings."""

    name: str = ""
    password: str | None = None
    beatmap_id: int = 0
    beatmap_md5: str = ""
    ruleset_id: int = 0
    mods: list[dict[str, Any]] = field(default_factory=list)
    max_players: int = MAX_PLAYERS

    def to_client(self) -> dict[int, Any]:
        """Hub shape for `MultiplayerRoomSettings`.

        **Integer keys, in the client's declared order.** Note key 2 is
        `Password`, and 3..7 are the settings/mode enum members the client
        declares -- sending `BeatmapId`/`RulesetId` here (as this did) shifted
        every field from 2 onwards by one.
        """
        return {
            0: self.name,  # Name
            1: self.beatmap_id,  # PlaylistItemId
            2: self.password or "",  # Password
            3: _MATCH_TYPE_HEAD_TO_HEAD,  # MatchType
            4: _QUEUE_MODE_HOST_ONLY,  # QueueMode
            5: 0,  # AutoStartDuration (TimeSpan ticks)
            6: False,  # AutoSkip
            7: self.max_players,  # MaxParticipants
        }


class MultiplayerRoom:
    """A single multiplayer room, owned by the hub."""

    def __init__(self, settings: RoomSettings, host: RoomUser) -> None:
        self.room_id = _next_room_id()
        self.settings = settings
        self.state = IDLE
        self.host_id = host.user_id
        self.users: dict[int, RoomUser] = {host.user_id: host}
        self.playlist: list[PlaylistItem] = []
        self.created_at = time.time()
        # `MatchRoomState` is an abstract base the client never declares keys on
        # (osu.Game/Online/Multiplayer/MatchRoomState.cs:22), so there is no
        # integer-key layout to match and nothing to send. `None` is what the
        # client's own nullable member expects.
        self.match_state: dict[int, Any] | None = None

        # seed the playlist from the settings the room was created with, which is
        # how the client expects the first item to appear
        if settings.beatmap_id:
            item = PlaylistItem(
                beatmap_id=settings.beatmap_id,
                beatmap_md5=settings.beatmap_md5,
                ruleset_id=settings.ruleset_id,
                mods=settings.mods,
                star_rating=None,
                player_id=host.user_id,
                item_id=1,
            )
            self.playlist.append(item)

    # ------------------------------------------------------------- membership

    @staticmethod
    def parse_state(raw: Any) -> str | None:
        """Read a room state as the client sends it, or None if unrecognised.

        The client's `MultiplayerRoomState` is an enum, so it arrives as the
        underlying **integer** on the hub protocol. Accepting only the old
        ``"Idle"``/``"InGame"`` strings meant a legitimate ChangeState was
        rejected as an unknown state.
        """
        if isinstance(raw, bool):
            return None
        if isinstance(raw, int):
            return _STATE_ROOM.get(raw)
        if isinstance(raw, str):
            if raw in _ROOM_STATE:
                return raw
            # tolerate a numeric string, since a REST-ish caller may stringify
            return _STATE_ROOM.get(int(raw)) if raw.isdigit() else None
        return None

    def free_slot(self) -> int | None:
        taken = {u.slot for u in self.users.values() if u.slot >= 0}
        for slot in range(min(self.settings.max_players, MAX_PLAYERS)):
            if slot not in taken:
                return slot
        return None

    def add_user(self, user: RoomUser) -> RoomUser:
        slot = self.free_slot()
        if slot is None:
            raise RoomFull(self.room_id)
        user.slot = slot
        self.users[user.user_id] = user
        return user

    def remove_user(self, user_id: int) -> RoomUser | None:
        return self.users.pop(user_id, None)

    def is_host(self, user_id: int) -> bool:
        return user_id == self.host_id

    # -------------------------------------------------------------- playlist

    def next_item_id(self) -> int:
        return max((i.item_id for i in self.playlist), default=0) + 1

    def add_item(self, item: PlaylistItem) -> PlaylistItem:
        """Append an item and re-derive queue order.

        A new item is appended with ``PlaylistOrder = ushort.MaxValue`` so the
        reorder pass can slot it in, then the queue is renumbered. Matches
        `RoomBase.AddPlaylistItemInternal` in the g0v0 reference, which appends
        with MaxValue and then calls `UpdatePlaylistOrder`.
        """
        item.expired = False
        item.playlist_order = 0xFFFF
        self.playlist.append(item)
        self.reorder_playlist()
        return item

    def remove_item(self, item_id: int) -> PlaylistItem | None:
        before = len(self.playlist)
        removed = next((i for i in self.playlist if i.item_id == item_id), None)

        if removed is not None:
            self.playlist.remove(removed)
            self.reorder_playlist()

        return removed if len(self.playlist) != before else None

    def reorder_playlist(self) -> None:
        """Renumber `playlist_order` from the active items, in id order.

        Expired items keep a high order so they sort after everything playable
        rather than disappearing from the client's view. Only the host-only
        queue mode is implemented; round-robin interleaving is not, so
        `QueueMode` is always reported as host-only.
        """
        active = sorted((i for i in self.playlist if not i.expired), key=lambda i: i.item_id)
        expired = sorted((i for i in self.playlist if i.expired), key=lambda i: i.item_id)

        for index, item in enumerate(active):
            item.playlist_order = index

        for index, item in enumerate(expired, start=len(active)):
            item.playlist_order = index

        self.playlist.sort(key=lambda i: (i.playlist_order, i.item_id))

    @property
    def current_item(self) -> PlaylistItem | None:
        """The item the room is on: the one settings points at, else the first
        playable item. `UpdateCurrentItem` in the reference."""
        if not self.playlist:
            return None

        for item in self.playlist:
            if item.item_id == self.settings.beatmap_id and not item.expired:
                return item

        for item in self.playlist:
            if not item.expired:
                return item

        return self.playlist[0]

    def sync_current_item(self) -> bool:
        """Point settings at the current item. Returns True if it moved.

        The client watches `Settings.PlaylistItemId`, so a change here is what
        makes the lobby highlight a different map.
        """
        item = self.current_item
        target = item.item_id if item else 0

        if target == self.settings.beatmap_id:
            return False

        self.settings.beatmap_id = target
        return True

    # -------------------------------------------------------------- rendering

    def to_client(self) -> dict[int, Any]:
        """The `MultiplayerRoom` payload the client binds to.

        **Integer keys, in the client's declared order** -- `[Key(N)]` members,
        so the map is positional and the names are documentation only. Getting
        this wrong fails inside the client's deserialiser with no useful message,
        which is why the order is spelled out member by member.
        """
        host = self.users.get(self.host_id)

        return {
            0: self.room_id,  # RoomID
            1: _ROOM_STATE[self.state],  # State
            2: self.settings.to_client(),  # Settings
            3: [u.to_client() for u in self.users.values()],  # Users
            4: host.to_client() if host else None,  # Host
            5: self.match_state,  # MatchState
            6: [i.to_client() for i in self.playlist],  # Playlist
            7: [],  # ActiveCountdowns
            8: self.room_id,  # ChannelID
        }

    async def summary(self) -> dict[str, Any]:
        """The trimmed payload for the REST room list (`Room`, not `MultiplayerRoom`).

        The lounge list is a `GET /api/v2/rooms` REST call
        (`Rooms/GetRoomsRequest.cs`), so it does not come from the hub.

        **Field names are the contract here.** `Room` binds by literal
        `[JsonProperty]` name -- `has_password`, `max_participants`,
        `participant_count`, `recent_participants`, `playlist` -- with no case or
        name conversion, so a 200 carrying `hasPassword` instead renders a
        panel of blanks rather than erroring. Every field below is named to
        match `osu.Game/Online/Rooms/Room.cs` exactly.

        That is a *different* naming convention from the hub payloads in this
        file, which are PascalCase (`RoomID`, `Settings`) because they are
        decoded by MessagePack. Do not make these two consistent -- they are
        genuinely different wire formats binding different DTOs.
        """
        host = self.users.get(self.host_id)

        # `_public_user` is async, so this has to be async too. It was a plain
        # `def` that built the coroutines into the payload unawaited, and
        # `JSONResponse` turned them into
        # `TypeError: Object of type coroutine is not JSON serializable`
        # -- a plain-text 500 for `GET /api/v2/rooms`.
        #
        # It only bit once a room had members: empty `users` yields `[]` and
        # `None`, both of which serialise fine. That is why the empty room list
        # returned 200 and every populated one failed, which reads exactly like
        # a race and is not one.
        participants = await asyncio.gather(*(_public_user(u.user_id) for u in self.users.values()))
        host_public = await _public_user(host.user_id) if host else None

        return {
            "id": self.room_id,
            "name": self.settings.name,
            "has_password": bool(self.settings.password),
            # RoomStatus is a snake_case string enum: "idle" / "playing"
            "status": "playing" if self.state == IN_GAME else "idle",
            # RoomCategory, likewise. Only "normal" exists here.
            "category": "normal",
            "type": "head_to_head",
            "queue_mode": "host_only",
            "max_participants": self.settings.max_players,
            "participant_count": len(self.users),
            # Room exposes participants through recent_participants; there is no
            # `participants` field on it at all.
            "recent_participants": list(participants),
            "host": host_public,
            "playlist": [i.to_rest() for i in self.playlist],
            "playlist_item_stats": None,
            "difficulty_range": None,
            "current_user_score": None,
            "current_playlist_item": self.playlist[0].to_rest() if self.playlist else None,
            "channel_id": self.room_id,
            "duration": None,
            "starts_at": None,
            "ends_at": None,
            "auto_skip": False,
            "auto_start_duration": 0,
            "pinned": False,
        }

    def touch(self) -> None:
        self.created_at = time.time()


class RoomFull(Exception):
    def __init__(self, room_id: int) -> None:
        super().__init__(f"room {room_id} is full")
        self.room_id = room_id


class NotInRoom(Exception):
    pass


class NotHost(Exception):
    pass


class NoSuchRoom(Exception):
    def __init__(self, room_id: int) -> None:
        super().__init__(f"no room {room_id}")
        self.room_id = room_id


class BadPassword(Exception):
    pass
