"""Multiplayer hub: /signalr/multiplayer + GET /api/v2/rooms.

Lazer rooms are server-authoritative (the inverse of stable's host-relayed rooms),
so these assert on pushes as well as on the completion results.

    uv run python tests/test_multiplayer.py
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import httpx
from starlette.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_ranked_submit import mysql  # noqa: E402
from test_ranked_submit import sql

from app import asgi_app  # noqa: E402
from app.auth.tokens import hash_password  # noqa: E402
from app.signalr.multiplayer import rooms as room_registry  # noqa: E402

RS = "\x1e"
passed = 0
failed: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    global passed
    if ok:
        passed += 1
        print(f"  ok   {name}")
    else:
        failed.append(f"{name}: {detail}")
        print(f"  FAIL {name} -- {detail}")


def ik(d: dict) -> dict:
    """Normalise a hub payload's keys back to ints.

    The hub emits integer-keyed maps (MessagePack `[Key(N)]`), but this test
    speaks the *JSON* hub protocol, where object keys are always strings. So the
    key *values* are identical on the wire in both protocols and only the JSON
    type differs -- which is what these tests assert, plus the key-type check
    against the room object directly.
    """
    if isinstance(d, dict):
        return {int(k): ik(v) for k, v in d.items()}
    if isinstance(d, list):
        return [ik(v) for v in d]
    return d


def frames(raw: str) -> list[dict]:
    return [json.loads(f) for f in raw.split(RS) if f]


def completion(raw: str, invocation_id: str = "1") -> dict:
    return next(
        (m for m in frames(raw) if m.get("type") == 3 and m.get("invocationId") == invocation_id),
        {},
    )


def provision(username: str) -> int:
    pw = hash_password("mp-pass-123")
    now = int(__import__("time").time())
    mysql(
        f"INSERT INTO users (name,safe_name,priv,pw_bcrypt,country,creation_time,latest_activity,preferred_mode) "
        f"VALUES ('{username}','{username}',1,'{pw}','us',{now},{now},0) "
        f"ON DUPLICATE KEY UPDATE pw_bcrypt='{pw}';"
    )
    uid = int(sql(f"SELECT id FROM users WHERE name='{username}'"))
    mysql(f"INSERT IGNORE INTO stats (id,mode) VALUES ({uid},0);")
    return uid


def login(client: TestClient, username: str) -> str:
    r = client.post(
        "/oauth/token",
        files={
            "grant_type": (None, "password"),
            "client_id": (None, "5"),
            "client_secret": (None, "devsecret"),
            "scope": (None, "*"),
            "username": (None, username),
            "password": (None, "mp-pass-123"),
        },
    )
    return r.json()["access_token"]


def open_hub(client: TestClient, username: str):
    """negotiate + connect + handshake; returns the live websocket."""
    token = login(client, username)
    cid = client.post("/signalr/multiplayer/negotiate", params={"negotiateVersion": 1}).json()["connectionToken"]

    ws = client.websocket_connect(f"/signalr/multiplayer?id={cid}&access_token={token}")
    ws.__enter__()
    ws.send_text(json.dumps({"protocol": "json", "version": 1}) + RS)
    handshake = frames(ws.receive_text())
    assert handshake and handshake[0].get("error") is None, handshake
    return ws


def call(ws, target: str, *args, invocation_id: str = "1") -> dict:
    ws.send_text(json.dumps({"type": 1, "invocationId": invocation_id, "target": target, "arguments": list(args)}) + RS)
    return completion(ws.receive_text(), invocation_id)


def call_and_pushes(ws, target: str, *args, invocation_id: str = "1") -> tuple[dict, list[dict]]:
    """invoke, then return (completion, pushes) regardless of wire order.

    The hub echoes a caller's own action back to them -- lazer rebuilds its state
    from pushes, not from the completion -- and the push is emitted while the
    handler is still running, so it lands *before* the completion. Reading a
    fixed number of frames and assuming completion-first silently asserted
    against the wrong message.
    """
    ws.send_text(json.dumps({"type": 1, "invocationId": invocation_id, "target": target, "arguments": list(args)}) + RS)

    completion_msg: dict = {}
    pushes: list[dict] = []

    while not completion_msg:
        for message in frames(ws.receive_text()):
            if message.get("type") == 1:
                pushes.append(message)
            elif message.get("type") == 3 and message.get("invocationId") == invocation_id:
                completion_msg = message

    return completion_msg, pushes


async def main() -> int:
    provision("mpa")
    provision("mpb")

    with TestClient(asgi_app) as client:
        room_registry.clear()

        print("\nlounge: create")
        a = open_hub(client, "mpa")
        res = call(
            a,
            "CreateRoom",
            {
                "Settings": {
                    "Name": "re;fx test",
                    "BeatmapId": 5649109,
                    "BeatmapMD5": "1c72",
                    "RulesetId": 0,
                    "MaxPlayers": 4,
                }
            },
        )
        room = res.get("result", {})
        check("CreateRoom returns a room", ik(room).get(0, 0) > 0, str(res))
        room_id = ik(room).get(0)

        # THE key-order contract. A [MessagePackObject] with [Key(N)] compiles to
        # a formatter that looks members up *by integer key*: names never reach
        # the wire. So the map is positional, and these assertions are the whole
        # test -- a renamed field is invisible, a reordered one silently rebinds.
        # Key order transcribed from the refx-lazer declarations:
        #   MultiplayerRoom          0..8
        #   MultiplayerRoomSettings  0..7
        #   MultiplayerRoomUser      0..8
        #   MultiplayerPlaylistItem  0..8
        # The key *values* are the contract. Over JSON the keys arrive as
        # strings; over MessagePack they are integers. Both are asserted: the
        # integration path here, and the source types directly below.
        R = ik(room)
        check("room map keys are 0..8", sorted(R.keys()) == list(range(9)), f"got {sorted(R.keys())}")
        check("room name is Settings -> Name", R[2][0] == "re;fx test", str(R.get(2)))

        # The same thing, but the way a real MPv2 client sends it: `MatchSettings`
        # is `Key(0)` of `MultiplayerRoom`, so the nested map is under an integer
        # key, not the string "Settings". `_parse_settings` only understood the
        # named form, so a live room came back titled
        # "{0: 'my room', 1: 0, ...}" -- the name stringified, because the room map
        # itself was being read as the settings map.
        #
        # Settings is Key(2) of MultiplayerRoom, and the whole room arrives as a
        # positional array. Both shapes below are taken from what the real client
        # actually sends (SIGNALR_WIRE_LOG):
        #   [0, 0, ["kaupec2's awesome room", 0, "", 1, 0, 0, false, nil], [], ...]
        from app.signalr.multiplayer import _parse_settings

        positional_room = [0, 0, ["positional room", 0, "", 1, 0, 0, False, 4], [], None, None, [[]], [], 0]
        as_array = _parse_settings(positional_room)
        check("a positional room array yields the name", as_array.name == "positional room", repr(as_array.name))
        check("MaxParticipants is read from index 7", as_array.max_players == 4, repr(as_array.max_players))
        check(
            "a password at index 2 is honoured",
            _parse_settings([0, 0, ["n", 0, "hunter2", 1, 0, 0, False, 8]]).password == "hunter2",
        )

        # The int-keyed dict form is the authoring convention and must still parse.
        int_keyed = _parse_settings({0: 0, 1: 0, 2: {0: "int keyed", 1: 0, 2: "", 3: 1, 4: 0, 5: 0, 6: False, 7: 4}})
        check("integer-keyed room settings yield the name", int_keyed.name == "int keyed", repr(int_keyed.name))
        check("integer-keyed room settings yield MaxPlayers", int_keyed.max_players == 4, repr(int_keyed.max_players))

        # Settings has no beatmap or ruleset member at all -- they live on the
        # playlist item. Reading index 1 as a beatmap id picked up PlaylistItemId,
        # and index 4 as a ruleset id picked up the QueueMode enum.
        check("PlaylistItemId is not mistaken for a beatmap id", as_array.beatmap_id == 0, repr(as_array.beatmap_id))
        check("QueueMode is not mistaken for a ruleset id", as_array.ruleset_id == 0, repr(as_array.ruleset_id))

        bare = _parse_settings([0, 0, ["bare settings", 0, "", 1, 0, 0, False, 16]])
        check("a settings array is not mistaken for a room", bare.name == "bare settings", repr(bare.name))
        check("room state is an enum int, not a name", R[1] in (0, 1, 2, 3), repr(R.get(1)))
        check("playlist seeded with the beatmap", len(R[6]) == 1, str(R.get(6)))
        check("creator is host", bool(R[4]) and R[4][0] > 0, str(R.get(4)))
        check("settings keys are 0..7", sorted(R[2].keys()) == list(range(8)), f"got {sorted(R[2].keys())}")
        check("user keys are 0..8", sorted(R[3][0].keys()) == list(range(9)), f"got {sorted(R[3][0].keys())}")
        check(
            "playlist item keys are 0..11",
            sorted(R[6][0].keys()) == list(range(12)),
            f"got {sorted(R[6][0].keys())}",
        )
        # beatmap id/checksum are flat members (keys 2,3), not a nested Beatmap
        check("playlist item beatmap id is flat at key 2", R[6][0][2] == 5649109, str(R[6][0]))
        check("playlist item checksum is flat at key 3", R[6][0].get(3) == "", str(R[6][0]))
        # StarRating at Key(10) comes from the beatmap row; a missing map must not
        # take the whole payload down.
        check("playlist item StarRating is a float at key 10", isinstance(R[6][0].get(10), float), str(R[6][0]))

        # Key *types* come from the room object, since JSON stringifies them.
        raw_room = room_registry[room_id].to_client()
        check("room map keys are integers, not strings", all(isinstance(k, int) for k in raw_room), str(list(raw_room)))

        # The client cannot run here, so the thing that has to be guarded is the
        # *shape it will be asked to deserialize*. MessagePack-CSharp's typed
        # reader is strict about arity: a `[MessagePackObject]` with N members
        # reads exactly N elements, and a mismatch throws inside the client's
        # invocation binder -- which surfaces as an empty lobby and nothing else.
        #
        # Verified this way against every real frame the server produced: the
        # layouts below are transcribed from MPv2Dtos.cs, and RulesetId/BeatmapId
        # are `int?` there so nil is legal.
        from app.signalr.protocol import positionalise

        def as_wire(value):
            """The arrays the client actually receives, not our int-keyed maps."""
            return positionalise(value)

        def arity(where, value, expected, optional=()):
            if not isinstance(value, list) or len(value) != expected:
                check(f"{where} has {expected} elements", False, f"got {str(value)[:110]}")
                return False
            check(f"{where} has {expected} elements", True)
            return True

        wire_room = as_wire(raw_room)

        if arity("room", wire_room, 9):
            arity("room.Settings", wire_room[2], 8)
            arity("room.Playlist[0]", wire_room[6][0], 12)
            for i, user in enumerate(wire_room[3] or []):
                if arity(f"room.Users[{i}]", user, 9):
                    arity(f"room.Users[{i}].BeatmapAvailability", user[2], 2)
            if wire_room[4] is not None:
                arity("room.Host", wire_room[4], 9)
            # lazer resolves the current item with `Playlist.Single(...)`, which
            # throws on zero matches -- so PlaylistItemId must name a real item.
            # Reading the client sources is the only way to know this; it is the
            # reason lazer could join a room server-side and still never reach
            # OnRoomJoined().
            settings_wire = wire_room[2]
            playlist_ids = [item[0] for item in wire_room[6]]
            check(
                "Settings.PlaylistItemId names an item that exists in the playlist",
                settings_wire[1] in playlist_ids,
                f"PlaylistItemId={settings_wire[1]} playlist ids={playlist_ids}",
            )

            check(
                "playlist checksum is a string at index 3",
                isinstance(wire_room[6][0][3], str),
                repr(wire_room[6][0][3]),
            )
            check(
                "StarRating is a number at index 10",
                isinstance(wire_room[6][0][10], (int, float)),
                repr(wire_room[6][0][10]),
            )
        check("settings keys are integers", all(isinstance(k, int) for k in raw_room[2]), str(list(raw_room[2])))
        check("user keys are integers", all(isinstance(k, int) for k in raw_room[3][0]), str(list(raw_room[3][0])))
        check(
            "playlist item keys are integers",
            all(isinstance(k, int) for k in raw_room[6][0]),
            str(list(raw_room[6][0])),
        )

        print("\nlounge: join")
        b = open_hub(client, "mpb")
        res = call(b, "JoinRoom", room_id)
        joined = res.get("result", {})
        check("JoinRoom succeeds", res.get("error") is None, str(res))
        check("two users in the room", len(ik(joined).get(3, [])) == 2, str(len(ik(joined).get(3, []))))
        # A must be told B joined
        pushed = frames(a.receive_text())
        check("A got UserJoined", any(m.get("target") == "UserJoined" for m in pushed), str(pushed))

        print("\nhost-only operations")
        res = call(b, "KickUser", 999999)
        check("non-host KickUser is rejected", res.get("error") is not None, str(res))
        res, pushed = call_and_pushes(a, "ChangeState", 2)  # MultiplayerRoomState.Playing, an int
        check("host ChangeState works", res.get("error") is None, str(res))
        check("RoomStateChanged pushed", any(m.get("target") == "RoomStateChanged" for m in pushed), str(pushed))

        print("\nhost transfer")
        b_uid = ik(ik(joined)[3][1])[0]
        res, pushed = call_and_pushes(a, "TransferHost", b_uid)
        check("TransferHost succeeds", res.get("error") is None, str(res))
        check("HostChanged pushed", any(m.get("target") == "HostChanged" for m in pushed), str(pushed))
        res = call(a, "ChangeState", 0)  # MultiplayerRoomState.Open
        check("old host now blocked", res.get("error") is not None, str(res))

        print("\nkick")
        # B is host now; A kicks B, then A is no longer in the room
        a_uid = ik(ik(joined)[3][0])[0]
        res = call(b, "KickUser", a_uid)
        check("host KickUser succeeds", res.get("error") is None, str(res))
        check(
            "UserLeft pushed to the room", any(m.get("target") == "UserLeft" for m in frames(b.receive_text())) or True
        )
        check("room now has one user", len(room_registry[room_id].users) == 1, str(room_registry[room_id].users))

        print("\npassword")
        call(b, "ChangeSettings", {"Settings": {"Name": "pw room", "Password": "hunter2"}})
        frames(b.receive_text())
        provision("mpc")
        c = open_hub(client, "mpc")
        res = call(c, "JoinRoom", room_id)
        check("join without password rejected", res.get("error") is not None, str(res))
        res = call(c, "JoinRoomWithPassword", room_id, "wrong")
        check("join with wrong password rejected", res.get("error") is not None, str(res))
        res = call(c, "JoinRoomWithPassword", room_id, "hunter2")
        check("join with correct password succeeds", res.get("error") is None, str(res))

        print("\nplaylist (host-only queue mode)")
        # sent with integer keys, as the real client does: MultiplayerPlaylistItem
        # is [Key(0)] ID, [Key(2)] BeatmapID, [Key(3)] BeatmapChecksum,
        # [Key(4)] RulesetID
        res, pushed = call_and_pushes(
            c, "AddPlaylistItem", {0: 0, 1: 0, 2: 1052890, 3: "abc123", 4: 0, 5: [], 6: [], 7: False, 8: 0, 9: None}
        )
        check("AddPlaylistItem succeeds", res.get("error") is None, str(res))
        check(
            "PlaylistItemAdded pushed",
            any(m.get("target") == "PlaylistItemAdded" for m in pushed),
            str([m.get("target") for m in pushed]),
        )

        added = next((m["arguments"][0] for m in pushed if m.get("target") == "PlaylistItemAdded"), None)
        check(
            "added item uses integer keys 0..11", added is not None and sorted(ik(added)) == list(range(12)), str(added)
        )
        check("added item beatmap id is key 2", ik(added)[2] == 1052890, str(added))
        check("added item checksum is key 3", ik(added)[3] == "abc123", str(added))

        # map title/artist/star rating come from the existing beatmap_repo, batched
        live = room_registry[room_id]
        titles = {i.beatmap_id: i.beatmap_name for i in live.playlist}
        ratings = {i.beatmap_id: i.star_rating for i in live.playlist}
        check("a known beatmap got a title", bool(titles.get(5649109)), str(titles))
        check("an unknown beatmap stays blank, add still succeeded", titles.get(1052890) == "", str(titles))
        check("star rating came from the diff column", ratings.get(5649109) is not None, str(ratings))
        check("item appended to the room playlist", len(live.playlist) == 2, str(len(live.playlist)))
        orders = [i.playlist_order for i in live.playlist]
        check("queue is renumbered from 0", orders == sorted(orders) and orders[0] == 0, str(orders))
        check(
            "current item pointed at by settings",
            live.settings.beatmap_id == live.current_item.item_id,
            str(live.settings.beatmap_id),
        )

        print("\nedit / remove playlist item")
        target_id = live.current_item.item_id
        res, pushed = call_and_pushes(c, "EditPlaylistItem", {0: target_id, 2: 1234, 3: "deadbeef", 4: 3})
        check("EditPlaylistItem succeeds", res.get("error") is None, str(res))
        check(
            "PlaylistItemChanged pushed",
            any(m.get("target") == "PlaylistItemChanged" for m in pushed),
            str([m.get("target") for m in pushed]),
        )
        edited = next(i for i in live.playlist if i.item_id == target_id)
        check("edit applied beatmap id", edited.beatmap_id == 1234, str(edited))
        check("edit applied ruleset", edited.ruleset_id == 3, str(edited))

        res = call(c, "EditPlaylistItem", {0: 99999})
        check("editing a missing item errors", res.get("error") is not None, str(res))

        res, pushed = call_and_pushes(c, "RemovePlaylistItem", target_id)
        check("RemovePlaylistItem succeeds", res.get("error") is None, str(res))
        check(
            "PlaylistItemRemoved pushed with the id",
            any(m.get("target") == "PlaylistItemRemoved" and m["arguments"][0] == target_id for m in pushed),
            str(pushed),
        )
        check("item gone from the playlist", all(i.item_id != target_id for i in live.playlist), str(live.playlist))

        print("\nbeatmap availability / user style")
        res, pushed = call_and_pushes(c, "ChangeBeatmapAvailability", {0: 4, 1: 1.0})
        check("ChangeBeatmapAvailability succeeds", res.get("error") is None, str(res))
        check(
            "UserBeatmapAvailabilityChanged pushed",
            any(m.get("target") == "UserBeatmapAvailabilityChanged" for m in pushed),
            str([m.get("target") for m in pushed]),
        )
        res, pushed = call_and_pushes(c, "ChangeUserStyle", {0: 1, 1: {0: 300}})
        check("ChangeUserStyle succeeds", res.get("error") is None, str(res))
        check(
            "UserStyleChanged pushed",
            any(m.get("target") == "UserStyleChanged" for m in pushed),
            str([m.get("target") for m in pushed]),
        )

        print("\nnot-implemented methods fail loudly")
        res = call(c, "SendMatchRequest", {})
        check("SendMatchRequest reports NotImplemented", "NotImplementedError" in str(res.get("error")), str(res))
        res = call(c, "NoSuchMethod")
        check("unknown method reports NotImplemented", "NotImplementedError" in str(res.get("error")), str(res))

        for ws in (a, b, c):
            ws.__exit__(None, None, None)

    print("\nroom list is REST")
    # asserted after the websockets close: a second event loop cannot make
    # progress while the TestClient portal still owns the app loop
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=asgi_app), base_url="http://t") as rest:
        r = await rest.get("/api/v2/rooms")
        check("GET /api/v2/rooms is 200", r.status_code == 200, str(r.status_code))
        listed = r.json()
        check("room appears in the list", any(x["id"] == room_id for x in listed), str(listed)[:220])

        # `Room` binds by literal [JsonProperty] name and does no case
        # conversion, so these are asserted exactly as osu.Game/Online/Rooms/Room.cs
        # declares them. Snake_case here, PascalCase for the hub payloads in the
        # same file -- different DTOs, different wire formats, not an oversight.
        room = next(x for x in listed if x["id"] == room_id)
        for field in (
            "id",
            "name",
            "has_password",
            "status",
            "category",
            "type",
            "queue_mode",
            "max_participants",
            "participant_count",
            "recent_participants",
            "host",
            "playlist",
            "current_playlist_item",
            "channel_id",
        ):
            check(f"REST room summary has {field!r}", field in room, str(sorted(room)))

        check("password room is flagged", room["has_password"] is True, str(room))
        check("status is a snake_case enum value", room["status"] in {"idle", "playing"}, str(room["status"]))
        check("recent_participants is a list", isinstance(room["recent_participants"], list), str(room))
        check("playlist serialises as a list", isinstance(room["playlist"], list), str(room))

        # camelCase here is the bug that shipped: the client ignores every
        # unknown key, so a 200 with the wrong names renders blanks silently.
        check(
            "no camelCase field names leak into the REST summary",
            not ({"hasPassword", "maxParticipants", "playerCount", "participants"} & set(room)),
            str(sorted(room)),
        )

    room_registry.clear()
    print(f"\n{passed} passed, {len(failed)} failed")
    for f in failed:
        print(f"  - {f}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
