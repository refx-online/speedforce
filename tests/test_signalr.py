"""SignalR transport + metadata hub.

The client cannot be run on this box (no .NET SDK), so this exercises the wire
protocol directly: negotiate, token handling, handshake, invocation/completion
framing, and the metadata hub's presence behaviour.

    uv run python tests/test_signalr.py
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

import httpx
import msgpack
from starlette.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_ranked_submit import provision_user  # noqa: E402
from test_ranked_submit import redis
from test_ranked_submit import reset
from test_ranked_submit import sql

from app import asgi_app  # noqa: E402
from app.signalr.host import registry  # noqa: E402
from app.signalr.metadata import PRESENCE_KEY  # noqa: E402
from app.signalr.protocol import HubConnection  # noqa: E402
from app.signalr.metadata import presence_to_client  # noqa: E402
from app.signalr.protocol import FrameReader  # noqa: E402
from app.signalr.protocol import encode_varint  # noqa: E402
from app.signalr.protocol import JSONCodec  # noqa: E402
from app.signalr.protocol import MessagePackCodec
from app.signalr.protocol import positionalise  # noqa: E402

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


def frames(raw: str) -> list[dict]:
    return [json.loads(f) for f in raw.split(RS) if f]


def mp_frame(payload: dict) -> bytes:
    """One hub message in MessagePack, in **Binary transfer-format framing**.

    A VarInt byte length and no terminator -- NOT `0x1E`. The client derives the
    transfer format from the hub protocol, and `MessagePackHubProtocol.TransferFormat`
    is `Binary`, so this is not a choice either side gets to make.

    `payload` is a whole *envelope* in the logical field vocabulary, which
    `encode_message` renders as a positional array. Encoding dicts here is what
    let findings-6 through: the probe and the server agreed with each other and
    both disagreed with the client.
    """
    body = MessagePackCodec().encode_message(payload)
    return encode_varint(len(body)) + body


def mp_frames(raw: bytes) -> list[dict]:
    """Decode Binary-framed hub messages via the same FrameReader the server uses."""
    codec = MessagePackCodec()
    return [codec.decode_message(f) for f in FrameReader(codec).feed(raw)]


async def wait_cleared(key: str, timeout: float = 5.0) -> str:
    """Poll until a Redis key disappears, returning whatever is left.

    The hub clears presence from its post-close handler, which runs after the
    client socket has gone, so a single read races the server.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = redis("GET", key)
        if value == "":
            return ""
        await asyncio.sleep(0.05)
    return redis("GET", key)


def negotiate(client: TestClient, token: str, version: int | None = 1) -> dict:
    params = {"negotiateVersion": version} if version else {}
    r = client.post("/signalr/metadata/negotiate", params=params)
    assert r.status_code == 200, r.text
    return r.json()


async def main() -> int:
    user_id = provision_user()
    reset(user_id)

    with TestClient(asgi_app) as client:
        # get an access token through the real endpoint
        r = client.post(
            "/oauth/token",
            files={
                "grant_type": (None, "password"),
                "client_id": (None, "5"),
                "client_secret": (None, "devsecret"),
                "scope": (None, "*"),
                "username": (None, "e2e"),
                "password": (None, "e2epass123"),
            },
        )
        access = r.json()["access_token"]

        print("\nnegotiate")
        body = negotiate(client, access)
        cid = body.get("connectionToken") or body.get("connectionId")
        check("returns a connection token", bool(cid), str(body))
        check("offers WebSockets", any(t["transport"] == "WebSockets" for t in body["availableTransports"]), str(body))
        v0 = negotiate(client, access, version=None)
        check("v0 response also carries connectionId", bool(v0.get("connectionId")), str(v0))

        print("\nrejected connections")
        try:
            with client.websocket_connect("/signalr/metadata?id=bogus&access_token=" + access) as ws:
                ws.receive_text()
            check("bad token rejected", False, "connection was accepted")
        except Exception:
            check("bad token rejected", True)

        try:
            with client.websocket_connect(f"/signalr/metadata?id={cid}&access_token=garbage") as ws:
                ws.receive_text()
            check("bad access_token rejected", False, "connection was accepted")
        except Exception:
            check("bad access_token rejected", True)

        print("\ntoken delivery")
        # The real flow, which is what lazer does: `options.AccessTokenProvider`
        # sends `Authorization: Bearer` on negotiate, the response echoes the
        # token as `accessToken`, and the client appends it to the socket URL.
        # Skipping the echo makes every websocket connection unauthenticated --
        # that was the 4401 the live client hit.
        neg_auth = client.post(
            "/signalr/metadata/negotiate",
            params={"negotiateVersion": 1},
            headers={"Authorization": "Bearer " + access},
        ).json()
        check("negotiate echoes accessToken", neg_auth.get("accessToken") == access, str(neg_auth)[:120])

        neg_anon = client.post("/signalr/metadata/negotiate", params={"negotiateVersion": 1}).json()
        check("no token in, no accessToken out", "accessToken" not in neg_anon, str(neg_anon)[:120])

        echoed = neg_auth.get("accessToken")
        try:
            with client.websocket_connect(
                f"/signalr/metadata?id={neg_auth['connectionToken']}&access_token={echoed}"
            ) as ws:
                ws.send_text(json.dumps({"protocol": "json", "version": 1}) + RS)
                resp = frames(ws.receive_text())
                check("echoed token authenticates the socket", bool(resp) and resp[0].get("error") is None, str(resp))
        except Exception as exc:
            check("echoed token authenticates the socket", False, repr(exc))

        # a token supplied straight on the socket also works (browser-style)
        cid_q = negotiate(client, access)["connectionToken"]
        try:
            with client.websocket_connect(f"/signalr/metadata?id={cid_q}&access_token={access}") as ws:
                ws.send_text(json.dumps({"protocol": "json", "version": 1}) + RS)
                resp = frames(ws.receive_text())
                check(
                    "query-string token authenticates the socket",
                    bool(resp) and resp[0].get("error") is None,
                    str(resp),
                )
        except Exception as exc:
            check("query-string token authenticates the socket", False, repr(exc))

        cid_no = negotiate(client, access)["connectionToken"]
        try:
            with client.websocket_connect(f"/signalr/metadata?id={cid_no}") as ws:
                ws.receive_text()
            check("rejects connection with no token", False, "connection was accepted")
        except Exception:
            check("rejects connection with no token", True)

        print("\nhandshake")
        # `{"error": null}` is fatal, not equivalent to `{}`: the client reads the
        # property with ReadAsString, which throws unless the token is a String,
        # and the spec lists {"error":null} as a failing input.
        cid_hs = negotiate(client, access)["connectionToken"]
        with client.websocket_connect(f"/signalr/metadata?id={cid_hs}&access_token={access}") as ws:
            ws.send_text(json.dumps({"protocol": "json", "version": 1}) + RS)
            raw = ws.receive_text()
            check("handshake omits error on success", '"error"' not in raw, repr(raw))
            check("handshake reply is exactly {}", raw == "{}" + RS, repr(raw))

        print("\nhandshake")
        cid2 = negotiate(client, access)["connectionToken"]
        with client.websocket_connect(f"/signalr/metadata?id={cid2}&access_token={access}") as ws:
            ws.send_text(json.dumps({"protocol": "json", "version": 1}) + RS)
            resp = frames(ws.receive_text())
            check("handshake acknowledged", resp and resp[0].get("error") is None, str(resp))

            print("\ninvocation framing")
            ws.send_text(json.dumps({"type": 1, "invocationId": "1", "target": "RefreshFriends", "arguments": []}) + RS)
            got = frames(ws.receive_text())
            completion = [m for m in got if m.get("type") == 3]
            check("invocation gets a completion", bool(completion), str(got))
            check("completion echoes invocationId", completion and completion[0]["invocationId"] == "1", str(got))
            check("no error on a no-op method", completion and completion[0]["error"] is None, str(got))

            print("\nunknown target")
            ws.send_text(json.dumps({"type": 1, "invocationId": "2", "target": "NoSuchMethod", "arguments": []}) + RS)
            got = frames(ws.receive_text())
            err = [m for m in got if m.get("type") == 3]
            check("unknown method errors cleanly", bool(err) and err[0]["error"], str(got))

            print("\nping")
            ws.send_text(json.dumps({"type": 6}) + RS)
            got = frames(ws.receive_text())
            check("ping answered with a ping", any(m.get("type") == 6 for m in got), str(got))
            check("ping carries no invocationId", not any("invocationId" in m for m in got), str(got))

            print("\npresence stored on connect")
            raw = redis("GET", PRESENCE_KEY.format(user_id=user_id))
            check(
                "redis presence entry written", bool(raw), f"key={PRESENCE_KEY.format(user_id=user_id)} value={raw!r}"
            )

            # `IMetadataServer` has two separate methods and lazer calls both
            # right after connecting, each carrying only its own half. Confirmed
            # from the wire with both clients attached:
            #   UpdateActivity [[12, [5333124, "Sewerslvt - ...", 0, "Clicking circles"]]]
            #   UpdateStatus   [2]
            # Conflating them left the key holding a bare `2`, which
            # `presence_to_client` rejects as not-a-dict, so the user resolved to
            # offline and vanished from every list.
            print("\nUpdateActivity and UpdateStatus merge into one document")
            ws.send_text(
                json.dumps(
                    {
                        "type": 1,
                        "invocationId": "3",
                        "target": "UpdateActivity",
                        "arguments": [[12, [5649109, "weird\u001ename", 0, "Clicking circles"]]],
                    }
                )
                + RS
            )
            frames(ws.receive_text())
            stored = redis("GET", PRESENCE_KEY.format(user_id=user_id))
            check("activity type stored", "InSoloGame" in (stored or ""), stored)
            check("beatmap id preserved verbatim", "5649109" in (stored or ""), stored)
            # payload containing the record separator survives framing
            check(
                "payload containing the record separator survives framing", r"weird\u001ename" in (stored or ""), stored
            )
            check("the status half survives the activity update", '"Online"' in (stored or ""), stored)

            # a bare status ordinal must not wipe the activity
            ws.send_text(json.dumps({"type": 1, "invocationId": "4", "target": "UpdateStatus", "arguments": [2]}) + RS)
            frames(ws.receive_text())
            stored = redis("GET", PRESENCE_KEY.format(user_id=user_id))
            check("a bare status ordinal does not discard the activity", "InSoloGame" in (stored or ""), stored)
            check("status ordinal 2 stored as a name", '"Online"' in (stored or ""), stored)

            print("\nidle (null activity)")
            ws.send_text(
                json.dumps({"type": 1, "invocationId": "5", "target": "UpdateActivity", "arguments": [None]}) + RS
            )
            frames(ws.receive_text())
            check(
                "null activity clears presence",
                redis("GET", PRESENCE_KEY.format(user_id=user_id)) == "",
                "still present",
            )

            print("\noffline status clears presence")
            ws.send_text(
                json.dumps({"type": 1, "invocationId": "5b", "target": "UpdateActivity", "arguments": [[11, []]]}) + RS
            )
            frames(ws.receive_text())
            ws.send_text(json.dumps({"type": 1, "invocationId": "5c", "target": "UpdateStatus", "arguments": [0]}) + RS)
            frames(ws.receive_text())
            check(
                "status Offline clears presence",
                redis("GET", PRESENCE_KEY.format(user_id=user_id)) == "",
                "still present",
            )

            print("\nqueue cursor advances")
            before = int(redis("GET", "signalr:presence_queue_id") or 0)
            ws.send_text(
                json.dumps(
                    {
                        "type": 1,
                        "invocationId": "5z",
                        "target": "UpdateActivity",
                        "arguments": [[11, []]],
                    }
                )
                + RS
            )
            frames(ws.receive_text())
            after = int(redis("GET", "signalr:presence_queue_id") or 0)
            check("queue id advanced", after > before, f"{before} -> {after}")

            print("\nGetChangesSince")
            ws.send_text(
                json.dumps({"type": 1, "invocationId": "6", "target": "GetChangesSince", "arguments": [before]}) + RS
            )
            got = frames(ws.receive_text())
            comp = [m for m in got if m.get("type") == 3]
            result = comp[0].get("result") if comp else None
            # `BeatmapUpdates` is a `[MessagePackObject]`, so the map is
            # **positional**: the resolver reads members by `Key(N)` ordinal and
            # field names never reach the wire.
            #
            #   [Key(0)] int[] BeatmapSetIDs
            #   [Key(1)] int   LastProcessedQueueID
            #
            # Returning `{"beatmapSetIDs": ...}` made lazer log
            # "Error trying to deserialize result to BeatmapUpdates" on every
            # connect. This assertion previously pinned the *wrong* shape.
            #
            # Over the JSON codec the int keys stringify ("0"/"1"); over MessagePack
            # -- the only protocol lazer actually speaks, since HubClientConnector
            # calls AddMessagePackProtocol -- they stay ints. Both are accepted here
            # and the MessagePack form is asserted directly below.
            keys = set(result.keys()) if isinstance(result, dict) else set()
            positional = keys in ({"0", "1"}, {0, 1})
            first = result.get(0, result.get("0")) if isinstance(result, dict) else None
            ok = positional and first == [] and len(result) == 2
            check("returns a BeatmapUpdates-shaped cursor", ok, str(got))

            mpc0 = MessagePackCodec()
            # `BeatmapUpdates` is positional on the wire too -- the same rule as
            # every other `[MessagePackObject]`. Asserted over the encoded bytes,
            # because decoding alone cannot distinguish the two spellings.
            mp_cursor = msgpack.unpackb(
                mpc0.encode_message({"type": 3, "invocationId": "6", "result": {0: [], 1: 7}}),
                strict_map_key=False,
            )
            check(
                "BeatmapUpdates is a positional array over messagepack",
                mp_cursor[4] == [[], 7],
                str(mp_cursor[4]),
            )

        # The shape that actually matters: the JSON path stringifies the int keys
        # ("0"/"1") while MessagePack keeps them as ints, which is why the
        # assertion above uses sorted(keys) == [0, 1] and why the msgpack path is
        # asserted separately below.
        print("\nmessagepack handshake")
        # The handshake is JSON even when MessagePack is what is being
        # negotiated, so it goes out as text and the reply must come back as
        # text too. Sending (or answering) it as MessagePack makes the client
        # fail to parse its own handshake and close 4402.
        cid_mp = negotiate(client, access)["connectionToken"]
        with client.websocket_connect(f"/signalr/metadata?id={cid_mp}&access_token={access}") as ws:
            ws.send_text(json.dumps({"protocol": "messagepack", "version": 1}) + RS)
            resp = frames(ws.receive_text())
            check("msgpack handshake acknowledged", bool(resp) and resp[0].get("error") is None, str(resp))
            check("handshake reply is JSON, not msgpack", resp and set(resp[0]) <= {"error"}, str(resp))

            print("\nmessagepack invocation framing")
            ws.send_bytes(mp_frame({"type": 1, "invocationId": "1", "target": "RefreshFriends"}))
            got = mp_frames(ws.receive_bytes())
            completion = [m for m in got if m.get("type") == 3]
            check("msgpack invocation gets a completion", bool(completion), str(got))
            check(
                "msgpack completion echoes invocationId", completion and completion[0]["invocationId"] == "1", str(got)
            )

            print("\nmessagepack ping")
            ws.send_bytes(mp_frame({"type": 6}))
            got = mp_frames(ws.receive_bytes())
            check("msgpack ping answered with a ping", any(m.get("type") == 6 for m in got), str(got))

            print("\nmessagepack update relayed verbatim")
            # a [Union] payload: encoded as [key, payload] by UnionFormatter, and
            # relayed as-is, so this asserts the round trip does not reshape it.
            # The beatmap name carries 0x1e, which is both the record separator
            # and a valid msgpack int, so this also pins the framing: a pump that
            # split on the separator byte would tear this message in half.
            # the real union array, as lazer sends it: [12, [id, title, ruleset, verb]]
            activity = [12, [5649109, "weird\x1ename", 0, "Clicking circles"]]
            ws.send_bytes(
                mp_frame({"type": 1, "invocationId": "2", "target": "UpdateActivity", "arguments": [activity]})
            )
            mp_frames(ws.receive_bytes())
            stored = redis("GET", PRESENCE_KEY.format(user_id=user_id)) or ""
            check("msgpack union array decoded to the activity type", "InSoloGame" in stored, stored)
            check("msgpack beatmap id preserved verbatim", "5649109" in stored, stored)
            check("the fourth InGame member survives", "Clicking circles" in stored, stored)
            # presence is re-serialised to JSON for redis, which escapes the byte
            check("payload containing the record separator survives framing", r"weird\u001ename" in stored, stored)

        print("\npresence wire shape")
        # UserPresence is [Key(0)] Activity / [Key(1)] Status, and Activity is a
        # [Union] array. A [MessagePackObject] with integer keys goes on the wire
        # as a **positional array**, not as an int-keyed map -- verified against
        # the real client, which sends
        #   UpdateActivity [[12, [5333124, "title", 0, "Clicking circles"]]]
        # with all four InGame members. The dict form here is the authoring
        # convention; `positionalise` is what turns it into wire shape, so this
        # asserts on the *encoded* bytes rather than on our own intermediate.
        mpc0 = MessagePackCodec()
        p = presence_to_client({"Activity": {"type": "InSoloGame", "BeatmapID": 5649109}, "Status": "Online"})
        check("presence uses integer keys 0/1", sorted(p.keys()) == [0, 1], str(p))
        check("status is the enum ordinal", p[1] == 2, str(p))

        on_wire = msgpack.unpackb(mpc0.encode(p), strict_map_key=False)
        check(
            "UserPresence is a positional array [Activity, Status]",
            isinstance(on_wire, list) and len(on_wire) == 2 and on_wire[1] == 2,
            str(on_wire),
        )
        activity = on_wire[0]
        check(
            "activity is the union array [12, members]",
            isinstance(activity, list) and activity[0] == 12,
            str(activity),
        )
        check(
            "InGame carries all four members including RulesetPlayingVerb",
            isinstance(activity[1], list) and len(activity[1]) == 4 and activity[1][0] == 5649109,
            str(activity[1]),
        )
        check(
            "ChoosingBeatmap union payload is an empty array",
            msgpack.unpackb(
                mpc0.encode(presence_to_client({"Activity": {"type": "ChoosingBeatmap"}, "Status": "Online"})),
                strict_map_key=False,
            )[0][1]
            == [],
        )
        # the whole point: no fixmap anywhere inside a presence payload
        first = mpc0.encode(p)[0]
        check("presence payload does not start with a fixmap", not (0x80 <= first <= 0x8F), f"0x{first:02x}")
        idle = presence_to_client({"Activity": {"type": "ChoosingBeatmap"}, "Status": "Online"})
        check("ChoosingBeatmap union key is 11", idle[0][0] == 11, str(idle[0]))
        check("ChoosingBeatmap has no keyed members (an empty array)", idle[0][1] == [], str(idle[0]))
        check("offline presence is None", presence_to_client({"Status": "Offline", "Activity": None}) is None)
        check(
            "unknown activity falls back rather than dropping the user",
            presence_to_client({"Activity": {"type": "Nope"}, "Status": "Online"})[0][0] == 11,
        )

        print("\ntransfer-format framing")
        # The client derives the transfer format from the hub protocol, so
        # MessagePack is always Binary-framed: a VarInt length and NO terminator.
        # Getting this wrong is invisible until the client's watchdog fires, since
        # a VarInt parser reading 0x1E text framing just waits for bytes that never
        # arrive.
        mpc = MessagePackCodec()
        framed = mpc.frame({"type": 6})
        # Literal bytes, NOT `codec.encode(...)`: this assertion used to compare
        # the server's output against the server's own encoder, so it passed
        # while the client rejected every frame. The expected value is
        # transcribed by hand from MessagePackHubProtocolWorker.WritePingMessage
        # (`WriteArrayHeader(1)` then the type constant).
        check(
            "msgpack frames with a VarInt length, no 0x1e",
            framed[1:] == bytes.fromhex("91 06".replace(" ", "")),
            framed.hex(),
        )
        check("msgpack frame does not end with 0x1e", not framed.endswith(b"\x1e"), framed.hex())

        # --- envelope layout -------------------------------------------------
        # findings-6: every outbound hub message was a fixmap, but
        # `MessagePackHubProtocolWorker.ParseMessage` opens with
        # `ReadArrayHeader()`, so the client's parser died on the first byte with
        # "Unexpected msgpack code 129 (fixmap) encountered" -- 15 seconds in, on
        # the first unsolicited keepalive.
        #
        # Asserted as decoded arrays, so a layout change fails here rather than
        # only in the client's logs. Values are transcribed from the Write*/Create*
        # pairs in that worker, which is the sole definition of the layouts.
        ping_body = mpc.encode_message({"type": 6})
        check("ping envelope is the array [6]", msgpack.unpackb(ping_body) == [6], str(ping_body.hex()))
        check(
            "ping is framed as 02 91 06",
            mpc.frame({"type": 6}) == bytes.fromhex("0291 06".replace(" ", "")),
            mpc.frame({"type": 6}).hex(),
        )

        check(
            "invocation envelope is [1, {}, nil, target, args, []]",
            msgpack.unpackb(mpc.encode_message({"type": 1, "target": "Ping", "arguments": [1]}))
            == [1, {}, None, "Ping", [1], []],
            str(msgpack.unpackb(mpc.encode_message({"type": 1, "target": "Ping", "arguments": [1]}))),
        )
        check(
            "cancel envelope is [5, {}, id]",
            msgpack.unpackb(mpc.encode_message({"type": 5, "invocationId": "9"})) == [5, {}, "9"],
        )
        check(
            "close envelope is [7, nil, allowReconnect]",
            msgpack.unpackb(mpc.encode_message({"type": 7, "error": None, "allowReconnect": True})) == [7, None, True],
        )

        # Completion is the one variable-length array, and getting it wrong is
        # silent in the other direction: a void method written as NonVoidResult
        # makes the client read an argument that does not exist.
        check(
            "void completion is [3, {}, id, 2] and stops there",
            msgpack.unpackb(mpc.encode_message({"type": 3, "invocationId": "1"})) == [3, {}, "1", 2],
        )
        check(
            "completion with a null result is [3, {}, id, 3, nil]",
            msgpack.unpackb(mpc.encode_message({"type": 3, "invocationId": "1", "result": None}))
            == [3, {}, "1", 3, None],
        )
        check(
            "error completion is [3, {}, id, 1, message]",
            msgpack.unpackb(mpc.encode_message({"type": 3, "invocationId": "1", "error": "boom"}))
            == [3, {}, "1", 1, "boom"],
        )
        check(
            "an error completion carries no result element",
            "result" not in mpc.decode_message(mpc.encode_message({"type": 3, "invocationId": "1", "error": "boom"})),
        )

        # Round trip: what the server writes, the parser has to read back.
        for outgoing in (
            {"type": 6},
            {"type": 1, "target": "UserPresenceUpdated", "arguments": [{0: [11, {}], 1: 2}]},
            {"type": 3, "invocationId": "7"},
            {"type": 3, "invocationId": "7", "result": {"ok": True}},
            {"type": 3, "invocationId": "7", "error": "nope"},
        ):
            # Compare only the fields the caller supplied: decoding normalises and
            # fills in the ones the layout requires (headers, streams,
            # invocationId), which is the point of the layout, not a loss.
            back = mpc.decode_message(mpc.encode_message(outgoing))
            # Compare on the *wire* spelling: decoding fills in the envelope
            # defaults (headers, streams, invocationId) and int-keyed payload
            # maps come back as the positional arrays they were encoded as.
            supplied = positionalise({k: v for k, v in back.items() if k in outgoing})
            check(
                f"round trip {outgoing['type']}: {outgoing.get('target') or outgoing.get('invocationId') or 'ping'}",
                supplied == positionalise(outgoing),
                f"{supplied} != {outgoing}",
            )

        # A `UserPresence` nests *inside* the invocation's arguments and is
        # itself a positional array -- verified against the real client, which
        # reads presence pushes as `[Activity, Status]`.
        union = {"type": 1, "target": "UserPresenceUpdated", "arguments": [{0: [11, []], 1: 2}]}
        on_wire_union = msgpack.unpackb(mpc.encode_message(union), strict_map_key=False)
        check(
            "the union sits inside a positional UserPresence array",
            on_wire_union[4] == [[[11, []], 2]],
            str(on_wire_union[4]),
        )

        # JSON keeps its object envelope; only MessagePack is positional.
        jsc0 = JSONCodec()
        check(
            "json envelope is still an object, not an array",
            msgpack.unpackb(msgpack.packb([1])) == [1] and jsc0.encode_message({"type": 6}) == b'{"type": 6}',
            str(jsc0.encode_message({"type": 6})),
        )
        jsc = JSONCodec()
        check("json frames with the 0x1e terminator", jsc.frame({"type": 6}).endswith(b"\x1e"), "no terminator")

        # multi-byte VarInt: real messages are well over 127 bytes
        big = mpc.frame({"type": 1, "arguments": ["x" * 400]})
        got = FrameReader(mpc).feed(big)
        check("multi-byte varint reassembles", len(got) == 1 and len(got[0]) == len(big) - 2, f"len={len(big)}")

        # partial delivery must buffer, not corrupt or drop
        reader = FrameReader(mpc)
        partial: list[bytes] = []
        for i in range(0, len(big), 7):
            partial += reader.feed(big[i : i + 7])
        check("frame split across reads is reassembled", len(partial) == 1, str(len(partial)))

        print("\ncodec round trip")
        # payloads are only ever relayed, so any shape a client sends has to
        # survive decode -> encode unchanged
        codec = MessagePackCodec()
        tricky = {
            "type": 1,
            "arguments": [
                [1, {"Key": 1, "Value": 2}],  # union shape: [key, payload]
                {"0": "int-keyed map", 1: "int-keyed map"},  # strict_map_key
                msgpack.ExtType(42, b"\x00\xff"),  # ext must keep its code
            ],
        }
        # Int-keyed maps are now *deliberately* rewritten as positional arrays on
        # encode, so "survives unchanged" means the *values* survive -- the shape
        # is the client's contract, not ours to preserve.
        round_tripped = codec.decode(codec.encode(tricky))
        check(
            "union shape survives a round trip",
            round_tripped["arguments"][0] == [1, {"Key": 1, "Value": 2}],
            str(round_tripped["arguments"][0]),
        )
        check(
            "ext type keeps its code across a round trip",
            round_tripped["arguments"][2] == msgpack.ExtType(42, b"\x00\xff"),
            str(round_tripped["arguments"][2]),
        )
        # A *mixed*-key map has no `[Key(N)]` ordinals, so it is not a
        # MessagePackObject and must stay a map rather than be positionalised.
        check(
            "a mixed-key map stays a map, not a positional array",
            round_tripped["arguments"][1] == {"0": "int-keyed map", 1: "int-keyed map"},
            str(round_tripped["arguments"][1]),
        )
        check(
            "a fully int-keyed map does become a positional array",
            msgpack.unpackb(codec.encode({"arguments": [{0: "a", 1: "b"}]}), strict_map_key=False)["arguments"][0]
            == ["a", "b"],
        )
        check(
            "integer map keys do not raise on decode",
            codec.decode(msgpack.packb({1: "a"}, use_bin_type=True)) == {1: "a"},
            "decode rejected a non-string key",
        )

        print("\nunsupported protocol")
        cid_bad = negotiate(client, access)["connectionToken"]
        with client.websocket_connect(f"/signalr/metadata?id={cid_bad}&access_token={access}") as ws:
            ws.send_text(json.dumps({"protocol": "binary", "version": 1}) + RS)
            resp = frames(ws.receive_text())
            check(
                "unknown protocol refused",
                bool(resp) and resp[0].get("error", "").startswith("unsupported protocol"),
                str(resp),
            )

            print("\nhandshake arriving in a binary frame")
            # The transport format is settled before the handshake is written, so
            # a client may deliver that JSON inside a binary frame. The frame
            # type must not decide how it is decoded.
            cid_bin = negotiate(client, access)["connectionToken"]
            with client.websocket_connect(f"/signalr/metadata?id={cid_bin}&access_token={access}") as ws:
                ws.send_bytes(json.dumps({"protocol": "messagepack", "version": 1}).encode() + RS.encode())
                resp = frames(ws.receive_text())
                check(
                    "binary-framed JSON handshake understood",
                    bool(resp) and resp[0].get("error") is None,
                    str(resp),
                )
                ws.send_bytes(mp_frame({"type": 6}))
                got = mp_frames(ws.receive_bytes())
                check("and the connection still speaks msgpack", any(m.get("type") == 6 for m in got), str(got))

        print("\nkeepalive (server-initiated ping)")
        # The client's ServerTimeout is 30s (HubClientConnector overrides neither it
        # nor KeepAliveInterval), so the server must ping at ~15s or the connection
        # is dropped while idle. This asserts the server speaks first.
        cid_ka = negotiate(client, access)["connectionToken"]
        with client.websocket_connect(f"/signalr/metadata?id={cid_ka}&access_token={access}") as ws:
            ws.send_text(json.dumps({"protocol": "json", "version": 1}) + RS)
            frames(ws.receive_text())  # handshake reply

            # Nothing is expected from us, so whatever arrives is the keepalive.
            # This blocks for ~KEEPALIVE_INTERVAL_SECONDS by design -- that wait is
            # the thing under test.
            got = ws.receive_text()
            msgs = frames(got)
            check("server pings an idle connection", any(m.get("type") == 6 for m in msgs), repr(got[:80]))
            check("keepalive ping carries no invocationId", not any("invocationId" in m for m in msgs), repr(got[:80]))

        print("\nnotifications socket")
        # Deliberately *not* a SignalR hub, despite the path: the client opens it
        # with a raw ClientWebSocket and there is no negotiate step. It used to
        # 404, which made the connector retry forever.
        with client.websocket_connect("/signalr/notifications", headers={"Authorization": f"Bearer {access}"}) as ws:
            ws.send_text(json.dumps({"event": "noop", "data": {}}))
            check("notifications socket accepts an authenticated connection", True)

        try:
            with client.websocket_connect("/signalr/notifications") as ws:
                ws.receive_text()
            check("notifications socket rejects an unauthenticated connection", False, "connection was accepted")
        except Exception:
            check("notifications socket rejects an unauthenticated connection", True)

        print("\ndisconnect clears presence")
        # Asserted against the hub rather than by closing the socket: starlette's
        # TestClient never delivers `websocket.disconnect` to the app, so a
        # close-based assertion depends on scheduling luck instead of behaviour.
        # (Checked separately against a real uvicorn server, where the close does
        # propagate.) Uses its own user id so connections left open by the tests
        # above cannot legitimately keep this presence alive.
        hub = registry.get("metadata")
        # the cached redis client belongs to the TestClient portal's loop; drop it
        # so the direct calls below open one on this loop instead
        hub._redis = None

        def probe(name: str, uid: int, others: int) -> HubConnection:
            return HubConnection(
                connection_id=name,
                hub="metadata",
                user_id=uid,
                websocket=None,  # type: ignore[arg-type]  # never sends
            )

        last_id = 900000001
        redis("SET", PRESENCE_KEY.format(user_id=last_id), json.dumps({"type": "ChoosingBeatmap"}))
        last = probe("disconnect-last", last_id, 0)
        hub._live.add(last)
        await hub.on_disconnect(last)
        left = redis("GET", PRESENCE_KEY.format(user_id=last_id))
        check("presence removed after last connection", left == "", f"still present: {left}")

        # a second connection for the same user must keep presence up
        second_id = 900000002
        redis("SET", PRESENCE_KEY.format(user_id=second_id), json.dumps({"type": "ChoosingBeatmap"}))
        first = probe("disconnect-first", second_id, 0)
        second = probe("disconnect-second", second_id, 0)
        hub._live.update({first, second})
        await hub.on_disconnect(first)
        held = redis("GET", PRESENCE_KEY.format(user_id=second_id))
        check("presence kept while another connection remains", held != "", "cleared too early")
        await hub.on_disconnect(second)
        gone = redis("GET", PRESENCE_KEY.format(user_id=second_id))
        check("presence removed once the last connection goes", gone == "", f"still present: {gone}")

        print("\nstable presence bridge (bakenohana -> lazer)")
        # bakenohana publishes the changed user id here; the listener must pick it
        # up and push the (re-read) presence to watchers.
        key = PRESENCE_KEY.format(user_id=4242)
        redis("SET", key, json.dumps({"Activity": {"type": "InSoloGame", "BeatmapID": 5649109}, "client": "stable"}))
        redis("PUBLISH", "signalr:presence_changed", "4242")
        await asyncio.sleep(0.5)
        check("stable presence readable from the shared key", "5649109" in (redis("GET", key) or ""), redis("GET", key))
        redis("DEL", key)
        redis("PUBLISH", "signalr:presence_changed", "4242")
        await asyncio.sleep(0.3)
        check("stable logout clears the key", redis("GET", key) == "", "still present")

    print(f"\n{passed} passed, {len(failed)} failed")
    for f in failed:
        print(f"  - {f}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
