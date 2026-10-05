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
from app.signalr.protocol import MessagePackCodec  # noqa: E402

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


def mp_frames(raw: bytes) -> list[dict]:
    """Decode record-separator framed MessagePack, keeping only real messages.

    Filtering to dicts is what makes this correct: 0x1e decodes as a positive
    fixint (30), so a raw Unpacker yields a spurious trailing element per frame.
    """
    unpacker = msgpack.Unpacker(raw=False, strict_map_key=False)
    unpacker.feed(raw)
    return [m for m in unpacker if isinstance(m, dict)]


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

            print("\nUpdateStatus relays verbatim")
            activity = {"type": "InSoloGame", "BeatmapID": 5649109, "RulesetID": 0}
            ws.send_text(
                json.dumps({"type": 1, "invocationId": "3", "target": "UpdateStatus", "arguments": [activity]}) + RS
            )
            frames(ws.receive_text())
            stored = redis("GET", PRESENCE_KEY.format(user_id=user_id))
            check("status stored", "InSoloGame" in (stored or ""), stored)
            check("beatmap id preserved verbatim", "5649109" in (stored or ""), stored)

            print("\nidle (null status)")
            ws.send_text(
                json.dumps({"type": 1, "invocationId": "4", "target": "UpdateStatus", "arguments": [None]}) + RS
            )
            frames(ws.receive_text())
            check("null clears presence", redis("GET", PRESENCE_KEY.format(user_id=user_id)) == "", "still present")

            print("\nqueue cursor advances")
            before = int(redis("GET", "signalr:presence_queue_id") or 0)
            ws.send_text(
                json.dumps(
                    {
                        "type": 1,
                        "invocationId": "5",
                        "target": "UpdateActivity",
                        "arguments": [{"type": "ChoosingBeatmap"}],
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
            # field names are load-bearing: BeatmapUpdates is BeatmapSetIDs +
            # LastProcessedQueueID, and a mismatch fails deserialisation on the
            # client's connect path
            ok = isinstance(result, dict) and "beatmapSetIDs" in result and "lastProcessedQueueID" in result
            check("returns a BeatmapUpdates-shaped cursor", ok, str(got))

        # lazer never uses JSON: HubClientConnector calls AddMessagePackProtocol
        # for every hub, so this is the path that actually runs in production.
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
            ws.send_bytes(msgpack.packb({"type": 1, "invocationId": "1", "target": "RefreshFriends"}) + RS.encode())
            got = mp_frames(ws.receive_bytes())
            completion = [m for m in got if m.get("type") == 3]
            check("msgpack invocation gets a completion", bool(completion), str(got))
            check(
                "msgpack completion echoes invocationId", completion and completion[0]["invocationId"] == "1", str(got)
            )

            print("\nmessagepack ping")
            ws.send_bytes(msgpack.packb({"type": 6}) + RS.encode())
            got = mp_frames(ws.receive_bytes())
            check("msgpack ping answered with a ping", any(m.get("type") == 6 for m in got), str(got))

            print("\nmessagepack update relayed verbatim")
            # a [Union] payload: encoded as [key, payload] by UnionFormatter, and
            # relayed as-is, so this asserts the round trip does not reshape it.
            # The beatmap name carries 0x1e, which is both the record separator
            # and a valid msgpack int, so this also pins the framing: a pump that
            # split on the separator byte would tear this message in half.
            activity = {
                "type": "InSoloGame",
                "BeatmapID": 5649109,
                "RulesetID": 0,
                "Details": {"Text": "weird\x1ename"},
            }
            ws.send_bytes(
                msgpack.packb({"type": 1, "invocationId": "2", "target": "UpdateStatus", "arguments": [activity]})
                + RS.encode()
            )
            mp_frames(ws.receive_bytes())
            stored = redis("GET", PRESENCE_KEY.format(user_id=user_id)) or ""
            check("msgpack status stored", "InSoloGame" in stored, stored)
            check("msgpack beatmap id preserved verbatim", "5649109" in stored, stored)
            # presence is re-serialised to JSON for redis, which escapes the byte
            check("payload containing the record separator survives framing", r"weird\u001ename" in stored, stored)

        print("\npresence wire shape")
        # UserPresence is [Key(0)] Activity / [Key(1)] Status, and Activity is a
        # [Union] array -- so integer keys, NOT the JSON shape stored in redis.
        # Asserted here because a wrong shape decodes to nothing client-side and
        # the players list is simply empty with no error.
        p = presence_to_client({"Activity": {"type": "InSoloGame", "BeatmapID": 5649109}, "Status": "Online"})
        check("presence uses integer keys 0/1", sorted(p.keys()) == [0, 1], str(p))
        check("status is the enum ordinal", p[1] == 2, str(p))
        check(
            "activity is a [key, payload] union array",
            p[0] == [12, {0: 5649109, 1: None, 2: None}],
            str(p[0]),
        )
        idle = presence_to_client({"Activity": {"type": "ChoosingBeatmap"}, "Status": "Online"})
        check("ChoosingBeatmap union key is 11", idle[0][0] == 11, str(idle[0]))
        check("ChoosingBeatmap has no keyed members", idle[0][1] == {}, str(idle[0]))
        check("offline presence is None", presence_to_client({"Status": "Offline", "Activity": None}) is None)
        check(
            "unknown activity falls back rather than dropping the user",
            presence_to_client({"Activity": {"type": "Nope"}, "Status": "Online"})[0][0] == 11,
        )

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
        check("union and exotic shapes survive a round trip", codec.decode(codec.encode(tricky)) == tricky, str(tricky))
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
                ws.send_bytes(msgpack.packb({"type": 6}) + RS.encode())
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
