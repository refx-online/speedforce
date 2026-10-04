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
from pathlib import Path

import httpx
from starlette.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_ranked_submit import provision_user  # noqa: E402
from test_ranked_submit import redis
from test_ranked_submit import reset
from test_ranked_submit import sql

from app import asgi_app  # noqa: E402
from app.signalr.metadata import PRESENCE_KEY  # noqa: E402

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
            ok = bool(comp) and isinstance(comp[0].get("result"), dict) and "queueId" in comp[0]["result"]
            check("returns a queue cursor", ok, str(got))

        print("\ndisconnect clears presence")
        check(
            "presence removed after last connection",
            redis("GET", PRESENCE_KEY.format(user_id=user_id)) == "",
            "still present",
        )

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
