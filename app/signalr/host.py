"""Hub hosting: negotiate, websocket accept, and the message pump.

A hub is registered by name with a handler object exposing the server methods the
client may invoke. This module owns the transport; the metadata hub owns the
behaviour.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any
from typing import Protocol

from fastapi import APIRouter
from fastapi import Query
from fastapi import Request
from fastapi import WebSocket
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocketDisconnect

from app.signalr import protocol
from app.signalr.protocol import HubConnection
from app.signalr.protocol import NegotiationError

router = APIRouter()


class HubHandler(Protocol):
    """What a hub must provide.

    ``on_connect``/``on_disconnect`` bracket the connection lifetime; ``invoke``
    dispatches a client -> server hub method and returns the completion result.
    """

    name: str

    async def on_connect(self, connection: HubConnection) -> None: ...

    async def on_disconnect(self, connection: HubConnection) -> None: ...

    async def invoke(self, connection: HubConnection, target: str, args: list[Any]) -> Any: ...


class HubRegistry:
    """Hubs by name, plus the connection tokens they hand out."""

    def __init__(self) -> None:
        self._hubs: dict[str, HubHandler] = {}
        self.tokens = protocol.TokenStore()

    def register(self, hub: HubHandler) -> None:
        self._hubs[hub.name] = hub

    def get(self, name: str) -> HubHandler | None:
        return self._hubs.get(name)


registry = HubRegistry()


def mount(hub: HubHandler) -> None:
    """Register a hub and its two transport endpoints.

    Called at import time from the module that implements the hub.
    """
    registry.register(hub)

    @router.post(f"/signalr/{hub.name}/negotiate")
    async def negotiate(  # noqa: ANN202
        request: Request,
        negotiateVersion: int | None = Query(None),  # noqa: N803 - wire name
    ) -> JSONResponse:
        # SignalR authenticates hubs by `access_token` on the query string,
        # because a browser websocket cannot carry an Authorization header.
        token = protocol.ConnectionToken(
            connection_id=registry.tokens.issue(hub.name).connection_id,
            hub=hub.name,
            expires_at=0.0,
        )
        return JSONResponse(protocol.negotiate_payload(token, negotiateVersion))

    @router.websocket(f"/signalr/{hub.name}")
    async def connect(websocket: WebSocket) -> None:
        connection_id = websocket.query_params.get("id", "")
        access_token = websocket.query_params.get("access_token", "")

        try:
            token = registry.tokens.consume(connection_id, hub.name)
        except NegotiationError:
            # Must accept before closing, otherwise the client sees an opaque
            # transport failure rather than a rejected handshake.
            await websocket.accept()
            await websocket.close(code=4400)
            return

        user_id = resolve_user(websocket, access_token)
        if user_id is None:
            await websocket.accept()
            await websocket.close(code=4401)
            return

        await websocket.accept()

        connection = HubConnection(
            connection_id=token.connection_id,
            hub=hub.name,
            user_id=user_id,
            websocket=websocket,
        )

        # Server-authoritative: if the handshake is missing or names a protocol
        # we do not speak, refuse before any method can be invoked.
        handshake_ok = await _perform_handshake(websocket)
        if not handshake_ok:
            with contextlib.suppress(Exception):
                await websocket.close(code=4402)
            return

        await hub.on_connect(connection)

        try:
            await _pump(websocket, hub, connection)
        finally:
            connection.close()
            with contextlib.suppress(Exception):
                await hub.on_disconnect(connection)


async def _perform_handshake(websocket: WebSocket) -> bool:
    """Read the handshake request and answer it.

    The client sends ``{"protocol":"json","version":1}`` with the record
    separator. Anything other than our protocol is refused so a MessagePack
    client fails fast and visibly rather than misparsing frames.
    """
    try:
        raw = await asyncio.wait_for(websocket.receive_text(), timeout=protocol.IDLE_TIMEOUT)
    except (asyncio.TimeoutError, WebSocketDisconnect, RuntimeError):
        return False

    frame = raw.rstrip("\x1e")
    if not frame:
        await websocket.send_text(protocol.handshake_response("expected handshake"))
        return False

    try:
        request = protocol.parse(frame)
    except json.JSONDecodeError:
        await websocket.send_text(protocol.handshake_response("invalid handshake payload"))
        return False

    if request.get("protocol") != protocol.PROTOCOL:
        await websocket.send_text(protocol.handshake_response(f"unsupported protocol {request.get('protocol')!r}"))
        return False

    await websocket.send_text(protocol.handshake_response())
    return True


async def _pump(websocket: WebSocket, hub: HubHandler, connection: HubConnection) -> None:
    """Read framed messages until the socket closes.

    Buffering matters: a websocket text frame does not have to align with a
    SignalR message, so incomplete tails are carried over to the next read.
    """
    buffer = ""

    while True:
        try:
            raw = await asyncio.wait_for(websocket.receive_text(), timeout=protocol.IDLE_TIMEOUT)
        except (asyncio.TimeoutError, WebSocketDisconnect, RuntimeError):
            return

        buffer += raw

        while "\x1e" in buffer:
            frame, _, buffer = buffer.partition("\x1e")
            if not frame:
                continue
            await _dispatch(hub, connection, frame)


async def _dispatch(hub: HubHandler, connection: HubConnection, frame: str) -> None:
    try:
        message = protocol.parse(frame)
    except json.JSONDecodeError:
        return

    msg_type = message.get("type")

    # keepalive: answered with a Ping, which is what the spec calls for
    if msg_type == protocol.MSG_PING:
        await connection.ping()
        return

    if msg_type == protocol.MSG_INVOCATION:
        target = str(message.get("target") or "")
        args = message.get("arguments") or []
        invocation_id = message.get("invocationId")

        try:
            result = await hub.invoke(connection, target, list(args))
        except Exception as exc:  # noqa: BLE001 - surfaced to the client as a completion error
            if invocation_id:
                await connection.complete(str(invocation_id), None, str(exc))
            return

        if invocation_id:
            await connection.complete(str(invocation_id), result)
        return

    if msg_type == protocol.MSG_CLOSE:
        # the client is closing; the pump loop exits when the socket drops
        connection.close()
        return


def resolve_user(websocket: WebSocket, access_token: str) -> int | None:
    """Authenticate a hub connection from its `access_token` query parameter."""
    from app.auth.tokens import decode_token

    if not access_token:
        return None

    claims = decode_token(access_token)

    if not claims or claims.get("typ") == "refresh":
        return None

    # The audience must be the hub's client id; a token minted for another
    # audience must not be usable here.
    try:
        return int(claims["osu_user_id"])
    except (KeyError, TypeError, ValueError):
        return None
