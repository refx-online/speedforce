"""Hub hosting: negotiate, websocket accept, and the message pump.

A hub is registered by name with a handler object exposing the server methods the
client may invoke. This module owns the transport; the metadata hub owns the
behaviour.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from typing import Protocol

from fastapi import APIRouter
from fastapi import Query
from fastapi import Request
from fastapi import WebSocket
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocketDisconnect

from app.signalr import protocol
from app.signalr import wire
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
    wire.configure()
    registry.register(hub)

    @router.post(f"/signalr/{hub.name}/negotiate")
    async def negotiate(  # noqa: ANN202
        request: Request,
        negotiateVersion: int | None = Query(None),  # noqa: N803 - wire name
    ) -> JSONResponse:
        # The client authenticates with `Authorization: Bearer` on this POST and
        # then reads the token back out of `accessToken` to put on the websocket
        # URL -- a websocket cannot carry an Authorization header. Echoing it is
        # therefore mandatory, not a convenience.
        access_token = _bearer_token(request)
        token = protocol.ConnectionToken(
            connection_id=registry.tokens.issue(hub.name).connection_id,
            hub=hub.name,
            expires_at=0.0,
        )
        return JSONResponse(protocol.negotiate_payload(token, negotiateVersion, access_token))

    @router.websocket(f"/signalr/{hub.name}")
    async def connect(websocket: WebSocket) -> None:
        connection_id = websocket.query_params.get("id", "")
        access_token = _bearer_token(websocket)

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
        codec = await _perform_handshake(websocket)
        if codec is None:
            with contextlib.suppress(Exception):
                await websocket.close(code=4402)
            return

        # Only now is the wire protocol known, so this is the earliest point the
        # connection can be told how to encode.
        connection.codec = codec

        await hub.on_connect(connection)

        keepalive = asyncio.create_task(_keepalive(connection))

        try:
            await _pump(websocket, hub, connection)
        finally:
            keepalive.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await keepalive
            connection.close()
            with contextlib.suppress(Exception):
                await hub.on_disconnect(connection)


async def _receive_any(websocket: WebSocket, timeout: float) -> tuple[bytes, bool] | None:
    """Receive one websocket message as raw bytes, plus whether it arrived binary.

    Returns None for a non-data message (close, etc).

    Two things are deliberately preserved rather than normalised away:

    * **Bytes, not text.** A MessagePack payload is binary, and decoding it as
      UTF-8 would corrupt it before the codec ever sees it. A text frame is
      UTF-8 by definition, so encoding one back to bytes is lossless.
    * **The frame type.** It is a reliable discriminator for the hub protocol,
      because the protocol determines the transfer format -- JSON goes out as
      text, MessagePack as binary. Guessing by trying each codec in turn is not
      safe: ``{`` is both an ASCII brace and a msgpack fixmap header, so a JSON
      text frame can decode as plausible-looking msgpack garbage.
    """
    message = await asyncio.wait_for(websocket.receive(), timeout=timeout)

    if message["type"] == "websocket.disconnect":
        raise WebSocketDisconnect(message.get("code", 1000))

    if (text := message.get("text")) is not None:
        return text.encode(), False

    if (raw := message.get("bytes")) is not None:
        return raw, True

    return None


async def _perform_handshake(websocket: WebSocket) -> protocol.Codec | None:
    """Read the handshake request, answer it, and return the agreed codec.

    **The handshake is always JSON**, whatever hub protocol was requested. This
    is not a detail of this client but of the protocol: the spec states the
    handshake request and response are "always a JSON message", because the
    protocol name is what they *carry* and nothing has been negotiated yet. The
    ASP.NET client agrees -- ``HandshakeProtocol.TryParseResponseMessage`` runs a
    ``Utf8JsonReader`` over the response whatever ``HandshakeProtocols`` holds.

    So the request goes out as JSON text, the reply comes back as JSON, and only
    *afterwards* does the negotiated codec take over. Answering a MessagePack
    handshake in MessagePack makes the client fail on its own handshake
    (``'0x81' is an invalid start of a value`` -- ``0x81`` is a msgpack fixmap),
    which it reports as a handshake failure and closes with ``4402``.

    The request may still arrive in a *binary* frame, since the transport
    format is settled before the handshake is written, hence reading raw bytes.
    """
    try:
        received = await _receive_any(websocket, protocol.IDLE_TIMEOUT)
    except (asyncio.TimeoutError, WebSocketDisconnect, RuntimeError):
        return None

    if received is None:
        return None

    json_codec = protocol.JSONCodec()
    frame = received[0].rstrip(protocol.RECORD_SEPARATOR)

    if not frame:
        await protocol.send_handshake(websocket, "expected handshake")
        return None

    try:
        # JSON regardless of the frame it arrived in: a binary frame carrying
        # JSON text is still JSON, and guessing from the frame type is what
        # produced the bug this comment exists to prevent.
        request = protocol.parse(json_codec, frame)
    except Exception:  # noqa: BLE001 - any codec failure means "unreadable handshake"
        await protocol.send_handshake(websocket, "invalid handshake payload")
        return None

    requested = request.get("protocol")
    agreed = protocol.CODECS.get(str(requested)) if requested is not None else None

    if agreed is None:
        await protocol.send_handshake(websocket, f"unsupported protocol {requested!r}")
        return None

    await protocol.send_handshake(websocket)
    return agreed


async def _keepalive(connection: HubConnection) -> None:
    """Ping the client periodically so its ``ServerTimeout`` never fires.

    A SignalR server is expected to *initiate*; without this the connection is
    silent whenever the client has nothing to say, and the ASP.NET client's
    ``ServerTimeout`` (30s default) drops it:

        System.TimeoutException: Server timeout (30000,00ms) elapsed
                                  without receiving a message from the server.

    ``HubClientConnector`` sets no ``ServerTimeout`` and no ``KeepAliveInterval``,
    so both are stock defaults -- 15s keepalive against a 30s timeout, which is
    the pairing ASP.NET Core ships and the reason the interval here is 15s rather
    than something more aggressive.

    Doing this server-side also makes the question of whether *the client* pings
    moot: its watchdog is satisfied by construction either way. The Ping is a
    protocol requirement, not an optimisation.

    ``ping()`` goes through the connection's send lock, so it cannot interleave
    with a half-written frame.
    """
    while not connection._closed:
        await asyncio.sleep(protocol.KEEPALIVE_INTERVAL_SECONDS)
        await connection.ping()


async def _pump(websocket: WebSocket, hub: HubHandler, connection: HubConnection) -> None:
    """Read framed messages until the socket closes.

    Framing is entirely `FrameReader`'s job, driven by the negotiated codec's
    transfer format -- see `protocol.FrameReader` for why Binary cannot be split
    on `0x1E`. This loop only moves bytes and hands whole messages to the
    dispatcher.
    """
    reader = protocol.FrameReader(connection.codec)

    while True:
        try:
            received = await _receive_any(websocket, protocol.IDLE_TIMEOUT)
        except (asyncio.TimeoutError, WebSocketDisconnect, RuntimeError):
            return

        if received is None:
            continue

        for payload in reader.feed(received[0]):
            try:
                # decode the *envelope*: under MessagePack that is the positional
                # array the client's ReadArrayHeader() expects, so this cannot use
                # the payload-only decoder.
                message = protocol.parse(connection.codec, payload)
            except Exception as exc:  # noqa: BLE001 - a malformed message is dropped, not fatal
                # Logged because this `continue` is invisible: a frame the client
                # sent that we cannot parse is dropped in total silence, which is
                # how a client->server invocation can fail without a trace.
                wire.frame(
                    "in",
                    connection.hub,
                    connection.connection_id,
                    payload,
                    exc,
                    note="UNDECODABLE - dropped",
                )
                continue

            wire.frame(
                "in",
                connection.hub,
                connection.connection_id,
                payload,
                message,
                note=_describe_inbound(message),
            )
            await _dispatch(hub, connection, message)


def _describe_inbound(message: dict[str, Any]) -> str:
    """Name the hub method an inbound envelope invokes, for the wire log."""
    kind = message.get(protocol.FIELD_TYPE)

    if kind == protocol.MSG_INVOCATION:
        target = message.get(protocol.FIELD_TARGET)
        args = message.get(protocol.FIELD_ARGUMENTS) or []
        return f"invocation {target} argc={len(args)}"

    if kind == protocol.MSG_PING:
        return "ping"
    if kind == protocol.MSG_COMPLETION:
        return "completion"

    return ""


async def _dispatch(hub: HubHandler, connection: HubConnection, message: dict[str, Any]) -> None:
    msg_type = message.get(protocol.FIELD_TYPE)

    # keepalive: answered with a Ping, which is what the spec calls for
    if msg_type == protocol.MSG_PING:
        await connection.ping()
        return

    if msg_type == protocol.MSG_INVOCATION:
        target = str(message.get(protocol.FIELD_TARGET) or "")
        args = message.get(protocol.FIELD_ARGUMENTS) or []
        invocation_id = message.get(protocol.FIELD_INVOCATION_ID)

        try:
            result = await hub.invoke(connection, target, list(args))
        except Exception as exc:  # noqa: BLE001 - surfaced to the client as a completion error
            if invocation_id:
                # include the type: a client has to be able to tell
                # NotImplementedError from a validation failure, and str(exc)
                # alone throws that away
                await connection.complete(str(invocation_id), None, f"{type(exc).__name__}: {exc}")
            return

        if invocation_id:
            # A hub method that returns nothing is VoidResult on the wire
            # ([3, {}, id, 2]); sending NonVoidResult with a nil result
            # ([3, {}, id, 3, nil]) tells the client the method has a return
            # value it did not get. None is how every void method in these hubs
            # reports success, so it maps to VOID rather than to a null result.
            await connection.complete(str(invocation_id), protocol.VOID if result is None else result)
        return

    if msg_type == protocol.MSG_CLOSE:
        # the client is closing; the pump loop exits when the socket drops
        connection.close()
        return


def _bearer_token(request: Request | WebSocket) -> str:
    """Pull the access token off a negotiate request or hub connection.

    lazer authenticates hubs the way the ASP.NET SignalR client does:
    ``HubClientConnector`` sets ``options.AccessTokenProvider``, which sends
    ``Authorization: Bearer <jwt>`` on the negotiate POST; the response echoes it
    back as ``accessToken`` and the client appends that to the websocket URL as
    ``?access_token=``. So the header appears on negotiate and the query
    parameter on the socket -- read both, on both, since either may carry it.

    Deliberately not read from ``Sec-WebSocket-Protocol``: that is the other
    transport some clients use, but uvicorn rejects a subprotocol containing a
    space with ``HTTP 400`` before the request ever reaches the app, so it cannot
    be the mechanism here.
    """
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()

    return request.query_params.get("access_token", "")


def resolve_user(websocket: WebSocket, access_token: str) -> int | None:
    """Authenticate a hub connection from its bearer token."""
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
