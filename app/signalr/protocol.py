"""Minimal server side of the SignalR wire protocol.

There is no SignalR server implementation for FastAPI/Starlette on PyPI, and
`signalrcore` is client-only, so the pieces lazer's hub client actually speaks
are implemented here directly on top of a starlette WebSocket.

Only what is needed:

* ``POST /{hub}/negotiate``            -> connection token + available transports
* ``GET  /{hub}/id={token}`` (ws)      -> handshake, then message pump
* both hub protocols -- JSON and MessagePack

Framing is the record separator ``0x1e`` and message types are the standard hub
ones, shared by both protocols. Only the payload encoding differs, so that is
the only thing ``Codec`` abstracts over.

lazer speaks **MessagePack**: ``HubClientConnector`` calls
``AddMessagePackProtocol(...)`` for every hub, and it sorts first in
``HandshakeProtocols``, so that is the protocol the client always negotiates.
The "intentionally not using MessagePack" comments in
``OnlineMetadataClient.cs`` / ``OnlineMultiplayerClient.cs`` are about *payload*
derived-class serialization and say nothing about the handshake protocol -- an
earlier note here drew the wrong conclusion from them and cost a debugging
round.

One asymmetry is easy to get wrong, and did: **the handshake itself is always
JSON**, under every hub protocol. The spec says the request and response are
"always a JSON message", because the protocol name is what they carry and
nothing has been negotiated yet, and the ASP.NET client parses the reply with
``Utf8JsonReader`` regardless of what it asked for. MessagePack applies only to
what comes *after*. See ``send_handshake``.
"""

from __future__ import annotations

import asyncio
import json
import secrets
import time
from dataclasses import dataclass
from dataclasses import field
from typing import Any

import msgpack
from starlette.websockets import WebSocket
from starlette.websockets import WebSocketDisconnect

# Record separator. Every SignalR message on the wire ends with this byte.
RECORD_SEPARATOR = b"\x1e"

# Hub message types. Identical under both protocols.
MSG_INVOCATION = 1
MSG_STREAM_ITEM = 2
MSG_COMPLETION = 3
MSG_STREAM_INVOCATION = 4
MSG_CANCEL_INVOCATION = 5
MSG_PING = 6
MSG_CLOSE = 7

PROTOCOL_VERSION = 1

PROTOCOL_JSON = "json"
PROTOCOL_MSGPACK = "messagepack"


class Codec:
    """Encodes hub messages for one negotiated hub protocol.

    This is not only a serialiser swap: it decides whether a frame goes out as
    a websocket *text* or *binary* frame, which is the observable difference
    between the two protocols on the wire.
    """

    name: str
    binary: bool

    def encode(self, payload: dict[str, Any]) -> bytes:
        raise NotImplementedError

    def decode(self, raw: bytes) -> dict[str, Any]:
        raise NotImplementedError

    async def send(self, websocket: WebSocket, payload: dict[str, Any]) -> None:
        """Encode, frame and write one *message* in this protocol's transfer format.

        The single place that decides text vs binary. Note this is deliberately
        not used for the handshake, which is JSON under every protocol -- see
        ``send_handshake``.
        """
        frame = self.encode(payload) + RECORD_SEPARATOR

        if self.binary:
            await websocket.send_bytes(frame)
        else:
            await websocket.send_text(frame.decode())


class JSONCodec(Codec):
    name = PROTOCOL_JSON
    binary = False

    def encode(self, payload: dict[str, Any]) -> bytes:
        return json.dumps(payload).encode()

    def decode(self, raw: bytes) -> dict[str, Any]:
        value = json.loads(raw.decode("utf-8", "replace"))
        if not isinstance(value, dict):
            raise ValueError(f"expected a message map, got {type(value).__name__}")
        return value


class MessagePackCodec(Codec):
    """MessagePack, with payload shapes preserved verbatim.

    Payloads are relayed between clients, so a payload must survive a
    decode/encode round trip *untouched* -- reinterpreting a shape we do not
    understand is worse than never parsing it. Two settings buy that:

    * ``raw=False`` leaves msgpack **ext** types as ``ExtType`` (type code plus
      payload bytes) instead of decoding them, so an ext re-encodes with its
      code intact.
    * ``strict_map_key=False`` accepts non-string map keys. The client's
      ``[MessagePackObject]`` types serialise as maps with *integer* keys, and
      ``unpackb`` rejects those by default -- which would raise on frames whose
      entire purpose is to be forwarded elsewhere.

    ``[Union]`` types (``UserActivity`` and friends) need no special handling:
    ``SignalRUnionWorkaroundResolver`` delegates to the standard resolver, and
    ``UnionFormatter`` encodes them as a plain array of ``[key, payload]``.
    """

    name = PROTOCOL_MSGPACK
    binary = True

    def encode(self, payload: dict[str, Any]) -> bytes:
        return msgpack.packb(payload, use_bin_type=True)

    def decode(self, raw: bytes) -> dict[str, Any]:
        value = msgpack.unpackb(raw, raw=False, strict_map_key=False)
        if not isinstance(value, dict):
            raise ValueError(f"expected a message map, got {type(value).__name__}")
        return value


CODECS: dict[str, Codec] = {
    PROTOCOL_JSON: JSONCodec(),
    PROTOCOL_MSGPACK: MessagePackCodec(),
}

# Offered to the client in the negotiate response, in preference order.
SUPPORTED_PROTOCOLS = (PROTOCOL_MSGPACK, PROTOCOL_JSON)

# Connections are handed a token at negotiate time and must present it when the
# websocket opens. Tokens are short-lived and single-use.
CONNECTION_TOKEN_TTL = 60.0

# How often the server pings an otherwise-idle connection. ASP.NET Core ships
# 15s against a 30s client-side `ServerTimeout`, and lazer's `HubClientConnector`
# overrides neither, so the stock pairing applies. See host.py `_keepalive`.
KEEPALIVE_INTERVAL_SECONDS = 15.0

# A hub that receives nothing from the client for this long is presumed dead.
# The client sends keepalive pings, so this only fires on a wedged socket.
IDLE_TIMEOUT = 60.0


class NegotiationError(Exception):
    """Raised when a websocket is presented without a usable connection token."""


@dataclass(slots=True)
class ConnectionToken:
    connection_id: str
    hub: str
    expires_at: float


@dataclass(slots=True, eq=False)
class HubConnection:
    """One live hub connection.

    ``eq=False`` keeps the identity-based ``__hash__``: connections are held in a
    set for watcher fan-out, and a generated ``__eq__`` would drop it.

    ``send`` is the only way to push to the client; every write goes through the
    per-connection lock so two coroutines cannot interleave halves of a frame.

    ``codec`` is whatever the client negotiated. It is not known when the
    connection is built (the handshake has not happened yet), so it defaults to
    JSON and the pump swaps in the negotiated one immediately after the
    handshake completes.
    """

    connection_id: str
    hub: str
    user_id: int
    websocket: WebSocket
    codec: Codec = field(default_factory=JSONCodec)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _closed: bool = False

    async def _write(self, payload: dict[str, Any]) -> None:
        """Encode and send one hub message, framed with the record separator."""
        if self._closed:
            return

        async with self._lock:
            if self._closed:
                return
            try:
                await self.codec.send(self.websocket, payload)
            except (WebSocketDisconnect, RuntimeError):
                self._closed = True

    async def send(self, target: str, *args: Any) -> None:
        """Push a hub method to the client (server -> client invocation)."""
        await self._write({"type": MSG_INVOCATION, "target": target, "arguments": list(args)})

    async def complete(self, invocation_id: str, result: Any = None, error: str | None = None) -> None:
        """Reply to a client -> server invocation that expects a completion."""
        payload: dict[str, Any] = {"type": MSG_COMPLETION, "invocationId": invocation_id, "error": error}

        if error is None:
            payload["result"] = result

        await self._write(payload)

    async def ping(self) -> None:
        """Answer a client keepalive with a keepalive.

        A Ping carries no invocationId, so replying with a Completion (even an
        empty one) is not protocol-correct and a strict client can object to it.
        """
        await self._write({"type": MSG_PING})

    def close(self) -> None:
        self._closed = True


class TokenStore:
    """Short-lived connection tokens issued by ``/negotiate``.

    A dict with lazy expiry rather than a real cache: tokens live for a minute,
    are consumed on first use, and there are only ever as many as there are
    connecting clients.
    """

    def __init__(self) -> None:
        self._tokens: dict[str, ConnectionToken] = {}

    def issue(self, hub: str) -> ConnectionToken:
        self._reap()

        token = ConnectionToken(
            connection_id=secrets.token_urlsafe(16),
            hub=hub,
            expires_at=time.monotonic() + CONNECTION_TOKEN_TTL,
        )
        self._tokens[token.connection_id] = token
        return token

    def consume(self, connection_id: str, hub: str) -> ConnectionToken:
        """Redeem a token. Single-use: a replayed token is rejected."""
        self._reap()

        token = self._tokens.pop(connection_id, None)

        if token is None:
            raise NegotiationError("unknown or already-used connection token")

        if token.hub != hub:
            raise NegotiationError("connection token was issued for a different hub")

        return token

    def _reap(self) -> None:
        now = time.monotonic()
        for cid in [c for c, t in self._tokens.items() if t.expires_at <= now]:
            del self._tokens[cid]


def negotiate_payload(
    token: ConnectionToken,
    negotiate_version: int | None,
    access_token: str = "",
) -> dict[str, Any]:
    """Build the negotiate response.

    ``negotiateVersion=1`` responses use ``connectionToken``; version 0 (the
    default) uses ``connectionId``. The client reads whichever its version
    dictates, so both are always present.

    ``access_token`` is echoed back only when the negotiate request carried one.
    A browser websocket cannot set an ``Authorization`` header, so this is how
    the token reaches the socket: the client sends it on negotiate, reads it back
    out of this field, and appends it to the websocket URL. Omitting it makes
    every websocket connection unauthenticated.
    """
    payload: dict[str, Any] = {
        "connectionId": token.connection_id,
        "connectionToken": token.connection_id,
        # **Text only, deliberately.**
        #
        # ASP.NET SignalR has two framings: the Text transfer format delimits
        # messages with the 0x1E record separator (`TextMessageParser`), while the
        # Binary transfer format uses a VarInt length prefix instead
        # (`BinaryMessageParser` -- no 0x1E at all). We implement the Text framing
        # only, and `Codec.send` terminates every frame with 0x1E to match it.
        #
        # Advertising "Binary" here would let a client negotiate the length-prefix
        # framing, which we do not implement, and it would then mis-parse every
        # message. The client picks the *first* supported format it wants, and its
        # default request is Text, so this is a no-op for lazer -- it just makes the
        # one framing we support the only one on offer.
        "availableTransports": [
            {"transport": "WebSockets", "transferFormats": ["Text"]},
        ],
        "supportedProtocols": list(SUPPORTED_PROTOCOLS),
    }

    if negotiate_version is not None:
        payload["negotiateVersion"] = negotiate_version

    if access_token:
        payload["accessToken"] = access_token

    return payload


async def send_handshake(websocket: WebSocket, error: str | None = None) -> None:
    """The server's reply to the client's handshake request.

    Always JSON, and always a *text* frame. The hub protocol is what the
    handshake negotiates, so it cannot also be what encodes it: the client
    parses this reply with ``Utf8JsonReader`` regardless of what it asked for.

    **On success the ``error`` key is omitted entirely**, giving ``{}``.

    ``{"error": null}`` is *not* equivalent and it is fatal. The client reads the
    property with ``ReadAsString``, which throws unless the JSON token is a
    String -- there is no null special case:

        error = reader.ReadAsString(ErrorPropertyName);

    and the SignalR spec's own test data lists ``{"error":null}`` as a failing
    input ("Expected 'error' to be of type String"), with ``{}`` as the passing
    success case. An earlier version here always wrote the key, so every lazer
    handshake failed with ``InvalidDataException`` and all three hubs stayed
    disconnected while a synthetic Python probe cheerfully reported success --
    ``json.loads`` maps ``null`` to ``None``, so the probe asserted against its
    own encoder rather than against the bytes a real client rejects.
    """
    payload = {"error": error} if error else {}
    frame = json.dumps(payload).encode() + RECORD_SEPARATOR
    await websocket.send_text(frame.decode())


def parse(codec: Codec, payload: bytes) -> dict[str, Any]:
    """Decode one unframed hub message. Raises on malformed input."""
    return codec.decode(payload)
