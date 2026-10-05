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


def encode_varint(value: int) -> bytes:
    """LEB128, least-significant group first.

    This is ``BinaryMessageFormatter.WriteLengthPrefix``: every byte but the last
    has the high bit set, and it encodes how many groups follow. Real hub messages
    are far larger than 127 bytes, so the multi-byte form is the common case here,
    not an edge case.
    """
    if value < 0:
        raise ValueError("varint length cannot be negative")

    out = bytearray()

    while True:
        byte = value & 0x7F
        value >>= 7

        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def read_varint(buffer: bytes) -> tuple[int, int] | None:
    """Decode a length prefix. Returns ``(value, consumed)`` or None if truncated.

    ``None`` means "not enough bytes yet", which is the normal case for a message
    arriving across several websocket frames.
    """
    value = 0
    shift = 0

    for i, byte in enumerate(buffer[:MAX_VARINT_BYTES]):
        value |= (byte & 0x7F) << shift

        if not byte & 0x80:
            return value, i + 1

        shift += 7

    return None


# ``BinaryMessageParser`` caps the prefix at 5 bytes (2GB payloads).
MAX_VARINT_BYTES = 5


# Logical hub-message field names. These are protocol-agnostic: the JSON codec
# emits them as object keys and the MessagePack codec as array positions. Keeping
# one vocabulary means the rest of the server never learns which wire format is in
# use.
FIELD_TYPE = "type"
FIELD_HEADERS = "headers"
FIELD_INVOCATION_ID = "invocationId"
FIELD_TARGET = "target"
FIELD_ARGUMENTS = "arguments"
FIELD_ERROR = "error"
FIELD_RESULT = "result"
FIELD_ALLOW_RECONNECT = "allowReconnect"
FIELD_STREAMS = "streams"

# `MessagePackHubProtocolWorker.CompletionMessage` result kinds.
RESULT_ERROR = 1
RESULT_VOID = 2
RESULT_NON_VOID = 3


class _Void:
    """Sentinel distinguishing "this method returns nothing" from "returns null"."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "VOID"


VOID = _Void()


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

    def encode_message(self, message: dict[str, Any]) -> bytes:
        """Encode a whole hub message (the envelope)."""
        raise NotImplementedError

    def decode_message(self, raw: bytes) -> dict[str, Any]:
        """Decode a whole hub message into the logical field vocabulary."""
        raise NotImplementedError

    async def send(self, websocket: WebSocket, message: dict[str, Any]) -> None:
        """Encode, frame and write one *message* for this protocol's transfer format.

        Note this is deliberately not used for the handshake, which is always
        JSON delimited by ``0x1E`` regardless of transfer format -- see
        ``send_handshake``.
        """
        frame = self.frame(message)

        if self.binary:
            await websocket.send_bytes(frame)
        else:
            await websocket.send_text(frame.decode())

    def frame(self, message: dict[str, Any]) -> bytes:
        """Encode a message *with* its transfer-format framing.

        The framing is a property of the transfer format, and the client derives
        the transfer format from the hub protocol: ``JsonHubProtocol.TransferFormat``
        is ``Text`` and ``MessagePackHubProtocol.TransferFormat`` is ``Binary``.
        They are not independently choosable -- asking lazer to speak MessagePack
        over the Text transfer format is not a configuration that exists.

        ``encode_message`` is used rather than ``encode`` because the envelope shape
        differs per protocol: JSON sends an object, MessagePack a positional array.
        """
        body = self.encode_message(message)

        if self.binary:
            return encode_varint(len(body)) + body

        return body + RECORD_SEPARATOR


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

    def encode_message(self, message: dict[str, Any]) -> bytes:
        # The JSON hub protocol's envelope is the object itself, so the logical
        # field names are already the wire keys.
        return self.encode(message)

    def decode_message(self, raw: bytes) -> dict[str, Any]:
        return self.decode(raw)


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
            raise ValueError(f"expected a payload map, got {type(value).__name__}")
        return value

    # The MessagePack envelope is a **positional array**, not a map.
    #
    # `MessagePackHubProtocolWorker.ParseMessage` opens with `ReadArrayHeader()`
    # before reading anything else, so a fixmap dies on its first byte:
    #   "Unexpected msgpack code 129 (fixmap) encountered"
    # Transcribed from that file's Write*/Create* pairs, which is the only place
    # the layouts are defined:
    #
    #   Invocation        [1, headers, invocationId, target, arguments, streams]
    #   StreamInvocation  [4, headers, invocationId, target, arguments, streams]
    #   StreamItem        [2, headers, invocationId, item]
    #   Completion        [3, headers, invocationId, resultKind(, error|result)]
    #   CancelInvocation  [5, headers, invocationId]
    #   Ping              [6]
    #   Close             [7, error, allowReconnect]
    #
    # Written as data rather than as scattered `list.index(...)` calls so a test can
    # assert the layout directly -- this is the fourth time the envelope shape has
    # been wrong, and a declarative table is what makes it checkable.
    ARRAY_LAYOUT: dict[int, tuple[str, ...]] = {
        MSG_INVOCATION: (FIELD_TYPE, FIELD_HEADERS, FIELD_INVOCATION_ID, FIELD_TARGET, FIELD_ARGUMENTS, FIELD_STREAMS),
        MSG_STREAM_INVOCATION: (
            FIELD_TYPE,
            FIELD_HEADERS,
            FIELD_INVOCATION_ID,
            FIELD_TARGET,
            FIELD_ARGUMENTS,
            FIELD_STREAMS,
        ),
        MSG_STREAM_ITEM: (FIELD_TYPE, FIELD_HEADERS, FIELD_INVOCATION_ID, FIELD_RESULT),
        MSG_CANCEL_INVOCATION: (FIELD_TYPE, FIELD_HEADERS, FIELD_INVOCATION_ID),
        MSG_CLOSE: (FIELD_TYPE, FIELD_ERROR, FIELD_ALLOW_RECONNECT),
    }

    def encode_message(self, message: dict[str, Any]) -> bytes:
        kind = int(message.get(FIELD_TYPE, 0))

        if kind == MSG_PING:
            # PingMessage is the one-element array, nothing else.
            return msgpack.packb([kind], use_bin_type=True)

        if kind == MSG_COMPLETION:
            return self._encode_completion(message)

        layout = self.ARRAY_LAYOUT.get(kind)
        if layout is None:
            raise ValueError(f"no MessagePack array layout for message type {kind}")

        return msgpack.packb([self._element(field, message) for field in layout], use_bin_type=True)

    def _encode_completion(self, message: dict[str, Any]) -> bytes:
        """Completion is variable-length: its tail depends on the result kind."""
        error = message.get(FIELD_ERROR)
        has_result = FIELD_RESULT in message

        if error:
            # ErrorResult: the array carries the message, never a result.
            return msgpack.packb(
                [
                    MSG_COMPLETION,
                    message.get(FIELD_HEADERS) or {},
                    message.get(FIELD_INVOCATION_ID),
                    RESULT_ERROR,
                    error,
                ],
                use_bin_type=True,
            )

        if has_result:
            return msgpack.packb(
                [
                    MSG_COMPLETION,
                    message.get(FIELD_HEADERS) or {},
                    message.get(FIELD_INVOCATION_ID),
                    RESULT_NON_VOID,
                    message.get(FIELD_RESULT),
                ],
                use_bin_type=True,
            )

        # VoidResult: the array stops after the kind. An extra nil here would be
        # read as an argument and misparse.
        return msgpack.packb(
            [MSG_COMPLETION, message.get(FIELD_HEADERS) or {}, message.get(FIELD_INVOCATION_ID), RESULT_VOID],
            use_bin_type=True,
        )

    def _element(self, field: str, message: dict[str, Any]) -> Any:
        """Render one logical field for its array position."""
        if field == FIELD_HEADERS:
            # PackHeaders writes an empty map, not nil, when there are none.
            return message.get(FIELD_HEADERS) or {}
        if field == FIELD_ARGUMENTS:
            return list(message.get(FIELD_ARGUMENTS) or [])
        if field == FIELD_STREAMS:
            # WriteStreamIds writes an empty array rather than omitting it.
            return list(message.get(FIELD_STREAMS) or [])
        if field == FIELD_INVOCATION_ID:
            # An empty invocation id means "non-blocking" and is written as nil.
            return message.get(field) or None
        return message.get(field)

    def decode_message(self, raw: bytes) -> dict[str, Any]:
        """Decode a positional array back into the logical field vocabulary."""
        value = msgpack.unpackb(raw, raw=False, strict_map_key=False)

        if not isinstance(value, list) or not value:
            raise ValueError(f"expected a message array, got {type(value).__name__}")

        kind = value[0]

        if kind == MSG_PING:
            return {FIELD_TYPE: MSG_PING}

        if kind == MSG_COMPLETION:
            return self._decode_completion(value)

        layout = self.ARRAY_LAYOUT.get(kind)
        if layout is None:
            raise ValueError(f"no MessagePack array layout for message type {kind}")

        return {field: value[i] for i, field in enumerate(layout) if i < len(value)}

    def _decode_completion(self, value: list[Any]) -> dict[str, Any]:
        kind = value[3] if len(value) > 3 else RESULT_VOID
        message: dict[str, Any] = {
            FIELD_TYPE: MSG_COMPLETION,
            FIELD_INVOCATION_ID: value[2] if len(value) > 2 else None,
        }

        if kind == RESULT_ERROR and len(value) > 4:
            message[FIELD_ERROR] = value[4]
        elif kind == RESULT_NON_VOID and len(value) > 4:
            message[FIELD_RESULT] = value[4]

        return message


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

    async def _write(self, message: dict[str, Any]) -> None:
        """Encode and send one hub message, framed for the negotiated format."""
        if self._closed:
            return

        async with self._lock:
            if self._closed:
                return
            try:
                await self.codec.send(self.websocket, message)
            except (WebSocketDisconnect, RuntimeError):
                self._closed = True

    async def send(self, target: str, *args: Any) -> None:
        """Push a hub method to the client (server -> client invocation)."""
        await self._write(
            {
                FIELD_TYPE: MSG_INVOCATION,
                FIELD_TARGET: target,
                FIELD_ARGUMENTS: list(args),
            }
        )

    async def complete(self, invocation_id: str, result: Any = VOID, error: str | None = None) -> None:
        """Reply to a client -> server invocation that expects a completion.

        ``result`` defaults to :data:`VOID` rather than ``None`` because the two
        are different on the wire. A void completion is ``[3, {}, id, 2]`` and
        stops there; a completion whose result happens to be null is
        ``[3, {}, id, 3, nil]``. Sending the non-void form for a void method makes
        the client read one argument too many.
        """
        message: dict[str, Any] = {
            FIELD_TYPE: MSG_COMPLETION,
            FIELD_INVOCATION_ID: invocation_id,
            FIELD_ERROR: error,
        }

        if error is None and result is not VOID:
            message[FIELD_RESULT] = result

        await self._write(message)

    async def ping(self) -> None:
        """Answer a client keepalive with a keepalive.

        A Ping carries no invocationId, so replying with a Completion (even an
        empty one) is not protocol-correct and a strict client can object to it.
        """
        await self._write({FIELD_TYPE: MSG_PING})

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
        # Both transfer formats, and **both are implemented**:
        # `Codec.frame` delimits Text with 0x1E and prefixes Binary with a VarInt
        # length, mirroring `TextMessageFormatter` / `BinaryMessageFormatter`.
        #
        # An earlier note here advertised Text only, on the reasoning that the
        # client's default request is Text. That was wrong: the transfer format is
        # *derived* from the hub protocol (`MessagePackHubProtocol.TransferFormat`
        # is Binary), so a client speaking MessagePack always takes Binary and
        # declining it makes the combination impossible:
        #   "The transport does not support the 'Binary' transfer format."
        # Worse, serving 0x1E to a Binary-framed client explains the original
        # 30s `ServerTimeout`: `BinaryMessageParser` reads `{` (0x7B = 123) as a
        # length and waits for 123 bytes that never arrive.
        "availableTransports": [
            {"transport": "WebSockets", "transferFormats": ["Text", "Binary"]},
        ],
        "transferFormat": "Binary",
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
    """Decode one unframed hub message. Raises on malformed input.

    Decodes the *envelope*, so the JSON codec yields a message object and the
    MessagePack codec yields the positional array the client's
    ``ReadArrayHeader()`` expects.
    """
    return codec.decode_message(payload)


class FrameReader:
    """Reassembles transfer-format-framed hub messages from a byte stream.

    A websocket frame does not have to align with a SignalR message, so partial
    data is buffered until a whole message is available. The framing is the codec's,
    because it follows the transfer format:

    * **Text** -- each message ends with ``0x1E``.
    * **Binary** -- each message is preceded by a VarInt byte length and has no
      terminator.

    Binary framing matters here for a second reason: ``0x1E`` is also a valid
    msgpack positive fixint (30), so splitting on it corrupts any payload that
    contains that byte. Length-prefix framing has no such hazard, which is exactly
    why the client uses it for MessagePack.
    """

    def __init__(self, codec: Codec) -> None:
        self._codec = codec
        self._buffer = b""

    def feed(self, data: bytes) -> list[bytes]:
        """Add received bytes and return every complete message now available."""
        self._buffer += data
        messages: list[bytes] = []

        while True:
            if self._codec.binary:
                header = read_varint(self._buffer)

                if header is None:
                    # truncated prefix, or more than MAX_VARINT_BYTES -- wait
                    return messages

                length, consumed = header

                if len(self._buffer) < consumed + length:
                    return messages

                messages.append(self._buffer[consumed : consumed + length])
                self._buffer = self._buffer[consumed + length :]
                continue

            separator = self._buffer.find(RECORD_SEPARATOR)

            if separator < 0:
                return messages

            messages.append(self._buffer[:separator])
            self._buffer = self._buffer[separator + 1 :]
