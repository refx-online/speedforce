"""Minimal server side of the SignalR wire protocol.

There is no SignalR server implementation for FastAPI/Starlette on PyPI, and
`signalrcore` is client-only, so the pieces lazer's hub client actually speaks
are implemented here directly on top of a starlette WebSocket.

Only what is needed:

* ``POST /{hub}/negotiate``            -> connection token + available transports
* ``GET  /{hub}/id={token}`` (ws)      -> handshake, then message pump
* the JSON hub protocol (lazer
  deliberately does not use MessagePack here -- see
  ``osu.Game/Online/Metadata/OnlineMetadataClient.cs``, "intentionally not using
  MessagePack ... to correctly support derived class serialization")

Framing is the record separator ``0x1e``: every message is a JSON payload
followed by that byte. Message types are the standard hub ones.
"""

from __future__ import annotations

import asyncio
import json
import secrets
import time
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from starlette.websockets import WebSocket
from starlette.websockets import WebSocketDisconnect

# Record separator. Every SignalR message on the wire ends with this byte.
RECORD_SEPARATOR = b"\x1e"

# Hub message types (JSON hub protocol).
MSG_INVOCATION = 1
MSG_STREAM_ITEM = 2
MSG_COMPLETION = 3
MSG_STREAM_INVOCATION = 4
MSG_CANCEL_INVOCATION = 5
MSG_PING = 6
MSG_CLOSE = 7

PROTOCOL = "json"
PROTOCOL_VERSION = 1

# Connections are handed a token at negotiate time and must present it when the
# websocket opens. Tokens are short-lived and single-use.
CONNECTION_TOKEN_TTL = 60.0

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
    """

    connection_id: str
    hub: str
    user_id: int
    websocket: WebSocket
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _closed: bool = False

    async def send(self, target: str, *args: Any) -> None:
        """Push a hub method to the client (server -> client invocation)."""
        if self._closed:
            return

        payload = {"type": MSG_INVOCATION, "target": target, "arguments": list(args)}

        async with self._lock:
            if self._closed:
                return
            try:
                await self.websocket.send_text(json.dumps(payload) + "\x1e")
            except (WebSocketDisconnect, RuntimeError):
                self._closed = True

    async def complete(self, invocation_id: str, result: Any = None, error: str | None = None) -> None:
        """Reply to a client -> server invocation that expects a completion."""
        payload: dict[str, Any] = {"type": MSG_COMPLETION, "invocationId": invocation_id, "error": error}

        if error is None:
            payload["result"] = result

        async with self._lock:
            if self._closed:
                return
            try:
                await self.websocket.send_text(json.dumps(payload) + "\x1e")
            except (WebSocketDisconnect, RuntimeError):
                self._closed = True

    async def ping(self) -> None:
        """Answer a client keepalive with a keepalive.

        A Ping carries no invocationId, so replying with a Completion (even an
        empty one) is not protocol-correct and a strict client can object to it.
        """
        async with self._lock:
            if self._closed:
                return
            try:
                await self.websocket.send_text(json.dumps({"type": MSG_PING}) + "\x1e")
            except (WebSocketDisconnect, RuntimeError):
                self._closed = True

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


def negotiate_payload(token: ConnectionToken, negotiate_version: int | None) -> dict[str, Any]:
    """Build the negotiate response.

    ``negotiateVersion=1`` responses use ``connectionToken``; version 0 (the
    default) uses ``connectionId``. The client reads whichever its version
    dictates, so both are always present.
    """
    payload: dict[str, Any] = {
        "connectionId": token.connection_id,
        "connectionToken": token.connection_id,
        "availableTransports": [
            {"transport": "WebSockets", "transferFormats": ["Text", "Binary"]},
        ],
    }

    if negotiate_version is not None:
        payload["negotiateVersion"] = negotiate_version

    return payload


def handshake_response(error: str | None = None) -> str:
    """The server's reply to the client's handshake request."""
    return json.dumps({"error": error}) + "\x1e"


def is_handshake(payload: str) -> bool:
    """The handshake is the only message with no ``type`` field."""
    try:
        return "type" not in json.loads(payload)
    except json.JSONDecodeError:
        return False


def parse(payload: str) -> dict[str, Any]:
    return json.loads(payload)
