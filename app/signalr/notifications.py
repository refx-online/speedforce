"""The notifications websocket: ``/signalr/notifications``.

Not a SignalR hub, despite living under ``/signalr/``. ``/api/v2/notifications``
hands the client this URL in ``notification_endpoint`` and it opens it with a
plain ``ClientWebSocket``, so there is no negotiate step, no connection token
and no hub protocol here -- just a raw socket that must stay open.

That distinction is the whole reason this file exists. ``GET
/api/v2/notifications`` is a SignalR-negotiate-shaped REST call whose *response*
points at a socket, and ``WebSocketNotificationsClientConnector`` then does
``req.Response!.Endpoint`` with nothing in the way: a 404 on either one leaves
the connector throwing, on every retry, forever.

Current scope is deliberately **hold the socket open and send nothing**. The
connector only settles once the connection is up, and login does not finish
until then. Actual notification *delivery* -- score events, beatmapset
favourites, chat -- is not implemented, so nothing is ever pushed down this
socket and no player receives a notification.
"""

from __future__ import annotations

from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

from app.signalr.host import _bearer_token
from app.signalr.host import resolve_user
from app.signalr.host import router


@router.websocket("/signalr/notifications")
async def notifications_socket(websocket: WebSocket) -> None:
    """Authenticate, then hold the connection open until the client leaves.

    Text-only in principle: ``WebSocketNotificationsClient`` throws
    ``NotImplementedException`` on a binary frame, so nothing binary is ever
    sent. Nothing is sent at all today.

    Auth is by the ``Authorization`` header, which is what
    ``WebSocketNotificationsClientConnector`` sets -- and it is the only option
    here. The SignalR hubs can echo the token back out of their negotiate
    response into the socket URL, because a negotiate round trip exists to carry
    it; a raw ``ClientWebSocket`` has no such step.
    """
    user_id = resolve_user(websocket, _bearer_token(websocket))

    if user_id is None:
        # Accept before closing, or the client sees an opaque transport failure
        # instead of a rejected connection.
        await websocket.accept()
        await websocket.close(code=4401)
        return

    await websocket.accept()

    try:
        # Read to the close rather than just sleeping: draining frames is what
        # notices the client going away, and the client is free to send.
        while True:
            message = await websocket.receive()
            if message["type"] == "websocket.disconnect":
                return
    except (WebSocketDisconnect, RuntimeError):
        return
