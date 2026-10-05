"""The spectator hub: ``/signalr/spectator``.

Nine methods on ``ISpectatorServer`` (``osu.Game/Online/Spectator/
ISpectatorServer.cs``). They are accepted and acknowledged, but they do not relay
anything, so spectating does not work yet.

**Mounted anyway, on purpose.** An unmounted hub answers negotiate with ``404``,
which the client reports as an exception on every retry with an escalating
backoff, forever. Accepting the calls keeps the connector healthy and keeps the
failure mode "spectating does nothing" instead of "the socket is broken".

What relaying actually needs, for whoever picks this up:

- ``StartWatchingUser``/``EndWatchingUser`` map onto the presence fan-out
  ``metadata.py`` already has -- a spectator is just another watcher of
  ``signalr:presence:{user_id}``, but keyed on the *spectator's* connection so
  the frames are routed to them and not to the metadata client.
- ``SendFrameDataV2``/``SendFrameData`` then push ``FrameDataBundle`` to every
  spectator of that score token. The bundle is only meaningful between two
  clients on the same build, so it must be relayed verbatim, the same decision
  as relaying ``UserActivity`` (see ``metadata.py``).
- ``BeginPlaySessionV2``/``EndPlaySessionV2`` carry ``SpectatorState``, which the
  client needs to bind to the right score, so they cannot be no-ops in the final
  version even though they are here.
"""

from __future__ import annotations

from typing import Any

from app.signalr.host import mount
from app.signalr.protocol import HubConnection

__all__ = ["SpectatorHub", "hub"]

# every method on ISpectatorServer, plus the two IStatefulUserHubClient
# housekeeping calls the client subscribes to
METHODS = frozenset(
    {
        "BeginPlaySession",
        "SendFrameData",
        "EndPlaySession",
        "BeginPlaySessionV2",
        "SendFrameDataV2",
        "EndPlaySessionV2",
        "StartWatchingUser",
        "EndWatchingUser",
    }
)


class SpectatorHub:
    """Accepts the spectator contract without relaying frames."""

    name = "spectator"

    async def on_connect(self, connection: HubConnection) -> None:
        return None

    async def on_disconnect(self, connection: HubConnection) -> None:
        return None

    async def invoke(self, connection: HubConnection, target: str, args: list[Any]) -> Any:
        if target in METHODS:
            return None

        raise NotImplementedError(f"unknown spectator method: {target}")


hub = SpectatorHub()
mount(hub)
