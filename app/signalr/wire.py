"""Opt-in wire logging for hub frames.

Off by default. Enable with ``SIGNALR_WIRE_LOG=1``; ``SIGNALR_WIRE_LOG_MAX``
caps how many bytes of each frame are shown in hex (default 240).

Why this exists
---------------
Every shape bug so far in this protocol has hidden behind a probe that agreed
with the server. The probe decoded frames with python-msgpack, which accepts maps
*and* arrays, so a frame the real client rejects decoded cleanly and the test
passed. That happened three times: the hub envelope (fixmap where the client
calls ``ReadArrayHeader()``), the ``Status`` object-vs-enum mismatch, and the
``BeatmapUpdates`` string keys.

The only trustworthy oracle is the bytes themselves, seen from the server side
while a real client is attached. So this logs, per frame:

* the leading msgpack code *by name* -- ``fixmap`` vs ``fixarray`` is the
  difference that mattered, and ``0x81``/``0x91`` are easy to misread;
* the decoded value rendered so that maps stay ``{...}`` and arrays stay
  ``[...]`` -- Python would happily print both as similar dict/list output
  downstream, which is precisely how the discrepancy got missed;
* the raw hex, so the log can be diffed against a spec.

This is a diagnostic aid, not a parser: nothing here is used to drive behaviour,
and a failure to describe a frame must never affect the connection.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

_logger = logging.getLogger("signalr.wire")

_TRUE = {"1", "true", "yes", "on"}

# msgpack leading bytes worth naming. Anything outside this prints as its hex.
_CODES = {
    0x00: "positive fixint 0",
    0xC0: "nil",
    0xC2: "false",
    0xC3: "true",
    0xC4: "bin8",
    0xC5: "bin16",
    0xC6: "bin32",
    0xCA: "float32",
    0xCB: "float64",
    0xCC: "uint8",
    0xCD: "uint16",
    0xCE: "uint32",
    0xCF: "uint64",
    0xD0: "int8",
    0xD1: "int16",
    0xD2: "int32",
    0xD3: "int64",
    0xD9: "str8",
    0xDA: "str16",
    0xDB: "str32",
    0xDC: "array16",
    0xDD: "array32",
    0xDE: "map16",
    0xDF: "map32",
}


def enabled() -> bool:
    """Whether ``SIGNALR_WIRE_LOG`` asks for wire logging."""
    return os.getenv("SIGNALR_WIRE_LOG", "").strip().lower() in _TRUE


def configure() -> None:
    """Attach a stdout handler to the wire logger.

    Uvicorn only configures its own loggers, so a logger in the ``signalr``
    namespace propagates to the root logger and is dropped for want of a handler
    -- which would make this silently log nothing at all. Idempotent, and only
    acts when logging is actually enabled.
    """
    if not enabled() or _logger.handlers:
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(handler)
    _logger.setLevel(logging.INFO)
    # keep it off the root logger so it is not emitted twice
    _logger.propagate = False


def _max_bytes() -> int:
    try:
        return max(0, int(os.getenv("SIGNALR_WIRE_LOG_MAX", "240")))
    except ValueError:
        return 240


def code_name(raw: bytes) -> str:
    """Name the leading msgpack code of ``raw``.

    The point is to make ``fixmap`` and ``fixarray`` legible: the client rejects
    ``fixmap`` where it expects an array, and the two differ only in one bit
    (``0x81`` vs ``0x91``), which is easy to misread in a hex dump.
    """
    if not raw:
        return "empty"

    first = raw[0]

    if 0x00 <= first <= 0x7F:
        return f"positive fixint {first}"
    if 0x80 <= first <= 0x8F:
        return f"fixmap {first & 0x0F}"
    if 0x90 <= first <= 0x9F:
        return f"fixarray {first & 0x0F}"
    if 0xA0 <= first <= 0xBF:
        return f"fixstr {first & 0x1F}"

    return _CODES.get(first, f"code 0x{first:02x}")


def describe(value: Any, depth: int = 0) -> str:
    """Render a decoded value keeping maps and arrays visually distinct.

    Deliberately not ``repr``: a Python dict and a msgpack map print alike, and
    likewise a list and a msgpack array, so the distinction that decides whether
    the client can parse a frame gets lost at exactly the point of review.
    """
    if depth > 4:
        return "..."

    if value is None:
        return "nil"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, dict):
        if not value:
            return "{}"
        inner = ", ".join(f"{k!r}: {describe(v, depth + 1)}" for k, v in value.items())
        return "{" + inner + "}"
    if isinstance(value, (list, tuple)):
        if not value:
            return "[]"
        return "[" + ", ".join(describe(v, depth + 1) for v in value) + "]"
    if isinstance(value, (bytes, bytearray)):
        return f"bin[{len(value)}]"
    if isinstance(value, str) and len(value) > 60:
        return value[:60] + "..."

    return repr(value)


def _hex(raw: bytes) -> str:
    limit = _max_bytes()

    if limit == 0:
        return ""
    if len(raw) <= limit:
        return raw.hex(" ")

    return raw[:limit].hex(" ") + f" ... (+{len(raw) - limit}B)"


def frame(direction: str, hub: str, connection_id: str, raw: bytes, decoded: Any = None, note: str = "") -> None:
    """Log one hub frame.

    ``decoded`` is best-effort: a value that failed to decode is passed as the
    exception so the log shows *that* rather than pretending the frame was empty.
    """
    if not enabled():
        return

    arrow = "-->" if direction == "out" else "<--"
    head = f"{arrow} {hub}/{connection_id[:8]} {len(raw)}B {code_name(raw)}"

    if decoded is None:
        body = ""
    elif isinstance(decoded, BaseException):
        body = f" decode-failed: {type(decoded).__name__}: {decoded}"
    else:
        body = f" {describe(decoded)}"

    if note:
        body += f"  ({note})"

    _logger.info("%s%s | %s", head, body, _hex(raw))
