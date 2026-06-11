"""Fast JSON serialization for the hot spill paths, with a stdlib fallback.

``orjson`` is a C-accelerated JSON serializer that is markedly faster than stdlib
``json`` on the large tables/scrollback dacli spills off-context. It is *not* a
hard dependency: when it is missing we fall back to ``json`` so a bare install
keeps working. The one wrinkle callers must respect — ``orjson.dumps`` returns
**bytes**, not ``str`` — is encapsulated here behind two explicit entry points:

* :func:`dumps` — a ``str`` (decoded), drop-in for ``json.dumps(obj, default=…)``.
* :func:`dumps_bytes` — the raw ``bytes``, for an atomic byte writer.

Only ``default`` is supported (the lone ``json.dumps`` kwarg the spill paths
use); orjson takes it as a callable just like json, so ``default=str`` works in
both backends. Other formatting kwargs (``indent``, ``sort_keys``) are
deliberately unsupported here — full-file state writers that need them keep using
:func:`dacli.core.atomicio.write_json_atomic` (stdlib json).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

try:
    import orjson

    HAVE_ORJSON = True
except ImportError:  # pragma: no cover - exercised only on a bare install
    orjson = None  # type: ignore[assignment]
    HAVE_ORJSON = False

import json


def dumps_bytes(obj: Any, *, default: Callable[[Any], Any] | None = None) -> bytes:
    """Serialize *obj* to UTF-8 JSON ``bytes`` (orjson when available)."""
    if orjson is not None:
        return orjson.dumps(obj, default=default)
    return json.dumps(obj, default=default).encode("utf-8")


def dumps(obj: Any, *, default: Callable[[Any], Any] | None = None) -> str:
    """Serialize *obj* to a JSON ``str`` (orjson when available, decoded)."""
    if orjson is not None:
        return orjson.dumps(obj, default=default).decode("utf-8")
    return json.dumps(obj, default=default)
