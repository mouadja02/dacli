"""Atomic, crash-safe state persistence — the *only* way dacli writes state files.

The problem this solves: a plain ``open(path, "w")`` / ``Path.write_text`` truncates
the target **before** writing the new bytes. A ``KeyboardInterrupt``, exception, or
power loss between truncate and completed write leaves a truncated or empty file.
For ``.dacli/dacli.json`` that silently wipes every stored credential.

The fix: write the full payload to a sibling temp file on the **same filesystem**,
``fsync`` it durably to disk, then ``os.replace`` it over the target. ``os.replace``
is atomic on both POSIX and Windows when source and destination share a filesystem,
so a reader (or a crash) ever sees either the complete old file or the complete new
one — never a half-written one. Keeping the temp file in ``path.parent`` (via
``mkstemp(dir=...)``) is what guarantees the same-filesystem precondition.

Every full-file state writer in dacli routes through :func:`write_text_atomic` /
:func:`write_json_atomic`. Append-only JSONL ledgers are a separate concern (they
``fsync`` per record at their call sites).
"""

from __future__ import annotations

import json
import os
import tempfile

from pathlib import Path
from typing import Any

__all__ = ["write_bytes_atomic", "write_json_atomic", "write_text_atomic"]


def write_bytes_atomic(path: str | Path, data: bytes) -> None:
    """Atomically write *data* (bytes) to *path*, prior file intact on failure.

    The bytes counterpart of :func:`write_text_atomic`, used by the orjson hot
    paths (``orjson.dumps`` returns bytes). Same crash-safety guarantee: write to
    a sibling temp file, ``fsync``, then ``os.replace``; on any failure before
    the replace the original *path* is untouched and no ``.tmp`` litter remains.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)  # atomic on same filesystem (Windows + POSIX)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def write_text_atomic(path: str | Path, text: str, encoding: str = "utf-8") -> None:
    """Atomically write *text* to *path*, leaving the prior file intact on failure.

    Writes to a sibling temp file, ``fsync``s it, then ``os.replace``s it over
    *path*. If anything raises before the replace, the original *path* is
    untouched and the temp file is cleaned up — no truncation, no ``.tmp`` litter.
    Parent directories are created as needed.
    """
    write_bytes_atomic(path, text.encode(encoding))


def write_json_atomic(path: str | Path, obj: Any, **kwargs: Any) -> None:
    """Atomically serialise *obj* to JSON at *path*.

    ``kwargs`` are forwarded to :func:`json.dumps` (e.g. ``indent=2``,
    ``default=str``). Crash-safety semantics are those of :func:`write_text_atomic`.
    """
    write_text_atomic(path, json.dumps(obj, **kwargs))
