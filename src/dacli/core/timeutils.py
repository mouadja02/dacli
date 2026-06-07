"""One source of audit timestamps.

Every append-only ledger (routing, model-routing, self-correction, governance
audit, usage store, terminal scrollback, sandbox) stamps its records with
:func:`now_iso`. Routing them all through a single tz-aware UTC helper is what
makes audit trails *comparable* — a naive ``datetime.now()`` in one ledger and a
``datetime.now(timezone.utc)`` in another silently differ by the local UTC
offset, which corrupts any cross-ledger ordering or diff. tz-aware UTC ISO-8601
is the wire format; ``datetime.fromisoformat`` round-trips it.
"""

from __future__ import annotations

from datetime import datetime, timezone


def now_iso() -> str:
    """Return the current time as a tz-aware UTC ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()
