"""Trust-as-runtime-decision protocol (workstream 2.4).

A retrieved fact is a *hypothesis*. ``verify`` re-checks it against the **live
system** and updates the trust fields accordingly:

* confirmed  -> refresh ``last_verified``, nudge ``confidence`` toward the cap,
               mark ``VERIFIED``;
* contradicted -> append a new entry carrying the fresh truth and link
               supersession (the old entry is preserved, flagged
               ``CONTRADICTED`` / ``superseded_by``) — never a silent rewrite.

The kernel's contract (enforced in *defined* here): **before any
``risky``/``irreversible`` action, the memory it relies on MUST be
re-verified.** To keep verification cheap, only (a) facts about to back such an
action and (b) facts past their TTL are re-checked — not the whole store.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any
from collections.abc import Callable

from dacli.memory.store import (
    MemoryEntry,
    MemoryStore,
    VerificationStatus,
    MAX_CONFIDENCE,
)


@dataclass
class VerificationOutcome:
    """Result of checking one entry against the live system.

    ``exists``/``contradicts`` describe the live finding. ``content`` and
    ``scope`` carry the corrected fact when the live system disagrees.
    """
    contradicts: bool
    exists: bool = True
    content: str | None = None
    scope: dict[str, Any] | None = None
    source: str = "introspection"
    detail: str = ""


# A verifier turns an entry into a live finding. For catalog-backed facts this
# wraps the connector's introspection op (e.g. ``INFORMATION_SCHEMA`` query).
Verifier = Callable[[MemoryEntry], VerificationOutcome]

# Confidence nudge applied on a successful confirmation (capped at MAX).
CONFIRM_BUMP = 0.05


def build_catalog_verifier(describe: Callable[[MemoryEntry], dict[str, Any]]) -> Verifier:
    """Build a :data:`Verifier` from a live-introspection ``describe`` callable.

    ``describe(entry)`` inspects the live system and returns a dict like
    ``{"exists": bool, "content": str | None, "scope": dict | None,
    "source": str}``. A missing object, or content that differs from what the
    entry asserts, is treated as a contradiction.
    """
    def verifier(entry: MemoryEntry) -> VerificationOutcome:
        finding = describe(entry) or {}
        source = finding.get("source", "introspection")
        if not finding.get("exists", True):
            return VerificationOutcome(
                contradicts=True, exists=False,
                content=finding.get("content"), scope=finding.get("scope"),
                source=source, detail="object not found in live system",
            )
        new_content = finding.get("content")
        contradicts = new_content is not None and new_content != entry.content
        return VerificationOutcome(
            contradicts=contradicts, exists=True,
            content=new_content, scope=finding.get("scope"), source=source,
        )
    return verifier


def needs_reverification(
    entry: MemoryEntry,
    *,
    now: datetime | None = None,
    ttl_days: float = 7.0,
) -> bool:
    """True if the entry is stale enough to warrant a live re-check."""
    now = now or datetime.now()
    if entry.verification_status in (
        VerificationStatus.STALE.value,
        VerificationStatus.UNVERIFIED.value,
    ):
        return True
    if entry.valid_until is not None and now > entry.valid_until:
        return True
    age_days = (now - entry.last_verified).total_seconds() / 86400.0
    return age_days > ttl_days


def verify(
    entry: MemoryEntry,
    verifier: Verifier,
    *,
    store: MemoryStore | None = None,
    now: datetime | None = None,
) -> MemoryEntry:
    """Re-check ``entry`` against the live system; return the trusted entry.

    On contradiction the returned entry is a *new* fact that supersedes the old
    one. When a ``store`` is given the trust update / supersession is persisted
    (append-only); otherwise the refreshed entry is returned for the caller to
    persist.
    """
    now = now or datetime.now()
    outcome = verifier(entry)

    if not outcome.contradicts:
        # Confirmed: refresh recency + confidence in place (audited as a new
        # snapshot of the same id by the append-only store).
        refreshed = replace(
            entry,
            last_verified=now,
            verification_status=VerificationStatus.VERIFIED.value,
            confidence=min(entry.confidence + CONFIRM_BUMP, MAX_CONFIDENCE),
        )
        if store is not None:
            store.add(refreshed)
        return refreshed

    # Contradicted: the live system disagrees. Mark the old entry contradicted
    # and append a fresh, superseding entry with the live truth.
    new_entry = MemoryEntry(
        content=outcome.content if outcome.content is not None else entry.content,
        kind=entry.kind,
        scope=outcome.scope if outcome.scope is not None else entry.scope,
        source=outcome.source,
        confidence=min(MAX_CONFIDENCE, max(entry.confidence, 0.9)),
        last_verified=now,
        verification_status=VerificationStatus.VERIFIED.value,
        memory_scope=entry.memory_scope,
        tags=list(entry.tags),
        supersedes=entry.id,
    )

    if store is not None:
        # Persist the contradiction marker on the old entry first (append a new
        # snapshot of the same id), then the superseding entry.
        contradicted = replace(
            entry,
            verification_status=VerificationStatus.CONTRADICTED.value,
            superseded_by=new_entry.id,
        )
        store.add(contradicted)
        store.add(new_entry)
    else:
        entry.verification_status = VerificationStatus.CONTRADICTED.value
        entry.superseded_by = new_entry.id

    return new_entry
