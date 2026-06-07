"""Typed, trust-aware memory store (workstream 2.1).

The antidote to the *stale-but-confident* failure mode: every fact carries
confidence, recency, and provenance, and **trust is decided at retrieval time,
not stored as truth** (see ``memory/retrieval.py`` and ``memory/verify.py``).

Persistence is an **append-only JSONL event log**: a correction never rewrites a
line, it appends a new entry that links back via ``supersedes`` (and the
superseded entry is flagged ``superseded_by`` on replay). The current state is
the result of replaying the log — full history is preserved for 's audit
ledger for free.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class MemoryKind(str, Enum):
    SEMANTIC = "semantic"        # durable facts: configs, conventions, constraints
    EPISODIC = "episodic"        # task traces: "how I built the Bronze layer"
    PROCEDURAL = "procedural" # distilled runbooks (promotes episodes)


class MemoryScope(str, Enum):
    USER = "user"        # persists across all sessions for this user
    PROJECT = "project"  # persists for this project/codebase
    SESSION = "session"  # volatile — tied to a single session


class VerificationStatus(str, Enum):
    UNVERIFIED = "unverified"      # never checked against the live system
    VERIFIED = "verified"          # recently confirmed against the live system
    STALE = "stale"                # due for re-verification
    CONTRADICTED = "contradicted"  # live system contradicts this entry


# ---------------------------------------------------------------------------
# Confidence priors (computed, not vibes — §4)
# ---------------------------------------------------------------------------
# ``source`` drives a prior. Introspection of a live system is the most
# trustworthy; a user assertion slightly less; pure inference least. Verification
# bumps toward the cap; age decays it at retrieval time (never stored as decayed).
CONFIDENCE_PRIORS = {
    "information_schema": 0.95,  # any ``*.information_schema`` introspection source
    "introspection": 0.95,
    "user": 0.90,
    "inference": 0.50,
}
# Anti-pattern guard: reserve 1.0 for human-verified facts only.
MAX_CONFIDENCE = 0.95
DEFAULT_CONFIDENCE = 0.60


def confidence_for_source(source: str) -> float:
    """Map a provenance ``source`` string to a confidence prior."""
    if not source:
        return DEFAULT_CONFIDENCE
    s = source.lower()
    for key, value in CONFIDENCE_PRIORS.items():
        if key in s:
            return value
    return DEFAULT_CONFIDENCE


# ---------------------------------------------------------------------------
# MemoryEntry
# ---------------------------------------------------------------------------
def _now() -> datetime:
    return datetime.now()


def _new_id() -> str:
    return uuid.uuid4().hex


@dataclass
class MemoryEntry:
    """A single trust-aware fact.

    The trust axes (``confidence``, ``last_verified``, ``valid_until``) are
    *metadata from which trust is derived at retrieval time* — never a stored
    ``trust`` verdict (see ``trustworthy-memory`` reference).
    """

    content: str
    kind: str = MemoryKind.SEMANTIC.value
    scope: dict[str, Any] = field(default_factory=dict)

    # Trust axes
    confidence: float = DEFAULT_CONFIDENCE
    last_verified: datetime = field(default_factory=_now)
    valid_until: datetime | None = None
    verification_status: str = VerificationStatus.UNVERIFIED.value

    # Provenance
    source: str = "inference"
    memory_scope: str = MemoryScope.PROJECT.value

    # Durability / supersession (append-only links, never silent rewrites)
    supersedes: str | None = None     # id of the entry this one replaces
    superseded_by: str | None = None  # set on replay when something supersedes us

    # Identity / housekeeping
    id: str = field(default_factory=_new_id)
    created_at: datetime = field(default_factory=_now)
    tags: list[str] = field(default_factory=list)

    @property
    def is_active(self) -> bool:
        """An entry is active unless it has been superseded or contradicted."""
        return (
            self.superseded_by is None
            and self.verification_status != VerificationStatus.CONTRADICTED.value
        )

    def to_record(self) -> dict[str, Any]:
        """Serialize for the JSONL log (datetimes -> ISO strings)."""
        data = asdict(self)
        data["last_verified"] = self.last_verified.isoformat()
        data["created_at"] = self.created_at.isoformat()
        data["valid_until"] = self.valid_until.isoformat() if self.valid_until else None
        return data

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> MemoryEntry:
        record = dict(record)
        record["last_verified"] = datetime.fromisoformat(record["last_verified"])
        record["created_at"] = datetime.fromisoformat(record["created_at"])
        vu = record.get("valid_until")
        record["valid_until"] = datetime.fromisoformat(vu) if vu else None
        # Tolerate logs written by older schemas: drop unknown keys.
        known = cls.__dataclass_fields__.keys()
        return cls(**{k: v for k, v in record.items() if k in known})


# ---------------------------------------------------------------------------
# MemoryStore
# ---------------------------------------------------------------------------
class MemoryStore:
    """Append-only, event-sourced store of :class:`MemoryEntry`.

    Every ``add`` / ``supersede`` / ``verify`` appends one JSONL line. The
    in-memory view is the replay of the log, latest snapshot per ``id`` winning,
    with supersession links resolved. Nothing is ever deleted or overwritten.
    """

    def __init__(self, path: str = ".dacli/memory/store.jsonl"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # id -> latest snapshot
        self._entries: dict[str, MemoryEntry] = {}
        self._load()

    # -- persistence --------------------------------------------------------
    def _load(self) -> None:
        if not self.path.exists():
            return
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = MemoryEntry.from_record(json.loads(line))
                except Exception:
                    continue  # skip a corrupt line rather than fail the session
                self._entries[entry.id] = entry  # last write per id wins
        self._resolve_supersession()

    def _append(self, entry: MemoryEntry) -> None:
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry.to_record(), default=str) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _resolve_supersession(self) -> None:
        """Flag any entry pointed at by another's ``supersedes``."""
        for entry in self._entries.values():
            if entry.supersedes and entry.supersedes in self._entries:
                self._entries[entry.supersedes].superseded_by = entry.id

    # -- writes -------------------------------------------------------------
    def add(self, entry: MemoryEntry) -> MemoryEntry:
        """Append a new entry (or a new snapshot of an existing id)."""
        self._entries[entry.id] = entry
        self._append(entry)
        if entry.supersedes and entry.supersedes in self._entries:
            self._entries[entry.supersedes].superseded_by = entry.id
        return entry

    def remember(
        self,
        content: str,
        *,
        kind: str = MemoryKind.SEMANTIC.value,
        scope: dict[str, Any] | None = None,
        source: str = "inference",
        confidence: float | None = None,
        valid_until: datetime | None = None,
        memory_scope: str = MemoryScope.PROJECT.value,
        tags: list[str] | None = None,
        supersedes: str | None = None,
    ) -> MemoryEntry:
        """Convenience constructor: confidence defaults from ``source`` prior."""
        entry = MemoryEntry(
            content=content,
            kind=kind,
            scope=scope or {},
            source=source,
            confidence=min(confidence if confidence is not None else confidence_for_source(source), MAX_CONFIDENCE),
            valid_until=valid_until,
            memory_scope=memory_scope,
            tags=tags or [],
            supersedes=supersedes,
        )
        return self.add(entry)

    def supersede(self, old_id: str, new_entry: MemoryEntry) -> MemoryEntry:
        """Replace ``old_id`` with ``new_entry`` via an append + link.

        Preserves the audit trail: the old entry stays in the log, flagged
        ``superseded_by`` rather than deleted.
        """
        new_entry.supersedes = old_id
        return self.add(new_entry)

    # -- reads --------------------------------------------------------------
    def get(self, entry_id: str) -> MemoryEntry | None:
        return self._entries.get(entry_id)

    def active(self, kind: str | None = None) -> list[MemoryEntry]:
        """All non-superseded, non-contradicted entries (optionally by kind)."""
        return [
            e for e in self._entries.values()
            if e.is_active and (kind is None or e.kind == kind)
        ]

    def all_entries(self) -> list[MemoryEntry]:
        """Every entry including superseded ones (for audit / temporal queries)."""
        return list(self._entries.values())
