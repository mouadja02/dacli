"""Catalog cache — the data-agent specialization (workstream 2.2).

Data agents live or die on schema accuracy, so introspected structure
(databases, schemas, tables, columns, types, row-count estimates) is a
**first-class memory type**, not generic semantic memory. It gets its own
TTL / invalidation machinery.

Two reliability rules:

* **TTL-based staleness** — beyond its TTL a catalog entry is a *hint*, not a
  fact (``is_stale`` is True; retrieval down-ranks it; ``verify`` must re-check
  before a risky action relies on it).
* **Write-invalidation** — when a connector performs a ``write`` / ``risky`` /
  ``irreversible`` op touching an object, the kernel/dispatcher invalidates the
  matching catalog scope. This is where 's ``risk`` metadata first earns
  its keep, and it correctly replaces the regex side-effects deleted in.

The catalog is a *cache* (rebuildable by re-introspecting), so it persists as a
JSON snapshot keyed by canonical scope rather than the append-only log used by
the durable :class:`~memory.store.MemoryStore`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from dacli.core.atomicio import write_json_atomic


# Default time-to-live per object type, in seconds. Structure that changes
# rarely (databases) lives longer than structure that churns (tables/row counts).
DEFAULT_TTL_SECONDS: dict[str, int] = {
    "database": 7 * 24 * 3600,
    "schema": 24 * 3600,
    "table": 6 * 3600,
    "view": 6 * 3600,
    "column": 6 * 3600,
    "file_format": 24 * 3600,
    "stage": 24 * 3600,
}
FALLBACK_TTL_SECONDS = 6 * 3600


def _now() -> datetime:
    return datetime.now()


def _canonical(value: str | None) -> str:
    """Normalize an identifier for keying: strip quotes/semicolons, upper-case.

    SQL identifiers are case-insensitive unless quoted; we fold case and drop
    surrounding double quotes so ``"Bronze"``, ``BRONZE`` and ``bronze`` key
    together. This is what made the deleted regex tracking unreliable.
    """
    if value is None:
        return ""
    return value.strip().strip(';').strip().strip('"').upper()


@dataclass
class CatalogEntry:
    """A cached piece of introspected structure for one object."""

    connector: str
    object_type: str               # "database" | "schema" | "table" | "column" | ...
    scope: dict[str, Any]          # {database, schema, object, column}
    last_verified: datetime = field(default_factory=_now)
    ttl_seconds: int = FALLBACK_TTL_SECONDS
    source: str = "introspection"
    confidence: float = 0.95
    # Whether the entry is still considered live. Write-invalidation flips this
    # to False: the structure may have changed under us, so treat it as a hint.
    valid: bool = True
    # Optional payload
    columns: list[dict[str, Any]] | None = None  # [{name, type, ...}]
    row_count_estimate: int | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def key(self) -> str:
        return self.make_key(self.connector, self.object_type, self.scope)

    @staticmethod
    def make_key(connector: str, object_type: str, scope: dict[str, Any]) -> str:
        parts = [
            connector,
            object_type,
            _canonical(scope.get("database")),
            _canonical(scope.get("schema")),
            _canonical(scope.get("object")),
            _canonical(scope.get("column")),
        ]
        return "::".join(parts)

    def age_seconds(self, now: datetime | None = None) -> float:
        now = now or _now()
        return (now - self.last_verified).total_seconds()

    def is_stale(self, now: datetime | None = None) -> bool:
        """Stale = invalidated by a write, OR older than its TTL."""
        if not self.valid:
            return True
        # At or beyond TTL the entry is a hint, not a fact (ttl=0 -> stale now).
        return self.age_seconds(now) >= self.ttl_seconds

    def to_record(self) -> dict[str, Any]:
        data = asdict(self)
        data["last_verified"] = self.last_verified.isoformat()
        return data

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> CatalogEntry:
        record = dict(record)
        record["last_verified"] = datetime.fromisoformat(record["last_verified"])
        known = cls.__dataclass_fields__.keys()
        return cls(**{k: v for k, v in record.items() if k in known})


class CatalogCache:
    """Per-connector schema/object cache with TTL + write-invalidation."""

    def __init__(self, path: str = ".dacli/memory/catalog.json"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._entries: dict[str, CatalogEntry] = {}
        self._load()

    # -- persistence --------------------------------------------------------
    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return
        for record in data.get("entries", []):
            try:
                entry = CatalogEntry.from_record(record)
            except Exception:
                continue
            self._entries[entry.key()] = entry

    def _save(self) -> None:
        data = {"entries": [e.to_record() for e in self._entries.values()]}
        write_json_atomic(self.path, data, indent=2, default=str)

    # -- writes -------------------------------------------------------------
    def record_object(
        self,
        connector: str,
        object_type: str,
        scope: dict[str, Any],
        *,
        source: str = "introspection",
        confidence: float = 0.95,
        ttl_seconds: int | None = None,
        columns: list[dict[str, Any]] | None = None,
        row_count_estimate: int | None = None,
        extra: dict[str, Any] | None = None,
    ) -> CatalogEntry:
        """Upsert an object, refreshing ``last_verified`` and marking it valid."""
        entry = CatalogEntry(
            connector=connector,
            object_type=object_type,
            scope=scope,
            last_verified=_now(),
            ttl_seconds=ttl_seconds if ttl_seconds is not None
            else DEFAULT_TTL_SECONDS.get(object_type, FALLBACK_TTL_SECONDS),
            source=source,
            confidence=confidence,
            valid=True,
            columns=columns,
            row_count_estimate=row_count_estimate,
            extra=extra or {},
        )
        self._entries[entry.key()] = entry
        self._save()
        return entry

    def invalidate_scope(
        self,
        connector: str,
        scope: dict[str, Any],
        object_type: str | None = None,
    ) -> list[CatalogEntry]:
        """Mark every entry matching ``scope`` as no longer trustworthy.

        Matching is hierarchical: invalidating a schema invalidates the schema
        entry *and* every table/column beneath it. Returns the affected entries.
        After this, the entries become *hints* (``is_stale`` True) until
        re-introspected — the agent must not trust them for the object it just
        mutated.
        """
        affected: list[CatalogEntry] = []
        tgt_db = _canonical(scope.get("database"))
        tgt_schema = _canonical(scope.get("schema"))
        tgt_object = _canonical(scope.get("object"))

        for entry in self._entries.values():
            if entry.connector != connector:
                continue
            if object_type is not None and entry.object_type != object_type:
                continue
            e_db = _canonical(entry.scope.get("database"))
            e_schema = _canonical(entry.scope.get("schema"))
            e_object = _canonical(entry.scope.get("object"))

            # A target field constrains the match only when it is provided.
            if tgt_db and e_db and tgt_db != e_db:
                continue
            if tgt_schema and e_schema and tgt_schema != e_schema:
                continue
            if tgt_object and e_object and tgt_object != e_object:
                continue

            entry.valid = False
            affected.append(entry)

        if affected:
            self._save()
        return affected

    # -- reads --------------------------------------------------------------
    def get(
        self,
        connector: str,
        object_type: str,
        scope: dict[str, Any],
    ) -> CatalogEntry | None:
        return self._entries.get(CatalogEntry.make_key(connector, object_type, scope))

    def is_known(self, connector: str, object_type: str, scope: dict[str, Any]) -> bool:
        """Known *and* currently trustworthy (not stale)."""
        entry = self.get(connector, object_type, scope)
        return entry is not None and not entry.is_stale()

    def find(
        self,
        name: str,
        connector: str | None = None,
    ) -> list[CatalogEntry]:
        """Find entries whose (possibly qualified) object name matches ``name``.

        Accepts ``object``, ``schema.object`` or ``db.schema.object``; matching
        is canonicalized (case/quote-insensitive), so ``dacli schema orders``
        finds ``ANALYTICS.MARTS.ORDERS``.
        """
        parts = [_canonical(p) for p in str(name).split(".") if p.strip()]
        if not parts:
            return []
        want_db, want_schema, want_object = "", "", parts[-1]
        if len(parts) >= 2:
            want_schema = parts[-2]
        if len(parts) >= 3:
            want_db = parts[-3]

        out = []
        for entry in self._entries.values():
            if connector is not None and entry.connector != connector:
                continue
            if _canonical(entry.scope.get("object")) != want_object:
                continue
            if want_schema and _canonical(entry.scope.get("schema")) != want_schema:
                continue
            if want_db and _canonical(entry.scope.get("database")) != want_db:
                continue
            out.append(entry)
        return out

    def list_objects(
        self,
        connector: str | None = None,
        object_type: str | None = None,
        include_stale: bool = True,
    ) -> list[CatalogEntry]:
        out = []
        for entry in self._entries.values():
            if connector is not None and entry.connector != connector:
                continue
            if object_type is not None and entry.object_type != object_type:
                continue
            if not include_stale and entry.is_stale():
                continue
            out.append(entry)
        return out
