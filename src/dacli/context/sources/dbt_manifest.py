"""dbt manifest ingestion for the live-env context layer (F-5).

Competing agents feed the dbt ``manifest.json`` (model docs, lineage, tests)
into model context; dacli's dbt connector only ran/built/tested. This source
closes that gap: it parses ``<project_dir>/target/manifest.json`` into compact,
catalog-shaped entries the context assembler ranks by task overlap alongside
the live catalog cache.

Reliability posture:

* **No dbt project configured -> no-op** (``entries()`` returns ``[]``).
* The manifest is large, so the parse is **cached** and refreshed only when the
  file's mtime changes.
* A broken/missing manifest never raises into the assembler — it yields ``[]``
  with a ``log.debug`` breadcrumb.
* Lines are bounded (columns/deps/tests capped, description truncated) so a
  big project cannot blow the LIVE token budget; the assembler's per-source
  cap still applies on top.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from dacli.core.logging_setup import get_logger

log = get_logger(__name__)

#: Bounds on what one model folds into its single context line.
MAX_COLUMNS = 12
MAX_DEPENDS_ON = 8
MAX_TESTS = 6
MAX_DESCRIPTION_CHARS = 160


@dataclass
class DbtModelEntry:
    """One dbt model, shaped like a catalog entry for the live-env layer.

    Mirrors the attributes ``context.assembler._format_catalog_entry`` reads
    (``connector``, ``object_type``, ``scope``, ``row_count_estimate``,
    ``is_stale``, ``last_verified``) and adds the dbt-specific payload. The
    assembler renders it via :meth:`context_line`.
    """

    connector: str = "dbt"
    object_type: str = "model"
    scope: dict[str, Any] = field(default_factory=dict)
    description: str = ""
    columns: list[dict[str, Any]] = field(default_factory=list)
    depends_on: list[str] = field(default_factory=list)
    tests: list[str] = field(default_factory=list)
    last_verified: datetime | None = None
    row_count_estimate: int | None = None

    @property
    def name(self) -> str:
        return str(self.scope.get("object", ""))

    def is_stale(self) -> bool:
        # Freshness is mtime-tracked by the source; a parsed entry is current.
        return False

    def context_line(self) -> str:
        """One bounded line: name, docs, column docs, lineage, tests."""
        bits = [f"dbt model {self.name}"]
        if self.description:
            bits.append(self.description[:MAX_DESCRIPTION_CHARS])
        if self.columns:
            cols = []
            for col in self.columns[:MAX_COLUMNS]:
                cname = col.get("name", "")
                cdesc = col.get("description", "")
                cols.append(f"{cname} ({cdesc[:60]})" if cdesc else cname)
            more = len(self.columns) - MAX_COLUMNS
            if more > 0:
                cols.append(f"+{more} more")
            bits.append("cols: " + ", ".join(cols))
        if self.depends_on:
            deps = self.depends_on[:MAX_DEPENDS_ON]
            bits.append("depends_on: " + ", ".join(deps))
        if self.tests:
            bits.append("tests: " + ", ".join(self.tests[:MAX_TESTS]))
        return " · ".join(bits)


def parse_manifest(data: dict[str, Any]) -> list[DbtModelEntry]:
    """Extract per-model name/docs/columns/lineage/tests from a manifest dict."""
    nodes = data.get("nodes") or {}
    if not isinstance(nodes, dict):
        return []

    # node unique_id ("model.proj.orders") -> model name, for lineage mapping.
    model_names: dict[str, str] = {
        uid: node.get("name", "")
        for uid, node in nodes.items()
        if isinstance(node, dict) and node.get("resource_type") == "model"
    }

    # Tests attach to models through their depends_on edges.
    tests_by_model: dict[str, list[str]] = {}
    for node in nodes.values():
        if not isinstance(node, dict) or node.get("resource_type") != "test":
            continue
        deps = (node.get("depends_on") or {}).get("nodes") or []
        for dep in deps:
            if dep in model_names:
                tests_by_model.setdefault(dep, []).append(node.get("name", ""))

    entries: list[DbtModelEntry] = []
    for uid, node in nodes.items():
        if not isinstance(node, dict) or node.get("resource_type") != "model":
            continue
        columns = [
            {
                "name": col.get("name", cname),
                "description": col.get("description", ""),
                "type": col.get("data_type") or "",
            }
            for cname, col in (node.get("columns") or {}).items()
            if isinstance(col, dict)
        ]
        deps = (node.get("depends_on") or {}).get("nodes") or []
        entries.append(
            DbtModelEntry(
                scope={
                    "database": node.get("database") or "",
                    "schema": node.get("schema") or "",
                    "object": node.get("name", ""),
                },
                description=node.get("description", "") or "",
                columns=columns,
                depends_on=[model_names[d] for d in deps if d in model_names],
                tests=sorted(tests_by_model.get(uid, [])),
            )
        )
    return entries


class DbtManifestSource:
    """mtime-cached reader of ``<project_dir>/target/manifest.json``."""

    def __init__(self, project_dir: str):
        self.manifest_path = Path(project_dir) / "target" / "manifest.json"
        self._mtime: float | None = None
        self._entries: list[DbtModelEntry] = []

    def entries(self) -> list[DbtModelEntry]:
        """Parsed model entries; refreshed when the manifest mtime changes.

        Missing or unreadable manifest -> ``[]`` (the no-dbt no-op).
        """
        try:
            mtime = self.manifest_path.stat().st_mtime
        except OSError:
            self._mtime, self._entries = None, []
            return []
        if mtime == self._mtime:
            return self._entries
        try:
            data = json.loads(self.manifest_path.read_text(encoding="utf-8"))
            parsed = parse_manifest(data)
        except Exception:
            log.debug(
                "failed to parse dbt manifest %s", self.manifest_path, exc_info=True
            )
            return self._entries  # keep the last good parse
        now = datetime.now()
        for entry in parsed:
            entry.last_verified = now
        self._mtime, self._entries = mtime, parsed
        return self._entries
