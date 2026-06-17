"""Object lineage as blast-radius evidence (P12).

A drop is scary in proportion to who reads the object. This module keeps a
best-effort, fail-soft map of *who consumes what* and feeds it to the governor:
dropping or replacing an object with known downstream consumers names them and
raises the tier. It is evidence *for* the existing classification, not a parallel
gate — absence of lineage never blocks an action and never invents a "safe"
signal.

Edges come from three sources the agent already touches: the dbt ``manifest.json``
(model + source dependencies), warehouse view dependencies cached in the catalog,
and orchestrator DAG/dataset bindings. Endpoints are matched loosely on the
trailing identifier components so a bare ``orders`` resolves to the fully
qualified ``analytics.marts.orders``.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dacli.core.atomicio import write_json_atomic
from dacli.core.logging_setup import get_logger

log = get_logger(__name__)


def _parts(name: str) -> tuple[str, ...]:
    """Canonical dotted components: case-folded, quote/bracket-stripped."""
    cleaned = re.sub(r'[`"\[\]]', "", str(name)).strip().strip(";").strip()
    return tuple(p.upper() for p in cleaned.split(".") if p.strip())


def _leaf(name: str) -> str:
    parts = name.split(".")
    return parts[-1] if parts else name


def _suffix_match(query: tuple[str, ...], target: tuple[str, ...]) -> bool:
    """True when the shorter component list is a suffix of the longer.

    Lets a less-qualified reference resolve to a fully qualified object in
    either direction (``orders`` ↔ ``analytics.marts.orders``) without matching
    a same-length name in a different schema.
    """
    if not query or not target:
        return False
    short, long = (query, target) if len(query) <= len(target) else (target, query)
    return long[len(long) - len(short):] == short


@dataclass
class LineageNode:
    name: str               # dotted identifier used for matching
    kind: str               # "dbt model" | "dbt source" | "view" | "table" | "airflow DAG"
    label: str | None = None  # human name when it differs from the relation

    def display(self) -> str:
        return f"{self.kind} {self.label or _leaf(self.name)}"

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "kind": self.kind, "label": self.label}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> LineageNode:
        return cls(name=d.get("name", ""), kind=d.get("kind", ""), label=d.get("label"))


@dataclass
class LineageEdge:
    upstream: LineageNode      # the producer / the object being read
    downstream: LineageNode    # the consumer that reads it
    source: str = "unknown"    # which adapter reported the edge

    def to_dict(self) -> dict[str, Any]:
        return {"upstream": self.upstream.to_dict(),
                "downstream": self.downstream.to_dict(), "source": self.source}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> LineageEdge:
        return cls(LineageNode.from_dict(d.get("upstream") or {}),
                   LineageNode.from_dict(d.get("downstream") or {}),
                   source=d.get("source", "unknown"))


def _dedup(nodes: list[LineageNode]) -> list[LineageNode]:
    seen: set[tuple[str, str]] = set()
    out: list[LineageNode] = []
    for n in nodes:
        key = (n.name.upper(), n.kind)
        if key not in seen:
            seen.add(key)
            out.append(n)
    return out


class LineageStore:
    """In-process edge set with loose, qualifier-aware lookups + JSON persistence."""

    def __init__(self, path: str | Path | None = None):
        self.path = Path(path) if path else None
        self._edges: list[LineageEdge] = []

    def __len__(self) -> int:
        return len(self._edges)

    def add(self, upstream: LineageNode, downstream: LineageNode, *, source: str = "unknown") -> None:
        self._edges.append(LineageEdge(upstream, downstream, source=source))

    def ingest(self, edges: Iterable[LineageEdge]) -> None:
        self._edges.extend(edges)

    def downstream(self, name: str) -> list[LineageNode]:
        """Consumers that read ``name`` (its blast radius)."""
        q = _parts(name)
        return _dedup([e.downstream for e in self._edges
                       if _suffix_match(q, _parts(e.upstream.name))])

    def upstream(self, name: str) -> list[LineageNode]:
        """Objects that ``name`` reads from."""
        q = _parts(name)
        return _dedup([e.upstream for e in self._edges
                       if _suffix_match(q, _parts(e.downstream.name))])

    # -- persistence --------------------------------------------------------
    def save(self) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        write_json_atomic(self.path, {"edges": [e.to_dict() for e in self._edges]}, indent=2)

    @classmethod
    def load(cls, path: str | Path) -> LineageStore:
        store = cls(path)
        p = Path(path)
        if not p.exists():
            return store
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            log.debug("failed to read lineage store %s", p, exc_info=True)
            return store
        store._edges = [LineageEdge.from_dict(e) for e in data.get("edges", [])]
        return store


# ---------------------------------------------------------------------------
# Adapters — pure (source data → edges), so they are trivially testable.
# ---------------------------------------------------------------------------
def _relation(node: dict[str, Any], name: str) -> str:
    parts = [node.get("database"), node.get("schema"), name]
    return ".".join(str(p) for p in parts if p)


def edges_from_dbt_manifest(data: dict[str, Any]) -> list[LineageEdge]:
    """model→model and source→model edges from a dbt ``manifest.json`` dict."""
    nodes = data.get("nodes") or {}
    sources = data.get("sources") or {}
    node_of: dict[str, LineageNode] = {}
    for uid, n in nodes.items():
        if isinstance(n, dict) and n.get("resource_type") == "model":
            node_of[uid] = LineageNode(
                _relation(n, n.get("alias") or n.get("name", "")),
                "dbt model", label=n.get("name"))
    for uid, s in sources.items():
        if isinstance(s, dict):
            node_of[uid] = LineageNode(
                _relation(s, s.get("identifier") or s.get("name", "")),
                "dbt source", label=s.get("name"))

    edges: list[LineageEdge] = []
    for uid, n in nodes.items():
        down = node_of.get(uid)
        if down is None:
            continue
        for dep in ((n.get("depends_on") or {}).get("nodes") or []):
            up = node_of.get(dep)
            if up is not None:
                edges.append(LineageEdge(up, down, source="dbt"))
    return edges


def edges_from_catalog(entries: Iterable[Any]) -> list[LineageEdge]:
    """base-table→view edges from cached view dependencies (best-effort)."""
    edges: list[LineageEdge] = []
    for e in entries:
        extra = getattr(e, "extra", None) or {}
        deps = extra.get("view_dependencies") or extra.get("depends_on") or []
        if not deps:
            continue
        scope = getattr(e, "scope", {}) or {}
        rel = ".".join(str(scope[k]) for k in ("database", "schema", "object") if scope.get(k))
        view = LineageNode(rel, getattr(e, "object_type", "view"), label=scope.get("object"))
        for d in deps:
            up = LineageNode(str(d), "table", label=_leaf(str(d)))
            edges.append(LineageEdge(up, view, source="warehouse"))
    return edges


def edges_from_orchestrator(
    records: Iterable[dict[str, Any]], *, source: str = "airflow", kind: str = "airflow DAG",
) -> list[LineageEdge]:
    """object→DAG edges from orchestrator dataset/DAG bindings."""
    edges: list[LineageEdge] = []
    for r in records:
        obj = r.get("object") or r.get("dataset")
        dag = r.get("dag") or r.get("dag_id")
        if not obj or not dag:
            continue
        up = LineageNode(str(obj), "table", label=_leaf(str(obj)))
        down = LineageNode(str(dag), kind, label=str(dag))
        edges.append(LineageEdge(up, down, source=source))
    return edges


# ---------------------------------------------------------------------------
# Target extraction — what object does a pending action drop/replace?
# ---------------------------------------------------------------------------
_IDENT = r'[A-Za-z_"`\[][\w."`\]$]*'
_DESTRUCTIVE_SQL = re.compile(
    r"\b(?:"
    r"DROP\s+(?:TABLE|VIEW|MATERIALIZED\s+VIEW|EXTERNAL\s+TABLE|TEMP(?:ORARY)?\s+TABLE)?\s*(?:IF\s+EXISTS\s+)?"
    r"|TRUNCATE\s+(?:TABLE\s+)?"
    r"|DELETE\s+FROM\s+"
    r"|CREATE\s+OR\s+REPLACE\s+(?:TABLE|VIEW|MATERIALIZED\s+VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r")(" + _IDENT + r")",
    re.IGNORECASE,
)
_DESTRUCTIVE_OP = re.compile(
    r"drop|delete|remove|truncate|replace|overwrite|destroy|purge", re.IGNORECASE
)


def destructive_targets(sql: str) -> list[str]:
    """Objects a SQL statement drops, truncates, deletes from, or replaces."""
    out: list[str] = []
    for m in _DESTRUCTIVE_SQL.finditer(sql or ""):
        cleaned = re.sub(r'[`"\[\]]', "", m.group(1)).strip().strip(";")
        if cleaned:
            out.append(cleaned)
    return out


def action_targets(tool_name: str, args: dict[str, Any]) -> list[str]:
    """Best-effort objects a pending action removes or replaces.

    SQL is the truth when present; otherwise an explicit object/table arg is
    taken only for a destructively-named op (``delete_object``, ``overwrite_table``).
    """
    args = args or {}
    out: list[str] = []
    for key in ("query", "sql", "statement"):
        v = args.get(key)
        if isinstance(v, str):
            out.extend(destructive_targets(v))
    if not out and _DESTRUCTIVE_OP.search(tool_name or ""):
        for key in ("object", "table", "target", "dataset", "key", "path", "name"):
            v = args.get(key)
            if isinstance(v, str) and v.strip():
                out.append(v.strip())
                break
    seen: set[str] = set()
    res: list[str] = []
    for t in out:
        if t.lower() not in seen:
            seen.add(t.lower())
            res.append(t)
    return res


# ---------------------------------------------------------------------------
# Project builder — assemble whatever the project already knows (fail-soft).
# ---------------------------------------------------------------------------
def _lineage_path() -> Path:
    from dacli.core import paths
    return paths.state_dir() / "memory" / "lineage.json"


def build_project_lineage(settings: Any = None) -> LineageStore:
    """Lineage assembled from the persisted store + dbt manifest + catalog cache.

    Every source is optional; a missing or broken one contributes nothing rather
    than raising.
    """
    store = LineageStore.load(_lineage_path())

    try:
        from dacli.config.settings import ConnectorConfig
        project_dir = ConnectorConfig(settings, "dbt").get("project_dir", "") if settings else ""
        if project_dir:
            mp = Path(project_dir) / "target" / "manifest.json"
            if mp.exists():
                store.ingest(edges_from_dbt_manifest(
                    json.loads(mp.read_text(encoding="utf-8"))))
    except Exception:
        log.debug("dbt manifest lineage unavailable", exc_info=True)

    try:
        from dacli.memory.catalog import CatalogCache
        from dacli.core import paths
        cache = CatalogCache(path=str(paths.state_dir() / "memory" / "catalog.json"))
        store.ingest(edges_from_catalog(cache.list_objects()))
    except Exception:
        log.debug("catalog lineage unavailable", exc_info=True)

    return store
