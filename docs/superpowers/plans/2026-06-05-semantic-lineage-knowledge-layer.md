# Semantic & Lineage Knowledge Layer (Era 3 — Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local, trust-aware estate knowledge graph (catalog + column-level lineage + semantics + PII tags) so the agent reasons over meaning and dependencies — answering "what breaks if I drop this?" and escalating ops that touch PII — without ever letting a stale or low-confidence edge authorize a destructive change.

**Architecture:** A new `memory/graph/` package mirrors the Era-1 trust invariants at estate scale: typed `Node`/`Edge` dataclasses carry `confidence`/`last_verified`/`valid_until`/`source` (same axes as `memory/store.py`); a `GraphStore` persists an append-only JSONL event log with a last-write-per-key in-memory index (the hybrid of `memory/store.py`'s audit log and `memory/catalog.py`'s keyed cache); a `GraphQuery` exposes `upstream`/`downstream`/`impact_of`/`owners_of`/`pii_reachable_from`. Builders construct the graph offline from sources the connectors already expose (catalog, dbt manifest, query logs, orchestration DAGs), each stamping a method-specific confidence (dbt=high, query-log=medium, heuristic=low). A `verify.py` re-introspects stale/low-confidence neighborhoods *before* an action relies on them. A `governance/knowledge.py` `KnowledgeGate` plugs into the existing `Governor` (strictly additive, default-off) to (a) attach impact analysis to risky/irreversible approvals, (b) escalate blast radius when PII is reachable, and (c) force live re-verification before a stale edge can clear a destructive gate. A `context/sources/graph.py` source returns a budgeted, provenance-tagged subgraph. A graph-hygiene metric lands in `eval/dashboard.py`, and offline golden scenarios in `eval/golden/graph.py` prove the six exit criteria.

**Tech Stack:** Python 3 stdlib only (dataclasses, json, datetime, enum, re, pathlib) — no new dependencies, consistent with the repo's no-frameworks constraint. Tests: `unittest` (matching `tests/test_*_phase*.py`). Eval: the existing `eval/` harness (`GoldenTask`/`TaskResult`/`Stakes`).

---

## File Structure

| File | Responsibility |
|---|---|
| `memory/graph/__init__.py` | Package exports (`Node`, `Edge`, `GraphStore`, `GraphQuery`, `ImpactReport`, builders). |
| `memory/graph/model.py` | Typed `Node`/`Edge` + enums (`NodeType`, `EdgeType`), trust fields, deterministic canonical keys, confidence-by-method, freshness/decay, serialization. |
| `memory/graph/store.py` | `GraphStore`: append-only JSONL event log + last-write-per-key index; merge-on-readd; supersession; node/edge readers. |
| `memory/graph/query.py` | `GraphQuery` + `ImpactReport`: lineage traversal, impact analysis (with min-confidence/staleness tracking), ownership, PII reachability, budgeted subgraph. |
| `memory/graph/verify.py` | Trust-as-runtime-decision for the graph: `needs_reverification`, `verify_node`, `reverify_for_action` (forces live re-introspection before a destructive gate). |
| `memory/graph/builders/__init__.py` | Builder exports + shared upsert helpers. |
| `memory/graph/builders/introspection.py` | Catalog cache → table/column nodes. |
| `memory/graph/builders/dbt.py` | dbt `manifest.json` (+ `catalog.json`) → model/source/dashboard/metric nodes, ref edges, owners, column lineage (heuristic, low-confidence). |
| `memory/graph/builders/query_log.py` | Warehouse query history → table-level lineage edges (medium confidence). |
| `memory/graph/builders/orchestration.py` | Airflow/Dagster DAG dict → job nodes + `produces`/`depends_on` edges. |
| `governance/knowledge.py` | `KnowledgeGate` + `KnowledgeAssessment`: resolves an op's target node, computes impact + PII, forces re-verification, returns escalation steps. |
| `governance/governor.py` (modify) | Optional `knowledge_gate` collaborator; additive hook after classification; impact summary on `ApprovalRequest`. |
| `governance/__init__.py` (modify) | Export `KnowledgeGate`, `KnowledgeAssessment`. |
| `context/sources/graph.py` | `GraphContextSource`: budgeted, provenance-tagged relevant subgraph for the assembler. |
| `context/sources/__init__.py` (modify) | Export `GraphContextSource`. |
| `eval/dashboard.py` (modify) | `GraphHygiene` dataclass + `graph_hygiene_report()` + render (coverage/staleness/confidence/PII). |
| `eval/sim/estate.py` | Deterministic offline fixtures: a dbt manifest dict, catalog.json dict, query log, orchestration DAG. |
| `eval/golden/graph.py` | Golden tasks: offline multi-source build, impact-gated drop, stale-edge block, PII escalation. |
| `eval/golden/__init__.py` (modify) | Add `build_graph_suite()` to the full suite. |
| `tests/test_graph_model.py` … `tests/test_graph_governance.py` | Unit suites, one per milestone, each mapping to exit criteria. |

**Edge-direction convention (locked, used by every builder and query):** an edge stores `src` and `dst` node keys where **`src` is upstream and `dst` is downstream — data flows `src → dst`**. So `Edge(src=parent_table, dst=child_model, edge_type=DERIVES_FROM)` reads "the child model derives from the parent table." `downstream(x)` follows edges with `src==x`; `upstream(x)` follows edges with `dst==x`; `impact_of(x)` = `downstream(x)` (what breaks if `x` changes). Builders never pass raw `src`/`dst` by hand — they call the `link_lineage(upstream=…, downstream=…)` helper (Task 11) so direction can never be inverted.

---

## Milestone 1 — Graph core (model, store, query)

### Task 1: Node/Edge model with trust fields

**Files:**
- Create: `memory/graph/__init__.py`
- Create: `memory/graph/model.py`
- Test: `tests/test_graph_model.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_graph_model.py
"""memory/graph model: trust fields, deterministic keys, decay (Era-3 P1)."""
import unittest
from datetime import datetime, timedelta

from memory.graph.model import (
    Node, Edge, NodeType, EdgeType, VerificationStatus,
    confidence_for_method, _canonical, MAX_CONFIDENCE, LOW_CONFIDENCE,
)


class NodeKeyTest(unittest.TestCase):
    def test_key_is_deterministic_and_case_folded(self):
        a = Node(node_type=NodeType.TABLE.value,
                 scope={"connector": "snowflake", "schema": "bronze", "object": "crm"})
        b = Node(node_type=NodeType.TABLE.value,
                 scope={"connector": "SNOWFLAKE", "schema": "BRONZE", "object": '"CRM"'})
        self.assertEqual(a.key(), b.key())

    def test_column_key_includes_column(self):
        col = Node(node_type=NodeType.COLUMN.value,
                   scope={"schema": "bronze", "object": "crm", "column": "email"})
        tbl = Node(node_type=NodeType.TABLE.value,
                   scope={"schema": "bronze", "object": "crm"})
        self.assertNotEqual(col.key(), tbl.key())

    def test_name_defaults_to_qualified_name(self):
        n = Node(node_type=NodeType.TABLE.value, scope={"schema": "s", "object": "t"})
        self.assertEqual(n.name, "s.t")


class TrustTest(unittest.TestCase):
    def test_confidence_for_method(self):
        self.assertEqual(confidence_for_method("dbt.manifest"), 0.95)
        self.assertEqual(confidence_for_method("query_log"), 0.60)
        self.assertEqual(confidence_for_method("heuristic"), 0.40)
        self.assertLess(confidence_for_method("query_log"), LOW_CONFIDENCE + 0.11)

    def test_pii_detection(self):
        n = Node(node_type=NodeType.COLUMN.value, scope={"object": "c", "column": "ssn"},
                 tags=["PII"])
        self.assertTrue(n.is_pii)

    def test_effective_confidence_decays_with_age(self):
        old = Node(node_type=NodeType.TABLE.value, scope={"object": "t"},
                   confidence=0.9, last_verified=datetime.now() - timedelta(days=30))
        fresh = Node(node_type=NodeType.TABLE.value, scope={"object": "t"},
                     confidence=0.9, last_verified=datetime.now())
        self.assertLess(old.effective_confidence(), fresh.effective_confidence())

    def test_round_trip_preserves_trust_fields(self):
        e = Edge(src="a", dst="b", edge_type=EdgeType.DERIVES_FROM.value,
                 confidence=0.6, source="query_log")
        e2 = Edge.from_record(e.to_record())
        self.assertEqual(e2.key(), e.key())
        self.assertEqual(e2.confidence, 0.6)
        self.assertEqual(e2.source, "query_log")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_graph_model -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'memory.graph'`.

- [ ] **Step 3: Write the model**

```python
# memory/graph/__init__.py
"""The estate knowledge graph (Era-3 P1).

A trust-aware graph of the data estate — tables, columns, models, jobs,
dashboards, metrics, owners — with column-level lineage and PII/classification
tags. Every node and edge carries the *same* trust axes as
``memory/store.py`` (confidence / last_verified / valid_until / source), so the
graph inherits the append-only, never-silently-rewrite discipline. The graph is
a **hypothesis**: an edge is re-verified against the live system before it
authorizes a destructive action (see ``memory/graph/verify.py``).
"""

from memory.graph.model import (
    Node, Edge, NodeType, EdgeType, VerificationStatus,
    confidence_for_method, MAX_CONFIDENCE, LOW_CONFIDENCE, LINEAGE_EDGES,
)
from memory.graph.store import GraphStore
from memory.graph.query import GraphQuery, ImpactReport

__all__ = [
    "Node", "Edge", "NodeType", "EdgeType", "VerificationStatus",
    "confidence_for_method", "MAX_CONFIDENCE", "LOW_CONFIDENCE", "LINEAGE_EDGES",
    "GraphStore", "GraphQuery", "ImpactReport",
]
```

```python
# memory/graph/model.py
"""Typed nodes/edges for the estate graph, with Era-1 trust axes.

Identity is a *deterministic canonical key* (so the same table introspected by
two builders dedupes), unlike ``memory/store.py``'s random ids — this mirrors
``memory/catalog.py``'s keyed cache. Corrections still append (the store keeps an
audit log) and can link a rename via ``supersedes``; same-key re-adds merge.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class NodeType(str, Enum):
    TABLE = "table"
    COLUMN = "column"
    MODEL = "model"
    JOB = "job"
    DASHBOARD = "dashboard"
    METRIC = "metric"
    ENTITY = "entity"
    OWNER = "owner"
    TAG = "tag"


class EdgeType(str, Enum):
    # src is upstream, dst is downstream; data flows src -> dst.
    DERIVES_FROM = "derives_from"   # dst derives from src (table/column lineage)
    PRODUCES = "produces"           # src (a job) produces dst (a table)
    DEPENDS_ON = "depends_on"       # dst depends on src (generic dependency)
    OWNED_BY = "owned_by"           # src is owned by dst (an OWNER node)
    TAGGED = "tagged"               # src is tagged with dst (a TAG node)


# Only these carry data lineage and participate in upstream/downstream/impact.
LINEAGE_EDGES = {EdgeType.DERIVES_FROM, EdgeType.PRODUCES, EdgeType.DEPENDS_ON}


class VerificationStatus(str, Enum):
    UNVERIFIED = "unverified"
    VERIFIED = "verified"
    STALE = "stale"
    CONTRADICTED = "contradicted"


# Confidence prior per construction method (the self-critique's defense against
# noisy lineage): dbt manifest = high, query-log inference = medium, name-match
# heuristic = low. A low-confidence edge may never *solely* authorize a
# destructive action (verify.py / governance force a live re-check first).
GRAPH_CONFIDENCE: Dict[str, float] = {
    "dbt.manifest": 0.95,
    "information_schema": 0.95,
    "introspection": 0.95,
    "orchestration": 0.80,
    "user": 0.90,
    "query_log": 0.60,
    "heuristic": 0.40,
}
DEFAULT_CONFIDENCE = 0.50
MAX_CONFIDENCE = 0.95
LOW_CONFIDENCE = 0.50  # at/below: weak hypothesis — re-verify before destructive use

DEFAULT_TTL_SECONDS: Dict[str, int] = {
    "table": 6 * 3600, "column": 6 * 3600, "model": 24 * 3600,
    "job": 24 * 3600, "dashboard": 24 * 3600, "metric": 24 * 3600,
    "entity": 7 * 24 * 3600, "owner": 7 * 24 * 3600, "tag": 7 * 24 * 3600,
}
FALLBACK_TTL_SECONDS = 6 * 3600
DECAY_HORIZON_DAYS = 30.0
MAX_DECAY_PENALTY = 0.9


def confidence_for_method(source: str) -> float:
    if not source:
        return DEFAULT_CONFIDENCE
    s = source.lower()
    for key, val in GRAPH_CONFIDENCE.items():
        if key in s:
            return val
    return DEFAULT_CONFIDENCE


def _now() -> datetime:
    return datetime.now()


def _canonical(value: Optional[str]) -> str:
    """Fold case + strip quotes/semicolons (matches memory/catalog.py)."""
    if value is None:
        return ""
    return value.strip().strip(';').strip().strip('"').upper()


def _decay_penalty(age_seconds: float) -> float:
    age_days = max(age_seconds / 86400.0, 0.0)
    return min(age_days / DECAY_HORIZON_DAYS, MAX_DECAY_PENALTY)


@dataclass
class Node:
    node_type: str
    scope: Dict[str, Any]
    name: str = ""
    confidence: float = DEFAULT_CONFIDENCE
    last_verified: datetime = field(default_factory=_now)
    valid_until: Optional[datetime] = None
    verification_status: str = VerificationStatus.UNVERIFIED.value
    source: str = "inference"
    ttl_seconds: int = FALLBACK_TTL_SECONDS
    tags: List[str] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)
    supersedes: Optional[str] = None
    superseded_by: Optional[str] = None
    created_at: datetime = field(default_factory=_now)

    def __post_init__(self) -> None:
        if not self.name:
            self.name = self.qualified_name()
        if self.ttl_seconds == FALLBACK_TTL_SECONDS:
            self.ttl_seconds = DEFAULT_TTL_SECONDS.get(self.node_type, FALLBACK_TTL_SECONDS)

    def qualified_name(self) -> str:
        s = self.scope
        parts = [s.get(k) for k in ("database", "schema", "object", "column") if s.get(k)]
        if parts:
            return ".".join(str(p) for p in parts)
        return str(s.get("name") or "(unscoped)")

    def key(self) -> str:
        return self.make_key(self.node_type, self.scope)

    @staticmethod
    def make_key(node_type: str, scope: Dict[str, Any]) -> str:
        parts = [
            str(node_type),
            _canonical(scope.get("connector")),
            _canonical(scope.get("database")),
            _canonical(scope.get("schema")),
            _canonical(scope.get("object")),
            _canonical(scope.get("column")),
        ]
        # Non-warehouse nodes (jobs/dashboards/owners/tags) key on their name.
        if not any(parts[2:]) and scope.get("name"):
            parts.append(_canonical(scope.get("name")))
        return "::".join(parts)

    @property
    def is_pii(self) -> bool:
        return ("pii" in [t.lower() for t in self.tags]) or bool(self.attributes.get("pii"))

    @property
    def is_active(self) -> bool:
        return (self.superseded_by is None
                and self.verification_status != VerificationStatus.CONTRADICTED.value)

    def age_seconds(self, now: Optional[datetime] = None) -> float:
        return ((now or _now()) - self.last_verified).total_seconds()

    def is_stale(self, now: Optional[datetime] = None) -> bool:
        now = now or _now()
        if self.verification_status == VerificationStatus.STALE.value:
            return True
        if self.valid_until is not None and now > self.valid_until:
            return True
        return self.age_seconds(now) >= self.ttl_seconds

    def effective_confidence(self, now: Optional[datetime] = None) -> float:
        return self.confidence * (1.0 - _decay_penalty(self.age_seconds(now)))

    def to_record(self) -> Dict[str, Any]:
        data = asdict(self)
        data["last_verified"] = self.last_verified.isoformat()
        data["created_at"] = self.created_at.isoformat()
        data["valid_until"] = self.valid_until.isoformat() if self.valid_until else None
        return data

    @classmethod
    def from_record(cls, record: Dict[str, Any]) -> "Node":
        record = dict(record)
        record["last_verified"] = datetime.fromisoformat(record["last_verified"])
        record["created_at"] = datetime.fromisoformat(record["created_at"])
        vu = record.get("valid_until")
        record["valid_until"] = datetime.fromisoformat(vu) if vu else None
        known = cls.__dataclass_fields__.keys()
        return cls(**{k: v for k, v in record.items() if k in known})


@dataclass
class Edge:
    src: str
    dst: str
    edge_type: str
    confidence: float = DEFAULT_CONFIDENCE
    last_verified: datetime = field(default_factory=_now)
    valid_until: Optional[datetime] = None
    verification_status: str = VerificationStatus.UNVERIFIED.value
    source: str = "inference"
    ttl_seconds: int = FALLBACK_TTL_SECONDS
    attributes: Dict[str, Any] = field(default_factory=dict)
    supersedes: Optional[str] = None
    superseded_by: Optional[str] = None
    created_at: datetime = field(default_factory=_now)

    def key(self) -> str:
        return self.make_key(self.src, self.dst, self.edge_type)

    @staticmethod
    def make_key(src: str, dst: str, edge_type: str) -> str:
        return f"{src}|{edge_type}|{dst}"

    @property
    def is_active(self) -> bool:
        return (self.superseded_by is None
                and self.verification_status != VerificationStatus.CONTRADICTED.value)

    def age_seconds(self, now: Optional[datetime] = None) -> float:
        return ((now or _now()) - self.last_verified).total_seconds()

    def is_stale(self, now: Optional[datetime] = None) -> bool:
        now = now or _now()
        if self.verification_status == VerificationStatus.STALE.value:
            return True
        if self.valid_until is not None and now > self.valid_until:
            return True
        return self.age_seconds(now) >= self.ttl_seconds

    def effective_confidence(self, now: Optional[datetime] = None) -> float:
        return self.confidence * (1.0 - _decay_penalty(self.age_seconds(now)))

    def to_record(self) -> Dict[str, Any]:
        data = asdict(self)
        data["last_verified"] = self.last_verified.isoformat()
        data["created_at"] = self.created_at.isoformat()
        data["valid_until"] = self.valid_until.isoformat() if self.valid_until else None
        return data

    @classmethod
    def from_record(cls, record: Dict[str, Any]) -> "Edge":
        record = dict(record)
        record["last_verified"] = datetime.fromisoformat(record["last_verified"])
        record["created_at"] = datetime.fromisoformat(record["created_at"])
        vu = record.get("valid_until")
        record["valid_until"] = datetime.fromisoformat(vu) if vu else None
        known = cls.__dataclass_fields__.keys()
        return cls(**{k: v for k, v in record.items() if k in known})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_graph_model -v`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add memory/graph/__init__.py memory/graph/model.py tests/test_graph_model.py
git commit -m "feat(graph): trust-aware Node/Edge model with deterministic keys"
```

---

### Task 2: GraphStore — append-only log + keyed index

**Files:**
- Create: `memory/graph/store.py`
- Test: `tests/test_graph_store.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_graph_store.py
"""GraphStore: append-only persistence, last-write-per-key, merge, supersession."""
import tempfile
import unittest

from memory.graph.model import Node, Edge, NodeType, EdgeType, VerificationStatus
from memory.graph.store import GraphStore


def _tmp():
    return tempfile.mkdtemp(prefix="dacli_graph_") + "/graph.jsonl"


class StoreTest(unittest.TestCase):
    def test_add_and_read_back_from_disk(self):
        path = _tmp()
        s = GraphStore(path=path)
        s.add_node(Node(node_type=NodeType.TABLE.value, scope={"object": "orders"}))
        s.add_edge(Edge(src="a", dst="b", edge_type=EdgeType.DERIVES_FROM.value))
        s2 = GraphStore(path=path)
        self.assertEqual(len(s2.nodes()), 1)
        self.assertEqual(len(s2.edges()), 1)

    def test_same_key_readd_merges_tags_and_keeps_best_trust(self):
        s = GraphStore(path=_tmp())
        k = s.add_node(Node(node_type=NodeType.COLUMN.value,
                            scope={"object": "crm", "column": "email"},
                            confidence=0.6, source="query_log")).key()
        s.add_node(Node(node_type=NodeType.COLUMN.value,
                        scope={"object": "crm", "column": "email"},
                        confidence=0.95, source="introspection", tags=["pii"]))
        node = s.get_node(k)
        self.assertEqual(node.confidence, 0.95)         # best confidence wins
        self.assertIn("pii", node.tags)                  # tags unioned
        self.assertEqual(len([n for n in s.nodes()]), 1)  # still one node

    def test_out_and_in_edges(self):
        s = GraphStore(path=_tmp())
        s.add_edge(Edge(src="x", dst="y", edge_type=EdgeType.DERIVES_FROM.value))
        s.add_edge(Edge(src="x", dst="z", edge_type=EdgeType.DERIVES_FROM.value))
        self.assertEqual({e.dst for e in s.out_edges("x")}, {"y", "z"})
        self.assertEqual([e.src for e in s.in_edges("y")], ["x"])

    def test_supersede_node_preserves_audit_trail(self):
        path = _tmp()
        s = GraphStore(path=path)
        old = s.add_node(Node(node_type=NodeType.TABLE.value, scope={"object": "old"}))
        new = Node(node_type=NodeType.TABLE.value, scope={"object": "new"})
        s.supersede_node(old.key(), new)
        self.assertEqual(s.get_node(old.key(), include_inactive=True).superseded_by, new.key())
        self.assertNotIn(old.key(), [n.key() for n in s.nodes()])  # active() excludes it
        # survives reload
        s2 = GraphStore(path=path)
        self.assertEqual(s2.get_node(old.key(), include_inactive=True).superseded_by, new.key())


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_graph_store -v`
Expected: FAIL — `No module named 'memory.graph.store'`.

- [ ] **Step 3: Write the store**

```python
# memory/graph/store.py
"""Append-only persistence for the estate graph.

Hybrid of the two Era-1 stores: an **append-only JSONL event log** (one line per
``add``/``supersede`` — the audit trail of ``memory/store.py``) replayed into a
**last-write-per-key in-memory index** (the keyed cache of ``memory/catalog.py``).
A same-key re-add *merges* (union tags, keep best confidence + latest verified +
strongest source) so multiple builders compose into one coherent graph; a rename
or replacement is a ``supersede`` that links via ``supersedes`` and is preserved
in the log.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

from memory.graph.model import Node, Edge, VerificationStatus


class GraphStore:
    def __init__(self, path: str = ".dacli/memory/graph.jsonl"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._nodes: Dict[str, Node] = {}
        self._edges: Dict[str, Edge] = {}
        self._load()

    # -- persistence --------------------------------------------------------
    def _load(self) -> None:
        if not self.path.exists():
            return
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    if rec.get("_event") == "edge":
                        e = Edge.from_record(rec)
                        self._edges[e.key()] = e
                    else:
                        n = Node.from_record(rec)
                        self._nodes[n.key()] = n
                except Exception:
                    continue  # skip a corrupt line rather than fail the session
        self._resolve_supersession()

    def _append(self, record: dict, kind: str) -> None:
        record = dict(record)
        record["_event"] = kind
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")

    def _resolve_supersession(self) -> None:
        for n in self._nodes.values():
            if n.supersedes and n.supersedes in self._nodes:
                self._nodes[n.supersedes].superseded_by = n.key()
        for e in self._edges.values():
            if e.supersedes and e.supersedes in self._edges:
                self._edges[e.supersedes].superseded_by = e.key()

    # -- writes -------------------------------------------------------------
    def add_node(self, node: Node) -> Node:
        key = node.key()
        existing = self._nodes.get(key)
        if existing is not None:
            node = self._merge_node(existing, node)
        self._nodes[key] = node
        self._append(node.to_record(), "node")
        if node.supersedes and node.supersedes in self._nodes:
            self._nodes[node.supersedes].superseded_by = key
        return node

    def add_edge(self, edge: Edge) -> Edge:
        key = edge.key()
        existing = self._edges.get(key)
        if existing is not None:
            edge = self._merge_edge(existing, edge)
        self._edges[key] = edge
        self._append(edge.to_record(), "edge")
        if edge.supersedes and edge.supersedes in self._edges:
            self._edges[edge.supersedes].superseded_by = key
        return edge

    def supersede_node(self, old_key: str, new_node: Node) -> Node:
        new_node.supersedes = old_key
        added = self.add_node(new_node)
        old = self._nodes.get(old_key)
        if old is not None:
            old.superseded_by = added.key()
            old.verification_status = VerificationStatus.CONTRADICTED.value
            self._append(old.to_record(), "node")
        return added

    @staticmethod
    def _merge_node(a: Node, b: Node) -> Node:
        """Keep the most-trustworthy snapshot, union tags/attributes."""
        winner = b if (b.confidence, b.last_verified) >= (a.confidence, a.last_verified) else a
        other = a if winner is b else b
        merged_tags = list(dict.fromkeys([*a.tags, *b.tags]))
        merged_attrs = {**other.attributes, **winner.attributes}
        winner.tags = merged_tags
        winner.attributes = merged_attrs
        return winner

    @staticmethod
    def _merge_edge(a: Edge, b: Edge) -> Edge:
        winner = b if (b.confidence, b.last_verified) >= (a.confidence, a.last_verified) else a
        other = a if winner is b else b
        winner.attributes = {**other.attributes, **winner.attributes}
        return winner

    # -- reads --------------------------------------------------------------
    def get_node(self, key: str, *, include_inactive: bool = False) -> Optional[Node]:
        n = self._nodes.get(key)
        if n is None:
            return None
        return n if (include_inactive or n.is_active) else None

    def get_edge(self, key: str) -> Optional[Edge]:
        return self._edges.get(key)

    def nodes(self, node_type: Optional[str] = None, *, include_inactive: bool = False) -> List[Node]:
        return [n for n in self._nodes.values()
                if (include_inactive or n.is_active)
                and (node_type is None or n.node_type == node_type)]

    def edges(self, edge_type: Optional[str] = None, *, include_inactive: bool = False) -> List[Edge]:
        return [e for e in self._edges.values()
                if (include_inactive or e.is_active)
                and (edge_type is None or e.edge_type == edge_type)]

    def out_edges(self, key: str, *, include_inactive: bool = False) -> List[Edge]:
        return [e for e in self._edges.values()
                if e.src == key and (include_inactive or e.is_active)]

    def in_edges(self, key: str, *, include_inactive: bool = False) -> List[Edge]:
        return [e for e in self._edges.values()
                if e.dst == key and (include_inactive or e.is_active)]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_graph_store -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add memory/graph/store.py tests/test_graph_store.py
git commit -m "feat(graph): append-only GraphStore with last-write-per-key index"
```

---

### Task 3: GraphQuery — traversal, impact, ownership, PII

**Files:**
- Create: `memory/graph/query.py`
- Test: `tests/test_graph_query.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_graph_query.py
"""GraphQuery: lineage traversal, impact (+confidence/staleness), owners, PII."""
import tempfile
import unittest
from datetime import datetime, timedelta

from memory.graph.model import Node, Edge, NodeType, EdgeType, VerificationStatus
from memory.graph.store import GraphStore
from memory.graph.query import GraphQuery, ImpactReport


def _store():
    return GraphStore(path=tempfile.mkdtemp(prefix="dacli_q_") + "/g.jsonl")


def _table(store, name, **kw):
    return store.add_node(Node(node_type=NodeType.TABLE.value,
                               scope={"object": name}, **kw)).key()


def _link(store, up, down, **kw):
    store.add_edge(Edge(src=up, dst=down, edge_type=EdgeType.DERIVES_FROM.value, **kw))


class TraversalTest(unittest.TestCase):
    def setUp(self):
        self.s = _store()
        # raw -> staging -> mart ; mart -> dashboard
        self.raw = _table(self.s, "raw")
        self.staging = _table(self.s, "staging")
        self.mart = _table(self.s, "mart")
        self.dash = self.s.add_node(Node(node_type=NodeType.DASHBOARD.value,
                                         scope={"name": "revenue_dash"})).key()
        _link(self.s, self.raw, self.staging, confidence=0.95, source="dbt.manifest")
        _link(self.s, self.staging, self.mart, confidence=0.95, source="dbt.manifest")
        _link(self.s, self.mart, self.dash, confidence=0.95, source="dbt.manifest")
        self.q = GraphQuery(self.s)

    def test_downstream_is_full_closure(self):
        self.assertEqual(self.q.downstream(self.raw), {self.staging, self.mart, self.dash})

    def test_upstream_is_full_closure(self):
        self.assertEqual(self.q.upstream(self.dash), {self.mart, self.staging, self.raw})

    def test_impact_of_returns_true_downstream_and_groups_by_type(self):
        report = self.q.impact_of(self.staging)
        self.assertEqual(report.impacted, {self.mart, self.dash})
        self.assertIn(self.dash, report.by_type.get("dashboard", []))
        self.assertFalse(report.is_empty)


class TrustAwareImpactTest(unittest.TestCase):
    def test_low_confidence_edge_flags_report(self):
        s = _store()
        a, b = _table(s, "a"), _table(s, "b")
        _link(s, a, b, confidence=0.4, source="heuristic")
        report = GraphQuery(s).impact_of(a)
        self.assertTrue(report.low_confidence)
        self.assertTrue(report.requires_reverification)

    def test_stale_edge_flags_report(self):
        s = _store()
        a, b = _table(s, "a"), _table(s, "b")
        _link(s, a, b, confidence=0.95, source="dbt.manifest",
              last_verified=datetime.now() - timedelta(days=400))
        report = GraphQuery(s).impact_of(a)
        self.assertTrue(report.stale)
        self.assertTrue(report.requires_reverification)


class OwnersAndPiiTest(unittest.TestCase):
    def test_owners_of(self):
        s = _store()
        t = _table(s, "orders")
        owner = s.add_node(Node(node_type=NodeType.OWNER.value, scope={"name": "data-eng"})).key()
        s.add_edge(Edge(src=t, dst=owner, edge_type=EdgeType.OWNED_BY.value))
        self.assertIn("data-eng", GraphQuery(s).owners_of(t))

    def test_pii_propagates_to_downstream(self):
        s = _store()
        src = s.add_node(Node(node_type=NodeType.COLUMN.value,
                              scope={"object": "users", "column": "ssn"}, tags=["pii"])).key()
        dn = s.add_node(Node(node_type=NodeType.COLUMN.value,
                             scope={"object": "report", "column": "ssn_copy"})).key()
        _link(s, src, dn)
        q = GraphQuery(s)
        self.assertIn(src, q.pii_reachable_from(dn))   # downstream node is PII-tainted
        self.assertIn(src, q.pii_reachable_from(src))  # the source itself counts

    def test_clean_node_has_no_pii(self):
        s = _store()
        a, b = _table(s, "a"), _table(s, "b")
        _link(s, a, b)
        self.assertEqual(GraphQuery(s).pii_reachable_from(b), set())


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_graph_query -v`
Expected: FAIL — `No module named 'memory.graph.query'`.

- [ ] **Step 3: Write the query API**

```python
# memory/graph/query.py
"""Traversal + impact analysis over the estate graph.

``impact_of`` is the keystone: it returns the *true downstream set* of a node
(what breaks if it changes) **and** the trust signals the governance gate needs —
the minimum edge confidence and whether any edge/node in the closure is stale or
low-confidence. A report that ``requires_reverification`` must not, on its own,
authorize a destructive action (verify.py forces a live re-check first).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from memory.graph.model import (
    Node, Edge, EdgeType, NodeType, LINEAGE_EDGES, LOW_CONFIDENCE,
)
from memory.graph.store import GraphStore


@dataclass
class ImpactReport:
    target: str
    impacted: Set[str]
    by_type: Dict[str, List[str]]
    owners: Set[str]
    min_confidence: float          # min effective confidence along traversed edges
    stale: bool                    # any traversed edge/node stale
    low_confidence: bool           # any traversed edge at/below LOW_CONFIDENCE

    @property
    def is_empty(self) -> bool:
        return len(self.impacted) == 0

    @property
    def requires_reverification(self) -> bool:
        """A non-empty closure that leans on a stale/weak edge is untrustworthy;
        an *empty* closure is judged by the caller against the target node's own
        freshness (an unverified node with no edges may simply be unmapped)."""
        return (not self.is_empty) and (self.stale or self.low_confidence)

    def summary(self) -> str:
        if self.is_empty:
            return f"no known downstream dependencies of {self.target}"
        bits = [f"{len(self.impacted)} downstream node(s)"]
        for t, ks in sorted(self.by_type.items()):
            bits.append(f"{len(ks)} {t}(s)")
        if self.owners:
            bits.append(f"owners: {', '.join(sorted(self.owners))}")
        bits.append(f"min_conf={self.min_confidence:.2f}")
        if self.stale:
            bits.append("STALE")
        if self.low_confidence:
            bits.append("LOW-CONFIDENCE")
        return "; ".join(bits)


class GraphQuery:
    def __init__(self, store: GraphStore):
        self.store = store

    def _lineage_out(self, key: str) -> List[Edge]:
        return [e for e in self.store.out_edges(key)
                if EdgeType(e.edge_type) in LINEAGE_EDGES]

    def _lineage_in(self, key: str) -> List[Edge]:
        return [e for e in self.store.in_edges(key)
                if EdgeType(e.edge_type) in LINEAGE_EDGES]

    def downstream(self, key: str, max_depth: int = 100) -> Set[str]:
        return self._walk(key, self._lineage_out, lambda e: e.dst, max_depth)

    def upstream(self, key: str, max_depth: int = 100) -> Set[str]:
        return self._walk(key, self._lineage_in, lambda e: e.src, max_depth)

    @staticmethod
    def _walk(key, edges_fn, next_fn, max_depth) -> Set[str]:
        seen: Set[str] = set()
        stack: List[Tuple[str, int]] = [(key, 0)]
        while stack:
            cur, d = stack.pop()
            if d >= max_depth:
                continue
            for e in edges_fn(cur):
                nxt = next_fn(e)
                if nxt not in seen and nxt != key:
                    seen.add(nxt)
                    stack.append((nxt, d + 1))
        return seen

    def impact_of(self, key: str, *, now: Optional[datetime] = None) -> ImpactReport:
        now = now or datetime.now()
        impacted: Set[str] = set()
        by_type: Dict[str, List[str]] = {}
        min_conf = 1.0
        stale = False
        low_conf = False
        stack: List[Tuple[str, int]] = [(key, 0)]
        while stack:
            cur, d = stack.pop()
            if d >= 100:
                continue
            for e in self._lineage_out(cur):
                eff = e.effective_confidence(now)
                min_conf = min(min_conf, eff)
                if e.is_stale(now):
                    stale = True
                if e.confidence <= LOW_CONFIDENCE:
                    low_conf = True
                nxt = e.dst
                if nxt not in impacted and nxt != key:
                    impacted.add(nxt)
                    node = self.store.get_node(nxt)
                    nt = node.node_type if node else "unknown"
                    by_type.setdefault(nt, []).append(nxt)
                    if node is not None and node.is_stale(now):
                        stale = True
                    stack.append((nxt, d + 1))
        owners: Set[str] = set()
        for k in {key, *impacted}:
            owners |= self.owners_of(k)
        return ImpactReport(
            target=key, impacted=impacted, by_type=by_type, owners=owners,
            min_confidence=min_conf if impacted else 1.0,
            stale=stale, low_confidence=low_conf,
        )

    def owners_of(self, key: str) -> Set[str]:
        owners: Set[str] = set()
        for e in self.store.out_edges(key):
            if EdgeType(e.edge_type) == EdgeType.OWNED_BY:
                node = self.store.get_node(e.dst)
                owners.add(node.name if node else e.dst)
        node = self.store.get_node(key)
        if node is not None and node.attributes.get("owner"):
            owners.add(str(node.attributes["owner"]))
        return owners

    def pii_reachable_from(self, key: str) -> Set[str]:
        """PII-tagged nodes that taint ``key`` (itself or any ancestor).

        Non-empty ⇒ an op on ``key`` touches PII-derived data and must escalate.
        Because every downstream node has the PII source among its ancestors, this
        also "flags downstream nodes" (exit criterion 4)."""
        candidates = self.upstream(key) | {key}
        out: Set[str] = set()
        for k in candidates:
            node = self.store.get_node(k)
            if node is not None and node.is_pii:
                out.add(k)
        return out

    def subgraph(self, key: str, radius: int = 1) -> Tuple[List[Node], List[Edge]]:
        """The neighborhood of ``key`` within ``radius`` hops (both directions)."""
        keys: Set[str] = {key}
        frontier = {key}
        for _ in range(max(radius, 0)):
            nxt: Set[str] = set()
            for k in frontier:
                for e in self.store.out_edges(k):
                    nxt.add(e.dst)
                for e in self.store.in_edges(k):
                    nxt.add(e.src)
            nxt -= keys
            keys |= nxt
            frontier = nxt
            if not frontier:
                break
        nodes = [n for n in (self.store.get_node(k) for k in keys) if n is not None]
        edges = [e for e in self.store.edges() if e.src in keys and e.dst in keys]
        return nodes, edges
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_graph_query -v`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add memory/graph/query.py tests/test_graph_query.py
git commit -m "feat(graph): GraphQuery with trust-aware impact, owners, PII reachability"
```

---

## Milestone 2 — Builders (offline, multi-source)

### Task 4: Builder package + shared lineage helper

**Files:**
- Create: `memory/graph/builders/__init__.py`
- Test: `tests/test_graph_builders.py` (created here, extended by Tasks 5–8)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_graph_builders.py
"""Graph builders: catalog, dbt, query-log, orchestration → one coherent graph."""
import tempfile
import unittest

from memory.graph.model import NodeType, EdgeType
from memory.graph.store import GraphStore
from memory.graph.builders import link_lineage


def _store():
    return GraphStore(path=tempfile.mkdtemp(prefix="dacli_b_") + "/g.jsonl")


class HelperTest(unittest.TestCase):
    def test_link_lineage_sets_src_upstream_dst_downstream(self):
        s = _store()
        edge = link_lineage(s, upstream="UP", downstream="DOWN", source="query_log")
        self.assertEqual(edge.src, "UP")
        self.assertEqual(edge.dst, "DOWN")
        self.assertEqual(edge.edge_type, EdgeType.DERIVES_FROM.value)
        self.assertEqual(edge.confidence, 0.60)  # query_log prior


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_graph_builders -v`
Expected: FAIL — `No module named 'memory.graph.builders'`.

- [ ] **Step 3: Write the builder package init + helper**

```python
# memory/graph/builders/__init__.py
"""Graph builders — construct the estate graph from sources the connectors
already expose. Each builder stamps a method-specific confidence
(``confidence_for_method``), so a name-match heuristic never outranks a dbt
manifest, and a low-confidence edge can never *solely* authorize a destructive
action (verify.py / governance enforce this).
"""

from __future__ import annotations

from typing import Optional

from memory.graph.model import (
    Node, Edge, EdgeType, confidence_for_method, DEFAULT_TTL_SECONDS, FALLBACK_TTL_SECONDS,
)
from memory.graph.store import GraphStore


def link_lineage(
    store: GraphStore,
    *,
    upstream: str,
    downstream: str,
    edge_type: EdgeType = EdgeType.DERIVES_FROM,
    source: str = "inference",
    confidence: Optional[float] = None,
) -> Edge:
    """The *only* sanctioned way to add a lineage edge.

    Enforces the locked direction convention (``src`` upstream, ``dst``
    downstream) so a builder can never invert lineage, and derives confidence
    from the construction method unless overridden.
    """
    edge = Edge(
        src=upstream, dst=downstream, edge_type=edge_type.value,
        source=source,
        confidence=confidence if confidence is not None else confidence_for_method(source),
    )
    return store.add_edge(edge)


def upsert_node(store: GraphStore, node: Node) -> Node:
    return store.add_node(node)


# Re-exported once the per-source builders exist (Tasks 5–8).
from memory.graph.builders.introspection import build_from_catalog          # noqa: E402
from memory.graph.builders.dbt import build_from_dbt_manifest               # noqa: E402
from memory.graph.builders.query_log import build_from_query_log            # noqa: E402
from memory.graph.builders.orchestration import build_from_orchestration    # noqa: E402

__all__ = [
    "link_lineage", "upsert_node",
    "build_from_catalog", "build_from_dbt_manifest",
    "build_from_query_log", "build_from_orchestration",
]
```

> **Note:** the four `from … import` lines at the bottom will fail until Tasks 5–8 create those modules. To keep this task green in isolation, temporarily comment them out and the `__all__` entries, run the test, then **uncomment them as part of Task 5–8's "run to pass" step**. (Subagent executing strictly task-by-task: leave them commented now; Task 8's final run re-enables all four.)

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_graph_builders -v`
Expected: PASS (1 test) — with the four bottom imports commented out.

- [ ] **Step 5: Commit**

```bash
git add memory/graph/builders/__init__.py tests/test_graph_builders.py
git commit -m "feat(graph): builder package with direction-safe link_lineage helper"
```

---

### Task 5: Introspection builder (catalog → nodes)

**Files:**
- Create: `memory/graph/builders/introspection.py`
- Test: `tests/test_graph_builders.py` (add `IntrospectionBuilderTest`)

- [ ] **Step 1: Write the failing test (append to the file)**

```python
# tests/test_graph_builders.py  (add this class)
from memory.catalog import CatalogCache
from memory.graph.builders import build_from_catalog


class IntrospectionBuilderTest(unittest.TestCase):
    def test_catalog_tables_and_columns_become_nodes(self):
        cat = CatalogCache(path=tempfile.mkdtemp(prefix="dacli_cat_") + "/c.json")
        cat.record_object("snowflake", "table", {"schema": "bronze", "object": "crm"},
                          columns=[{"name": "email", "type": "VARCHAR"},
                                   {"name": "id", "type": "INT"}])
        s = _store()
        build_from_catalog(s, cat)
        tables = s.nodes(NodeType.TABLE.value)
        cols = s.nodes(NodeType.COLUMN.value)
        self.assertEqual(len(tables), 1)
        self.assertEqual({c.scope.get("column") for c in cols}, {"email", "id"})
        self.assertEqual(tables[0].source, "introspection")
        self.assertEqual(tables[0].confidence, 0.95)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_graph_builders.IntrospectionBuilderTest -v`
Expected: FAIL — `cannot import name 'build_from_catalog'`.

- [ ] **Step 3: Write the introspection builder**

```python
# memory/graph/builders/introspection.py
"""Catalog cache → table/column nodes (reuses memory/catalog.py)."""

from __future__ import annotations

from typing import Any

from memory.graph.model import Node, NodeType, VerificationStatus
from memory.graph.store import GraphStore


def build_from_catalog(store: GraphStore, catalog: Any) -> int:
    """Add a TABLE node per cached table/view and a COLUMN node per column.

    Live introspection is the most trustworthy source (confidence 0.95,
    ``VERIFIED``). Returns the number of nodes added/refreshed.
    """
    count = 0
    for entry in catalog.list_objects():
        if entry.object_type not in ("table", "view"):
            continue
        scope = {
            "connector": entry.connector,
            "database": entry.scope.get("database"),
            "schema": entry.scope.get("schema"),
            "object": entry.scope.get("object"),
        }
        tbl = Node(
            node_type=NodeType.TABLE.value, scope=scope, source="introspection",
            confidence=0.95, verification_status=VerificationStatus.VERIFIED.value,
            attributes={"row_count_estimate": entry.row_count_estimate},
        )
        store.add_node(tbl)
        count += 1
        for col in (entry.columns or []):
            cname = col.get("name") if isinstance(col, dict) else str(col)
            if not cname:
                continue
            col_scope = {**scope, "column": cname}
            store.add_node(Node(
                node_type=NodeType.COLUMN.value, scope=col_scope, source="introspection",
                confidence=0.95, verification_status=VerificationStatus.VERIFIED.value,
                attributes={"data_type": col.get("type") if isinstance(col, dict) else None},
            ))
            count += 1
    return count
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_graph_builders.IntrospectionBuilderTest -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add memory/graph/builders/introspection.py tests/test_graph_builders.py
git commit -m "feat(graph): introspection builder (catalog -> table/column nodes)"
```

---

### Task 6: dbt builder (manifest → models, refs, owners, column lineage)

**Files:**
- Create: `memory/graph/builders/dbt.py`
- Test: `tests/test_graph_builders.py` (add `DbtBuilderTest`)

- [ ] **Step 1: Write the failing test (append)**

```python
# tests/test_graph_builders.py  (add this class)
from memory.graph.builders import build_from_dbt_manifest

_MANIFEST = {
    "nodes": {
        "model.shop.stg_orders": {
            "resource_type": "model", "name": "stg_orders",
            "schema": "staging", "database": "analytics",
            "depends_on": {"nodes": ["source.shop.raw.orders"]},
        },
        "model.shop.fct_orders": {
            "resource_type": "model", "name": "fct_orders",
            "schema": "marts", "database": "analytics",
            "depends_on": {"nodes": ["model.shop.stg_orders"]},
        },
    },
    "sources": {
        "source.shop.raw.orders": {
            "resource_type": "source", "name": "orders",
            "schema": "raw", "database": "analytics",
        },
    },
    "exposures": {
        "exposure.shop.revenue": {
            "resource_type": "exposure", "name": "revenue", "type": "dashboard",
            "depends_on": {"nodes": ["model.shop.fct_orders"]},
            "owner": {"name": "analytics-team"},
        },
    },
    "metrics": {},
}

_CATALOG_JSON = {
    "nodes": {
        "model.shop.fct_orders": {"columns": {"ORDER_ID": {"type": "INT"},
                                              "AMOUNT": {"type": "NUMBER"}}},
        "model.shop.stg_orders": {"columns": {"ORDER_ID": {"type": "INT"},
                                              "AMOUNT": {"type": "NUMBER"}}},
    }
}


class DbtBuilderTest(unittest.TestCase):
    def test_models_refs_exposures_owners(self):
        s = _store()
        build_from_dbt_manifest(s, _MANIFEST)
        names = {n.name for n in s.nodes()}
        self.assertTrue({"analytics.staging.stg_orders",
                         "analytics.marts.fct_orders"} <= names)
        # an exposure becomes a downstream dashboard
        dash = [n for n in s.nodes(NodeType.DASHBOARD.value)]
        self.assertEqual(len(dash), 1)
        # ref edge fires with manifest (high) confidence
        ref_edges = [e for e in s.edges(EdgeType.DERIVES_FROM.value)]
        self.assertTrue(any(e.confidence == 0.95 for e in ref_edges))
        # owner edge
        self.assertTrue(any(e.edge_type == EdgeType.OWNED_BY.value for e in s.edges()))

    def test_column_lineage_is_heuristic_low_confidence(self):
        s = _store()
        build_from_dbt_manifest(s, _MANIFEST, catalog_json=_CATALOG_JSON)
        col_edges = [e for e in s.edges(EdgeType.DERIVES_FROM.value)
                     if "::COLUMN::" in e.src.upper() or e.src.startswith("column")]
        self.assertTrue(col_edges, "expected name-match column lineage edges")
        self.assertTrue(all(e.confidence == 0.40 for e in col_edges))  # heuristic
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_graph_builders.DbtBuilderTest -v`
Expected: FAIL — `cannot import name 'build_from_dbt_manifest'`.

- [ ] **Step 3: Write the dbt builder**

```python
# memory/graph/builders/dbt.py
"""dbt manifest.json (+ catalog.json) → the richest free lineage source.

Model-level lineage comes from ``depends_on.nodes`` (dbt manifest = high
confidence). Column-level lineage is *not* in a vanilla manifest, so we infer it
by 1:1 column-name match between a model and its parents — explicitly a
**heuristic** (low confidence), so it can inform but never solely authorize a
destructive action. True column lineage from compiled SQL is the query-log
builder's job (Task 7).
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from memory.graph.model import Node, NodeType, EdgeType, VerificationStatus
from memory.graph.store import GraphStore
from memory.graph.builders import link_lineage  # type: ignore  (set up in Task 4)

_RESOURCE_TO_NODETYPE = {
    "model": NodeType.MODEL,
    "source": NodeType.TABLE,
    "seed": NodeType.TABLE,
    "snapshot": NodeType.TABLE,
    "exposure": NodeType.DASHBOARD,
    "metric": NodeType.METRIC,
}


def _scope_for(meta: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "connector": "dbt",
        "database": meta.get("database"),
        "schema": meta.get("schema"),
        "object": meta.get("name"),
    }


def _node_for_uid(uid: str, manifest: Dict[str, Any]):
    meta = (manifest.get("nodes", {}).get(uid)
            or manifest.get("sources", {}).get(uid)
            or manifest.get("exposures", {}).get(uid)
            or manifest.get("metrics", {}).get(uid))
    if not meta:
        return None, None
    nt = _RESOURCE_TO_NODETYPE.get(meta.get("resource_type", "model"), NodeType.MODEL)
    if nt == NodeType.DASHBOARD or nt == NodeType.METRIC:
        scope = {"connector": "dbt", "name": meta.get("name")}
    else:
        scope = _scope_for(meta)
    return nt, scope


def build_from_dbt_manifest(
    store: GraphStore, manifest: Dict[str, Any],
    catalog_json: Optional[Dict[str, Any]] = None,
) -> int:
    count = 0
    sections = ["nodes", "sources", "exposures", "metrics"]
    # 1. nodes
    for section in sections:
        for uid, meta in (manifest.get(section, {}) or {}).items():
            nt, scope = _node_for_uid(uid, manifest)
            if nt is None:
                continue
            owner = meta.get("owner")
            owner_name = owner.get("name") if isinstance(owner, dict) else owner
            node = store.add_node(Node(
                node_type=nt.value, scope=scope, source="dbt.manifest",
                confidence=0.95, verification_status=VerificationStatus.VERIFIED.value,
                attributes={"unique_id": uid, "owner": owner_name},
            ))
            count += 1
            if owner_name:
                own = store.add_node(Node(node_type=NodeType.OWNER.value,
                                          scope={"name": owner_name}, source="dbt.manifest",
                                          confidence=0.95))
                store.add_edge_owned_by(node.key(), own.key()) if hasattr(store, "add_edge_owned_by") else \
                    store.add_edge(__import__("memory.graph.model", fromlist=["Edge"]).Edge(
                        src=node.key(), dst=own.key(),
                        edge_type=EdgeType.OWNED_BY.value, source="dbt.manifest", confidence=0.95))
    # 2. ref edges (model/exposure depends_on its parents)
    for section in ("nodes", "exposures", "metrics"):
        for uid, meta in (manifest.get(section, {}) or {}).items():
            nt, scope = _node_for_uid(uid, manifest)
            if nt is None:
                continue
            child_key = Node.make_key(nt.value, scope)
            for parent_uid in (meta.get("depends_on", {}) or {}).get("nodes", []) or []:
                p_nt, p_scope = _node_for_uid(parent_uid, manifest)
                if p_nt is None:
                    continue
                parent_key = Node.make_key(p_nt.value, p_scope)
                link_lineage(store, upstream=parent_key, downstream=child_key,
                             source="dbt.manifest")
                count += 1
    # 3. heuristic column lineage from catalog.json (name match)
    if catalog_json:
        _build_column_lineage(store, manifest, catalog_json)
    return count


def _build_column_lineage(store, manifest, catalog_json) -> None:
    cat_nodes = catalog_json.get("nodes", {}) or {}
    for uid, cat in cat_nodes.items():
        nt, scope = _node_for_uid(uid, manifest)
        if nt is None:
            continue
        child_key = Node.make_key(nt.value, scope)
        cols = list((cat.get("columns") or {}).keys())
        # add column nodes
        for col in cols:
            store.add_node(Node(node_type=NodeType.COLUMN.value,
                                scope={**scope, "column": col}, source="dbt.manifest",
                                confidence=0.95))
        # for each parent in the manifest catalog, link same-named columns (heuristic)
        meta = manifest.get("nodes", {}).get(uid, {})
        for parent_uid in (meta.get("depends_on", {}) or {}).get("nodes", []) or []:
            p_cat = cat_nodes.get(parent_uid)
            p_nt, p_scope = _node_for_uid(parent_uid, manifest)
            if not p_cat or p_nt is None:
                continue
            p_cols = set((p_cat.get("columns") or {}).keys())
            for col in cols:
                if col in p_cols:
                    up = Node.make_key(NodeType.COLUMN.value, {**p_scope, "column": col})
                    down = Node.make_key(NodeType.COLUMN.value, {**scope, "column": col})
                    link_lineage(store, upstream=up, downstream=down, source="heuristic")
```

> **Self-note for the implementer:** the `add_edge_owned_by`/`__import__` dance above is ugly. Replace it with a clean import at the top of the file: `from memory.graph.model import Edge` and write the owner edge as
> ```python
> store.add_edge(Edge(src=node.key(), dst=own.key(), edge_type=EdgeType.OWNED_BY.value, source="dbt.manifest", confidence=0.95))
> ```
> The inline version is shown only to avoid a forward-reference trap; prefer the clean import.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_graph_builders.DbtBuilderTest -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add memory/graph/builders/dbt.py tests/test_graph_builders.py
git commit -m "feat(graph): dbt builder (refs, exposures, owners, heuristic column lineage)"
```

---

### Task 7: Query-log builder (SQL → table lineage)

**Files:**
- Create: `memory/graph/builders/query_log.py`
- Test: `tests/test_graph_builders.py` (add `QueryLogBuilderTest`)

- [ ] **Step 1: Write the failing test (append)**

```python
# tests/test_graph_builders.py  (add this class)
from memory.graph.builders import build_from_query_log


class QueryLogBuilderTest(unittest.TestCase):
    def test_insert_select_infers_table_lineage(self):
        s = _store()
        build_from_query_log(s, [
            "INSERT INTO marts.fct_orders SELECT * FROM staging.stg_orders s "
            "JOIN staging.customers c ON s.cid = c.id",
        ])
        edges = s.edges(EdgeType.DERIVES_FROM.value)
        dsts = {s.get_node(e.dst).name for e in edges}
        srcs = {s.get_node(e.src).name for e in edges}
        self.assertIn("marts.fct_orders", dsts)
        self.assertTrue({"staging.stg_orders", "staging.customers"} <= srcs)
        self.assertTrue(all(e.confidence == 0.60 for e in edges))  # query_log = medium

    def test_ctas_infers_lineage(self):
        s = _store()
        build_from_query_log(s, ["CREATE TABLE a.b AS SELECT x FROM c.d"])
        edges = s.edges(EdgeType.DERIVES_FROM.value)
        self.assertEqual({s.get_node(e.src).name for e in edges}, {"c.d"})

    def test_pure_select_creates_no_lineage(self):
        s = _store()
        build_from_query_log(s, ["SELECT 1"])
        self.assertEqual(s.edges(EdgeType.DERIVES_FROM.value), [])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_graph_builders.QueryLogBuilderTest -v`
Expected: FAIL — `cannot import name 'build_from_query_log'`.

- [ ] **Step 3: Write the query-log builder**

```python
# memory/graph/builders/query_log.py
"""Warehouse query history → inferred table-level lineage (medium confidence).

A deliberately conservative regex parser: it recognizes the write target
(``INSERT INTO`` / ``CREATE … TABLE … AS`` / ``MERGE INTO``) and the read sources
(``FROM`` / ``JOIN``), and links each source → target. Confidence is ``query_log``
(0.60) — enough to inform, never enough to *solely* clear a destructive gate.
Column-level inference is intentionally out of scope here (noisy without a real
SQL parser); dbt is the high-confidence column source.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Union

from memory.graph.model import Node, NodeType, VerificationStatus
from memory.graph.store import GraphStore
from memory.graph.builders import link_lineage

_IDENT = r'([A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*){0,2})'
_TARGET_RE = re.compile(
    rf'(?:INSERT\s+INTO|MERGE\s+INTO|CREATE\s+(?:OR\s+REPLACE\s+)?(?:TRANSIENT\s+)?TABLE'
    rf'|CREATE\s+(?:OR\s+REPLACE\s+)?VIEW)\s+{_IDENT}',
    re.IGNORECASE,
)
_SOURCE_RE = re.compile(rf'(?:FROM|JOIN)\s+{_IDENT}', re.IGNORECASE)


def _strip(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", " ", sql)
    return sql


def _scope(qualified: str) -> Dict[str, Any]:
    parts = [p.strip('"') for p in qualified.split(".")]
    if len(parts) == 3:
        return {"database": parts[0], "schema": parts[1], "object": parts[2]}
    if len(parts) == 2:
        return {"schema": parts[0], "object": parts[1]}
    return {"object": parts[0]}


def _table(store: GraphStore, qualified: str) -> str:
    node = store.add_node(Node(node_type=NodeType.TABLE.value, scope=_scope(qualified),
                               source="query_log", confidence=0.60))
    return node.key()


def build_from_query_log(
    store: GraphStore, statements: Iterable[Union[str, Dict[str, Any]]],
) -> int:
    count = 0
    for stmt in statements:
        sql = _strip(stmt["query"] if isinstance(stmt, dict) else stmt)
        tgt = _TARGET_RE.search(sql)
        if not tgt:
            continue  # read-only or unrecognized → no lineage
        target_key = _table(store, tgt.group(1))
        # sources are FROM/JOIN matches *after* the target clause
        body = sql[tgt.end():]
        sources = {m.group(1) for m in _SOURCE_RE.finditer(body)}
        for src in sources:
            src_key = _table(store, src)
            if src_key == target_key:
                continue
            link_lineage(store, upstream=src_key, downstream=target_key, source="query_log")
            count += 1
    return count
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_graph_builders.QueryLogBuilderTest -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add memory/graph/builders/query_log.py tests/test_graph_builders.py
git commit -m "feat(graph): query-log builder (SQL -> table lineage, medium confidence)"
```

---

### Task 8: Orchestration builder + multi-source coherence (EC3)

**Files:**
- Create: `memory/graph/builders/orchestration.py`
- Modify: `memory/graph/builders/__init__.py` (uncomment the four bottom imports + `__all__` entries from Task 4)
- Test: `tests/test_graph_builders.py` (add `OrchestrationBuilderTest` + `MultiSourceTest`)

- [ ] **Step 1: Write the failing test (append)**

```python
# tests/test_graph_builders.py  (add these classes)
from memory.graph.builders import build_from_orchestration


class OrchestrationBuilderTest(unittest.TestCase):
    def test_jobs_produce_tables(self):
        s = _store()
        build_from_orchestration(s, {"jobs": [
            {"name": "load_orders", "produces": ["raw.orders"]},
            {"name": "build_marts", "produces": ["marts.fct_orders"],
             "depends_on": ["load_orders"]},
        ]})
        jobs = s.nodes(NodeType.JOB.value)
        self.assertEqual(len(jobs), 2)
        produces = [e for e in s.edges(EdgeType.PRODUCES.value)]
        self.assertEqual(len(produces), 2)
        self.assertTrue(all(e.confidence == 0.80 for e in produces))


class MultiSourceTest(unittest.TestCase):
    """Exit criterion 3: introspection + dbt + query-log + orchestration build a
    coherent graph offline, deterministically."""

    def _build(self):
        from eval.sim.estate import (
            DBT_MANIFEST, DBT_CATALOG, QUERY_LOG, ORCH_DAG, seed_catalog,
        )
        from memory.graph.builders import (
            build_from_catalog, build_from_dbt_manifest,
            build_from_query_log, build_from_orchestration,
        )
        s = _store()
        build_from_catalog(s, seed_catalog())
        build_from_dbt_manifest(s, DBT_MANIFEST, catalog_json=DBT_CATALOG)
        build_from_query_log(s, QUERY_LOG)
        build_from_orchestration(s, ORCH_DAG)
        return s

    def test_graph_is_coherent_and_deterministic(self):
        s1 = self._build()
        s2 = self._build()
        keys1 = {n.key() for n in s1.nodes()}
        keys2 = {n.key() for n in s2.nodes()}
        self.assertEqual(keys1, keys2)                  # deterministic
        self.assertGreater(len(keys1), 5)
        self.assertTrue(s1.edges(EdgeType.DERIVES_FROM.value))
        self.assertTrue(s1.edges(EdgeType.PRODUCES.value))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_graph_builders -v`
Expected: FAIL — `cannot import name 'build_from_orchestration'` (and `eval.sim.estate` missing — created in Task 12; for now this task only needs `OrchestrationBuilderTest` green and `MultiSourceTest` will pass after Task 12). Run just the orchestration class for this task: `python -m unittest tests.test_graph_builders.OrchestrationBuilderTest -v`.

- [ ] **Step 3: Write the orchestration builder + re-enable builder exports**

```python
# memory/graph/builders/orchestration.py
"""Airflow/Dagster DAG → job nodes + produces/depends_on edges."""

from __future__ import annotations

from typing import Any, Dict

from memory.graph.model import Node, Edge, NodeType, EdgeType, VerificationStatus
from memory.graph.store import GraphStore
from memory.graph.builders import link_lineage


def _table_key(store: GraphStore, qualified: str) -> str:
    parts = [p.strip('"') for p in qualified.split(".")]
    if len(parts) == 3:
        scope = {"database": parts[0], "schema": parts[1], "object": parts[2]}
    elif len(parts) == 2:
        scope = {"schema": parts[0], "object": parts[1]}
    else:
        scope = {"object": parts[0]}
    return store.add_node(Node(node_type=NodeType.TABLE.value, scope=scope,
                               source="orchestration", confidence=0.80)).key()


def build_from_orchestration(store: GraphStore, dag: Dict[str, Any]) -> int:
    count = 0
    job_keys: Dict[str, str] = {}
    for job in dag.get("jobs", []) or []:
        name = job.get("name")
        if not name:
            continue
        jk = store.add_node(Node(node_type=NodeType.JOB.value, scope={"name": name},
                                 source="orchestration", confidence=0.80,
                                 verification_status=VerificationStatus.VERIFIED.value)).key()
        job_keys[name] = jk
        count += 1
        for produced in job.get("produces", []) or []:
            tk = _table_key(store, produced)
            link_lineage(store, upstream=jk, downstream=tk,
                         edge_type=EdgeType.PRODUCES, source="orchestration")
            count += 1
    # job dependencies (resolve after all jobs exist)
    for job in dag.get("jobs", []) or []:
        jk = job_keys.get(job.get("name"))
        for dep in job.get("depends_on", []) or []:
            up = job_keys.get(dep)
            if jk and up:
                store.add_edge(Edge(src=up, dst=jk, edge_type=EdgeType.DEPENDS_ON.value,
                                    source="orchestration", confidence=0.80))
    return count
```

Then **uncomment** the four bottom imports and their `__all__` entries in `memory/graph/builders/__init__.py` (added in Task 4).

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.test_graph_builders.OrchestrationBuilderTest -v`
Expected: PASS. (Full builder suite incl. `MultiSourceTest` goes green after Task 12.)

- [ ] **Step 5: Commit**

```bash
git add memory/graph/builders/orchestration.py memory/graph/builders/__init__.py tests/test_graph_builders.py
git commit -m "feat(graph): orchestration builder + enable all builder exports"
```

---

## Milestone 3 — Trust & refresh (anti stale-but-confident)

### Task 9: Graph verify — re-introspect before action

**Files:**
- Create: `memory/graph/verify.py`
- Test: `tests/test_graph_verify.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_graph_verify.py
"""Graph trust-as-runtime-decision: stale/low-confidence forces re-introspection."""
import tempfile
import unittest
from datetime import datetime, timedelta

from memory.graph.model import Node, Edge, NodeType, EdgeType, VerificationStatus
from memory.graph.store import GraphStore
from memory.graph.query import GraphQuery
from memory.graph.verify import needs_reverification, reverify_for_action


def _store():
    return GraphStore(path=tempfile.mkdtemp(prefix="dacli_v_") + "/g.jsonl")


class NeedsReverifyTest(unittest.TestCase):
    def test_unverified_needs_reverification(self):
        n = Node(node_type=NodeType.TABLE.value, scope={"object": "t"},
                 verification_status=VerificationStatus.UNVERIFIED.value)
        self.assertTrue(needs_reverification(n))

    def test_low_confidence_needs_reverification(self):
        e = Edge(src="a", dst="b", edge_type=EdgeType.DERIVES_FROM.value,
                 confidence=0.4, verification_status=VerificationStatus.VERIFIED.value)
        self.assertTrue(needs_reverification(e))

    def test_fresh_high_confidence_does_not(self):
        n = Node(node_type=NodeType.TABLE.value, scope={"object": "t"}, confidence=0.95,
                 verification_status=VerificationStatus.VERIFIED.value)
        self.assertFalse(needs_reverification(n))


class ReverifyForActionTest(unittest.TestCase):
    def test_stale_empty_impact_forces_reintrospection_that_finds_dependency(self):
        """Exit criterion 2: a planted stale node with (wrongly) empty impact must
        NOT authorize a destructive change — re-introspection corrects it first."""
        s = _store()
        # target column exists but is UNVERIFIED/stale and has no known downstream
        target = s.add_node(Node(node_type=NodeType.COLUMN.value,
                                 scope={"object": "users", "column": "email"},
                                 verification_status=VerificationStatus.STALE.value,
                                 last_verified=datetime.now() - timedelta(days=400))).key()
        q = GraphQuery(s)
        self.assertEqual(q.impact_of(target).impacted, set())  # looks safe (but stale!)

        # the live re-introspection reveals a dashboard actually depends on it
        def reintrospect(key):
            dash = s.add_node(Node(node_type=NodeType.DASHBOARD.value,
                                   scope={"name": "pii_report"}, source="introspection",
                                   confidence=0.95)).key()
            s.add_edge(Edge(src=key, dst=dash, edge_type=EdgeType.DERIVES_FROM.value,
                            source="introspection", confidence=0.95,
                            verification_status=VerificationStatus.VERIFIED.value))
            # refresh the target itself
            fresh = s.get_node(key, include_inactive=True)
            fresh.verification_status = VerificationStatus.VERIFIED.value
            fresh.last_verified = datetime.now()
            s.add_node(fresh)
            return 1

        report, reverified = reverify_for_action(q, target, reintrospect)
        self.assertTrue(reverified)
        self.assertNotEqual(report.impacted, set())  # the dependency is now visible

    def test_no_reverifier_returns_unchanged(self):
        s = _store()
        a = s.add_node(Node(node_type=NodeType.TABLE.value, scope={"object": "a"},
                            confidence=0.95,
                            verification_status=VerificationStatus.VERIFIED.value)).key()
        report, reverified = reverify_for_action(GraphQuery(s), a, None)
        self.assertFalse(reverified)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_graph_verify -v`
Expected: FAIL — `No module named 'memory.graph.verify'`.

- [ ] **Step 3: Write verify**

```python
# memory/graph/verify.py
"""Trust-as-runtime-decision for the estate graph.

The graph is a *hypothesis*. Before a destructive action relies on an impact
analysis, any stale or low-confidence node/edge in the relevant neighborhood is
re-introspected against the live system (``reverifier``), which appends corrected
facts to the store. Then the impact is recomputed. This is the estate-scale
analogue of ``memory/verify.py`` and the direct defense against the scariest
failure: a confidently-wrong "nothing depends on this" authorizing a DROP.
"""

from __future__ import annotations

from datetime import datetime
from typing import Callable, Optional, Tuple, Union

from memory.graph.model import Node, Edge, VerificationStatus, LOW_CONFIDENCE
from memory.graph.query import GraphQuery, ImpactReport

# Re-introspect the live neighborhood of a node key; append corrected facts to the
# store; return how many edges/nodes were refreshed.
Reverifier = Callable[[str], int]


def needs_reverification(
    item: Union[Node, Edge], *, now: Optional[datetime] = None, ttl_days: float = 7.0,
) -> bool:
    now = now or datetime.now()
    if item.verification_status in (VerificationStatus.STALE.value,
                                    VerificationStatus.UNVERIFIED.value):
        return True
    if item.confidence <= LOW_CONFIDENCE:
        return True
    if item.valid_until is not None and now > item.valid_until:
        return True
    if item.is_stale(now):
        return True
    age_days = item.age_seconds(now) / 86400.0
    return age_days > ttl_days


def reverify_for_action(
    query: GraphQuery, target_key: str, reverifier: Optional[Reverifier],
    *, now: Optional[datetime] = None,
) -> Tuple[ImpactReport, bool]:
    """Return ``(impact, reverified)`` for an action against ``target_key``.

    Forces a live re-introspection when (a) the impact closure leans on a
    stale/low-confidence edge, or (b) the impact is empty but the *target node*
    is itself stale/unverified (an "empty" closure we cannot trust). Without a
    reverifier, returns the as-stored impact and ``reverified=False``.
    """
    now = now or datetime.now()
    impact = query.impact_of(target_key, now=now)
    node = query.store.get_node(target_key, include_inactive=True)

    untrusted_empty = impact.is_empty and node is not None and needs_reverification(node, now=now)
    if reverifier is not None and (impact.requires_reverification or untrusted_empty):
        reverifier(target_key)
        return query.impact_of(target_key, now=now), True
    return impact, False
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_graph_verify -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add memory/graph/verify.py tests/test_graph_verify.py
git commit -m "feat(graph): verify — re-introspect stale/low-confidence before action"
```

---

## Milestone 4 — Context integration

### Task 10: Graph context source (budgeted subgraph) (EC6)

**Files:**
- Create: `context/sources/graph.py`
- Modify: `context/sources/__init__.py`
- Test: `tests/test_graph_context.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_graph_context.py
"""GraphContextSource: budgeted, provenance-tagged subgraph (not a full dump)."""
import tempfile
import unittest

from memory.graph.model import Node, Edge, NodeType, EdgeType
from memory.graph.store import GraphStore
from memory.graph.query import GraphQuery
from context.sources.graph import GraphContextSource


def _big_store(n=40):
    s = GraphStore(path=tempfile.mkdtemp(prefix="dacli_ctx_") + "/g.jsonl")
    prev = None
    for i in range(n):
        k = s.add_node(Node(node_type=NodeType.TABLE.value,
                            scope={"object": f"t{i}"}, source="dbt.manifest",
                            confidence=0.95)).key()
        if prev:
            s.add_edge(Edge(src=prev, dst=k, edge_type=EdgeType.DERIVES_FROM.value,
                            source="dbt.manifest", confidence=0.95))
        prev = k
    return s


class ContextSourceTest(unittest.TestCase):
    def test_returns_budgeted_subgraph_not_full_dump(self):
        s = _big_store(40)
        src = GraphContextSource(GraphQuery(s))
        result = src.assemble("what depends on t5", max_nodes=8)
        self.assertLessEqual(len(result["chunks"]), 8)
        self.assertGreater(len(result["chunks"]), 0)
        self.assertLess(len(result["chunks"]), 40)  # not the whole graph

    def test_each_line_carries_provenance(self):
        s = _big_store(10)
        src = GraphContextSource(GraphQuery(s))
        result = src.assemble("t5", max_nodes=8)
        for chunk in result["chunks"]:
            self.assertIn("source", chunk)
            self.assertIn("confidence", chunk)
            self.assertIn("stale", chunk)
        self.assertIn("t5", result["section"])

    def test_no_match_returns_empty(self):
        s = _big_store(5)
        src = GraphContextSource(GraphQuery(s))
        result = src.assemble("totally unrelated zzz", max_nodes=8)
        self.assertEqual(result["chunks"], [])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_graph_context -v`
Expected: FAIL — `No module named 'context.sources.graph'`.

- [ ] **Step 3: Write the context source**

```python
# context/sources/graph.py
"""The estate graph as a budgeted, provenance-tagged context source (𝒞).

Relevance over exposure: instead of dumping INFORMATION_SCHEMA, the assembler
pulls the *lineage neighborhood* of the task's target — a small subgraph, each
node tagged with its provenance (source/confidence/staleness) so the model knows
how much to trust it and the audit trail can reconstruct what was shown.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from memory.graph.query import GraphQuery

_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


def _tokens(text: str) -> set:
    return {t.lower() for t in _TOKEN_RE.findall(text or "")}


class GraphContextSource:
    def __init__(self, query: GraphQuery):
        self.query = query

    def _best_target(self, task: str) -> Optional[str]:
        task_tokens = _tokens(task)
        if not task_tokens:
            return None
        best, best_score = None, 0
        for node in self.query.store.nodes():
            score = len(task_tokens & _tokens(node.name))
            if score > best_score:
                best, best_score = node.key(), score
        return best

    def assemble(self, task: str, *, max_nodes: int = 8, radius: int = 1,
                 now: Optional[datetime] = None) -> Dict[str, Any]:
        now = now or datetime.now()
        target = self._best_target(task)
        if target is None:
            return {"section": "", "chunks": []}
        nodes, _edges = self.query.subgraph(target, radius=radius)
        # rank: the target first, then by effective confidence (freshest/strongest).
        nodes.sort(key=lambda n: (n.key() != target, -n.effective_confidence(now)))
        chunks: List[Dict[str, Any]] = []
        lines: List[str] = []
        tname = self.query.store.get_node(target)
        for node in nodes[:max_nodes]:
            stale = node.is_stale(now)
            chunks.append({
                "key": node.key(), "name": node.name, "type": node.node_type,
                "source": node.source, "confidence": round(node.effective_confidence(now), 2),
                "stale": stale,
            })
            lines.append(f"- {node.name} ({node.node_type}) "
                         f"[{node.source} conf={node.effective_confidence(now):.2f}"
                         f"{'; STALE — re-verify' if stale else ''}]")
        header = f"Estate subgraph for '{tname.name if tname else target}'"
        section = f"## {header}\n" + "\n".join(lines)
        return {"section": section, "chunks": chunks}
```

- [ ] **Step 4: Wire the export + run test**

Edit `context/sources/__init__.py` to add:

```python
from context.sources.graph import GraphContextSource
```

and append `"GraphContextSource"` to `__all__`.

Run: `python -m unittest tests.test_graph_context -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add context/sources/graph.py context/sources/__init__.py tests/test_graph_context.py
git commit -m "feat(context): graph context source — budgeted provenance-tagged subgraph"
```

---

## Milestone 5 — Governance integration

### Task 11: KnowledgeGate

**Files:**
- Create: `governance/knowledge.py`
- Modify: `governance/__init__.py`
- Test: `tests/test_graph_governance.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_graph_governance.py
"""KnowledgeGate + Governor: impact-gated drops, PII escalation, stale block."""
import tempfile
import unittest
from datetime import datetime, timedelta

from memory.graph.model import Node, Edge, NodeType, EdgeType, VerificationStatus
from memory.graph.store import GraphStore
from memory.graph.query import GraphQuery
from governance.knowledge import KnowledgeGate, KnowledgeAssessment


def _store():
    return GraphStore(path=tempfile.mkdtemp(prefix="dacli_gov_") + "/g.jsonl")


class KnowledgeGateTest(unittest.TestCase):
    def test_resolves_target_and_reports_impact(self):
        s = _store()
        tbl = s.add_node(Node(node_type=NodeType.TABLE.value,
                              scope={"schema": "marts", "object": "fct_orders"},
                              confidence=0.95,
                              verification_status=VerificationStatus.VERIFIED.value)).key()
        dash = s.add_node(Node(node_type=NodeType.DASHBOARD.value,
                               scope={"name": "rev"}, confidence=0.95)).key()
        s.add_edge(Edge(src=tbl, dst=dash, edge_type=EdgeType.DERIVES_FROM.value,
                        confidence=0.95, source="dbt.manifest",
                        verification_status=VerificationStatus.VERIFIED.value))
        gate = KnowledgeGate(GraphQuery(s))
        a = gate.assess("drop_table", {"schema": "marts", "object": "fct_orders"}, None)
        self.assertEqual(a.target, tbl)
        self.assertFalse(a.impact.is_empty)
        self.assertEqual(a.escalate_steps, 0)  # no PII here

    def test_pii_reachable_escalates(self):
        s = _store()
        col = s.add_node(Node(node_type=NodeType.COLUMN.value,
                              scope={"object": "users", "column": "ssn"}, tags=["pii"],
                              confidence=0.95,
                              verification_status=VerificationStatus.VERIFIED.value)).key()
        gate = KnowledgeGate(GraphQuery(s))
        a = gate.assess("update_rows", {"object": "users", "column": "ssn"}, None)
        self.assertGreaterEqual(a.escalate_steps, 1)
        self.assertIn(col, a.pii_sources)

    def test_stale_empty_impact_forces_reverify(self):
        s = _store()
        tgt = s.add_node(Node(node_type=NodeType.TABLE.value, scope={"object": "t"},
                              verification_status=VerificationStatus.STALE.value,
                              last_verified=datetime.now() - timedelta(days=400))).key()

        def reintrospect(key):
            d = s.add_node(Node(node_type=NodeType.DASHBOARD.value, scope={"name": "d"},
                                source="introspection", confidence=0.95)).key()
            s.add_edge(Edge(src=key, dst=d, edge_type=EdgeType.DERIVES_FROM.value,
                            source="introspection", confidence=0.95))
            return 1

        gate = KnowledgeGate(GraphQuery(s), reverifier=reintrospect)
        a = gate.assess("drop_table", {"object": "t"}, None)
        self.assertTrue(a.reverified)
        self.assertFalse(a.impact.is_empty)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_graph_governance -v`
Expected: FAIL — `No module named 'governance.knowledge'`.

- [ ] **Step 3: Write the KnowledgeGate**

```python
# governance/knowledge.py
"""Knowledge-aware governance signal (𝒢 × the estate graph).

Before a state-changing action, the gate consults the graph to answer two
questions the blast-radius classifier can't see on its own:

* **Impact** — what *breaks* if this op runs? (attached to the approval request)
* **PII reachability** — does the target derive from PII? (escalates the tier)

Crucially it is *trust-aware*: if the impact analysis leans on a stale or
low-confidence edge — or the target looks safe only because its node is stale —
it forces a live re-introspection (``reverifier``) **before** returning, so a
stale "nothing depends on this" can never clear a destructive gate (exit
criterion 2). The deep role-registry control plane is P4; this is the mechanism
P4 wires into.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set

from memory.graph.model import Node, NodeType
from memory.graph.query import GraphQuery, ImpactReport
from memory.graph.verify import Reverifier, reverify_for_action


@dataclass
class KnowledgeAssessment:
    target: Optional[str]
    impact: Optional[ImpactReport]
    pii_sources: Set[str] = field(default_factory=set)
    escalate_steps: int = 0
    reverified: bool = False

    @property
    def summary(self) -> str:
        if self.target is None:
            return ""
        bits = [self.impact.summary()] if self.impact else []
        if self.pii_sources:
            bits.append(f"PII reachable ({len(self.pii_sources)} source(s)) → escalate")
        if self.reverified:
            bits.append("re-verified live before decision")
        return "; ".join(b for b in bits if b)


# Args a connector op commonly uses to name its target object.
_SCOPE_KEYS = ("connector", "database", "schema", "object", "column", "name")


class KnowledgeGate:
    def __init__(self, query: GraphQuery, reverifier: Optional[Reverifier] = None):
        self.query = query
        self.reverifier = reverifier

    def resolve_target(self, args: Dict[str, Any]) -> Optional[str]:
        scope = {k: args.get(k) for k in _SCOPE_KEYS if args.get(k)}
        # tolerate a single "table"/"key"/"target" arg as the object name
        if "object" not in scope:
            for alias in ("table", "target", "key", "name"):
                if isinstance(args.get(alias), str):
                    scope["object"] = args[alias]
                    break
        if not scope:
            return None
        order = [NodeType.COLUMN, NodeType.TABLE, NodeType.MODEL] if scope.get("column") \
            else [NodeType.TABLE, NodeType.MODEL, NodeType.DASHBOARD]
        for nt in order:
            key = Node.make_key(nt.value, scope)
            if self.query.store.get_node(key, include_inactive=True) is not None:
                return key
        return None

    def assess(self, tool_name: str, args: Dict[str, Any], connector: Any) -> KnowledgeAssessment:
        target = self.resolve_target(args or {})
        if target is None:
            return KnowledgeAssessment(target=None, impact=None)
        impact, reverified = reverify_for_action(self.query, target, self.reverifier)
        pii = self.query.pii_reachable_from(target)
        return KnowledgeAssessment(
            target=target, impact=impact, pii_sources=pii,
            escalate_steps=1 if pii else 0, reverified=reverified,
        )
```

- [ ] **Step 4: Wire the export + run test**

Edit `governance/__init__.py` to add `KnowledgeGate` and `KnowledgeAssessment` to its imports and `__all__` (follow the existing export style in that file).

Run: `python -m unittest tests.test_graph_governance.KnowledgeGateTest -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add governance/knowledge.py governance/__init__.py tests/test_graph_governance.py
git commit -m "feat(governance): KnowledgeGate — impact + PII + forced reverify signal"
```

---

### Task 12: Wire KnowledgeGate into the Governor (additive) + estate fixtures

**Files:**
- Modify: `governance/governor.py`
- Create: `eval/sim/estate.py`
- Test: `tests/test_graph_governance.py` (add `GovernorIntegrationTest`); `tests/test_graph_builders.py::MultiSourceTest` now green

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_graph_governance.py`:

```python
# tests/test_graph_governance.py  (add this class)
import asyncio
from connectors.base import Risk, OperationSpec
from governance import (
    Governor, ActionClassifier, PolicyEngine, PermissionRegistry, Scope,
    RollbackStrategist, AuditLedger,
)
from governance.knowledge import KnowledgeGate


class _FakeConn:
    name = "snowflake"


def _gov(knowledge_gate):
    perms = PermissionRegistry(default_scope=Scope.ADMIN)
    perms.grant("snowflake", Scope.ADMIN)
    return Governor(
        classifier=ActionClassifier(), policy=PolicyEngine(), permissions=perms,
        strategist=RollbackStrategist(),
        ledger=AuditLedger(path=tempfile.mkdtemp(prefix="dacli_led_") + "/l.jsonl"),
        enforce=True, use_shadow=False, knowledge_gate=knowledge_gate,
    )


class GovernorIntegrationTest(unittest.TestCase):
    def test_pii_escalates_write_to_gated(self):
        """Exit criterion 4: an op on a PII-reachable column escalates blast radius
        so a would-be auto-run WRITE now requires approval (fail-closed → blocked)."""
        s = _store()
        s.add_node(Node(node_type=NodeType.COLUMN.value,
                        scope={"connector": "snowflake", "object": "users", "column": "ssn"},
                        tags=["pii"], confidence=0.95,
                        verification_status=VerificationStatus.VERIFIED.value))
        gate = KnowledgeGate(GraphQuery(s))
        gov = _gov(gate)
        spec = OperationSpec(name="write_rows", description="", parameters={},
                             capability="snowflake.write", risk=Risk.WRITE)
        # no approver wired → an escalated (now-interrupting) action is fail-closed.
        decision = asyncio.run(gov.review(
            "write_rows", spec,
            {"connector": "snowflake", "object": "users", "column": "ssn"}, _FakeConn()))
        self.assertFalse(decision.allowed)
        self.assertIn("knowledge", decision.metadata)

    def test_no_gate_preserves_legacy_behavior(self):
        gov = _gov(None)
        spec = OperationSpec(name="read_rows", description="", parameters={},
                             capability="snowflake.read", risk=Risk.SAFE)
        decision = asyncio.run(gov.review("read_rows", spec, {"object": "x"}, _FakeConn()))
        self.assertTrue(decision.allowed)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_graph_governance.GovernorIntegrationTest -v`
Expected: FAIL — `Governor.__init__() got an unexpected keyword argument 'knowledge_gate'`.

- [ ] **Step 3a: Modify the Governor (additive hook)**

In `governance/governor.py`:

1. Add the import near the classifier import:

```python
from governance.classifier import ActionClassifier, Classification, Tier
```

(`Tier` is now imported; add a `_promote` helper import too:)

```python
from governance.classifier import _promote  # tier promotion (right op, wrong/dangerous data)
```

2. Add the constructor parameter (in `__init__`, alongside the other optionals) and store it:

```python
        knowledge_gate: Optional[Any] = None,
```
```python
        self._knowledge_gate = knowledge_gate
```

3. Add the `impact_summary` field to `ApprovalRequest`:

```python
    dry_run_preview: Optional[str] = None
    shadow: Optional[ShadowResult] = None
    impact_summary: Optional[str] = None
```

and surface it in `describe()` (append before the `return`):

```python
        if self.impact_summary:
            lines.append(f"Impact      : {self.impact_summary}")
```

4. In `review()`, immediately **after** the `classification` audit block (after step 1, before step 2 "Permission / least-privilege scope.") insert:

```python
        # 1b. Knowledge-aware blast radius (𝒢 × estate graph). Additive + optional:
        # when wired, the graph tells us what *breaks* and whether the target is
        # PII-derived, escalating the tier and attaching impact to the approval.
        # A stale/low-confidence impact forces a live re-introspection inside the
        # gate *before* it can clear a destructive decision (anti stale-but-confident).
        assessment = None
        if self._knowledge_gate is not None and tier is not Tier.SAFE:
            try:
                assessment = self._knowledge_gate.assess(tool_name, args, connector)
            except Exception:
                assessment = None
        if assessment is not None and assessment.target is not None:
            if assessment.escalate_steps:
                promoted = _promote(tier, assessment.escalate_steps)
                classification.reasons.append(
                    f"knowledge: {assessment.summary} → promote {tier.value}→{promoted.value}")
                classification.tier = promoted
                tier = promoted
            elif assessment.impact is not None and not assessment.impact.is_empty:
                classification.reasons.append(f"knowledge impact: {assessment.impact.summary()}")
            self._audit("knowledge", tool_name, decision_id, actor, tier.value,
                        assessment.summary or "assessed",
                        knowledge={"target": assessment.target,
                                   "reverified": assessment.reverified,
                                   "pii_sources": sorted(assessment.pii_sources),
                                   "impact": assessment.impact.summary() if assessment.impact else ""})
```

5. Make the assessment available to the approval request and to every returned decision. Where `ApprovalRequest(...)` is constructed (step 8), add `impact_summary=assessment.summary if assessment else None`. And add a helper so all decisions carry it — at each `return GovernanceDecision(...)` you may add `metadata={"knowledge": ...}`; the simplest faithful change is to set a local `knowledge_meta` once:

```python
        knowledge_meta = (
            {"knowledge": {"target": assessment.target,
                           "reverified": assessment.reverified,
                           "pii_sources": sorted(assessment.pii_sources),
                           "impact": assessment.impact.summary() if assessment.impact else "",
                           "escalated": assessment.escalate_steps}}
            if assessment is not None and assessment.target is not None else {}
        )
```

place this right after the assessment block, then merge `**knowledge_meta` into the `metadata=` of each `GovernanceDecision` return (the blocked-permission, blocked-rollback, denied, and allowed returns). For returns that currently pass no `metadata`, add `metadata=dict(knowledge_meta)`; for the allowed return that already sets `metadata={"dry_run_preview": preview}`, change it to `metadata={"dry_run_preview": preview, **knowledge_meta}`.

> The test only asserts `"knowledge" in decision.metadata` for the escalated/blocked path; ensure at minimum the permission-denied and rollback-blocked and fail-closed-denied returns include `**knowledge_meta`.

- [ ] **Step 3b: Create the estate fixtures**

```python
# eval/sim/estate.py
"""Deterministic, offline estate fixtures for graph builders + golden scenarios.

A tiny but multi-source estate: a dbt project (manifest + catalog.json), a
warehouse query log, and an orchestration DAG — all canned so the graph builds
identically in CI with no network/credentials (exit criterion 3)."""

from __future__ import annotations

import tempfile
from typing import Any, Dict, List

from memory.catalog import CatalogCache

DBT_MANIFEST: Dict[str, Any] = {
    "nodes": {
        "model.shop.stg_orders": {
            "resource_type": "model", "name": "stg_orders",
            "schema": "staging", "database": "analytics",
            "depends_on": {"nodes": ["source.shop.raw.orders"]},
        },
        "model.shop.fct_orders": {
            "resource_type": "model", "name": "fct_orders",
            "schema": "marts", "database": "analytics",
            "depends_on": {"nodes": ["model.shop.stg_orders"]},
        },
    },
    "sources": {
        "source.shop.raw.orders": {
            "resource_type": "source", "name": "orders",
            "schema": "raw", "database": "analytics",
        },
    },
    "exposures": {
        "exposure.shop.revenue": {
            "resource_type": "exposure", "name": "revenue_dashboard", "type": "dashboard",
            "depends_on": {"nodes": ["model.shop.fct_orders"]},
            "owner": {"name": "analytics-team"},
        },
    },
    "metrics": {},
}

DBT_CATALOG: Dict[str, Any] = {
    "nodes": {
        "source.shop.raw.orders": {"columns": {"ORDER_ID": {"type": "INT"},
                                               "EMAIL": {"type": "VARCHAR"}}},
        "model.shop.stg_orders": {"columns": {"ORDER_ID": {"type": "INT"},
                                              "EMAIL": {"type": "VARCHAR"}}},
        "model.shop.fct_orders": {"columns": {"ORDER_ID": {"type": "INT"}}},
    }
}

QUERY_LOG: List[str] = [
    "INSERT INTO marts.fct_orders SELECT order_id FROM staging.stg_orders",
    "CREATE TABLE marts.daily_rev AS SELECT * FROM marts.fct_orders",
]

ORCH_DAG: Dict[str, Any] = {
    "jobs": [
        {"name": "load_raw", "produces": ["analytics.raw.orders"]},
        {"name": "run_dbt", "produces": ["analytics.marts.fct_orders"],
         "depends_on": ["load_raw"]},
    ]
}


def seed_catalog() -> CatalogCache:
    cat = CatalogCache(path=tempfile.mkdtemp(prefix="dacli_estate_") + "/c.json")
    cat.record_object("snowflake", "table", {"schema": "raw", "object": "orders"},
                      columns=[{"name": "ORDER_ID", "type": "INT"},
                               {"name": "EMAIL", "type": "VARCHAR"}])
    return cat
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.test_graph_governance tests.test_graph_builders -v`
Expected: PASS — `GovernorIntegrationTest` (2) and `MultiSourceTest` (1) now green.

- [ ] **Step 5: Commit**

```bash
git add governance/governor.py eval/sim/estate.py tests/test_graph_governance.py
git commit -m "feat(governance): wire optional KnowledgeGate into Governor + estate fixtures"
```

---

## Milestone 6 — Eval scenarios + hygiene metric

### Task 13: Golden scenarios (EC1, EC2, EC4) + suite registration

**Files:**
- Create: `eval/golden/graph.py`
- Modify: `eval/golden/__init__.py`
- Test: `tests/test_graph_governance.py` (add `GoldenGraphSuiteTest`)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_graph_governance.py  (add this class)
from eval.golden.graph import build_graph_suite


class GoldenGraphSuiteTest(unittest.TestCase):
    def test_suite_tasks_pass(self):
        suite = build_graph_suite()
        self.assertGreaterEqual(len(suite), 3)
        for task in suite:
            result = asyncio.run(task.run())
            self.assertTrue(result.success,
                            f"{task.id} failed: {result.error or result.detail}")
            self.assertFalse(result.unguarded_execution, f"{task.id} ran unguarded!")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_graph_governance.GoldenGraphSuiteTest -v`
Expected: FAIL — `No module named 'eval.golden.graph'`.

- [ ] **Step 3: Write the golden scenarios**

```python
# eval/golden/graph.py
"""Golden scenarios for the semantic & lineage layer (Era-3 P1).

Each maps to an exit criterion, runs fully offline against canned fixtures, and
returns a machine-verifiable TaskResult — mirroring eval/golden/spine.py.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timedelta
from typing import List

from eval.types import GoldenTask, Stakes, TaskResult
from eval.sim.estate import DBT_MANIFEST, DBT_CATALOG, QUERY_LOG, ORCH_DAG, seed_catalog


def _store():
    from memory.graph.store import GraphStore
    return GraphStore(path=tempfile.mkdtemp(prefix="dacli_gold_") + "/g.jsonl")


def _build_full(s):
    from memory.graph.builders import (
        build_from_catalog, build_from_dbt_manifest,
        build_from_query_log, build_from_orchestration,
    )
    build_from_catalog(s, seed_catalog())
    build_from_dbt_manifest(s, DBT_MANIFEST, catalog_json=DBT_CATALOG)
    build_from_query_log(s, QUERY_LOG)
    build_from_orchestration(s, ORCH_DAG)


def _impact_gated_drop():
    """EC1: impact_of(drop) returns the true downstream set, and a DROP whose
    impact is non-empty is gated (fail-closed, no approver)."""
    from memory.graph.model import Node, NodeType
    from memory.graph.query import GraphQuery
    from governance.knowledge import KnowledgeGate

    async def run() -> TaskResult:
        s = _store()
        _build_full(s)
        q = GraphQuery(s)
        target = Node.make_key(NodeType.MODEL.value,
                               {"connector": "dbt", "database": "analytics",
                                "schema": "marts", "object": "fct_orders"})
        report = q.impact_of(target)
        # ground truth: the revenue dashboard is downstream of fct_orders
        dash_present = any(s.get_node(k).node_type == "dashboard" for k in report.impacted)
        if report.is_empty or not dash_present:
            return TaskResult("graph.impact_gated_drop", success=False, steps_total=2,
                              failed_step=1, error=f"impact wrong: {report.summary()}")
        gate = KnowledgeGate(q)
        a = gate.assess("drop_model", {"connector": "dbt", "database": "analytics",
                                       "schema": "marts", "object": "fct_orders"}, None)
        ok = a.impact is not None and not a.impact.is_empty
        return TaskResult("graph.impact_gated_drop", success=ok, steps_total=2,
                          governance_interrupt=True,
                          detail=a.summary if ok else "impact not surfaced to gate")
    return run


def _stale_edge_blocked():
    """EC2: a stale node with (wrongly) empty impact must NOT look safe — the gate
    forces a live re-introspection that finds the real dependency first."""
    from memory.graph.model import Node, Edge, NodeType, EdgeType, VerificationStatus
    from memory.graph.query import GraphQuery
    from governance.knowledge import KnowledgeGate

    async def run() -> TaskResult:
        s = _store()
        target = s.add_node(Node(node_type=NodeType.TABLE.value,
                                 scope={"schema": "marts", "object": "legacy"},
                                 verification_status=VerificationStatus.STALE.value,
                                 last_verified=datetime.now() - timedelta(days=400))).key()
        before = GraphQuery(s).impact_of(target)

        def reintrospect(key):
            d = s.add_node(Node(node_type=NodeType.DASHBOARD.value, scope={"name": "exec_kpi"},
                                source="introspection", confidence=0.95)).key()
            s.add_edge(Edge(src=key, dst=d, edge_type=EdgeType.DERIVES_FROM.value,
                            source="introspection", confidence=0.95,
                            verification_status=VerificationStatus.VERIFIED.value))
            return 1

        gate = KnowledgeGate(GraphQuery(s), reverifier=reintrospect)
        a = gate.assess("drop_table", {"schema": "marts", "object": "legacy"}, None)
        # success = was stale-empty before, but the gate re-verified and now sees impact
        ok = before.is_empty and a.reverified and not a.impact.is_empty
        return TaskResult("graph.stale_edge_blocked", success=ok, steps_total=1,
                          governance_interrupt=True,
                          failed_step=None if ok else 1,
                          detail=a.summary,
                          error="" if ok else "stale empty impact was trusted as safe")
    return run


def _pii_escalation():
    """EC4: an op on a PII-reachable column escalates blast radius."""
    from memory.graph.model import Node, Edge, NodeType, EdgeType, VerificationStatus
    from memory.graph.query import GraphQuery
    from governance.knowledge import KnowledgeGate

    async def run() -> TaskResult:
        s = _store()
        pii = s.add_node(Node(node_type=NodeType.COLUMN.value,
                              scope={"object": "users", "column": "ssn"}, tags=["pii"],
                              confidence=0.95,
                              verification_status=VerificationStatus.VERIFIED.value)).key()
        derived = s.add_node(Node(node_type=NodeType.COLUMN.value,
                                  scope={"object": "report", "column": "ssn_copy"},
                                  confidence=0.95)).key()
        s.add_edge(Edge(src=pii, dst=derived, edge_type=EdgeType.DERIVES_FROM.value,
                        source="dbt.manifest", confidence=0.95))
        gate = KnowledgeGate(GraphQuery(s))
        a = gate.assess("update_rows", {"object": "report", "column": "ssn_copy"}, None)
        ok = a.escalate_steps >= 1 and pii in a.pii_sources
        return TaskResult("graph.pii_escalation", success=ok, steps_total=1,
                          governance_interrupt=ok, failed_step=None if ok else 1,
                          detail=a.summary,
                          error="" if ok else "PII reachability did not escalate")
    return run


def _multi_source_build():
    """EC3: the four builders construct a coherent graph offline, deterministically."""
    async def run() -> TaskResult:
        s1, s2 = _store(), _store()
        _build_full(s1)
        _build_full(s2)
        k1 = {n.key() for n in s1.nodes()}
        k2 = {n.key() for n in s2.nodes()}
        ok = k1 == k2 and len(k1) > 5 and bool(s1.edges("derives_from")) and bool(s1.edges("produces"))
        return TaskResult("graph.multi_source_build", success=ok, steps_total=1,
                          failed_step=None if ok else 1,
                          detail=f"{len(k1)} nodes",
                          error="" if ok else "non-deterministic or incoherent build")
    return run


def build_graph_suite() -> List[GoldenTask]:
    return [
        GoldenTask("graph.multi_source_build", "graph",
                   "four builders → one coherent graph offline", _multi_source_build(),
                   stakes=Stakes.READ_ONLY),
        GoldenTask("graph.impact_gated_drop", "graph",
                   "impact_of(drop) correct + gated", _impact_gated_drop(),
                   stakes=Stakes.DESTRUCTIVE),
        GoldenTask("graph.stale_edge_blocked", "graph",
                   "stale-but-confident edge cannot authorize a drop", _stale_edge_blocked(),
                   stakes=Stakes.DESTRUCTIVE),
        GoldenTask("graph.pii_escalation", "graph",
                   "PII reachability escalates blast radius", _pii_escalation(),
                   stakes=Stakes.WRITE),
    ]
```

- [ ] **Step 4: Register the suite + run test**

Edit `eval/golden/__init__.py`:
- add `from eval.golden.graph import build_graph_suite`
- add `+ build_graph_suite()` to `build_golden_suite()`'s return
- add `"build_graph_suite"` to `__all__`

Run: `python -m unittest tests.test_graph_governance.GoldenGraphSuiteTest -v`
Expected: PASS (the suite of 4 tasks all succeed, none unguarded).

- [ ] **Step 5: Commit**

```bash
git add eval/golden/graph.py eval/golden/__init__.py tests/test_graph_governance.py
git commit -m "feat(eval): graph golden scenarios (impact-gate, stale-block, PII-escalate)"
```

---

### Task 14: Graph hygiene metric in the dashboard (EC5)

**Files:**
- Modify: `eval/dashboard.py`
- Test: `tests/test_graph_governance.py` (add `HygieneTest`)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_graph_governance.py  (add this class)
from eval.dashboard import graph_hygiene_report, GraphHygiene


class HygieneTest(unittest.TestCase):
    def test_hygiene_reports_coverage_staleness_confidence_pii(self):
        s = _store()
        # fresh table with a lineage edge → counts as covered
        a = s.add_node(Node(node_type=NodeType.TABLE.value, scope={"object": "a"},
                            confidence=0.95,
                            verification_status=VerificationStatus.VERIFIED.value)).key()
        b = s.add_node(Node(node_type=NodeType.TABLE.value, scope={"object": "b"},
                            confidence=0.95,
                            verification_status=VerificationStatus.VERIFIED.value)).key()
        s.add_edge(Edge(src=a, dst=b, edge_type=EdgeType.DERIVES_FROM.value,
                        confidence=0.95, source="dbt.manifest"))
        # an orphan stale PII column → drags coverage down, flags PII + staleness
        s.add_node(Node(node_type=NodeType.COLUMN.value,
                        scope={"object": "users", "column": "ssn"}, tags=["pii"],
                        verification_status=VerificationStatus.STALE.value,
                        last_verified=datetime.now() - timedelta(days=400)))
        report = graph_hygiene_report(s)
        self.assertIsInstance(report, GraphHygiene)
        self.assertEqual(report.total_nodes, 3)
        self.assertEqual(report.pii_nodes, 1)
        self.assertGreater(report.stale_fraction, 0.0)
        self.assertGreater(report.lineage_coverage, 0.0)
        self.assertLess(report.lineage_coverage, 1.0)
        self.assertIn("coverage", report.render().lower())

    def test_to_dict_is_machine_readable(self):
        s = _store()
        s.add_node(Node(node_type=NodeType.TABLE.value, scope={"object": "a"}))
        d = graph_hygiene_report(s).to_dict()
        self.assertIn("lineage_coverage", d)
        self.assertIn("stale_fraction", d)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_graph_governance.HygieneTest -v`
Expected: FAIL — `cannot import name 'graph_hygiene_report'`.

- [ ] **Step 3: Add the hygiene metric to the dashboard**

Append to `eval/dashboard.py`:

```python
# --- Graph hygiene (Era-3 P1, exit criterion 5) ----------------------------
from datetime import datetime as _dt


@dataclass
class GraphHygiene:
    """Estate-graph health: coverage, staleness, confidence, PII surface.

    Answers "is the knowledge layer trustworthy?" the way the reliability
    dashboard answers "is the harness getting better?" — with data, not vibes.
    Stale nodes decay in effective confidence and are due for re-verification on
    access (memory/graph/verify.py); this surfaces the backlog."""

    total_nodes: int
    total_edges: int
    lineage_coverage: float        # fraction of table/model nodes with ≥1 lineage edge
    stale_fraction: float          # fraction of nodes that are stale
    mean_effective_confidence: float
    pii_nodes: int
    low_confidence_edges: int

    def to_dict(self) -> dict:
        return {
            "total_nodes": self.total_nodes,
            "total_edges": self.total_edges,
            "lineage_coverage": round(self.lineage_coverage, 3),
            "stale_fraction": round(self.stale_fraction, 3),
            "mean_effective_confidence": round(self.mean_effective_confidence, 3),
            "pii_nodes": self.pii_nodes,
            "low_confidence_edges": self.low_confidence_edges,
        }

    def render(self) -> str:
        d = self.to_dict()
        return (
            "Graph hygiene\n"
            "-------------\n"
            f"nodes               : {d['total_nodes']}\n"
            f"edges               : {d['total_edges']}\n"
            f"lineage coverage    : {d['lineage_coverage']:.2f}\n"
            f"stale fraction      : {d['stale_fraction']:.2f}\n"
            f"mean eff. confidence: {d['mean_effective_confidence']:.2f}\n"
            f"PII nodes           : {d['pii_nodes']}\n"
            f"low-confidence edges: {d['low_confidence_edges']}"
        )


def graph_hygiene_report(store, *, now=None) -> GraphHygiene:
    from memory.graph.model import NodeType, EdgeType, LINEAGE_EDGES, LOW_CONFIDENCE
    now = now or _dt.now()
    nodes = store.nodes()
    edges = store.edges()
    total = len(nodes) or 1
    lineage_types = {t.value for t in LINEAGE_EDGES}
    # a node is "covered" if it touches a lineage edge (as src or dst)
    touched = set()
    for e in edges:
        if e.edge_type in lineage_types:
            touched.add(e.src)
            touched.add(e.dst)
    coverable = [n for n in nodes if n.node_type in (NodeType.TABLE.value,
                                                     NodeType.MODEL.value,
                                                     NodeType.COLUMN.value)]
    covered = sum(1 for n in coverable if n.key() in touched)
    coverage = covered / (len(coverable) or 1)
    stale = sum(1 for n in nodes if n.is_stale(now))
    mean_conf = sum(n.effective_confidence(now) for n in nodes) / total
    pii = sum(1 for n in nodes if n.is_pii)
    low_edges = sum(1 for e in edges if e.confidence <= LOW_CONFIDENCE)
    return GraphHygiene(
        total_nodes=len(nodes), total_edges=len(edges),
        lineage_coverage=coverage, stale_fraction=stale / total,
        mean_effective_confidence=mean_conf, pii_nodes=pii,
        low_confidence_edges=low_edges,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_graph_governance.HygieneTest -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add eval/dashboard.py tests/test_graph_governance.py
git commit -m "feat(eval): graph hygiene metric (coverage/staleness/confidence/PII)"
```

---

### Task 15: Full-suite regression sweep + wiring note

**Files:**
- Test: run the entire suite; no new code unless a regression surfaces.

- [ ] **Step 1: Run the whole unittest suite**

Run: `python -m unittest discover -s tests -v`
Expected: PASS — the pre-existing ~157 tests plus the new graph suites, all green. The most likely breakage is the Governor edits (Task 12) altering an existing return path; if any `test_governance_phase5.py` test fails, the regression is in the additive hook — confirm `knowledge_gate` defaults to `None` and the new block is fully skipped when it is `None`, and that `**knowledge_meta` (empty dict when no gate) didn't change any existing metadata key.

- [ ] **Step 2: Run the offline eval (graph suite included)**

Run: `python -m eval --quick`
Expected: the dashboard prints with a `graph` connector row at high pass^k; **zero unguarded destructive executions** (CI hard-fails otherwise). If `graph.stale_edge_blocked` or `graph.impact_gated_drop` flakes, that is a real reliability signal — debug with superpowers:systematic-debugging, do not loosen the assertion.

- [ ] **Step 3: Verify the model/index doc pointer**

Confirm `roadmap/era3/` and the architecture-decisions memory describe P1 as roadmap-only; this plan's completion flips P1 to "implemented." (Doc/memory update is a follow-up, not part of this code plan.)

- [ ] **Step 4: Commit (only if a regression fix was needed)**

```bash
git add -A
git commit -m "test(graph): full-suite regression sweep for the semantic & lineage layer"
```

- [ ] **Step 5: Finish the branch**

Use superpowers:finishing-a-development-branch to choose merge / PR / cleanup.

---

## Self-Review

**1. Spec coverage** — every exit criterion maps to a task:

| Exit criterion | Where |
|---|---|
| EC1 impact correct + gated | Task 3 (`impact_of`), Task 11–12 (gate), Task 13 (`graph.impact_gated_drop`) |
| EC2 stale-but-confident blocked | Task 9 (`reverify_for_action`), Task 11–12 (gate forces reverify), Task 13 (`graph.stale_edge_blocked`) |
| EC3 multi-source offline build | Tasks 5–8 (builders), Task 12 (`estate.py` fixtures), Task 8/13 (`MultiSourceTest`, `graph.multi_source_build`) |
| EC4 PII propagation + escalation | Task 3 (`pii_reachable_from`), Task 6 (PII tags via tags/attributes), Task 11–12 (escalation), Task 13 (`graph.pii_escalation`) |
| EC5 hygiene metric + decay/reverify-on-access | Task 1 (`effective_confidence`/`is_stale`), Task 9 (verify-on-access), Task 14 (dashboard hygiene) |
| EC6 budgeted provenance subgraph | Task 3 (`subgraph`), Task 10 (`GraphContextSource`) |

Spec module map: `model.py`✓(T1) `store.py`✓(T2) `query.py`✓(T3) `builders/`✓(T4–8) `verify.py`✓(T9) `context/sources/graph.py`✓(T10) governance feed✓(T11–12). All present.

**2. Placeholder scan** — no "TBD"/"add error handling"/"similar to Task N"; every code step shows complete code. The one deliberate forward-reference (the four builder imports at the bottom of `builders/__init__.py`) is called out explicitly with the comment-out/uncomment procedure in Tasks 4 and 8, and the dbt owner-edge has a clean-import note flagged for the implementer.

**3. Type consistency** — names verified across tasks: `Node.make_key(node_type, scope)`, `Edge.make_key(src, dst, edge_type)`, `GraphStore.get_node(key, include_inactive=)`, `out_edges`/`in_edges`, `GraphQuery.impact_of`→`ImpactReport(target, impacted, by_type, owners, min_confidence, stale, low_confidence)` with `.is_empty`/`.requires_reverification`/`.summary()`, `pii_reachable_from`, `reverify_for_action(query, target_key, reverifier)→(ImpactReport, bool)`, `KnowledgeGate.assess(...)→KnowledgeAssessment(target, impact, pii_sources, escalate_steps, reverified, .summary)`, `link_lineage(store, upstream=, downstream=, edge_type=, source=)`, `confidence_for_method`, `LOW_CONFIDENCE`, `LINEAGE_EDGES`. Builder fixture symbols (`DBT_MANIFEST`, `DBT_CATALOG`, `QUERY_LOG`, `ORCH_DAG`, `seed_catalog`) match between `eval/sim/estate.py`, `eval/golden/graph.py`, and `tests/test_graph_builders.py::MultiSourceTest`. Confidence priors are consistent (dbt/introspection 0.95, orchestration 0.80, query_log 0.60, heuristic 0.40) everywhere they are asserted.

**Scope note:** this is one coherent subsystem (the estate knowledge graph) built in dependency order, so it is a single plan rather than several — but it is large. Each milestone leaves the suite green and is independently committable, so it can be paused/resumed cleanly at any milestone boundary.
