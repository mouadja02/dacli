"""Task DAG & planner (𝒪) — workstream 6.2.

The opaque ``while iteration < max_iterations`` counter is replaced by an
explicit **dependency DAG of subtasks**. Decomposing a goal ("stand up a
Bronze→Silver→Gold pipeline for the CRM source") into a DAG makes three things
first-class that a flat loop hides:

* **dependencies** — Silver can't start before Bronze; the DAG encodes it,
* **parallelism** — independent nodes ("profile all 14 tables") fan out,
* **resumability** — a node that pauses on an irreversible-action gate leaves the
  completed nodes completed; the branch resumes without redoing work.

Each subtask carries **explicit success criteria**, which become the node's
post-conditions: a node is "done" only when its criteria verify, never
just because its step returned.

A **complexity gate** keeps this from becoming ceremony: a goal that decomposes
to fewer than ``complexity_gate`` subtasks is *not* worth a DAG — the orchestrator
runs it single-step (the router still picks tool vs. sandbox). The
planner is heuristic and offline-safe; a cheap model may *refine* a draft but is
never required.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class NodeStatus(str, Enum):
    PENDING = "pending"        # not started; deps may be unmet
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"          # stopped at a gate (e.g. irreversible action) awaiting approval


# Verbs that imply an irreversible / high-blast-radius step → the node is marked
# so the loop pauses it for approval (resumable). Kept aligned with the
# governance intuition, but here it's only a *hint* that gates the branch.
_IRREVERSIBLE_MARKERS = [
    "drop", "delete", "truncate", "overwrite", "replace into", "purge",
    "remove", "destroy", "rewrite", "force push",
]
# Breadth-first markers: work that repeats the same step over many objects →
# a candidate for parallel sub-agents (6.5).
_BREADTH_MARKERS = [
    "all tables", "every table", "each table", "all schemas", "every schema",
    "all sources", "every source", "each source", "profile all", "introspect every",
    "across all",
]
# Sequencing connectives that split a compound goal into ordered steps.
_SEQUENCE_SPLITS = [
    r"\bthen\b", r"\band then\b", r"\bafter that\b", r"\bfollowed by\b",
    r"\bnext\b", r"\bfinally\b", r"->", r"→", r";",
]


@dataclass
class Subtask:
    """One node in the plan DAG."""

    id: str
    description: str
    depends_on: list[str] = field(default_factory=list)
    success_criteria: list[str] = field(default_factory=list)
    tier: str = "tool"                 # router hint (tool|sandbox)
    irreversible: bool = False         # pause for approval before running
    breadth_first: bool = False        # fan out to parallel sub-agents
    items: list[str] = field(default_factory=list)  # the objects a breadth-first node iterates
    status: NodeStatus = NodeStatus.PENDING
    result: Any = None
    error: str | None = None
    attempts: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "description": self.description,
            "depends_on": list(self.depends_on),
            "success_criteria": list(self.success_criteria),
            "tier": self.tier,
            "irreversible": self.irreversible,
            "breadth_first": self.breadth_first,
            "items": list(self.items),
            "status": self.status.value,
            "error": self.error,
            "attempts": self.attempts,
        }


class CyclicPlanError(ValueError):
    """Raised when the declared dependencies form a cycle (not a DAG)."""


class TaskDAG:
    """A dependency DAG of subtasks with first-class readiness + resumability."""

    def __init__(self, goal: str, nodes: list[Subtask] | None = None):
        self.goal = goal
        self._nodes: dict[str, Subtask] = {}
        for node in nodes or []:
            self.add(node)

    # ------------------------------------------------------------------
    def add(self, node: Subtask) -> Subtask:
        if node.id in self._nodes:
            raise ValueError(f"duplicate subtask id: {node.id}")
        self._nodes[node.id] = node
        return node

    @property
    def nodes(self) -> list[Subtask]:
        return list(self._nodes.values())

    def get(self, node_id: str) -> Subtask | None:
        return self._nodes.get(node_id)

    def __len__(self) -> int:
        return len(self._nodes)

    # ------------------------------------------------------------------
    def validate(self) -> None:
        """Ensure every dependency exists and the graph is acyclic."""
        for node in self._nodes.values():
            for dep in node.depends_on:
                if dep not in self._nodes:
                    raise ValueError(f"subtask '{node.id}' depends on unknown '{dep}'")
        self.topological_order()  # raises CyclicPlanError on a cycle

    def topological_order(self) -> list[Subtask]:
        """Kahn's algorithm — also the cycle detector."""
        indegree = dict.fromkeys(self._nodes, 0)
        for node in self._nodes.values():
            indegree[node.id] += len(node.depends_on)
        queue = [nid for nid, d in indegree.items() if d == 0]
        order: list[str] = []
        while queue:
            nid = queue.pop(0)
            order.append(nid)
            for other in self._nodes.values():
                if nid in other.depends_on:
                    indegree[other.id] -= 1
                    if indegree[other.id] == 0:
                        queue.append(other.id)
        if len(order) != len(self._nodes):
            raise CyclicPlanError(f"dependency cycle in plan for goal: {self.goal!r}")
        return [self._nodes[nid] for nid in order]

    # ------------------------------------------------------------------
    # Execution-state queries (drive the loop, 6.1)
    # ------------------------------------------------------------------
    def _deps_satisfied(self, node: Subtask) -> bool:
        return all(
            self._nodes[d].status == NodeStatus.COMPLETED
            for d in node.depends_on
            if d in self._nodes
        )

    def ready(self) -> list[Subtask]:
        """Pending nodes whose dependencies are all completed — runnable now.

        Multiple ready nodes with no path between them are safe to run in
        parallel (the basis for sub-agent fan-out).
        """
        return [
            n for n in self.topological_order()
            if n.status == NodeStatus.PENDING and self._deps_satisfied(n)
        ]

    def parallel_groups(self) -> list[list[Subtask]]:
        """Topological *levels*: each inner list can run concurrently.

        A node sits at level = 1 + max(level(dep)); nodes sharing a level have no
        dependency between them.
        """
        level: dict[str, int] = {}
        for node in self.topological_order():
            level[node.id] = (
                0 if not node.depends_on
                else 1 + max(level[d] for d in node.depends_on if d in level)
            )
        groups: dict[int, list[Subtask]] = {}
        for nid, lvl in level.items():
            groups.setdefault(lvl, []).append(self._nodes[nid])
        return [groups[k] for k in sorted(groups)]

    def is_complete(self) -> bool:
        return all(n.status == NodeStatus.COMPLETED for n in self._nodes.values())

    def has_failures(self) -> bool:
        return any(n.status == NodeStatus.FAILED for n in self._nodes.values())

    def paused(self) -> list[Subtask]:
        return [n for n in self._nodes.values() if n.status == NodeStatus.PAUSED]

    def remaining(self) -> list[Subtask]:
        """Nodes still to do — the resumability surface (completed ones excluded)."""
        return [
            n for n in self._nodes.values()
            if n.status not in (NodeStatus.COMPLETED, NodeStatus.FAILED)
        ]

    # ------------------------------------------------------------------
    def render(self) -> str:
        """Human-inspectable plan, presented for approval (plan-approve-execute)."""
        lines = [f"Plan for: {self.goal}", ""]
        for i, node in enumerate(self.topological_order(), 1):
            deps = f" (after {', '.join(node.depends_on)})" if node.depends_on else ""
            flags = []
            if node.breadth_first:
                flags.append(f"breadth-first ×{len(node.items) or '?'}")
            if node.irreversible:
                flags.append("irreversible — needs approval")
            flag_str = f"  [{'; '.join(flags)}]" if flags else ""
            lines.append(f"{i}. [{node.id}] {node.description}{deps}{flag_str}")
            for crit in node.success_criteria:
                lines.append(f"      ✓ {crit}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        return {"goal": self.goal, "nodes": [n.to_dict() for n in self.topological_order()]}


class Planner:
    """Decomposes a goal into a :class:`TaskDAG`, with a complexity gate.

    ``llm`` is optional and only used to *refine* a heuristic draft (cheap tier);
    the heuristic path is deterministic and offline-safe, so plans are stable and
    testable. Decomposition is the easy half — the planner stays simple on
    purpose and leaves *collaboration* to the loop + blackboard.
    """

    def __init__(self, llm: Any = None, *, complexity_gate: int = 2):
        self._llm = llm
        self.complexity_gate = complexity_gate

    # ------------------------------------------------------------------
    def _split_steps(self, goal: str) -> list[str]:
        pattern = "|".join(_SEQUENCE_SPLITS)
        parts = re.split(pattern, goal, flags=re.IGNORECASE)
        return [p.strip(" .,-").strip() for p in parts if p and p.strip(" .,-").strip()]

    @staticmethod
    def _is_irreversible(text: str) -> bool:
        low = text.lower()
        return any(m in low for m in _IRREVERSIBLE_MARKERS)

    @staticmethod
    def _breadth_items(text: str) -> list[str]:
        """Best-effort extraction of an explicit object list for a breadth node.

        Pulls a count ("profile all 14 tables") into placeholder item slots so the
        plan is inspectable; the real object names are resolved at run time from
        the catalog. Falls back to a single generic slot.
        """
        m = re.search(r"\ball\s+(\d+)\b", text.lower())
        if m:
            n = int(m.group(1))
            return [f"item_{i+1}" for i in range(min(n, 50))]
        return []

    @staticmethod
    def _criteria_for(step: str) -> list[str]:
        low = step.lower()
        crits: list[str] = []
        if any(w in low for w in ("create", "build", "stand up", "set up")):
            crits.append("the created object exists in the live catalog with the intended schema")
        if any(w in low for w in ("load", "ingest", "copy", "backfill")):
            crits.append("row count is greater than zero and matches the source")
        if any(w in low for w in ("profile", "introspect", "describe", "analyze")):
            crits.append("a profile is produced for every targeted object")
        if any(w in low for w in ("diff", "compare", "reconcile", "validate", "test")):
            crits.append("the comparison/test reports zero unexplained discrepancies")
        if not crits:
            crits.append("the step's stated outcome is observable in the environment")
        return crits

    def _tier_for(self, step: str) -> str:
        low = step.lower()
        if any(w in low for w in ("diff", "compare", "reconcile", "pipeline", "migrate", "backfill", "across")):
            return "sandbox"
        if any(m in low for m in _BREADTH_MARKERS):
            return "sandbox"
        return "tool"

    # ------------------------------------------------------------------
    def is_complex(self, goal: str) -> bool:
        """The complexity gate: does this goal warrant a DAG at all?"""
        steps = self._split_steps(goal)
        breadth = any(m in goal.lower() for m in _BREADTH_MARKERS)
        return len(steps) >= self.complexity_gate or breadth

    def decompose(self, goal: str) -> TaskDAG:
        """Heuristic decomposition into a validated, chained DAG.

        Sequencing connectives ("then", "→", ";") become chained dependencies;
        a breadth-first phrase becomes a single fan-out node. A goal with no
        connectives yields a single node (still a valid one-node DAG, which the
        complexity gate may choose to run single-step).
        """
        steps = self._split_steps(goal) or [goal.strip()]
        nodes: list[Subtask] = []
        prev_id: str | None = None
        for i, step in enumerate(steps, 1):
            nid = f"s{i}"
            breadth = any(m in step.lower() for m in _BREADTH_MARKERS)
            node = Subtask(
                id=nid,
                description=step,
                depends_on=[prev_id] if prev_id else [],
                success_criteria=self._criteria_for(step),
                tier=self._tier_for(step),
                irreversible=self._is_irreversible(step),
                breadth_first=breadth,
                items=self._breadth_items(step) if breadth else [],
            )
            nodes.append(node)
            prev_id = nid

        dag = TaskDAG(goal, nodes)
        dag.validate()
        return dag
