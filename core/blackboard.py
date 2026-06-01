"""The blackboard (𝒪) — workstream 6.6.

Multi-agent's documented failures are *collaboration* failures, not
decomposition ones: inter-agent misalignment, task duplication, and — most
dangerous — missing contradiction detection (two agents quietly assert different
facts about the same object and the lead never notices). The blackboard is the
collaboration substrate that targets those directly.

It is shared state every agent reads and writes:

* **assertions** — facts agents discover, keyed by the object they describe
  (``snowflake:BRONZE.RAW.CUSTOMERS#column:ID``). Writing a *different* value for
  a key another agent already wrote raises a logged **contradiction** the lead
  resolves before the result is accepted.
* **decisions** — choices the lead/agents commit to (append-only).
* **open_questions** — things nobody has resolved yet.
* **task claims** — who owns which subtask, so two agents don't both do it.

Writes follow the same **supersession discipline** as memory: nothing is
mutated in place; a new write that agrees refreshes provenance, a new write that
disagrees opens a contradiction rather than silently overwriting. All state is
append-only and serializable so a run is reconstructable end to end.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


def _now() -> str:
    return datetime.now().isoformat()


@dataclass
class Assertion:
    """One agent's claim about one object, with provenance."""

    key: str
    value: Any
    agent: str
    confidence: float = 1.0
    superseded: bool = False
    timestamp: str = field(default_factory=_now)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Contradiction:
    """Two agents asserting incompatible values for the same key."""

    key: str
    existing: Assertion
    incoming: Assertion
    resolved: bool = False
    resolution: Optional[str] = None
    resolver: Optional[str] = None
    timestamp: str = field(default_factory=_now)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["existing"] = self.existing.to_dict()
        d["incoming"] = self.incoming.to_dict()
        return d


@dataclass
class TaskClaim:
    task_id: str
    agent: str
    timestamp: str = field(default_factory=_now)

    def to_dict(self) -> dict:
        return asdict(self)


class Blackboard:
    """Shared, append-only, contradiction-aware state for collaborating agents.

    Thread/async-safe via a single lock — parallel sub-agents (6.5) write
    concurrently, and a lost update here would re-open the very misalignment the
    blackboard exists to prevent. ``values_equal`` decides whether two writes for
    a key agree (default: structural equality after light normalization), so a
    domain can treat e.g. ``"NUMBER"`` and ``"NUMERIC"`` as the same type.
    """

    def __init__(
        self,
        *,
        path: Optional[str] = None,
        values_equal: Optional[Any] = None,
    ):
        self._lock = threading.RLock()
        self._assertions: Dict[str, List[Assertion]] = {}
        self._decisions: List[Dict[str, Any]] = []
        self._open_questions: List[Dict[str, Any]] = []
        self._contradictions: List[Contradiction] = []
        self._claims: Dict[str, TaskClaim] = {}
        self._path = Path(path) if path else None
        if self._path is not None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
        self._equal = values_equal or self._default_equal

    # ------------------------------------------------------------------
    @staticmethod
    def _default_equal(a: Any, b: Any) -> bool:
        def norm(x: Any) -> Any:
            if isinstance(x, str):
                return x.strip().lower()
            return x
        return norm(a) == norm(b)

    # ------------------------------------------------------------------
    # Assertions + contradiction detection
    # ------------------------------------------------------------------
    def assert_fact(
        self,
        key: str,
        value: Any,
        agent: str,
        *,
        confidence: float = 1.0,
    ) -> Optional[Contradiction]:
        """Record an agent's claim about ``key``.

        Returns a :class:`Contradiction` (also logged) when the active assertion
        for ``key`` was made by a *different* agent with an *incompatible* value;
        otherwise returns None. A matching re-assertion is recorded as fresh
        provenance (and can raise confidence) without conflict.
        """
        incoming = Assertion(key=key, value=value, agent=agent, confidence=confidence)
        with self._lock:
            history = self._assertions.setdefault(key, [])
            active = next((a for a in reversed(history) if not a.superseded), None)

            if active is None:
                history.append(incoming)
                self._persist()
                return None

            if self._equal(active.value, value):
                # Agreement: keep the record, no contradiction. Append so the
                # provenance trail shows independent corroboration.
                history.append(incoming)
                self._persist()
                return None

            # Disagreement on the same object → a contradiction for the lead.
            contradiction = Contradiction(key=key, existing=active, incoming=incoming)
            self._contradictions.append(contradiction)
            # The incoming assertion is parked (not active) until resolution, so
            # downstream reads still see one coherent value, not a flicker.
            incoming.superseded = True
            history.append(incoming)
            self._persist()
            return contradiction

    def get(self, key: str) -> Optional[Any]:
        """The current accepted value for ``key`` (latest non-superseded)."""
        with self._lock:
            history = self._assertions.get(key, [])
            active = next((a for a in reversed(history) if not a.superseded), None)
            return active.value if active else None

    def assertions(self, key: Optional[str] = None) -> List[Assertion]:
        with self._lock:
            if key is not None:
                return list(self._assertions.get(key, []))
            return [a for hist in self._assertions.values() for a in hist]

    # ------------------------------------------------------------------
    # Contradiction resolution (the lead arbitrates)
    # ------------------------------------------------------------------
    def contradictions(self, *, unresolved_only: bool = False) -> List[Contradiction]:
        with self._lock:
            if unresolved_only:
                return [c for c in self._contradictions if not c.resolved]
            return list(self._contradictions)

    def resolve_contradiction(
        self,
        contradiction: Contradiction,
        *,
        winner: Assertion,
        resolver: str = "lead",
        note: str = "",
    ) -> None:
        """Settle a contradiction by promoting ``winner`` to the active value."""
        with self._lock:
            key = contradiction.key
            history = self._assertions.setdefault(key, [])
            for a in history:
                a.superseded = True
            promoted = Assertion(
                key=key, value=winner.value, agent=winner.agent,
                confidence=winner.confidence,
            )
            history.append(promoted)
            contradiction.resolved = True
            contradiction.resolver = resolver
            contradiction.resolution = note or f"kept {winner.agent}'s value"
            self._persist()

    # ------------------------------------------------------------------
    # Task claims (de-duplication)
    # ------------------------------------------------------------------
    def claim_task(self, task_id: str, agent: str) -> bool:
        """Claim a subtask. Returns False if another agent already owns it."""
        with self._lock:
            existing = self._claims.get(task_id)
            if existing is not None and existing.agent != agent:
                return False
            self._claims[task_id] = TaskClaim(task_id=task_id, agent=agent)
            self._persist()
            return True

    def claimant(self, task_id: str) -> Optional[str]:
        with self._lock:
            claim = self._claims.get(task_id)
            return claim.agent if claim else None

    # ------------------------------------------------------------------
    # Decisions + open questions (append-only narrative)
    # ------------------------------------------------------------------
    def record_decision(self, what: str, *, agent: str = "lead", **extra: Any) -> None:
        with self._lock:
            self._decisions.append({"what": what, "agent": agent, "ts": _now(), **extra})
            self._persist()

    def decisions(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._decisions)

    def add_open_question(self, question: str, *, agent: str = "lead") -> None:
        with self._lock:
            self._open_questions.append({"question": question, "agent": agent, "ts": _now()})
            self._persist()

    def open_questions(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._open_questions)

    # ------------------------------------------------------------------
    def snapshot(self) -> Dict[str, Any]:
        """A serializable view of the whole board (audit / debugging)."""
        with self._lock:
            return {
                "assertions": {k: [a.to_dict() for a in v] for k, v in self._assertions.items()},
                "decisions": list(self._decisions),
                "open_questions": list(self._open_questions),
                "contradictions": [c.to_dict() for c in self._contradictions],
                "claims": {k: v.to_dict() for k, v in self._claims.items()},
            }

    def _persist(self) -> None:
        # Best-effort snapshot to disk — collaboration state must never break the
        # run, so a write failure is swallowed (the in-memory board is canonical).
        if self._path is None:
            return
        try:
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(self.snapshot(), f, indent=2, default=str)
        except Exception:
            pass
