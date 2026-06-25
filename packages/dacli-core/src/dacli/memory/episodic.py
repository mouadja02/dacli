"""Episodic memory (workstream 2.5): task traces.

On task completion the agent stores the trace (goal -> tool calls -> outcomes)
as an episodic entry. Past traces are reusable later as programs (AgentSM-style:
replaying successful trajectories raises accuracy). distills the good
ones into procedural runbooks.

Episodic capture is allowed to trail the reliability core (catalog + staleness
ranking); it is intentionally lightweight here.
"""

from __future__ import annotations

import json
from typing import Any

from dacli.memory.store import MemoryEntry, MemoryKind, MemoryScope, MemoryStore
from dacli.memory.retrieval import retrieve


def _summarize_steps(steps: list[dict[str, Any]]) -> str:
    lines = []
    for i, step in enumerate(steps, 1):
        name = step.get("tool") or step.get("name") or "step"
        status = step.get("status", "")
        lines.append(f"  {i}. {name} -> {status}".rstrip())
    return "\n".join(lines)


class EpisodicMemory:
    def __init__(self, store: MemoryStore):
        self._store = store

    def capture(
        self,
        goal: str,
        steps: list[dict[str, Any]],
        *,
        outcome: str = "completed",
        scope: dict[str, Any] | None = None,
        tags: list[str] | None = None,
    ) -> MemoryEntry:
        """Store one task trace.

        Confidence reflects the *outcome*: a clean completion is a more reliable
        exemplar than a failed run (which is still worth keeping for learning).
        """
        content = (
            f"GOAL: {goal}\n"
            f"OUTCOME: {outcome}\n"
            f"TRACE ({len(steps)} steps):\n{_summarize_steps(steps)}"
        )
        confidence = 0.8 if outcome == "completed" else 0.4
        entry = self._store.remember(
            content,
            kind=MemoryKind.EPISODIC.value,
            scope=scope,
            source="episode",
            confidence=confidence,
            tags=(tags or []) + ["episode", outcome],
            memory_scope=MemoryScope.PROJECT.value,
        )
        # Stash the structured trace for later distillation.
        entry.scope.setdefault("_trace", json.dumps(steps, default=str))
        return entry

    def all(self) -> list[MemoryEntry]:
        return self._store.active(kind=MemoryKind.EPISODIC.value)

    def search(self, query: str, top_k: int = 3) -> list[MemoryEntry]:
        return retrieve(query, self.all(), top_k=top_k)
