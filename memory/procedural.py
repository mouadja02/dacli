"""Procedural memory (Phase 2): distilled runbooks.

Reusable, generalized procedures promoted from successful episodes. **Phase 8
writes these** (the distillation step); Phase 2 defines the type and the
retrieval path so the rest of the harness can already consume runbooks.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from memory.store import MemoryEntry, MemoryKind, MemoryScope, MemoryStore
from memory.retrieval import retrieve


class ProceduralMemory:
    def __init__(self, store: MemoryStore):
        self._store = store

    def add_runbook(
        self,
        name: str,
        steps: str,
        *,
        scope: Optional[Dict[str, Any]] = None,
        source: str = "distillation",
        confidence: float = 0.85,
        derived_from: Optional[List[str]] = None,
    ) -> MemoryEntry:
        content = f"RUNBOOK: {name}\n{steps}"
        tags = ["runbook", name]
        if derived_from:
            tags += [f"from:{eid}" for eid in derived_from]
        return self._store.remember(
            content,
            kind=MemoryKind.PROCEDURAL.value,
            scope=scope,
            source=source,
            confidence=confidence,
            tags=tags,
            memory_scope=MemoryScope.PROJECT.value,
        )

    def all(self) -> List[MemoryEntry]:
        return self._store.active(kind=MemoryKind.PROCEDURAL.value)

    def search(self, query: str, top_k: int = 3) -> List[MemoryEntry]:
        return retrieve(query, self.all(), top_k=top_k)
