"""Semantic memory: durable facts.

Platform configs, naming conventions, and learned constraints — facts that hold
across tasks and sessions. A thin typed facade over :class:`MemoryStore` that
fixes ``kind="semantic"`` and routes retrieval through the staleness-aware
ranking.
"""

from __future__ import annotations

from typing import Any

from dacli.memory.store import MemoryEntry, MemoryKind, MemoryScope, MemoryStore
from dacli.memory.retrieval import retrieve


class SemanticMemory:
    def __init__(self, store: MemoryStore):
        self._store = store

    def add(
        self,
        content: str,
        *,
        scope: dict[str, Any] | None = None,
        source: str = "inference",
        confidence: float | None = None,
        tags: list[str] | None = None,
        memory_scope: str = MemoryScope.PROJECT.value,
    ) -> MemoryEntry:
        return self._store.remember(
            content,
            kind=MemoryKind.SEMANTIC.value,
            scope=scope,
            source=source,
            confidence=confidence,
            tags=tags,
            memory_scope=memory_scope,
        )

    def all(self) -> list[MemoryEntry]:
        return self._store.active(kind=MemoryKind.SEMANTIC.value)

    def search(self, query: str, top_k: int = 5) -> list[MemoryEntry]:
        return retrieve(query, self.all(), top_k=top_k)
