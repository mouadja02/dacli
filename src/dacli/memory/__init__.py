"""Trustworthy memory (ℳ).

A general, typed, trust-aware memory where every fact carries confidence,
recency, and provenance, and where **trust is decided at runtime, not stored as
truth**. Plus a catalog cache that knows when it is stale — the antidote to the
*stale-but-confident* failure mode.
"""

from dacli.memory.store import (
    MemoryEntry,
    MemoryStore,
    MemoryKind,
    MemoryScope,
    VerificationStatus,
    confidence_for_source,
    CONFIDENCE_PRIORS,
    MAX_CONFIDENCE,
)
from dacli.memory.catalog import CatalogCache, CatalogEntry
from dacli.memory.retrieval import rank, retrieve, ScoredEntry, lexical_relevance, staleness_penalty
from dacli.memory.verify import (
    verify,
    needs_reverification,
    build_catalog_verifier,
    VerificationOutcome,
    Verifier,
)
from dacli.memory.semantic import SemanticMemory
from dacli.memory.episodic import EpisodicMemory
from dacli.memory.procedural import ProceduralMemory
from dacli.memory.priors import load_priors, find_priors_file, generate_dacli_md

__all__ = [
    "CONFIDENCE_PRIORS",
    "MAX_CONFIDENCE",
    "CatalogCache",
    "CatalogEntry",
    "EpisodicMemory",
    "MemoryEntry",
    "MemoryKind",
    "MemoryScope",
    "MemoryStore",
    "ProceduralMemory",
    "ScoredEntry",
    "SemanticMemory",
    "VerificationOutcome",
    "VerificationStatus",
    "Verifier",
    "build_catalog_verifier",
    "confidence_for_source",
    "find_priors_file",
    "generate_dacli_md",
    "lexical_relevance",
    "load_priors",
    "needs_reverification",
    "rank",
    "retrieve",
    "staleness_penalty",
    "verify",
]
