"""Trustworthy memory (ℳ).

A general, typed, trust-aware memory where every fact carries confidence,
recency, and provenance, and where **trust is decided at runtime, not stored as
truth**. Plus a catalog cache that knows when it is stale — the antidote to the
*stale-but-confident* failure mode.
"""

from memory.store import (
    MemoryEntry,
    MemoryStore,
    MemoryKind,
    MemoryScope,
    VerificationStatus,
    confidence_for_source,
    CONFIDENCE_PRIORS,
    MAX_CONFIDENCE,
)
from memory.catalog import CatalogCache, CatalogEntry
from memory.retrieval import rank, retrieve, ScoredEntry, lexical_relevance, staleness_penalty
from memory.verify import (
    verify,
    needs_reverification,
    build_catalog_verifier,
    VerificationOutcome,
    Verifier,
)
from memory.semantic import SemanticMemory
from memory.episodic import EpisodicMemory
from memory.procedural import ProceduralMemory
from memory.priors import load_priors, find_priors_file, generate_dacli_md

__all__ = [
    "MemoryEntry",
    "MemoryStore",
    "MemoryKind",
    "MemoryScope",
    "VerificationStatus",
    "confidence_for_source",
    "CONFIDENCE_PRIORS",
    "MAX_CONFIDENCE",
    "CatalogCache",
    "CatalogEntry",
    "rank",
    "retrieve",
    "ScoredEntry",
    "lexical_relevance",
    "staleness_penalty",
    "verify",
    "needs_reverification",
    "build_catalog_verifier",
    "VerificationOutcome",
    "Verifier",
    "SemanticMemory",
    "EpisodicMemory",
    "ProceduralMemory",
    "load_priors",
    "find_priors_file",
    "generate_dacli_md",
]
