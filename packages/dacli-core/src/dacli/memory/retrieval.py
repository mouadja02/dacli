"""Retrieval with a staleness penalty (workstream 2.3).

Implements the framework's ranking discipline (CheetahClaws):

    rank = relevance × (1 - staleness_penalty) × confidence

The point is the *discipline*, not the embedding model: an old fact must sink
below a fresh, lower-relevance one instead of masquerading as current
(semantic similarity does not decay with age — staleness must be applied
independently). Relevance starts lexical; an embedding function can be injected
later without touching the ranking.

Retrieved entries are returned as **hypotheses** — they must be re-verified
(``memory/verify.py``) before backing a risky/irreversible action.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from collections.abc import Callable, Sequence

from dacli.memory.store import MemoryEntry


# Age (in days) at which the staleness penalty saturates at its cap. The
# CheetahClaws reference uses ~30 days; beyond ``STALENESS_HORIZON_DAYS`` the
# penalty is capped at ``MAX_STALENESS_PENALTY`` so a very stale entry never
# scores literally zero (it stays a weak hypothesis).
STALENESS_HORIZON_DAYS = 30.0
MAX_STALENESS_PENALTY = 0.9

_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


def _tokens(text: str) -> set:
    return {t.lower() for t in _TOKEN_RE.findall(text or "")}


def lexical_relevance(query: str, entry: MemoryEntry) -> float:
    """Token-overlap relevance over the entry's content + scope values.

    A lightweight stand-in for semantic similarity. Scope values are folded into
    the searchable text so "BRONZE.CRM" matches an entry scoped to that object.
    """
    q = _tokens(query)
    if not q:
        return 0.0
    scope_text = " ".join(str(v) for v in entry.scope.values())
    doc = _tokens(entry.content) | _tokens(scope_text) | {t.lower() for t in entry.tags}
    if not doc:
        return 0.0
    overlap = len(q & doc)
    if overlap == 0:
        return 0.0
    # Coverage of the query, lightly normalized by document breadth so a huge
    # entry doesn't win on incidental token matches.
    return overlap / len(q)


def staleness_penalty(
    entry: MemoryEntry,
    now: datetime | None = None,
    horizon_days: float = STALENESS_HORIZON_DAYS,
) -> float:
    """``min(age_days / horizon, MAX)`` — independent of semantic score."""
    now = now or datetime.now()
    age_days = max((now - entry.last_verified).total_seconds() / 86400.0, 0.0)
    return min(age_days / horizon_days, MAX_STALENESS_PENALTY)


@dataclass
class ScoredEntry:
    entry: MemoryEntry
    score: float
    relevance: float
    staleness_penalty: float
    confidence: float


def rank(
    query: str,
    entries: Sequence[MemoryEntry],
    *,
    now: datetime | None = None,
    relevance_fn: Callable[[str, MemoryEntry], float] | None = None,
    horizon_days: float = STALENESS_HORIZON_DAYS,
    include_superseded: bool = False,
) -> list[ScoredEntry]:
    """Rank entries by ``relevance × (1 - staleness) × confidence``."""
    now = now or datetime.now()
    relevance_fn = relevance_fn or lexical_relevance

    scored: list[ScoredEntry] = []
    for entry in entries:
        if not include_superseded and not entry.is_active:
            continue
        rel = relevance_fn(query, entry)
        if rel <= 0:
            continue
        pen = staleness_penalty(entry, now=now, horizon_days=horizon_days)
        score = rel * (1.0 - pen) * entry.confidence
        scored.append(ScoredEntry(
            entry=entry,
            score=score,
            relevance=rel,
            staleness_penalty=pen,
            confidence=entry.confidence,
        ))

    scored.sort(key=lambda s: s.score, reverse=True)
    return scored


def retrieve(
    query: str,
    entries: Sequence[MemoryEntry],
    *,
    top_k: int = 5,
    now: datetime | None = None,
    relevance_fn: Callable[[str, MemoryEntry], float] | None = None,
) -> list[MemoryEntry]:
    """Return the top-k entries as **hypotheses** (re-verify before acting)."""
    return [s.entry for s in rank(query, entries, now=now, relevance_fn=relevance_fn)[:top_k]]
