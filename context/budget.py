"""Token budget accounting.

Tracks tokens *per source* (priors / memory / live / skills / history) with
per-source caps, so no single source — typically a chatty tool result — can crowd
out the task. Budgets are config-driven per model (see ``ContextSettings`` in
``config/settings.py``).

The assembler (3.1) asks the :class:`BudgetTracker` whether a candidate chunk
fits before placing it; pinned chunks (current task, latest tool result) are
charged but never rejected — losing them would defeat the point.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Canonical source ids. Kept as constants so the assembler, budget and
# ``--explain`` renderer all agree on the labels.
PRIORS = "priors"
MEMORY = "memory"
LIVE = "live"
SKILLS = "skills"
HISTORY = "history"
PINNED = "pinned"

SOURCES = (PRIORS, MEMORY, LIVE, SKILLS, HISTORY)

# Default share of the total budget each source may consume. Pinned content is
# uncapped (charged against the total but never rejected). The fractions need not
# sum to 1: they are independent ceilings, and the *total* is the hard wall.
DEFAULT_FRACTIONS: dict[str, float] = {
    PRIORS: 0.20,
    MEMORY: 0.20,
    LIVE: 0.25,
    SKILLS: 0.15,
    HISTORY: 0.55,
}


@dataclass
class Budget:
    """Immutable budget spec: a total and per-source fractional ceilings."""

    total: int
    fractions: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_FRACTIONS))

    def cap(self, source: str) -> int:
        """Token ceiling for ``source`` (the total for pinned/unknown sources)."""
        if source == PINNED:
            return self.total
        frac = self.fractions.get(source)
        if frac is None:
            return self.total
        return int(self.total * frac)

    @classmethod
    def from_settings(cls, settings) -> Budget:
        """Build from ``settings.context`` (falls back to defaults)."""
        ctx = getattr(settings, "context", None)
        total = getattr(ctx, "budget_tokens", None) or 12000
        fractions = dict(DEFAULT_FRACTIONS)
        configured = getattr(ctx, "source_fractions", None)
        if configured:
            fractions.update(configured)
        return cls(total=total, fractions=fractions)


class BudgetTracker:
    """Mutable usage ledger for one context assembly."""

    def __init__(self, budget: Budget):
        self.budget = budget
        self._used: dict[str, int] = dict.fromkeys((*SOURCES, PINNED), 0)

    @property
    def total_used(self) -> int:
        return sum(self._used.values())

    @property
    def total_remaining(self) -> int:
        return max(self.budget.total - self.total_used, 0)

    def used(self, source: str) -> int:
        return self._used.get(source, 0)

    def source_remaining(self, source: str) -> int:
        """Tokens still available to ``source`` (min of its cap and the total)."""
        cap_left = max(self.budget.cap(source) - self._used.get(source, 0), 0)
        return min(cap_left, self.total_remaining)

    def fits(self, source: str, tokens: int) -> bool:
        """True if ``tokens`` fit under both the source cap and the total."""
        return tokens <= self.source_remaining(source)

    def charge(self, source: str, tokens: int) -> None:
        """Record usage. Pinned content uses this directly (bypassing ``fits``)."""
        self._used[source] = self._used.get(source, 0) + tokens

    def add(self, source: str, tokens: int, *, pinned: bool = False) -> bool:
        """Try to place ``tokens`` for ``source``.

        Pinned content is always charged and returns True (never rejected).
        Otherwise charge only if it fits; returns whether it was placed.
        """
        if pinned:
            self.charge(PINNED, tokens)
            return True
        if not self.fits(source, tokens):
            return False
        self.charge(source, tokens)
        return True

    def snapshot(self) -> dict[str, dict[str, int]]:
        """Per-source ``{used, cap}`` plus a ``total`` row, for ``--explain``."""
        out: dict[str, dict[str, int]] = {}
        for source in (*SOURCES, PINNED):
            out[source] = {"used": self._used.get(source, 0), "cap": self.budget.cap(source)}
        out["total"] = {"used": self.total_used, "cap": self.budget.total}
        return out
