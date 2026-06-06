"""Lead + isolated-context sub-agents (𝒪) — workstream 6.5.

The highest-risk capability, deliberately built last (on top of a proven
single-agent loop). Per Anthropic's sub-agent pattern: the **lead decomposes**,
and **sub-agents execute subtasks in their own clean context windows**, returning
**condensed summaries (~1–2k tokens)** rather than their full working transcript.
This is what keeps total context bounded even as breadth grows — the documented
+90.2% on breadth-first research came *with* large token cost, so the mitigations
are first-class here: sub-agents only for breadth-first/large work, condensed
returns, and a hard cap on fan-out.

Sub-agents are **opt-in**, not the default. The win case is breadth-first data
work — "profile all 14 tables", "introspect every connected platform's catalog"
→ one focused sub-agent per object, run in parallel, results merged by the lead.

Collaboration (not just decomposition) is the hard part, so the lead merges
through the :class:`~core.blackboard.Blackboard`: every sub-agent fact is
asserted there, and **conflicting facts about the same object raise a logged
contradiction the lead resolves** before accepting the merged result. That
directly targets the documented multi-agent failures: misalignment, duplication,
missing contradiction detection.

Offline-safe + decoupled: the lead is parameterized by a ``worker`` (how a
sub-agent actually does its task — kernel, dispatcher, or a fake), so the whole
fan-out/merge/contradiction path is testable without a live model.
"""

from __future__ import annotations

from core.logging_setup import get_logger

log = get_logger(__name__)

import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional

from core.blackboard import Blackboard


def _estimate_tokens(text: str) -> int:
    # Cheap, dependency-free estimate (~4 chars/token). Good enough to *bound*
    # context; exact accounting is the tokenizer's job elsewhere.
    return (len(text) + 3) // 4 if text else 0


def _truncate_to_tokens(text: str, max_tokens: int) -> str:
    if _estimate_tokens(text) <= max_tokens:
        return text
    keep = max(0, max_tokens * 4 - 1)
    return text[:keep].rstrip() + "…"


@dataclass
class Assignment:
    """A single focused unit of work handed to one isolated-context sub-agent."""

    agent_id: str
    task: str
    item: Optional[str] = None      # the specific object (e.g. a table name)
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkerOutput:
    """What a worker returns for one assignment (before condensation)."""

    text: str = ""                   # raw findings (may be long — gets condensed)
    facts: Dict[str, Any] = field(default_factory=dict)  # key -> value to assert
    confidence: float = 1.0
    success: bool = True
    error: Optional[str] = None
    tokens: int = 0                  # tokens the worker itself spent (for budget tracking)


# A worker runs one assignment in its own clean context and returns findings.
Worker = Callable[[Assignment], Awaitable[WorkerOutput]]


@dataclass
class SubAgentResult:
    agent_id: str
    task: str
    item: Optional[str]
    summary: str                     # condensed, token-bounded
    facts: Dict[str, Any]
    confidence: float
    success: bool
    error: Optional[str]
    summary_tokens: int
    worker_tokens: int = 0


@dataclass
class LeadReport:
    """The merged outcome the lead folds back into its own context."""

    task: str
    results: List[SubAgentResult]
    merged_summary: str
    contradictions_resolved: int
    failures: List[str] = field(default_factory=list)
    merged_tokens: int = 0
    worker_tokens_total: int = 0

    @property
    def context_bounded(self) -> bool:
        # Sanity flag: the lead-facing merged summary is what enters context.
        return True


class SubAgent:
    """One isolated-context worker. Runs a focused assignment, returns a digest.

    The sub-agent sees *only* its assignment — not the lead's full context — which
    is the whole point: parallel clean windows. Its return is condensed to the
    configured token bound so breadth doesn't blow up the lead's context.
    """

    def __init__(self, worker: Worker, *, summary_tokens: int = 2000):
        self._worker = worker
        self.summary_tokens = summary_tokens

    async def run(self, assignment: Assignment) -> SubAgentResult:
        try:
            out = await self._worker(assignment)
        except Exception as e:
            return SubAgentResult(
                agent_id=assignment.agent_id, task=assignment.task, item=assignment.item,
                summary=f"sub-agent error: {e}", facts={}, confidence=0.0,
                success=False, error=str(e), summary_tokens=0,
            )
        summary = _truncate_to_tokens(out.text or "", self.summary_tokens)
        return SubAgentResult(
            agent_id=assignment.agent_id, task=assignment.task, item=assignment.item,
            summary=summary, facts=dict(out.facts or {}), confidence=out.confidence,
            success=out.success, error=out.error,
            summary_tokens=_estimate_tokens(summary), worker_tokens=out.tokens,
        )


class Lead:
    """Decomposes a breadth-first task, fans out sub-agents, merges via blackboard."""

    def __init__(
        self,
        blackboard: Optional[Blackboard] = None,
        *,
        max_subagents: int = 6,
        summary_tokens: int = 2000,
        merged_tokens: Optional[int] = None,
        on_event: Optional[Callable[[str], None]] = None,
    ):
        self.blackboard = blackboard or Blackboard()
        self.max_subagents = max_subagents
        self.summary_tokens = summary_tokens
        # The merged digest the lead keeps is itself bounded — breadth must not
        # smuggle the whole fan-out back into context one summary at a time.
        self.merged_tokens = merged_tokens or (summary_tokens * 2)
        self._on_event = on_event

    def _emit(self, msg: str) -> None:
        if self._on_event:
            try:
                self._on_event(msg)
            except Exception:
                log.debug("on_event callback failed", exc_info=True)

    async def fan_out(
        self,
        task: str,
        items: List[str],
        worker: Worker,
    ) -> LeadReport:
        """Spawn one sub-agent per item (parallel, capped), then merge.

        ``items`` is the resolved breadth set (e.g. the 14 table names from the
        catalog). Each sub-agent is claimed on the blackboard first, so two
        sub-agents never duplicate the same object.
        """
        if not items:
            items = [task]

        sub = SubAgent(worker, summary_tokens=self.summary_tokens)
        semaphore = asyncio.Semaphore(self.max_subagents)

        async def _run_one(index: int, item: str) -> Optional[SubAgentResult]:
            agent_id = f"sub-{index}:{item}"
            # De-duplication: claim the item before working it.
            if not self.blackboard.claim_task(item, agent_id):
                self._emit(f"{agent_id} skipped — '{item}' already claimed by {self.blackboard.claimant(item)}")
                return None
            assignment = Assignment(agent_id=agent_id, task=task, item=item)
            async with semaphore:
                self._emit(f"{agent_id} working (isolated context)")
                return await sub.run(assignment)

        spawned = await asyncio.gather(
            *(_run_one(i, item) for i, item in enumerate(items, 1))
        )
        results = [r for r in spawned if r is not None]
        return self.merge(task, results)

    def merge(self, task: str, results: List[SubAgentResult]) -> LeadReport:
        """Fold sub-agent results into the blackboard, resolving contradictions.

        The lead arbitrates a contradiction by **higher confidence wins** (ties
        keep the existing assertion); every resolution is recorded as a decision,
        so the trail shows the conflict *and* how it was settled.
        """
        resolved = 0
        failures: List[str] = []
        for r in results:
            if not r.success:
                failures.append(f"{r.agent_id}: {r.error or 'failed'}")
                continue
            for key, value in r.facts.items():
                contradiction = self.blackboard.assert_fact(
                    key, value, r.agent_id, confidence=r.confidence
                )
                if contradiction is not None:
                    existing = contradiction.existing
                    incoming = contradiction.incoming
                    winner = incoming if incoming.confidence > existing.confidence else existing
                    self.blackboard.resolve_contradiction(
                        contradiction, winner=winner, resolver="lead",
                        note=(f"conflict on '{key}': {existing.agent}={existing.value!r} vs "
                              f"{incoming.agent}={incoming.value!r} → kept {winner.agent}'s "
                              f"(confidence {winner.confidence})"),
                    )
                    self.blackboard.record_decision(
                        f"resolved contradiction on '{key}' in favor of {winner.agent}",
                        agent="lead", key=key, winner=winner.agent,
                    )
                    self._emit(f"lead resolved contradiction on '{key}' → {winner.agent}")
                    resolved += 1

        merged = self._merge_summary(task, results)
        return LeadReport(
            task=task,
            results=results,
            merged_summary=merged,
            contradictions_resolved=resolved,
            failures=failures,
            merged_tokens=_estimate_tokens(merged),
            worker_tokens_total=sum(r.worker_tokens for r in results),
        )

    def _merge_summary(self, task: str, results: List[SubAgentResult]) -> str:
        """A single bounded digest of the fan-out — what enters the lead's context."""
        ok = [r for r in results if r.success]
        header = f"Merged {len(ok)}/{len(results)} sub-agent results for: {task}"
        lines = [header]
        for r in ok:
            label = r.item or r.agent_id
            # One condensed line per sub-agent; the full per-agent summary stays
            # off the lead's context (it's already on the blackboard if asserted).
            first_line = (r.summary or "").strip().splitlines()[0] if r.summary.strip() else "(no findings)"
            lines.append(f"- {label}: {_truncate_to_tokens(first_line, 40)}")
        digest = "\n".join(lines)
        return _truncate_to_tokens(digest, self.merged_tokens)
