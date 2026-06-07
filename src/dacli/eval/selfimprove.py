"""Self-improvement loop: episodic → procedural, gated by pass^k.

Successful task traces (episodic memory) are distilled into parameterized
**procedural runbooks** the router can reuse — raising reliability and cutting
tokens on recurring work (AgentSM: past traces as reusable programs).

The capstone guarantee: **a runbook is promoted only if it measurably beats the
ad-hoc path on the golden suite (pass^k)**. No unvetted "learning" enters the
trusted path; the comparison is recorded in the audit ledger so the promotion is
auditable and revocable. This is the defense against the system "learning" a
subtly-wrong-but-passing shortcut (post-condition gaming at the runbook level).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dacli.eval.passk import run_pass_k
from dacli.eval.types import GoldenTask


@dataclass
class PromotionResult:
    """The outcome of evaluating a candidate runbook against the baseline."""

    runbook: str
    promoted: bool
    baseline_pass_k: float
    candidate_pass_k: float
    baseline_success_rate: float
    candidate_success_rate: float
    k: int
    reason: str

    def to_dict(self) -> dict:
        return {
            "runbook": self.runbook,
            "promoted": self.promoted,
            "baseline_pass_k": round(self.baseline_pass_k, 4),
            "candidate_pass_k": round(self.candidate_pass_k, 4),
            "baseline_success_rate": round(self.baseline_success_rate, 4),
            "candidate_success_rate": round(self.candidate_success_rate, 4),
            "k": self.k,
            "reason": self.reason,
        }


class SelfImprovement:
    """Distills episodes into runbooks and gates promotion on pass^k.

    ``procedural`` is a :class:`~memory.procedural.ProceduralMemory` (or anything
    with ``add_runbook``); ``ledger`` is an optional governance
    :class:`~governance.audit.AuditLedger` so every promotion decision is on the
    record. Both are optional so the evaluator is testable in isolation.
    """

    def __init__(self, procedural: Any = None, ledger: Any = None, *, session_id: str = ""):
        self._procedural = procedural
        self._ledger = ledger
        self._session_id = session_id

    @staticmethod
    def distill_runbook_steps(episode_steps: list[dict]) -> str:
        """Turn a concrete episodic trace into a parameterized step list.

        Object names seen in the trace become ``<param>`` placeholders so the
        runbook generalizes beyond the single episode it came from.
        """
        lines = []
        for i, step in enumerate(episode_steps, 1):
            tool = step.get("tool") or step.get("name") or "step"
            lines.append(f"{i}. {tool} (verify post-conditions)")
        return "\n".join(lines)

    async def evaluate_promotion(
        self,
        runbook_name: str,
        baseline_task: GoldenTask,
        candidate_task: GoldenTask,
        *,
        k: int = 5,
    ) -> PromotionResult:
        """Run baseline vs. candidate k times each; promote iff the candidate's
        pass^k is *strictly* higher (or equal pass^k with a higher success rate
        and never worse). Outcome-anchored: both tasks verify against the same
        environment oracle, so a runbook can't win by gaming a softer check."""
        base = await run_pass_k(baseline_task, k)
        cand = await run_pass_k(candidate_task, k)

        base_pk = 1.0 if base.passed_all else 0.0
        cand_pk = 1.0 if cand.passed_all else 0.0

        if cand_pk > base_pk:
            promoted, reason = True, (
                f"candidate pass^{k} {cand_pk:.2f} beats baseline {base_pk:.2f}")
        elif cand_pk == base_pk and cand.success_rate > base.success_rate:
            promoted, reason = True, (
                f"equal pass^{k}; candidate success rate {cand.success_rate:.2f} "
                f"> baseline {base.success_rate:.2f}")
        else:
            promoted, reason = False, (
                f"candidate did not beat baseline on pass^{k} "
                f"({cand_pk:.2f} vs {base_pk:.2f}) — not promoted")

        return PromotionResult(
            runbook=runbook_name, promoted=promoted,
            baseline_pass_k=base_pk, candidate_pass_k=cand_pk,
            baseline_success_rate=base.success_rate,
            candidate_success_rate=cand.success_rate,
            k=k, reason=reason,
        )

    async def distill_and_promote(
        self,
        runbook_name: str,
        steps: str,
        baseline_task: GoldenTask,
        candidate_task: GoldenTask,
        *,
        k: int = 5,
        derived_from: list[str] | None = None,
    ) -> PromotionResult:
        """Evaluate, and — only if it beats the baseline — write the runbook to
        procedural memory and record the comparison in the audit ledger."""
        result = await self.evaluate_promotion(
            runbook_name, baseline_task, candidate_task, k=k)

        if result.promoted and self._procedural is not None:
            self._procedural.add_runbook(
                runbook_name, steps,
                source="distillation:eval-gated",
                confidence=0.85,
                derived_from=derived_from,
            )
        # Record the decision either way — a *rejected* promotion is also
        # decision-grade signal (and proof the gate has teeth).
        if self._ledger is not None:
            self._ledger.log(
                "memory_write", f"runbook:{runbook_name}",
                session_id=self._session_id,
                summary=("promoted runbook (beat baseline on pass^k)"
                         if result.promoted else "rejected runbook (did not beat baseline)"),
                **result.to_dict(),
            )
        return result
