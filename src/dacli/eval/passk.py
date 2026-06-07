"""pass^k — the headline reliability metric.

``pass@1`` answers "did it work once?". ``pass^k`` answers "does it work *every*
time across k independent rollouts?" — the τ-bench-style consistency metric that
is dacli's true reliability KPI. Agents strong under single-shot pass rates
collapse under pass^k; that collapse is exactly what we measure for here.

A task's ``passed_all`` (all k rollouts succeeded) is the per-task pass^k
indicator; the suite-level pass^k is the fraction of tasks that ``passed_all``.
We also keep ``success_rate`` (k-run mean) and ``variance`` so a task that is
flaky-but-not-dead is visible, not hidden behind a binary.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from dacli.eval.types import GoldenTask, TaskResult


@dataclass
class PassKResult:
    """Aggregate of k rollouts of one golden task."""

    task_id: str
    connector: str
    stakes: str
    k: int
    runs: list[TaskResult] = field(default_factory=list)

    @property
    def successes(self) -> int:
        return sum(1 for r in self.runs if r.success)

    @property
    def pass_at_1(self) -> bool:
        return bool(self.runs) and self.runs[0].success

    @property
    def success_rate(self) -> float:
        return self.successes / self.k if self.k else 0.0

    @property
    def passed_all(self) -> bool:
        """The pass^k indicator: every rollout succeeded."""
        return self.k > 0 and self.successes == self.k

    @property
    def variance(self) -> float:
        if not self.runs:
            return 0.0
        p = self.success_rate
        return p * (1.0 - p)  # Bernoulli variance of the per-run success bit

    @property
    def earliest_failed_step(self) -> int | None:
        """The earliest step at which *any* rollout failed (None = always passed).

        Drives earlier-failure-recurrence detection: if this regresses to a
        smaller number, the task is failing *sooner* — degradation a rolling
        success average would hide.
        """
        steps = [r.failed_step for r in self.runs if r.failed_step is not None]
        return min(steps) if steps else None

    @property
    def unguarded_executions(self) -> int:
        """Destructive actions that ran with no gate — must be zero, always."""
        return sum(1 for r in self.runs if r.unguarded_execution)

    @property
    def escalation_rate(self) -> float:
        return sum(1 for r in self.runs if r.escalated) / self.k if self.k else 0.0

    @property
    def correction_rate(self) -> float:
        return sum(r.corrections for r in self.runs) / self.k if self.k else 0.0

    @property
    def governance_interrupt_rate(self) -> float:
        return (sum(1 for r in self.runs if r.governance_interrupt) / self.k
                if self.k else 0.0)

    @property
    def avg_tokens(self) -> float:
        return sum(r.tokens for r in self.runs) / self.k if self.k else 0.0

    @property
    def avg_latency_ms(self) -> float:
        return sum(r.latency_ms for r in self.runs) / self.k if self.k else 0.0

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "connector": self.connector,
            "stakes": self.stakes,
            "k": self.k,
            "successes": self.successes,
            "pass_at_1": self.pass_at_1,
            "pass_k": self.passed_all,
            "success_rate": round(self.success_rate, 4),
            "variance": round(self.variance, 4),
            "earliest_failed_step": self.earliest_failed_step,
            "unguarded_executions": self.unguarded_executions,
            "escalation_rate": round(self.escalation_rate, 4),
            "correction_rate": round(self.correction_rate, 4),
            "governance_interrupt_rate": round(self.governance_interrupt_rate, 4),
            "avg_tokens": round(self.avg_tokens, 1),
            "avg_latency_ms": round(self.avg_latency_ms, 3),
            "runs": [r.to_dict() for r in self.runs],
        }

    @classmethod
    def from_dict(cls, d: dict) -> PassKResult:
        return cls(
            task_id=str(d.get("task_id", "")),
            connector=str(d.get("connector", "")),
            stakes=str(d.get("stakes", "")),
            k=int(d.get("k", 0) or 0),
            runs=[TaskResult.from_dict(r) for r in d.get("runs", [])],
        )


async def run_pass_k(task: GoldenTask, k: int | None = None) -> PassKResult:
    """Run ``task`` k times and aggregate. Latency is measured per rollout here so
    individual ``run`` callables don't each have to time themselves."""
    k = k if k is not None else task.rollouts()
    k = max(1, int(k))
    runs: list[TaskResult] = []
    for _ in range(k):
        start = time.perf_counter()
        try:
            result = await task.run()
        except Exception as e:  # a task that explodes is a failed rollout, not a crash
            result = TaskResult(task_id=task.id, success=False,
                                error=f"task raised: {e}", failed_step=1)
        if not result.latency_ms:
            result.latency_ms = (time.perf_counter() - start) * 1000
        if not result.task_id:
            result.task_id = task.id
        runs.append(result)
    return PassKResult(
        task_id=task.id, connector=task.connector,
        stakes=task.stakes.value, k=k, runs=runs,
    )


def suite_pass_k(results: list[PassKResult]) -> float:
    """Suite-level pass^k: the fraction of tasks that succeeded on *every* rollout."""
    if not results:
        return 0.0
    return sum(1 for r in results if r.passed_all) / len(results)
