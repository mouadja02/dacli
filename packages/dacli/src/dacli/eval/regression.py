"""Regression & reward-hacking guards.

Compares a new :class:`~eval.harness.SuiteReport` against a prior one and flags:

* **new failures** — a task that used to ``pass^k`` and now doesn't,
* **earlier-failure recurrence** — a task now failing at an *earlier* step than
  before (degradation the rolling success average hides — the reward-hacking
  defense: watch the *shape* of failures, not just the count),
* **cost / latency drift** — the task got more expensive or slower,
* **unguarded executions** — any destructive action that ran without a gate (a
  hard, never-tolerated regression).

Net improvement can mask important regressions, so this reports the deltas
explicitly rather than a single rolling number.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from dacli.eval.harness import SuiteReport


# A task whose average cost/latency rises by more than this fraction is "drifting".
_DRIFT_THRESHOLD = 0.25


@dataclass
class TaskRegression:
    task_id: str
    kind: str           # "new_failure" | "earlier_failure" | "cost_drift" | "latency_drift" | "unguarded"
    detail: str
    before: float | None = None
    after: float | None = None


@dataclass
class RegressionReport:
    suite: str
    new_failures: list[TaskRegression] = field(default_factory=list)
    earlier_failures: list[TaskRegression] = field(default_factory=list)
    fixed: list[str] = field(default_factory=list)
    cost_drift: list[TaskRegression] = field(default_factory=list)
    latency_drift: list[TaskRegression] = field(default_factory=list)
    unguarded: list[TaskRegression] = field(default_factory=list)
    prev_pass_k: float = 0.0
    curr_pass_k: float = 0.0

    @property
    def regressed(self) -> bool:
        """Any kind of degradation that should fail a gate."""
        return bool(self.new_failures or self.earlier_failures or self.unguarded)

    @property
    def net_delta(self) -> int:
        return len(self.fixed) - len(self.new_failures)

    def summary(self) -> str:
        bits = [
            f"pass^k {self.prev_pass_k:.2f} → {self.curr_pass_k:.2f}",
            f"{len(self.new_failures)} new failure(s)",
            f"{len(self.earlier_failures)} earlier-failure recurrence(s)",
            f"{len(self.fixed)} fixed",
        ]
        if self.cost_drift:
            bits.append(f"{len(self.cost_drift)} cost-drift")
        if self.latency_drift:
            bits.append(f"{len(self.latency_drift)} latency-drift")
        if self.unguarded:
            bits.append(f"⚠ {len(self.unguarded)} UNGUARDED execution(s)")
        return ", ".join(bits)

    def to_dict(self) -> dict:
        def _rows(rs: list[TaskRegression]):
            return [{"task_id": r.task_id, "kind": r.kind, "detail": r.detail,
                     "before": r.before, "after": r.after} for r in rs]
        return {
            "suite": self.suite,
            "prev_pass_k": round(self.prev_pass_k, 4),
            "curr_pass_k": round(self.curr_pass_k, 4),
            "regressed": self.regressed,
            "net_delta": self.net_delta,
            "new_failures": _rows(self.new_failures),
            "earlier_failures": _rows(self.earlier_failures),
            "fixed": list(self.fixed),
            "cost_drift": _rows(self.cost_drift),
            "latency_drift": _rows(self.latency_drift),
            "unguarded": _rows(self.unguarded),
        }


def _drift(before: float, after: float) -> bool:
    if before <= 0:
        return after > 0 and before == 0 and after > 1.0  # 0→tiny isn't drift
    return (after - before) / before > _DRIFT_THRESHOLD


def compare(prev: SuiteReport, curr: SuiteReport) -> RegressionReport:
    """Diff two suite reports of the same suite."""
    report = RegressionReport(
        suite=curr.suite, prev_pass_k=prev.pass_k, curr_pass_k=curr.pass_k,
    )
    prev_by_id = {r.task_id: r for r in prev.results}

    for cur in curr.results:
        # An unguarded destructive execution is always a hard regression.
        if cur.unguarded_executions > 0:
            report.unguarded.append(TaskRegression(
                cur.task_id, "unguarded",
                f"{cur.unguarded_executions} destructive execution(s) ran with no gate"))

        old = prev_by_id.get(cur.task_id)
        if old is None:
            continue  # a brand-new task can't regress against history

        # pass^k transitions.
        if old.passed_all and not cur.passed_all:
            report.new_failures.append(TaskRegression(
                cur.task_id, "new_failure",
                f"pass^k {old.success_rate:.2f} → {cur.success_rate:.2f}",
                before=old.success_rate, after=cur.success_rate))
        elif not old.passed_all and cur.passed_all:
            report.fixed.append(cur.task_id)

        # earlier-failure recurrence (the shape of the failure got worse).
        ob, cb = old.earliest_failed_step, cur.earliest_failed_step
        if ob is not None and cb is not None and cb < ob:
            report.earlier_failures.append(TaskRegression(
                cur.task_id, "earlier_failure",
                f"now fails at step {cb} (was step {ob})",
                before=ob, after=cb))

        # cost / latency drift.
        if _drift(old.avg_tokens, cur.avg_tokens):
            report.cost_drift.append(TaskRegression(
                cur.task_id, "cost_drift",
                f"avg tokens {old.avg_tokens:.0f} → {cur.avg_tokens:.0f}",
                before=old.avg_tokens, after=cur.avg_tokens))
        if _drift(old.avg_latency_ms, cur.avg_latency_ms):
            report.latency_drift.append(TaskRegression(
                cur.task_id, "latency_drift",
                f"avg latency {old.avg_latency_ms:.1f}ms → {cur.avg_latency_ms:.1f}ms",
                before=old.avg_latency_ms, after=cur.avg_latency_ms))

    return report
