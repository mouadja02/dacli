"""The eval harness: run a task suite, aggregate pass^k, persist run history.

It takes a list of :class:`~eval.types.GoldenTask`,
runs each k times (k tiered by stakes, optionally scaled for fast CI), and produces
a :class:`SuiteReport`. Reports are appended to a JSONL :class:`RunHistory` so
:mod:`eval.regression` can answer "is the harness getting better or worse?" over
time — the longitudinal view single-shot benchmarks can't give.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from dacli.core.timeutils import now_iso as _now_iso
from dacli.eval.passk import PassKResult, run_pass_k, suite_pass_k
from dacli.eval.types import GoldenTask


@dataclass
class SuiteReport:
    """The result of running one suite: per-task pass^k + suite aggregates."""

    suite: str
    timestamp: str = field(default_factory=_now_iso)
    results: list[PassKResult] = field(default_factory=list)

    # ------------------------------------------------------------------
    @property
    def pass_k(self) -> float:
        return suite_pass_k(self.results)

    @property
    def pass_at_1(self) -> float:
        if not self.results:
            return 0.0
        return sum(1 for r in self.results if r.pass_at_1) / len(self.results)

    @property
    def total_unguarded_executions(self) -> int:
        return sum(r.unguarded_executions for r in self.results)

    def by_connector(self) -> dict[str, list[PassKResult]]:
        groups: dict[str, list[PassKResult]] = {}
        for r in self.results:
            groups.setdefault(r.connector, []).append(r)
        return groups

    def get(self, task_id: str) -> PassKResult | None:
        return next((r for r in self.results if r.task_id == task_id), None)

    def to_dict(self) -> dict:
        return {
            "suite": self.suite,
            "timestamp": self.timestamp,
            "pass_k": round(self.pass_k, 4),
            "pass_at_1": round(self.pass_at_1, 4),
            "total_unguarded_executions": self.total_unguarded_executions,
            "results": [r.to_dict() for r in self.results],
        }

    @classmethod
    def from_dict(cls, d: dict) -> SuiteReport:
        return cls(
            suite=str(d.get("suite", "")),
            timestamp=str(d.get("timestamp", "")),
            results=[PassKResult.from_dict(r) for r in d.get("results", [])],
        )


class RunHistory:
    """Append-only JSONL history of suite reports (one line per run)."""

    def __init__(self, path: str = ".dacli/eval/history.jsonl"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, report: SuiteReport) -> None:
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(report.to_dict(), default=str) + "\n")

    def all(self) -> list[SuiteReport]:
        if not self.path.exists():
            return []
        out: list[SuiteReport] = []
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(SuiteReport.from_dict(json.loads(line)))
                except Exception:
                    continue
        return out

    def latest(self, suite: str | None = None) -> SuiteReport | None:
        reports = self.all()
        if suite is not None:
            reports = [r for r in reports if r.suite == suite]
        return reports[-1] if reports else None

    def previous(self, suite: str | None = None) -> SuiteReport | None:
        """The second-most-recent report (the baseline a new run regresses against)."""
        reports = self.all()
        if suite is not None:
            reports = [r for r in reports if r.suite == suite]
        return reports[-2] if len(reports) >= 2 else None


class EvalHarness:
    """Runs golden suites with stakes-tiered pass^k and optional history persistence."""

    def __init__(
        self,
        history_path: str = ".dacli/eval/history.jsonl",
        *,
        k_scale: float = 1.0,
    ):
        # k_scale dials every task's k (e.g. 0.5 for a fast PR run, 1.0 for a
        # milestone run). Destructive tasks never drop below k=2 so the gate is
        # always exercised more than once even under the cheapest CI budget.
        self.k_scale = k_scale
        self.history = RunHistory(history_path)

    def _scaled_k(self, task: GoldenTask) -> int:
        base = task.rollouts()
        scaled = max(1, round(base * self.k_scale))
        if task.stakes.value == "destructive":
            scaled = max(2, scaled)
        return scaled

    async def run_suite(
        self,
        suite_name: str,
        tasks: list[GoldenTask],
        *,
        persist: bool = True,
    ) -> SuiteReport:
        results: list[PassKResult] = [
            await run_pass_k(task, self._scaled_k(task)) for task in tasks
        ]
        report = SuiteReport(suite=suite_name, results=results)
        if persist:
            self.history.append(report)
        return report
