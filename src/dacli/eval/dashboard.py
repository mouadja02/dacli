"""Reliability dashboard.

Surfaces, per connector and overall: success rate, **pass^k**, escalation rate,
self-correction rate, governance-interrupt rate, and cost/latency per task. This
is where the audit ledger (𝒢) and routing log (𝒮) become decision-grade signal,
answering "is the harness getting better or worse?" with data rather than vibes.

Renders as plain text (no rich dependency) so it works in CI logs and the CLI
alike; :meth:`to_dict` gives the machine-readable form.
"""

from __future__ import annotations

from dataclasses import dataclass

from dacli.eval.harness import SuiteReport
from dacli.eval.passk import PassKResult


@dataclass
class ConnectorRow:
    connector: str
    tasks: int
    pass_at_1: float
    pass_k: float
    success_rate: float
    escalation_rate: float
    correction_rate: float
    governance_interrupt_rate: float
    unguarded: int
    avg_tokens: float
    avg_latency_ms: float

    @classmethod
    def from_results(cls, connector: str, results: list[PassKResult]) -> ConnectorRow:
        n = len(results) or 1
        return cls(
            connector=connector,
            tasks=len(results),
            pass_at_1=sum(1 for r in results if r.pass_at_1) / n,
            pass_k=sum(1 for r in results if r.passed_all) / n,
            success_rate=sum(r.success_rate for r in results) / n,
            escalation_rate=sum(r.escalation_rate for r in results) / n,
            correction_rate=sum(r.correction_rate for r in results) / n,
            governance_interrupt_rate=sum(r.governance_interrupt_rate for r in results) / n,
            unguarded=sum(r.unguarded_executions for r in results),
            avg_tokens=sum(r.avg_tokens for r in results) / n,
            avg_latency_ms=sum(r.avg_latency_ms for r in results) / n,
        )

    def to_dict(self) -> dict:
        return {
            "connector": self.connector, "tasks": self.tasks,
            "pass_at_1": round(self.pass_at_1, 3), "pass_k": round(self.pass_k, 3),
            "success_rate": round(self.success_rate, 3),
            "escalation_rate": round(self.escalation_rate, 3),
            "correction_rate": round(self.correction_rate, 3),
            "governance_interrupt_rate": round(self.governance_interrupt_rate, 3),
            "unguarded": self.unguarded,
            "avg_tokens": round(self.avg_tokens, 1),
            "avg_latency_ms": round(self.avg_latency_ms, 2),
        }


@dataclass
class Dashboard:
    suite: str
    rows: list[ConnectorRow]
    overall: ConnectorRow

    @classmethod
    def from_report(cls, report: SuiteReport) -> Dashboard:
        rows = [
            ConnectorRow.from_results(cid, results)
            for cid, results in sorted(report.by_connector().items())
        ]
        overall = ConnectorRow.from_results("OVERALL", report.results)
        return cls(suite=report.suite, rows=rows, overall=overall)

    def to_dict(self) -> dict:
        return {
            "suite": self.suite,
            "overall": self.overall.to_dict(),
            "connectors": [r.to_dict() for r in self.rows],
        }

    def render(self) -> str:
        header = (f"{'connector':<18} {'tasks':>5} {'pass@1':>7} {'pass^k':>7} "
                  f"{'succ':>6} {'esc':>6} {'corr':>6} {'gov':>6} {'unguard':>8} "
                  f"{'tok':>7} {'ms':>8}")
        sep = "-" * len(header)
        lines = [f"Reliability dashboard — suite: {self.suite}", sep, header, sep]

        def fmt(r: ConnectorRow) -> str:
            return (f"{r.connector:<18} {r.tasks:>5} {r.pass_at_1:>7.2f} {r.pass_k:>7.2f} "
                    f"{r.success_rate:>6.2f} {r.escalation_rate:>6.2f} "
                    f"{r.correction_rate:>6.2f} {r.governance_interrupt_rate:>6.2f} "
                    f"{r.unguarded:>8} {r.avg_tokens:>7.0f} {r.avg_latency_ms:>8.1f}")

        lines.extend(fmt(row) for row in self.rows)
        lines.append(sep)
        lines.append(fmt(self.overall))
        lines.append(sep)
        if self.overall.unguarded:
            lines.append(f"⚠ {self.overall.unguarded} UNGUARDED destructive execution(s) — "
                         f"this must be zero.")
        else:
            lines.append("✓ zero unguarded destructive executions.")
        return "\n".join(lines)
