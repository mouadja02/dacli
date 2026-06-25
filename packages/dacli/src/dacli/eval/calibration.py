"""Calibration feedback.

Eval output feeds back into the tunable thresholds so calibration is *data-driven,
not guessed*: the router's ``min_confidence``, the memory staleness horizon
/ TTLs, and governance tier overrides. This module reads a
:class:`~eval.harness.SuiteReport` and emits concrete, documented recommendations
— it never silently mutates config; a human (or a gated job) applies them.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from dacli.eval.harness import SuiteReport


@dataclass
class CalibrationRecommendation:
    router_min_confidence: float | None = None
    memory_staleness_horizon_days: float | None = None
    governance_overrides: dict[str, str] = field(default_factory=dict)
    rationale: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "router_min_confidence": self.router_min_confidence,
            "memory_staleness_horizon_days": self.memory_staleness_horizon_days,
            "governance_overrides": self.governance_overrides,
            "rationale": self.rationale,
        }

    def to_markdown(self) -> str:
        lines = ["# Calibration recommendations (data-driven)", ""]
        if self.router_min_confidence is not None:
            lines.append(f"- **router.min_confidence** → `{self.router_min_confidence}`")
        if self.memory_staleness_horizon_days is not None:
            lines.append(f"- **memory.staleness_horizon_days** → `{self.memory_staleness_horizon_days}`")
        for target, decision in self.governance_overrides.items():
            lines.append(f"- **governance override** `{target}` → `{decision}`")
        if not (self.router_min_confidence or self.memory_staleness_horizon_days
                or self.governance_overrides):
            lines.append("- No changes recommended — thresholds are well-calibrated.")
        lines += ["", "## Rationale", ""]
        lines += [f"- {r}" for r in self.rationale] or ["- (none)"]
        return "\n".join(lines)


def calibrate(
    report: SuiteReport,
    *,
    current_min_confidence: float = 0.7,
    current_staleness_horizon_days: float = 30.0,
) -> CalibrationRecommendation:
    """Derive threshold recommendations from a suite report.

    Heuristics, each anchored to an observed signal:

    * **High escalation with high success** → routing is too timid; *lower*
      ``min_confidence`` a notch. **Low success with low escalation** → routing
      is over-confident; *raise* it.
    * **A memory task regressing** (the staleness task failing) → *shorten* the
      staleness horizon so stale facts are demoted sooner.
    * **Any unguarded destructive execution, or a destructive task below pass^k=1**
      → *tighten* governance to ``dry_run+approve`` for that connector.
    """
    rec = CalibrationRecommendation()
    results = report.results
    if not results:
        rec.rationale.append("empty suite — nothing to calibrate")
        return rec

    n = len(results)
    avg_success = sum(r.success_rate for r in results) / n
    avg_escalation = sum(r.escalation_rate for r in results) / n

    # --- router min_confidence -------------------------------------------
    if avg_escalation > 0.3 and avg_success > 0.9:
        rec.router_min_confidence = round(max(0.5, current_min_confidence - 0.05), 3)
        rec.rationale.append(
            f"escalation rate {avg_escalation:.2f} is high while success is "
            f"{avg_success:.2f}: routing is too timid → lower min_confidence to "
            f"{rec.router_min_confidence}.")
    elif avg_success < 0.8 and avg_escalation < 0.1:
        rec.router_min_confidence = round(min(0.95, current_min_confidence + 0.05), 3)
        rec.rationale.append(
            f"success {avg_success:.2f} is low with little escalation "
            f"({avg_escalation:.2f}): routing is over-confident → raise "
            f"min_confidence to {rec.router_min_confidence}.")

    # --- memory staleness horizon ----------------------------------------
    mem = report.get("spine.memory_staleness")
    if mem is not None and not mem.passed_all:
        rec.memory_staleness_horizon_days = round(max(7.0, current_staleness_horizon_days / 2), 1)
        rec.rationale.append(
            "the stale-but-confident memory task is failing → shorten the "
            f"staleness horizon to {rec.memory_staleness_horizon_days} days so "
            "old facts are demoted sooner.")

    # --- governance tightening -------------------------------------------
    for r in results:
        if r.stakes == "destructive" and (r.unguarded_executions > 0 or not r.passed_all):
            rec.governance_overrides[r.connector] = "dry_run+approve"
            rec.rationale.append(
                f"destructive task '{r.task_id}' did not hold the pass^k bar "
                f"(unguarded={r.unguarded_executions}, pass^k={r.passed_all}) → "
                f"force dry_run+approve for '{r.connector}'.")

    if not rec.rationale:
        rec.rationale.append(
            f"thresholds well-calibrated: success {avg_success:.2f}, "
            f"escalation {avg_escalation:.2f}, zero unguarded executions.")
    return rec
