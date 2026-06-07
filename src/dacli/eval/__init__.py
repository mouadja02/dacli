"""Evaluation, reliability hardening & self-improvement.

This package *proves* reliability instead of asserting it. It measures the whole
harness ``𝒫_H = Φ(ℛ, ℳ, 𝒞, 𝒮, 𝒪, 𝒢)`` against versioned golden task suites and
reports **pass^k** (consistency across repeated rollouts) — not single-shot luck —
plus regression, cost/latency, escalation and self-correction rates.

Layout::

    eval/
      types.py        # Task / TaskResult contracts
      passk.py        # pass^k: run each task k times, report consistency
      harness.py      # run a task suite, persist run history
      golden/         # per-platform + spine task specs with verifiable outcomes
      sim/            # deterministic, offline simulated platforms (cheap/safe CI)
      regression.py   # compare runs; flag new failures + earlier-failure recurrence
      dashboard.py    # reliability surface (success, pass^k, cost, latency, ...)
      selfimprove.py  # episodic → procedural runbook distillation, gated by pass^k
      calibration.py  # feed eval output back into the tunable thresholds

Everything here is offline-safe (no live credentials, no network, no cost): the
inner loop runs against simulated platforms; live-sandbox runs supplement at
milestones.
"""

from dacli.eval.types import GoldenTask, TaskResult, Stakes, default_k_for
from dacli.eval.passk import PassKResult, run_pass_k, suite_pass_k
from dacli.eval.harness import EvalHarness, SuiteReport, RunHistory

__all__ = [
    "EvalHarness",
    "GoldenTask",
    "PassKResult",
    "RunHistory",
    "Stakes",
    "SuiteReport",
    "TaskResult",
    "default_k_for",
    "run_pass_k",
    "suite_pass_k",
]
