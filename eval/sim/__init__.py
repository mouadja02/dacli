"""Deterministic, offline simulated platforms (workstream 8.2).

The eval inner loop runs against these fakes so the golden suite is cheap, safe,
and repeatable in CI — no live credentials, no cost, no network flakiness. They
mirror exactly the seam the connectors already expose for testing: a CLI-first
connector funnels every subprocess call through an injectable ``runner(argv) ->
CliResult``, so a simulated platform is a *programmable responder* over that seam.

A responder can also be made **seeded-flaky** (:class:`SimCli` ``failure_rate``)
so pass^k measures real consistency rather than a degenerate deterministic 1.0 —
the whole point of pass^k is to catch the agent that aces one run and flakes the
next. ``inject_error`` supports the regression exit criterion (deliberately
introduce a degradation and prove the net catches it).

Live-sandbox runs (Phase 5) reconcile sim vs. reality at milestones; a divergence
is treated as a sim bug to fix (PHASE8 §6 risk: simulators diverging from prod).
"""

from eval.sim.cli import SimCli, Call
from eval.sim.platforms import (
    s3_responder,
    gcs_responder,
    bigquery_responder,
    databricks_responder,
    SIM_SETTINGS,
)

__all__ = [
    "SimCli",
    "Call",
    "s3_responder",
    "gcs_responder",
    "bigquery_responder",
    "databricks_responder",
    "SIM_SETTINGS",
]
