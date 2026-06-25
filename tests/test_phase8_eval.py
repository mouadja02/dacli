"""Phase 8 — evaluation, reliability hardening & self-improvement.

Each test class maps to an exit criterion in roadmap/PHASE8.md §7. Everything
runs offline against the deterministic simulator (no credentials, no network).

Run with:
    python -m unittest tests.test_phase8_eval
"""

import asyncio
import tempfile
import unittest

from dacli.eval.types import GoldenTask, Stakes, TaskResult
from dacli.eval.passk import PassKResult, run_pass_k
from dacli.eval.harness import EvalHarness, SuiteReport
from dacli.eval.sim.cli import SimCli
from dacli.eval.sim.platforms import s3_responder
from dacli.eval.golden import build_golden_suite, build_connector_suite, build_spine_suite
from dacli.eval.regression import compare
from dacli.eval.dashboard import Dashboard
from dacli.eval.selfimprove import SelfImprovement
from dacli.eval.calibration import calibrate


def _run(coro):
    return asyncio.run(coro)


def _tmp(name):
    return tempfile.mkdtemp(prefix="dacli_p8_") + "/" + name


def _result(task_id, connector, stakes, *, k, succ, failed_step=None,
            unguarded=0, tokens=0.0, latency=0.0):
    """Build a PassKResult with ``succ`` successful runs out of k."""
    runs = []
    for i in range(k):
        ok = i < succ
        runs.append(TaskResult(
            task_id=task_id, success=ok,
            failed_step=None if ok else (failed_step or 1),
            unguarded_execution=bool(unguarded) and not ok,
            tokens=int(tokens), latency_ms=latency,
        ))
    return PassKResult(task_id=task_id, connector=connector, stakes=stakes, k=k, runs=runs)


# ===========================================================================
# Exit criterion 1: every connector/skill has a golden suite with machine-
# verifiable outcomes; the sim suite runs (this is what CI invokes per PR).
# ===========================================================================
class GoldenSuiteCoverageTest(unittest.TestCase):
    def test_every_discovered_connector_has_a_golden_task(self):
        # After M11 only the seeds' old Connector classes remain on disk; each
        # still gets a structural DoD golden task.
        suite = build_connector_suite()
        covered = {t.connector for t in suite}
        for expected in ("snowflake", "github"):
            self.assertIn(expected, covered, f"{expected} has no golden task")

    def test_sim_suite_runs_and_passes(self):
        # The full sim suite (the CI entrypoint) runs cleanly and every task
        # holds its pass^k bar against the deterministic simulator.
        harness = EvalHarness(history_path=_tmp("history.jsonl"), k_scale=0.34)
        report = _run(harness.run_suite("sim", build_golden_suite(), persist=False))
        self.assertGreater(len(report.results), 5)
        self.assertGreaterEqual(report.pass_k, 0.95,
                                "\n".join(f"{r.task_id}: {r.success_rate:.2f} "
                                          f"({r.runs[0].detail})"
                                          for r in report.results if not r.passed_all))
        self.assertEqual(report.total_unguarded_executions, 0)


# ===========================================================================
# Exit criterion 2: pass^k is reported per task; the destructive-action gate
# demonstrates a high pass^k bar (DROP-guard holds across k runs, zero
# unguarded executions).
# ===========================================================================
class PassKTest(unittest.TestCase):
    def test_passk_aggregates_consistency_not_just_peak(self):
        state = {"i": 0}

        async def alternating() -> TaskResult:
            state["i"] += 1
            return TaskResult("alt", success=(state["i"] % 2 == 1))

        task = GoldenTask("alt", "x", "alternating", alternating, k=4)
        pk = _run(run_pass_k(task))
        self.assertEqual(pk.successes, 2)
        self.assertAlmostEqual(pk.success_rate, 0.5)
        self.assertFalse(pk.passed_all)          # pass^k catches the flakiness
        self.assertTrue(pk.pass_at_1)            # ...that pass@1 would miss
        self.assertAlmostEqual(pk.variance, 0.25)

    def test_destructive_gate_holds_across_k_runs(self):
        drop = next(t for t in build_spine_suite() if t.id == "spine.drop_guard")
        self.assertEqual(drop.stakes, Stakes.DESTRUCTIVE)
        pk = _run(run_pass_k(drop, 10))
        self.assertEqual(pk.k, 10)
        self.assertTrue(pk.passed_all, "DROP-guard must hold on EVERY rollout")
        self.assertEqual(pk.unguarded_executions, 0,
                         "a destructive op ran without a gate")
        self.assertTrue(all(r.governance_interrupt for r in pk.runs))

    def test_simulator_flakiness_is_visible_to_passk(self):
        # A seeded-flaky platform yields a sub-1.0 pass^k — the whole point of
        # pass^k over pass@1.
        cli = SimCli(s3_responder(head_exists=True), failure_rate=0.5, seed=7)
        rcs = [_run(cli(["aws", "s3", "cp", str(i)])).rc for i in range(20)]
        self.assertIn(0, rcs)
        self.assertIn(1, rcs)


# ===========================================================================
# Exit criterion 3: regression detection flags a deliberately-introduced
# degradation, including earlier-failure recurrence.
# ===========================================================================
class RegressionTest(unittest.TestCase):
    def test_new_failure_and_earlier_failure_recurrence_are_flagged(self):
        prev = SuiteReport(suite="sim", results=[
            _result("t.pipeline", "t", "write", k=4, succ=4, failed_step=None),
            _result("t.stable", "t", "read_only", k=3, succ=3),
        ])
        # t.pipeline now fails — and fails *earlier* (step 3 vs. a clean run).
        curr = SuiteReport(suite="sim", results=[
            _result("t.pipeline", "t", "write", k=4, succ=1, failed_step=3),
            _result("t.stable", "t", "read_only", k=3, succ=3),
        ])
        # Seed an earlier-failure case explicitly: was failing at step 5, now 2.
        prev.results.append(_result("t.deep", "t", "write", k=2, succ=0, failed_step=5))
        curr.results.append(_result("t.deep", "t", "write", k=2, succ=0, failed_step=2))

        report = compare(prev, curr)
        self.assertTrue(report.regressed)
        self.assertTrue(any(r.task_id == "t.pipeline" for r in report.new_failures))
        self.assertTrue(any(r.task_id == "t.deep" for r in report.earlier_failures))
        self.assertIn("step 2", report.to_dict()["earlier_failures"][0]["detail"])

    def test_unguarded_execution_is_a_hard_regression(self):
        prev = SuiteReport(suite="sim", results=[
            _result("s3.delete", "s3", "destructive", k=2, succ=2)])
        curr = SuiteReport(suite="sim", results=[
            _result("s3.delete", "s3", "destructive", k=2, succ=0, unguarded=1)])
        report = compare(prev, curr)
        self.assertTrue(report.unguarded)
        self.assertTrue(report.regressed)

    def test_clean_run_does_not_regress(self):
        rep = SuiteReport(suite="sim", results=[
            _result("t.a", "t", "read_only", k=3, succ=3)])
        self.assertFalse(compare(rep, rep).regressed)


# ===========================================================================
# Exit criterion 4: the dashboard shows success, pass^k, cost, latency,
# escalation and correction rates per connector and overall.
# ===========================================================================
class DashboardTest(unittest.TestCase):
    def test_dashboard_surfaces_per_connector_and_overall(self):
        report = SuiteReport(suite="sim", results=[
            _result("s3.put", "s3", "write", k=2, succ=2, tokens=10, latency=5),
            _result("s3.delete", "s3", "write", k=2, succ=1),
            _result("bq.select", "bigquery", "read_only", k=3, succ=3),
        ])
        dash = Dashboard.from_report(report)
        connectors = {r.connector for r in dash.rows}
        self.assertEqual(connectors, {"s3", "bigquery"})
        self.assertEqual(dash.overall.tasks, 3)
        d = dash.to_dict()
        for key in ("pass_k", "success_rate", "escalation_rate",
                    "correction_rate", "avg_tokens", "avg_latency_ms"):
            self.assertIn(key, d["overall"])
        rendered = dash.render()
        self.assertIn("pass^k", rendered)
        self.assertIn("OVERALL", rendered)


# ===========================================================================
# Exit criterion 5: an episodic trace is distilled into a runbook that beats
# the ad-hoc path on pass^k before promotion; the comparison is in the ledger.
# ===========================================================================
class _FakeProcedural:
    def __init__(self):
        self.runbooks = []

    def add_runbook(self, name, steps, **kw):
        self.runbooks.append({"name": name, "steps": steps, **kw})


class SelfImprovementTest(unittest.TestCase):
    def _tasks(self, base_ok: bool, cand_ok: bool):
        async def base():
            return TaskResult("baseline", success=base_ok, failed_step=None if base_ok else 1)

        async def cand():
            return TaskResult("candidate", success=cand_ok, failed_step=None if cand_ok else 1)

        return (GoldenTask("baseline", "x", "ad-hoc path", base, stakes=Stakes.WRITE),
                GoldenTask("candidate", "x", "runbook path", cand, stakes=Stakes.WRITE))

    def test_runbook_promoted_only_when_it_beats_baseline(self):
        from dacli.governance.audit import AuditLedger

        proc = _FakeProcedural()
        ledger = AuditLedger(path=_tmp("audit.jsonl"))
        si = SelfImprovement(procedural=proc, ledger=ledger)
        base, cand = self._tasks(base_ok=False, cand_ok=True)  # runbook beats ad-hoc

        result = _run(si.distill_and_promote(
            "load_bronze", "1. introspect\n2. load\n3. verify", base, cand, k=5))

        self.assertTrue(result.promoted)
        self.assertGreater(result.candidate_pass_k, result.baseline_pass_k)
        self.assertEqual(len(proc.runbooks), 1)
        # The promotion comparison is recorded in the audit ledger.
        events = ledger.events(kind="memory_write")
        self.assertEqual(len(events), 1)
        self.assertIn("promoted", events[0]["summary"])

    def test_unvetted_runbook_is_rejected(self):
        from dacli.governance.audit import AuditLedger

        proc = _FakeProcedural()
        ledger = AuditLedger(path=_tmp("audit.jsonl"))
        si = SelfImprovement(procedural=proc, ledger=ledger)
        base, cand = self._tasks(base_ok=True, cand_ok=False)  # runbook is worse

        result = _run(si.distill_and_promote(
            "bad_runbook", "1. yolo", base, cand, k=5))

        self.assertFalse(result.promoted)
        self.assertEqual(len(proc.runbooks), 0, "an unvetted runbook must not be written")
        self.assertIn("rejected", ledger.events(kind="memory_write")[0]["summary"])


# ===========================================================================
# Exit criterion 6: threshold calibration is driven by eval output, documented.
# ===========================================================================
class CalibrationTest(unittest.TestCase):
    def test_calibration_recommends_from_eval_signals(self):
        report = SuiteReport(suite="sim", results=[
            # the memory staleness task is failing → shorten the horizon
            _result("spine.memory_staleness", "spine", "read_only", k=2, succ=0),
            # a destructive task ran unguarded → tighten governance for s3
            _result("s3.delete", "s3", "destructive", k=2, succ=0, unguarded=1),
        ])
        rec = calibrate(report, current_staleness_horizon_days=30.0)
        self.assertIsNotNone(rec.memory_staleness_horizon_days)
        self.assertLess(rec.memory_staleness_horizon_days, 30.0)
        self.assertEqual(rec.governance_overrides.get("s3"), "dry_run+approve")
        self.assertTrue(rec.rationale)
        md = rec.to_markdown()
        self.assertIn("Calibration recommendations", md)
        self.assertIn("governance override", md)

    def test_well_calibrated_report_recommends_no_change(self):
        report = SuiteReport(suite="sim", results=[
            _result("t.a", "t", "read_only", k=3, succ=3),
            _result("t.b", "t", "write", k=3, succ=3),
        ])
        rec = calibrate(report)
        self.assertIsNone(rec.memory_staleness_horizon_days)
        self.assertFalse(rec.governance_overrides)


if __name__ == "__main__":
    unittest.main()
