"""Phase 6 (𝒪 Orchestration / ℛ Model Routing) test suite.

Each test class maps to an exit criterion in roadmap/PHASE6.md, section 7. Run:
    python -m unittest tests.test_orchestration_phase6
"""

import asyncio
import tempfile
import unittest
from typing import List

from core.planner import Planner, TaskDAG, Subtask, NodeStatus, CyclicPlanError
from core.loop import (
    PlanActObserveVerify, StepResult, StepContext, CorrectionAuditLog,
)
from core.blackboard import Blackboard
from core.subagent import Lead, Assignment, WorkerOutput
from reasoning.model_router import (
    ModelRouter, ModelTier, Stakes, ModelRoutingAuditLog,
)


def _run(coro):
    return asyncio.run(coro)


def _tmp(name):
    return tempfile.mkdtemp(prefix="dacli_p6_") + "/" + name


# ===========================================================================
# Exit criterion 1: a multi-step goal → inspectable DAG with per-node criteria
# ===========================================================================
class PlanDAGTest(unittest.TestCase):
    GOAL = "stand up a Bronze pipeline then load the CRM source then validate the result"

    def test_complexity_gate_skips_simple_goals(self):
        p = Planner(complexity_gate=2)
        self.assertFalse(p.is_complex("show me the row counts in snowflake"))
        self.assertTrue(p.is_complex(self.GOAL))

    def test_multistep_goal_produces_chained_dag_with_criteria(self):
        dag = Planner().decompose(self.GOAL)
        self.assertEqual(len(dag), 3)
        ids = [n.id for n in dag.topological_order()]
        self.assertEqual(ids, ["s1", "s2", "s3"])
        # Dependencies are chained (s2 after s1, s3 after s2).
        self.assertEqual(dag.get("s2").depends_on, ["s1"])
        self.assertEqual(dag.get("s3").depends_on, ["s2"])
        # Every node carries explicit success criteria (its post-conditions).
        for node in dag.nodes:
            self.assertTrue(node.success_criteria)
        # The plan is inspectable / renderable for approval.
        rendered = dag.render()
        self.assertIn("Plan for:", rendered)
        self.assertIn("✓", rendered)

    def test_only_s1_is_ready_initially(self):
        dag = Planner().decompose(self.GOAL)
        self.assertEqual([n.id for n in dag.ready()], ["s1"])

    def test_cycle_is_rejected(self):
        a = Subtask(id="a", description="a", depends_on=["b"])
        b = Subtask(id="b", description="b", depends_on=["a"])
        dag = TaskDAG("cyclic", [a, b])
        with self.assertRaises(CyclicPlanError):
            dag.validate()

    def test_independent_nodes_share_a_parallel_level(self):
        # Two independent leaves under one root → root at level 0, leaves at 1.
        root = Subtask(id="r", description="root")
        l1 = Subtask(id="l1", description="leaf1", depends_on=["r"])
        l2 = Subtask(id="l2", description="leaf2", depends_on=["r"])
        dag = TaskDAG("fan", [root, l1, l2])
        dag.validate()
        groups = dag.parallel_groups()
        self.assertEqual([n.id for n in groups[0]], ["r"])
        self.assertEqual({n.id for n in groups[1]}, {"l1", "l2"})


# ===========================================================================
# Exit criterion 2: a failed test triggers bounded, feedback-driven correction
# ===========================================================================
class SelfCorrectionTest(unittest.TestCase):
    async def _verifier(self, node, result: StepResult):
        return result.success, (result.error or "ok")

    def test_failed_dbt_test_triggers_informed_retry_then_passes(self):
        seen_feedback: List[str] = []

        async def executor(node, ctx: StepContext) -> StepResult:
            # Only the "validate" node's dbt test fails (once); the load succeeds.
            if node.id == "s2" and ctx.attempt == 1:
                return StepResult(
                    success=False, error="dbt test failed",
                    feedback="not_null_crm_id failed: 3 null keys in CRM.ID",
                )
            # The correction attempt is *fed* the prior failure (informed retry).
            if node.id == "s2":
                seen_feedback.append(ctx.feedback or "")
            return StepResult(success=True, output="fixed: added NOT NULL filter")

        log = CorrectionAuditLog(path=_tmp("corrections.jsonl"))
        ctrl = PlanActObserveVerify(
            executor, verifier=self._verifier, correction_log=log, correction_budget=2,
        )
        dag = Planner().decompose("load the CRM source then validate it")
        res = _run(ctrl.run_dag(dag))
        self.assertTrue(res.done)
        # The retry received the actual failure feedback (not a blind re-run).
        self.assertTrue(any("3 null keys" in f for f in seen_feedback))
        # The correction was logged + surfaced.
        self.assertEqual(len(log.recent()), 1)
        self.assertIn("3 null keys", log.recent()[0]["feedback"])

    def test_correction_budget_is_bounded_then_escalates(self):
        async def always_fail(node, ctx: StepContext) -> StepResult:
            return StepResult(success=False, error="still broken", feedback="constraint persists")

        ctrl = PlanActObserveVerify(always_fail, verifier=self._verifier, correction_budget=2)
        dag = TaskDAG("g", [Subtask(id="s1", description="do it")])
        res = _run(ctrl.run_dag(dag))
        self.assertFalse(res.done)
        self.assertEqual(res.escalated, ["s1"])
        out = res.outcomes[0]
        # 1 initial attempt + exactly 2 bounded corrections.
        self.assertEqual(out.attempts, 3)
        self.assertEqual(out.status, "escalated")


# ===========================================================================
# Exit criterion 3: breadth-first fans out to parallel sub-agents, bounded ctx
# ===========================================================================
class BreadthFirstSubAgentTest(unittest.TestCase):
    def test_profile_n_tables_fans_out_and_bounds_context(self):
        items = [f"T{i}" for i in range(14)]
        running = {"n": 0, "max": 0}

        async def worker(a: Assignment) -> WorkerOutput:
            # Track concurrency to prove parallel execution.
            running["n"] += 1
            running["max"] = max(running["max"], running["n"])
            await asyncio.sleep(0)  # yield so siblings interleave
            running["n"] -= 1
            # A deliberately huge transcript — must be condensed on return.
            return WorkerOutput(text=("row " * 5000), facts={f"{a.item}#rows": 100})

        bb = Blackboard()
        lead = Lead(bb, max_subagents=8, summary_tokens=200)
        report = _run(lead.fan_out("profile all 14 tables", items, worker))

        self.assertEqual(len(report.results), 14)
        self.assertTrue(all(r.success for r in report.results))
        # Isolated context bound: every returned summary is ≤ the configured cap.
        self.assertTrue(all(r.summary_tokens <= 200 for r in report.results))
        # Total context the lead keeps stays bounded (not 14 × full transcripts).
        self.assertLessEqual(report.merged_tokens, lead.merged_tokens)
        # Genuinely parallel (more than one sub-agent in flight at the peak).
        self.assertGreater(running["max"], 1)

    def test_duplicate_items_are_claimed_once(self):
        async def worker(a: Assignment) -> WorkerOutput:
            return WorkerOutput(text=f"{a.item} done")

        bb = Blackboard()
        lead = Lead(bb, max_subagents=4)
        # Same object twice → the second claim is refused (de-duplication).
        report = _run(lead.fan_out("profile", ["CUSTOMERS", "CUSTOMERS"], worker))
        self.assertEqual(len(report.results), 1)


# ===========================================================================
# Exit criterion 4: conflicting sub-agent facts → logged contradiction, resolved
# ===========================================================================
class ContradictionTest(unittest.TestCase):
    def test_conflicting_facts_trigger_logged_contradiction_lead_resolves(self):
        async def worker(a: Assignment) -> WorkerOutput:
            # Two sub-agents inferred different types for the SAME column.
            if a.item == "view_a":
                return WorkerOutput(text="a", facts={"CUST#col:ID": "NUMBER"}, confidence=0.9)
            return WorkerOutput(text="b", facts={"CUST#col:ID": "VARCHAR"}, confidence=0.5)

        bb = Blackboard()
        lead = Lead(bb, max_subagents=2)
        report = _run(lead.fan_out("infer types", ["view_a", "view_b"], worker))

        # The conflict was detected and resolved by the lead.
        self.assertEqual(report.contradictions_resolved, 1)
        self.assertEqual(len(bb.contradictions()), 1)
        self.assertEqual(len(bb.contradictions(unresolved_only=True)), 0)
        # Higher-confidence assertion won; the resolution is on the record.
        self.assertEqual(bb.get("CUST#col:ID"), "NUMBER")
        self.assertTrue(any("contradiction" in d["what"] for d in bb.decisions()))

    def test_agreeing_facts_do_not_conflict(self):
        bb = Blackboard()
        self.assertIsNone(bb.assert_fact("k", "NUMBER", "a"))
        self.assertIsNone(bb.assert_fact("k", "number", "b"))  # equal (normalized)
        self.assertEqual(len(bb.contradictions()), 0)


# ===========================================================================
# Exit criterion 5: model router — cheap for classification, strong for
# diagnosis, escalates on failed verification (all visible in the audit log)
# ===========================================================================
class ModelRouterTest(unittest.TestCase):
    def _router(self):
        log = ModelRoutingAuditLog(path=_tmp("model_routing.jsonl"))
        return ModelRouter(
            cheap_model="cheap-m", strong_model="strong-m",
            default_model="cheap-m", audit_log=log,
        ), log

    def test_classification_uses_cheap_model(self):
        router, log = self._router()
        c = router.choose("classification")
        self.assertEqual(c.tier, ModelTier.CHEAP.value)
        self.assertEqual(c.model, "cheap-m")
        self.assertEqual(log.recent()[-1]["tier"], "cheap")

    def test_diagnosis_uses_strong_model(self):
        router, log = self._router()
        c = router.choose("diagnosis")
        self.assertEqual(c.tier, ModelTier.STRONG.value)
        self.assertEqual(c.model, "strong-m")
        self.assertEqual(log.recent()[-1]["tier"], "strong")

    def test_escalates_to_strong_on_failed_verification(self):
        router, log = self._router()
        c = router.choose("classification", after_failed_verification=True)
        self.assertEqual(c.tier, ModelTier.STRONG.value)
        self.assertTrue(c.escalated)
        # The escalation is visible (and explained) in the audit log.
        row = log.recent()[-1]
        self.assertTrue(row["escalated"])
        self.assertTrue(any("escalate" in step for step in row["trail"]))

    def test_low_confidence_and_high_stakes_escalate(self):
        router, _ = self._router()
        self.assertEqual(router.choose("classification", confidence=0.3).tier, "strong")
        self.assertEqual(router.choose("summarization", stakes=Stakes.HIGH).tier, "strong")
        # Strong never de-escalates to cheap.
        self.assertEqual(router.choose("diagnosis", confidence=0.99).tier, "strong")

    def test_single_model_config_routes_everything_to_default(self):
        # No tier ids → both tiers resolve to the default (no behavior change).
        router = ModelRouter(default_model="only-m")
        self.assertEqual(router.choose("classification").model, "only-m")
        self.assertEqual(router.choose("diagnosis").model, "only-m")


# ===========================================================================
# Exit criterion 6: an irreversible-action gate pauses the branch and resumes
# after approval without redoing completed nodes
# ===========================================================================
class ResumableGateTest(unittest.TestCase):
    GOAL = "build the staging table then drop the old table then rebuild downstream"

    async def _ok_verifier(self, node, result: StepResult):
        return result.success, "ok"

    def test_irreversible_node_pauses_then_resumes_without_redo(self):
        ran: List[str] = []

        async def executor(node, ctx: StepContext) -> StepResult:
            ran.append(node.id)
            return StepResult(success=True, output=f"{node.id} ok")

        approval = {"granted": False}

        def on_approval(node):
            return approval["granted"]

        ctrl = PlanActObserveVerify(
            executor, verifier=self._ok_verifier, on_approval=on_approval,
            correction_budget=1, require_approval=True,
        )
        dag = Planner().decompose(self.GOAL)
        # s2 ("drop the old table") is the irreversible node.
        self.assertTrue(dag.get("s2").irreversible)

        # First run: approval withheld → s1 completes, s2 pauses, s3 blocked.
        res1 = _run(ctrl.run_dag(dag))
        self.assertFalse(res1.done)
        self.assertEqual(res1.completed, ["s1"])
        self.assertEqual(res1.paused, ["s2"])
        self.assertEqual(ran, ["s1"])
        self.assertEqual(dag.get("s2").status, NodeStatus.PAUSED)

        # Grant approval and resume: s1 is NOT re-run; s2 then s3 proceed.
        ran.clear()
        approval["granted"] = True
        res2 = _run(ctrl.run_dag(dag))
        self.assertTrue(res2.done)
        self.assertEqual(ran, ["s2", "s3"])  # completed s1 never redone
        self.assertEqual(sorted(res2.completed), ["s1", "s2", "s3"])


if __name__ == "__main__":
    unittest.main()
