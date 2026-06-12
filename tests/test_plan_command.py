"""F-1: `dacli plan` — an inspectable plan + governance preview, with NO execution.

The command decomposes a goal into the planner's DAG and, for each step,
*statically* previews the blast-radius tier, the policy decision, and the
rollback primitive that would apply — without constructing an LLM, touching a
connector, or making any network call.
"""

import unittest
from unittest.mock import patch

from click.testing import CliRunner

from dacli.core.plan_preview import build_plan_preview
from dacli.governance.classifier import Tier
from dacli.scripts.cli import cli


GOAL = (
    "create the bronze schema, then load the raw CRM extract, "
    "then drop the staging table"
)


class _ExplodingLLM:
    """Stand-in for LLMClient: instantiating it means the plan path went wrong."""

    def __init__(self, *args, **kwargs):
        raise AssertionError("plan preview must never construct an LLM client")


class BuildPlanPreviewTest(unittest.TestCase):
    def test_multi_step_goal_decomposes_into_chained_steps(self):
        preview = build_plan_preview(GOAL)
        self.assertEqual(len(preview.steps), 3)
        # Sequencing connectives become chained dependencies.
        self.assertEqual(preview.steps[1].node.depends_on, [preview.steps[0].node.id])
        self.assertEqual(preview.steps[2].node.depends_on, [preview.steps[1].node.id])

    def test_create_step_previews_as_write_tier(self):
        preview = build_plan_preview(GOAL)
        self.assertEqual(preview.steps[0].tier, Tier.WRITE)

    def test_drop_step_is_irreversible_and_needs_approval(self):
        preview = build_plan_preview("drop the staging table")
        step = preview.steps[0]
        self.assertEqual(step.tier, Tier.IRREVERSIBLE)
        self.assertTrue(step.node.irreversible)
        self.assertTrue(step.policy.requires_human)
        self.assertEqual(step.policy.decision.value, "dry_run+approve")

    def test_read_only_step_previews_as_safe_auto(self):
        preview = build_plan_preview("profile the customer tables")
        step = preview.steps[0]
        self.assertEqual(step.tier, Tier.SAFE)
        self.assertFalse(step.policy.requires_human)
        # A read-only step needs no undo.
        self.assertEqual(step.rollback.primitive, "noop")

    def test_platform_hint_selects_native_rollback_primitive(self):
        preview = build_plan_preview("drop the staging table in snowflake")
        step = preview.steps[0]
        self.assertEqual(step.platform, "snowflake")
        self.assertEqual(step.rollback.primitive, "time_travel_undrop")

    def test_unknown_platform_is_honest_about_no_native_undo(self):
        preview = build_plan_preview("drop the staging table")
        step = preview.steps[0]
        self.assertFalse(step.rollback.available)

    def test_prod_marker_promotes_the_tier(self):
        plain = build_plan_preview("update the orders table").steps[0]
        prod = build_plan_preview("update the PROD orders table").steps[0]
        self.assertEqual(plain.tier, Tier.RISKY)
        self.assertEqual(prod.tier, Tier.IRREVERSIBLE)
        self.assertTrue(prod.classification.is_prod)


class PlanCommandTest(unittest.TestCase):
    """The CLI surface: renders the preview and makes no LLM/network call."""

    def _invoke(self, *argv):
        runner = CliRunner()
        with (
            runner.isolated_filesystem(),
            patch("dacli.reasoning.llm.LLMClient", _ExplodingLLM),
            # A wide terminal so Rich doesn't wrap step text mid-word.
            patch.dict("os.environ", {"COLUMNS": "300"}),
        ):
            return runner.invoke(cli, ["plan", *argv], obj={})

    def test_plan_renders_dag_with_tiers_without_executing(self):
        result = self._invoke(GOAL)
        self.assertEqual(result.exit_code, 0, result.output)
        # All three steps appear, with their tiers.
        self.assertIn("create the bronze schema", result.output)
        self.assertIn("drop the staging table", result.output)
        self.assertIn("write", result.output)
        self.assertIn("irreversible", result.output)

    def test_irreversible_step_is_flagged_for_approval(self):
        result = self._invoke("drop the staging table")
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("approval", result.output.lower())

    def test_plan_never_constructs_an_llm(self):
        # _ExplodingLLM raises on instantiation; a clean exit proves the plan
        # path never built an LLM client (and therefore made no network call).
        result = self._invoke(GOAL)
        self.assertEqual(result.exit_code, 0, result.output)


if __name__ == "__main__":
    unittest.main()
