"""F-4: cost preview as a governance dimension.

An optional ``Connector.estimate_cost(op, args)`` hook (BigQuery first, via its
native ``dry_run``) feeds the Governor: when the estimate exceeds the
configurable ``governance.cost_confirm_usd`` threshold, the effective tier is
raised so the existing confirm/approval path fires — cost becomes a blast-radius
dimension. With no threshold configured the hook is never consulted (zero
behaviour change).
"""

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from dacli.config.settings import GovernanceSettings
from dacli.connectors.base import (
    Connector, OperationSpec, Risk, ToolResult, ToolStatus,
)
from dacli.connectors.bigquery.connector import BigQueryConnector
from dacli.connectors.cli_base import CliResult
from dacli.eval.sim.cli import SimCli
from dacli.eval.sim.platforms import sim_settings
from dacli.governance.audit import AuditLedger
from dacli.governance.classifier import Tier
from dacli.governance.governor import Governor
from dacli.governance.permissions import PermissionRegistry, Scope


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# BigQuery: estimate_cost via the native dry_run
# ---------------------------------------------------------------------------
def _bq_dry_run_responder(byte_count):
    def respond(argv):
        if "--dry_run" in argv:
            if byte_count is None:
                return CliResult(1, "", "Syntax error", argv)
            return CliResult(
                0,
                "Query successfully validated. Assuming the tables are not "
                f"modified, running this query will process {byte_count:,} "
                "bytes of data.",
                "", argv,
            )
        return CliResult(0, json.dumps([]), "", argv)
    return respond


class BigQueryEstimateCostTest(unittest.TestCase):
    def _conn(self, byte_count):
        return BigQueryConnector(
            sim_settings("bigquery"), runner=SimCli(_bq_dry_run_responder(byte_count))
        )

    def test_estimate_reports_bytes_and_usd(self):
        ten_tib = 10 * 2**40
        est = _run(self._conn(ten_tib).estimate_cost(
            "execute_bigquery_query", {"query": "SELECT * FROM ds.huge"}
        ))
        self.assertEqual(est["bytes"], ten_tib)
        self.assertAlmostEqual(est["usd"], 62.5, places=2)

    def test_non_query_ops_have_no_estimate(self):
        est = _run(self._conn(100).estimate_cost(
            "introspect_bigquery_table", {"table": "t"}
        ))
        self.assertIsNone(est)

    def test_invalid_query_has_no_estimate(self):
        est = _run(self._conn(None).estimate_cost(
            "execute_bigquery_query", {"query": "SELEC nope"}
        ))
        self.assertIsNone(est)


# ---------------------------------------------------------------------------
# Governor: cost as a gate
# ---------------------------------------------------------------------------
class _CostConnector(Connector):
    name = "bigquery"

    def __init__(self, usd):
        super().__init__(settings=None)
        self._usd = usd
        self.estimate_calls = 0

    def operations(self):
        return [self.spec()]

    def spec(self):
        return OperationSpec(
            name="execute_bigquery_query", description="q",
            parameters={"type": "object", "properties": {}},
            capability="bigquery.query", risk=Risk.SAFE,
        )

    async def invoke(self, op, args):
        return ToolResult(tool_name=op, status=ToolStatus.SUCCESS, data=[])

    async def health(self):
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS)

    async def estimate_cost(self, op, args):
        self.estimate_calls += 1
        if self._usd is None:
            return None
        return {"bytes": 123, "usd": self._usd}


def _governor(*, threshold, approvals=None, approve=True):
    def approval_fn(request):
        approvals.append(request)
        return approve

    return Governor(
        permissions=PermissionRegistry(default_scope=Scope.ADMIN),
        ledger=AuditLedger(path=str(Path(tempfile.mkdtemp()) / "audit.jsonl")),
        approval_fn=approval_fn if approvals is not None else None,
        cost_confirm_usd=threshold,
        use_shadow=False,
    )


class CostGateTest(unittest.TestCase):
    SELECT = {"query": "SELECT * FROM ds.huge"}

    def test_settings_field_defaults_off(self):
        self.assertIsNone(GovernanceSettings().cost_confirm_usd)

    def test_estimate_above_threshold_triggers_confirm(self):
        conn = _CostConnector(usd=12.0)
        approvals = []
        gov = _governor(threshold=1.0, approvals=approvals)
        decision = _run(gov.review("execute_bigquery_query", conn.spec(), self.SELECT, conn))
        self.assertTrue(decision.allowed)
        self.assertEqual(len(approvals), 1)
        # The approval panel sees the estimate, and the tier was raised.
        self.assertEqual(approvals[0].cost_estimate["usd"], 12.0)
        self.assertEqual(decision.classification.tier, Tier.RISKY)
        self.assertTrue(any("cost" in r for r in decision.classification.reasons))

    def test_estimate_below_threshold_runs_auto(self):
        conn = _CostConnector(usd=0.4)
        approvals = []
        gov = _governor(threshold=1.0, approvals=approvals)
        decision = _run(gov.review("execute_bigquery_query", conn.spec(), self.SELECT, conn))
        self.assertTrue(decision.allowed)
        self.assertEqual(approvals, [])
        self.assertEqual(decision.classification.tier, Tier.SAFE)

    def test_no_threshold_means_hook_is_never_consulted(self):
        conn = _CostConnector(usd=1000.0)
        approvals = []
        gov = _governor(threshold=None, approvals=approvals)
        decision = _run(gov.review("execute_bigquery_query", conn.spec(), self.SELECT, conn))
        self.assertTrue(decision.allowed)
        self.assertEqual(conn.estimate_calls, 0)
        self.assertEqual(approvals, [])

    def test_cost_confirm_is_fail_closed_without_an_approver(self):
        conn = _CostConnector(usd=12.0)
        gov = _governor(threshold=1.0, approvals=None)
        decision = _run(gov.review("execute_bigquery_query", conn.spec(), self.SELECT, conn))
        self.assertFalse(decision.allowed)

    def test_a_raising_hook_never_breaks_review(self):
        conn = _CostConnector(usd=12.0)

        async def boom(op, args):
            raise RuntimeError("estimator exploded")

        conn.estimate_cost = boom
        approvals = []
        gov = _governor(threshold=1.0, approvals=approvals)
        decision = _run(gov.review("execute_bigquery_query", conn.spec(), self.SELECT, conn))
        self.assertTrue(decision.allowed)  # safe SELECT runs auto, estimate unknown
        self.assertEqual(approvals, [])


class ApprovalRequestCostTest(unittest.TestCase):
    def test_describe_includes_the_estimate(self):
        conn = _CostConnector(usd=42.0)
        approvals = []
        gov = _governor(threshold=1.0, approvals=approvals)
        _run(gov.review("execute_bigquery_query", conn.spec(),
                        {"query": "SELECT 1"}, conn))
        self.assertIn("42.00", approvals[0].describe())


if __name__ == "__main__":
    unittest.main()
