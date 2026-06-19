"""P14 slice C: warehouse cost / credit advisor.

Pre-run estimate reuses the connector's native estimator (BigQuery dry-run);
post-hoc session cost reads the platform history view (Snowflake QUERY_HISTORY /
BigQuery INFORMATION_SCHEMA.JOBS / Databricks system tables) read-only through
the governed dispatcher. Deterministic and offline.
"""

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from dacli.connectors.base import (
    Connector, OperationSpec, Risk, ToolResult, ToolStatus,
)
from dacli.connectors.bigquery.connector import BigQueryConnector
from dacli.connectors.cli_base import CliResult
from dacli.connectors.dispatcher import Dispatcher
from dacli.core import cost_advisor
from dacli.core.store import DacliStore
from dacli.core.verify import Verifier
from dacli.eval.sim.cli import SimCli
from dacli.eval.sim.platforms import sim_settings
from dacli.governance.audit import AuditLedger
from dacli.governance.governor import Governor
from dacli.governance.permissions import PermissionRegistry, Scope


def _run(coro):
    return asyncio.run(coro)


class _SimRegistry:
    def __init__(self, connector):
        self._conn = connector
        self._specs = {op.name: op for op in connector.operations()}

    def resolve(self, tool_name):
        return (self._conn, tool_name) if tool_name in self._specs else None

    def get_operation_spec(self, tool_name):
        return self._specs.get(tool_name)

    def get_connector(self, connector_id):
        return self._conn if connector_id == "bigquery" else None

    def is_builtin(self, connector_id):
        return False


def _bq_dispatcher(responder):
    conn = BigQueryConnector(sim_settings("bigquery"), runner=SimCli(responder))
    gov = Governor(
        permissions=PermissionRegistry(default_scope=Scope.ADMIN),
        ledger=AuditLedger(path=str(Path(tempfile.mkdtemp()) / "audit.jsonl")),
    )
    return Dispatcher(registry=_SimRegistry(conn), verifier=Verifier(), governor=gov), conn


# ---------------------------------------------------------------------------
# Pre-run estimate (BigQuery dry-run)
# ---------------------------------------------------------------------------
def _dry_run_responder(byte_count):
    def respond(argv):
        if "--dry_run" in argv:
            return CliResult(
                0, "Query successfully validated. Assuming the tables are not "
                f"modified, running this query will process {byte_count:,} bytes "
                "of data.", "", argv)
        return CliResult(0, json.dumps([]), "", argv)
    return respond


class EstimateTest(unittest.TestCase):
    def test_estimate_before_a_costly_query(self):
        ten_tib = 10 * 2**40
        _disp, conn = _bq_dispatcher(_dry_run_responder(ten_tib))
        est = _run(cost_advisor.estimate(
            conn, "execute_bigquery_query", {"query": "SELECT * FROM ds.huge"}))
        self.assertIsNotNone(est)
        self.assertEqual(est.bytes, ten_tib)
        self.assertAlmostEqual(est.usd, 62.5, places=2)

    def test_estimate_none_when_connector_has_no_estimator(self):
        class _Bare(Connector):
            name = "x"

            def operations(self):
                return []

            async def invoke(self, op, args):
                return ToolResult(tool_name=op, status=ToolStatus.SUCCESS)

            async def health(self):
                return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS)

        # Connector.estimate_cost defaults to None.
        est = _run(cost_advisor.estimate(_Bare(settings=None), "x", {}))
        self.assertIsNone(est)


# ---------------------------------------------------------------------------
# Post-hoc session cost
# ---------------------------------------------------------------------------
class SessionCostTest(unittest.TestCase):
    def test_bigquery_session_cost_from_history_view(self):
        def respond(argv):
            sql = argv[-1]
            if "JOBS_BY_PROJECT" in sql:
                return CliResult(0, json.dumps([
                    {"total_bytes_billed": 5 * 2**40},
                    {"total_bytes_billed": 2 * 2**40},
                ]), "", argv)
            return CliResult(0, json.dumps([]), "", argv)

        disp, _conn = _bq_dispatcher(respond)
        sc = _run(cost_advisor.session_cost("bigquery", disp))
        self.assertTrue(sc.ok)
        self.assertEqual(sc.queries, 2)
        self.assertEqual(sc.bytes, 7 * 2**40)
        self.assertAlmostEqual(sc.usd, 7 * 6.25, places=2)

    def test_unsupported_connector_is_an_error(self):
        disp, _conn = _bq_dispatcher(lambda argv: CliResult(0, "[]", "", argv))
        sc = _run(cost_advisor.session_cost("postgres", disp))
        self.assertFalse(sc.ok)
        self.assertIn("no warehouse cost view", sc.error)

    def test_blocked_history_read_degrades_to_error(self):
        class _DenyDispatcher:
            _registry = None

            async def execute(self, tool, args):
                return ToolResult(tool_name=tool, status=ToolStatus.DENIED,
                                  error="permission denied")

        sc = _run(cost_advisor.session_cost("snowflake", _DenyDispatcher()))
        self.assertFalse(sc.ok)
        self.assertIn("blocked by governance", sc.error)


class AggregatorTest(unittest.TestCase):
    def test_snowflake_credits_to_usd(self):
        agg = cost_advisor._snowflake_aggregate([
            {"CREDITS_USED_CLOUD_SERVICES": 1.5, "BYTES_SCANNED": 100},
            {"CREDITS_USED_CLOUD_SERVICES": 0.5, "BYTES_SCANNED": 200},
        ])
        self.assertEqual(agg["queries"], 2)
        self.assertEqual(agg["credits"], 2.0)
        self.assertAlmostEqual(agg["usd"], 2.0 * cost_advisor.USD_PER_CREDIT_SNOWFLAKE)

    def test_databricks_dbus_to_usd(self):
        agg = cost_advisor._databricks_aggregate([
            {"usage_quantity": 10}, {"usage_quantity": 5},
        ])
        self.assertEqual(agg["credits"], 15.0)
        self.assertAlmostEqual(agg["usd"], 15.0 * cost_advisor.USD_PER_DBU_DATABRICKS)


# ---------------------------------------------------------------------------
# Toolbar plumbing: store accumulator + governor on_cost hook
# ---------------------------------------------------------------------------
class StoreWarehouseCostTest(unittest.TestCase):
    def test_record_and_read_session_warehouse_usd(self):
        store = DacliStore(base_dir=tempfile.mkdtemp())
        self.assertEqual(store.session_warehouse_usd("s1"), 0.0)
        store.record_warehouse_cost("s1", 12.5)
        store.record_warehouse_cost("s1", 0.5)
        self.assertAlmostEqual(store.session_warehouse_usd("s1"), 13.0)


class _EstimatingConnector(Connector):
    name = "bigquery"

    def __init__(self, usd):
        super().__init__(settings=None)
        self._usd = usd

    def operations(self):
        return [self.spec()]

    def spec(self):
        return OperationSpec(name="execute_bigquery_query", description="q",
                             parameters={"type": "object", "properties": {}},
                             capability="bigquery.query", risk=Risk.SAFE)

    async def invoke(self, op, args):
        return ToolResult(tool_name=op, status=ToolStatus.SUCCESS, data=[])

    async def health(self):
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS)

    async def estimate_cost(self, op, args):
        return {"bytes": 123, "usd": self._usd}


class GovernorCostHookTest(unittest.TestCase):
    def test_on_cost_fires_with_the_estimate(self):
        recorded = []
        conn = _EstimatingConnector(usd=0.5)
        gov = Governor(
            permissions=PermissionRegistry(default_scope=Scope.ADMIN),
            ledger=AuditLedger(path=str(Path(tempfile.mkdtemp()) / "audit.jsonl")),
            cost_confirm_usd=10.0,            # gate on → estimate is computed
            on_cost=recorded.append,
            use_shadow=False,
        )
        decision = _run(gov.review("execute_bigquery_query", conn.spec(),
                                   {"query": "SELECT 1"}, conn))
        self.assertTrue(decision.allowed)     # 0.5 < 10.0 → auto
        self.assertEqual(recorded, [0.5])

    def test_no_gate_means_no_estimate_and_no_hook(self):
        recorded = []
        conn = _EstimatingConnector(usd=99.0)
        gov = Governor(
            permissions=PermissionRegistry(default_scope=Scope.ADMIN),
            ledger=AuditLedger(path=str(Path(tempfile.mkdtemp()) / "audit.jsonl")),
            cost_confirm_usd=None,            # gate off → hook never fires
            on_cost=recorded.append,
        )
        _run(gov.review("execute_bigquery_query", conn.spec(), {"query": "SELECT 1"}, conn))
        self.assertEqual(recorded, [])


class DoctorCostSectionTest(unittest.TestCase):
    def test_doctor_reports_cost_posture(self):
        from dacli.config.settings import Settings
        from dacli.core import doctor

        s = Settings.model_validate({"governance": {"cost_confirm_usd": 25.0}})
        diag = doctor.collect(s)
        self.assertEqual(diag.cost["confirm_usd"], 25.0)
        self.assertIn("advisors", diag.cost)
        self.assertIn("cost", diag.to_dict())


if __name__ == "__main__":
    unittest.main()
