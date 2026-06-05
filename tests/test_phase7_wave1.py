"""Wave 1 connector golden tests (offline, CLI runner injected).

CLI-first connectors are exercised with a fake subprocess runner returning canned
platform output, so these tests run with no cloud credentials and never shell
out. They verify the parts that must be correct regardless of the live platform:
operation surface, risk tiers, environment-anchored post-conditions, catalog
effects, and native rollback wiring.

Run with:
    python -m unittest tests.test_phase7_wave1
"""

import asyncio
import json
import os
import tempfile
import types
import unittest

from connectors.base import Risk, ToolStatus
from connectors.cli_base import CliResult
from core.verify import VerificationContext, run_postconditions

from connectors.dbt.connector import DbtConnector
from connectors.bigquery.connector import (
    BigQueryConnector, bigquery_ddl_object_exists, _split_ref,
)
from connectors.databricks.connector import DatabricksConnector
from connectors.s3.connector import S3Connector
from connectors.gcs.connector import GCSConnector

from governance.rollback import RollbackStrategist
from governance.classifier import Classification, Tier


def _run(coro):
    return asyncio.run(coro)


def _settings(**sections):
    return types.SimpleNamespace(**sections)


def _runner(responder):
    """Wrap a synchronous responder(argv, stdin)->CliResult as an async runner."""
    async def run(argv, *, cwd=None, env=None, timeout=None, stdin=None):
        return responder(list(argv), stdin)
    return run


# ===========================================================================
# dbt — artifacts (run_results.json / manifest.json) are the oracle
# ===========================================================================
class DbtConnectorTest(unittest.TestCase):
    def _project(self, run_results=None, manifest=None):
        d = tempfile.mkdtemp(prefix="dacli_dbt_")
        target = os.path.join(d, "target")
        os.makedirs(target, exist_ok=True)
        if run_results is not None:
            with open(os.path.join(target, "run_results.json"), "w", encoding="utf-8") as f:
                json.dump(run_results, f)
        if manifest is not None:
            with open(os.path.join(target, "manifest.json"), "w", encoding="utf-8") as f:
                json.dump(manifest, f)
        return d

    def _conn(self, project_dir):
        cfg = types.SimpleNamespace(project_dir=project_dir, profiles_dir="",
                                    target="", dbt_binary="dbt", timeout=900)
        return DbtConnector(_settings(dbt=cfg), runner=_runner(lambda argv, stdin: CliResult(0, "ok", "", argv)))

    def test_run_succeeds_and_nodes_pass(self):
        proj = self._project(run_results={"results": [
            {"unique_id": "model.proj.stg_orders", "status": "success"},
            {"unique_id": "model.proj.dim_orders", "status": "success"},
        ]})
        conn = self._conn(proj)
        res = _run(conn.invoke("dbt_run", {}))
        self.assertEqual(res.status, ToolStatus.SUCCESS)
        ctx = VerificationContext(args={}, result=res, target=conn)
        report = _run(run_postconditions(conn.operations()[1].postconditions, ctx))
        self.assertTrue(report.passed, report.summary())

    def test_failed_node_is_caught(self):
        proj = self._project(run_results={"results": [
            {"unique_id": "model.proj.dim_orders", "status": "error", "message": "boom"},
        ]})
        conn = self._conn(proj)
        res = _run(conn.invoke("dbt_run", {}))
        ctx = VerificationContext(args={}, result=res, target=conn)
        # dbt_nodes_succeeded should fail even though the CLI returned rc 0.
        report = _run(run_postconditions([conn.operations()[1].postconditions[1]], ctx))
        self.assertFalse(report.passed)
        self.assertIn("failed", report.summary())

    def test_run_nonzero_rc_fails(self):
        proj = self._project(run_results={"results": []})
        cfg = types.SimpleNamespace(project_dir=proj, profiles_dir="", target="",
                                    dbt_binary="dbt", timeout=900)
        conn = DbtConnector(_settings(dbt=cfg),
                            runner=_runner(lambda argv, stdin: CliResult(1, "", "compilation error", argv)))
        res = _run(conn.invoke("dbt_build", {}))
        self.assertEqual(res.status, ToolStatus.ERROR)

    def test_introspect_manifest_lists_models(self):
        proj = self._project(manifest={
            "nodes": {
                "model.proj.stg": {"resource_type": "model", "name": "stg"},
                "test.proj.t": {"resource_type": "test", "name": "t"},
            },
            "sources": {"source.proj.raw": {"name": "raw"}},
        })
        conn = self._conn(proj)
        res = _run(conn.invoke("introspect_dbt_manifest", {}))
        self.assertTrue(res.data["exists"])
        self.assertEqual(res.data["node_count"], 2)
        self.assertIn("stg", res.data["models"])
        # introspection emits a catalog effect per model
        self.assertTrue(res.metadata["catalog_effects"])

    def test_run_op_is_risky(self):
        conn = self._conn(self._project(run_results={"results": []}))
        run_op = next(o for o in conn.operations() if o.name == "dbt_run")
        self.assertEqual(run_op.risk, Risk.RISKY)


# ===========================================================================
# BigQuery — bq show is the oracle for a CREATE
# ===========================================================================
class BigQueryConnectorTest(unittest.TestCase):
    def _conn(self, responder):
        cfg = types.SimpleNamespace(project="proj", dataset="ds", location="US",
                                    bq_binary="bq", timeout=300)
        return BigQueryConnector(_settings(bigquery=cfg), runner=_runner(responder))

    def test_create_table_confirmed_by_bq_show(self):
        def responder(argv, stdin):
            if "show" in argv:
                return CliResult(0, json.dumps({"schema": {"fields": [
                    {"name": "ID", "type": "INT64"}]}}), "", argv)
            return CliResult(0, "[]", "", argv)  # query
        conn = self._conn(responder)
        q = "CREATE TABLE ds.customers (ID INT64)"
        res = _run(conn.invoke("execute_bigquery_query", {"query": q}))
        self.assertEqual(res.status, ToolStatus.SUCCESS)
        ctx = VerificationContext(args={"query": q}, result=res, target=conn)
        report = _run(run_postconditions([bigquery_ddl_object_exists()], ctx))
        self.assertTrue(report.passed, report.summary())

    def test_create_table_missing_object_is_caught(self):
        def responder(argv, stdin):
            if "show" in argv:
                return CliResult(1, "", "Not found: Table ds:customers", argv)
            return CliResult(0, "[]", "", argv)
        conn = self._conn(responder)
        q = "CREATE TABLE ds.customers (ID INT64)"
        res = _run(conn.invoke("execute_bigquery_query", {"query": q}))
        ctx = VerificationContext(args={"query": q}, result=res, target=conn)
        report = _run(run_postconditions([bigquery_ddl_object_exists()], ctx))
        self.assertFalse(report.passed)
        self.assertIn("not found", report.summary().lower())

    def test_dry_run_reports_bytes(self):
        def responder(argv, stdin):
            return CliResult(0, "Query successfully validated. Assuming the tables are "
                                "not modified, running this query will process 1234 bytes "
                                "of data.", "", argv)
        conn = self._conn(responder)
        res = _run(conn.invoke("bigquery_dry_run", {"query": "SELECT 1"}))
        self.assertTrue(res.data["valid"])
        self.assertEqual(res.data["bytes_processed"], 1234)

    def test_select_rows_parsed(self):
        rows = [{"id": 1}, {"id": 2}]
        conn = self._conn(lambda argv, stdin: CliResult(0, json.dumps(rows), "", argv))
        res = _run(conn.invoke("execute_bigquery_query", {"query": "SELECT id FROM ds.t"}))
        self.assertEqual(res.data, rows)

    def test_split_ref(self):
        self.assertEqual(_split_ref("proj:ds.tbl"),
                         {"project": "proj", "dataset": "ds", "table": "tbl"})
        self.assertEqual(_split_ref("ds.tbl")["table"], "tbl")

    def test_verify_rollback_blocks_drop_of_missing_table(self):
        def responder(argv, stdin):
            return CliResult(1, "", "Not found", argv)  # bq show → missing
        conn = self._conn(responder)
        plan = types.SimpleNamespace(primitive="bq_time_travel_snapshot")
        ok, detail = _run(conn.verify_rollback(plan, {"query": "DROP TABLE ds.gone"}))
        self.assertFalse(ok)


# ===========================================================================
# Databricks — the statement STATE is the oracle
# ===========================================================================
class DatabricksConnectorTest(unittest.TestCase):
    def _conn(self, responder):
        cfg = types.SimpleNamespace(host="h", token="t", warehouse_id="w",
                                    catalog="main", db_schema="default",
                                    databricks_binary="databricks", timeout=300)
        return DatabricksConnector(_settings(databricks=cfg), runner=_runner(responder))

    def test_succeeded_state_passes(self):
        payload = {"status": {"state": "SUCCEEDED"},
                   "manifest": {"schema": {"columns": [{"name": "c"}]}},
                   "result": {"data_array": [["1"]]}}
        conn = self._conn(lambda argv, stdin: CliResult(0, json.dumps(payload), "", argv))
        res = _run(conn.invoke("execute_databricks_sql", {"query": "SELECT 1 AS c"}))
        self.assertEqual(res.status, ToolStatus.SUCCESS)
        self.assertEqual(res.data, [{"c": "1"}])
        ctx = VerificationContext(args={}, result=res, target=conn)
        op = conn.operations()[0]
        report = _run(run_postconditions(op.postconditions, ctx))
        self.assertTrue(report.passed, report.summary())

    def test_failed_state_is_caught(self):
        payload = {"status": {"state": "FAILED"}}
        conn = self._conn(lambda argv, stdin: CliResult(0, json.dumps(payload), "", argv))
        res = _run(conn.invoke("execute_databricks_sql", {"query": "SELECT 1"}))
        ctx = VerificationContext(args={}, result=res, target=conn)
        op = conn.operations()[0]
        report = _run(run_postconditions(op.postconditions, ctx))
        self.assertFalse(report.passed)
        self.assertIn("FAILED", report.summary())


# ===========================================================================
# S3 / GCS — a live head/ls after the mutation is the oracle
# ===========================================================================
class S3ConnectorTest(unittest.TestCase):
    def _conn(self, responder):
        cfg = types.SimpleNamespace(bucket="b", prefix="", region="", profile="",
                                    aws_binary="aws", timeout=300)
        return S3Connector(_settings(s3=cfg), runner=_runner(responder))

    def test_put_then_present(self):
        def responder(argv, stdin):
            return CliResult(0, "", "", argv)  # cp ok; head-object ok (exists)
        conn = self._conn(responder)
        res = _run(conn.invoke("put_s3_object", {"key": "k", "content": "hi"}))
        self.assertTrue(res.data["exists"])
        op = next(o for o in conn.operations() if o.name == "put_s3_object")
        ctx = VerificationContext(args={}, result=res, target=conn)
        self.assertTrue(_run(run_postconditions(op.postconditions, ctx)).passed)

    def test_delete_then_absent(self):
        def responder(argv, stdin):
            if "head-object" in argv:
                return CliResult(255, "", "Not Found", argv)  # gone
            return CliResult(0, "", "", argv)  # delete ok
        conn = self._conn(responder)
        res = _run(conn.invoke("delete_s3_object", {"key": "k"}))
        self.assertFalse(res.data["exists"])
        op = next(o for o in conn.operations() if o.name == "delete_s3_object")
        ctx = VerificationContext(args={}, result=res, target=conn)
        self.assertTrue(_run(run_postconditions(op.postconditions, ctx)).passed)

    def test_delete_is_irreversible(self):
        conn = self._conn(lambda argv, stdin: CliResult(0, "", "", argv))
        op = next(o for o in conn.operations() if o.name == "delete_s3_object")
        self.assertEqual(op.risk, Risk.IRREVERSIBLE)

    def test_verify_rollback_requires_versioning(self):
        def enabled(argv, stdin):
            return CliResult(0, json.dumps({"Status": "Enabled"}), "", argv)
        conn = self._conn(enabled)
        plan = types.SimpleNamespace(primitive="versioned_copy_aside")
        ok, _ = _run(conn.verify_rollback(plan, {"key": "k"}))
        self.assertTrue(ok)

        def disabled(argv, stdin):
            return CliResult(0, json.dumps({}), "", argv)
        conn2 = self._conn(disabled)
        ok2, _ = _run(conn2.verify_rollback(plan, {"key": "k"}))
        self.assertFalse(ok2)

    def test_list_objects(self):
        payload = {"Contents": [{"Key": "a", "Size": 1}, {"Key": "b", "Size": 2}]}
        conn = self._conn(lambda argv, stdin: CliResult(0, json.dumps(payload), "", argv))
        res = _run(conn.invoke("list_s3_objects", {}))
        self.assertEqual(res.data["count"], 2)


class GCSConnectorTest(unittest.TestCase):
    def _conn(self, responder):
        cfg = types.SimpleNamespace(bucket="b", prefix="", project="p",
                                    credentials_path="", gcloud_binary="gcloud", timeout=300)
        return GCSConnector(_settings(gcs=cfg), runner=_runner(responder))

    def test_put_then_present(self):
        def responder(argv, stdin):
            if "ls" in argv:
                return CliResult(0, json.dumps([{"url": "gs://b/k", "size": 2}]), "", argv)
            return CliResult(0, "", "", argv)
        conn = self._conn(responder)
        res = _run(conn.invoke("put_gcs_object", {"key": "k", "content": "hi"}))
        self.assertTrue(res.data["exists"])

    def test_delete_then_absent(self):
        def responder(argv, stdin):
            if "ls" in argv:
                return CliResult(1, "[]", "", argv)  # gone
            return CliResult(0, "", "", argv)
        conn = self._conn(responder)
        res = _run(conn.invoke("delete_gcs_object", {"key": "k"}))
        self.assertFalse(res.data["exists"])

    def test_verify_rollback_requires_versioning(self):
        conn = self._conn(lambda argv, stdin: CliResult(
            0, json.dumps({"versioning": {"enabled": True}}), "", argv))
        plan = types.SimpleNamespace(primitive="versioned_copy_aside")
        ok, _ = _run(conn.verify_rollback(plan, {"key": "k"}))
        self.assertTrue(ok)


# ===========================================================================
# Rollback parity — each platform maps to its native primitive
# ===========================================================================
class RollbackParityTest(unittest.TestCase):
    def _cls(self, tool, tier, verb=None):
        return Classification(tool_name=tool, tier=tier, declared_risk=Risk.RISKY, sql_verb=verb)

    def test_native_primitives(self):
        strat = RollbackStrategist()
        cases = [
            ("bigquery", self._cls("q", Tier.IRREVERSIBLE, "DROP"), "bq_time_travel_snapshot"),
            ("bigquery", self._cls("q", Tier.RISKY, "DELETE"), "transaction"),
            ("databricks", self._cls("q", Tier.IRREVERSIBLE, "DROP"), "delta_time_travel"),
            ("dbt", self._cls("dbt_run", Tier.RISKY, None), "git_versioned_transform"),
            ("s3", self._cls("delete_s3_object", Tier.IRREVERSIBLE, None), "versioned_copy_aside"),
            ("gcs", self._cls("delete_gcs_object", Tier.IRREVERSIBLE, None), "versioned_copy_aside"),
        ]
        for connector_id, cls, expected in cases:
            plan = strat.plan_for(connector_id, cls)
            self.assertEqual(plan.primitive, expected,
                             f"{connector_id} expected {expected}, got {plan.primitive}")


if __name__ == "__main__":
    unittest.main()
