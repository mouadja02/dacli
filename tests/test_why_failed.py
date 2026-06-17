"""P13: the pipeline-failure explainer (`dacli why-failed`).

Drives the explainer against simulated dbt and Airflow failures with no live
platform: dbt via a fixture ``run_results.json``, Airflow via the connector's
injected HTTP transport. The reads run through the *real* governed dispatcher, so
"every read is governed and read-only" is asserted, not assumed; the proposed fix
is never executed unless the run opts in, and an opted-in fix flows through the
same classify→approve→verify→rollback gate as any action.
"""

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from dacli.connectors.airflow.connector import AirflowConnector
from dacli.connectors.http_base import HttpResult
from dacli.core.verify import Verifier
from dacli.core.why_failed import (
    ProposedFix,
    explain_failure,
    locate_airflow_failure,
    locate_dbt_failure,
    propose_fix,
    read_dbt_run_results,
)
from dacli.governance.audit import AuditLedger
from dacli.governance.governor import Governor
from dacli.governance.permissions import PermissionRegistry, Scope
from dacli.memory.graph.lineage import LineageNode, LineageStore


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Offline harness: sim connectors behind the real dispatcher + governor.
# ---------------------------------------------------------------------------
class _Reg:
    """Minimal registry over pre-built (sim-backed) connectors."""

    def __init__(self, connectors: dict):
        self._c = connectors
        self._idx: dict[str, tuple[str, str]] = {}
        for cid, conn in connectors.items():
            for spec in conn.operations():
                self._idx[spec.name] = (cid, spec.name)

    def resolve(self, name):
        e = self._idx.get(name)
        return (self._c[e[0]], e[1]) if e else None

    def get_operation_spec(self, name):
        e = self._idx.get(name)
        if not e:
            return None
        return next(s for s in self._c[e[0]].operations() if s.name == e[1])

    def is_builtin(self, _cid):
        return False


def _dispatcher(connectors, *, approvals=None, approve=True):
    from dacli.connectors.dispatcher import Dispatcher

    def approval_fn(request):
        if approvals is not None:
            approvals.append(request)
        return approve

    gov = Governor(
        permissions=PermissionRegistry(default_scope=Scope.ADMIN),
        ledger=AuditLedger(path=str(Path(tempfile.mkdtemp()) / "audit.jsonl")),
        approval_fn=approval_fn,
        use_shadow=False,
    )
    disp = Dispatcher(_Reg(connectors), governor=gov, verifier=Verifier(enforce=True))
    return disp, gov


def _settings_ns(**sections):
    import types

    return types.SimpleNamespace(**sections)


def _airflow(handler):
    async def transport(method, path, *, params=None, json=None, headers=None):
        return handler(method, path, params, json)

    cfg = {"base_url": "http://af", "username": "u", "password": "p",
           "token": "", "poll_interval": 1, "timeout": 4}
    return AirflowConnector(_settings_ns(connector_config={"airflow": cfg}), transport=transport)


def _failed_airflow_handler(*, calls=None):
    """A DAG whose latest run failed at the `load_orders` task."""

    def handler(method, path, params, body):
        if calls is not None:
            calls.append((method, path))
        # locate: most recent failed run
        if method == "GET" and path.endswith("/dagRuns") and (params or {}).get("state") == "failed":
            return HttpResult(200, {"dag_runs": [
                {"dag_run_id": "run_2", "state": "failed"}]})
        # task instances for that run
        if method == "GET" and path.endswith("/dagRuns/run_2/taskInstances"):
            return HttpResult(200, {"task_instances": [
                {"task_id": "extract", "state": "success"},
                {"task_id": "load_orders", "state": "failed"}]})
        # the failed task's logs
        if method == "GET" and "/taskInstances/load_orders/logs/" in path:
            return HttpResult(200, {"content":
                "Traceback ... psycopg2.errors.UndefinedTable: relation "
                "\"raw.orders\" does not exist"})
        # apply: re-trigger the DAG, polled to success
        if method == "POST" and path.endswith("/dagRuns"):
            return HttpResult(200, {"dag_run_id": "run_3", "state": "queued"})
        if method == "GET" and path.endswith("/dagRuns/run_3"):
            return HttpResult(200, {"state": "success"})
        if method == "GET" and path.endswith("/dagRuns/run_3/taskInstances"):
            return HttpResult(200, {"task_instances": [
                {"task_id": "load_orders", "state": "success"}]})
        return HttpResult(404, {})

    return handler


# ---------------------------------------------------------------------------
# Pure locators
# ---------------------------------------------------------------------------
class LocatorTest(unittest.TestCase):
    def test_dbt_locates_first_failed_node(self):
        results = [
            {"node": "model.shop.stg_orders", "status": "success", "message": None},
            {"node": "model.shop.fct_orders", "status": "error",
             "message": "Database Error: relation raw.orders does not exist"},
        ]
        finding = locate_dbt_failure(results)
        self.assertEqual(finding.failing_node, "model.shop.fct_orders")
        self.assertEqual(finding.object_name, "fct_orders")
        self.assertIn("raw.orders", finding.message)

    def test_dbt_clean_run_has_no_failure(self):
        results = [{"node": "model.shop.a", "status": "success", "message": None}]
        self.assertIsNone(locate_dbt_failure(results))

    def test_read_run_results_from_project(self):
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "target"
            target.mkdir()
            (target / "run_results.json").write_text(json.dumps({"results": [
                {"unique_id": "model.shop.x", "status": "error", "message": "boom"}]}),
                encoding="utf-8")
            results = read_dbt_run_results(d)
        self.assertEqual(results[0]["status"], "error")

    def test_airflow_locates_failed_task(self):
        finding = locate_airflow_failure(
            dag_id="daily",
            run={"dag_run_id": "run_2", "state": "failed"},
            task_instances=[{"task_id": "ok", "state": "success"},
                            {"task_id": "load", "state": "failed"}],
            logs="ERROR: boom",
        )
        self.assertEqual(finding.failing_node, "load")
        self.assertEqual(finding.run_id, "run_2")
        self.assertIn("boom", finding.log_excerpt)

    def test_propose_fix_dbt_reruns_failed_model(self):
        finding = locate_dbt_failure(
            [{"node": "model.shop.fct_orders", "status": "error", "message": "x"}])
        fix = propose_fix(finding)
        self.assertEqual(fix.tool_name, "dbt_run")
        self.assertEqual(fix.args.get("select"), "fct_orders")
        self.assertFalse(fix.applied)

    def test_propose_fix_airflow_retriggers(self):
        finding = locate_airflow_failure(
            dag_id="daily", run={"dag_run_id": "r", "state": "failed"},
            task_instances=[{"task_id": "load", "state": "failed"}], logs="")
        fix = propose_fix(finding)
        self.assertEqual(fix.tool_name, "trigger_airflow_dag")
        self.assertEqual(fix.args.get("dag_id"), "daily")


# ---------------------------------------------------------------------------
# Airflow connector read-only ops (new in P13)
# ---------------------------------------------------------------------------
class AirflowReadOpsTest(unittest.TestCase):
    def test_list_dag_runs_filters_by_state(self):
        conn = _airflow(_failed_airflow_handler())
        res = _run(conn.invoke("list_airflow_dag_runs",
                               {"dag_id": "daily", "state": "failed"}))
        self.assertTrue(res.success)
        self.assertEqual(res.data["dag_runs"][0]["dag_run_id"], "run_2")

    def test_task_logs_returns_content(self):
        conn = _airflow(_failed_airflow_handler())
        res = _run(conn.invoke("get_airflow_task_logs",
                               {"dag_id": "daily", "dag_run_id": "run_2",
                                "task_id": "load_orders"}))
        self.assertTrue(res.success)
        self.assertIn("raw.orders", res.data["content"])

    def test_read_ops_are_safe(self):
        conn = _airflow(_failed_airflow_handler())
        from dacli.connectors.base import Risk

        for name in ("list_airflow_dag_runs", "get_airflow_task_logs"):
            op = next(o for o in conn.operations() if o.name == name)
            self.assertEqual(op.risk, Risk.SAFE)


# ---------------------------------------------------------------------------
# End-to-end: dbt
# ---------------------------------------------------------------------------
class DbtExplainTest(unittest.TestCase):
    def _project(self, status="error"):
        d = Path(tempfile.mkdtemp())
        (d / "target").mkdir()
        (d / "target" / "run_results.json").write_text(json.dumps({"results": [
            {"unique_id": "model.shop.stg_orders", "status": "success"},
            {"unique_id": "model.shop.fct_orders", "status": status,
             "message": "Database Error: relation \"raw.orders\" does not exist"}]}),
            encoding="utf-8")
        return str(d)

    def _lineage(self):
        store = LineageStore()
        store.add(LineageNode("analytics.fct_orders", "table"),
                  LineageNode("analytics.revenue_dashboard", "dbt model",
                              label="revenue_dashboard"), source="dbt")
        return store

    def test_explains_dbt_failure_with_blast_radius_and_unapplied_fix(self):
        explanation = _run(explain_failure(
            source="dbt", dbt_project_dir=self._project(), lineage=self._lineage()))
        self.assertEqual(explanation.finding.failing_node, "model.shop.fct_orders")
        self.assertIn("raw.orders", explanation.root_cause)
        self.assertIn("raw.orders", explanation.finding.log_excerpt)
        labels = {n["label"] for n in explanation.downstream}
        self.assertIn("revenue_dashboard", labels)
        self.assertEqual(explanation.proposed_fix.tool_name, "dbt_run")
        self.assertFalse(explanation.proposed_fix.applied)

    def test_clean_dbt_run_reports_no_failure(self):
        explanation = _run(explain_failure(
            source="dbt", dbt_project_dir=self._project(status="success")))
        self.assertIsNone(explanation.finding)
        self.assertFalse(explanation.ok)

    def test_json_is_parseable(self):
        explanation = _run(explain_failure(
            source="dbt", dbt_project_dir=self._project()))
        payload = json.loads(explanation.to_json())
        self.assertEqual(payload["source"], "dbt")
        self.assertEqual(payload["proposed_fix"]["tool_name"], "dbt_run")


# ---------------------------------------------------------------------------
# End-to-end: Airflow (governed reads + governed, opt-in fix)
# ---------------------------------------------------------------------------
class AirflowExplainTest(unittest.TestCase):
    def test_locates_failure_through_governed_reads_without_applying_fix(self):
        calls = []
        conn = _airflow(_failed_airflow_handler(calls=calls))
        disp, _gov = _dispatcher({"airflow": conn})
        explanation = _run(explain_failure(
            source="airflow", dag="daily", dispatcher=disp))
        self.assertEqual(explanation.finding.failing_node, "load_orders")
        self.assertIn("raw.orders", explanation.finding.log_excerpt)
        self.assertEqual(explanation.proposed_fix.tool_name, "trigger_airflow_dag")
        self.assertFalse(explanation.proposed_fix.applied)
        # The fix (a POST) was never sent — reads only.
        self.assertFalse(any(m == "POST" for m, _ in calls))

    def test_accepted_fix_flows_through_classify_approve_verify_rollback(self):
        approvals = []
        conn = _airflow(_failed_airflow_handler())
        disp, gov = _dispatcher({"airflow": conn}, approvals=approvals, approve=True)
        explanation = _run(explain_failure(
            source="airflow", dag="daily", dispatcher=disp, apply=True))
        self.assertTrue(explanation.proposed_fix.applied)
        self.assertEqual(explanation.proposed_fix.status, "success")
        self.assertEqual(len(approvals), 1)
        # The fix's decision went through every governance stage.
        kinds = {e.get("kind") for e in gov.ledger.all_events()}
        self.assertTrue({"classification", "approval", "rollback",
                         "execution", "post_condition"} <= kinds)

    def test_denied_fix_is_not_executed(self):
        conn = _airflow(_failed_airflow_handler())
        disp, _gov = _dispatcher({"airflow": conn}, approvals=[], approve=False)
        explanation = _run(explain_failure(
            source="airflow", dag="daily", dispatcher=disp, apply=True))
        self.assertFalse(explanation.proposed_fix.applied)
        self.assertNotEqual(explanation.proposed_fix.status, "success")


class WhyFailedCliTest(unittest.TestCase):
    """`dacli why-failed --source dbt --json` over a fixture run_results.json."""

    def test_dbt_command_emits_parseable_json(self):
        import os

        import yaml
        from click.testing import CliRunner

        from dacli.scripts.cli import cli

        runner = CliRunner()
        with runner.isolated_filesystem() as fs:
            home = Path(fs)
            (home / "target").mkdir()
            (home / "target" / "run_results.json").write_text(json.dumps({"results": [
                {"unique_id": "model.shop.fct_orders", "status": "error",
                 "message": "Database Error: relation raw.orders does not exist"}]}),
                encoding="utf-8")
            cfg = {
                "llm": {"provider": "scripted", "model": "scripted",
                        "api_key": "x", "base_url": "https://api.test.local"},
                "terminal": {"enabled": False},
            }
            (home / "config.yaml").write_text(yaml.safe_dump(cfg), encoding="utf-8")
            env = {**os.environ, "DACLI_HOME": str(home)}
            result = runner.invoke(
                cli, ["why-failed", "--source", "dbt", "-c", "config.yaml", "--json"],
                env=env)

        # A located+explained failure is a successful run of the explainer (0);
        # the JSON's `finding` is the signal CI/alerting branches on.
        self.assertEqual(result.exit_code, 0, result.output)
        payload = json.loads(result.output[result.output.index("{"):])
        self.assertEqual(payload["finding"]["failing_node"], "model.shop.fct_orders")
        self.assertEqual(payload["proposed_fix"]["tool_name"], "dbt_run")
        self.assertFalse(payload["proposed_fix"]["applied"])


class ProposedFixDataclassTest(unittest.TestCase):
    def test_to_dict_round_trips_the_governed_action(self):
        fix = ProposedFix(tool_name="dbt_run", args={"select": "m"}, rationale="re-run")
        d = fix.to_dict()
        self.assertEqual(d["tool_name"], "dbt_run")
        self.assertEqual(d["args"], {"select": "m"})
        self.assertFalse(d["applied"])


if __name__ == "__main__":
    unittest.main()
