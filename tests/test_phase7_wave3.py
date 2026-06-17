"""Wave 3 — orchestration connector golden tests (offline, transport injected).

Airflow (REST) and Dagster (GraphQL) are driven with a fake HTTP transport that
returns canned responses, so these tests run with no live orchestrator. They
verify the run-status post-conditions (a trigger/launch is "done" only when the
run reaches a successful terminal state) and the rollback posture (pause is
reversible; trigger/launch/delete have no native undo).

Run with:
    python -m unittest tests.test_phase7_wave3
"""

import asyncio
import types
import unittest

from dacli.connectors.base import Risk, ToolStatus
from dacli.connectors.http_base import HttpResult
from dacli.core.verify import VerificationContext, run_postconditions

from dacli.connectors.airflow.connector import AirflowConnector
from dacli.connectors.dagster.connector import DagsterConnector

from dacli.governance.rollback import RollbackStrategist
from dacli.governance.classifier import Classification, Tier


def _run(coro):
    return asyncio.run(coro)


def _settings(**sections):
    return types.SimpleNamespace(**sections)


def _transport(handler):
    async def t(method, path, *, params=None, json=None, headers=None):
        return handler(method, path, json)
    return t


# ===========================================================================
# Airflow
# ===========================================================================
class AirflowConnectorTest(unittest.TestCase):
    def _conn(self, handler):
        cfg = {"base_url": "http://af", "username": "u", "password": "p",
               "token": "", "poll_interval": 1, "timeout": 10}
        return AirflowConnector(_settings(connector_config={"airflow": cfg}), transport=_transport(handler))

    def test_trigger_waits_for_success(self):
        def handler(method, path, body):
            if method == "POST" and path.endswith("/dagRuns"):
                return HttpResult(200, {"dag_run_id": "r1", "state": "queued"})
            if method == "GET" and path.endswith("/dagRuns/r1"):
                return HttpResult(200, {"state": "success"})
            if path.endswith("/taskInstances"):
                return HttpResult(200, {"task_instances": [{"task_id": "t1", "state": "success"}]})
            return HttpResult(404, {})
        conn = self._conn(handler)
        res = _run(conn.invoke("trigger_airflow_dag", {"dag_id": "d"}))
        self.assertEqual(res.data["state"], "success")
        op = next(o for o in conn.operations() if o.name == "trigger_airflow_dag")
        ctx = VerificationContext(args={"dag_id": "d"}, result=res, target=conn)
        self.assertTrue(_run(run_postconditions(op.postconditions, ctx)).passed)

    def test_trigger_failure_is_caught(self):
        def handler(method, path, body):
            if method == "POST":
                return HttpResult(200, {"dag_run_id": "r1", "state": "queued"})
            if path.endswith("/dagRuns/r1"):
                return HttpResult(200, {"state": "failed"})
            return HttpResult(200, {"task_instances": []})
        conn = self._conn(handler)
        res = _run(conn.invoke("trigger_airflow_dag", {"dag_id": "d"}))
        op = next(o for o in conn.operations() if o.name == "trigger_airflow_dag")
        ctx = VerificationContext(args={"dag_id": "d"}, result=res, target=conn)
        self.assertFalse(_run(run_postconditions(op.postconditions, ctx)).passed)

    def test_pause_confirmed(self):
        def handler(method, path, body):
            if method == "PATCH":
                return HttpResult(200, {"is_paused": True})
            if method == "GET" and path == "/api/v1/dags/d":
                return HttpResult(200, {"is_paused": True})
            return HttpResult(404, {})
        conn = self._conn(handler)
        res = _run(conn.invoke("pause_airflow_dag", {"dag_id": "d"}))
        op = next(o for o in conn.operations() if o.name == "pause_airflow_dag")
        ctx = VerificationContext(args={"dag_id": "d"}, result=res, target=conn)
        self.assertTrue(_run(run_postconditions(op.postconditions, ctx)).passed)

    def test_delete_confirmed_absent(self):
        def handler(method, path, body):
            if method == "DELETE":
                return HttpResult(204, None)
            if method == "GET" and path == "/api/v1/dags/d":
                return HttpResult(404, {})
            return HttpResult(404, {})
        conn = self._conn(handler)
        res = _run(conn.invoke("delete_airflow_dag", {"dag_id": "d"}))
        op = next(o for o in conn.operations() if o.name == "delete_airflow_dag")
        ctx = VerificationContext(args={"dag_id": "d"}, result=res, target=conn)
        self.assertTrue(_run(run_postconditions(op.postconditions, ctx)).passed)

    def test_delete_is_irreversible(self):
        conn = self._conn(lambda m, p, b: HttpResult(200, {}))
        op = next(o for o in conn.operations() if o.name == "delete_airflow_dag")
        self.assertEqual(op.risk, Risk.IRREVERSIBLE)

    def test_verify_rollback_pause_vs_delete(self):
        conn = self._conn(lambda m, p, b: HttpResult(200, {}))
        ok, _ = _run(conn.verify_rollback(types.SimpleNamespace(primitive="airflow_unpause"), {}))
        self.assertTrue(ok)
        bad, _ = _run(conn.verify_rollback(types.SimpleNamespace(primitive="none"), {}))
        self.assertFalse(bad)


# ===========================================================================
# Dagster
# ===========================================================================
class DagsterConnectorTest(unittest.TestCase):
    def _conn(self, handler):
        cfg = {"base_url": "http://dg", "token": "", "poll_interval": 1, "timeout": 10}
        return DagsterConnector(_settings(connector_config={"dagster": cfg}), transport=_transport(handler))

    def test_launch_waits_for_success(self):
        def handler(method, path, body):
            q = (body or {}).get("query", "")
            if "launchPipelineExecution" in q:
                return HttpResult(200, {"data": {"launchPipelineExecution": {
                    "__typename": "LaunchRunSuccess", "run": {"runId": "r1", "status": "STARTED"}}}})
            if "runOrError" in q:
                return HttpResult(200, {"data": {"runOrError": {"status": "SUCCESS"}}})
            return HttpResult(200, {"data": {}})
        conn = self._conn(handler)
        res = _run(conn.invoke("launch_dagster_run",
                               {"job": "j", "repository_location": "loc", "repository": "repo"}))
        self.assertEqual(res.data["status"], "SUCCESS")
        op = next(o for o in conn.operations() if o.name == "launch_dagster_run")
        ctx = VerificationContext(args={}, result=res, target=conn)
        self.assertTrue(_run(run_postconditions(op.postconditions, ctx)).passed)

    def test_launch_failure_is_caught(self):
        def handler(method, path, body):
            q = (body or {}).get("query", "")
            if "launchPipelineExecution" in q:
                return HttpResult(200, {"data": {"launchPipelineExecution": {
                    "__typename": "LaunchRunSuccess", "run": {"runId": "r1", "status": "STARTED"}}}})
            if "runOrError" in q:
                return HttpResult(200, {"data": {"runOrError": {"status": "FAILURE"}}})
            return HttpResult(200, {"data": {}})
        conn = self._conn(handler)
        res = _run(conn.invoke("launch_dagster_run",
                               {"job": "j", "repository_location": "loc", "repository": "repo"}))
        op = next(o for o in conn.operations() if o.name == "launch_dagster_run")
        ctx = VerificationContext(args={}, result=res, target=conn)
        self.assertFalse(_run(run_postconditions(op.postconditions, ctx)).passed)

    def test_list_assets(self):
        def handler(method, path, body):
            return HttpResult(200, {"data": {"assetNodes": [
                {"assetKey": {"path": ["orders"]}}, {"assetKey": {"path": ["customers"]}}]}})
        conn = self._conn(handler)
        res = _run(conn.invoke("list_dagster_assets", {}))
        self.assertEqual(res.data["count"], 2)
        self.assertIn("orders", res.data["assets"])

    def test_graphql_errors_surface(self):
        def handler(method, path, body):
            return HttpResult(200, {"errors": [{"message": "boom"}]})
        conn = self._conn(handler)
        res = _run(conn.invoke("get_dagster_run", {"run_id": "r1"}))
        self.assertEqual(res.status, ToolStatus.ERROR)


# ===========================================================================
# Rollback parity — orchestration is honest about "no native undo"
# ===========================================================================
class Wave3RollbackParityTest(unittest.TestCase):
    def test_native_primitives(self):
        strat = RollbackStrategist()
        delete = Classification(tool_name="delete_airflow_dag", tier=Tier.IRREVERSIBLE,
                                declared_risk=Risk.IRREVERSIBLE)
        pause = Classification(tool_name="pause_airflow_dag", tier=Tier.RISKY,
                               declared_risk=Risk.RISKY)
        launch = Classification(tool_name="launch_dagster_run", tier=Tier.RISKY,
                                declared_risk=Risk.RISKY)
        self.assertEqual(strat.plan_for("airflow", delete).primitive, "none")
        self.assertEqual(strat.plan_for("airflow", pause).primitive, "airflow_unpause")
        self.assertEqual(strat.plan_for("dagster", launch).primitive, "none")


if __name__ == "__main__":
    unittest.main()
