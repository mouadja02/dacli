"""Airflow connector (Wave 3) — orchestration over the stable REST API.

Turns dacli into a pipeline *operator*, not just a builder: list/trigger DAGs,
inspect runs and task instances, pause, and (hard-gated) delete. Governance:

* triggering a DAG run is **risky** (external side effects);
* pausing is **risky** but reversible (unpause is the rollback);
* deleting a DAG is **irreversible** — it drops run history with no native undo,
  so it is gated hard.

The platform is the oracle: a trigger is "done" only when the run reaches
``success`` (polled to a terminal state), and a pause/delete is confirmed by a
follow-up read.
"""

from __future__ import annotations

import asyncio
import base64
import time
from typing import Any

from dacli.config.settings import ConnectorConfig
from dacli.connectors.base import OperationSpec, Risk, ToolResult
from dacli.connectors.http_base import HttpConnector
from dacli.core.verify import PostCondition, VerificationContext, result_succeeded, data_has_keys

_TERMINAL = {"success", "failed"}


def airflow_run_succeeded() -> PostCondition:
    """A triggered DAG run reached terminal state ``success`` (not just queued)."""
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        state = data.get("state")
        if state is None:
            return True, "no run state reported (unverified)"
        if state != "success":
            return False, f"DAG run ended in state '{state}', not success"
        return True, ""
    return PostCondition("airflow_run_succeeded", check,
                         "triggered DAG run reached state success", anchored=True)


def airflow_dag_paused() -> PostCondition:
    """After a pause, a re-read confirms is_paused is true."""
    async def check(ctx: VerificationContext):
        target = ctx.target
        dag_id = ctx.args.get("dag_id")
        if target is None or not hasattr(target, "get_dag"):
            return True, "could not confirm pause (unverified)"
        dag = await target.get_dag(dag_id)
        if dag.get("is_paused") is not True:
            return False, f"DAG '{dag_id}' is not paused after pause"
        return True, ""
    return PostCondition("airflow_dag_paused", check,
                         "DAG reports is_paused after pause", anchored=True)


def airflow_dag_absent() -> PostCondition:
    """After a delete, a re-read returns not-found."""
    async def check(ctx: VerificationContext):
        target = ctx.target
        dag_id = ctx.args.get("dag_id")
        if target is None or not hasattr(target, "get_dag"):
            return True, "could not confirm deletion (unverified)"
        dag = await target.get_dag(dag_id)
        if dag.get("exists", True):
            return False, f"DAG '{dag_id}' still present after delete"
        return True, ""
    return PostCondition("airflow_dag_absent", check,
                         "DAG no longer present after delete", anchored=True)


class AirflowConnector(HttpConnector):
    name = "airflow"

    def _cfg(self) -> ConnectorConfig:
        return ConnectorConfig(self.settings, "airflow")

    def _base_url(self) -> str:
        return self._cfg().get("base_url", "") or ""

    def _timeout(self) -> int:
        return self._cfg().get("timeout", 600)

    def _default_headers(self) -> dict[str, str]:
        cfg = self._cfg()
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if cfg.get("token", ""):
            headers["Authorization"] = f"Bearer {cfg.get('token')}"
        elif cfg.get("username", ""):
            raw = f"{cfg.get('username')}:{cfg.get('password', '')}".encode()
            headers["Authorization"] = "Basic " + base64.b64encode(raw).decode("ascii")
        return headers

    def operations(self) -> list[OperationSpec]:
        dag = {"dag_id": {"type": "string", "description": "DAG id."}}
        return [
            OperationSpec(
                name="list_airflow_dags",
                description="List DAGs and their paused/active state (read-only).",
                parameters={"type": "object", "properties": {
                    "limit": {"type": "integer", "description": "Max DAGs (default 100)."}}},
                capability="airflow.introspection", risk=Risk.SAFE,
                display_name="List DAGs", category="introspection",
                postconditions=[data_has_keys("dags", name="lists_dags")],
            ),
            OperationSpec(
                name="get_airflow_dag_run",
                description="Get a DAG run's state (read-only).",
                parameters={"type": "object", "properties": {
                    **dag, "dag_run_id": {"type": "string"}}, "required": ["dag_id", "dag_run_id"]},
                capability="airflow.read", risk=Risk.SAFE,
                display_name="Get DAG Run", category="read",
                postconditions=[data_has_keys("state", name="reports_state")],
            ),
            OperationSpec(
                name="get_airflow_task_instances",
                description="List task instances (which tasks ran + their states) for a run.",
                parameters={"type": "object", "properties": {
                    **dag, "dag_run_id": {"type": "string"}}, "required": ["dag_id", "dag_run_id"]},
                capability="airflow.read", risk=Risk.SAFE,
                display_name="Get Task Instances", category="read",
                postconditions=[data_has_keys("task_instances", name="lists_tasks")],
            ),
            OperationSpec(
                name="trigger_airflow_dag",
                description="Trigger a DAG run and wait for it to finish. Risky — external side effects.",
                parameters={"type": "object", "properties": {
                    **dag, "conf": {"type": "object", "description": "Optional run configuration."}},
                    "required": ["dag_id"]},
                capability="airflow.trigger", risk=Risk.RISKY,
                display_name="Trigger DAG", category="operate",
                postconditions=[result_succeeded(), airflow_run_succeeded()],
            ),
            OperationSpec(
                name="pause_airflow_dag",
                description="Pause a DAG (stops scheduling). Reversible via unpause.",
                parameters={"type": "object", "properties": dag, "required": ["dag_id"]},
                capability="airflow.admin", risk=Risk.RISKY,
                display_name="Pause DAG", category="operate",
                postconditions=[result_succeeded(), airflow_dag_paused()],
            ),
            OperationSpec(
                name="delete_airflow_dag",
                description="Delete a DAG and its run history. Irreversible — gated hard.",
                parameters={"type": "object", "properties": dag, "required": ["dag_id"]},
                capability="airflow.admin", risk=Risk.IRREVERSIBLE,
                display_name="Delete DAG", category="operate",
                postconditions=[result_succeeded(), airflow_dag_absent()],
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        args = dict(args or {})
        if op == "list_airflow_dags":
            return await self._list_dags(args)
        if op == "get_airflow_dag_run":
            return await self._get_run(args)
        if op == "get_airflow_task_instances":
            return await self._task_instances(args)
        if op == "trigger_airflow_dag":
            return await self._trigger(args)
        if op == "pause_airflow_dag":
            return await self._pause(args)
        if op == "delete_airflow_dag":
            return await self._delete(args)
        return self._unknown_op(op)

    # ------------------------------------------------------------------
    async def get_dag(self, dag_id: str) -> dict[str, Any]:
        """Read one DAG's metadata (used by post-conditions)."""
        res = await self._request("GET", f"/api/v1/dags/{dag_id}")
        if res.status == 404:
            return {"exists": False, "dag_id": dag_id}
        if not res.ok:
            return {"exists": True, "dag_id": dag_id}
        data = res.data or {}
        return {"exists": True, "dag_id": dag_id, "is_paused": data.get("is_paused")}

    async def _list_dags(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        limit = int(args.get("limit") or 100)
        res = await self._request("GET", "/api/v1/dags", params={"limit": limit})
        if not res.ok:
            return self._fail("list_airflow_dags", f"HTTP {res.status}: {res.text[:500]}", started)
        dags = [{"dag_id": d.get("dag_id"), "is_paused": d.get("is_paused")}
                for d in (res.data or {}).get("dags", [])]
        return self._ok("list_airflow_dags", {"dags": dags, "count": len(dags)}, started)

    async def _get_run(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        dag_id, run_id = args.get("dag_id"), args.get("dag_run_id")
        res = await self._request("GET", f"/api/v1/dags/{dag_id}/dagRuns/{run_id}")
        if not res.ok:
            return self._fail("get_airflow_dag_run", f"HTTP {res.status}: {res.text[:500]}", started)
        d = res.data or {}
        return self._ok("get_airflow_dag_run",
                        {"dag_id": dag_id, "dag_run_id": run_id, "state": d.get("state")}, started)

    async def _task_instances(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        dag_id, run_id = args.get("dag_id"), args.get("dag_run_id")
        res = await self._request(
            "GET", f"/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        if not res.ok:
            return self._fail("get_airflow_task_instances", f"HTTP {res.status}: {res.text[:500]}", started)
        tis = [{"task_id": t.get("task_id"), "state": t.get("state")}
               for t in (res.data or {}).get("task_instances", [])]
        return self._ok("get_airflow_task_instances",
                        {"dag_id": dag_id, "dag_run_id": run_id, "task_instances": tis}, started)

    async def _trigger(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        dag_id = args.get("dag_id")
        body = {"conf": args.get("conf") or {}}
        res = await self._request("POST", f"/api/v1/dags/{dag_id}/dagRuns", json=body)
        if not res.ok:
            return self._fail("trigger_airflow_dag", f"HTTP {res.status}: {res.text[:500]}", started)
        run_id = (res.data or {}).get("dag_run_id")
        state = (res.data or {}).get("state")
        interval = self._cfg().get("poll_interval", 5)
        max_attempts = max(1, self._timeout() // max(interval, 1))
        for _attempt in range(max_attempts):
            if state in _TERMINAL:
                break
            self.emit_progress(
                f"run {run_id}: {state or 'queued'} — polling "
                f"({_attempt + 1}/{max_attempts})"
            )
            run = await self._request("GET", f"/api/v1/dags/{dag_id}/dagRuns/{run_id}")
            state = (run.data or {}).get("state") if run.ok else state
            if state in _TERMINAL:
                break
            await asyncio.sleep(interval)
        tis_res = await self._request(
            "GET", f"/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        task_instances = [{"task_id": t.get("task_id"), "state": t.get("state")}
                          for t in ((tis_res.data or {}).get("task_instances", []) if tis_res.ok else [])]
        data = {"dag_id": dag_id, "dag_run_id": run_id, "state": state,
                "task_instances": task_instances}
        return self._ok("trigger_airflow_dag", data, started)

    async def _pause(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        dag_id = args.get("dag_id")
        res = await self._request("PATCH", f"/api/v1/dags/{dag_id}", json={"is_paused": True})
        if not res.ok:
            return self._fail("pause_airflow_dag", f"HTTP {res.status}: {res.text[:500]}", started)
        return self._ok("pause_airflow_dag",
                        {"dag_id": dag_id, "is_paused": (res.data or {}).get("is_paused")}, started)

    async def _delete(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        dag_id = args.get("dag_id")
        res = await self._request("DELETE", f"/api/v1/dags/{dag_id}")
        if not res.ok and res.status != 404:
            return self._fail("delete_airflow_dag", f"HTTP {res.status}: {res.text[:500]}", started)
        return self._ok("delete_airflow_dag", {"dag_id": dag_id, "deleted": True}, started)

    # ------------------------------------------------------------------
    async def verify_rollback(self, plan, args: dict[str, Any]):
        if getattr(plan, "primitive", "") == "airflow_unpause":
            return True, "unpause restores scheduling"
        return False, ("deleting a DAG removes run history with no native undo "
                       "(re-deploy the DAG file from version control to restore "
                       "the definition only)")
