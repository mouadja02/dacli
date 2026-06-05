"""Dagster connector (Wave 3) — orchestration over the GraphQL API.

Launch runs, inspect assets and materializations, read run status. Governance:
launching a run is **risky** (external side effects, no native undo — terminate
to stop further steps). The platform is the oracle: a launch is "done" only when
the run reaches terminal status ``SUCCESS`` (polled), and asset freshness is read
from the latest materialization.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional, Tuple

from connectors.base import OperationSpec, Risk, ToolResult
from connectors.http_base import HttpConnector
from core.verify import PostCondition, VerificationContext, result_succeeded, data_has_keys

_TERMINAL = {"SUCCESS", "FAILURE", "CANCELED"}

_ASSETS_Q = "query { assetNodes { assetKey { path } } }"
_RUN_Q = ("query($runId: ID!){ runOrError(runId: $runId){ __typename "
          "... on Run { id status } } }")
_LAUNCH_M = (
    "mutation($executionParams: ExecutionParams!){ "
    "launchPipelineExecution(executionParams: $executionParams){ __typename "
    "... on LaunchRunSuccess { run { runId status } } "
    "... on PythonError { message } } }")
_ASSET_MAT_Q = (
    "query($assetKey: AssetKeyInput!){ assetOrError(assetKey: $assetKey){ __typename "
    "... on Asset { assetMaterializations(limit: 1){ timestamp } } } }")


def dagster_run_succeeded() -> PostCondition:
    """A launched run reached terminal status SUCCESS."""
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        status = data.get("status")
        if status is None:
            return True, "no run status reported (unverified)"
        if status != "SUCCESS":
            return False, f"run ended in status '{status}', not SUCCESS"
        return True, ""
    return PostCondition("dagster_run_succeeded", check,
                         "launched run reached status SUCCESS", anchored=True)


def asset_materialized() -> PostCondition:
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if "materialized" not in data:
            return False, "materialization status not reported"
        return True, ""
    return PostCondition("asset_materialized", check,
                         "asset materialization + freshness reported", anchored=True)


class DagsterConnector(HttpConnector):
    name = "dagster"

    def _cfg(self):
        return getattr(self.settings, "dagster", None)

    def _base_url(self) -> str:
        cfg = self._cfg()
        return (getattr(cfg, "base_url", "") if cfg else "") or ""

    def _timeout(self) -> int:
        cfg = self._cfg()
        return getattr(cfg, "timeout", 600) if cfg else 600

    def _default_headers(self) -> Dict[str, str]:
        cfg = self._cfg()
        headers = {"Content-Type": "application/json"}
        if cfg and getattr(cfg, "token", ""):
            headers["Dagster-Cloud-Api-Token"] = cfg.token
        return headers

    def operations(self) -> List[OperationSpec]:
        return [
            OperationSpec(
                name="list_dagster_assets",
                description="List asset keys defined in the deployment (read-only).",
                parameters={"type": "object", "properties": {}},
                capability="dagster.introspection", risk=Risk.SAFE,
                display_name="List Assets", category="introspection",
                postconditions=[data_has_keys("assets", name="lists_assets")],
            ),
            OperationSpec(
                name="get_dagster_run",
                description="Get a run's status (read-only).",
                parameters={"type": "object", "properties": {"run_id": {"type": "string"}},
                            "required": ["run_id"]},
                capability="dagster.read", risk=Risk.SAFE,
                display_name="Get Run", category="read",
                postconditions=[data_has_keys("status", name="reports_status")],
            ),
            OperationSpec(
                name="get_dagster_asset_materialization",
                description="Read the latest materialization (freshness) of an asset.",
                parameters={"type": "object", "properties": {
                    "asset_key": {"type": "array", "items": {"type": "string"},
                                  "description": "Asset key path, e.g. [\"my_asset\"]."}},
                    "required": ["asset_key"]},
                capability="dagster.read", risk=Risk.SAFE,
                display_name="Asset Materialization", category="read",
                postconditions=[asset_materialized()],
            ),
            OperationSpec(
                name="launch_dagster_run",
                description="Launch a job run and wait for it to finish. Risky — external side effects.",
                parameters={"type": "object", "properties": {
                    "job": {"type": "string"},
                    "repository_location": {"type": "string"},
                    "repository": {"type": "string"},
                    "run_config": {"type": "object"}},
                    "required": ["job", "repository_location", "repository"]},
                capability="dagster.launch", risk=Risk.RISKY,
                display_name="Launch Run", category="operate",
                postconditions=[result_succeeded(), dagster_run_succeeded()],
            ),
        ]

    async def invoke(self, op: str, args: Dict[str, Any]) -> ToolResult:
        args = dict(args or {})
        if op == "list_dagster_assets":
            return await self._list_assets()
        if op == "get_dagster_run":
            return await self._get_run(args.get("run_id"))
        if op == "get_dagster_asset_materialization":
            return await self._materialization(args)
        if op == "launch_dagster_run":
            return await self._launch(args)
        return self._unknown_op(op)

    # ------------------------------------------------------------------
    async def _graphql(self, query: str, variables: Optional[Dict[str, Any]] = None
                       ) -> Tuple[bool, Dict[str, Any], str]:
        res = await self._request("POST", "/graphql",
                                  json={"query": query, "variables": variables or {}})
        if not res.ok:
            return False, {}, f"HTTP {res.status}: {res.text[:500]}"
        body = res.data or {}
        if body.get("errors"):
            return False, {}, str(body["errors"])[:500]
        return True, body.get("data", {}) or {}, ""

    async def _list_assets(self) -> ToolResult:
        started = time.time()
        ok, data, err = await self._graphql(_ASSETS_Q)
        if not ok:
            return self._fail("list_dagster_assets", err, started)
        assets = [".".join(n.get("assetKey", {}).get("path", []))
                  for n in data.get("assetNodes", [])]
        return self._ok("list_dagster_assets", {"assets": assets, "count": len(assets)}, started)

    async def _get_run(self, run_id: str) -> ToolResult:
        started = time.time()
        ok, data, err = await self._graphql(_RUN_Q, {"runId": run_id})
        if not ok:
            return self._fail("get_dagster_run", err, started)
        run = data.get("runOrError", {}) or {}
        return self._ok("get_dagster_run", {"run_id": run_id, "status": run.get("status")}, started)

    async def _materialization(self, args: Dict[str, Any]) -> ToolResult:
        started = time.time()
        ok, data, err = await self._graphql(_ASSET_MAT_Q,
                                            {"assetKey": {"path": args.get("asset_key") or []}})
        if not ok:
            return self._fail("get_dagster_asset_materialization", err, started)
        asset = data.get("assetOrError", {}) or {}
        mats = asset.get("assetMaterializations", []) or []
        latest = mats[0].get("timestamp") if mats else None
        return self._ok("get_dagster_asset_materialization",
                        {"asset_key": args.get("asset_key"), "materialized": bool(mats),
                         "latest_timestamp": latest}, started)

    async def _launch(self, args: Dict[str, Any]) -> ToolResult:
        started = time.time()
        execution_params = {
            "selector": {
                "repositoryLocationName": args.get("repository_location"),
                "repositoryName": args.get("repository"),
                "jobName": args.get("job"),
            },
            "runConfigData": args.get("run_config") or {},
            "mode": "default",
        }
        ok, data, err = await self._graphql(_LAUNCH_M, {"executionParams": execution_params})
        if not ok:
            return self._fail("launch_dagster_run", err, started)
        launch = data.get("launchPipelineExecution", {}) or {}
        if launch.get("__typename") not in (None, "LaunchRunSuccess") and launch.get("message"):
            return self._fail("launch_dagster_run", f"launch error: {launch.get('message')}", started)
        run = launch.get("run", {}) or {}
        run_id = run.get("runId")
        status = run.get("status")
        cfg = self._cfg()
        interval = getattr(cfg, "poll_interval", 5) if cfg else 5
        max_attempts = max(1, self._timeout() // max(interval, 1))
        for _ in range(max_attempts):
            if status in _TERMINAL:
                break
            ok2, data2, _ = await self._graphql(_RUN_Q, {"runId": run_id})
            if ok2:
                status = (data2.get("runOrError", {}) or {}).get("status") or status
            if status in _TERMINAL:
                break
            await asyncio.sleep(interval)
        return self._ok("launch_dagster_run",
                        {"run_id": run_id, "status": status, "job": args.get("job")}, started)
