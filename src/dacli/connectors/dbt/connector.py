"""dbt connector (Wave 1) — CLI-first.

dbt is the center of gravity for analytics engineering: it makes the warehouses
*transform*, not just *query*. This connector promotes dbt from the ad-hoc
GitHub-Actions usage in the current prompt to a first-class, governed connector.

CLI-first: every operation shells out to the `dbt` CLI (no reimplementation of
dbt internals). The environment is the oracle for post-conditions — we parse
dbt's own `target/run_results.json` and `target/manifest.json` rather than trust
the model's read of stdout.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from dacli.config.settings import ConnectorConfig
from dacli.connectors.base import OperationSpec, Risk, ToolResult
from dacli.connectors.cli_base import CliConnector
from dacli.core.verify import PostCondition, VerificationContext, result_succeeded


# ---------------------------------------------------------------------------
# Post-conditions (environment-as-oracle: dbt's own artifacts)
# ---------------------------------------------------------------------------
def dbt_nodes_succeeded() -> PostCondition:
    """No node in run_results.json ended in error/fail.

    `dbt run` can exit cleanly per-invocation flags yet leave a failed model in
    its results; we read dbt's own artifact and reject any error/fail node."""
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        results = data.get("results")
        if not results:
            return True, "no run_results.json parsed (rc already gated)"
        bad = [r for r in results if str(r.get("status", "")).lower() in ("error", "fail", "runtime error")]
        if bad:
            names = ", ".join(str(r.get("node")) for r in bad[:5])
            return False, f"{len(bad)} node(s) failed: {names}"
        return True, ""
    return PostCondition(
        "dbt_nodes_succeeded", check,
        "every dbt node in run_results.json succeeded", anchored=True,
    )


def dbt_tests_passed() -> PostCondition:
    """Every test node passed (status 'pass')."""
    def applies(ctx: VerificationContext) -> bool:
        return (getattr(ctx.result, "data", None) or {}).get("command") in ("test", "build")

    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        results = data.get("results")
        if not results:
            return True, "no run_results.json parsed (rc already gated)"
        failed = [r for r in results if str(r.get("status", "")).lower() in ("fail", "error")]
        if failed:
            names = ", ".join(str(r.get("node")) for r in failed[:5])
            return False, f"{len(failed)} test(s) did not pass: {names}"
        return True, ""
    return PostCondition(
        "dbt_tests_passed", check, "every dbt test passed", anchored=True,
        applies_when=applies,
    )


def dbt_manifest_lists_nodes() -> PostCondition:
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if not data.get("exists"):
            return False, "no parsed dbt manifest (run `dbt compile` first)"
        if "node_count" not in data:
            return False, "introspection did not report node_count"
        return True, ""
    return PostCondition(
        "dbt_manifest_lists_nodes", check,
        "manifest.json parsed with a node inventory", anchored=True,
    )


class DbtConnector(CliConnector):
    """Drive a dbt project via the `dbt` CLI under tiered governance."""

    name = "dbt"
    binary = "dbt"

    def __init__(self, settings: Any, runner=None):
        super().__init__(settings, runner=runner)
        self.binary = ConnectorConfig(settings, "dbt").get("dbt_binary", "dbt") or "dbt"

    # ------------------------------------------------------------------
    def operations(self) -> list[OperationSpec]:
        select_param = {
            "type": "object",
            "properties": {
                "select": {"type": "string", "description": "Optional dbt node selector (e.g. 'my_model+', 'tag:nightly')."},
            },
        }
        return [
            OperationSpec(
                name="dbt_compile",
                description="Compile the dbt project (generates target/manifest.json). Read-only — no models are materialized.",
                parameters=select_param,
                capability="dbt.compile", risk=Risk.SAFE,
                display_name="dbt compile", category="build",
                postconditions=[result_succeeded()],
            ),
            OperationSpec(
                name="dbt_run",
                description="Run (materialize) dbt models into the target warehouse. Mutates target tables.",
                parameters=select_param,
                capability="dbt.run", risk=Risk.RISKY,
                display_name="dbt run", category="transform",
                postconditions=[result_succeeded(), dbt_nodes_succeeded()],
            ),
            OperationSpec(
                name="dbt_test",
                description="Run dbt data/schema tests. Executes test queries; does not materialize models.",
                parameters=select_param,
                capability="dbt.test", risk=Risk.SAFE,
                display_name="dbt test", category="test",
                postconditions=[result_succeeded(), dbt_tests_passed()],
            ),
            OperationSpec(
                name="dbt_build",
                description="Run + test models in DAG order (dbt build). Mutates target tables and validates them.",
                parameters=select_param,
                capability="dbt.build", risk=Risk.RISKY,
                display_name="dbt build", category="transform",
                postconditions=[result_succeeded(), dbt_nodes_succeeded(), dbt_tests_passed()],
            ),
            OperationSpec(
                name="introspect_dbt_manifest",
                description="Parse target/manifest.json for the model/source inventory and lineage. Feeds the catalog so memory can be re-verified.",
                parameters={"type": "object", "properties": {}},
                capability="dbt.introspection", risk=Risk.SAFE,
                display_name="Introspect dbt Manifest", category="introspection",
                postconditions=[dbt_manifest_lists_nodes()],
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        args = dict(args or {})
        if op == "introspect_dbt_manifest":
            return await self._introspect()
        command = {
            "dbt_compile": "compile",
            "dbt_run": "run",
            "dbt_test": "test",
            "dbt_build": "build",
        }.get(op)
        if command is None:
            return self._unknown_op(op)
        return await self._run_command(command, select=args.get("select"))

    # ------------------------------------------------------------------
    def _cfg(self) -> ConnectorConfig:
        return ConnectorConfig(self.settings, "dbt")

    def _project_dir(self) -> str:
        return self._cfg().get("project_dir", "") or "."

    def _common_flags(self) -> list[str]:
        cfg = self._cfg()
        flags: list[str] = []
        if cfg.get("project_dir", ""):
            flags += ["--project-dir", cfg.get("project_dir")]
        if cfg.get("profiles_dir", ""):
            flags += ["--profiles-dir", cfg.get("profiles_dir")]
        if cfg.get("target", ""):
            flags += ["--target", cfg.get("target")]
        return flags

    async def _run_command(self, command: str, select: str | None) -> ToolResult:
        started = time.time()
        argv = [self.binary, command, *self._common_flags()]
        if select:
            argv += ["--select", select]
        timeout = self._cfg().get("timeout", 900)
        res = await self._run(argv, cwd=self._project_dir(), timeout=timeout)
        results = self._read_run_results()
        data = {
            "command": command,
            "returncode": res.rc,
            "stdout": res.stdout,
            "stdout_tail": res.stdout[-2000:],
            "results": results,
        }
        if not res.ok:
            return self._fail(command, f"dbt {command} failed (rc={res.rc}): "
                                       f"{(res.stderr or res.stdout)[-1500:]}", started,
                              command=command, results=results)
        return self._ok(command, data, started, command=command,
                        catalog_effects=self._effects_from_results(results))

    async def _introspect(self) -> ToolResult:
        started = time.time()
        manifest = self._read_manifest()
        if manifest is None:
            return self._fail("introspect_dbt_manifest",
                              "target/manifest.json not found — run `dbt compile` first.",
                              started)
        nodes = manifest.get("nodes", {}) or {}
        models = [v.get("name") for k, v in nodes.items() if v.get("resource_type") == "model"]
        sources = [v.get("name") for k, v in (manifest.get("sources", {}) or {}).items()]
        effects = [{"action": "create", "object_type": "model",
                    "scope": {"object": m}, "source": "dbt.manifest"} for m in models if m]
        data = {
            "exists": True,
            "node_count": len(nodes),
            "models": models,
            "sources": sources,
        }
        return self._ok("introspect_dbt_manifest", data, started, catalog_effects=effects)

    # ------------------------------------------------------------------
    # dbt artifact readers (the environment-as-oracle inputs)
    # ------------------------------------------------------------------
    def _target_path(self, filename: str) -> Path:
        return Path(self._project_dir()) / "target" / filename

    def _read_run_results(self) -> list[dict[str, Any]] | None:
        try:
            raw = json.loads(self._target_path("run_results.json").read_text(encoding="utf-8"))
        except Exception:
            return None
        return [
            {
                "node": r.get("unique_id"),
                "status": r.get("status"),
                "message": r.get("message"),
            }
            for r in raw.get("results", []) or []
        ]

    def _read_manifest(self) -> dict[str, Any] | None:
        try:
            return json.loads(self._target_path("manifest.json").read_text(encoding="utf-8"))
        except Exception:
            return None

    @staticmethod
    def _effects_from_results(results: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
        # A materialized model invalidates any cached fact about that object.
        if not results:
            return []
        effects = []
        for r in results:
            node = r.get("node") or ""
            name = node.split(".")[-1] if node else None
            if name and str(r.get("status", "")).lower() == "success":
                effects.append({"action": "invalidate", "object_type": "table",
                                "scope": {"object": name}, "source": "dbt.run_results"})
        return effects
