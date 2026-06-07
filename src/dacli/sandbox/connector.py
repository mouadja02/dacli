"""Built-in 'sandbox' connector.

Surfaces the code-execution sandbox as a single operation so it flows through
the *one* dispatch path (and the Governor) like every other tool. The agent
routes a complex/multi-step/cross-platform task here (per the tier
router) and writes Python against the SDK; the runtime executes it in isolation
and returns only a bounded summary.

Like the ``system`` connector, this is injected by the agent (not discovered
from a manifest) because it needs runtime collaborators — the SDK's governed
``execute`` entry point, late-bound after the dispatcher exists.
"""

from __future__ import annotations

from typing import Any

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.core.verify import PostCondition, VerificationContext
import contextlib


def _run_completed() -> PostCondition:
    """The run returned a structured outcome (ok flag + bounded output)."""
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None)
        if not isinstance(data, dict) or "ok" not in data:
            return False, "sandbox run did not return a structured {ok, ...} result"
        if data.get("timed_out"):
            return False, "sandbox run hit the wall-clock limit"
        return True, ""
    return PostCondition(
        "sandbox_run_completed", check,
        "sandbox returned a structured outcome within limits", anchored=True,
    )


class SandboxConnector(Connector):

    name = "sandbox"

    def __init__(self, settings: Any = None):
        super().__init__(settings)
        self._runtime: Any = None        # late-bound SandboxRuntime
        self._enabled = True
        self._is_connected = True

    def bind_runtime(self, runtime: Any) -> None:
        """Late-bind the runtime (needs the dispatcher's governed execute)."""
        self._runtime = runtime

    def bind_result_store(self, store: Any) -> None:
        """Forward the session's spilled-result store to the runtime so sandbox
        code can ``sdk.fetch_result(handle)`` a large result back for processing."""
        if self._runtime is not None and hasattr(self._runtime, "bind_result_store"):
            self._runtime.bind_result_store(store)

    def close(self) -> None:
        """Release runtime resources (e.g. tear down a per-session container)."""
        if self._runtime is not None and hasattr(self._runtime, "close"):
            with contextlib.suppress(Exception):
                self._runtime.close()

    def operations(self) -> list[OperationSpec]:
        return [
            OperationSpec(
                name="run_sandbox_code",
                description=(
                    "Run Python in the governed code-execution sandbox for a "
                    "complex / multi-step / cross-platform job. Your code gets a "
                    "global `sdk`: call `sdk.run('<tool_name>', **args)` to invoke "
                    "any connector operation (each call is governed exactly like a "
                    "direct tool call — large results are written to disk and only "
                    "a bounded preview + handle are returned). Use "
                    "`sdk.save_rows(name, rows)` / `sdk.read_rows(name)` for "
                    "intermediate data, and `sdk.finish(summary)` (or set a "
                    "top-level `RESULT`) to return a small summary to me. Data "
                    "stays out of my context; credentials are never exposed to "
                    "your code."
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "code": {"type": "string", "description": "The Python script to run."},
                        "description": {"type": "string", "description": "One line on what the script does (for the audit log)."},
                    },
                    "required": ["code"],
                },
                capability="sandbox.execute",
                # The outer op auto-runs (write tier); the real blast-radius
                # gating happens on each governed sdk.run() inside the script.
                risk=Risk.WRITE,
                display_name="Run Sandbox Code",
                category="sandbox",
                postconditions=[_run_completed()],
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        if op != "run_sandbox_code":
            return ToolResult(tool_name=op, status=ToolStatus.ERROR,
                              error=f"Unknown operation '{op}' for connector '{self.name}'")
        if self._runtime is None:
            return ToolResult(tool_name=op, status=ToolStatus.ERROR,
                              error="Sandbox runtime is not available in this session.")
        code = args.get("code") or ""
        if not code.strip():
            return ToolResult(tool_name=op, status=ToolStatus.ERROR, error="code is required.")
        run = await self._runtime.run_script(code)
        status = ToolStatus.SUCCESS if run.ok else ToolStatus.ERROR
        return ToolResult(
            tool_name=op,
            status=status,
            data=run.to_dict(),
            error=run.error if not run.ok else None,
            metadata={"sandbox": True, "calls": run.calls, "workdir": run.workdir},
        )

    async def health(self) -> ToolResult:
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS,
                          data={"ready": self._runtime is not None})
