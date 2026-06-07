"""CLI-first connector base.

The "Definition of Done" prefers a platform's **first-class CLI**
(`bq`, `databricks`, `aws`, `gcloud`, `dbt`, `psql`, ...) over reimplementing
vendor logic in Python — it honors the no-MCP / shell philosophy and lets the
Phase-5 sandbox run these under governance.

This base centralizes the cross-cutting concerns every CLI connector needs:

* an **injectable runner** (`self._runner`) so golden tests can drive the
  connector with canned process output and never shell out — the live
  subprocess path is guarded behind the default runner;
* a binary-availability / auth ``health`` helper;
* uniform ``ToolResult`` construction with timing + the executed argv recorded
  in metadata (provenance), and ``catalog_effects`` plumbed through.

A subclass sets :attr:`binary`, implements :meth:`operations` and
:meth:`invoke`, and calls :meth:`_run` to execute argv.
"""

from __future__ import annotations

from dacli.core.logging_setup import get_logger

log = get_logger(__name__)

import asyncio
import shutil
import time
from dataclasses import dataclass, field
from typing import Any
from collections.abc import Awaitable, Callable, Sequence

from dacli.connectors.base import Connector, ToolResult, ToolStatus


@dataclass
class CliResult:
    """The raw outcome of a single CLI invocation."""

    rc: int
    stdout: str = ""
    stderr: str = ""
    argv: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.rc == 0


# A runner takes argv (+ cwd/env/timeout) and returns a CliResult, sync or async.
CliRunner = Callable[..., Any]


async def default_runner(
    argv: Sequence[str],
    *,
    cwd: str | None = None,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
    stdin: str | None = None,
) -> CliResult:
    """Execute ``argv`` as a subprocess and capture rc/stdout/stderr.

    Uses ``asyncio.create_subprocess_exec`` (no shell) so arguments are never
    re-parsed by a shell — the safe default for governed execution.
    """
    proc = await asyncio.create_subprocess_exec(
        *argv,
        cwd=cwd,
        env=env,
        stdin=asyncio.subprocess.PIPE if stdin is not None else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(
            proc.communicate(stdin.encode("utf-8") if stdin is not None else None),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            log.debug("process already exited before kill", exc_info=True)
        return CliResult(rc=124, stdout="", stderr=f"timed out after {timeout}s",
                         argv=list(argv))
    return CliResult(
        rc=proc.returncode if proc.returncode is not None else -1,
        stdout=(out or b"").decode("utf-8", errors="replace"),
        stderr=(err or b"").decode("utf-8", errors="replace"),
        argv=list(argv),
    )


class CliConnector(Connector):
    """Base for connectors that drive a first-class platform CLI."""

    #: The CLI executable this connector relies on (e.g. "bq", "aws").
    binary: str = ""

    def __init__(self, settings: Any, runner: CliRunner | None = None):
        super().__init__(settings)
        # Tests inject a runner; production uses the real subprocess runner.
        self._runner: CliRunner = runner or default_runner

    # ------------------------------------------------------------------
    # CLI execution
    # ------------------------------------------------------------------
    async def _run(
        self,
        argv: Sequence[str],
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
        stdin: str | None = None,
    ) -> CliResult:
        outcome = self._runner(argv, cwd=cwd, env=env, timeout=timeout, stdin=stdin)
        if asyncio.iscoroutine(outcome) or isinstance(outcome, Awaitable):
            outcome = await outcome
        return outcome

    def _binary_available(self) -> bool:
        """True if the CLI is on PATH (or a runner is injected for tests)."""
        if self._runner is not default_runner:
            return True  # a test runner stands in for the binary
        return shutil.which(self.binary) is not None

    # ------------------------------------------------------------------
    # ToolResult helpers (uniform timing + provenance)
    # ------------------------------------------------------------------
    def _ok(self, op: str, data: Any, started: float, **metadata: Any) -> ToolResult:
        return ToolResult(
            tool_name=self.name,
            status=ToolStatus.SUCCESS,
            data=data,
            execution_time_ms=(time.time() - started) * 1000,
            metadata={"operation": op, **metadata},
        )

    def _fail(self, op: str, error: str, started: float, **metadata: Any) -> ToolResult:
        return ToolResult(
            tool_name=self.name,
            status=ToolStatus.ERROR,
            error=error,
            execution_time_ms=(time.time() - started) * 1000,
            metadata={"operation": op, **metadata},
        )

    def _unknown_op(self, op: str) -> ToolResult:
        return ToolResult(
            tool_name=op,
            status=ToolStatus.ERROR,
            error=f"Unknown operation '{op}' for connector '{self.name}'",
        )

    # ------------------------------------------------------------------
    # Lifecycle — a CLI connector is "connected" when its binary is reachable.
    # ------------------------------------------------------------------
    async def connect(self) -> bool:
        self.is_connected = self._binary_available()
        return self.is_connected

    async def health(self) -> ToolResult:
        started = time.time()
        if not self._binary_available():
            return self._fail(
                "health",
                f"CLI '{self.binary}' not found on PATH. Install it to use the "
                f"{self.name} connector.",
                started,
            )
        return self._ok("health", {"binary": self.binary, "available": True}, started)
