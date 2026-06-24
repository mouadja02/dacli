"""Shell seed — run a command in the persistent, governed terminal session.

A reference ``register(api)`` extension (reporting/02 seed set), replacing the
old ``connectors/shell`` Connector. The one tool, ``run_shell_command``, does
**no** governance of its own: governed dispatch routes every call through the
Governor first, which blast-radius-classifies the *command string* (an ``ls`` is
safe even though this tool is write-capable; an ``rm -rf`` is blocked unless a
rollback verifies). The free-text terminal is not an exec bypass.

The live :class:`~sandbox.terminal.TerminalSession` is owned by the host, not the
seed — read through :mod:`core.runtime` (the late-bind the old connector did with
``bind_session``).
"""

from __future__ import annotations

import os
import time
from typing import Any

from dacli.connectors.base import ToolResult, ToolStatus
from dacli.core import runtime


def register(api):
    @api.tool(
        name="run_shell_command",
        description=(
            "Run a command in the persistent, governed terminal session (the shell "
            "tier). Use for local file/glue work and platform CLIs with no typed tool. "
            "The command is blast-radius-classified before it runs: reads auto-run; "
            "writes/overwrites and destructive commands require approval and a verified "
            "rollback, and irreversible ones with no undo are blocked. cwd stays inside "
            "the session workspace jail. Large output is spilled — fetch the full text "
            "with fetch_scrollback(command_id)."
        ),
        parameters={
            "command": {"type": "string", "description": "The shell command line to execute."},
            "timeout": {"type": "number", "description": "Optional per-command wall-clock limit in seconds."},
        },
        risk="write",
        # Anchored to the environment: exit 0 + the file the command meant to
        # write/delete actually (dis)appeared. The exit-code check always applies,
        # so "no post-condition, no register" is satisfied.
        postconditions=["shell_exit_zero", "shell_writes_observed", "shell_deletes_observed"],
        display_name="Run Shell Command",
        category="shell",
    )
    async def run_shell_command(args, ctx):
        command = (args.get("command") or args.get("cmd") or "").strip()
        if not command:
            return _fail("command is required.")
        term = runtime.terminal()
        session = getattr(term, "session", None)
        if session is None:
            return _fail("no terminal session is available in this run.")

        settings = getattr(term, "settings", None)
        network = getattr(settings, "network", "allowlist")
        allowlist = list(getattr(settings, "egress_allowlist", []) or [])
        max_output = int(getattr(settings, "max_output_chars", 20000))
        timeout = args.get("timeout")
        timeout = float(timeout) if timeout is not None else None

        # Re-derive the write/delete signals (pure parse) so the shell post-
        # conditions can re-observe the filesystem. Not a second governance pass —
        # the Governor already classified and gated the command.
        from dacli.governance.command_classifier import classify_command
        verdict = classify_command(command, network=network, egress_allowlist=allowlist)

        # Copy aside what the command is about to clobber/delete, before it runs —
        # the shell rollback plan's "versioned_copy_aside" made real.
        backups = _copy_aside(session, verdict.overwrites + verdict.deletes)

        t0 = time.time()
        result = session.run(command, timeout=timeout)
        elapsed_ms = (time.time() - t0) * 1000.0

        store = getattr(term, "store", None)
        handle = result.command_id
        if store is not None:
            try:
                handle = store.write(result)
            except Exception:
                handle = result.command_id

        from dacli.context.sources.terminal import bound_output
        bounded = bound_output(result.output, max_output)

        data: dict[str, Any] = {
            "command": command,
            "command_id": result.command_id,
            "scrollback_handle": handle,
            "exit_code": result.exit_code,
            "timed_out": result.timed_out,
            "cwd": result.cwd,
            "output": bounded["text"],
            "output_lines": len(result.output.splitlines()),
            "spilled": bounded["spilled"],
            "writes": list(verdict.writes),
            "overwrites": list(verdict.overwrites),
            "deletes": list(verdict.deletes),
            "egress_hosts": list(verdict.egress_hosts),
            "backups": backups,
        }
        return ToolResult(
            tool_name="run_shell_command", status=ToolStatus.SUCCESS, data=data,
            execution_time_ms=elapsed_ms,
            metadata={
                "scrollback_handle": handle, "spilled": bounded["spilled"],
                "backend": getattr(session, "backend_name", None),
                "tier": "shell", "backups": backups,
            },
        )


def _fail(message: str) -> ToolResult:
    return ToolResult(tool_name="run_shell_command", status=ToolStatus.ERROR, error=message)


def _copy_aside(session: Any, targets: list[str]) -> list[dict[str, str]]:
    ws = getattr(session, "workspace", None)
    if ws is None:
        return []
    cwd = getattr(session, "cwd", None) or "."
    backups: list[dict[str, str]] = []
    for tgt in targets:
        path = tgt if os.path.isabs(tgt) else os.path.join(cwd, tgt)
        if not ws.is_within_jail(path) or not os.path.exists(path):
            continue
        try:
            dest = ws.backup(path)
        except Exception:
            dest = None
        if dest is not None:
            backups.append({"target": tgt, "backup": str(dest)})
    return backups
