"""``ShellConnector`` — the governed shell tier as a connector.

One operation, ``run_shell_command``, runs a free-text command in the persistent
:class:`~sandbox.terminal.TerminalSession`. The connector itself does **no**
governance — the dispatcher routes every call through the shared Governor first,
which blast-radius-classifies the *command string* (an ``ls`` is safe even though
this op is write-capable; an ``rm -rf`` is irreversible and blocked unless a
rollback can be verified). That keeps the free-text terminal from becoming the
ungoverned-execution hole Era 1 closed.

Two post-condition-relevant things happen on the way out:

1. the command's full output is **spilled** to the scrollback store keyed by its
   ``command_id`` and the model sees only a bounded view + a fetch handle
   (a 10k-line output never enters context); and
2. the verdict's write/delete *signals* are attached to the result ``data`` so
   the shell post-conditions (:func:`core.verify.shell_writes_observed` etc.) can
   re-observe the live filesystem — fluent output is not proof the file landed.
"""

from __future__ import annotations

import os
import time
from typing import Any

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.core.verify import shell_deletes_observed, shell_exit_zero, shell_writes_observed


class ShellConnector(Connector):

    name = "shell"

    def __init__(
        self,
        settings: Any = None,
        *,
        session: Any = None,
        scrollback_store: Any = None,
    ):
        super().__init__(settings)
        self._session = session
        self._store = scrollback_store
        term = getattr(settings, "terminal", None)
        self._network = getattr(term, "network", "allowlist")
        self._allowlist = list(getattr(term, "egress_allowlist", []) or [])
        self._max_output_chars = int(getattr(term, "max_output_chars", 20000))
        self._wall_clock = float(getattr(term, "wall_clock_seconds", 120))
        # Always ready; the session is lazily started on first command.
        self._is_connected = True

    def bind_session(self, session: Any, scrollback_store: Any = None) -> None:
        """Late-bind the live terminal session + store (agent wires this)."""
        self._session = session
        if scrollback_store is not None:
            self._store = scrollback_store

    # ------------------------------------------------------------------
    # Connector contract
    # ------------------------------------------------------------------
    def operations(self) -> list[OperationSpec]:
        return [
            OperationSpec(
                name="run_shell_command",
                description=(
                    "Run a command in the persistent, governed terminal session "
                    "(the shell tier). Use for local file/glue work and running a "
                    "platform CLI that has no typed connector op. The command is "
                    "blast-radius-classified before it runs: reads auto-run; "
                    "writes/overwrites and destructive commands require approval "
                    "and a verified rollback, and irreversible ones with no undo "
                    "are blocked. cwd stays inside the session workspace jail. "
                    "Large output is spilled — fetch the full text with "
                    "fetch_scrollback(command_id)."
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "string",
                            "description": "The shell command line to execute (e.g. 'ls -la', 'mkdir reports', 'git status').",
                        },
                        "timeout": {
                            "type": "number",
                            "description": "Optional per-command wall-clock limit in seconds (defaults to the configured ceiling).",
                        },
                    },
                    "required": ["command"],
                },
                capability="shell.run",
                risk=Risk.WRITE,
                display_name="Run Shell Command",
                category="shell",
                # Anchored to the environment: exit 0 + the file the command meant
                # to write/delete actually (dis)appeared. At least one always
                # applies (exit code), satisfying "no post-condition, no register".
                postconditions=[
                    shell_exit_zero(),
                    shell_writes_observed(),
                    shell_deletes_observed(),
                ],
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        if op == "run_shell_command":
            return self._run(args)
        return ToolResult(
            tool_name=op, status=ToolStatus.ERROR,
            error=f"Unknown operation '{op}' for connector '{self.name}'",
        )

    async def health(self) -> ToolResult:
        ready = self._session is not None
        return ToolResult(
            tool_name=self.name,
            status=ToolStatus.SUCCESS if ready else ToolStatus.ERROR,
            data={"ready": ready, "backend": getattr(self._session, "backend_name", None)},
            error=None if ready else "no terminal session bound",
        )

    # ------------------------------------------------------------------
    # the one operation
    # ------------------------------------------------------------------
    def _run(self, args: dict[str, Any]) -> ToolResult:
        command = (args.get("command") or args.get("cmd") or "").strip()
        if not command:
            return ToolResult(tool_name="run_shell_command", status=ToolStatus.ERROR,
                              error="command is required.")
        if self._session is None:
            return ToolResult(tool_name="run_shell_command", status=ToolStatus.ERROR,
                              error="no terminal session is available in this run.")

        timeout = args.get("timeout")
        timeout = float(timeout) if timeout is not None else None

        # Re-derive the write/delete signals (pure parse) so the post-conditions
        # can re-observe the filesystem. This is signal extraction, not a second
        # governance pass — the Governor already classified + gated the command.
        from dacli.governance.command_classifier import classify_command
        verdict = classify_command(command, network=self._network, egress_allowlist=self._allowlist)

        # Copy-aside the files this command is about to clobber/delete *before* it
        # runs — the shell rollback plan's "versioned_copy_aside" made real. (An
        # irreversible `rm -rf` never reaches here; the Governor blocked it.)
        backups = self._copy_aside(verdict.overwrites + verdict.deletes)

        t0 = time.time()
        result = self._session.run(command, timeout=timeout)
        elapsed_ms = (time.time() - t0) * 1000.0

        # Spill the full output to the scrollback store; the model sees a bounded
        # view + the fetch handle, never the raw 10k-line dump.
        handle = result.command_id
        if self._store is not None:
            try:
                handle = self._store.write(result)
            except Exception:
                handle = result.command_id

        from dacli.context.sources.terminal import bound_output
        bounded = bound_output(result.output, self._max_output_chars)

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
            tool_name="run_shell_command",
            status=ToolStatus.SUCCESS,
            data=data,
            execution_time_ms=elapsed_ms,
            metadata={
                "scrollback_handle": handle,
                "spilled": bounded["spilled"],
                "backend": getattr(self._session, "backend_name", None),
                "tier": "shell",
                "backups": backups,
            },
        )

    # ------------------------------------------------------------------
    # rollback support — the copy-aside is taken here (the connector does it),
    # while ``verify_rollback`` (below) proves to the Governor it is feasible.
    # ------------------------------------------------------------------
    def _workspace(self) -> Any:
        return getattr(self._session, "workspace", None)

    def _resolve_target(self, target: str) -> str | None:
        cwd = getattr(self._session, "cwd", None) or "."
        path = target if os.path.isabs(target) else os.path.join(cwd, target)
        ws = self._workspace()
        if ws is not None and not ws.is_within_jail(path):
            return None  # outside the jail — never touch it
        return path

    def _copy_aside(self, targets: list[str]) -> list[dict[str, str]]:
        ws = self._workspace()
        if ws is None:
            return []
        backups: list[dict[str, str]] = []
        for tgt in targets:
            path = self._resolve_target(tgt)
            if not path or not os.path.exists(path):
                continue  # nothing to back up (new file → delete to undo)
            try:
                dest = ws.backup(path)
            except Exception:
                dest = None
            if dest is not None:
                backups.append({"target": tgt, "backup": str(dest)})
        return backups

    def verify_rollback(self, plan: Any, args: dict[str, Any]) -> tuple[bool, str]:
        """Prove to the Governor that this command's undo path actually exists.

        Called only when policy ``requires_verified_rollback``. Feasibility, not
        execution: the actual copy-aside is taken in ``_run`` just before the
        mutation. An honestly irreversible plan (``none``) stays unverified, so
        the Governor refuses it.
        """
        primitive = getattr(plan, "primitive", "")
        if primitive in ("none", ""):
            return False, "no native undo primitive for this command"
        if primitive == "delete_created_artifact":
            return True, "new artifact is recoverable by deletion"
        if primitive == "git_revert_or_stash":
            cwd = getattr(self._session, "cwd", None) or "."
            has_git = os.path.isdir(os.path.join(cwd, ".git"))
            return (has_git, "git repo present (revert/stash available)" if has_git
                    else "not a git repo — cannot guarantee a git-native undo")
        if primitive == "versioned_copy_aside":
            command = (args.get("command") or "").strip()
            from dacli.governance.command_classifier import classify_command
            v = classify_command(command, network=self._network, egress_allowlist=self._allowlist)
            ws = self._workspace()
            for tgt in (v.overwrites + v.deletes):
                path = self._resolve_target(tgt)
                if path is None:
                    return False, f"target '{tgt}' escapes the workspace jail"
                if ws is not None and not os.access(os.path.dirname(path) or ".", os.W_OK):
                    return False, f"backups dir not writable for '{tgt}'"
            return True, "copy-aside into the session backups/ is feasible"
        return True, f"'{primitive}' undo path assumed feasible"
