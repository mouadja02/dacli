"""``TerminalSession`` — a persistent, observable, governed shell session.

The substrate of Era 2: a long-lived shell subprocess (wrapped in a PTY when a
PTY library is present, a line-buffered pipe otherwise) whose every command is
captured faithfully — output **and** a reliable exit code — via the
sentinel-marker strategy. The session:

* lives across turns (created at session start, closed at session end);
* records a **provenance-tagged scrollback** (every line carries
  ``(session_id, command_id, timestamp)``) so the agent has full view of the
  terminal at any time (P2 renders it; the context source exposes it);
* **journals** each command + outcome to the workspace so the session can be
  resumed (P6);
* runs inside the session **workspace jail** (cwd cannot escape).

Execution is pluggable: a real run drives a :class:`ShellBackend` over a
:class:`Transport`; tests/eval inject a deterministic ``command_runner`` so the
golden suite is offline and repeatable. *This module does no governance* — that
is the dispatcher + Governor's job, exactly as for the tool and sandbox tiers.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from collections.abc import Callable

from dacli.core.logging_setup import get_logger
from dacli.core.timeutils import now_iso as _now_iso
from dacli.sandbox.shells.base import RawExec, ShellBackend, select_backend
from dacli.sandbox.shells.transports import Transport, make_transport
from dacli.sandbox.workspace import SessionWorkspace

log = get_logger(__name__)


# A test/eval seam: given a command (+ cwd, timeout) return its raw outcome.
CommandRunner = Callable[..., RawExec | tuple]


@dataclass
class ScrollbackLine:
    """One provenance-tagged line of terminal output."""

    session_id: str
    command_id: str
    timestamp: str
    stream: str          # "stdout" | "stderr" | "meta"
    text: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "command_id": self.command_id,
            "timestamp": self.timestamp,
            "stream": self.stream,
            "text": self.text,
        }


@dataclass
class CommandResult:
    """The outcome of one governed command, with provenance + timing."""

    command: str
    command_id: str
    session_id: str
    exit_code: int
    output: str
    cwd: str
    timed_out: bool = False
    started_at: str = field(default_factory=_now_iso)
    finished_at: str = field(default_factory=_now_iso)
    duration_ms: float = 0.0

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and not self.timed_out

    def to_dict(self) -> dict[str, Any]:
        return {
            "command": self.command,
            "command_id": self.command_id,
            "session_id": self.session_id,
            "exit_code": self.exit_code,
            "output": self.output,
            "cwd": self.cwd,
            "timed_out": self.timed_out,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": round(self.duration_ms, 3),
        }


class TerminalSession:
    """A persistent shell wrapped for faithful, journaled, jailed capture."""

    def __init__(
        self,
        session_id: str = "default",
        *,
        backend: ShellBackend | None = None,
        transport: Transport | None = None,
        command_runner: CommandRunner | None = None,
        workspace: SessionWorkspace | None = None,
        workspace_root: str = ".dacli/sessions",
        shell: str = "auto",
        wall_clock_seconds: float = 120.0,
        idle_timeout_ms: int = 400,
        journal: bool = True,
    ):
        self.session_id = session_id or "default"
        self.workspace = workspace or SessionWorkspace(self.session_id, workspace_root=workspace_root)
        # Real path collaborators (lazily started); ``command_runner`` short-
        # circuits both for deterministic offline use.
        self._backend = backend or select_backend(shell)
        self._transport = transport
        self._command_runner = command_runner
        self.wall_clock_seconds = float(wall_clock_seconds)
        self.idle_timeout_ms = int(idle_timeout_ms)
        self._journal_on = journal

        self._started = False
        self._cwd = str(self.workspace.root)
        self.scrollback: list[ScrollbackLine] = []
        self.commands: list[CommandResult] = []

    # ------------------------------------------------------------------
    # identity / lifecycle
    # ------------------------------------------------------------------
    @property
    def backend_name(self) -> str:
        return getattr(self._backend, "name", "?")

    @property
    def cwd(self) -> str:
        return self._cwd

    def start(self) -> None:
        """Spawn the shell (idempotent). No-op on the injected-runner path."""
        if self._started:
            return
        if self._command_runner is None:
            if self._transport is None:
                self._transport = make_transport()
            self._transport.start(self._backend.launch_argv(), cwd=str(self.workspace.root))
            self._record("meta", f"$ shell started: {self.backend_name} "
                                 f"({getattr(self._transport, 'kind', '?')}) in {self.workspace.root}")
        self._started = True

    def close(self) -> None:
        if self._transport is not None:
            self._transport.close()
        self._started = False

    # ------------------------------------------------------------------
    # the one execution entry point
    # ------------------------------------------------------------------
    def run(self, command: str, *, timeout: float | None = None) -> CommandResult:
        """Run one command in the persistent session and capture its outcome.

        This performs **no** governance — callers route through the dispatcher +
        Governor first. It only executes, tracks cwd within the jail, records the
        provenance-tagged scrollback and journals the result.
        """
        if not self._started:
            self.start()
        command = (command or "").strip()
        command_id = f"cmd_{datetime.now():%H%M%S}_{uuid.uuid4().hex[:6]}"
        started_at = _now_iso()
        t0 = time.time()
        deadline = t0 + (timeout if timeout is not None else self.wall_clock_seconds)

        raw = self._exec(command, deadline)

        duration_ms = (time.time() - t0) * 1000.0
        # Track cwd for the jail (best-effort parse of a leading `cd`).
        self._maybe_update_cwd(command)

        result = CommandResult(
            command=command,
            command_id=command_id,
            session_id=self.session_id,
            exit_code=raw.exit_code,
            output=raw.output,
            cwd=self._cwd,
            timed_out=raw.timed_out,
            started_at=started_at,
            finished_at=_now_iso(),
            duration_ms=duration_ms,
        )
        # Scrollback: one tagged line per output line (provenance per token).
        self._record("meta", f"$ {command}", command_id=command_id)
        for line in (raw.output.splitlines() or [""]):
            self._record("stdout", line, command_id=command_id)
        self._record(
            "meta",
            f"[exit {raw.exit_code}{' · timeout' if raw.timed_out else ''} · {duration_ms:.0f}ms]",
            command_id=command_id,
        )

        self.commands.append(result)
        self._journal(result)
        return result

    def interrupt(self) -> None:
        if self._transport is not None:
            self._transport.send_interrupt()

    # ------------------------------------------------------------------
    # scrollback access (the context source + fetch handle read from here)
    # ------------------------------------------------------------------
    def get_command(self, command_id: str) -> CommandResult | None:
        return next((c for c in self.commands if c.command_id == command_id), None)

    def scrollback_for(self, command_id: str) -> list[ScrollbackLine]:
        return [ln for ln in self.scrollback if ln.command_id == command_id]

    def tail(self, n: int = 50) -> list[ScrollbackLine]:
        return self.scrollback[-n:]

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------
    def _exec(self, command: str, deadline: float) -> RawExec:
        if self._command_runner is not None:
            return self._exec_via_runner(command, deadline)
        return self._exec_via_transport(command, deadline)

    def _exec_via_runner(self, command: str, deadline: float) -> RawExec:
        timeout = max(0.0, deadline - time.time())
        outcome = self._command_runner(command, cwd=self._cwd, timeout=timeout)
        if isinstance(outcome, RawExec):
            return outcome
        if isinstance(outcome, tuple):
            output, code = ([*list(outcome), 0])[:2]
            return RawExec(output=str(output), exit_code=int(code))
        # A bare string → assume success.
        return RawExec(output=str(outcome), exit_code=0)

    def _exec_via_transport(self, command: str, deadline: float) -> RawExec:
        assert self._transport is not None
        nonce = uuid.uuid4().hex[:10]
        self._transport.write(self._backend.format_command(command, nonce))

        idle = self.idle_timeout_ms / 1000.0
        collected: list[str] = []
        exit_code: int | None = None
        while time.time() < deadline:
            chunk = self._transport.read_available(timeout=idle)
            if not chunk:
                if not self._transport.is_alive():
                    break
                continue
            collected.append(chunk)
            # Look for the completion sentinel across everything seen so far.
            joined = "".join(collected)
            for line in joined.splitlines():
                rc = self._backend.is_sentinel_line(line, nonce)
                if rc is not None:
                    exit_code = rc
                    break
            if exit_code is not None:
                break

        timed_out = exit_code is None
        output = self._clean_output("".join(collected), command, nonce)
        return RawExec(
            output=output,
            exit_code=exit_code if exit_code is not None else 124,
            timed_out=timed_out,
        )

    def _clean_output(self, raw: str, command: str, nonce: str) -> str:
        """Drop echoed input + the sentinel line, keep genuine output."""
        kept: list[str] = []
        for line in raw.splitlines():
            if self._backend.is_sentinel_line(line, nonce) is not None:
                continue
            if self._backend.is_echo_of(line, command, nonce):
                continue
            kept.append(line.rstrip("\r"))
        # Trim leading/trailing blank noise from prompts.
        while kept and not kept[0].strip():
            kept.pop(0)
        while kept and not kept[-1].strip():
            kept.pop()
        return "\n".join(kept)

    def _maybe_update_cwd(self, command: str) -> None:
        # Advisory only: tracks a leading `cd` heuristically, so pushd /
        # subshells / `cd "$(…)"` are not reflected. The command classifier
        # gates obvious escapes; real isolation is the docker sandbox runtime
        # (see docs/GOVERNANCE.md, "The sandbox").
        parts = command.strip().split()
        if len(parts) >= 2 and parts[0] == "cd":
            target = command.strip()[2:].strip().strip('"').strip("'")
            try:
                new = self.workspace.resolve(target)
                self._cwd = str(new)
            except Exception:
                # An escape attempt: the classifier already flagged/blocked it;
                # we simply do not move the tracked cwd out of the jail.
                log.debug("cd target %r outside jail; tracked cwd unchanged", target, exc_info=True)

    def _record(self, stream: str, text: str, *, command_id: str = "") -> None:
        self.scrollback.append(ScrollbackLine(
            session_id=self.session_id, command_id=command_id,
            timestamp=_now_iso(), stream=stream, text=text,
        ))

    def _journal(self, result: CommandResult) -> None:
        if not self._journal_on:
            return
        try:
            import json
            path = Path(self.workspace.journal_dir) / "commands.jsonl"
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(result.to_dict(), default=str) + "\n")
        except Exception:
            log.debug("command journal write failed", exc_info=True)

    def load_journal(self) -> list[dict[str, Any]]:
        """Read the command journal back (P6 resume)."""
        import json
        path = Path(self.workspace.journal_dir) / "commands.jsonl"
        out: list[dict[str, Any]] = []
        if not path.exists():
            return out
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    out.append(json.loads(line))
        except Exception:
            return out
        return out
