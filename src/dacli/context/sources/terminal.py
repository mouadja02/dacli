"""Scrollback as a first-class, provenance-tagged context source (𝒞).

"The agent has full view of the terminal at any time" — but *exposure is not
access*. A 10k-line command output must not enter the model's context verbatim
(that is the exposure-without-access bottleneck the harness-scaling paper names);
it would dilute signal and blow the budget. So this source does two things:

1. **Spill + summarise** — the full output of every command is written to the
   session workspace keyed by its ``command_id`` (with provenance); the model
   sees a budgeted summary plus a fetch handle.
2. **JIT fetch** — the agent answers "what did step N output?" by calling
   ``fetch_scrollback(command_id=…)``, which reads the full (or a slice of the)
   output back from disk — never the raw dump in context.

Every line the store keeps carries ``(session_id, command_id, timestamp)`` so
the context assembler can place a budgeted, traceable terminal digest, and the
audit trail can reconstruct exactly what the terminal showed.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from dacli.core.atomicio import write_bytes_atomic
from dacli.core.fastjson import dumps_bytes as _json_dumps_bytes
from dacli.core.timeutils import now_iso as _now_iso
import contextlib

# How many head/tail lines the summary shows when output is spilled.
SAMPLE_LINES = 8


class ScrollbackStore:
    """On-disk store of full command outputs, keyed by ``command_id``.

    Written when a shell command completes; read by the ``fetch_scrollback``
    system op. Lives in the session workspace so it is journaled/resumable (P6)
    and inspectable in the TUI (P2).
    """

    def __init__(self, root: str = ".dacli/sessions", session_id: str = "default"):
        self.session_id = session_id or "default"
        self.dir = Path(root) / self.session_id / "scrollback"
        self.dir.mkdir(parents=True, exist_ok=True)
        self._order: list[str] = []

    def _path(self, command_id: str) -> Path:
        safe = "".join(c for c in command_id if c.isalnum() or c in ("_", "-"))
        return self.dir / f"{safe}.json"

    def write(self, result: Any) -> str:
        """Persist a command's full output + provenance; return its command_id."""
        data = result.to_dict() if hasattr(result, "to_dict") else dict(result)
        command_id = data.get("command_id") or f"cmd_{datetime.now():%H%M%S}"
        output = data.get("output", "")
        payload = {
            "command_id": command_id,
            "session_id": data.get("session_id", self.session_id),
            "command": data.get("command", ""),
            "exit_code": data.get("exit_code"),
            "cwd": data.get("cwd", ""),
            "timestamp": data.get("finished_at") or _now_iso(),
            "line_count": len(output.splitlines()),
            "output": output,
        }
        with contextlib.suppress(Exception):
            # orjson serializes the (potentially large) command output; bytes are
            # written crash-safely via the atomic writer (P03).
            write_bytes_atomic(self._path(command_id), _json_dumps_bytes(payload, default=str))
        if command_id not in self._order:
            self._order.append(command_id)
        return command_id

    def read(self, command_id: str, start: int = 0, count: int | None = None) -> dict[str, Any]:
        """Read full (or a line-window of a) command's output back from disk."""
        path = self._path(command_id)
        if not path.exists():
            return {"error": f"Unknown scrollback handle '{command_id}'."}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            return {"error": f"could not read scrollback '{command_id}': {e}"}
        lines = (payload.get("output") or "").splitlines()
        total = len(lines)
        end = total if count is None else min(start + count, total)
        window = lines[start:end]
        return {
            "command_id": command_id,
            "command": payload.get("command"),
            "exit_code": payload.get("exit_code"),
            "total_lines": total,
            "start": start,
            "returned": len(window),
            "output": "\n".join(window),
        }

    def recent(self, n: int = 5) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for p in sorted(self.dir.glob("*.json"), key=lambda x: x.stat().st_mtime)[-n:]:
            try:
                records.append(json.loads(p.read_text(encoding="utf-8")))
            except Exception:
                continue
        return records


def bound_output(output: str, max_chars: int) -> dict[str, Any]:
    """Return a model-facing view of a command's output, summarised if large.

    Small output is returned verbatim; large output is replaced by a head/tail
    sample + a count and a note that the full text is fetchable by command_id.
    The full text is preserved on disk by the store (the human/TUI still sees
    everything) — only the model's context copy is bounded.
    """
    output = output or ""
    if len(output) <= max_chars:
        return {"text": output, "spilled": False}
    lines = output.splitlines()
    head = lines[:SAMPLE_LINES]
    tail = lines[-SAMPLE_LINES:] if len(lines) > SAMPLE_LINES else []
    sample = "\n".join(head)
    if tail:
        sample += f"\n… [{len(lines) - 2 * SAMPLE_LINES} more lines] …\n" + "\n".join(tail)
    return {"text": sample, "spilled": True, "total_lines": len(lines)}


class ScrollbackSource:
    """The context layer over a live :class:`TerminalSession` + its store."""

    def __init__(self, session: Any = None, store: ScrollbackStore | None = None):
        self._session = session
        self._store = store

    def get(self, command_id: str, start: int = 0, count: int | None = None) -> dict[str, Any]:
        """JIT fetch: the full (or a slice of a) command's output by id."""
        if self._store is not None:
            result = self._store.read(command_id, start=start, count=count)
            if "error" not in result:
                return result
        # Fall back to the live session's in-memory record.
        if self._session is not None:
            cmd = self._session.get_command(command_id)
            if cmd is not None:
                lines = cmd.output.splitlines()
                end = len(lines) if count is None else min(start + count, len(lines))
                return {
                    "command_id": command_id, "command": cmd.command,
                    "exit_code": cmd.exit_code, "total_lines": len(lines),
                    "start": start, "returned": len(lines[start:end]),
                    "output": "\n".join(lines[start:end]),
                }
        return {"error": f"Unknown scrollback handle '{command_id}'."}

    def summary_lines(self, limit_commands: int = 5) -> list[str]:
        """Provenance-tagged, compact digest of recent terminal activity.

        One line per recent command — enough for the model to know *what
        happened* and *which handle to fetch* — never the raw output.
        """
        out: list[str] = []
        commands = []
        if self._session is not None and getattr(self._session, "commands", None):
            commands = self._session.commands[-limit_commands:]
        elif self._store is not None:
            commands = self._store.recent(limit_commands)

        for c in commands:
            cid = getattr(c, "command_id", None) or (c.get("command_id") if isinstance(c, dict) else None)
            command = getattr(c, "command", None) or (c.get("command") if isinstance(c, dict) else "")
            rc = getattr(c, "exit_code", None)
            if rc is None and isinstance(c, dict):
                rc = c.get("exit_code")
            output = getattr(c, "output", None)
            if output is None and isinstance(c, dict):
                output = c.get("output", "")
            n_lines = len((output or "").splitlines())
            out.append(
                f"[{cid}] $ {command}  → exit {rc} "
                f"({n_lines} line{'s' if n_lines != 1 else ''}; "
                f"fetch_scrollback(command_id=\"{cid}\") for full output)"
            )
        return out
