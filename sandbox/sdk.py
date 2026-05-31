"""The sandbox connector SDK (Phase 5.6) — capability-gated, secret-free.

This is the API the agent's sandbox code calls. Three properties make it the
"complex op" half of the hybrid model safe:

* **Governed, not trusted.** Every ``run`` goes through the *same* dispatcher +
  Governor as the tool tier, so a ``DROP`` issued from sandbox code is
  classified, policy-checked and (for irreversible) blocked exactly as it would
  be from a tool call. The sandbox is not a governance bypass.
* **Secrets are never exposed.** The connectors holding credentials live in the
  parent process; sandbox code references a connector *by id* and calls
  operations on it — it never sees a password or token. (Over the bridge, only
  tool name + args + a bounded result summary cross the boundary.)
* **Results stay off-context.** A large result set is written to the run's
  workspace on disk; ``run`` returns only a bounded preview + a handle + the row
  count, so querying a million rows grows model context by a small, fixed amount.
"""

from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


# The governed entry point: dispatcher.execute(tool_name, args) -> ToolResult.
ExecuteFn = Callable[[str, Dict[str, Any]], Any]


class ConnectorSDK:
    """In-process SDK backing both direct use and the worker bridge."""

    def __init__(
        self,
        execute_fn: ExecuteFn,
        *,
        registry: Any = None,
        workdir: str = ".dacli/sandbox/run",
        preview_rows: int = 20,
        spill_threshold_rows: int = 50,
    ):
        self._execute = execute_fn
        self._registry = registry
        self.workdir = Path(workdir)
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.preview_rows = preview_rows
        self.spill_threshold_rows = spill_threshold_rows

    # ------------------------------------------------------------------
    # discovery
    # ------------------------------------------------------------------
    def available_tools(self) -> List[Dict[str, Any]]:
        if self._registry is None:
            return []
        try:
            return self._registry.get_tool_digest()
        except Exception:
            return []

    # ------------------------------------------------------------------
    # the one governed call
    # ------------------------------------------------------------------
    async def run(self, tool_name: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Run a connector operation through the governor and bound the result.

        Returns a JSON-serializable summary (never the raw connection / creds):
        ``{status, tool, row_count, preview, saved_path?, handle?, error?}``.
        """
        args = dict(args or {})
        result = await self._execute(tool_name, args)
        status = getattr(getattr(result, "status", None), "value", str(getattr(result, "status", "")))
        error = getattr(result, "error", None)
        data = getattr(result, "data", None)

        summary: Dict[str, Any] = {"tool": tool_name, "status": status}
        if error:
            summary["error"] = error
        # A governance block/denial surfaces here so sandbox code sees the refusal
        # exactly like the tool tier does.
        if status in ("blocked", "denied"):
            summary["governed"] = (getattr(result, "metadata", {}) or {}).get("governance")
            return summary

        if isinstance(data, list):
            summary["row_count"] = len(data)
            if len(data) > self.spill_threshold_rows:
                path = self._spill(tool_name, data)
                summary["saved_path"] = str(path)
                summary["handle"] = path.name
                summary["preview"] = data[: self.preview_rows]
            else:
                summary["preview"] = data
        elif data is not None:
            summary["data"] = data
        return summary

    # ------------------------------------------------------------------
    # workspace I/O (off-context by construction)
    # ------------------------------------------------------------------
    def _spill(self, tool_name: str, rows: List[Any]) -> Path:
        stamp = f"{int(time.time()*1000)}"
        safe = "".join(c for c in tool_name if c.isalnum() or c in "_-")[:40]
        path = self.workdir / f"{safe}_{stamp}.jsonl"
        with open(path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, default=str) + "\n")
        return path

    def save_rows(self, name: str, rows: List[Dict[str, Any]], *, fmt: str = "jsonl") -> str:
        """Persist rows to the run workspace; returns the path (stays on disk)."""
        path = self.workdir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if fmt == "csv" and rows and isinstance(rows[0], dict):
            with open(path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)
        else:
            with open(path, "w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row, default=str) + "\n")
        return str(path)

    def read_rows(self, name: str, *, limit: Optional[int] = None) -> List[Any]:
        path = self.workdir / name
        out: List[Any] = []
        if not path.exists():
            return out
        with open(path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if limit is not None and i >= limit:
                    break
                line = line.strip()
                if line:
                    out.append(json.loads(line))
        return out

    @staticmethod
    def summarize(rows: List[Any], n: int = 20) -> Dict[str, Any]:
        """Build a bounded summary of a large row set for return to context."""
        return {"row_count": len(rows), "sample": rows[:n]}
