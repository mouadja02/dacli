"""Off-context spill of large tool results (Phase 3.4).

A 10k-row query must not enter the model's context verbatim — it would blow the
budget and bury the task. Instead the **full** result is written to the session
workspace on disk, and the model receives a structured *summary* (shape, columns,
row count, head/tail sample, simple anomalies) plus a **handle** to fetch more via
the ``fetch_result`` tool.

Reconciles with DACLI.md rule #7 ("never truncate/summarize returned data"): that
rule governs the **human** view, which is untouched — the CLI still renders the
full table from ``result.data`` (``tui/ui.py`` ``tool_end``), and the full result
is still persisted to session state by ``memory.log_tool_execution``. Only the
*model's context copy* is summarized, and the model can always pull the full data
back with ``fetch_result``.

Small results (errors, scalars, a few rows) stay inline verbatim — summarizing
them adds a round trip for no token saving.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# How many head/tail rows to show in the summary sample.
SAMPLE_ROWS = 5


class ResultStore:
    """On-disk store for spilled tool results, keyed by an opaque handle.

    Written by the kernel's spill hook; read by the ``fetch_result`` system op.
    Both share one store instance per session so handles resolve.
    """

    def __init__(self, root: str = ".dacli/workspace", session_id: str = "default"):
        self.dir = Path(root) / session_id
        self.dir.mkdir(parents=True, exist_ok=True)

    def _path(self, handle: str) -> Path:
        # Guard against path traversal in a model-supplied handle.
        safe = "".join(c for c in handle if c.isalnum() or c in ("_", "-"))
        return self.dir / f"{safe}.json"

    def write(self, tool_name: str, data: Any) -> str:
        handle = f"res_{datetime.now():%H%M%S}_{uuid.uuid4().hex[:6]}"
        payload = {
            "tool_name": tool_name,
            "spilled_at": datetime.now().isoformat(timespec="seconds"),
            "data": data,
        }
        with open(self._path(handle), "w", encoding="utf-8") as f:
            json.dump(payload, f, default=str)
        return handle

    def read(self, handle: str, start: int = 0, count: Optional[int] = None) -> Dict[str, Any]:
        path = self._path(handle)
        if not path.exists():
            return {"error": f"Unknown result handle '{handle}'."}
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        data = payload.get("data")
        if isinstance(data, list):
            total = len(data)
            end = total if count is None else min(start + count, total)
            window = data[start:end]
            return {
                "tool_name": payload.get("tool_name"),
                "total_rows": total,
                "start": start,
                "returned": len(window),
                "data": window,
            }
        return {"tool_name": payload.get("tool_name"), "data": data}


def _columns(rows: List[Any]) -> List[str]:
    if rows and isinstance(rows[0], dict):
        return list(rows[0].keys())
    return []


def _null_columns(rows: List[Dict[str, Any]], columns: List[str]) -> List[str]:
    """Columns containing any null/empty value — a cheap anomaly hint."""
    flagged = []
    for col in columns:
        if any(row.get(col) in (None, "") for row in rows):
            flagged.append(col)
    return flagged


def summarize(result: Any, handle: str) -> str:
    """Build the model-facing summary string for a spilled result."""
    data = result.data
    header = f"[{result.tool_name}] Executed successfully — result spilled off-context (handle: {handle})."
    fetch = (
        f"\nThe FULL result is preserved (the user already sees the complete table). "
        f"To read it, call fetch_result(handle=\"{handle}\", start=0, count=N)."
    )

    if isinstance(data, list) and data and isinstance(data[0], dict):
        cols = _columns(data)
        lines = [
            header,
            f"Shape: {len(data)} rows × {len(cols)} columns.",
            f"Columns: {', '.join(cols)}",
        ]
        nulls = _null_columns(data, cols)
        if nulls:
            lines.append(f"Columns containing nulls/empties: {', '.join(nulls)}")
        head = data[:SAMPLE_ROWS]
        tail = data[-SAMPLE_ROWS:] if len(data) > SAMPLE_ROWS else []
        lines.append(f"First {len(head)} rows: {json.dumps(head, default=str)}")
        if tail:
            lines.append(f"Last {len(tail)} rows: {json.dumps(tail, default=str)}")
        lines.append(fetch)
        return "\n".join(lines)

    if isinstance(data, list):
        return (
            f"{header}\nList of {len(data)} items. "
            f"First {min(SAMPLE_ROWS, len(data))}: {json.dumps(data[:SAMPLE_ROWS], default=str)}{fetch}"
        )

    # Large scalar/text/dict: note size and spill.
    return f"{header}\nLarge non-tabular result of {len(str(data))} chars.{fetch}"


def summarize_or_inline(
    result: Any,
    counter: Any,
    threshold_tokens: int,
    store: ResultStore,
) -> str:
    """Return the model-facing tool message: inline if small, summary if large.

    Errors are always inline (small and important). A successful result whose
    inline form exceeds ``threshold_tokens`` is spilled to ``store`` and replaced
    by a structured summary + fetch handle.
    """
    inline = result.to_message()
    if not getattr(result, "success", False):
        return inline
    if counter.count(inline) <= threshold_tokens:
        return inline
    handle = store.write(result.tool_name, result.data)
    return summarize(result, handle)
