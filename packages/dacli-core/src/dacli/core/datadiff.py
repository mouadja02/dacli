"""Read-only data diff between two tables on one connector.

Row-count delta + per-column null-rate delta over a bounded sample + a
position-wise sampled row comparison, each query dispatched through the
connector's governed query op (``Dispatcher.execute``). Backs ``dacli diff``.

This was the ``diff`` half of the old data-diff skill; the skill subsystem is
gone, so the engine lives here as a plain helper the CLI calls directly. The
``promote`` (CREATE OR REPLACE) variant went with the skill — promotion is a
mutation the agent runs through a normal governed tool call, not a CLI verb.
"""

from __future__ import annotations

import re
from typing import Any

from dacli.connectors.base import ToolResult, ToolStatus

# Dotted/qualified identifiers only (`project:dataset.table`, `db.schema.t`,
# `t$part`) — no quotes, spaces, or statement separators, so the interpolated
# SQL can't smuggle a second statement.
_IDENT_RE = re.compile(r"[A-Za-z0-9_][\w.$:\-]*\Z")

_METHOD = (
    "row-count delta + per-column null-rate delta over a bounded sample + "
    "position-wise sampled row comparison"
)


def _query_tool(registry: Any, connector_id: str) -> str:
    """Resolve the connector's SQL query op by its declared capability, falling
    back to the seed naming convention."""
    get = getattr(registry, "get_connector", None)
    conn: Any = get(connector_id) if callable(get) else None
    if conn is not None:
        for op in conn.operations():
            if op.capability == f"{connector_id}.query":
                return op.name
    return f"execute_{connector_id}_query"


async def _count(dispatcher: Any, tool: str, table: str) -> tuple[int, str | None]:
    res = await dispatcher.execute(tool, {"query": f"SELECT COUNT(*) AS N FROM {table}"})
    if not res.success:
        return 0, f"count query on {table} failed: {res.error}"
    rows = res.data if isinstance(res.data, list) else []
    try:
        first = rows[0]
        value = next(iter(first.values()))
        return int(float(value)), None
    except (IndexError, StopIteration, TypeError, ValueError):
        return 0, f"count query on {table} returned no parseable count: {res.data!r}"


async def _sample(dispatcher: Any, tool: str, table: str, n: int) -> tuple[list[dict], str | None]:
    res = await dispatcher.execute(tool, {"query": f"SELECT * FROM {table} LIMIT {n}"})
    if not res.success:
        return [], f"sample query on {table} failed: {res.error}"
    rows = res.data if isinstance(res.data, list) else []
    return [r for r in rows if isinstance(r, dict)], None


def _column_deltas(sample_a: list[dict], sample_b: list[dict]) -> list[dict[str, Any]]:
    columns: list[str] = []
    for row in (*sample_a, *sample_b):
        for key in row:
            if key not in columns:
                columns.append(key)

    def null_rate(sample: list[dict], col: str) -> float:
        if not sample:
            return 0.0
        return sum(1 for r in sample if r.get(col) is None) / len(sample)

    out = []
    for col in columns:
        rate_a = null_rate(sample_a, col)
        rate_b = null_rate(sample_b, col)
        out.append({
            "name": col,
            "null_rate_a": rate_a,
            "null_rate_b": rate_b,
            "delta": rate_b - rate_a,
        })
    return out


def _sample_summary(sample_a: list[dict], sample_b: list[dict]) -> dict[str, int]:
    compared = min(len(sample_a), len(sample_b))
    differing = sum(
        1 for ra, rb in zip(sample_a[:compared], sample_b[:compared], strict=True)
        if ra != rb
    )
    return {
        "size_a": len(sample_a),
        "size_b": len(sample_b),
        "rows_compared": compared,
        "rows_differing": differing,
    }


def _error(message: str) -> ToolResult:
    return ToolResult(tool_name="data_diff", status=ToolStatus.ERROR, error=message)


async def run_data_diff(
    dispatcher: Any,
    registry: Any,
    connector: str,
    table_a: str,
    table_b: str,
    sample_size: int = 100,
) -> ToolResult:
    """Diff ``table_a`` against ``table_b`` on ``connector``. Read-only: every
    statement is a SELECT/COUNT dispatched through the governed query op."""
    connector = str(connector or "").strip()
    table_a = str(table_a or "").strip()
    table_b = str(table_b or "").strip()
    sample_size = min(max(int(sample_size or 100), 1), 1000)

    if not connector:
        return _error("missing 'connector'")
    for label, table in (("table_a", table_a), ("table_b", table_b)):
        if not _IDENT_RE.match(table):
            return _error(f"invalid {label} identifier: {table!r}")

    tool = _query_tool(registry, connector)

    count_a, err = await _count(dispatcher, tool, table_a)
    if err:
        return _error(err)
    count_b, err = await _count(dispatcher, tool, table_b)
    if err:
        return _error(err)

    sample_a, err = await _sample(dispatcher, tool, table_a, sample_size)
    if err:
        return _error(err)
    sample_b, err = await _sample(dispatcher, tool, table_b, sample_size)
    if err:
        return _error(err)

    data = {
        "table_a": table_a,
        "table_b": table_b,
        "row_count_a": count_a,
        "row_count_b": count_b,
        "row_delta": count_b - count_a,
        "columns": _column_deltas(sample_a, sample_b),
        "sample": _sample_summary(sample_a, sample_b),
        "method": _METHOD,
        "mode": "diff",
    }
    return ToolResult(tool_name="data_diff", status=ToolStatus.SUCCESS, data=data)
