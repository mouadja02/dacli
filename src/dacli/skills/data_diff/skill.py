"""Data diff (diff-before-promote) — F-2.

Row-count + per-column null-rate + bounded-sample comparison between two
tables on the same connector, computed through the connector's **governed**
query op (every statement flows through ``Dispatcher.execute``). The promote
variant dispatches an irreversible ``CREATE OR REPLACE`` through that same
path, so the verified-rollback + approval machinery applies — a promote
without the diff and a governance gate is structurally impossible.
"""

from __future__ import annotations

import re
from typing import Any

from dacli.connectors.base import ToolResult, ToolStatus
from dacli.core.verify import PostCondition, VerificationContext
from dacli.skills.spec import Skill, SkillSpec, SkillContext

# Dotted/qualified identifiers only (`project:dataset.table`, `db.schema.t`,
# `t$part`) — no quotes, spaces, or statement separators, so the interpolated
# SQL cannot smuggle a second statement.
_IDENT_RE = re.compile(r"[A-Za-z0-9_][\w.$:\-]*\Z")

_METHOD = (
    "row-count delta + per-column null-rate delta over a bounded sample + "
    "position-wise sampled row comparison"
)


# ---------------------------------------------------------------------------
# Post-conditions
# ---------------------------------------------------------------------------
def diff_reports_expected_shape() -> PostCondition:
    """Counts are non-negative ints and the reported delta is consistent."""
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        for key in ("table_a", "table_b", "row_count_a", "row_count_b",
                    "row_delta", "columns", "sample", "method", "mode"):
            if key not in data:
                return False, f"diff result is missing '{key}'"
        a, b = data["row_count_a"], data["row_count_b"]
        if not (isinstance(a, int) and isinstance(b, int) and a >= 0 and b >= 0):
            return False, f"row counts are not non-negative ints: {a!r}, {b!r}"
        if data["row_delta"] != b - a:
            return False, f"row_delta {data['row_delta']} ≠ {b} - {a}"
        if not isinstance(data["columns"], list):
            return False, "columns is not a list"
        return True, ""
    return PostCondition(
        "diff_reports_expected_shape", check,
        "the diff result has the expected, arithmetically consistent shape",
        anchored=True,
    )


def promote_is_diff_gated() -> PostCondition:
    """A result claiming ``promoted`` must carry the diff it was gated on."""
    def applies(ctx: VerificationContext) -> bool:
        data = getattr(ctx.result, "data", None) or {}
        return data.get("mode") == "promote"

    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if data.get("promoted") and "row_count_a" not in data:
            return False, "promoted without a diff — the gate was bypassed"
        return True, ""
    return PostCondition(
        "promote_is_diff_gated", check,
        "a promote always carries the diff it was reviewed against",
        anchored=True, applies_when=applies,
    )


# ---------------------------------------------------------------------------
# Skill
# ---------------------------------------------------------------------------
class DataDiffSkill(Skill):
    spec = SkillSpec(
        name="data-diff",
        description=(
            "Diff two tables on one connector: row-count delta, per-column "
            "null-rate delta over a bounded sample, and a sampled value "
            "comparison. mode='promote' replaces table_b with table_a ONLY "
            "after the diff, through the governed (approval-gated) path."
        ),
        version="1.0.0",
        can_do=[
            "compare row counts between two tables",
            "compare per-column null rates over a bounded sample",
            "promote a candidate table over a target, gated on the diff + approval",
        ],
        cannot_do=[
            "full-table reconciliation (the sample is bounded)",
            "mutate anything in 'diff' mode",
            "promote without governance approval",
        ],
        input_schema={
            "type": "object",
            "properties": {
                "connector": {"type": "string", "description": "Connector id (e.g. snowflake, bigquery)."},
                "table_a": {"type": "string", "description": "Candidate table (qualified name)."},
                "table_b": {"type": "string", "description": "Target/baseline table (qualified name)."},
                "sample_size": {"type": "integer", "description": "Rows sampled per side (default 100, max 1000)."},
                "mode": {"type": "string", "enum": ["diff", "promote"],
                         "description": "'diff' is read-only; 'promote' replaces table_b with table_a after the diff + approval."},
            },
            "required": ["connector", "table_a", "table_b"],
        },
        output_schema={
            "type": "object",
            "properties": {
                "table_a": {"type": "string"},
                "table_b": {"type": "string"},
                "row_count_a": {"type": "integer"},
                "row_count_b": {"type": "integer"},
                "row_delta": {"type": "integer"},
                "columns": {"type": "array"},
                "sample": {"type": "object"},
                "method": {"type": "string"},
                "mode": {"type": "string"},
                "promoted": {"type": "boolean"},
            },
            "required": ["table_a", "table_b", "row_count_a", "row_count_b",
                         "row_delta", "columns", "sample", "method", "mode"],
        },
        postconditions=[diff_reports_expected_shape(), promote_is_diff_gated()],
        min_confidence=0.75,
        tier="tool",
        category="data-quality",
    )

    async def execute(self, args: dict[str, Any], context: SkillContext) -> ToolResult:
        args = dict(args or {})
        connector = str(args.get("connector") or "").strip()
        table_a = str(args.get("table_a") or "").strip()
        table_b = str(args.get("table_b") or "").strip()
        mode = (args.get("mode") or "diff").lower()
        sample_size = min(max(int(args.get("sample_size") or 100), 1), 1000)

        if context.dispatcher is None:
            return self._error("no dispatcher available — the diff must run through the governed path")
        if not connector:
            return self._error("missing 'connector'")
        for label, table in (("table_a", table_a), ("table_b", table_b)):
            if not _IDENT_RE.match(table):
                return self._error(f"invalid {label} identifier: {table!r}")

        tool = self._query_tool(context.registry, connector)

        # 1. Row counts (read-only, dispatched through governance).
        count_a, err = await self._count(context.dispatcher, tool, table_a)
        if err:
            return self._error(err)
        count_b, err = await self._count(context.dispatcher, tool, table_b)
        if err:
            return self._error(err)

        # 2. Bounded samples → per-column null rates + value comparison.
        sample_a, err = await self._sample(context.dispatcher, tool, table_a, sample_size)
        if err:
            return self._error(err)
        sample_b, err = await self._sample(context.dispatcher, tool, table_b, sample_size)
        if err:
            return self._error(err)

        data: dict[str, Any] = {
            "table_a": table_a,
            "table_b": table_b,
            "row_count_a": count_a,
            "row_count_b": count_b,
            "row_delta": count_b - count_a,
            "columns": self._column_deltas(sample_a, sample_b),
            "sample": self._sample_summary(sample_a, sample_b),
            "method": _METHOD,
            "mode": mode,
        }

        if mode != "promote":
            return ToolResult(tool_name=self.spec.name, status=ToolStatus.SUCCESS, data=data)

        # 3. Promote: an irreversible REPLACE, dispatched through the governed
        # path — classification, verified rollback, dry-run, and human approval
        # all apply. Denied/blocked means the platform was never touched.
        promote_sql = f"CREATE OR REPLACE TABLE {table_b} AS SELECT * FROM {table_a}"
        res = await context.dispatcher.execute(tool, {"query": promote_sql})
        if not res.success:
            data["promoted"] = False
            return ToolResult(
                tool_name=self.spec.name,
                status=res.status if res.status in (ToolStatus.DENIED, ToolStatus.BLOCKED) else ToolStatus.ERROR,
                data=data,
                error=f"promote was not executed: {res.error}",
            )
        data["promoted"] = True
        return ToolResult(tool_name=self.spec.name, status=ToolStatus.SUCCESS, data=data)

    # ------------------------------------------------------------------
    def _error(self, message: str) -> ToolResult:
        return ToolResult(tool_name=self.spec.name, status=ToolStatus.ERROR, error=message)

    @staticmethod
    def _query_tool(registry: Any, connector_id: str) -> str:
        """Resolve the connector's SQL query op by its declared capability."""
        get = getattr(registry, "get_connector", None)
        conn = get(connector_id) if callable(get) else None
        if conn is not None:
            for op in conn.operations():
                if op.capability == f"{connector_id}.query":
                    return op.name
        return f"execute_{connector_id}_query"

    async def _count(self, dispatcher: Any, tool: str, table: str) -> tuple[int, str | None]:
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

    async def _sample(self, dispatcher: Any, tool: str, table: str,
                      n: int) -> tuple[list[dict], str | None]:
        res = await dispatcher.execute(tool, {"query": f"SELECT * FROM {table} LIMIT {n}"})
        if not res.success:
            return [], f"sample query on {table} failed: {res.error}"
        rows = res.data if isinstance(res.data, list) else []
        return [r for r in rows if isinstance(r, dict)], None

    @staticmethod
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

    @staticmethod
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
