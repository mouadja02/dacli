"""Data-quality assertion skill (P14, slice A).

Lets the agent author and run a connector-agnostic assertion (e.g. *"null-rate
of orders.amount > 1%"*) through the governed dispatcher. The check is a
read-only metric query; a breach yields a *proposed* governed fix that is only
executed when ``apply`` is set — and even then through the normal
classify → approve → verify → rollback gate.

The engine lives in :mod:`dacli.core.quality`; this is the LLM-callable surface.
"""

from __future__ import annotations

from typing import Any

from dacli.connectors.base import ToolResult, ToolStatus
from dacli.core.quality import Assertion, evaluate
from dacli.core.verify import PostCondition, VerificationContext
from dacli.skills.spec import Skill, SkillContext, SkillSpec


def outcome_reports_expected_shape() -> PostCondition:
    """The outcome carries a numeric value + a boolean breach verdict."""
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        for key in ("name", "predicate", "value", "breached"):
            if key not in data:
                return False, f"assertion outcome is missing '{key}'"
        if not isinstance(data["breached"], bool):
            return False, "breached is not a boolean"
        if not isinstance(data["value"], (int, float)):
            return False, f"value is not numeric: {data['value']!r}"
        return True, ""
    return PostCondition(
        "outcome_reports_expected_shape", check,
        "the assertion outcome has a numeric value and a boolean breach verdict",
        anchored=True)


def breach_not_auto_applied() -> PostCondition:
    """Without ``apply``, a breach's fix is proposed, never executed (no bypass)."""
    def applies(ctx: VerificationContext) -> bool:
        return not ctx.args.get("apply") and bool((getattr(ctx.result, "data", None) or {}).get("breached"))

    def check(ctx: VerificationContext):
        fix = (getattr(ctx.result, "data", None) or {}).get("proposed_fix") or {}
        if fix.get("applied"):
            return False, "a breach was remediated without apply — the gate was bypassed"
        return True, ""
    return PostCondition(
        "breach_not_auto_applied", check,
        "a breach proposes a fix without applying it unless apply is set",
        anchored=True, applies_when=applies)


class QualityAssertSkill(Skill):
    spec = SkillSpec(
        name="quality-assert",
        description=(
            "Assert a data-quality condition on a connector and act on a breach: "
            "measure a metric (null_rate of a column, or row_count of a table) via "
            "the governed query op, compare it against a threshold, and on breach "
            "propose a governed remediation. apply=true routes the fix through the "
            "approval-gated path; otherwise the fix is only proposed."
        ),
        version="1.0.0",
        can_do=[
            "measure a column's null rate or a table's row count",
            "flag a breach when the metric crosses a threshold",
            "propose a governed remediation for a breach",
            "apply the remediation through the approval-gated path (apply=true)",
        ],
        cannot_do=[
            "mutate anything while measuring the metric",
            "apply a remediation without governance approval",
            "full-table profiling beyond the single declared metric",
        ],
        input_schema={
            "type": "object",
            "properties": {
                "connector": {"type": "string", "description": "Connector id (e.g. bigquery, snowflake)."},
                "table": {"type": "string", "description": "Qualified table name."},
                "metric": {"type": "string", "enum": ["null_rate", "row_count"]},
                "op": {"type": "string", "enum": [">", ">=", "<", "<=", "==", "!="],
                       "description": "Breach predicate: the condition that, when true, is a breach."},
                "threshold": {"type": "number"},
                "column": {"type": "string", "description": "Column for null_rate."},
                "apply": {"type": "boolean",
                          "description": "Route the proposed fix through the governed path on breach."},
            },
            "required": ["connector", "table", "metric", "op", "threshold"],
        },
        output_schema={
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "predicate": {"type": "string"},
                "value": {"type": "number"},
                "breached": {"type": "boolean"},
                "proposed_fix": {"type": "object"},
            },
            "required": ["name", "predicate", "value", "breached"],
        },
        postconditions=[outcome_reports_expected_shape(), breach_not_auto_applied()],
        min_confidence=0.75,
        tier="tool",
        category="data-quality",
    )

    async def execute(self, args: dict[str, Any], context: SkillContext) -> ToolResult:
        args = dict(args or {})
        if context.dispatcher is None:
            return ToolResult(tool_name=self.spec.name, status=ToolStatus.ERROR,
                              error="no dispatcher available — the check must run through the governed path")
        assertion = Assertion(
            name=str(args.get("name") or "inline"),
            connector=str(args.get("connector") or "").strip(),
            table=str(args.get("table") or "").strip(),
            metric=str(args.get("metric") or "").strip(),
            op=str(args.get("op") or "").strip(),
            threshold=float(args.get("threshold") or 0.0),
            column=(args.get("column") or None),
        )
        outcome = await evaluate(assertion, context.dispatcher, apply=bool(args.get("apply")))
        status = ToolStatus.ERROR if outcome.error else ToolStatus.SUCCESS
        return ToolResult(tool_name=self.spec.name, status=status,
                          data=outcome.to_dict(), error=outcome.error)
