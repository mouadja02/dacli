"""Data-quality assertions with governed remediation (P14, slice A).

A connector-agnostic assertion the agent can author, run, and *act on*: e.g.
*"null-rate of `orders.amount` > 1%"*. The check itself is a read-only metric
query through the **governed dispatcher** (so it is classified + audited like any
read); a breach yields a *proposed* remediation that is never executed unless the
caller opts in (``apply=True``), and even then it runs through the same
classify → approve → verify → rollback gate as any action.

Assertions persist under P01's state dir (``<state_dir>/assertions.json``). The
metric SQL reuses the bounded, identifier-validated style of the ``data_diff``
skill; the proposed fix reuses :class:`~dacli.core.why_failed.ProposedFix` and
its governed apply path, so quality remediation is not a second governance lane.

Fail-soft on read: an unreachable connector or an unparseable count degrades to
an error outcome rather than raising. The result is a machine-readable
:class:`AssertionOutcome` (the headless JSON contract style) so it drops into
CI/alerting.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from dacli.core.headless import EXIT_AGENT_ERROR, EXIT_GOVERNANCE_BLOCK, EXIT_OK
from dacli.core.logging_setup import get_logger
from dacli.core.paths import state_dir
from dacli.core.why_failed import ProposedFix, _apply_fix
from dacli.skills.data_diff.skill import _IDENT_RE

log = get_logger(__name__)

# A breach (the asserted *bad* condition held) is a non-zero, non-governance
# exit so CI can gate on it — distinct from a clean pass and from a read error.
EXIT_BREACH = EXIT_AGENT_ERROR

# Supported metrics → the read-only SQL that measures them. Each builder takes a
# validated table (and column, where relevant) and returns one SELECT.
_METRICS = ("null_rate", "row_count")

# Comparison operators usable in an assertion predicate. The predicate expresses
# the *breach*: `null_rate > 0.01` is breached when the live null rate exceeds 1%.
_OPS: dict[str, Any] = {
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}


def _leaf(name: str) -> str:
    return name.split(".")[-1] if name else name


@dataclass
class Assertion:
    """A persisted, connector-agnostic data-quality check + its breach predicate."""

    name: str
    connector: str
    table: str
    metric: str                 # one of _METRICS
    op: str                     # one of _OPS
    threshold: float
    column: str | None = None   # required for null_rate
    # Optional remediation override. Defaults to re-running the dbt model that
    # populates ``table`` (see :func:`propose_remediation`).
    remediation_tool: str | None = None
    remediation_args: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> str | None:
        """Return the first problem with this assertion, or None when well-formed."""
        if self.metric not in _METRICS:
            return f"unknown metric {self.metric!r} (try one of {list(_METRICS)})"
        if self.op not in _OPS:
            return f"unknown operator {self.op!r} (try one of {list(_OPS)})"
        if self.metric == "null_rate" and not self.column:
            return "null_rate needs a column"
        for label, ident in (("table", self.table), ("column", self.column)):
            if ident and not _IDENT_RE.match(ident):
                return f"invalid {label} identifier: {ident!r}"
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "connector": self.connector,
            "table": self.table,
            "metric": self.metric,
            "op": self.op,
            "threshold": self.threshold,
            "column": self.column,
            "remediation_tool": self.remediation_tool,
            "remediation_args": self.remediation_args,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Assertion:
        return cls(
            name=str(d.get("name") or ""),
            connector=str(d.get("connector") or ""),
            table=str(d.get("table") or ""),
            metric=str(d.get("metric") or ""),
            op=str(d.get("op") or ""),
            threshold=float(d.get("threshold") or 0.0),
            column=d.get("column"),
            remediation_tool=d.get("remediation_tool"),
            remediation_args=dict(d.get("remediation_args") or {}),
        )

    def describe(self) -> str:
        subject = f"{self.metric} of {self.table}" + (f".{self.column}" if self.column else "")
        return f"{subject} {self.op} {self.threshold}"


@dataclass
class AssertionOutcome:
    """The result of evaluating one assertion (machine-readable)."""

    name: str
    predicate: str
    value: float | None = None
    breached: bool = False
    proposed_fix: ProposedFix | None = None
    error: str | None = None
    blocked: bool = False

    @property
    def ok(self) -> bool:
        return self.error is None

    @property
    def exit_code(self) -> int:
        if self.blocked:
            return EXIT_GOVERNANCE_BLOCK
        if not self.ok:
            return EXIT_AGENT_ERROR
        return EXIT_BREACH if self.breached else EXIT_OK

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "exit_code": self.exit_code,
            "name": self.name,
            "predicate": self.predicate,
            "value": self.value,
            "breached": self.breached,
            "proposed_fix": self.proposed_fix.to_dict() if self.proposed_fix else None,
            "error": self.error,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)


# ---------------------------------------------------------------------------
# Persistence (under P01's state dir)
# ---------------------------------------------------------------------------
def _store_path() -> Any:
    return state_dir() / "assertions.json"


def load_assertions() -> dict[str, Assertion]:
    """Every persisted assertion, keyed by name (empty when none saved)."""
    path = _store_path()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[str, Assertion] = {}
    for name, d in (raw or {}).items():
        if isinstance(d, dict):
            out[name] = Assertion.from_dict({**d, "name": name})
    return out


def save_assertion(assertion: Assertion) -> None:
    from dacli.core.atomicio import write_json_atomic

    store = load_assertions()
    store[assertion.name] = assertion
    write_json_atomic(_store_path(), {n: a.to_dict() for n, a in store.items()})


def delete_assertion(name: str) -> bool:
    from dacli.core.atomicio import write_json_atomic

    store = load_assertions()
    if name not in store:
        return False
    del store[name]
    write_json_atomic(_store_path(), {n: a.to_dict() for n, a in store.items()})
    return True


# ---------------------------------------------------------------------------
# Remediation
# ---------------------------------------------------------------------------
def propose_remediation(assertion: Assertion) -> ProposedFix:
    """A governed remediation for a breached assertion (not executed here).

    An explicit ``remediation_tool`` wins; otherwise default to re-running the
    dbt model that populates the table — the same posture as the failure
    explainer's proposed fix.
    """
    if assertion.remediation_tool:
        return ProposedFix(
            tool_name=assertion.remediation_tool,
            args=dict(assertion.remediation_args),
            rationale=f"remediate breach of '{assertion.name}' ({assertion.describe()})",
        )
    model = _leaf(assertion.table)
    return ProposedFix(
        tool_name="dbt_run", args={"select": model},
        rationale=(f"re-run model '{model}' to correct the breached metric "
                   f"({assertion.describe()})"))


# ---------------------------------------------------------------------------
# Metric measurement (read-only, through the governed dispatcher)
# ---------------------------------------------------------------------------
def _query_tool(registry: Any, connector_id: str) -> str:
    """Resolve the connector's SQL query op by its declared capability."""
    get = getattr(registry, "get_connector", None)
    conn: Any = get(connector_id) if callable(get) else None
    if conn is not None:
        for op in conn.operations():
            if op.capability == f"{connector_id}.query":
                return op.name
    return f"execute_{connector_id}_query"


async def _scalar(dispatcher: Any, tool: str, sql: str) -> tuple[float | None, str | None]:
    res = await dispatcher.execute(tool, {"query": sql})
    status = getattr(getattr(res, "status", None), "value", None)
    if status in ("denied", "blocked"):
        return None, f"metric query blocked by governance: {res.error}"
    if not res.success:
        return None, f"metric query failed: {res.error}"
    rows = res.data if isinstance(res.data, list) else []
    try:
        return float(next(iter(rows[0].values()))), None
    except (IndexError, StopIteration, TypeError, ValueError, AttributeError):
        return None, f"metric query returned no parseable value: {res.data!r}"


async def _measure(assertion: Assertion, dispatcher: Any, tool: str
                   ) -> tuple[float | None, str | None]:
    if assertion.metric == "row_count":
        return await _scalar(dispatcher, tool, f"SELECT COUNT(*) AS N FROM {assertion.table}")
    # null_rate: total + non-null in one pass; rate = (total - non_null) / total.
    res = await dispatcher.execute(tool, {"query": (
        f"SELECT COUNT(*) AS TOTAL, COUNT({assertion.column}) AS NONNULL "
        f"FROM {assertion.table}")})
    status = getattr(getattr(res, "status", None), "value", None)
    if status in ("denied", "blocked"):
        return None, f"metric query blocked by governance: {res.error}"
    if not res.success:
        return None, f"metric query failed: {res.error}"
    rows = res.data if isinstance(res.data, list) else []
    try:
        row = {str(k).upper(): v for k, v in rows[0].items()}
        total, nonnull = float(row["TOTAL"]), float(row["NONNULL"])
    except (IndexError, KeyError, TypeError, ValueError, AttributeError):
        return None, f"null-rate query returned no parseable counts: {res.data!r}"
    return ((total - nonnull) / total if total else 0.0), None


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------
async def evaluate(assertion: Assertion, dispatcher: Any, *, apply: bool = False
                   ) -> AssertionOutcome:
    """Measure the assertion's metric and, on breach, propose a governed fix.

    With ``apply=True`` a breach's proposed fix is routed through the governed
    dispatcher (classify → approve → verify → rollback). A denial leaves the fix
    ``applied`` False and flags the outcome blocked.
    """
    outcome = AssertionOutcome(name=assertion.name, predicate=assertion.describe())
    problem = assertion.validate()
    if problem:
        outcome.error = problem
        return outcome
    if dispatcher is None:
        outcome.error = "no dispatcher available — assertions run through the governed path"
        return outcome

    registry = getattr(dispatcher, "_registry", None) or getattr(dispatcher, "registry", None)
    tool = _query_tool(registry, assertion.connector)
    value, err = await _measure(assertion, dispatcher, tool)
    if err is not None:
        outcome.error = err
        outcome.blocked = "blocked by governance" in err
        return outcome

    outcome.value = value
    outcome.breached = bool(_OPS[assertion.op](value, assertion.threshold))
    if not outcome.breached:
        return outcome

    fix = propose_remediation(assertion)
    outcome.proposed_fix = fix
    if apply:
        await _apply_fix(dispatcher, fix)
        if fix.status in ("denied", "blocked"):
            outcome.blocked = True
    return outcome
