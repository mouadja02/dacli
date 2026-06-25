"""Pipeline-failure explainer (P13) — the engine behind ``dacli why-failed``.

Answers "why did last night's run fail?" end to end: locate the most recent
failed task/run (dbt run-results artifact, or an orchestrator connector), read its
logs, correlate the failure to the failing object via P12 lineage, and produce a
root-cause summary plus a *proposed* fix.

Two postures are load-bearing:

* every external read goes through the governed dispatcher (read-only ops), so
  the explainer is not a side channel around governance;
* the proposed fix is a normal governed action — it is *never* executed unless the
  caller opts in (``apply=True``), and when it is, it runs through the same
  classify→approve→verify→rollback gate as any action.

Fail-soft throughout: a missing artifact, an unreachable connector, or an absent
lineage edge degrades the answer rather than raising. The result is a
machine-readable :class:`FailureExplanation` (the headless JSON contract style)
so it drops into CI/alerting.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dacli.core.headless import EXIT_AGENT_ERROR, EXIT_GOVERNANCE_BLOCK, EXIT_OK
from dacli.core.logging_setup import get_logger

log = get_logger(__name__)

# dbt/orchestrator states that mean "this node did not succeed".
_DBT_FAILED = {"error", "fail", "runtime error"}
_AIRFLOW_FAILED = {"failed", "upstream_failed"}


@dataclass
class FailureFinding:
    """The located failure: what failed, where, and the relevant log."""

    source: str                 # "dbt" | "airflow" | "dagster"
    failing_node: str           # model unique_id / task_id
    status: str
    message: str = ""           # the one-line cause
    log_excerpt: str = ""       # the relevant log text
    run_id: str = ""
    dag_id: str = ""
    object_name: str = ""       # the data object the node produces (for lineage)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "failing_node": self.failing_node,
            "status": self.status,
            "message": self.message,
            "log_excerpt": self.log_excerpt,
            "run_id": self.run_id,
            "dag_id": self.dag_id,
            "object_name": self.object_name,
        }


@dataclass
class ProposedFix:
    """A governed remediation. Never executed unless the caller opts in."""

    tool_name: str
    args: dict[str, Any]
    rationale: str
    applied: bool = False
    status: str | None = None
    error: str | None = None
    governance: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tool_name": self.tool_name,
            "args": self.args,
            "rationale": self.rationale,
            "applied": self.applied,
            "status": self.status,
            "error": self.error,
            "governance": self.governance,
        }


@dataclass
class FailureExplanation:
    source: str
    finding: FailureFinding | None = None
    root_cause: str = ""
    downstream: list[dict[str, Any]] = field(default_factory=list)
    proposed_fix: ProposedFix | None = None
    error: str | None = None
    blocked: bool = False

    @property
    def ok(self) -> bool:
        return self.finding is not None and self.error is None

    @property
    def exit_code(self) -> int:
        if self.blocked:
            return EXIT_GOVERNANCE_BLOCK
        if not self.ok:
            return EXIT_AGENT_ERROR
        return EXIT_OK

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "exit_code": self.exit_code,
            "source": self.source,
            "finding": self.finding.to_dict() if self.finding else None,
            "root_cause": self.root_cause,
            "downstream": self.downstream,
            "proposed_fix": self.proposed_fix.to_dict() if self.proposed_fix else None,
            "error": self.error,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------
def _leaf(node: str) -> str:
    return node.split(".")[-1] if node else node


def _first_error_line(text: str) -> str:
    """The most informative single line of a log/message (best-effort)."""
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return (text or "").strip()[:300]
    for ln in lines:
        if re.search(r"\b(error|exception|failed|traceback|fatal)\b", ln, re.IGNORECASE):
            return ln[:300]
    return lines[-1][:300]


def read_dbt_run_results(project_dir: str | Path) -> list[dict[str, Any]] | None:
    """The ``results`` array of dbt's ``target/run_results.json`` (or None)."""
    path = Path(project_dir) / "target" / "run_results.json"
    try:
        return json.loads(path.read_text(encoding="utf-8")).get("results") or []
    except Exception:
        log.debug("could not read dbt run_results at %s", path, exc_info=True)
        return None


def locate_dbt_failure(results: list[dict[str, Any]]) -> FailureFinding | None:
    """The first error/fail node in a dbt run-results array."""
    for r in results or []:
        if str(r.get("status", "")).lower() in _DBT_FAILED:
            node = r.get("node") or r.get("unique_id") or ""
            msg = r.get("message") or ""
            return FailureFinding(
                source="dbt", failing_node=node, status=str(r.get("status")),
                message=_first_error_line(msg), log_excerpt=msg or "",
                object_name=_leaf(node),
            )
    return None


def locate_airflow_failure(
    *, dag_id: str, run: dict[str, Any], task_instances: list[dict[str, Any]], logs: str,
) -> FailureFinding | None:
    """The first failed task in a failed Airflow run."""
    task = next((t for t in task_instances
                 if str(t.get("state", "")).lower() in _AIRFLOW_FAILED), None)
    if task is None:
        return None
    task_id = str(task.get("task_id") or "")
    return FailureFinding(
        source="airflow", failing_node=task_id, status=str(task.get("state")),
        message=_first_error_line(logs), log_excerpt=logs or "",
        run_id=str(run.get("dag_run_id") or ""), dag_id=dag_id, object_name=task_id,
    )


def correlate(finding: FailureFinding | None, lineage: Any) -> list[Any]:
    """Downstream consumers of the failing object — its blast radius (P12)."""
    if finding is None or lineage is None or not finding.object_name:
        return []
    try:
        return lineage.downstream(finding.object_name)
    except Exception:
        log.debug("lineage correlation raised", exc_info=True)
        return []


def root_cause_summary(finding: FailureFinding, downstream: list[Any]) -> str:
    parts = [f"{finding.source} node '{finding.failing_node}' ended in "
             f"'{finding.status}'"]
    cause = finding.message or _first_error_line(finding.log_excerpt)
    if cause:
        parts.append(f"root cause: {cause}")
    if downstream:
        named = ", ".join(n.display() for n in downstream[:5])
        more = len(downstream) - 5
        if more > 0:
            named += f", +{more} more"
        parts.append(f"blast radius: {len(downstream)} downstream consumer(s) "
                     f"read this object ({named})")
    return ". ".join(parts) + "."


def propose_fix(finding: FailureFinding | None) -> ProposedFix | None:
    """A governed remediation for the located failure (not executed here)."""
    if finding is None:
        return None
    if finding.source == "dbt":
        model = finding.object_name or _leaf(finding.failing_node)
        return ProposedFix(
            tool_name="dbt_run", args={"select": model},
            rationale=f"re-run the failed model '{model}' after fixing its inputs")
    if finding.source == "airflow":
        return ProposedFix(
            tool_name="trigger_airflow_dag", args={"dag_id": finding.dag_id},
            rationale=f"re-trigger DAG '{finding.dag_id}' once the cause is resolved")
    return None


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
async def _governed_read(dispatcher: Any, tool: str, args: dict[str, Any]
                         ) -> tuple[bool, dict[str, Any], str, bool]:
    """Run a read-only op through the governed dispatcher.

    Returns ``(ok, data, error, blocked)`` where ``blocked`` flags a governance
    denial/block (so the caller can exit with the governance code).
    """
    res = await dispatcher.execute(tool, dict(args))
    status = getattr(getattr(res, "status", None), "value", None)
    blocked = status in ("denied", "blocked")
    data = res.data if isinstance(res.data, dict) else {}
    return res.success, data, (res.error or ""), blocked


async def _locate_airflow(dispatcher: Any, dag: str | None, run: str | None
                          ) -> tuple[FailureFinding | None, str, bool]:
    if not dag:
        return None, "airflow needs a --dag to inspect", False
    if run:
        ok, data, err, blocked = await _governed_read(
            dispatcher, "get_airflow_dag_run", {"dag_id": dag, "dag_run_id": run})
        run_rec = {"dag_run_id": run, "state": data.get("state")} if ok else {}
    else:
        ok, data, err, blocked = await _governed_read(
            dispatcher, "list_airflow_dag_runs", {"dag_id": dag, "state": "failed"})
        runs = data.get("dag_runs") or []
        run_rec = runs[0] if runs else {}
    if blocked:
        return None, err, True
    if not run_rec.get("dag_run_id"):
        return None, "no failed run found for this DAG", False
    run_id = run_rec["dag_run_id"]

    ok, data, err, blocked = await _governed_read(
        dispatcher, "get_airflow_task_instances", {"dag_id": dag, "dag_run_id": run_id})
    if blocked:
        return None, err, True
    tis = data.get("task_instances") or []
    failed = next((t for t in tis
                   if str(t.get("state", "")).lower() in _AIRFLOW_FAILED), None)
    logs = ""
    if failed is not None:
        _, ldata, _err, lblocked = await _governed_read(
            dispatcher, "get_airflow_task_logs",
            {"dag_id": dag, "dag_run_id": run_id, "task_id": failed.get("task_id")})
        if lblocked:
            return None, _err, True
        logs = ldata.get("content") or ""

    finding = locate_airflow_failure(dag_id=dag, run=run_rec, task_instances=tis, logs=logs)
    return finding, ("" if finding else "run failed but no task is in a failed state"), False


async def explain_failure(
    *,
    source: str,
    dispatcher: Any = None,
    lineage: Any = None,
    dag: str | None = None,
    run: str | None = None,
    dbt_project_dir: str | None = None,
    apply: bool = False,
) -> FailureExplanation:
    """Locate, correlate, and explain the most recent failure for ``source``."""
    explanation = FailureExplanation(source=source)

    if source == "dbt":
        results = read_dbt_run_results(dbt_project_dir or ".")
        if results is None:
            explanation.error = "no dbt run_results.json found (run dbt first)"
            return explanation
        finding = locate_dbt_failure(results)
        if finding is None:
            explanation.error = "no failed dbt nodes in the latest run"
            return explanation
    elif source == "airflow":
        if dispatcher is None:
            explanation.error = "airflow inspection needs a governed dispatcher"
            return explanation
        finding, err, blocked = await _locate_airflow(dispatcher, dag, run)
        if finding is None:
            explanation.error = err or "no failure located"
            explanation.blocked = blocked
            return explanation
    else:
        explanation.error = f"unsupported source: {source!r} (try dbt or airflow)"
        return explanation

    explanation.finding = finding
    consumers = correlate(finding, lineage)
    explanation.downstream = [n.to_dict() for n in consumers]
    explanation.root_cause = root_cause_summary(finding, consumers)
    explanation.proposed_fix = propose_fix(finding)

    if apply and explanation.proposed_fix is not None and dispatcher is not None:
        await _apply_fix(dispatcher, explanation.proposed_fix)

    return explanation


async def _apply_fix(dispatcher: Any, fix: ProposedFix) -> None:
    """Route the proposed fix through the governed dispatcher.

    The dispatcher's governor classifies the action, asks for approval, runs it,
    and verifies its post-conditions — exactly like any other action. A denial
    leaves ``applied`` False.
    """
    res = await dispatcher.execute(fix.tool_name, dict(fix.args))
    fix.status = getattr(getattr(res, "status", None), "value", None)
    fix.applied = bool(res.success)
    fix.error = res.error
    decision = (res.metadata or {}).get("governance")
    if isinstance(decision, dict):
        fix.governance = [decision]
