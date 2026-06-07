"""Plan → Act → Observe → Verify loop + self-correction (𝒪) — 6.1 & 6.4.

This is the explicit replacement for the opaque ``while iteration < max_iterations``
counter. Grounded in MARS-SQL's Think-Act-Observe plus a **mandatory Verify**, the
controller drives a :class:`~core.planner.TaskDAG` one ready node at a time:

    propose → (approval gate) → act → observe → verify → decide

`decide` is one of three, never an implicit "looks done":

* **advance** — the node's post-conditions verified; mark it completed.
* **self-correct** (6.4) — a post-condition failed or the step errored; run a
  *bounded, feedback-driven* correction. The retry is fed the **actual
  environmental feedback** (the real error / EXPLAIN / failing dbt test), not a
  blind re-run, and escalates the model tier (weak→strong) via the router. This
  replaces ``system_message.md``'s old "do NOT retry, ask the user" rule with
  *informed* retry under a budget.
* **escalate** — the correction budget is exhausted (or an approval was
  withheld); the node is surfaced to a human with the full trail, never silently
  looped.

The **Verify step is not optional and not model-judged when the environment can
answer** — the executor runs the real op through the /5 dispatcher (which
already gates on post-conditions + governance), and the loop additionally checks
the node's declared success criteria.

Resumability: a node that hits an irreversible-action gate **pauses its branch**
without losing completed nodes; calling :meth:`run_dag` again after approval
resumes exactly where it stopped.

Offline-safe + decoupled: the loop never imports a connector or the LLM. It is
parameterized by an ``executor`` (how a step actually runs) and an optional
``verifier`` (how a step is checked), so it is fully testable with fakes.
"""

from __future__ import annotations

from core.logging_setup import get_logger

log = get_logger(__name__)

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from collections.abc import Awaitable, Callable

from core.planner import Subtask, TaskDAG, NodeStatus


# ---------------------------------------------------------------------------
# Step I/O contracts
# ---------------------------------------------------------------------------
@dataclass
class StepContext:
    """What the executor is told about *this attempt* of a node.

    On the first attempt ``feedback`` is None. On a correction attempt it carries
    the environmental feedback from the prior failure, and ``model`` is the
    strong-tier model id the router escalated to.
    """

    attempt: int = 1
    feedback: str | None = None
    after_failed_verification: bool = False
    model: str | None = None
    model_tier: str | None = None


@dataclass
class StepResult:
    """What the executor returns after running one node."""

    success: bool
    output: Any = None
    error: str | None = None
    # Environmental feedback to drive an informed retry: the real error text, an
    # EXPLAIN plan, a failing dbt test — anything that makes the next attempt
    # converge instead of thrash. Falls back to ``error`` when unset.
    feedback: str | None = None


# An executor runs a node for one attempt; a verifier checks the result against
# the node's success criteria (returns (passed, detail)).
Executor = Callable[[Subtask, StepContext], Awaitable[StepResult]]
Verifier = Callable[[Subtask, StepResult], Awaitable[tuple[bool, str]]]


@dataclass
class StepOutcome:
    node_id: str
    status: str                 # advanced | escalated | paused
    attempts: int
    verified: bool
    detail: str
    corrections: list[str] = field(default_factory=list)


@dataclass
class OrchestrationResult:
    goal: str
    done: bool
    completed: list[str] = field(default_factory=list)
    escalated: list[str] = field(default_factory=list)
    paused: list[str] = field(default_factory=list)
    outcomes: list[StepOutcome] = field(default_factory=list)

    def summary(self) -> str:
        bits = [f"{len(self.completed)} completed"]
        if self.escalated:
            bits.append(f"{len(self.escalated)} escalated to human")
        if self.paused:
            bits.append(f"{len(self.paused)} paused for approval")
        return ", ".join(bits)


class CorrectionAuditLog:
    """Append-only JSONL of every self-correction (logged + surfaced, per 6.4)."""

    def __init__(self, path: str = ".dacli/corrections.jsonl"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, record: dict) -> None:
        record = {"ts": datetime.now().isoformat(), **record}
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def recent(self, n: int = 20) -> list[dict]:
        if not self.path.exists():
            return []
        with open(self.path, encoding="utf-8") as f:
            lines = f.readlines()
        out = []
        for line in lines[-n:]:
            try:
                out.append(json.loads(line))
            except Exception:
                continue
        return out


class PlanActObserveVerify:
    """The controller. Executes a DAG with verify-in-loop + bounded self-correction."""

    def __init__(
        self,
        executor: Executor,
        *,
        verifier: Verifier | None = None,
        model_router: Any = None,
        on_approval: Callable[[Any], bool] | None = None,
        correction_log: CorrectionAuditLog | None = None,
        correction_budget: int = 2,
        require_approval: bool = True,
        on_event: Callable[[str], None] | None = None,
    ):
        self._executor = executor
        self._verifier = verifier
        self._router = model_router
        self._on_approval = on_approval
        self._corrections = correction_log
        self.correction_budget = correction_budget
        self.require_approval = require_approval
        self._on_event = on_event

    # ------------------------------------------------------------------
    def _emit(self, msg: str) -> None:
        if self._on_event:
            try:
                self._on_event(msg)
            except Exception:
                log.debug("on_event callback failed", exc_info=True)

    async def _verify(self, node: Subtask, result: StepResult) -> tuple[bool, str]:
        """Mandatory verify. Defaults to the step's own success when no verifier
        is injected; a real verifier asks the environment (never the model)."""
        if not result.success:
            return False, result.error or "step did not succeed"
        if self._verifier is None:
            return True, "step succeeded (no node-level verifier configured)"
        try:
            return await self._verifier(node, result)
        except Exception as e:  # a verifier that explodes is a failed verify
            return False, f"verifier raised: {e}"

    def _approve(self, node: Subtask) -> bool:
        # Irreversible-action gate: without an approval callback, default to
        # *withhold* (pause), never auto-approve a destructive step.
        if self._on_approval is None:
            return False
        try:
            return bool(self._on_approval(node))
        except Exception:
            return False

    def _escalated_model(self, node: Subtask) -> tuple[str | None, str | None]:
        """Ask the router to escalate weak→strong for a correction attempt.

        Returns (model_id, tier). Also *logs* the escalation in the router's
        audit log — this is the "escalates on a failed verification, visible in
        the audit log" exit criterion.
        """
        if self._router is None:
            return None, None
        try:
            choice = self._router.choose(
                "diagnosis", after_failed_verification=True,
            )
            return choice.model, choice.tier
        except Exception:
            return None, None

    # ------------------------------------------------------------------
    async def run_node(self, node: Subtask) -> StepOutcome:
        """Run one node through plan→act→observe→verify with self-correction."""
        # --- approval gate (irreversible actions pause, resumably) ---
        if node.irreversible and self.require_approval and not self._approve(node):
            node.status = NodeStatus.PAUSED
            self._emit(f"[{node.id}] paused — irreversible action needs approval")
            return StepOutcome(node.id, "paused", node.attempts, False,
                               "awaiting approval for irreversible action")

        node.status = NodeStatus.RUNNING
        corrections: list[str] = []
        feedback: str | None = None
        # total attempts = 1 initial + correction_budget informed retries
        for attempt in range(1, self.correction_budget + 2):
            node.attempts = attempt
            ctx = StepContext(attempt=attempt, feedback=feedback,
                              after_failed_verification=attempt > 1)
            if attempt > 1:
                model, tier = self._escalated_model(node)
                ctx.model, ctx.model_tier = model, tier

            self._emit(f"[{node.id}] act (attempt {attempt})")
            try:
                result = await self._executor(node, ctx)
            except Exception as e:
                result = StepResult(success=False, error=str(e), feedback=str(e))

            verified, detail = await self._verify(node, result)
            if verified:
                node.status = NodeStatus.COMPLETED
                node.result = result.output
                node.error = None
                self._emit(f"[{node.id}] verified → advance")
                return StepOutcome(node.id, "advanced", attempt, True, detail, corrections)

            # --- failed verify → informed self-correction (bounded) ---
            feedback = result.feedback or detail or result.error or "unknown failure"
            node.error = detail
            if attempt <= self.correction_budget:
                corrections.append(feedback)
                if self._corrections is not None:
                    self._corrections.log({
                        "node": node.id, "attempt": attempt,
                        "failure": detail, "feedback": feedback,
                        "next_tier": "strong",
                    })
                self._emit(f"[{node.id}] verify FAILED: {detail} → self-correct (informed retry)")
                continue

            # budget exhausted → escalate to human with the trail (don't loop)
            node.status = NodeStatus.FAILED
            self._emit(f"[{node.id}] correction budget exhausted → escalate to human")
            return StepOutcome(node.id, "escalated", attempt, False, detail, corrections)

        # Unreachable, but keep the type checker honest.
        node.status = NodeStatus.FAILED
        return StepOutcome(node.id, "escalated", node.attempts, False, "no attempts run", corrections)

    # ------------------------------------------------------------------
    async def run_dag(self, dag: TaskDAG) -> OrchestrationResult:
        """Drive the whole DAG to completion/escalation/pause.

        Resumable: any node left PAUSED by a prior run is re-armed at entry, so a
        second call (after approval) continues without redoing completed nodes.
        Independent ready nodes are executed in dependency order; a fan-out node
        is still one node here — sub-agent parallelism lives in 6.5.
        """
        dag.validate()
        # Re-arm paused branches for this (resumed) pass.
        for node in dag.paused():
            node.status = NodeStatus.PENDING

        outcomes: list[StepOutcome] = []
        while True:
            ready = dag.ready()
            if not ready:
                break
            progressed = False
            for node in ready:
                outcome = await self.run_node(node)
                outcomes.append(outcome)
                if outcome.status in ("advanced", "escalated"):
                    progressed = True
            if not progressed:
                # Everything ready paused — stop; the caller approves & resumes.
                break

        completed = [n.id for n in dag.nodes if n.status == NodeStatus.COMPLETED]
        escalated = [n.id for n in dag.nodes if n.status == NodeStatus.FAILED]
        paused = [n.id for n in dag.nodes if n.status == NodeStatus.PAUSED]
        return OrchestrationResult(
            goal=dag.goal,
            done=dag.is_complete(),
            completed=completed,
            escalated=escalated,
            paused=paused,
            outcomes=outcomes,
        )
