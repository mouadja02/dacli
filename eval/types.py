"""Shared eval contracts: a runnable :class:`GoldenTask` and its :class:`TaskResult`.

A golden task is a *machine-verifiable* unit of behavior: it carries an async
``run`` that exercises the real harness (a connector op + its environment-anchored
post-conditions, or a spine behavior like the destructive-action gate) against a
deterministic simulated environment, and returns a structured outcome.

The outcome records more than a pass/fail bit — it captures the **failure shape**
(which step failed) so regression detection can catch *earlier-failure recurrence*
(harness-benchmarking: track the shape of failures, not just the count), plus the
process signals the dashboard surfaces (tokens, latency, escalation, corrections,
governance interrupts).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from collections.abc import Awaitable, Callable


class Stakes(str, Enum):
    """How costly a wrong answer is — sets the pass^k bar (k tiered by stakes).

    Destructive/governance paths are held to a far higher pass^k bar than
    read-only tasks (the cheap sim suite runs often; the expensive high-k runs
    are reserved for where a 1-in-20 flake is catastrophic).
    """

    READ_ONLY = "read_only"
    WRITE = "write"
    DESTRUCTIVE = "destructive"


# Default rollout counts per stakes tier. High k where a flake is catastrophic
# (a 95%-reliable DROP-guard is a disaster waiting for its 1-in-20), low k for
# read-only. A harness ``k_scale`` can dial these down for fast CI.
_DEFAULT_K = {
    Stakes.READ_ONLY: 3,
    Stakes.WRITE: 5,
    Stakes.DESTRUCTIVE: 10,
}


def default_k_for(stakes: Stakes) -> int:
    return _DEFAULT_K.get(stakes, 3)


@dataclass
class TaskResult:
    """The outcome of one rollout of a golden task."""

    task_id: str
    success: bool
    steps_total: int = 1
    #: 1-based index of the first failing step; ``None`` when the task passed.
    #: This is what makes *earlier-failure recurrence* detectable.
    failed_step: int | None = None
    error: str = ""
    detail: str = ""
    # process metrics (harness-benchmarking Tier-2): more tokens ≠ better.
    tokens: int = 0
    latency_ms: float = 0.0
    escalated: bool = False          # surfaced to a human / strong tier
    corrections: int = 0             # bounded self-correction attempts used
    # governance signals: an action was gated (interrupt), or — the failure we
    # most fear — a destructive action ran with no gate at all.
    governance_interrupt: bool = False
    unguarded_execution: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "success": self.success,
            "steps_total": self.steps_total,
            "failed_step": self.failed_step,
            "error": self.error,
            "detail": self.detail,
            "tokens": self.tokens,
            "latency_ms": round(self.latency_ms, 3),
            "escalated": self.escalated,
            "corrections": self.corrections,
            "governance_interrupt": self.governance_interrupt,
            "unguarded_execution": self.unguarded_execution,
        }

    @classmethod
    def from_dict(cls, d: dict[str, object]) -> TaskResult:
        return cls(
            task_id=str(d.get("task_id", "")),
            success=bool(d.get("success", False)),
            steps_total=int(d.get("steps_total", 1) or 1),
            failed_step=(int(d["failed_step"]) if d.get("failed_step") is not None else None),
            error=str(d.get("error", "")),
            detail=str(d.get("detail", "")),
            tokens=int(d.get("tokens", 0) or 0),
            latency_ms=float(d.get("latency_ms", 0.0) or 0.0),
            escalated=bool(d.get("escalated", False)),
            corrections=int(d.get("corrections", 0) or 0),
            governance_interrupt=bool(d.get("governance_interrupt", False)),
            unguarded_execution=bool(d.get("unguarded_execution", False)),
        )


@dataclass
class GoldenTask:
    """A versioned, machine-verifiable task with an environment-anchored outcome.

    ``run`` is an async, side-effect-free-against-prod factory of a fresh rollout:
    calling it again must execute the behavior again (so pass^k measures real
    repeated rollouts, not a cached result). ``connector`` groups tasks for the
    per-connector dashboard; ``stakes`` sets the pass^k bar.
    """

    id: str
    connector: str
    description: str
    run: Callable[[], Awaitable[TaskResult]]
    stakes: Stakes = Stakes.READ_ONLY
    #: explicit override of the per-stakes default k (None → use the default).
    k: int | None = None
    tags: list[str] = field(default_factory=list)

    def rollouts(self) -> int:
        return self.k if self.k is not None else default_k_for(self.stakes)
