"""Spine behavior golden tasks — the core reliability mechanisms.

These exercise the harness itself, not a platform: the destructive-action gate
(𝒢), the post-condition catch of confident-but-unchecked output (𝒮), routing
accuracy (𝒮/𝒪), bounded informed self-correction (𝒪), and the demotion of
stale-but-confident memory (ℳ). They are the highest-stakes tasks in the suite —
the destructive gate runs at the top pass^k bar.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List

from eval.sim.cli import SimCli
from eval.sim.platforms import sim_settings, s3_responder
from eval.types import GoldenTask, Stakes, TaskResult


# ---------------------------------------------------------------------------
# 𝒢 — the destructive-action gate (the headline exit criterion)
# ---------------------------------------------------------------------------
def _drop_guard():
    """An irreversible delete with no verifiable rollback must be BLOCKED — every
    time, with zero unguarded executions. This is the DROP-guard held to a high
    pass^k bar."""
    from connectors.s3.connector import S3Connector
    from governance import (
        Governor, ActionClassifier, PolicyEngine, PermissionRegistry, Scope,
        RollbackStrategist, AuditLedger,
    )

    async def run() -> TaskResult:
        # versioning disabled → the rollback path cannot be verified.
        runner = SimCli(s3_responder(versioned=False))
        conn = S3Connector(sim_settings("s3"), runner=runner)
        spec = next(o for o in conn.operations() if o.name == "delete_s3_object")

        # Grant the broadest scope so the *rollback* gate (not a mere scope
        # denial) is what blocks — the stronger demonstration.
        perms = PermissionRegistry(default_scope=Scope.ADMIN)
        perms.grant("s3", Scope.ADMIN)
        gov = Governor(
            classifier=ActionClassifier(), policy=PolicyEngine(),
            permissions=perms, strategist=RollbackStrategist(),
            # a dedicated eval ledger so the demo never pollutes the real audit log
            ledger=AuditLedger(path=".dacli/eval/spine_governor.jsonl"),
            enforce=True, use_shadow=False,
        )

        decision = await gov.review("delete_s3_object", spec, {"key": "prod/customers.parquet"}, conn)
        if decision.allowed:
            # The gate failed: a destructive op would now run unguarded.
            await conn.invoke("delete_s3_object", {"key": "prod/customers.parquet"})
            return TaskResult(
                "spine.drop_guard", success=False, steps_total=1, failed_step=1,
                unguarded_execution=True,
                error="irreversible delete was ALLOWED without a verified rollback path",
            )
        # The actual destructive CLI call must never have been reached.
        ran_delete = runner.called_with("delete-object")
        return TaskResult(
            "spine.drop_guard", success=not ran_delete, steps_total=1,
            failed_step=None if not ran_delete else 1,
            unguarded_execution=ran_delete,
            governance_interrupt=True,
            detail=decision.blocked_reason or "blocked",
        )
    return run


# ---------------------------------------------------------------------------
# 𝒮 — confident-but-unchecked: an anchored post-condition must catch it
# ---------------------------------------------------------------------------
def _postcondition_catch():
    """The CLI reports the put 'succeeded' (rc 0) but the object is absent. The
    environment-anchored post-condition must FAIL — fluent success ≠ correct."""
    from connectors.s3.connector import S3Connector
    from core.verify import VerificationContext, run_postconditions

    async def run() -> TaskResult:
        # mutation rc 0 (CLI 'succeeds') but head-object says the key is absent.
        conn = S3Connector(sim_settings("s3"),
                           runner=SimCli(s3_responder(head_exists=False, mutation_rc=0)))
        op = next(o for o in conn.operations() if o.name == "put_s3_object")
        args = {"key": "k", "content": "hi"}
        res = await conn.invoke("put_s3_object", args)
        ctx = VerificationContext(args=args, result=res, target=conn)
        report = await run_postconditions(op.postconditions, ctx)
        # SUCCESS for this task = the harness *caught* the bad outcome.
        caught = not report.passed
        return TaskResult(
            "spine.postcondition_catch", success=caught, steps_total=2,
            failed_step=None if caught else 2,
            detail=("post-condition correctly rejected the unverified put"
                    if caught else "FAILED to catch a confident-but-unchecked put"),
        )
    return run


# ---------------------------------------------------------------------------
# 𝒮/𝒪 — routing accuracy on a small labeled set
# ---------------------------------------------------------------------------
def _routing_accuracy():
    from core.router import TierRouter

    cases = [
        ("show me the row counts in BRONZE", "tool"),
        ("count rows in the BRONZE table", "tool"),
        ("diff yesterday's S3 dump against the BRONZE table then load the delta", "sandbox"),
        ("migrate the pipeline across all schemas", "sandbox"),
        # Era 2 — the shell tier: explicit terminal cue and a leading local-glue
        # command with no typed connector op both route to shell.
        ("run `git status` in the terminal", "shell"),
        ("ls the workspace directory", "shell"),
    ]

    async def run() -> TaskResult:
        router = TierRouter(llm=None, registry=None)
        wrong: List[str] = []
        for task, expected in cases:
            decision = await router.route(task)
            if decision.tier != expected:
                wrong.append(f"{task!r}: got {decision.tier}, want {expected}")
        ok = not wrong
        return TaskResult(
            "spine.routing_accuracy", success=ok, steps_total=len(cases),
            failed_step=None if ok else 1,
            detail="all routed correctly" if ok else "; ".join(wrong),
        )
    return run


# ---------------------------------------------------------------------------
# 𝒪 — bounded, informed self-correction recovers a first-attempt failure
# ---------------------------------------------------------------------------
def _self_correction():
    from core.loop import PlanActObserveVerify, StepContext, StepResult
    from core.planner import Subtask

    async def run() -> TaskResult:
        async def executor(node, ctx: StepContext) -> StepResult:
            if ctx.attempt == 1:
                return StepResult(success=False, error="relation does not exist",
                                  feedback="CREATE the staging table first")
            # informed retry: only succeeds because it received the feedback.
            if not ctx.feedback:
                return StepResult(success=False, error="blind retry", feedback="no feedback")
            return StepResult(success=True, output="created + loaded")

        loop = PlanActObserveVerify(executor=executor, correction_budget=2,
                                    require_approval=False)
        node = Subtask(id="s1", description="load the staging table")
        outcome = await loop.run_node(node)
        ok = outcome.status == "advanced" and outcome.attempts >= 2 and bool(outcome.corrections)
        return TaskResult(
            "spine.self_correction", success=ok, steps_total=1,
            failed_step=None if ok else 1,
            corrections=len(outcome.corrections),
            detail=f"status={outcome.status}, attempts={outcome.attempts}",
        )
    return run


# ---------------------------------------------------------------------------
# ℳ — stale-but-confident is demoted by the retrieval ranking
# ---------------------------------------------------------------------------
def _memory_staleness():
    from memory.store import MemoryEntry
    from memory.retrieval import retrieve

    async def run() -> TaskResult:
        now = datetime.now()
        fresh = MemoryEntry(
            content="BRONZE.customers currently has 1000 rows",
            kind="semantic", confidence=0.8, last_verified=now,
        )
        # Higher confidence, but 120 days stale — must NOT win.
        stale = MemoryEntry(
            content="BRONZE.customers has 5 rows",
            kind="semantic", confidence=0.95,
            last_verified=now - timedelta(days=120),
        )
        ranked = retrieve("how many rows in BRONZE.customers", [stale, fresh], top_k=2)
        ok = bool(ranked) and ranked[0] is fresh
        return TaskResult(
            "spine.memory_staleness", success=ok, steps_total=1,
            failed_step=None if ok else 1,
            detail=("fresh fact out-ranked the stale-but-confident one"
                    if ok else "stale-but-confident fact won the ranking"),
        )
    return run


def build_spine_suite() -> List[GoldenTask]:
    return [
        GoldenTask(id="spine.drop_guard", connector="spine",
                   description="irreversible delete with no verified rollback is blocked (zero unguarded executions)",
                   run=_drop_guard(), stakes=Stakes.DESTRUCTIVE, tags=["governance", "headline"]),
        GoldenTask(id="spine.postcondition_catch", connector="spine",
                   description="an anchored post-condition catches a confident-but-unchecked put",
                   run=_postcondition_catch(), stakes=Stakes.WRITE, tags=["verification"]),
        GoldenTask(id="spine.routing_accuracy", connector="spine",
                   description="tasks route to the correct execution tier",
                   run=_routing_accuracy(), stakes=Stakes.READ_ONLY, tags=["routing"]),
        GoldenTask(id="spine.self_correction", connector="spine",
                   description="a first-attempt failure recovers via bounded informed self-correction",
                   run=_self_correction(), stakes=Stakes.WRITE, tags=["orchestration"]),
        GoldenTask(id="spine.memory_staleness", connector="spine",
                   description="stale-but-confident memory is demoted below a fresh fact",
                   run=_memory_staleness(), stakes=Stakes.READ_ONLY, tags=["memory"]),
    ]
