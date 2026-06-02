"""The Governor (𝒢) — the gate every state-changing action passes.

The Governor wires the governance pipeline into one call the dispatcher (and the
sandbox SDK) makes *before* an action runs::

    classify -> permission check -> policy decision
      -> (interrupting?) attach + verify rollback plan
      -> (irreversible & unverified rollback?) BLOCK
      -> (dry-run preview) -> (shadow run on clone -> diff)
      -> human confirm/approve  -> ALLOW or DENY
    ... action executes ...
    -> record outcome + post-condition verdict in the audit ledger

Crucially the **same Governor instance** is shared by the tool tier and the
sandbox, so code-execution is governed identically — *the sandbox is not a
governance bypass*. Every step is written to the append-only audit ledger so a
session is fully reconstructable.

Failure posture is **fail-closed**: if an interrupting decision has no approval
callback wired (e.g. a non-interactive run), the action is denied rather than
silently executed.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from connectors.base import Risk, ToolResult, ToolStatus
from governance.classifier import ActionClassifier, Classification, Tier
from governance.policy_engine import PolicyDecision, PolicyEngine, PolicyResult
from governance.permissions import PermissionRegistry, Scope
from governance.rollback import RollbackPlan, RollbackStrategist
from governance.shadow import ShadowExecutor, ShadowResult, supports_shadow
from governance.audit import AuditLedger


# A human approval callback receives a structured request and returns True to
# proceed. It is synchronous (like the existing on_user_input_needed hook).
ApprovalFn = Callable[["ApprovalRequest"], bool]
# Resolves the policy/classification environment for a connector+action.
EnvResolver = Callable[[str, Dict[str, Any], Any], Optional[str]]


@dataclass
class ApprovalRequest:
    """What the human is shown before a risky/irreversible action runs."""

    tool_name: str
    tier: Tier
    classification: Classification
    policy: PolicyResult
    rollback_plan: RollbackPlan
    args: Dict[str, Any]
    dry_run_preview: Optional[str] = None
    shadow: Optional[ShadowResult] = None

    def describe(self) -> str:
        lines = [
            f"Action      : {self.tool_name}",
            f"Blast radius: {self.tier.value}"
            + (f"  (PROD: {self.classification.prod_marker})" if self.classification.is_prod else ""),
            f"Why         : {'; '.join(self.classification.reasons)}",
            f"Decision    : {self.policy.decision.value}  [{self.policy.source}]",
            f"Rollback    : {self.rollback_plan.strategy}"
            + (f"  (verified: {self.rollback_plan.verify_detail})"
               if self.rollback_plan.primitive not in ("noop", "none") else ""),
        ]
        if self.dry_run_preview:
            lines.append(f"Dry-run     : {self.dry_run_preview}")
        if self.shadow and self.shadow.ran:
            lines.append(f"Shadow      : {self.shadow.summary()}")
        return "\n".join(lines)


@dataclass
class GovernanceDecision:
    allowed: bool
    decision_id: str
    classification: Classification
    policy: PolicyResult
    rollback_plan: Optional[RollbackPlan] = None
    shadow: Optional[ShadowResult] = None
    blocked_reason: Optional[str] = None
    short_circuit: Optional[ToolResult] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class Governor:
    def __init__(
        self,
        *,
        classifier: Optional[ActionClassifier] = None,
        policy: Optional[PolicyEngine] = None,
        permissions: Optional[PermissionRegistry] = None,
        strategist: Optional[RollbackStrategist] = None,
        shadow_executor: Optional[ShadowExecutor] = None,
        ledger: Optional[AuditLedger] = None,
        session_id: str = "",
        approval_fn: Optional[ApprovalFn] = None,
        dry_run_fn: Optional[Callable[[Any, str, Dict[str, Any]], Optional[str]]] = None,
        env_resolver: Optional[EnvResolver] = None,
        enforce: bool = True,
        use_shadow: bool = True,
    ):
        self.classifier = classifier or ActionClassifier(
            prod_markers=(policy.prod_markers if policy else None) or None
        )
        self.policy = policy or PolicyEngine()
        self.permissions = permissions or PermissionRegistry(default_scope=Scope.READ_ONLY)
        self.strategist = strategist or RollbackStrategist()
        self.shadow_executor = shadow_executor or ShadowExecutor()
        self.ledger = ledger or AuditLedger()
        self.session_id = session_id
        self._approval_fn = approval_fn
        self._dry_run_fn = dry_run_fn
        self._env_resolver = env_resolver
        self.enforce = enforce
        self.use_shadow = use_shadow

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _audit(self, kind: str, tool_name: str, decision_id: str, actor: str,
               tier: Optional[str], summary: str, **detail: Any) -> None:
        self.ledger.log(
            kind, tool_name, session_id=self.session_id, decision_id=decision_id,
            actor=actor, tier=tier, summary=summary, **detail,
        )

    @staticmethod
    def _shell_command(spec: Any, connector_id: str, args: Dict[str, Any]) -> Optional[str]:
        """Return the shell command string when this is a shell-tier op, else None.

        Gated on the op being the shell connector (or declaring a ``shell.``
        capability) so no other connector's ``command``-named arg is ever treated
        as a shell command — every existing call passes ``command=None``.
        """
        capability = getattr(spec, "capability", "") or ""
        if connector_id != "shell" and not capability.startswith("shell."):
            return None
        cmd = args.get("command") or args.get("cmd")
        return str(cmd) if cmd else None

    def _environment(self, connector_id: str, args: Dict[str, Any], connector: Any,
                     classification: Classification) -> Optional[str]:
        if self._env_resolver is not None:
            try:
                env = self._env_resolver(connector_id, args, connector)
                if env:
                    return env
            except Exception:
                pass
        # Fall back to the prod marker the classifier already found.
        return "prod" if classification.is_prod else None

    def _blocked_result(self, tool_name: str, reason: str, status: ToolStatus,
                        decision_id: str, classification: Classification,
                        policy: Optional[PolicyResult]) -> ToolResult:
        meta = {
            "governance": {
                "decision_id": decision_id,
                "blocked": True,
                "reason": reason,
                "classification": classification.to_dict(),
                "policy": policy.to_dict() if policy else None,
            }
        }
        return ToolResult(tool_name=tool_name, status=status, error=reason, metadata=meta)

    # ------------------------------------------------------------------
    # main entry: review a pending action
    # ------------------------------------------------------------------
    async def review(
        self,
        tool_name: str,
        spec: Any,
        args: Dict[str, Any],
        connector: Any,
        *,
        actor: str = "agent",
    ) -> GovernanceDecision:
        args = dict(args or {})
        decision_id = uuid.uuid4().hex[:12]
        connector_id = getattr(connector, "name", "") or "unknown"
        declared_risk = getattr(spec, "risk", Risk.SAFE) if spec is not None else Risk.SAFE

        # 1. Classify (blast radius). For the shell tier the *command* — not the
        # op's declared risk — is the truth, so it is parsed by the command
        # classifier (an `ls` is safe even though run_shell_command is write-
        # capable; an `rm -rf` is irreversible).
        env_hint_seed = self._env_resolver(connector_id, args, connector) if self._env_resolver else None
        command = self._shell_command(spec, connector_id, args)
        classification = self.classifier.classify(
            tool_name, args, declared_risk=declared_risk, env_hint=env_hint_seed,
            command=command,
        )
        tier = classification.tier
        environment = self._environment(connector_id, args, connector, classification)
        self._audit("classification", tool_name, decision_id, actor, tier.value,
                    f"tier={tier.value}", classification=classification.to_dict(),
                    environment=environment)

        # 2. Permission / least-privilege scope.
        scope_check = self.permissions.check(connector_id, tier)
        self._audit("permission", tool_name, decision_id, actor, tier.value,
                    scope_check.reason, permission=scope_check.to_dict(), connector=connector_id)
        if not scope_check.allowed and self.enforce:
            reason = scope_check.reason
            return GovernanceDecision(
                allowed=False, decision_id=decision_id, classification=classification,
                policy=PolicyResult(decision=PolicyDecision.DRY_RUN_APPROVE, tier=tier),
                blocked_reason=reason,
                short_circuit=self._blocked_result(
                    tool_name, f"permission denied: {reason}", ToolStatus.DENIED,
                    decision_id, classification, None),
            )

        # 3. Policy decision.
        policy = self.policy.decide(tier, connector_id=connector_id, environment=environment)
        self._audit("policy", tool_name, decision_id, actor, tier.value,
                    f"{policy.decision.value} [{policy.source}]", policy=policy.to_dict())

        # 4. Non-interrupting decisions run straight through (auto / verify).
        if not policy.requires_human:
            return GovernanceDecision(
                allowed=True, decision_id=decision_id, classification=classification,
                policy=policy,
                rollback_plan=self.strategist.plan_for(connector_id, classification),
            )

        # 5. Interrupting: build + (if irreversible) verify a rollback plan.
        plan = self.strategist.plan_for(connector_id, classification)
        if policy.requires_verified_rollback:
            plan = await self.strategist.verify(plan, connector, args)
        self._audit("rollback", tool_name, decision_id, actor, tier.value,
                    plan.strategy, rollback=plan.to_dict())

        # An irreversible action with no *verified* rollback path is refused
        # outright — no human is even asked (exit criterion #1).
        if policy.requires_verified_rollback and not plan.verified:
            reason = (f"blocked: '{tool_name}' is {tier.value} and no rollback path "
                      f"could be verified ({plan.verify_detail or 'no native undo'}).")
            self._audit("block", tool_name, decision_id, actor, tier.value, reason)
            return GovernanceDecision(
                allowed=False, decision_id=decision_id, classification=classification,
                policy=policy, rollback_plan=plan, blocked_reason=reason,
                short_circuit=self._blocked_result(
                    tool_name, reason, ToolStatus.BLOCKED, decision_id, classification, policy),
            )

        # 6. Dry-run preview (best-effort).
        preview = self._dry_run(connector, tool_name, args) if policy.requires_dry_run else None

        # 7. Shadow / clone-first execution for transforms (best-effort).
        shadow: Optional[ShadowResult] = None
        if self.use_shadow and supports_shadow(connector):
            shadow = await self.shadow_executor.run(connector, args)
            self._audit("shadow", tool_name, decision_id, actor, tier.value,
                        shadow.summary(), shadow=shadow.to_dict())

        # 8. Human confirm / approve (fail-closed if no callback wired).
        request = ApprovalRequest(
            tool_name=tool_name, tier=tier, classification=classification,
            policy=policy, rollback_plan=plan, args=args,
            dry_run_preview=preview, shadow=shadow,
        )
        approved = self._ask_human(request)
        self._audit("approval", tool_name, decision_id, "human", tier.value,
                    "approved" if approved else "denied",
                    approved=approved, had_callback=self._approval_fn is not None)

        if not approved:
            if shadow is not None:
                await self.shadow_executor.discard(connector, shadow)
            reason = ("denied by user" if self._approval_fn is not None
                      else "blocked: action requires approval but no approver is available (fail-closed)")
            return GovernanceDecision(
                allowed=False, decision_id=decision_id, classification=classification,
                policy=policy, rollback_plan=plan, shadow=shadow, blocked_reason=reason,
                short_circuit=self._blocked_result(
                    tool_name, reason, ToolStatus.DENIED, decision_id, classification, policy),
            )

        # Approved. The shadow clone (a preview) is discarded; the real action
        # now runs through the normal invoke path and is post-condition verified.
        if shadow is not None:
            await self.shadow_executor.discard(connector, shadow)
        return GovernanceDecision(
            allowed=True, decision_id=decision_id, classification=classification,
            policy=policy, rollback_plan=plan, shadow=shadow,
            metadata={"dry_run_preview": preview},
        )

    # ------------------------------------------------------------------
    # outcome recording (called by the dispatcher after execution)
    # ------------------------------------------------------------------
    def record_outcome(self, decision: GovernanceDecision, result: ToolResult,
                       *, actor: str = "agent") -> None:
        status = getattr(result.status, "value", str(result.status))
        self._audit("execution", result.tool_name, decision.decision_id, actor,
                    decision.classification.tier.value,
                    f"status={status}", status=status, error=result.error)
        verification = (result.metadata or {}).get("verification")
        if verification is not None:
            self._audit("post_condition", result.tool_name, decision.decision_id, actor,
                        decision.classification.tier.value,
                        "passed" if verification.get("passed") else "FAILED",
                        verification=verification)

    def record_memory_write(self, summary: str, **detail: Any) -> None:
        self._audit("memory_write", detail.get("tool_name", "memory"), detail.get("decision_id", ""),
                    "agent", None, summary, **detail)

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------
    def _dry_run(self, connector: Any, tool_name: str, args: Dict[str, Any]) -> Optional[str]:
        if self._dry_run_fn is not None:
            try:
                return self._dry_run_fn(connector, tool_name, args)
            except Exception:
                return None
        hook = getattr(connector, "dry_run", None)
        if callable(hook):
            try:
                return str(hook(tool_name, args))
            except Exception:
                return None
        return "(no dry-run available for this connector)"

    def _ask_human(self, request: ApprovalRequest) -> bool:
        if self._approval_fn is None:
            return False  # fail-closed
        try:
            return bool(self._approval_fn(request))
        except Exception:
            return False  # any error in approval → treat as denial
