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

import inspect
import uuid
from dataclasses import dataclass, field
from typing import Any
from collections.abc import Callable

from dacli.connectors.base import Risk, ToolResult, ToolStatus
from dacli.core.logging_setup import get_logger

log = get_logger(__name__)
from dacli.governance.classifier import ActionClassifier, Classification, Tier
from dacli.governance.vocab import promote as _promote
from dacli.governance.policy_engine import PolicyDecision, PolicyEngine, PolicyResult
from dacli.governance.permissions import PermissionRegistry, Scope
from dacli.governance.rollback import RollbackPlan, RollbackStrategist
from dacli.governance.shadow import ShadowExecutor, ShadowResult, supports_shadow
from dacli.governance.audit import AuditLedger


# A human approval callback receives a structured request and returns True to
# proceed. It is synchronous (like the existing on_user_input_needed hook).
ApprovalFn = Callable[["ApprovalRequest"], bool]
# Resolves the policy/classification environment for a connector+action.
EnvResolver = Callable[[str, dict[str, Any], Any], str | None]


@dataclass
class ApprovalRequest:
    """What the human is shown before a risky/irreversible action runs."""

    tool_name: str
    tier: Tier
    classification: Classification
    policy: PolicyResult
    rollback_plan: RollbackPlan
    args: dict[str, Any]
    dry_run_preview: str | None = None
    shadow: ShadowResult | None = None
    cost_estimate: dict[str, Any] | None = None

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
        if self.cost_estimate:
            lines.append(f"Est. cost   : {_format_cost_estimate(self.cost_estimate)}")
        if self.dry_run_preview:
            lines.append(f"Dry-run     : {self.dry_run_preview}")
        if self.shadow and self.shadow.ran:
            lines.append(f"Shadow      : {self.shadow.summary()}")
        return "\n".join(lines)


def _format_cost_estimate(estimate: dict[str, Any]) -> str:
    bits = []
    if estimate.get("bytes") is not None:
        bits.append(f"{estimate['bytes']:,} bytes scanned")
    if estimate.get("credits") is not None:
        bits.append(f"{estimate['credits']} credits")
    if estimate.get("usd") is not None:
        bits.append(f"≈ ${estimate['usd']:,.2f}")
    return "  ·  ".join(bits) or "(no detail)"


@dataclass
class GovernanceDecision:
    allowed: bool
    decision_id: str
    classification: Classification
    policy: PolicyResult
    rollback_plan: RollbackPlan | None = None
    shadow: ShadowResult | None = None
    blocked_reason: str | None = None
    short_circuit: ToolResult | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class Governor:
    def __init__(
        self,
        *,
        classifier: ActionClassifier | None = None,
        policy: PolicyEngine | None = None,
        permissions: PermissionRegistry | None = None,
        strategist: RollbackStrategist | None = None,
        shadow_executor: ShadowExecutor | None = None,
        ledger: AuditLedger | None = None,
        session_id: str = "",
        approval_fn: ApprovalFn | None = None,
        dry_run_fn: Callable[[Any, str, dict[str, Any]], str | None] | None = None,
        env_resolver: EnvResolver | None = None,
        enforce: bool = True,
        use_shadow: bool = True,
        cost_confirm_usd: float | None = None,
        on_cost: Callable[[float], None] | None = None,
        lineage: Any = None,
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
        #: F-4 cost gate. When set, a connector-provided estimate above this
        # many USD raises the effective tier so the confirm path fires. None
        # (the default) never consults the estimator — zero behaviour change.
        self.cost_confirm_usd = cost_confirm_usd
        #: Slice-C cost advisor sink. When a per-action USD estimate is computed
        # (only happens while the cost gate is configured), it is reported here
        # so the session's running warehouse spend can surface in the toolbar.
        self._on_cost = on_cost
        #: P12 lineage store. When set, dropping/replacing an object with known
        # downstream consumers names them and raises the tier. Best-effort and
        # fail-soft: absence of lineage never blocks and never marks a thing safe.
        self.lineage = lineage

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _audit(self, kind: str, tool_name: str, decision_id: str, actor: str,
               tier: str | None, summary: str, **detail: Any) -> None:
        self.ledger.log(
            kind, tool_name, session_id=self.session_id, decision_id=decision_id,
            actor=actor, tier=tier, summary=summary, **detail,
        )

    @staticmethod
    def _shell_command(spec: Any, connector_id: str, args: dict[str, Any]) -> str | None:
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

    #: How many consumers to name inline before "+N more".
    _LINEAGE_CITE_LIMIT = 5

    def _apply_lineage(self, tool_name: str, args: dict[str, Any],
                       classification: Classification, decision_id: str, actor: str) -> None:
        if self.lineage is None:
            return
        try:
            from dacli.memory.graph.lineage import action_targets

            consumers: list[Any] = []
            for target in action_targets(tool_name, args):
                consumers.extend(self.lineage.downstream(target))
            # Dedup across targets by (name, kind).
            seen: set[tuple[str, str]] = set()
            unique = []
            for c in consumers:
                key = (c.name.upper(), c.kind)
                if key not in seen:
                    seen.add(key)
                    unique.append(c)
            if not unique:
                return

            named = ", ".join(c.display() for c in unique[:self._LINEAGE_CITE_LIMIT])
            more = len(unique) - self._LINEAGE_CITE_LIMIT
            if more > 0:
                named += f", +{more} more"
            tier = classification.tier
            reason = (f"lineage: {len(unique)} downstream consumer(s) read this object "
                      f"({named}) → wider blast radius")
            if tier in (Tier.WRITE, Tier.RISKY):
                promoted = _promote(tier, 1)
                reason += f"; promote {tier.value}→{promoted.value}"
                classification.tier = promoted
            classification.reasons.append(reason)
            self._audit("lineage", tool_name, decision_id, actor,
                        classification.tier.value, reason,
                        consumers=[c.to_dict() for c in unique])
        except Exception:
            log.debug("lineage blast-radius check raised", exc_info=True)

    def _environment(self, connector_id: str, args: dict[str, Any], connector: Any,
                     classification: Classification) -> str | None:
        if self._env_resolver is not None:
            try:
                env = self._env_resolver(connector_id, args, connector)
                if env:
                    return env
            except Exception:
                log.debug("env_resolver raised in _environment", exc_info=True)
        # Fall back to the prod marker the classifier already found.
        return "prod" if classification.is_prod else None

    def _blocked_result(self, tool_name: str, reason: str, status: ToolStatus,
                        decision_id: str, classification: Classification,
                        policy: PolicyResult | None) -> ToolResult:
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
        args: dict[str, Any],
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
        # Guard the resolver exactly like _environment (3.6): a future raising
        # resolver must not throw out of review() and crash the governance spine.
        env_hint_seed = None
        if self._env_resolver:
            try:
                env_hint_seed = self._env_resolver(connector_id, args, connector)
            except Exception:
                log.debug("env_resolver raised while seeding env hint", exc_info=True)
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

        # 1b. Cost gate (F-4): when configured, an estimate above the
        # threshold raises the effective tier so the confirm path fires —
        # cost is a blast-radius dimension. Best-effort: a failing estimator
        # never breaks review (the action is then judged on tier alone).
        cost_estimate = await self._estimate_cost(spec, args, connector)
        usd = (cost_estimate or {}).get("usd")
        if usd is not None and self._on_cost is not None:
            try:
                self._on_cost(float(usd))
            except Exception:
                log.debug("on_cost sink raised", exc_info=True)
        if usd is not None and self.cost_confirm_usd is not None and usd > self.cost_confirm_usd:
            reason = (f"estimated cost ${usd:,.2f} exceeds the "
                      f"cost_confirm_usd threshold (${self.cost_confirm_usd:,.2f}) → confirm")
            if tier in (Tier.SAFE, Tier.WRITE):
                tier = Tier.RISKY
                classification.tier = tier
            classification.reasons.append(reason)
            self._audit("cost", tool_name, decision_id, actor, tier.value,
                        reason, estimate=cost_estimate)

        # 1c. Lineage / blast-radius (P12): dropping or replacing an object with
        # known downstream consumers names them and raises the effective tier —
        # lineage is evidence *for* the classification, not a parallel gate.
        # Fail-soft: a missing store or a raising lookup never breaks review.
        self._apply_lineage(tool_name, args, classification, decision_id, actor)
        tier = classification.tier

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
        shadow: ShadowResult | None = None
        if self.use_shadow and supports_shadow(connector):
            shadow = await self.shadow_executor.run(connector, args)
            self._audit("shadow", tool_name, decision_id, actor, tier.value,
                        shadow.summary(), shadow=shadow.to_dict())

        # 8. Human confirm / approve (fail-closed if no callback wired).
        request = ApprovalRequest(
            tool_name=tool_name, tier=tier, classification=classification,
            policy=policy, rollback_plan=plan, args=args,
            dry_run_preview=preview, shadow=shadow, cost_estimate=cost_estimate,
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
    async def _estimate_cost(self, spec: Any, args: dict[str, Any],
                             connector: Any) -> dict[str, Any] | None:
        """Best-effort cost preview via the connector's optional hook.

        Only consulted when the cost gate is configured, so the default posture
        adds no calls. Any estimator error is swallowed (with a breadcrumb) —
        a side concern never breaks the governance spine.
        """
        if self.cost_confirm_usd is None:
            return None
        hook = getattr(connector, "estimate_cost", None)
        if not callable(hook):
            return None
        op = getattr(spec, "name", None) or ""
        try:
            estimate = hook(op, dict(args or {}))
            if inspect.isawaitable(estimate):
                estimate = await estimate
        except Exception:
            log.debug("estimate_cost hook raised", exc_info=True)
            return None
        return estimate if isinstance(estimate, dict) else None

    def _dry_run(self, connector: Any, tool_name: str, args: dict[str, Any]) -> str | None:
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
