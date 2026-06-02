"""Governance (𝒢) — the reliability keystone.

Every state-changing action is gated through a pipeline:

    classifier (blast radius) -> policy engine (tier -> decision)
        -> permissions (least-privilege scope) -> rollback (verified undo plan)
        -> [human confirm/approve] -> execute -> audit (append-only ledger)

The :class:`~governance.governor.Governor` wires these together and is plugged
into the one dispatch path (``connectors.dispatcher.Dispatcher``) so the tool
tier *and* the code-execution sandbox are governed identically — the sandbox is
not a governance bypass.
"""

from governance.classifier import (
    ActionClassifier,
    Classification,
    Tier,
    classify_sql,
    detect_prod,
)
from governance.command_classifier import (
    CommandClassifier,
    CommandVerdict,
    classify_command,
)
from governance.policy_engine import (
    PolicyDecision,
    PolicyEngine,
    PolicyResult,
    load_policy_config,
)
from governance.permissions import (
    PermissionError as ScopeViolation,
    PermissionRegistry,
    Scope,
)
from governance.audit import AuditLedger, AuditEvent
from governance.rollback import RollbackPlan, RollbackStrategist
from governance.shadow import ShadowExecutor, ShadowResult
from governance.governor import Governor, GovernanceDecision, ApprovalRequest

__all__ = [
    "ActionClassifier", "Classification", "Tier", "classify_sql", "detect_prod",
    "CommandClassifier", "CommandVerdict", "classify_command",
    "PolicyDecision", "PolicyEngine", "PolicyResult", "load_policy_config",
    "PermissionRegistry", "Scope", "ScopeViolation",
    "AuditLedger", "AuditEvent",
    "RollbackPlan", "RollbackStrategist",
    "ShadowExecutor", "ShadowResult",
    "Governor", "GovernanceDecision", "ApprovalRequest",
]
