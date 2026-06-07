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

from dacli.governance.classifier import (
    ActionClassifier,
    Classification,
    Tier,
    classify_sql,
    detect_prod,
)
from dacli.governance.command_classifier import (
    CommandClassifier,
    CommandVerdict,
    classify_command,
)
from dacli.governance.policy_engine import (
    PolicyDecision,
    PolicyEngine,
    PolicyResult,
    load_policy_config,
)
from dacli.governance.permissions import (
    PermissionError as ScopeViolation,
    PermissionRegistry,
    Scope,
)
from dacli.governance.audit import AuditLedger, AuditEvent
from dacli.governance.rollback import RollbackPlan, RollbackStrategist
from dacli.governance.shadow import ShadowExecutor, ShadowResult
from dacli.governance.governor import Governor, GovernanceDecision, ApprovalRequest

__all__ = [
    "ActionClassifier",
    "ApprovalRequest",
    "AuditEvent",
    "AuditLedger",
    "Classification",
    "CommandClassifier",
    "CommandVerdict",
    "GovernanceDecision",
    "Governor",
    "PermissionRegistry",
    "PolicyDecision",
    "PolicyEngine",
    "PolicyResult",
    "RollbackPlan",
    "RollbackStrategist",
    "Scope",
    "ScopeViolation",
    "ShadowExecutor",
    "ShadowResult",
    "Tier",
    "classify_command",
    "classify_sql",
    "detect_prod",
    "load_policy_config",
]
