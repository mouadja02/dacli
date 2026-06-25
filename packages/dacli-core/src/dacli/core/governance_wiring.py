"""Governor construction, shared by the old agent and the thin host (M09).

Lifted verbatim from ``DACLI._build_governor`` so the host wires governance and
verify *identically* to the live path it replaces — one definition, no drift.
``env_resolver`` and ``on_cost`` are the two callbacks that were agent methods.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from collections.abc import Callable

from dacli.config.settings import Settings
from dacli.core.logging_setup import get_logger
from dacli.governance import (
    ActionClassifier, AuditLedger, Governor, PermissionRegistry,
    PolicyEngine, RollbackStrategist, Scope, ShadowExecutor,
)
from dacli.governance.policy_engine import load_policy_config

log = get_logger(__name__)


def build_governor(
    settings: Settings,
    *,
    session_id: str,
    on_approval: Callable[[Any], bool] | None,
    env_resolver: Callable[[str, dict, Any], str | None],
    on_cost: Callable[[float], None],
) -> Governor | None:
    gov = getattr(settings, "governance", None)
    if gov is not None and not gov.enabled:
        return None  # explicitly disabled (trusted offline run)

    policy_path = getattr(gov, "policy_path", "config/policy.yaml") if gov else "config/policy.yaml"
    config = load_policy_config(policy_path)
    policy = PolicyEngine(config)

    # Least-privilege: connectors get the configured default scope unless the
    # policy profile grants more. Write/admin is opt-in per connection.
    try:
        default_scope = Scope(getattr(gov, "default_scope", "read_only"))
    except Exception:
        default_scope = Scope.READ_ONLY
    permissions = PermissionRegistry.from_policy_config(config, default_scope=default_scope)
    # Built-in harness connectors (system/skills/sandbox) are not external
    # platforms — least-privilege scoping targets platform blast radius, and
    # their sub-actions (e.g. each governed sdk.run inside the sandbox) are
    # gated independently. Exempt them so the harness itself isn't crippled.
    for _builtin in ("system", "skills", "sandbox"):
        permissions.grant(_builtin, Scope.ADMIN)
    # The shell tier, by contrast, IS scoped by least privilege — that is the
    # whole point of a governed terminal. Its ceiling is the configured
    # ``terminal.scope`` (default 'write'), so an `rm file` (risky) or
    # `rm -rf` (irreversible) is permission-denied unless the operator widened
    # the shell scope. The *command's* tier (from the command classifier), not
    # the op's declared risk, is what the check sees.
    _term = getattr(settings, "terminal", None)
    try:
        _shell_scope = Scope(str(getattr(_term, "scope", "write")).strip().lower())
    except Exception:
        _shell_scope = Scope.WRITE
    permissions.grant("shell", _shell_scope)

    state_dir = str(Path(settings.agent.state_path).parent)
    audit_path = (getattr(gov, "audit_path", None) or f"{state_dir}/audit.jsonl") if gov else f"{state_dir}/audit.jsonl"
    ledger = AuditLedger(path=audit_path)

    # P12 lineage: best-effort blast-radius evidence (dbt + catalog + persisted
    # store). Build failures are non-fatal — governance runs without it.
    lineage = None
    try:
        from dacli.memory.graph.lineage import build_project_lineage
        lineage = build_project_lineage(settings)
    except Exception:
        log.debug("lineage store unavailable", exc_info=True)

    # The classifier embeds the shell command classifier; give it the
    # terminal's egress posture so a `curl`/`wget` in a shell command is
    # judged against the same allowlist a connector fetch would be.
    return Governor(
        classifier=ActionClassifier(
            prod_markers=policy.prod_markers or None,
            network=getattr(_term, "network", "allowlist"),
            egress_allowlist=list(getattr(_term, "egress_allowlist", []) or []),
        ),
        policy=policy,
        permissions=permissions,
        strategist=RollbackStrategist(),
        shadow_executor=ShadowExecutor(),
        ledger=ledger,
        session_id=session_id,
        approval_fn=on_approval,
        env_resolver=env_resolver,
        enforce=True,
        use_shadow=bool(getattr(gov, "shadow_execution", True)) if gov else True,
        cost_confirm_usd=getattr(gov, "cost_confirm_usd", None) if gov else None,
        on_cost=on_cost,
        lineage=lineage,
    )
