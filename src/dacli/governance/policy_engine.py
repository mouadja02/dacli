"""Policy engine (𝒢) — tier → enforcement decision.

Turns a blast-radius :class:`~governance.classifier.Tier` into one of four
enforcement decisions, honoring per-connector / per-environment overrides loaded
from ``config/policy.yaml``:

| Tier | Default decision | Meaning |
|---|---|---|
| ``safe``        | ``auto``            | run immediately |
| ``write`` | ``verify`` | run + mandatory post-condition |
| ``risky``       | ``confirm``         | human confirm + attach rollback plan first |
| ``irreversible``| ``dry_run+approve`` | dry-run, verify rollback path, explicit approval |

The table is config-overridable so a team can tune velocity vs. caution **without
code changes** — e.g. mark a ``dev`` warehouse ``auto`` for writes, or a prod
profile ``confirm`` even for writes. Overrides may only be looked up by the
governor; the engine itself is pure (tier + override map → decision), which keeps
it trivially testable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from dacli.governance.classifier import Tier

DEFAULT_POLICY_PATH = "config/policy.yaml"


class PolicyDecision(str, Enum):
    AUTO = "auto"                       # run immediately
    VERIFY = "verify"                   # run + mandatory post-condition
    CONFIRM = "confirm"                 # human confirm + rollback plan
    DRY_RUN_APPROVE = "dry_run+approve"  # dry-run + verified rollback + approval


# The locked posture (matches the roadmap table). This is the floor: an override
# may *tighten* or *loosen* a specific connector/environment, but absent any
# override these are the decisions.
DEFAULT_DECISIONS: dict[Tier, PolicyDecision] = {
    Tier.SAFE: PolicyDecision.AUTO,
    Tier.WRITE: PolicyDecision.VERIFY,
    Tier.RISKY: PolicyDecision.CONFIRM,
    Tier.IRREVERSIBLE: PolicyDecision.DRY_RUN_APPROVE,
}

# Decisions that interrupt for a human. ``safe``/``write`` flow automatically;
# only ``risky``/``irreversible`` (or an override that maps to these) gate.
INTERRUPTING = {PolicyDecision.CONFIRM, PolicyDecision.DRY_RUN_APPROVE}


@dataclass
class PolicyResult:
    decision: PolicyDecision
    tier: Tier
    source: str = "default"          # "default" | "connector:<id>" | "env:<name>"
    requires_human: bool = False
    requires_rollback: bool = False  # rollback plan must be attached
    requires_verified_rollback: bool = False  # rollback path must be proven first
    requires_dry_run: bool = False
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision.value,
            "tier": self.tier.value,
            "source": self.source,
            "requires_human": self.requires_human,
            "requires_rollback": self.requires_rollback,
            "requires_verified_rollback": self.requires_verified_rollback,
            "requires_dry_run": self.requires_dry_run,
            "reason": self.reason,
        }


def _decision_flags(decision: PolicyDecision, tier: Tier, source: str, reason: str) -> PolicyResult:
    return PolicyResult(
        decision=decision,
        tier=tier,
        source=source,
        requires_human=decision in INTERRUPTING,
        requires_rollback=decision in INTERRUPTING,
        requires_verified_rollback=decision == PolicyDecision.DRY_RUN_APPROVE,
        requires_dry_run=decision == PolicyDecision.DRY_RUN_APPROVE,
        reason=reason,
    )


@dataclass
class PolicyConfig:
    """Parsed ``config/policy.yaml``.

    Shape::

        defaults:            # override the global tier→decision table
          write: auto
        connectors:
          snowflake:
            environments:
              dev:           # matched against the classifier env hint / prod marker
                write: auto
                risky: confirm
            tiers:           # connector-wide (any environment)
              risky: confirm
        prod_markers: [PROD, GOLD]
    """

    defaults: dict[str, str] = field(default_factory=dict)
    connectors: dict[str, Any] = field(default_factory=dict)
    prod_markers: list = field(default_factory=list)


def load_policy_config(path: str = DEFAULT_POLICY_PATH) -> PolicyConfig:
    p = Path(path)
    if not p.exists():
        return PolicyConfig()
    try:
        import yaml
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return PolicyConfig()
    return PolicyConfig(
        defaults=data.get("defaults") or {},
        connectors=data.get("connectors") or {},
        prod_markers=data.get("prod_markers") or [],
    )


def _coerce(value: Any) -> PolicyDecision | None:
    try:
        return PolicyDecision(str(value))
    except ValueError:
        # Allow the friendly aliases used in policy.yaml.
        alias = {
            "auto": PolicyDecision.AUTO,
            "auto-write": PolicyDecision.AUTO,
            "verify": PolicyDecision.VERIFY,
            "confirm": PolicyDecision.CONFIRM,
            "approve": PolicyDecision.DRY_RUN_APPROVE,
            "dry_run": PolicyDecision.DRY_RUN_APPROVE,
            "dry_run+approve": PolicyDecision.DRY_RUN_APPROVE,
            "block": PolicyDecision.DRY_RUN_APPROVE,
        }
        return alias.get(str(value).strip().lower())


class PolicyEngine:
    """Resolves ``(tier, connector, environment)`` → :class:`PolicyResult`.

    Most-specific override wins: ``connector.environments.<env>`` beats
    ``connector.tiers`` beats global ``defaults`` beats the locked table.
    """

    def __init__(self, config: PolicyConfig | None = None):
        self._config = config or PolicyConfig()

    @classmethod
    def from_path(cls, path: str = DEFAULT_POLICY_PATH) -> PolicyEngine:
        return cls(load_policy_config(path))

    @property
    def prod_markers(self) -> list:
        return list(self._config.prod_markers)

    def decide(
        self,
        tier: Tier,
        *,
        connector_id: str | None = None,
        environment: str | None = None,
    ) -> PolicyResult:
        key = tier.value
        # 1. connector + environment (most specific)
        if connector_id and environment:
            conn = self._config.connectors.get(connector_id, {}) or {}
            envs = conn.get("environments", {}) or {}
            env_block = envs.get(environment) or envs.get(environment.lower()) or {}
            override = _coerce(env_block.get(key)) if isinstance(env_block, dict) else None
            if override:
                return _decision_flags(
                    override, tier, f"connector:{connector_id}/env:{environment}",
                    f"override for {connector_id} in {environment}",
                )
        # 2. connector-wide
        if connector_id:
            conn = self._config.connectors.get(connector_id, {}) or {}
            tiers = conn.get("tiers", {}) or {}
            override = _coerce(tiers.get(key)) if isinstance(tiers, dict) else None
            if override:
                return _decision_flags(
                    override, tier, f"connector:{connector_id}",
                    f"connector-wide override for {connector_id}",
                )
        # 3. global defaults
        override = _coerce(self._config.defaults.get(key))
        if override:
            return _decision_flags(override, tier, "defaults", "global default override")
        # 4. the locked posture
        decision = DEFAULT_DECISIONS[tier]
        return _decision_flags(decision, tier, "default", "locked blast-radius posture")
