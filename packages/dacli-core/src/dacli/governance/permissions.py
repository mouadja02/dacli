"""Permission scoping (𝒢) — least privilege by default.

Each connector is granted a **scope** that bounds what it may do. The default is
``read-only``: even if the model asks a connector to write, the request is
denied unless a write/admin scope was explicitly granted for that connection
profile. This shrinks the blast radius of a bad generation *structurally* — the
capability simply isn't there to misuse.

Scopes map onto the same blast-radius tiers the classifier produces, so the
permission check is just "is this action's tier within the granted scope?":

| Scope | Permits up to tier |
|---|---|
| ``read_only`` | ``safe``        |
| ``write``     | ``write``       |
| ``risky``     | ``risky``       |
| ``admin``     | ``irreversible``|

Grants come from the connection profile (``config/policy.yaml`` →
``connectors.<id>.scope`` or per-environment ``scope``), defaulting to
``read_only``. The sandbox SDK consults the same registry, so code-execution
cannot escape its connector's scope either.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from dacli.governance.classifier import Tier


class Scope(str, Enum):
    READ_ONLY = "read_only"
    WRITE = "write"
    RISKY = "risky"
    ADMIN = "admin"


# The highest tier each scope permits.
_SCOPE_CEILING: dict[Scope, Tier] = {
    Scope.READ_ONLY: Tier.SAFE,
    Scope.WRITE: Tier.WRITE,
    Scope.RISKY: Tier.RISKY,
    Scope.ADMIN: Tier.IRREVERSIBLE,
}

_TIER_ORDER = [Tier.SAFE, Tier.WRITE, Tier.RISKY, Tier.IRREVERSIBLE]


def _tier_rank(t: Tier) -> int:
    return _TIER_ORDER.index(t)


class PermissionError(Exception):
    """Raised (or reported) when an action exceeds a connector's granted scope."""


@dataclass
class ScopeCheck:
    allowed: bool
    scope: Scope
    tier: Tier
    reason: str

    def to_dict(self) -> dict[str, object]:
        return {
            "allowed": self.allowed,
            "granted_scope": self.scope.value,
            "action_tier": self.tier.value,
            "reason": self.reason,
        }


def _coerce_scope(value: object) -> Scope | None:
    try:
        return Scope(str(value).strip().lower())
    except ValueError:
        alias = {
            "read": Scope.READ_ONLY,
            "readonly": Scope.READ_ONLY,
            "ro": Scope.READ_ONLY,
            "rw": Scope.WRITE,
            "readwrite": Scope.WRITE,
            "full": Scope.ADMIN,
        }
        return alias.get(str(value).strip().lower())


class PermissionRegistry:
    """Holds the granted scope per connector (least-privilege; read-only floor)."""

    def __init__(self, default_scope: Scope = Scope.READ_ONLY):
        self._default = default_scope
        self._grants: dict[str, Scope] = {}

    def grant(self, connector_id: str, scope: Scope) -> None:
        self._grants[connector_id] = scope

    def scope_for(self, connector_id: str) -> Scope:
        return self._grants.get(connector_id, self._default)

    @classmethod
    def from_policy_config(cls, config, default_scope: Scope = Scope.READ_ONLY) -> PermissionRegistry:
        """Build grants from a :class:`~governance.policy_engine.PolicyConfig`.

        Reads ``connectors.<id>.scope`` (a connection-profile grant). Anything
        unspecified stays at the read-only default — write/admin is opt-in.
        """
        reg = cls(default_scope=default_scope)
        connectors = getattr(config, "connectors", {}) or {}
        for cid, block in connectors.items():
            if not isinstance(block, dict):
                continue
            scope = _coerce_scope(block.get("scope"))
            if scope is not None:
                reg.grant(cid, scope)
        return reg

    def check(self, connector_id: str, tier: Tier) -> ScopeCheck:
        """Is an action of ``tier`` permitted by ``connector_id``'s scope?"""
        scope = self.scope_for(connector_id)
        ceiling = _SCOPE_CEILING[scope]
        allowed = _tier_rank(tier) <= _tier_rank(ceiling)
        if allowed:
            reason = f"{tier.value} within granted scope '{scope.value}' (≤ {ceiling.value})"
        else:
            reason = (
                f"action tier '{tier.value}' exceeds granted scope '{scope.value}' "
                f"(permits up to '{ceiling.value}'). Grant a wider scope in the "
                f"connection profile to allow this."
            )
        return ScopeCheck(allowed=allowed, scope=scope, tier=tier, reason=reason)
