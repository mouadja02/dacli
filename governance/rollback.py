"""Rollback strategies (𝒢, Phase 5.3) — *the environment is the oracle, again*.

For every gated action we attach a **rollback plan** built from the platform's
**native** undo primitive rather than a generic compensating action — more
reliable, cheaper, and honest about what truly *cannot* be undone:

| Platform | Native rollback primitive |
|---|---|
| Snowflake | ``BEGIN/ROLLBACK`` for DML; **Time Travel + ``UNDROP``** for dropped tables; zero-copy ``CLONE`` for shadow execution |
| BigQuery  | transactions; table snapshots; ``dry_run`` preview |
| Databricks| Delta time travel (``RESTORE`` / ``VERSION AS OF``); shallow clone |
| Postgres/MySQL | transactional DDL/DML; ``pg_dump`` of the touched object |
| S3 / GCS  | versioned buckets / copy-aside before overwrite |
| GitHub    | revert commit / restore prior blob by SHA |

The decisive rule (exit criterion #1): an **irreversible** action is *blocked*
unless its rollback path is **verified to exist** — retention actually enabled,
clone actually creatable — not merely assumed. Verification is delegated to the
connector via an optional ``verify_rollback`` hook; a connector that cannot
confirm a path yields ``verified=False`` and the action is refused.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any, Dict

from governance.classifier import Classification, Tier


@dataclass
class RollbackPlan:
    """A platform-native plan to undo a single action."""

    available: bool                  # a native undo primitive exists for this op
    primitive: str                   # e.g. "time_travel_undrop", "transaction"
    strategy: str                    # human-readable description of the undo
    verified: bool = False           # the path was *checked* to exist (not assumed)
    verify_detail: str = ""          # why verified / why not
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "available": self.available,
            "primitive": self.primitive,
            "strategy": self.strategy,
            "verified": self.verified,
            "verify_detail": self.verify_detail,
            "details": self.details,
        }

    @classmethod
    def none(cls, reason: str) -> "RollbackPlan":
        return cls(available=False, primitive="none", strategy=reason,
                   verified=False, verify_detail=reason)


# Native primitive selection per (platform, situation). Kept declarative so it
# is obvious what undo each action gets and easy to extend per Phase 7 platform.
def _snowflake_plan(cls: Classification) -> RollbackPlan:
    verb = (cls.sql_verb or "").upper()
    if verb in ("DROP", "TRUNCATE"):
        return RollbackPlan(
            available=True, primitive="time_travel_undrop",
            strategy=("Snowflake Time Travel: recover via UNDROP TABLE / "
                      "AT(OFFSET=>-n) within the object's data-retention window."),
            details={"requires": "DATA_RETENTION_TIME_IN_DAYS > 0"},
        )
    if verb in ("DELETE", "UPDATE", "MERGE", "INSERT", "COPY"):
        return RollbackPlan(
            available=True, primitive="transaction",
            strategy=("Wrap in an explicit transaction (BEGIN … ROLLBACK on "
                      "failure); Time Travel as a second line of defense."),
        )
    return RollbackPlan(
        available=True, primitive="zero_copy_clone",
        strategy=("Shadow execution on a zero-copy CLONE, diff, promote on "
                  "approval; original untouched until then."),
    )


def _github_plan(cls: Classification) -> RollbackPlan:
    name = cls.tool_name
    if "delete" in name:
        return RollbackPlan(
            available=True, primitive="git_restore_blob",
            strategy="Restore the deleted file from its prior blob SHA (recommit).",
        )
    if "push" in name or "create" in name or "update" in name:
        return RollbackPlan(
            available=True, primitive="git_revert",
            strategy="Revert the commit / restore the prior blob by SHA.",
        )
    if "workflow" in name or "trigger" in name:
        return RollbackPlan.none(
            "Triggering a workflow has external side effects with no native undo.")
    return RollbackPlan.none("No native GitHub undo for this operation.")


def _object_store_plan(_cls: Classification) -> RollbackPlan:
    return RollbackPlan(
        available=True, primitive="versioned_copy_aside",
        strategy=("Copy-aside the object (or rely on bucket versioning) before "
                  "overwrite; restore the prior version to undo."),
    )


_PLATFORM_PLANNERS = {
    "snowflake": _snowflake_plan,
    "github": _github_plan,
    "s3": _object_store_plan,
    "gcs": _object_store_plan,
}


class RollbackStrategist:
    """Builds and verifies a :class:`RollbackPlan` for a classified action."""

    def plan_for(
        self,
        connector_id: str,
        classification: Classification,
    ) -> RollbackPlan:
        planner = _PLATFORM_PLANNERS.get(connector_id)
        if planner is None:
            # Unknown platform: be honest — no native undo we can name.
            if classification.tier == Tier.SAFE:
                return RollbackPlan(available=True, primitive="noop",
                                    strategy="Read-only action — nothing to undo.",
                                    verified=True, verify_detail="no state change")
            return RollbackPlan.none(
                f"No native rollback strategy registered for '{connector_id}'.")
        plan = planner(classification)
        # A safe (read-only) action needs no undo; mark it trivially verified.
        if classification.tier == Tier.SAFE:
            return RollbackPlan(available=True, primitive="noop",
                                strategy="Read-only action — nothing to undo.",
                                verified=True, verify_detail="no state change")
        return plan

    async def verify(
        self,
        plan: RollbackPlan,
        connector: Any,
        args: Dict[str, Any],
    ) -> RollbackPlan:
        """Verify the rollback path *actually exists* before the action runs.

        Delegates to the connector's optional ``verify_rollback(plan, args)``
        hook (sync or async), which interrogates the live platform (retention
        enabled? clone creatable?). Absent the hook, the path is treated as
        **unverified** — conservative by design, so an irreversible action with
        no proof of an undo path is refused rather than gambled on.
        """
        if plan.verified:
            return plan
        if not plan.available:
            plan.verify_detail = plan.verify_detail or "no native undo primitive"
            return plan

        hook = getattr(connector, "verify_rollback", None)
        if hook is None:
            plan.verified = False
            plan.verify_detail = (
                f"connector '{getattr(connector, 'name', '?')}' cannot confirm a "
                f"'{plan.primitive}' path (no verify_rollback hook)")
            return plan
        try:
            outcome = hook(plan, args)
            if inspect.isawaitable(outcome):
                outcome = await outcome
            if isinstance(outcome, tuple):
                ok, detail = outcome
            else:
                ok, detail = bool(outcome), ""
            plan.verified = bool(ok)
            plan.verify_detail = str(detail or ("verified" if ok else "not verified"))
        except Exception as e:
            plan.verified = False
            plan.verify_detail = f"rollback verification raised: {e}"
        return plan
