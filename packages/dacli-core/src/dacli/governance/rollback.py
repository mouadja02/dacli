"""Rollback strategies (𝒢) — *the environment is the oracle, again*.

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
from typing import Any

from dacli.governance.classifier import Classification, Tier


@dataclass
class RollbackPlan:
    """A platform-native plan to undo a single action."""

    available: bool                  # a native undo primitive exists for this op
    primitive: str                   # e.g. "time_travel_undrop", "transaction"
    strategy: str                    # human-readable description of the undo
    verified: bool = False           # the path was *checked* to exist (not assumed)
    verify_detail: str = ""          # why verified / why not
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "primitive": self.primitive,
            "strategy": self.strategy,
            "verified": self.verified,
            "verify_detail": self.verify_detail,
            "details": self.details,
        }

    @classmethod
    def none(cls, reason: str) -> RollbackPlan:
        return cls(available=False, primitive="none", strategy=reason,
                   verified=False, verify_detail=reason)


# Native primitive selection per (platform, situation). Kept declarative so it
# is obvious what undo each action gets and easy to extend per platform.
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
        details={"requires": "bucket/object versioning enabled"},
    )


def _bigquery_plan(cls: Classification) -> RollbackPlan:
    verb = (cls.sql_verb or "").upper()
    if verb in ("DROP", "TRUNCATE"):
        return RollbackPlan(
            available=True, primitive="bq_time_travel_snapshot",
            strategy=("BigQuery time travel (FOR SYSTEM_TIME AS OF) recovers the "
                      "table within its 7-day window; take a table snapshot first "
                      "for a durable restore point."),
            details={"requires": "within time-travel window / snapshot taken"},
        )
    if verb in ("DELETE", "UPDATE", "MERGE", "INSERT"):
        return RollbackPlan(
            available=True, primitive="transaction",
            strategy=("Wrap DML in a multi-statement transaction (BEGIN "
                      "TRANSACTION … ROLLBACK TRANSACTION on failure)."),
        )
    return RollbackPlan(
        available=True, primitive="bq_snapshot",
        strategy=("Create a table snapshot before the change; restore from the "
                  "snapshot to undo. dry_run previews exact bytes/effect first."),
    )


def _databricks_plan(cls: Classification) -> RollbackPlan:
    verb = (cls.sql_verb or "").upper()
    if verb in ("DROP", "TRUNCATE", "DELETE", "UPDATE", "MERGE", "INSERT"):
        return RollbackPlan(
            available=True, primitive="delta_time_travel",
            strategy=("Delta Lake time travel: RESTORE TABLE … TO VERSION AS OF / "
                      "TIMESTAMP AS OF to undo within the retention window."),
            details={"requires": "Delta history retained (delta.deletedFileRetentionDuration)"},
        )
    return RollbackPlan(
        available=True, primitive="delta_shallow_clone",
        strategy=("Shadow on a shallow CLONE, diff, promote on approval; original "
                  "Delta table untouched until then."),
    )


def _postgres_plan(cls: Classification) -> RollbackPlan:
    verb = (cls.sql_verb or "").upper()
    if verb in ("DROP", "TRUNCATE"):
        return RollbackPlan(
            available=True, primitive="pg_dump_snapshot",
            strategy=("pg_dump the object before DROP/TRUNCATE; restore from the "
                      "dump to undo. PostgreSQL DDL is transactional, so an "
                      "in-transaction DROP can also be ROLLBACKed."),
            details={"requires": "snapshot taken or run inside an explicit transaction"},
        )
    return RollbackPlan(
        available=True, primitive="transaction",
        strategy=("Wrap in BEGIN … ROLLBACK on failure. PostgreSQL is fully "
                  "transactional (DDL included), so this is a true undo."),
    )


def _mysql_plan(cls: Classification) -> RollbackPlan:
    verb = (cls.sql_verb or "").upper()
    if verb in ("DROP", "TRUNCATE"):
        return RollbackPlan(
            available=True, primitive="mysqldump_snapshot",
            strategy=("mysqldump the object before DROP/TRUNCATE; restore from the "
                      "dump to undo. MySQL DDL auto-commits — it is NOT "
                      "transactional — so a dump is the only undo."),
            details={"requires": "mysqldump snapshot taken (DDL is not transactional)"},
        )
    return RollbackPlan(
        available=True, primitive="transaction",
        strategy=("Wrap DML in BEGIN … ROLLBACK (InnoDB is transactional; DDL is "
                  "not, so DDL relies on mysqldump)."),
    )


def _mongodb_plan(_cls: Classification) -> RollbackPlan:
    return RollbackPlan(
        available=True, primitive="mongodump_snapshot",
        strategy=("mongodump the collection (copy-aside) before a delete/drop; "
                  "mongorestore to undo. MongoDB has no general native undo."),
        details={"requires": "collection dumpable before the mutation"},
    )


def _dynamodb_plan(_cls: Classification) -> RollbackPlan:
    return RollbackPlan(
        available=True, primitive="dynamodb_pitr",
        strategy=("Point-in-time recovery (PITR) restores the table to a moment "
                  "before the change; an on-demand backup is a durable fallback."),
        details={"requires": "PITR enabled (continuous backups) on the table"},
    )


def _airflow_plan(cls: Classification) -> RollbackPlan:
    name = cls.tool_name
    if "delete" in name:
        return RollbackPlan.none(
            "Deleting a DAG removes its run history with no native undo; "
            "re-deploying the DAG file from version control restores only the "
            "definition. Gated hard.")
    if "pause" in name:
        return RollbackPlan(
            available=True, primitive="airflow_unpause",
            strategy="Unpause the DAG to restore scheduling.")
    if "trigger" in name:
        return RollbackPlan.none(
            "Triggering a DAG run has external side effects with no native undo; "
            "clear/mark-failed the run to stop further tasks.")
    return RollbackPlan.none("No native Airflow undo for this operation.")


def _dagster_plan(_cls: Classification) -> RollbackPlan:
    return RollbackPlan.none(
        "Launching a run has external side effects with no native undo; "
        "terminate the run to stop further steps.")


def _dbt_plan(_cls: Classification) -> RollbackPlan:
    return RollbackPlan(
        available=True, primitive="git_versioned_transform",
        strategy=("Transforms are git-versioned (revert the model commit) and the "
                  "target table is snapshot/cloned before a run; restore the "
                  "snapshot + revert to undo."),
        details={"requires": "model under version control; target snapshot taken"},
    )


def _shell_plan(cls: Classification) -> RollbackPlan:
    """Native undo for a shell-tier command (Era 2).

    The "environment is the oracle" rule again: the undo is the shell's / file
    system's own primitive — copy-aside before an overwrite or delete, ``git``
    revert/stash for git mutations — and a recursive/forced delete (``rm -rf``)
    or a history rewrite (``git push --force``) honestly has **no** native undo,
    so it yields an unavailable plan and is refused (the S3-delete bar, applied
    to the shell).
    """
    signals = cls.command_signals or {}
    verb = (cls.command_verb or "").lower()
    overwrites = signals.get("overwrites") or []
    deletes = signals.get("deletes") or []

    # Honestly irreversible: recursive/forced delete, force-push, fork bomb,
    # device overwrite, embedded DROP/TRUNCATE — no native undo we can name.
    if signals.get("irreversible"):
        return RollbackPlan.none(
            "Command is irreversible (recursive/forced delete, history rewrite, "
            "or destructive SQL) — no native undo primitive exists. Refused.")

    if verb == "git":
        return RollbackPlan(
            available=True, primitive="git_revert_or_stash",
            strategy=("Git is the oracle: `git revert <sha>` undoes a commit, "
                      "`git stash`/`git checkout -- <path>` restores working tree "
                      "changes, `git reset --soft HEAD~1` unwinds the last commit."),
            details={"requires": "the repository is under version control"},
        )

    if overwrites or deletes:
        targets = overwrites + deletes
        return RollbackPlan(
            available=True, primitive="versioned_copy_aside",
            strategy=("Copy-aside the affected file(s) into the session "
                      "workspace backups/ before the change; restore the copy to "
                      "undo."),
            details={"targets": targets, "requires": "target exists & is inside the jail"},
        )

    # New-file / mkdir writes are recoverable by deletion.
    return RollbackPlan(
        available=True, primitive="delete_created_artifact",
        strategy=("Newly-created files/dirs are recoverable by deleting them; "
                  "the session workspace is the only writable surface."),
    )


_PLATFORM_PLANNERS = {
    "shell": _shell_plan,
    "snowflake": _snowflake_plan,
    "github": _github_plan,
    "s3": _object_store_plan,
    "gcs": _object_store_plan,
    "bigquery": _bigquery_plan,
    "databricks": _databricks_plan,
    "dbt": _dbt_plan,
    "postgres": _postgres_plan,
    "mysql": _mysql_plan,
    "mongodb": _mongodb_plan,
    "dynamodb": _dynamodb_plan,
    "airflow": _airflow_plan,
    "dagster": _dagster_plan,
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
        args: dict[str, Any],
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
