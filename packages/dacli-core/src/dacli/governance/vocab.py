"""Shared blast-radius vocabulary (𝒢) — one source of truth for verb → tier.

:mod:`governance.classifier` (an ``OperationSpec`` + SQL statement) and
:mod:`governance.command_classifier` (a free-text shell line) consume different
*inputs*, but they must agree on what a verb *means*: ``drop`` / ``truncate`` /
``delete`` are irreversible whether they arrive as SQL or as a CLI sub-command.
This module holds that vocabulary — the tier order, the SQL keyword tables, the
destructive-SQL regex, and the CLI sub-verb sets — so the two classifiers
cannot drift apart (A-3).

Only the vocab lives here; classification *logic* stays with each classifier.
"""

from __future__ import annotations

import re
from enum import Enum


class Tier(str, Enum):
    """Blast-radius tier. Ordered: SAFE < WRITE < RISKY < IRREVERSIBLE."""

    SAFE = "safe"
    WRITE = "write"
    RISKY = "risky"
    IRREVERSIBLE = "irreversible"


# Severity order so callers can take the *max* of several signals and "promote".
TIER_ORDER: list[Tier] = [Tier.SAFE, Tier.WRITE, Tier.RISKY, Tier.IRREVERSIBLE]


def rank(tier: Tier) -> int:
    return TIER_ORDER.index(tier)


def max_tier(*tiers: Tier) -> Tier:
    return max(tiers, key=rank)


def promote(tier: Tier, steps: int = 1) -> Tier:
    return TIER_ORDER[min(rank(tier) + steps, len(TIER_ORDER) - 1)]


# ---------------------------------------------------------------------------
# SQL verb classification — whole-statement, defense-in-depth.
# ---------------------------------------------------------------------------
# A keyword anywhere in the statement promotes to (at least) its tier. This
# deliberately over-classifies on string literals/identifiers (fail-safe): we
# would rather ask for confirmation on a benign query than auto-run a hidden
# DELETE. Word boundaries keep ``UPDATED_AT`` from matching ``UPDATE``.
SQL_KEYWORD_TIERS: list[tuple[Tier, list[str]]] = [
    (Tier.IRREVERSIBLE, ["DROP", "TRUNCATE", "RENAME", "REPLACE", "UNDROP", "PURGE"]),
    (Tier.RISKY, ["DELETE", "UPDATE", "MERGE", "ALTER", "GRANT", "REVOKE",
                  "OVERWRITE", "CALL", "EXECUTE"]),
    (Tier.WRITE, ["CREATE", "INSERT", "COPY", "PUT", "UPSERT", "UNLOAD", "COMMENT"]),
    (Tier.SAFE, ["SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "LIST",
                 "USE", "VALUES", "GET", "WITH"]),
]

# Destructive SQL keywords that may appear in a `psql -c "..."` / `bq query`.
DESTRUCTIVE_SQL_RE = re.compile(r"\b(DROP|TRUNCATE|DELETE\s+FROM|ALTER)\b", re.IGNORECASE)


# ---------------------------------------------------------------------------
# CLI sub-verbs (git, dbt, aws, gcloud, bq, kubectl, docker, …).
# ---------------------------------------------------------------------------
# Read-only sub-verbs — auto-run.
READ_SUBVERBS = {
    "status", "log", "diff", "show", "branch", "remote", "describe",
    "list", "ls", "get", "head", "cat", "ps", "version", "help", "config",
    "rev-parse", "ls-files", "blame", "whoami", "info", "inspect", "view",
    "compile", "parse", "debug", "explain", "validate", "plan", "fmt",
    "top", "logs", "history", "current-context", "find",
}

# Mutating / dangerous sub-verbs.
WRITE_SUBVERBS = {"add", "fetch", "clone", "init", "stage", "tag", "seed", "cp", "sync"}
RISKY_SUBVERBS = {
    "commit", "merge", "rebase", "pull", "push", "checkout", "switch",
    "stash", "cherry-pick", "revert", "run", "build", "snapshot", "apply",
    "create", "update", "set", "put", "deploy", "restart", "scale", "exec",
    "run-operation", "mv", "move",
}
IRREVERSIBLE_SUBVERBS = {
    "prune", "destroy", "delete", "rm", "drop", "truncate", "purge",
    "force-delete", "terminate", "uninstall", "reset",
}
