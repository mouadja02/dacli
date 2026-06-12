"""Static plan + governance preview (F-1) — `dacli plan` / dry inspection.

Decomposes a goal with the heuristic :class:`~dacli.core.planner.Planner` and,
for each step, previews what governance *would* do — blast-radius tier, policy
decision, and the platform-native rollback primitive — entirely statically:
no LLM, no connector, no network, no execution.

The tier here is a *preview* derived from the step's natural-language verbs via
the shared blast-radius vocabulary (:mod:`dacli.governance.vocab`); at execution
time the real :class:`~dacli.governance.classifier.ActionClassifier` re-derives
it from the actual SQL/command, which is always the truth.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from dacli.connectors.base import Risk
from dacli.core.planner import Planner, Subtask, TaskDAG
from dacli.governance.classifier import Classification, Tier, detect_prod
from dacli.governance.policy_engine import PolicyEngine, PolicyResult
from dacli.governance.rollback import RollbackPlan, RollbackStrategist
from dacli.governance.vocab import SQL_KEYWORD_TIERS, max_tier, promote

# Natural-language verb → tier, layered over the shared SQL keyword vocabulary
# so a goal phrased as prose ("load the raw CRM extract") previews sensibly.
_NL_VERB_TIERS: list[tuple[Tier, list[str]]] = [
    (Tier.IRREVERSIBLE, ["drop", "truncate", "purge", "destroy", "force push"]),
    (Tier.RISKY, ["delete", "remove", "update", "merge", "overwrite", "replace",
                  "rewrite", "migrate", "promote", "modify", "alter"]),
    (Tier.WRITE, ["create", "build", "stand up", "set up", "load", "ingest",
                  "copy", "backfill", "insert", "add", "write", "push"]),
    (Tier.SAFE, ["profile", "introspect", "describe", "analyze", "list", "show",
                 "read", "inspect", "diff", "compare", "reconcile", "validate",
                 "select", "explain", "review", "audit", "check"]),
]

# Platform names the rollback strategist knows, longest first so "bigquery"
# wins over a hypothetical shorter overlap.
_KNOWN_PLATFORMS = sorted(
    ("snowflake", "bigquery", "databricks", "postgres", "mysql", "mongodb",
     "dynamodb", "airflow", "dagster", "github", "dbt", "gcs", "s3"),
    key=len, reverse=True,
)

_TIER_TO_RISK: dict[Tier, Risk] = {
    Tier.SAFE: Risk.SAFE,
    Tier.WRITE: Risk.WRITE,
    Tier.RISKY: Risk.RISKY,
    Tier.IRREVERSIBLE: Risk.IRREVERSIBLE,
}


@dataclass
class StepPreview:
    """One plan step with its statically-derived governance verdicts."""

    node: Subtask
    tier: Tier
    classification: Classification
    policy: PolicyResult
    rollback: RollbackPlan
    platform: str | None = None


@dataclass
class PlanPreview:
    """The decomposed DAG plus per-step governance previews."""

    goal: str
    dag: TaskDAG
    steps: list[StepPreview]


def _verb_scan(text: str) -> tuple[Tier, list[str]]:
    """Highest tier any recognized verb in ``text`` fires, with the matches."""
    low = text.lower()
    fired = Tier.SAFE
    matched: list[str] = []
    for tier, verbs in _NL_VERB_TIERS:
        for verb in verbs:
            if re.search(rf"\b{re.escape(verb)}\b", low):
                matched.append(verb)
                fired = max_tier(fired, tier)
    # SQL keywords embedded in the step text (e.g. a quoted statement) also count.
    tokens = {t.upper() for t in re.findall(r"[A-Za-z_]+", text)}
    for tier, keywords in SQL_KEYWORD_TIERS:
        for kw in keywords:
            if kw in tokens:
                matched.append(kw.lower())
                fired = max_tier(fired, tier)
    return fired, sorted(set(matched))


def _platform_for(text: str) -> str | None:
    low = text.lower()
    return next(
        (p for p in _KNOWN_PLATFORMS if re.search(rf"\b{re.escape(p)}\b", low)),
        None,
    )


def _sql_verb_for(matched: list[str], tier: Tier) -> str | None:
    """The most destructive matched verb, uppercased for the strategist."""
    if tier in (Tier.SAFE, Tier.WRITE):
        return None
    for t, verbs in _NL_VERB_TIERS:
        if t in (Tier.IRREVERSIBLE, Tier.RISKY):
            for verb in verbs:
                if verb in matched:
                    return verb.upper()
    return None


def _classify_step(node: Subtask, prod_markers: list[str] | None) -> Classification:
    """A static, prose-level analogue of ``ActionClassifier.classify``."""
    reasons: list[str] = []
    tier, matched = _verb_scan(node.description)
    if matched:
        reasons.append(f"step verb(s) {matched} → {tier.value}")
    else:
        # No recognized verb: assume state-changing work (post-conditions and
        # the real classifier re-verify at execution time).
        tier = Tier.WRITE
        reasons.append("no recognized verb → assumed write (re-classified at execution)")
    if node.irreversible and tier is not Tier.IRREVERSIBLE:
        tier = Tier.IRREVERSIBLE
        reasons.append("planner marked the step irreversible")

    marker = detect_prod({"description": node.description}, markers=prod_markers)
    is_prod = marker is not None
    if is_prod and tier in (Tier.WRITE, Tier.RISKY):
        promoted = promote(tier, 1)
        reasons.append(f"prod target ('{marker}') → promote {tier.value} to {promoted.value}")
        tier = promoted
    elif is_prod:
        reasons.append(f"prod target ('{marker}') noted (no promotion at {tier.value})")

    return Classification(
        tool_name=node.id,
        tier=tier,
        declared_risk=_TIER_TO_RISK[tier],
        is_prod=is_prod,
        prod_marker=marker,
        sql_verb=_sql_verb_for(matched, tier),
        reasons=reasons,
    )


def build_plan_preview(
    goal: str,
    *,
    policy: PolicyEngine | None = None,
    prod_markers: list[str] | None = None,
) -> PlanPreview:
    """Decompose ``goal`` and preview each step's governance verdicts.

    Pure and offline: the heuristic planner decomposes, the policy engine and
    rollback strategist are consulted statically, and nothing executes.
    """
    engine = policy or PolicyEngine()
    strategist = RollbackStrategist()
    dag = Planner().decompose(goal)

    steps: list[StepPreview] = []
    for node in dag.topological_order():
        classification = _classify_step(node, prod_markers)
        tier = classification.tier
        platform = _platform_for(node.description)
        decision = engine.decide(
            tier,
            connector_id=platform,
            environment="prod" if classification.is_prod else None,
        )
        rollback = strategist.plan_for(platform or "unknown", classification)
        steps.append(StepPreview(
            node=node,
            tier=tier,
            classification=classification,
            policy=decision,
            rollback=rollback,
            platform=platform,
        ))
    return PlanPreview(goal=goal, dag=dag, steps=steps)
