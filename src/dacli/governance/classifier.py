"""Action classifier (𝒢) — *blast-radius first*.

Every state-changing action is mapped to a **tier** before it runs. The tier,
not the model's confidence, is what the policy engine gates on.

| Tier | Examples | Policy (see policy_engine) |
|---|---|---|
| ``safe``        | SELECT, list, describe, EXPLAIN, dry-run | auto-run |
| ``write``       | CREATE, INSERT, COPY INTO, push new file | auto-run + post-condition |
| ``risky``       | UPDATE, DELETE, MERGE, overwrite, trigger | confirm + rollback plan |
| ``irreversible``| DROP, TRUNCATE, prod writes, force-push   | dry-run + explicit approval |

The tier is derived from three grounded signals (never the model's say-so):

1. **The op's declared ``risk`` hint** (metadata) — the floor for any
   op we cannot inspect more closely.
2. **A SQL verb parse** for SQL ops — the *actual* statement tells the truth
   (a SELECT through a ``RISKY``-declared ``execute_query`` op is really safe;
   a ``DROP`` is really irreversible). Parsing scans the **whole statement**,
   not just the leading verb, so a destructive keyword hidden in a CTE or a
   multi-statement string still promotes the tier. Unparseable / ambiguous SQL
   **defaults to ``risky``** (default-deny), per the self-critique.
3. **Production detection** — most catastrophic agent actions are the *right*
   operation against the *wrong* (prod) environment. A prod target **promotes**
   the tier one step (``write`` → ``risky``, ``risky`` → ``irreversible``).

The classifier is pure and dependency-light so it is trivially testable and can
run identically inside the tool tier and inside the sandbox SDK.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from dacli.connectors.base import Risk

# The verb → tier vocabulary is shared with the shell command classifier (one
# source of truth — A-3). ``Tier`` is re-exported here so existing
# ``from dacli.governance.classifier import Tier`` imports keep working.
from dacli.governance.vocab import (
    SQL_KEYWORD_TIERS as _SQL_KEYWORD_TIERS,
    Tier,
    max_tier as _max_tier,
    promote as _promote,
    rank as _rank,
)


# The op-level Risk hint maps straight onto a tier as the floor.
_RISK_TO_TIER: dict[Risk, Tier] = {
    Risk.SAFE: Tier.SAFE,
    Risk.WRITE: Tier.WRITE,
    Risk.RISKY: Tier.RISKY,
    Risk.IRREVERSIBLE: Tier.IRREVERSIBLE,
}


# ---------------------------------------------------------------------------
# SQL verb classification — whole-statement, defense-in-depth.
# ---------------------------------------------------------------------------
# The keyword → tier tables live in governance.vocab (shared with the shell
# command classifier). A keyword anywhere in the statement promotes to (at
# least) its tier; the scan deliberately over-classifies on string literals/
# identifiers (fail-safe) and word boundaries keep ``UPDATED_AT`` from
# matching ``UPDATE``.

# A statement whose leading verb is none of these (and that we therefore cannot
# vouch for) is treated as ambiguous → default-deny → RISKY.
_KNOWN_LEADING_VERBS = {
    kw for _tier, kws in _SQL_KEYWORD_TIERS for kw in kws
}

_WORD_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# Arg names that, when present, mark an op as SQL-bearing.
_SQL_ARG_KEYS = ("query", "sql", "statement")


def _looks_like_sql(args: dict[str, Any]) -> str | None:
    for key in _SQL_ARG_KEYS:
        val = args.get(key)
        if isinstance(val, str) and val.strip():
            return val
    return None


def _strip_sql(sql: str) -> str:
    """Remove comments and string literals so keyword scanning sees only code."""
    no_block = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    no_line = re.sub(r"--[^\n]*", " ", no_block)
    # Drop single- and double-quoted literals (so a literal 'DROP' is invisible).
    no_strings = re.sub(r"'(?:[^']|'')*'", " ", no_line)
    return re.sub(r'"(?:[^"]|"")*"', " ", no_strings)


def classify_sql(sql: str) -> SqlVerdict:
    """Return the tier + leading verb + every destructive keyword that fired."""
    cleaned = _strip_sql(sql or "").strip().rstrip(";").strip()
    if not cleaned:
        return SqlVerdict(Tier.RISKY, leading_verb=None, ambiguous=True,
                          reason="empty/ uninspectable SQL → default-deny (risky)")

    tokens = [m.group(0).upper() for m in _WORD_RE.finditer(cleaned)]
    token_set = set(tokens)
    leading = tokens[0] if tokens else None

    # Multiple top-level statements are hard to vouch for as one unit.
    multi_statement = ";" in cleaned

    matched: list[str] = []
    fired = Tier.SAFE
    for tier, keywords in _SQL_KEYWORD_TIERS:
        for kw in keywords:
            # Multi-word handled token-wise: COPY/INSERT etc. are single tokens.
            if kw in token_set:
                matched.append(kw)
                fired = _max_tier(fired, tier)

    ambiguous = leading not in _KNOWN_LEADING_VERBS or multi_statement
    if ambiguous:
        # Default-deny: never let an unparseable / multi-statement string run as
        # though it were safe. Floor at RISKY, but still honor a parsed DROP.
        fired = _max_tier(fired, Tier.RISKY)

    reason = (
        f"SQL leading verb '{leading or '?'}'"
        + (f"; destructive keyword(s) {sorted(set(matched))}" if any(
            _kw_tier(k) >= _rank(Tier.RISKY) for k in matched) else "")
        + ("; multi-statement → default-deny" if multi_statement else "")
        + ("; unknown verb → default-deny" if leading not in _KNOWN_LEADING_VERBS else "")
    )
    return SqlVerdict(fired, leading_verb=leading, ambiguous=ambiguous,
                      reason=reason, keywords=sorted(set(matched)))


def _kw_tier(keyword: str) -> int:
    for tier, kws in _SQL_KEYWORD_TIERS:
        if keyword in kws:
            return _rank(tier)
    return _rank(Tier.SAFE)


@dataclass
class SqlVerdict:
    tier: Tier
    leading_verb: str | None
    ambiguous: bool
    reason: str
    keywords: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Production detection
# ---------------------------------------------------------------------------
# Default markers; extendable via policy/settings. Matched case-insensitively as
# whole identifier tokens against arg values + an explicit environment hint.
DEFAULT_PROD_MARKERS = ["PROD", "PRODUCTION", "GOLD", "_PROD", "PRD"]


def detect_prod(
    args: dict[str, Any],
    *,
    env_hint: str | None = None,
    markers: list[str] | None = None,
) -> str | None:
    """Return the first prod marker found (in args or the env hint), else None."""
    marks = [m.upper() for m in (markers or DEFAULT_PROD_MARKERS)]
    haystacks: list[str] = []
    if env_hint:
        haystacks.append(str(env_hint))
    for v in args.values():
        if isinstance(v, str):
            haystacks.append(v)
        elif isinstance(v, dict):
            haystacks.extend(str(x) for x in v.values() if isinstance(x, str))
    for hay in haystacks:
        upper = hay.upper()
        tokens = {m.group(0).upper() for m in _WORD_RE.finditer(upper)}
        for m in marks:
            # token-level match (DB name "GOLD") or substring for compound names.
            if m in tokens or m in upper:
                return m
    return None


# ---------------------------------------------------------------------------
# Classification result + classifier
# ---------------------------------------------------------------------------
@dataclass
class Classification:
    """The auditable verdict for one pending action."""

    tool_name: str
    tier: Tier
    declared_risk: Risk
    is_prod: bool = False
    prod_marker: str | None = None
    sql_verb: str | None = None
    sql_ambiguous: bool = False
    reasons: list[str] = field(default_factory=list)
    # Shell tier (Era 2): the parsed command verb + its blast-radius signals
    # (writes/overwrites/deletes/egress/jail-escape), so the shell post-conditions
    # and the shell rollback planner can read what the command intended.
    command_verb: str | None = None
    command_signals: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tool_name": self.tool_name,
            "tier": self.tier.value,
            "declared_risk": self.declared_risk.value,
            "is_prod": self.is_prod,
            "prod_marker": self.prod_marker,
            "sql_verb": self.sql_verb,
            "sql_ambiguous": self.sql_ambiguous,
            "reasons": list(self.reasons),
            "command_verb": self.command_verb,
            "command_signals": self.command_signals or {},
        }


class ActionClassifier:
    """Maps ``(op, args, environment)`` → :class:`Classification`.

    ``network`` / ``egress_allowlist`` configure the embedded shell command
    classifier so the same egress posture applies whether a fetch happens via a
    connector or a free-text shell command.
    """

    def __init__(
        self,
        *,
        prod_markers: list[str] | None = None,
        network: str = "allowlist",
        egress_allowlist: list[str] | None = None,
    ):
        self._prod_markers = prod_markers or list(DEFAULT_PROD_MARKERS)
        self._network = network
        self._egress_allowlist = list(egress_allowlist or [])

    def classify(
        self,
        tool_name: str,
        args: dict[str, Any],
        *,
        declared_risk: Risk = Risk.SAFE,
        env_hint: str | None = None,
        command: str | None = None,
    ) -> Classification:
        args = args or {}
        reasons: list[str] = []

        # 1. Floor from the declared op risk.
        tier = _RISK_TO_TIER.get(declared_risk, Tier.RISKY)
        reasons.append(f"declared op risk '{declared_risk.value}' → floor {tier.value}")

        # 2. SQL parse (the actual statement is the truth for SQL ops).
        sql = _looks_like_sql(args)
        sql_verb: str | None = None
        sql_ambiguous = False
        if sql is not None:
            verdict = classify_sql(sql)
            sql_verb = verdict.leading_verb
            sql_ambiguous = verdict.ambiguous
            # The SQL verb *replaces* the conservative op floor when it can be
            # parsed confidently (a real SELECT is safe even though the op that
            # carries it is declared RISKY); otherwise it can only promote.
            tier = _max_tier(tier, verdict.tier) if verdict.ambiguous else verdict.tier
            reasons.append(verdict.reason)

        # 2b. Shell command parse (the actual command is the truth for the shell
        # tier). Like a confidently-parsed SQL verb, the command verdict *is* the
        # blast radius — `ls` is safe even though the run_shell_command op is
        # write-capable; `rm -rf` is irreversible. Unknown commands default-deny
        # to risky inside the command classifier.
        command_verb: str | None = None
        command_signals: dict[str, Any] = {}
        if command is not None:
            from dacli.governance.command_classifier import CommandClassifier
            cv = CommandClassifier(
                network=self._network, egress_allowlist=self._egress_allowlist,
            ).classify(command)
            command_verb = cv.leading
            command_signals = cv.to_dict()
            tier = cv.tier
            reasons.extend(cv.reasons)

        # 3. Production promotion (right op, wrong environment).
        marker = detect_prod(args, env_hint=env_hint, markers=self._prod_markers)
        is_prod = marker is not None
        if is_prod and tier in (Tier.WRITE, Tier.RISKY):
            promoted = _promote(tier, 1)
            reasons.append(
                f"prod target ('{marker}') → promote {tier.value} to {promoted.value}"
            )
            tier = promoted
        elif is_prod:
            reasons.append(f"prod target ('{marker}') noted (no promotion at {tier.value})")

        return Classification(
            tool_name=tool_name,
            tier=tier,
            declared_risk=declared_risk,
            is_prod=is_prod,
            prod_marker=marker,
            sql_verb=sql_verb,
            sql_ambiguous=sql_ambiguous,
            reasons=reasons,
            command_verb=command_verb,
            command_signals=command_signals,
        )
