"""Task classifier & tier router (𝒮 + the routing half of 𝒪).

Every task is routed, per the locked hybrid-execution decision, to one of three
tiers:

* **Tool tier** — a single, well-scoped op on one platform ("show me row counts").
  Low latency, a direct typed connector call.
* **Shell tier** (Era 2) — local glue / file work, or running a platform CLI with
  no typed op ("ls the workspace", "run `git status` in the terminal"). Executes
  in the governed terminal session; every command is blast-radius-classified by
  the command classifier before it runs. *Destructive platform mutations never go
  here when a connector can do them* (connector-preference).
* **Sandbox tier** — multi-step, large-data, or cross-platform work ("diff
  yesterday's S3 dump against the BRONZE table and load the delta"). The agent
  writes a script against the connector SDK; it runs in the sandbox and
  only a summary returns to context.

Classification is cheap and fast (a heuristic over the registry's known
platforms, optionally confirmed by the cheap model) so routing never burns the
strong model's budget. When confidence is below threshold the router **escalates
rather than guesses** (tool→sandbox→human), with a bounded budget; exhausting it
surfaces to the user with the trail. Every decision is logged for audit (feeds
 audit + calibration).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, List, Optional, Tuple


class Tier(str, Enum):
    TOOL = "tool"
    SHELL = "shell"
    SANDBOX = "sandbox"


# Lexical signals. Kept small and explicit so a routing decision is explainable
# (not a black box) — the rationale string names which signal fired.
_MULTISTEP_SIGNALS = [
    "and then", "then ", "after that", "diff", "compare", "reconcile", "sync",
    "migrate", "pipeline", "for each", "delta", "join across", "backfill",
    "end-to-end", "step by step", "first ", "finally",
]
_LARGE_DATA_SIGNALS = [
    "all tables", "every table", "entire", "bulk", "millions", "whole database",
    "all schemas", "across all",
]
_SINGLE_OP_VERBS = [
    "show", "list", "count", "describe", "get", "read", "display", "how many",
    "what is", "fetch", "check",
]
# Explicit "do this in the terminal" cues — a user override that sends work to
# the governed shell tier even when a connector platform is named.
_SHELL_CUES = [
    "in the terminal", "in a terminal", "in the shell", "shell command",
    "command line", "command-line", "run the command", "bash", "powershell",
    "pwsh", "zsh", " cli ", "terminal session", "scrollback", "on disk",
    "local file", "working directory", "workspace directory",
]
# Local filesystem / glue commands that, as the *leading* word of a task and
# with no named platform connector, belong on the shell tier (tools-as-code for
# local glue). Curated to imperative command tokens that rarely open an English
# sentence, so "find the rows" / "make a table" don't misroute.
_LEADING_GLUE = {
    "ls", "cat", "mkdir", "touch", "chmod", "chown", "grep", "pwd", "rm",
    "cp", "mv", "tail", "head", "tar", "unzip", "zip", "rsync", "wc", "tree",
    "curl", "wget", "cd", "cls", "clear", "rg", "stat",
}


@dataclass
class RoutingDecision:
    """An auditable routing decision (mirrors the skill-routing reference log)."""

    task: str
    tier: str
    target: Optional[str]
    confidence: float
    rationale: str
    escalations: int = 0
    escalation_target: Optional[str] = None
    surfaced_to_user: bool = False
    trail: List[str] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        return asdict(self)


class RoutingAuditLog:
    """Append-only JSONL log of every routing decision."""

    def __init__(self, path: str = ".dacli/routing.jsonl"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, decision: RoutingDecision) -> None:
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(decision.to_dict(), default=str) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def recent(self, n: int = 20) -> List[dict]:
        if not self.path.exists():
            return []
        with open(self.path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        out = []
        for line in lines[-n:]:
            try:
                out.append(json.loads(line))
            except Exception:
                continue
        return out


class TierRouter:
    """Classifies a task and routes it to a tier with confidence-aware escalation.

    ``registry`` (optional) provides the set of known platforms so cross-platform
    detection is grounded in what's actually installed. ``llm`` (optional) is the
    cheap model used only to *confirm* a weak heuristic — never required, so the
    router stays testable and offline-safe.
    """

    def __init__(
        self,
        llm: Any = None,
        registry: Any = None,
        memory: Any = None,
        audit_log: Optional[RoutingAuditLog] = None,
        *,
        min_confidence: float = 0.7,
        escalation_budget: int = 2,
        use_model: bool = True,
        model_band: Tuple[float, float] = (0.45, 0.8),
    ):
        self._llm = llm
        self._registry = registry
        self._memory = memory
        self._audit = audit_log
        self.min_confidence = min_confidence
        self.escalation_budget = escalation_budget
        self._use_model = use_model
        # Only ask the model when the heuristic is genuinely undecided (inside
        # this band) — confident heuristics aren't second-guessed (cost economy).
        self._model_band = model_band

    # ------------------------------------------------------------------
    # Platform detection (grounded in the registry when available)
    # ------------------------------------------------------------------
    def _known_platforms(self) -> List[str]:
        if self._registry is None:
            return ["snowflake", "github", "pinecone"]
        try:
            return [
                d["id"] for d in self._registry.get_tool_digest()
            ] or ["snowflake", "github", "pinecone"]
        except Exception:
            return ["snowflake", "github", "pinecone"]

    def _platforms_in(self, task: str) -> List[str]:
        low = task.lower()
        hits = []
        # Platform aliases beyond the bare id so "S3"/"warehouse" still match.
        aliases = {
            "snowflake": ["snowflake", "warehouse", "bronze", "silver", "gold", "sql"],
            "github": ["github", "git", "repo", "commit", "workflow", "dbt"],
            "pinecone": ["pinecone", "vector", "docs", "documentation"],
            "s3": ["s3", "bucket"],
            "gcs": ["gcs", "cloud storage"],
        }
        for platform in self._known_platforms():
            keys = aliases.get(platform, [platform])
            if any(k in low for k in keys):
                hits.append(platform)
        # Cross-platform words that imply a second system even if unnamed.
        for extra, keys in aliases.items():
            if extra in hits:
                continue
            if any(k in low for k in keys) and extra in ("s3", "gcs"):
                hits.append(extra)
        return hits

    # ------------------------------------------------------------------
    # Heuristic classification
    # ------------------------------------------------------------------
    def _heuristic(self, task: str) -> Tuple[Tier, Optional[str], float, str]:
        low = (task or "").strip().lower()
        if not low:
            return Tier.TOOL, None, 0.2, "empty task — cannot classify"

        platforms = self._platforms_in(low)
        multistep = [s for s in _MULTISTEP_SIGNALS if s in low]
        large = [s for s in _LARGE_DATA_SIGNALS if s in low]
        single_verb = next((v for v in _SINGLE_OP_VERBS if low.startswith(v) or f" {v}" in low), None)
        first_word = low.split()[0] if low.split() else ""

        # Strongest sandbox signal: genuinely cross-platform. (Destructive
        # platform mutations stay on the verified connector/sandbox path, never
        # the free-text shell — connector-preference.)
        if len(set(platforms)) >= 2:
            return (Tier.SANDBOX, "sandbox", 0.9,
                    f"cross-platform ({', '.join(sorted(set(platforms)))}) → sandbox")

        # Multi-step or large-data → sandbox.
        if multistep or large:
            why = multistep[0] if multistep else large[0]
            return (Tier.SANDBOX, "sandbox", 0.85,
                    f"multi-step/large-data signal '{why.strip()}' → sandbox")

        # Shell tier (Era 2): an explicit "do it in the terminal" cue is a user
        # override honored even when a platform is named; otherwise a leading
        # local-glue command with NO typed connector to prefer routes to shell.
        if any(cue in low for cue in _SHELL_CUES):
            return (Tier.SHELL, "shell", 0.85,
                    "explicit terminal/shell cue → shell tier")
        if first_word in _LEADING_GLUE and not platforms:
            return (Tier.SHELL, "shell", 0.8,
                    f"local glue command '{first_word}' with no typed connector op → shell tier")

        # Single, scoped op on one named platform → tool tier.
        if len(platforms) == 1 and single_verb:
            return (Tier.TOOL, platforms[0], 0.9,
                    f"single op '{single_verb.strip()}' on {platforms[0]} → tool")

        if len(platforms) == 1:
            return (Tier.TOOL, platforms[0], 0.72,
                    f"single platform {platforms[0]}, op unclear → tool (low margin)")

        if single_verb:
            return (Tier.TOOL, None, 0.55,
                    f"read-style verb '{single_verb.strip()}' but no platform named → tool (uncertain)")

        # No usable signal — let escalation decide.
        return (Tier.TOOL, None, 0.3, "no platform/op/step signal → ambiguous")

    async def classify(self, task: str) -> RoutingDecision:
        tier, target, confidence, rationale = self._heuristic(task)

        # Optional cheap-model confirmation, only inside the undecided band.
        lo, hi = self._model_band
        if self._llm is not None and self._use_model and lo <= confidence < hi:
            model_tier, note = await self._confirm_with_model(task)
            if model_tier is not None:
                if model_tier == tier:
                    confidence = min(0.85, confidence + 0.15)
                    rationale += f"; model agreed ({note})"
                else:
                    tier = model_tier
                    target = "sandbox" if tier == Tier.SANDBOX else target
                    confidence = 0.7
                    rationale += f"; model overrode to {tier.value} ({note})"

        return RoutingDecision(
            task=task, tier=tier.value, target=target,
            confidence=round(confidence, 3), rationale=rationale,
        )

    async def _confirm_with_model(self, task: str) -> Tuple[Optional[Tier], str]:
        try:
            label = await self._llm.classify(
                task,
                labels=["tool", "sandbox"],
                instructions=(
                    "You route a data-engineering task to an execution tier. "
                    "'tool' = a single, well-scoped operation on ONE platform. "
                    "'sandbox' = multi-step, large-data, or cross-platform work. "
                    "Answer with exactly one label."
                ),
            )
            label = (label or "").strip().lower()
            if label in ("tool", "sandbox"):
                return Tier(label), "cheap-model"
        except Exception:
            pass
        return None, "model unavailable"

    # ------------------------------------------------------------------
    # Escalation (4.3)
    # ------------------------------------------------------------------
    def _escalate(self, decision: RoutingDecision) -> Tuple[str, Optional[str], float, str]:
        """One escalation step: tool→sandbox, then nudge confidence upward.

        Escalating to the more general tier *increases* our ability to handle the
        task, so confidence rises; a task we still can't place after the budget
        is exhausted is surfaced to a human rather than guessed.
        """
        if decision.tier == Tier.TOOL.value:
            return (Tier.SANDBOX.value, "sandbox", min(1.0, decision.confidence + 0.15),
                    "tool→sandbox: single-op route was low-confidence; using general tier")
        if decision.tier == Tier.SHELL.value:
            return (Tier.SANDBOX.value, "sandbox", min(1.0, decision.confidence + 0.15),
                    "shell→sandbox: low-confidence local route; using the general code tier")
        # Already at sandbox: the only thing more capable is a human.
        return (decision.tier, decision.target, min(1.0, decision.confidence + 0.15),
                "sandbox confidence still low; one step closer to human review")

    async def route(self, task: str) -> RoutingDecision:
        """Classify, then escalate while under threshold (bounded), then log."""
        decision = await self.classify(task)
        attempts = 0
        trail: List[str] = [f"classified: {decision.tier} ({decision.confidence}) — {decision.rationale}"]

        while decision.confidence < self.min_confidence and attempts < self.escalation_budget:
            attempts += 1
            tier, target, confidence, reason = self._escalate(decision)
            trail.append(f"escalate #{attempts}: {reason} -> {tier} ({round(confidence, 3)})")
            decision.tier = tier
            decision.target = target
            decision.confidence = round(confidence, 3)
            decision.escalations = attempts

        if decision.confidence < self.min_confidence:
            # Budget exhausted and still uncertain — hand to the user with the trail.
            decision.escalation_target = "human"
            decision.surfaced_to_user = True
            trail.append("escalation budget exhausted → surfaced to user")

        decision.trail = trail
        if self._audit is not None:
            self._audit.log(decision)
        return decision
