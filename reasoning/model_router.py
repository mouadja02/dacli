"""Model tiering (ℛ) — Phase 6, workstream 6.3.

The reasoning substrate is no longer "one model for everything". Two tiers:

* **cheap** — classification, planning drafts, summarization, post-condition
  judgments. High-volume, low-stakes calls where a small fast model is adequate.
* **strong** — ambiguous reasoning, error diagnosis, and plans for irreversible
  actions. Low-volume, high-stakes calls where mistakes are expensive.

The paper that grounds this phase found **token usage explained ~80% of the
performance variance** — so the lever is *spend tokens where they matter*. A
``ModelRouter`` picks the tier by the *kind* of call and its *stakes*, and
**escalates weak→strong** on low confidence or a failed verification (never the
reverse — once a step is hard, it stays on the strong model). Every choice is
logged so the "cheap for classification, strong for diagnosis" split is visible
in the audit trail (a Phase 6 exit criterion).

Offline-safe: the router is a pure decision function plus a thin ``generate``
wrapper around the existing :class:`~reasoning.llm.LLMClient`. With no live model
it still routes (and is fully testable); the resolved model id falls back to the
configured ``model`` so a single-model deployment behaves exactly as before.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


class ModelTier(str, Enum):
    CHEAP = "cheap"
    STRONG = "strong"


class Stakes(str, Enum):
    """How expensive a wrong answer is — orthogonal to the call *kind*."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


# The canonical map from call *kind* to its default tier. Kept explicit (not a
# learned policy) so a routing decision is auditable and stable run to run. The
# kinds mirror the Phase 6 plan verbatim.
_CHEAP_KINDS = {
    "classification",      # task/tier classification (the router from Phase 4)
    "planning_draft",      # first-pass DAG decomposition
    "summarization",       # condensing a sub-agent result / spilled output
    "postcondition_judgment",  # the rare model-judged post-condition (anchored=False)
    "routing",             # any cheap dispatch decision
}
_STRONG_KINDS = {
    "ambiguous_reasoning",  # the goal/data is genuinely under-determined
    "diagnosis",            # error diagnosis for self-correction (6.4)
    "irreversible_plan",    # a plan that includes an irreversible action
    "synthesis",            # the lead merging conflicting sub-agent results
}


@dataclass
class ModelChoice:
    """An auditable model-routing decision (mirrors the routing log shape)."""

    kind: str
    tier: str
    model: str
    stakes: str
    rationale: str
    confidence: Optional[float] = None
    escalated: bool = False
    trail: List[str] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        return asdict(self)


class ModelRoutingAuditLog:
    """Append-only JSONL log of every model-tier decision (audit + calibration)."""

    def __init__(self, path: str = ".dacli/model_routing.jsonl"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, choice: ModelChoice) -> None:
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(choice.to_dict(), default=str) + "\n")

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


class ModelRouter:
    """Routes each reasoning call to a model tier with weak→strong escalation.

    ``cheap_model`` / ``strong_model`` are concrete model ids; either may be None,
    in which case the configured default ``model`` is used (so a single-model
    deployment is unchanged). ``llm`` is optional — only :meth:`generate` needs
    it; :meth:`choose` is a pure function and works offline.
    """

    def __init__(
        self,
        llm: Any = None,
        *,
        cheap_model: Optional[str] = None,
        strong_model: Optional[str] = None,
        default_model: Optional[str] = None,
        audit_log: Optional[ModelRoutingAuditLog] = None,
        min_confidence: float = 0.7,
    ):
        self._llm = llm
        # Resolve the default model id: explicit arg, else the client's configured
        # ``settings.llm.model``, else empty (let the client decide at call time).
        llm_settings = getattr(getattr(llm, "settings", None), "llm", None)
        self._default = default_model or getattr(llm_settings, "model", None)
        self._cheap = cheap_model or self._default
        self._strong = strong_model or self._default
        self._audit = audit_log
        self.min_confidence = min_confidence

    # ------------------------------------------------------------------
    def _model_for(self, tier: ModelTier) -> str:
        model = self._strong if tier == ModelTier.STRONG else self._cheap
        return model or self._default or ""

    def _base_tier(self, kind: str, stakes: Stakes) -> Tuple[ModelTier, str]:
        """The tier before any escalation: from the kind, raised by HIGH stakes."""
        if kind in _STRONG_KINDS:
            return ModelTier.STRONG, f"kind '{kind}' is a strong-model job"
        if stakes == Stakes.HIGH:
            return ModelTier.STRONG, f"kind '{kind}' but stakes=high → strong"
        if kind in _CHEAP_KINDS:
            return ModelTier.CHEAP, f"kind '{kind}' is a cheap-model job"
        # Unknown kind: default to cheap on low/medium stakes, the conservative
        # spend; an unknown high-stakes kind already routed strong above.
        return ModelTier.CHEAP, f"kind '{kind}' unmapped → cheap (low/medium stakes)"

    def choose(
        self,
        kind: str,
        *,
        stakes: Stakes = Stakes.MEDIUM,
        confidence: Optional[float] = None,
        irreversible: bool = False,
        after_failed_verification: bool = False,
    ) -> ModelChoice:
        """Decide the tier for one call. Pure + deterministic (no I/O except log).

        Escalates cheap→strong when any *hard-signal* fires:
        - an irreversible action is in scope,
        - the previous attempt failed verification (informed retry, 6.4),
        - confidence is below ``min_confidence``.
        Strong never de-escalates to cheap.
        """
        tier, why = self._base_tier(kind, stakes)
        trail = [f"base: {tier.value} — {why}"]
        escalated = False

        reasons = []
        if irreversible:
            reasons.append("irreversible action in scope")
        if after_failed_verification:
            reasons.append("previous attempt failed verification")
        if confidence is not None and confidence < self.min_confidence:
            reasons.append(f"confidence {round(confidence, 3)} < {self.min_confidence}")

        if reasons and tier != ModelTier.STRONG:
            tier = ModelTier.STRONG
            escalated = True
            trail.append("escalate cheap→strong: " + "; ".join(reasons))
        elif reasons:
            trail.append("already strong; hard signals: " + "; ".join(reasons))

        choice = ModelChoice(
            kind=kind,
            tier=tier.value,
            model=self._model_for(tier),
            stakes=stakes.value,
            rationale=trail[-1],
            confidence=confidence,
            escalated=escalated,
            trail=trail,
        )
        if self._audit is not None:
            self._audit.log(choice)
        return choice

    async def generate(
        self,
        kind: str,
        messages: List[Dict[str, str]],
        *,
        tools: Optional[List[Dict]] = None,
        system_prompt: Optional[str] = None,
        on_text: Any = None,
        stakes: Stakes = Stakes.MEDIUM,
        confidence: Optional[float] = None,
        irreversible: bool = False,
        after_failed_verification: bool = False,
    ) -> Tuple[str, List[Dict], ModelChoice]:
        """Choose a tier, then run the call on that tier's model.

        Returns the usual ``(content, tool_calls)`` plus the :class:`ModelChoice`
        so the caller can record which tier actually ran (and re-issue on the
        strong tier if its own verification later fails).
        """
        choice = self.choose(
            kind, stakes=stakes, confidence=confidence,
            irreversible=irreversible, after_failed_verification=after_failed_verification,
        )
        if self._llm is None:
            raise RuntimeError("ModelRouter.generate requires an LLM client")
        content, tool_calls = await self._llm.generate(
            messages=messages, tools=tools, system_prompt=system_prompt,
            on_text=on_text, model=choice.model,
        )
        return content, tool_calls, choice
