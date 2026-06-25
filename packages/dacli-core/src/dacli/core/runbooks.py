"""Governed runbooks + scheduling (P14, slice B).

A runbook is a persisted, parameterized headless task with a **policy envelope**:
approvals are pre-granted *only within* the envelope (a set of allowed tools and
a tier ceiling); anything outside still prompts — which, on the headless path
with no interactive approver, means it blocks (fail-closed). The envelope never
widens the secure defaults globally; it is scoped to the one runbook and every
in/out-of-envelope decision lands in the audit ledger.

Runbooks run over the existing headless contract (:func:`core.headless.run_headless`),
so cron/CI drive them exactly like ``dacli run`` / ``dacli replay``. They persist
under P01's state dir (``<state_dir>/runbooks/<name>.yaml``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dacli.core.logging_setup import get_logger
from dacli.core.paths import state_dir
from dacli.governance.vocab import Tier, rank

log = get_logger(__name__)


@dataclass
class PolicyEnvelope:
    """The pre-granted approval scope for one runbook.

    ``tools`` lists the tool names auto-approved within the run (``["*"]`` allows
    any tool); ``max_tier`` is the blast-radius ceiling. An action is permitted
    only if its tool is listed *and* its tier is at or below the ceiling —
    anything else is refused, so the envelope can never escalate beyond what it
    declares.
    """

    tools: list[str] = field(default_factory=list)
    max_tier: str = "write"

    def _ceiling(self) -> Tier:
        try:
            return Tier(self.max_tier)
        except ValueError:
            return Tier.WRITE

    def permits(self, request: Any) -> tuple[bool, str]:
        tool = getattr(request, "tool_name", "") or ""
        tier = getattr(request, "tier", None)
        ceiling = self._ceiling()
        if "*" not in self.tools and tool not in self.tools:
            return False, f"tool '{tool}' is outside the envelope (tools={self.tools})"
        if isinstance(tier, Tier) and rank(tier) > rank(ceiling):
            return False, (f"tier '{tier.value}' exceeds the envelope ceiling "
                           f"'{ceiling.value}'")
        return True, f"within envelope (tool '{tool}', tier ≤ '{ceiling.value}')"

    def to_dict(self) -> dict[str, Any]:
        return {"tools": list(self.tools), "max_tier": self.max_tier}

    @classmethod
    def from_dict(cls, d: dict[str, Any] | None) -> PolicyEnvelope:
        d = d or {}
        return cls(tools=list(d.get("tools") or []),
                   max_tier=str(d.get("max_tier") or "write"))


@dataclass
class Runbook:
    """A saved, parameterized headless task + its policy envelope."""

    name: str
    turns: list[str] = field(default_factory=list)
    params: dict[str, str] = field(default_factory=dict)
    envelope: PolicyEnvelope = field(default_factory=PolicyEnvelope)
    no_connectors: bool = True

    def render(self, params: dict[str, str] | None = None) -> list[str]:
        """The turns with ``{param}`` placeholders filled (defaults + overrides)."""
        merged = {**self.params, **(params or {})}
        try:
            return [t.format(**merged) for t in self.turns]
        except KeyError as e:
            raise ValueError(f"runbook '{self.name}' is missing param {e}") from e

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "turns": list(self.turns),
            "params": dict(self.params),
            "envelope": self.envelope.to_dict(),
            "no_connectors": self.no_connectors,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Runbook:
        return cls(
            name=str(d.get("name") or ""),
            turns=list(d.get("turns") or []),
            params=dict(d.get("params") or {}),
            envelope=PolicyEnvelope.from_dict(d.get("envelope")),
            no_connectors=bool(d.get("no_connectors", True)),
        )


# ---------------------------------------------------------------------------
# Persistence (under P01's state dir)
# ---------------------------------------------------------------------------
def _runbook_dir():
    return state_dir() / "runbooks"


def _runbook_path(name: str):
    return _runbook_dir() / f"{name}.yaml"


def save_runbook(runbook: Runbook) -> None:
    import yaml

    d = _runbook_dir()
    d.mkdir(parents=True, exist_ok=True)
    _runbook_path(runbook.name).write_text(
        yaml.safe_dump(runbook.to_dict(), sort_keys=False), encoding="utf-8")


def load_runbook(name: str) -> Runbook | None:
    import yaml

    path = _runbook_path(name)
    try:
        return Runbook.from_dict(yaml.safe_load(path.read_text(encoding="utf-8")) or {})
    except Exception:
        log.debug("could not load runbook %s", path, exc_info=True)
        return None


def list_runbooks() -> list[str]:
    d = _runbook_dir()
    if not d.exists():
        return []
    return sorted(p.stem for p in d.glob("*.yaml"))


def delete_runbook(name: str) -> bool:
    path = _runbook_path(name)
    if not path.exists():
        return False
    path.unlink()
    return True


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
async def run_runbook(
    runbook: Runbook,
    *,
    settings: Any,
    params: dict[str, str] | None = None,
    llm: object | None = None,
    audit: bool = True,
):
    """Run ``runbook`` headlessly under its policy envelope.

    The envelope becomes the headless approver: in-envelope actions auto-approve,
    out-of-envelope actions are refused (fail-closed). Every decision is recorded
    and, with ``audit`` on, written to the audit ledger alongside the envelope.
    """
    from dacli.core.headless import run_headless

    inputs = runbook.render(params)
    decisions: list[dict[str, Any]] = []

    def approve(request: Any) -> bool:
        permitted, reason = runbook.envelope.permits(request)
        decisions.append({
            "tool": getattr(request, "tool_name", ""),
            "tier": getattr(getattr(request, "tier", None), "value", None),
            "permitted": permitted,
            "reason": reason,
        })
        return permitted

    result = await run_headless(
        inputs=inputs, settings=settings, llm=llm, approve=approve,
        no_connectors=runbook.no_connectors,
    )
    if audit:
        _audit_envelope(runbook, decisions, result)
    return result


def _audit_envelope(runbook: Runbook, decisions: list[dict[str, Any]], result: Any) -> None:
    """Record the envelope and every approval decision in the audit ledger."""
    path = getattr(result, "audit_path", "") or ""
    if not path:
        return
    try:
        from dacli.governance.audit import AuditLedger

        AuditLedger(path=path).log(
            "runbook", runbook.name,
            session_id=getattr(result, "session_id", ""),
            summary=(f"runbook '{runbook.name}' ran under envelope "
                     f"tools={runbook.envelope.tools} max_tier={runbook.envelope.max_tier}"),
            envelope=runbook.envelope.to_dict(), decisions=decisions,
        )
    except Exception:
        log.debug("failed to audit runbook envelope", exc_info=True)
