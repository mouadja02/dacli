"""Audit ledger (𝒢) — *decisions, not just calls*.

The tool log records *what ran*. This ledger records *why* — the chain
of reliability decisions that let (or stopped) an action:

* the classifier's tier and its reasoning,
* the policy decision and which override (if any) produced it,
* the permission/scope check,
* the rollback plan attached (and whether it was verified to exist),
* the human approval / denial,
* the post-condition outcome,
* memory writes.

It is **append-only** (immutability is what makes it an audit trail) and stored
as JSON Lines so it survives across sessions and is greppable. ``dacli audit``
reads it back to reconstruct "why did the agent do that?" end to end.
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

from dacli.core.logging_setup import get_logger
from dacli.core.timeutils import now_iso as _now_iso

log = get_logger(__name__)


@dataclass
class AuditEvent:
    """One reconstructable reliability decision/outcome."""

    kind: str                       # "classification" | "policy" | "permission"
                                    # | "rollback" | "approval" | "execution"
                                    # | "post_condition" | "memory_write" | "block"
    tool_name: str
    session_id: str = ""
    decision_id: str = ""           # correlates all events for one action
    actor: str = "agent"            # "agent" | "sandbox" | "human"
    tier: str | None = None
    summary: str = ""
    detail: dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=_now_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class AuditLedger:
    """Append-only JSONL ledger of governance events."""

    def __init__(self, path: str = ".dacli/audit.jsonl"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # A process-local lock so concurrent tool calls (and the sandbox SDK)
        # never interleave a half-written line. Append mode keeps it atomic-ish
        # per line on POSIX/NTFS for the small records we write.
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # write
    # ------------------------------------------------------------------
    def append(self, event: AuditEvent) -> None:
        line = json.dumps(event.to_dict(), default=str)
        with self._lock:
            try:
                with open(self.path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
                    f.flush()
                    os.fsync(f.fileno())
            except Exception:
                # Telemetry must never crash the control loop — but unlike usage
                # tracking, a *silently* lost audit record is a reliability hole,
                # so we still surface it (at error level, with the traceback).
                log.error("failed to write audit event %s", event.kind, exc_info=True)

    def log(
        self,
        kind: str,
        tool_name: str,
        *,
        session_id: str = "",
        decision_id: str = "",
        actor: str = "agent",
        tier: str | None = None,
        summary: str = "",
        **detail: Any,
    ) -> AuditEvent:
        event = AuditEvent(
            kind=kind, tool_name=tool_name, session_id=session_id,
            decision_id=decision_id, actor=actor, tier=tier, summary=summary,
            detail=detail,
        )
        self.append(event)
        return event

    # ------------------------------------------------------------------
    # read / reconstruct
    # ------------------------------------------------------------------
    def all_events(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        out: list[dict[str, Any]] = []
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    continue
        return out

    def events(
        self,
        *,
        session_id: str | None = None,
        kind: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.all_events()
        if session_id:
            rows = [r for r in rows if r.get("session_id") == session_id]
        if kind:
            rows = [r for r in rows if r.get("kind") == kind]
        return rows

    def sessions(self) -> list[str]:
        seen: list[str] = []
        for r in self.all_events():
            sid = r.get("session_id") or ""
            if sid and sid not in seen:
                seen.append(sid)
        return seen

    def decisions(self, session_id: str | None = None) -> list[dict[str, Any]]:
        """Group the flat event stream into one record per governed action.

        Returns a list of ``{decision_id, tool_name, tier, events:[...]}`` in
        first-seen order — the structure ``dacli audit`` renders to answer
        "why did the agent do that?".
        """
        grouped: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for r in self.events(session_id=session_id):
            did = r.get("decision_id") or f"_{len(order)}"
            if did not in grouped:
                grouped[did] = {
                    "decision_id": did,
                    "session_id": r.get("session_id", ""),
                    "tool_name": r.get("tool_name", ""),
                    "tier": r.get("tier"),
                    "started_at": r.get("timestamp"),
                    "events": [],
                }
                order.append(did)
            rec = grouped[did]
            rec["events"].append(r)
            if r.get("tier") and not rec.get("tier"):
                rec["tier"] = r.get("tier")
            if r.get("tool_name") and not rec.get("tool_name"):
                rec["tool_name"] = r.get("tool_name")
        return [grouped[d] for d in order]
