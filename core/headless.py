"""Headless, auditable driver for the dacli agent.

Drives :class:`core.agent.DACLI` with no interactive I/O and aggregates a
machine-readable :class:`HeadlessResult` covering the four assertable
dimensions: final answer text, tool calls made, governance decisions, and
token/cost usage. Backs ``dacli run`` and ``dacli replay``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# Exit-code contract (consumed by CI steps and AI agents driving the terminal).
EXIT_OK = 0
EXIT_AGENT_ERROR = 1
EXIT_GOVERNANCE_BLOCK = 2
EXIT_SCENARIO_ERROR = 3

# ToolStatus.DENIED / BLOCKED string values (a blocked action's result status).
_BLOCK_STATUSES = {"denied", "blocked"}


@dataclass
class TurnRecord:
    """One user message -> agent outcome."""

    input: str
    content: str = ""
    error: Optional[str] = None
    needs_user_input: bool = False
    iterations: int = 0
    tool_calls: List[Dict[str, Any]] = field(default_factory=list)
    governance: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "input": self.input,
            "content": self.content,
            "error": self.error,
            "needs_user_input": self.needs_user_input,
            "iterations": self.iterations,
            "tool_calls": self.tool_calls,
            "governance": self.governance,
        }


@dataclass
class HeadlessResult:
    """The aggregated result of a headless run (one or more turns)."""

    session_id: str
    turns: List[TurnRecord] = field(default_factory=list)
    usage: Dict[str, Any] = field(default_factory=dict)
    audit_path: str = ""
    scenario_error: Optional[str] = None

    @property
    def exit_code(self) -> int:
        # Precedence: scenario error (3) > governance block (2) > agent error (1).
        if self.scenario_error is not None:
            return EXIT_SCENARIO_ERROR
        code = EXIT_OK
        for t in self.turns:
            if any(tc.get("status") in _BLOCK_STATUSES for tc in t.tool_calls):
                code = max(code, EXIT_GOVERNANCE_BLOCK)
            if t.error or t.needs_user_input:
                code = max(code, EXIT_AGENT_ERROR)
        return code

    @property
    def ok(self) -> bool:
        return self.exit_code == EXIT_OK

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "exit_code": self.exit_code,
            "session_id": self.session_id,
            "turns": [t.to_dict() for t in self.turns],
            "usage": self.usage,
            "audit_path": self.audit_path,
            "scenario_error": self.scenario_error,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)
