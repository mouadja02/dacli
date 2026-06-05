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


import os
import tempfile


def _write_minimal_connectors_config(settings: Any) -> str:
    """Write a temp connectors.yaml that disables every external connector.

    Built-ins (system/skills/sandbox/shell) are injected by the agent and stay
    available; this only forces the manifest-discovered platform connectors off
    so a headless run makes no external network calls.
    """
    import yaml
    from connectors.registry import ConnectorRegistry

    try:
        ids = ConnectorRegistry(settings).get_connector_ids()
    except Exception:
        ids = []
    cfg = {"setup_completed": True,
           "connectors": {cid: {"enabled": False} for cid in ids}}
    fd, path = tempfile.mkstemp(suffix="_headless_connectors.yaml")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)
    return path


def _session_usage(agent: Any, session_id: str) -> Dict[str, Any]:
    try:
        sess = agent.store.usage_summary(session_id).get("session") or {}
    except Exception:
        sess = {}
    return {
        "requests": sess.get("requests", 0),
        "input": sess.get("input", 0),
        "output": sess.get("output", 0),
        "costUSD": sess.get("costUSD", 0.0),
    }


class _CannedInputExhausted(RuntimeError):
    """The agent asked the user something with no canned answer left."""


async def run_headless(
    *,
    inputs: List[str],
    settings: Any,
    llm: Optional[object] = None,
    approve: Any = "deny",
    canned_inputs: Optional[List[str]] = None,
    session_id: Optional[str] = None,
    no_connectors: bool = True,
    max_iterations: Optional[int] = None,
) -> HeadlessResult:
    """Drive the agent over ``inputs`` with no interactive I/O.

    ``llm`` injects a :class:`~reasoning.scripted.ScriptedLLM` for offline runs;
    ``None`` uses the configured provider. ``approve`` is ``"deny"`` (default,
    fail-safe), ``"approve"``, or a list of booleans consumed in order.
    """
    from core.agent import DACLI
    from core.memory import AgentMemory
    from connectors.registry import CONNECTORS_CONFIG_PATH
    from reasoning.scripted import ScriptExhausted

    if max_iterations is not None:
        try:
            settings.agent.max_iterations = int(max_iterations)
        except Exception:
            pass

    cfg_path = CONNECTORS_CONFIG_PATH
    tmp_cfg: Optional[str] = None
    if no_connectors:
        tmp_cfg = _write_minimal_connectors_config(settings)
        cfg_path = tmp_cfg

    memory = AgentMemory(
        state_path=settings.agent.state_path,
        history_path=settings.agent.history_path,
        memory_window=settings.agent.memory_window,
    )
    if session_id:
        memory.load_session(session_id)
    sid = memory.session_id

    # Per-turn tool-call capture. on_tool_start records name+args; on_tool_end
    # fills status+error (governance blocks fire on_tool_end with DENIED/BLOCKED).
    current_calls: List[Dict[str, Any]] = []

    def on_tool_start(name: str, args: Dict[str, Any]) -> None:
        current_calls.append({"name": name, "args": args, "status": None, "error": None})

    def on_tool_end(name: str, result: Any) -> None:
        status = getattr(getattr(result, "status", None), "value", None)
        for rec in reversed(current_calls):
            if rec["name"] == name and rec["status"] is None:
                rec["status"] = status
                rec["error"] = getattr(result, "error", None)
                return
        current_calls.append({"name": name, "args": {}, "status": status,
                              "error": getattr(result, "error", None)})

    approvals = list(approve) if isinstance(approve, list) else None

    def on_approval(_request: Any) -> bool:
        if approvals is not None:
            return bool(approvals.pop(0)) if approvals else False
        return approve == "approve"

    canned = list(canned_inputs or [])

    def on_user_input_needed(question: str) -> str:
        if not canned:
            raise _CannedInputExhausted(
                f"agent requested input with no canned answer left: {question!r}"
            )
        return canned.pop(0)

    agent = DACLI(
        settings=settings,
        memory=memory,
        llm=llm,
        on_tool_start=on_tool_start,
        on_tool_end=on_tool_end,
        on_approval=on_approval,
        on_user_input_needed=on_user_input_needed,
        connectors_config_path=cfg_path,
    )

    result = HeadlessResult(session_id=sid)
    ledger = getattr(getattr(agent, "governor", None), "ledger", None)
    seen_decisions = 0

    try:
        await agent.initialize()
        for msg in inputs:
            current_calls.clear()
            turn = TurnRecord(input=msg)
            try:
                resp = await agent.process_message(msg)
                # The kernel catches ALL exceptions and surfaces them as
                # AgentResponse(error=...). Re-raise ScriptExhausted so the
                # headless driver can distinguish a scenario-script overrun
                # (exit 3) from a genuine agent error (exit 1).
                if resp.error and "ScriptedLLM exhausted" in resp.error:
                    raise ScriptExhausted(resp.error)
                turn.content = resp.content or ""
                turn.error = resp.error
                turn.needs_user_input = bool(resp.needs_user_input)
                turn.iterations = getattr(resp, "iteration", 0)
            except (ScriptExhausted, _CannedInputExhausted) as exc:
                turn.tool_calls = list(current_calls)
                result.turns.append(turn)
                result.scenario_error = str(exc)
                break
            except Exception as exc:  # noqa: BLE001 - surface as a turn error
                turn.error = repr(exc)
            turn.tool_calls = list(current_calls)
            if ledger is not None:
                decs = ledger.decisions(session_id=sid)
                turn.governance = decs[seen_decisions:]
                seen_decisions = len(decs)
            result.turns.append(turn)
    finally:
        try:
            await agent.shutdown()
        except Exception:
            pass
        result.usage = _session_usage(agent, sid)
        result.audit_path = str(getattr(ledger, "path", "")) if ledger is not None else ""
        if tmp_cfg:
            try:
                os.unlink(tmp_cfg)
            except Exception:
                pass

    return result
