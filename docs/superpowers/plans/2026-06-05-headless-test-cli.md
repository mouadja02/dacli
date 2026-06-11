# Headless Test CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a non-interactive, auditable way to drive the real dacli agent — `dacli run` (one-shot) and `dacli replay <file>` (scenario) — emitting stable JSON (final text, tool calls, governance decisions, token/cost) for CI e2e/smoke tests and AI-agent terminal use, backed by a deterministic offline scripted LLM.

**Architecture:** One seam in `core/agent.py` (inject the LLM), a `ScriptedLLM` test double in `reasoning/`, a `HeadlessRunner` in `core/headless.py` that wires the agent with recording/non-interactive callbacks and aggregates a `HeadlessResult`, and two Click commands in `scripts/cli.py`. No change to the interactive chat behavior.

**Tech Stack:** Python 3.10+, Click, asyncio, PyYAML, `unittest` (repo's test runner), `click.testing.CliRunner`.

---

## Background the engineer needs

- The agent's programmatic interface (`core/agent.py`):
  - `DACLI(settings, memory=None, on_status_update=None, on_tool_start=None, on_tool_end=None, on_user_input_needed=None, on_approval=None, on_stream_start=None, on_text=None, on_stream_end=None, connectors_config_path=CONNECTORS_CONFIG_PATH, store=None)` — **we add `llm=None`**.
  - `await agent.initialize() -> bool`, `await agent.process_message(text) -> AgentResponse`, `await agent.shutdown()`.
  - `AgentResponse` (`core/kernel.py:15`) fields: `content`, `tool_calls`, `thinking`, `needs_user_input`, `error`, `iteration`.
  - `agent.governor` may be `None` (governance disabled); else `agent.governor.ledger.decisions(session_id=...) -> List[{decision_id, session_id, tool_name, tier, started_at, events:[...]}]` and `agent.governor.ledger.path`.
  - `agent.store.usage_summary(session_id) -> {numStartups, totals, byModel, session}`; `session` bucket keys: `input, output, cache_read, cache_creation, requests, costUSD`.
  - `agent.memory.session_id` is the live session id; `agent.memory.load_session(id) -> bool`.
- The dispatcher (`connectors/dispatcher.py`) invokes callbacks `on_tool_start(tool_name, args_dict)` and `on_tool_end(tool_name, ToolResult)`. A governance-blocked call still fires `on_tool_end` with a `ToolResult` whose `status` is `ToolStatus.DENIED` or `ToolStatus.BLOCKED`.
- `ToolResult` (`connectors/base.py:27`): `.tool_name`, `.status` (`ToolStatus` enum; `.status.value` is a string — `ToolStatus.SUCCESS.value == "success"` — always compare via the enum), `.error`, `.data`, `.metadata`, `.success`.
- The LLM contract the kernel depends on (`core/kernel.py:184`): `await llm.generate(messages=..., tools=..., system_prompt=..., on_text=..., [model=...]) -> (content, tool_calls)`, plus `llm.last_usage: Dict[str,int]` read after each call. `tool_calls` is a list of `{"id": str, "name": str, "arguments": dict}`. An empty list ends the loop (final answer).
- A safe built-in op for tests: `update_plan` (connector `system`, risk SAFE, mutates memory todos only). A deterministic offline governance **block**: `run_shell_command` with an irreversible command like `dd if=/dev/zero of=/tmp/zz bs=1M count=1` — the shell command classifier flags `dd` as irreversible (pure string parsing, never executed), and the shell tier's default scope is `write`, so governance blocks it before `invoke`.
- `fetch_pricing` runs inside `DACLI.__init__` (`core/agent.py:77`) and does a bounded `httpx.get(timeout=10.0)` with a cache. **Unit tests must stub `core.agent.fetch_pricing` to return `None`** to stay fast and offline.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `core/agent.py` | (modify) add `llm=None` injection param. |
| `reasoning/scripted.py` | (new) `ScriptedLLM`, `ScriptExhausted` — deterministic offline LLM double. |
| `core/headless.py` | (new) `HeadlessResult`, `TurnRecord`, exit-code constants, `run_headless()`, helpers. |
| `scripts/cli.py` | (modify) `run` and `replay` Click commands + a small async helper. |
| `tests/test_headless.py` | (new) TDD coverage for all of the above. |
| `scenarios/smoke_headless.json` | (new) committed offline scenario for CI. |
| `.github/workflows/ci.yml` | (modify) add a step running the offline scenario. |
| `docs/TESTING.md` | (new) JSON contract, exit codes, CI + AI-agent usage patterns. |

---

## Task 1: LLM injection seam in DACLI

**Files:**
- Modify: `core/agent.py` (`__init__` signature ~line 45-60, and `self.llm = LLMClient(settings)` at line 70)
- Test: `tests/test_headless.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_headless.py` with:

```python
import unittest
from unittest import mock

from config.settings import load_config


class _Sentinel:
    """A stand-in object accepted anywhere the LLM client is expected."""
    async def initialize(self):
        return None


class LLMInjectionTest(unittest.TestCase):
    def setUp(self):
        # Keep construction offline + fast.
        self._pricing_patch = mock.patch("core.agent.fetch_pricing", return_value=None)
        self._pricing_patch.start()
        self.addCleanup(self._pricing_patch.stop)

    def test_injected_llm_is_used(self):
        from core.agent import DACLI
        settings = load_config()
        try:
            settings.sandbox.enabled = False
        except Exception:
            pass
        sentinel = _Sentinel()
        agent = DACLI(settings=settings, llm=sentinel)
        self.assertIs(agent.llm, sentinel)

    def test_default_llm_constructed_when_none(self):
        from core.agent import DACLI
        from reasoning.llm import LLMClient
        settings = load_config()
        try:
            settings.sandbox.enabled = False
        except Exception:
            pass
        agent = DACLI(settings=settings)
        self.assertIsInstance(agent.llm, LLMClient)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_headless.LLMInjectionTest -v`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'llm'`.

- [ ] **Step 3: Add the injection param**

In `core/agent.py`, add `llm` to the `__init__` signature (place it right after `store`):

```python
        connectors_config_path: str = CONNECTORS_CONFIG_PATH,
        store: Optional[DacliStore] = None,
        llm: Optional[object] = None,
    ):
```

Then change the construction line (currently `self.llm = LLMClient(settings)  # reasoning client`) to:

```python
        # Reasoning client. Injectable so a headless/test harness can supply a
        # deterministic ScriptedLLM; defaults to the real provider client.
        self.llm = llm or LLMClient(settings)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_headless.LLMInjectionTest -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add core/agent.py tests/test_headless.py
git commit -m "feat(agent): allow injecting the reasoning LLM client"
```

---

## Task 2: ScriptedLLM

**Files:**
- Create: `reasoning/scripted.py`
- Test: `tests/test_headless.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_headless.py`:

```python
import asyncio


def _run(coro):
    return asyncio.run(coro)


class ScriptedLLMTest(unittest.TestCase):
    def test_returns_text_tool_calls_and_usage(self):
        from reasoning.scripted import ScriptedLLM
        llm = ScriptedLLM([
            {"text": "hi", "tool_calls": [{"name": "update_plan", "arguments": {"todos": []}}],
             "usage": {"input": 10, "output": 2}},
            {"text": "done"},
        ])
        content, tool_calls = _run(llm.generate(messages=[], tools=[], system_prompt=""))
        self.assertEqual(content, "hi")
        self.assertEqual(len(tool_calls), 1)
        self.assertEqual(tool_calls[0]["name"], "update_plan")
        self.assertIn("id", tool_calls[0])
        self.assertEqual(tool_calls[0]["arguments"], {"todos": []})
        self.assertEqual(llm.last_usage, {"input": 10, "output": 2})

        content2, tc2 = _run(llm.generate(messages=[]))
        self.assertEqual(content2, "done")
        self.assertEqual(tc2, [])

    def test_assigns_unique_ids(self):
        from reasoning.scripted import ScriptedLLM
        llm = ScriptedLLM([
            {"tool_calls": [{"name": "a", "arguments": {}}, {"name": "b", "arguments": {}}]},
        ])
        _content, tcs = _run(llm.generate(messages=[]))
        ids = [tc["id"] for tc in tcs]
        self.assertEqual(len(ids), len(set(ids)))

    def test_raises_when_exhausted(self):
        from reasoning.scripted import ScriptedLLM, ScriptExhausted
        llm = ScriptedLLM([{"text": "only one"}])
        _run(llm.generate(messages=[]))
        with self.assertRaises(ScriptExhausted):
            _run(llm.generate(messages=[]))

    def test_accepts_model_kwarg(self):
        from reasoning.scripted import ScriptedLLM
        llm = ScriptedLLM([{"text": "x"}])
        content, _tc = _run(llm.generate(messages=[], model="some-model"))
        self.assertEqual(content, "x")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_headless.ScriptedLLMTest -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'reasoning.scripted'`.

- [ ] **Step 3: Create `reasoning/scripted.py`**

```python
"""Deterministic, offline stand-in for :class:`reasoning.llm.LLMClient`.

Driven by an ordered list of scripted *responses*; each ``generate()`` call pops
the next one and returns it in the exact shape the kernel parses
(``core/kernel.py``): ``(content, tool_calls)`` where each tool call is
``{"id", "name", "arguments"}``. An empty ``tool_calls`` ends the agent loop
(final answer). Running past the end raises :class:`ScriptExhausted` — a real
signal that the agent looped more than the scenario anticipated.

A scripted response is a dict::

    {
      "text": "optional assistant text",
      "tool_calls": [ {"name": "update_plan", "arguments": {...}} ],  # optional
      "usage": {"input": 100, "output": 20},                          # optional
    }
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


class ScriptExhausted(RuntimeError):
    """Raised when ``generate()`` is called after the script is exhausted."""


class ScriptedLLM:
    """An offline LLM double satisfying the kernel's LLM contract."""

    def __init__(self, responses: List[Dict[str, Any]]):
        self._responses: List[Dict[str, Any]] = list(responses or [])
        self._i = 0
        #: Provider-normalized usage of the most recent generate() call.
        self.last_usage: Dict[str, int] = {}

    async def initialize(self) -> None:
        # No network, nothing to set up.
        return None

    async def generate(
        self,
        messages: Optional[List[Dict[str, Any]]] = None,
        tools: Optional[List[Dict[str, Any]]] = None,
        system_prompt: Optional[str] = None,
        on_text: Optional[Any] = None,
        model: Optional[str] = None,
    ) -> Tuple[str, List[Dict[str, Any]]]:
        if self._i >= len(self._responses):
            raise ScriptExhausted(
                f"ScriptedLLM exhausted after {len(self._responses)} response(s): "
                "the agent requested another generation the scenario did not script."
            )
        spec = self._responses[self._i]
        self._i += 1

        text = spec.get("text") or ""
        self.last_usage = dict(spec.get("usage") or {})

        tool_calls: List[Dict[str, Any]] = []
        for j, tc in enumerate(spec.get("tool_calls") or [], start=1):
            tool_calls.append(
                {
                    "id": tc.get("id") or f"call_{self._i}_{j}",
                    "name": tc["name"],
                    "arguments": tc.get("arguments") or {},
                }
            )

        # Presentation parity with streaming providers (headless on_text is a
        # no-op; the chat UI streams). Never let a presentation hook break us.
        if on_text and text:
            try:
                on_text(text)
            except Exception:
                pass

        return text, tool_calls
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_headless.ScriptedLLMTest -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add reasoning/scripted.py tests/test_headless.py
git commit -m "feat(reasoning): add ScriptedLLM offline test double"
```

---

## Task 3: HeadlessResult + exit-code logic

**Files:**
- Create: `core/headless.py` (result types + exit codes only in this task)
- Test: `tests/test_headless.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_headless.py`:

```python
class ExitCodeTest(unittest.TestCase):
    def _result(self, **kw):
        from core.headless import HeadlessResult, TurnRecord
        turns = kw.pop("turns", [])
        recs = []
        for t in turns:
            recs.append(TurnRecord(
                input=t.get("input", ""),
                content=t.get("content", ""),
                error=t.get("error"),
                needs_user_input=t.get("needs_user_input", False),
                tool_calls=t.get("tool_calls", []),
            ))
        return HeadlessResult(session_id="s", turns=recs, **kw)

    def test_ok(self):
        r = self._result(turns=[{"content": "done"}])
        self.assertEqual(r.exit_code, 0)
        self.assertTrue(r.ok)

    def test_agent_error_is_1(self):
        r = self._result(turns=[{"error": "boom"}])
        self.assertEqual(r.exit_code, 1)

    def test_needs_user_input_is_1(self):
        r = self._result(turns=[{"needs_user_input": True}])
        self.assertEqual(r.exit_code, 1)

    def test_governance_block_is_2(self):
        r = self._result(turns=[{"tool_calls": [{"name": "x", "args": {}, "status": "blocked", "error": "no"}]}])
        self.assertEqual(r.exit_code, 2)

    def test_block_beats_error(self):
        r = self._result(turns=[
            {"error": "boom"},
            {"tool_calls": [{"name": "x", "args": {}, "status": "denied"}]},
        ])
        self.assertEqual(r.exit_code, 2)

    def test_scenario_error_is_3(self):
        r = self._result(turns=[{"tool_calls": [{"name": "x", "args": {}, "status": "blocked"}]}],
                         scenario_error="script ran dry")
        self.assertEqual(r.exit_code, 3)

    def test_to_dict_shape(self):
        r = self._result(turns=[{"content": "hi"}], usage={"requests": 1})
        d = r.to_dict()
        self.assertEqual(set(d.keys()),
                         {"ok", "exit_code", "session_id", "turns", "usage", "audit_path", "scenario_error"})
        self.assertEqual(d["turns"][0]["content"], "hi")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_headless.ExitCodeTest -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'core.headless'`.

- [ ] **Step 3: Create `core/headless.py` (result layer)**

```python
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
    """One user message → agent outcome."""

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
```

Note: `max(code, EXIT_AGENT_ERROR)` then `max(code, EXIT_GOVERNANCE_BLOCK)` — since BLOCK(2) > AGENT_ERROR(1), a turn with both yields 2, satisfying `test_block_beats_error`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_headless.ExitCodeTest -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add core/headless.py tests/test_headless.py
git commit -m "feat(headless): add HeadlessResult and exit-code contract"
```

---

## Task 4: run_headless runner + integration tests

**Files:**
- Modify: `core/headless.py` (add `run_headless` + helpers)
- Test: `tests/test_headless.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_headless.py`:

```python
class RunHeadlessTest(unittest.TestCase):
    def setUp(self):
        self._pricing_patch = mock.patch("core.agent.fetch_pricing", return_value=None)
        self._pricing_patch.start()
        self.addCleanup(self._pricing_patch.stop)

    def _settings(self):
        s = load_config()
        try:
            s.sandbox.enabled = False
        except Exception:
            pass
        return s

    def test_happy_path_tool_call_and_usage(self):
        from core.headless import run_headless
        from reasoning.scripted import ScriptedLLM
        llm = ScriptedLLM([
            {"text": "planning",
             "tool_calls": [{"name": "update_plan",
                             "arguments": {"todos": [{"content": "do it", "status": "completed"}]}}],
             "usage": {"input": 50, "output": 10}},
            {"text": "All done.", "usage": {"input": 20, "output": 5}},
        ])
        result = _run(run_headless(
            inputs=["load the data"], settings=self._settings(),
            llm=llm, no_connectors=True,
        ))
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(len(result.turns), 1)
        turn = result.turns[0]
        self.assertEqual(turn.content, "All done.")
        names = [tc["name"] for tc in turn.tool_calls]
        self.assertIn("update_plan", names)
        self.assertEqual(result.usage.get("requests"), 2)
        self.assertEqual(result.usage.get("input"), 70)

    def test_governance_block_is_exit_2(self):
        from core.headless import run_headless
        from reasoning.scripted import ScriptedLLM
        llm = ScriptedLLM([
            {"text": "wiping",
             "tool_calls": [{"name": "run_shell_command",
                             "arguments": {"command": "dd if=/dev/zero of=/tmp/zz bs=1M count=1"}}]},
            {"text": "I was blocked."},
        ])
        result = _run(run_headless(
            inputs=["wipe the disk"], settings=self._settings(),
            llm=llm, no_connectors=True, approve="deny",
        ))
        self.assertEqual(result.exit_code, 2)
        statuses = [tc.get("status") for tc in result.turns[0].tool_calls]
        self.assertTrue(any(s in ("denied", "blocked") for s in statuses))

    def test_script_exhausted_is_exit_3(self):
        from core.headless import run_headless
        from reasoning.scripted import ScriptedLLM
        # Calls a tool but never scripts a final answer -> loop pulls again -> dry.
        llm = ScriptedLLM([
            {"tool_calls": [{"name": "update_plan", "arguments": {"todos": []}}]},
        ])
        result = _run(run_headless(
            inputs=["go"], settings=self._settings(),
            llm=llm, no_connectors=True,
        ))
        self.assertEqual(result.exit_code, 3)
        self.assertIsNotNone(result.scenario_error)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_headless.RunHeadlessTest -v`
Expected: FAIL — `ImportError: cannot import name 'run_headless' from 'core.headless'`.

- [ ] **Step 3: Add `run_headless` + helpers to `core/headless.py`**

Append to `core/headless.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.test_headless.RunHeadlessTest -v`
Expected: PASS (3 tests).

If `test_governance_block_is_exit_2` fails because governance is disabled in the loaded config, the default `config/policy.yaml` should keep it on; verify `settings.governance` is `None` or `.enabled` is True. If a local `config.yaml` disabled governance, run with the repo default by ensuring no `config.yaml` overrides it.

- [ ] **Step 5: Commit**

```bash
git add core/headless.py tests/test_headless.py
git commit -m "feat(headless): add run_headless agent driver"
```

---

## Task 5: `dacli run` and `dacli replay` commands

**Files:**
- Modify: `scripts/cli.py` (add two commands + one async helper; place after the `eval_cmd` command, before `prompt`)
- Test: `tests/test_headless.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_headless.py`:

```python
import json as _json
import tempfile as _tempfile
import os as _os


class CliCommandTest(unittest.TestCase):
    def setUp(self):
        self._pricing_patch = mock.patch("core.agent.fetch_pricing", return_value=None)
        self._pricing_patch.start()
        self.addCleanup(self._pricing_patch.stop)

    def _write(self, suffix, text):
        fd, path = _tempfile.mkstemp(suffix=suffix)
        with _os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        self.addCleanup(lambda: _os.path.exists(path) and _os.unlink(path))
        return path

    def test_run_command_emits_json_exit_0(self):
        from click.testing import CliRunner
        from scripts.cli import cli
        script = self._write("_llm.json", _json.dumps([
            {"text": "done", "usage": {"input": 5, "output": 1}},
        ]))
        runner = CliRunner()
        res = runner.invoke(cli, ["run", "hello", "--llm-script", script,
                                  "--no-connectors", "--json"])
        self.assertEqual(res.exit_code, 0, msg=res.output)
        payload = _json.loads(res.output)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["turns"][0]["content"], "done")

    def test_replay_command_runs_scenario(self):
        from click.testing import CliRunner
        from scripts.cli import cli
        scenario = self._write("_scenario.json", _json.dumps({
            "no_connectors": True,
            "approve": "deny",
            "llm_script": [
                {"text": "ok", "tool_calls": [
                    {"name": "update_plan", "arguments": {"todos": []}}]},
                {"text": "finished"},
            ],
            "turns": ["do the thing"],
        }))
        runner = CliRunner()
        res = runner.invoke(cli, ["replay", scenario, "--json"])
        self.assertEqual(res.exit_code, 0, msg=res.output)
        payload = _json.loads(res.output)
        self.assertEqual(payload["turns"][0]["content"], "finished")
        self.assertIn("update_plan", [tc["name"] for tc in payload["turns"][0]["tool_calls"]])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_headless.CliCommandTest -v`
Expected: FAIL — `SystemExit: 2` / "No such command 'run'".

- [ ] **Step 3: Add the commands to `scripts/cli.py`**

No new top-level imports are needed; `yaml` is imported locally inside the helpers
(matching the existing `init` command's `import yaml` style).

Add these commands after the `eval_cmd` function (before `def prompt`):

```python
async def _run_headless_cli(
    *,
    inputs,
    config,
    session,
    approve,
    llm_script,
    no_connectors,
    max_iterations,
):
    # Shared driver for `run` and `replay`. Builds settings, optionally injects a
    # ScriptedLLM from a JSON/YAML file, and returns a HeadlessResult.
    import yaml

    from core.headless import run_headless
    from reasoning.scripted import ScriptedLLM

    settings = load_config(config)
    llm = None
    if llm_script:
        responses = yaml.safe_load(Path(llm_script).read_text(encoding="utf-8")) or []
        llm = ScriptedLLM(responses)
    return await run_headless(
        inputs=inputs,
        settings=settings,
        llm=llm,
        approve=approve,
        session_id=session,
        no_connectors=no_connectors,
        max_iterations=max_iterations,
    )


def _emit_headless(result, as_json):
    # Machine path: emit ONLY the JSON via click.echo (plain stdout, no Rich
    # styling/ANSI) so consumers can json.loads(stdout) safely. Human path: a
    # short themed summary.
    if as_json:
        click.echo(result.to_json())
    else:
        for i, turn in enumerate(result.turns, 1):
            console.print(f"[accent]turn {i}[/accent]: {turn.content or '(no text)'}")
            if turn.error:
                console.print(f"[error]error:[/error] {turn.error}")
        if result.scenario_error:
            console.print(f"[error]scenario error:[/error] {result.scenario_error}")
        console.print(f"[muted]exit {result.exit_code} · session {result.session_id}[/muted]")


@cli.command(name="run")
@click.argument("message")
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--session", "-s", type=str, help="Session ID to resume")
@click.option("--approve", type=click.Choice(["deny", "approve"]), default="deny",
              help="Approval policy for governed actions (default: deny = fail-safe)")
@click.option("--llm-script", type=click.Path(exists=True),
              help="JSON/YAML file of scripted LLM responses (offline, deterministic)")
@click.option("--no-connectors", is_flag=True, default=False,
              help="Disable external connectors (built-ins only) for a hermetic run")
@click.option("--max-iterations", type=int, default=None, help="Override the agent iteration cap")
@click.option("--json", "as_json", is_flag=True, help="Emit the machine-readable JSON result")
def run_cmd(message, config, session, approve, llm_script, no_connectors, max_iterations, as_json):
    """Run a single message through the agent headlessly and emit a JSON result."""
    result = asyncio.run(_run_headless_cli(
        inputs=[message], config=config, session=session, approve=approve,
        llm_script=llm_script, no_connectors=no_connectors, max_iterations=max_iterations,
    ))
    _emit_headless(result, as_json)
    raise SystemExit(result.exit_code)


@cli.command(name="replay")
@click.argument("scenario_file", type=click.Path(exists=True))
@click.option("--json", "as_json", is_flag=True, help="Emit the machine-readable JSON result")
def replay_cmd(scenario_file, as_json):
    """Replay a scenario file (ordered user turns + optional scripted LLM)."""
    from core.headless import run_headless
    from reasoning.scripted import ScriptedLLM

    scenario = _yaml.safe_load(Path(scenario_file).read_text(encoding="utf-8")) or {}
    llm = None
    if scenario.get("llm_script"):
        llm = ScriptedLLM(scenario["llm_script"])
    settings = load_config(scenario.get("config"))
    result = asyncio.run(run_headless(
        inputs=list(scenario.get("turns") or []),
        settings=settings,
        llm=llm,
        approve=scenario.get("approve", "deny"),
        canned_inputs=scenario.get("inputs"),
        no_connectors=bool(scenario.get("no_connectors", True)),
        max_iterations=scenario.get("max_iterations"),
    ))
    _emit_headless(result, as_json)
    raise SystemExit(result.exit_code)
```

Also add `run`, `replay` to the `CLI_COMMANDS` completion list — open `config/__init__.py`, find `CLI_COMMANDS`, and add two entries mirroring the existing tuple style, e.g.:

```python
    ("/run", "headless one-shot run (mostly for testing)"),
    ("/replay", "replay a headless scenario file"),
```

(Only if `CLI_COMMANDS` entries are slash-prefixed chat commands; if `run`/`replay` are CLI-only and not chat slash-commands, skip this and leave `CLI_COMMANDS` untouched.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.test_headless.CliCommandTest -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Run the whole new test module + a smoke of the real CLI**

Run: `python -m unittest tests.test_headless -v`
Expected: PASS (all tests).

Run: `python run.py run "hello" --llm-script <a temp llm.json like in the test> --no-connectors --json`
Expected: a JSON object printed, process exit code 0.

- [ ] **Step 6: Commit**

```bash
git add scripts/cli.py config/__init__.py tests/test_headless.py
git commit -m "feat(cli): add headless 'run' and 'replay' commands"
```

---

## Task 6: Offline scenario file + CI step

**Files:**
- Create: `scenarios/smoke_headless.json`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Create the scenario**

`scenarios/smoke_headless.json`:

```json
{
  "no_connectors": true,
  "approve": "deny",
  "llm_script": [
    {
      "text": "Planning the work.",
      "tool_calls": [
        {"name": "update_plan", "arguments": {"todos": [{"content": "smoke step", "status": "completed"}]}}
      ],
      "usage": {"input": 40, "output": 8}
    },
    {"text": "Smoke scenario complete.", "usage": {"input": 12, "output": 4}}
  ],
  "turns": ["run the headless smoke scenario"]
}
```

- [ ] **Step 2: Verify it runs and exits 0**

Run: `python run.py replay scenarios/smoke_headless.json --json`
Expected: JSON with `"ok": true`, `"exit_code": 0`, a `update_plan` tool call, final content "Smoke scenario complete."; process exit 0.

- [ ] **Step 3: Add the CI step**

In `.github/workflows/ci.yml`, after the "Reliability eval" step, add:

```yaml
      # End-to-end smoke of the headless agent surface: drives the real agent
      # loop + governance with a scripted (offline, free) LLM and asserts the
      # JSON contract holds and the process exits 0. No secrets required.
      - name: Headless e2e smoke (scripted LLM, offline)
        run: python run.py replay scenarios/smoke_headless.json --json
```

- [ ] **Step 4: Commit**

```bash
git add scenarios/smoke_headless.json .github/workflows/ci.yml
git commit -m "ci: run offline headless e2e smoke scenario"
```

---

## Task 7: docs/TESTING.md

**Files:**
- Create: `docs/TESTING.md`

- [ ] **Step 1: Write the doc**

`docs/TESTING.md`:

````markdown
# Testing dacli headlessly

`dacli` can be driven non-interactively for CI e2e/smoke tests and for an AI agent
operating the terminal. Two commands return a stable JSON result.

## Commands

- `dacli run "<message>" [--json] [--config PATH] [--session ID] [--approve deny|approve] [--llm-script FILE] [--no-connectors] [--max-iterations N]`
  — one message through the agent. With `--llm-script` the run is fully offline and
  deterministic; without it, the configured provider is used (real e2e).
- `dacli replay FILE [--json]` — a scenario file: ordered user turns plus an optional
  embedded `llm_script` and approval/input policy.

(Invoke via `python run.py run ...` / `python run.py replay ...` from the repo root.)

## JSON contract

```json
{
  "ok": true,
  "exit_code": 0,
  "session_id": "…",
  "turns": [
    {
      "input": "…",
      "content": "final assistant text",
      "error": null,
      "needs_user_input": false,
      "iterations": 2,
      "tool_calls": [{"name": "update_plan", "args": {…}, "status": "success", "error": null}],
      "governance": [{"decision_id": "…", "tool_name": "…", "tier": "risky", "events": [...]}]
    }
  ],
  "usage": {"requests": 1, "input": 100, "output": 20, "costUSD": 0.0},
  "audit_path": ".dacli/audit.jsonl"
}
```

(`tool_calls[].status` mirrors the engine's `ToolStatus` value — prefer matching
the enum over the literal if you assert on it.)

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | All turns completed without error or governance block. |
| 1 | An agent error, or a turn ended waiting on user input. |
| 2 | An action was blocked/denied by governance. |
| 3 | Scenario/script error (scripted LLM ran dry, malformed scenario, missing canned input). |

## Scenario file

```yaml
config: null
no_connectors: true        # built-ins only; no external network
approve: deny              # deny | approve | [true, false, ...]
inputs: ["answer 1"]       # canned answers for request_user_input, in order
llm_script:                # omit to use the real provider
  - { text: "Running it.", tool_calls: [ { name: update_plan, arguments: { todos: [] } } ] }
  - { text: "Done." }
turns:
  - "do the thing"
```

## CI pattern

```yaml
- name: Headless e2e smoke (scripted LLM, offline)
  run: python run.py replay scenarios/smoke_headless.json --json
```

The step fails (non-zero exit) on any agent error, governance block, or script error.

## AI agent driving the terminal

```bash
python run.py run "list the connectors and load the sales table" --json
# parse stdout JSON; branch on .exit_code; inspect .turns[].tool_calls and .governance
```
````

- [ ] **Step 2: Commit**

```bash
git add docs/TESTING.md
git commit -m "docs: headless testing contract and usage"
```

---

## Final verification

- [ ] Run the full suite: `python -m unittest discover -s tests -p "test_*.py"` — expected: all pass (existing + `test_headless`).
- [ ] Run the offline scenario: `python run.py replay scenarios/smoke_headless.json --json` — expected: `"exit_code": 0`.
- [ ] Run the reliability eval to confirm no regression: `python -m eval --quick` — expected: exit 0.
```
