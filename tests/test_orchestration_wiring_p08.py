"""P08 — the orchestration / multi-agent stack is wired in, gated, and lean.

These tests pin the two halves of the P08 decision (Option A — wire it in):

* **Lean by default.** With ``orchestration.enabled = false`` (the new default), a
  plain startup constructs NONE of the planner / blackboard / lead / orchestrator
  / model-router / tier-router, and writes no ``blackboard.json`` / ``routing.jsonl``.
* **Reachable when enabled.** ``process_message`` routes a genuine multi-step goal
  (or an explicit ``/plan``) through the planner DAG *and* ``TierRouter.route()``,
  while a simple, single-step turn stays on the cheap kernel loop.

Offline + deterministic: the kernel's per-node LLM call is stubbed, so the
planner/router heuristics (which are themselves offline) drive the assertions.

    python -m unittest tests.test_orchestration_wiring_p08
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from core.kernel import AgentResponse
import contextlib

_TEMP_DIRS = []


def tearDownModule():
    for d in _TEMP_DIRS:
        shutil.rmtree(d, ignore_errors=True)


def _run(coro):
    return asyncio.run(coro)


def _settings(orchestration=None):
    """Hermetic settings with placeholder creds + isolated state (mirrors
    tests/test_headless._settings_for_test). ``orchestration`` overrides the
    orchestration sub-block when given."""
    from config.settings import Settings

    kwargs = {
        "llm": {"provider": "scripted", "model": "scripted",
             "api_key": "scripted", "base_url": "https://api.test.local"},
        "github": {"token": "x"},
        "snowflake": {"account": "a", "user": "u", "password": "p",
                   "warehouse": "w", "role": "r", "database": "d"},
        "pinecone": {"api_key": "k", "index_name": "i", "environment": "e"},
        "embeddings": {"provider": "openai", "api_key": "k", "model": "m"},
    }
    if orchestration is not None:
        kwargs["orchestration"] = orchestration
    settings = Settings(**kwargs)
    root = tempfile.mkdtemp(prefix="dacli_p08_test_")
    _TEMP_DIRS.append(root)
    settings.agent.state_path = os.path.join(root, "state.json")
    settings.agent.history_path = os.path.join(root, "history.json")
    with contextlib.suppress(Exception):
        settings.sandbox.enabled = False
    try:
        settings.terminal.enabled = False  # avoid spawning a workspace/shell
    except Exception:
        pass
    return settings


class _Sentinel:
    """Stand-in accepted anywhere the LLM client is expected."""
    async def initialize(self):
        return None


def _agent(settings):
    from core.agent import DACLI
    return DACLI(settings=settings, llm=_Sentinel())


class LeanDefaultStartupTest(unittest.TestCase):
    """Acceptance: a default startup builds none of the six subsystems."""

    def setUp(self):
        self._pricing = mock.patch("core.agent.fetch_pricing", return_value=None)
        self._pricing.start()
        self.addCleanup(self._pricing.stop)

    def test_default_settings_disable_orchestration(self):
        from config.settings import OrchestrationSettings
        self.assertFalse(OrchestrationSettings().enabled)

    def test_no_orchestration_subsystems_built_by_default(self):
        agent = _agent(_settings())  # orchestration defaults to disabled
        self.assertFalse(agent._orchestration_on)
        for attr in ("router", "model_router", "planner",
                     "blackboard", "lead", "orchestrator"):
            self.assertIsNone(getattr(agent, attr), f"{attr} should not be built")

    def test_no_artifacts_written_on_plain_startup(self):
        settings = _settings()
        _agent(settings)
        state_dir = Path(settings.agent.state_path).parent
        self.assertFalse((state_dir / "blackboard.json").exists())
        self.assertFalse((state_dir / "routing.jsonl").exists())
        self.assertFalse((state_dir / "model_routing.jsonl").exists())

    def test_disabled_path_still_runs_the_kernel_loop(self):
        agent = _agent(_settings())
        seen = []

        async def fake_orchestrate(msg, model=None):
            seen.append(msg)
            return AgentResponse(content="ok", tool_calls=[])

        agent.kernel.orchestrate = fake_orchestrate
        # Even a multi-step-looking goal stays on the kernel when disabled.
        resp = _run(agent.process_message("create x then load y then validate z"))
        self.assertEqual(seen, ["create x then load y then validate z"])
        self.assertEqual(resp.content, "ok")


class OrchestratedRoutingE2ETest(unittest.TestCase):
    """Acceptance: when enabled, a multi-step goal goes through the planner DAG
    and TierRouter.route(); simple turns stay on the kernel loop."""

    def setUp(self):
        self._pricing = mock.patch("core.agent.fetch_pricing", return_value=None)
        self._pricing.start()
        self.addCleanup(self._pricing.stop)

        settings = _settings(orchestration={"enabled": True, "require_plan_approval": False})
        self.agent = _agent(settings)

        # Subsystems exist now.
        self.assertTrue(self.agent._orchestration_on)
        for attr in ("router", "model_router", "planner", "blackboard",
                     "lead", "orchestrator"):
            self.assertIsNotNone(getattr(self.agent, attr))

        # Spy on the planner + router without changing behavior.
        self.calls = {"route": 0, "decompose": 0}
        self.captured = {}
        orig_route = self.agent.router.route
        orig_decompose = self.agent.planner.decompose

        async def spy_route(task):
            self.calls["route"] += 1
            d = await orig_route(task)
            self.captured["decision"] = d
            return d

        def spy_decompose(goal):
            self.calls["decompose"] += 1
            return orig_decompose(goal)

        self.agent.router.route = spy_route
        self.agent.planner.decompose = spy_decompose

        # Stub the per-node kernel call so nodes succeed offline.
        self.kernel_calls = []

        async def fake_orchestrate(msg, model=None):
            self.kernel_calls.append(msg)
            return AgentResponse(content=f"did: {msg}", tool_calls=[])

        self.agent.kernel.orchestrate = fake_orchestrate

    def test_multistep_goal_routes_through_planner_and_router(self):
        goal = "create the bronze table then load the crm source then validate the result"
        resp = _run(self.agent.process_message(goal))

        # The orchestrated path was taken: planner decomposed + router routed.
        self.assertEqual(self.calls["decompose"], 1, "planner.decompose must run")
        self.assertEqual(self.calls["route"], 1, "TierRouter.route must run")
        # A multi-step goal routes to the sandbox tier (the expected tier).
        self.assertEqual(self.captured["decision"].tier, "sandbox")
        # The orchestrator drove the DAG (3 chained nodes) through the kernel.
        self.assertEqual(len(self.kernel_calls), 3)
        # The orchestrated outcome is folded back into an AgentResponse.
        self.assertIsInstance(resp, AgentResponse)
        self.assertIsNone(resp.error)
        self.assertIn("completed", resp.content)

    def test_simple_turn_stays_on_the_kernel_loop(self):
        resp = _run(self.agent.process_message("show me the row counts"))
        # The planner/router were NOT consulted; the kernel ran once directly.
        self.assertEqual(self.calls["decompose"], 0)
        self.assertEqual(self.calls["route"], 0)
        self.assertEqual(self.kernel_calls, ["show me the row counts"])
        self.assertEqual(resp.content, "did: show me the row counts")

    def test_explicit_plan_prefix_forces_orchestration(self):
        # A short goal that the complexity gate would otherwise skip is forced
        # through the planner+router by the explicit /plan request.
        resp = _run(self.agent.process_message("/plan just do one focused thing"))
        self.assertEqual(self.calls["route"], 1)
        self.assertEqual(self.calls["decompose"], 1)
        self.assertIsInstance(resp, AgentResponse)


if __name__ == "__main__":
    unittest.main()
