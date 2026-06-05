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
