"""Golden transcript test (workstream 1.7).

A recorded multi-tool session (mock LLM + mock connectors) is replayed through
the real Kernel + Dispatcher + ConnectorRegistry + system connector. This is the
behavior-equivalence regression net for the whole roadmap: the new spine must
produce the exact same dispatch sequence and final response every time.

Run with:  python -m unittest tests.test_golden_transcript
"""

import asyncio
import os
import tempfile
import unittest

from dacli.connectors.registry import ConnectorRegistry
from dacli.connectors.dispatcher import Dispatcher
from dacli.connectors.system.connector import SystemConnector
from dacli.core.kernel import Kernel
from tests.golden_echo import EchoConnector


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------
class FakeLLM:
    """Replays a scripted sequence of (content, tool_calls) responses."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = []

    async def generate(self, messages, tools=None, system_prompt=None, on_text=None):
        self.calls.append({"tools": tools, "system_prompt": system_prompt})
        content, tool_calls = self.script.pop(0)
        # Mirror the real client: when streaming is requested, emit the text.
        if on_text and content:
            on_text(content)
        return content, tool_calls


class FakeMemory:
    def __init__(self):
        self.user_messages = []
        self.assistant_finals = []
        self.tool_logs = []
        self.todo_updates = []

    def add_user_message(self, content):
        self.user_messages.append(content)

    def get_context_messages(self):
        # Fresh list each call; the kernel mutates its own copy.
        return [{"role": "user", "content": "GO"}]

    def add_assistant_message(self, content):
        self.assistant_finals.append(content)

    def log_tool_execution(self, tool_name, input_params, result=None, error=None, execution_time_ms=0.0):
        self.tool_logs.append(tool_name)

    def set_todos(self, todos):
        self.todo_updates.append(list(todos))


def _tc(call_id, name, arguments):
    return {"id": call_id, "name": name, "arguments": arguments}


def build_spine(script, memory, on_user_input_needed=None):
    """Wire the real kernel/dispatcher/registry with injected fakes."""
    empty_dir = tempfile.mkdtemp(prefix="dacli_no_connectors_")
    dispatched = []

    system = SystemConnector(settings=None, memory=memory, on_user_input_needed=on_user_input_needed)
    registry = ConnectorRegistry(
        settings=None,
        connectors_dir=empty_dir,            # no manifests -> no real connectors
        config_path="__nonexistent__.yaml",
        extra_connectors=[EchoConnector(None), system],
    )

    dispatcher = Dispatcher(
        registry,
        memory=memory,
        on_tool_start=lambda name, args: dispatched.append((name, dict(args))),
    )

    kernel = Kernel(
        llm=FakeLLM(script),
        dispatcher=dispatcher,
        memory=memory,
        tools=registry.get_tool_definitions(),
        system_prompt="SYS",
        max_iterations=10,
    )
    os.rmdir(empty_dir)
    return kernel, dispatched, registry


# The recorded session: echo a string, record a plan, then answer.
_PLAN = [{"content": "did the thing", "status": "completed"}]
GOLDEN_SCRIPT = [
    ("", [_tc("c1", "echo_say", {"text": "hello"})]),
    ("", [_tc("c2", "update_plan", {"todos": _PLAN})]),
    ("done", []),
]

GOLDEN_DISPATCH = [
    ("echo_say", {"text": "hello"}),
    ("update_plan", {"todos": _PLAN}),
]


class GoldenTranscriptTest(unittest.TestCase):

    def test_multi_tool_session_replays_to_golden(self):
        memory = FakeMemory()
        kernel, dispatched, _ = build_spine(GOLDEN_SCRIPT, memory)

        resp = asyncio.run(kernel.orchestrate("GO"))

        # Dispatch sequence is byte-for-byte the recorded transcript.
        self.assertEqual(dispatched, GOLDEN_DISPATCH)

        # Final response.
        self.assertEqual(resp.content, "done")
        self.assertEqual(resp.iteration, 3)
        self.assertFalse(resp.needs_user_input)
        self.assertIsNone(resp.error)

        # Side effects flowed through the single dispatch path.
        self.assertEqual(memory.user_messages, ["GO"])
        self.assertEqual(memory.assistant_finals, ["done"])
        self.assertEqual(memory.tool_logs, ["echo_say", "update_plan"])
        self.assertEqual(memory.todo_updates, [_PLAN])

    def test_replay_is_deterministic(self):
        runs = []
        for _ in range(2):
            memory = FakeMemory()
            kernel, dispatched, _ = build_spine(GOLDEN_SCRIPT, memory)
            resp = asyncio.run(kernel.orchestrate("GO"))
            runs.append((dispatched, resp.content, resp.iteration, resp.needs_user_input))
        self.assertEqual(runs[0], runs[1])

    def test_tool_definitions_presented_to_llm(self):
        # The kernel must hand the registry-built tool defs to the LLM, including
        # the injected echo connector and the built-in system tools.
        memory = FakeMemory()
        _kernel, _, registry = build_spine(GOLDEN_SCRIPT, memory)
        names = {d["function"]["name"] for d in registry.get_tool_definitions()}
        self.assertIn("echo_say", names)
        self.assertIn("request_user_input", names)
        self.assertIn("update_plan", names)

    def test_pending_approval_pauses_for_user_input(self):
        # With no user-input callback, request_user_input yields PENDING_APPROVAL,
        # and the kernel returns early asking for input (no tool result appended).
        script = [("", [_tc("c1", "request_user_input", {"question": "what now?"})])]
        memory = FakeMemory()
        kernel, dispatched, _ = build_spine(script, memory, on_user_input_needed=None)

        resp = asyncio.run(kernel.orchestrate("GO"))

        self.assertTrue(resp.needs_user_input)
        self.assertEqual(resp.iteration, 1)
        self.assertEqual(dispatched, [("request_user_input", {"question": "what now?"})])

    def test_unknown_tool_is_reported_not_crashed(self):
        # A tool the registry cannot resolve produces an error ToolResult that the
        # loop feeds back to the LLM, which then answers.
        script = [
            ("", [_tc("c1", "does_not_exist", {})]),
            ("recovered", []),
        ]
        memory = FakeMemory()
        kernel, dispatched, _ = build_spine(script, memory)

        resp = asyncio.run(kernel.orchestrate("GO"))

        self.assertEqual(resp.content, "recovered")
        self.assertEqual(dispatched, [("does_not_exist", {})])


class EchoConnectorDiscoveryTest(unittest.TestCase):
    """Proves a 4th connector is added by 'dropping a folder' with a manifest,
    requiring zero edits to core/ or reasoning/."""

    def test_manifest_discovery_zero_core_edits(self):
        tmp = tempfile.mkdtemp(prefix="dacli_echo_pkg_")
        echo_dir = os.path.join(tmp, "echo")
        os.makedirs(echo_dir)
        with open(os.path.join(echo_dir, "manifest.yaml"), "w", encoding="utf-8") as f:
            f.write(
                "id: echo\n"
                "name: Echo\n"
                "description: Throwaway echo connector\n"
                "icon: \"E\"\n"
                "class: tests.golden_echo.EchoConnector\n"
                "required_config: []\n"
                "enabled: true\n"
            )

        try:
            registry = ConnectorRegistry(
                settings=None,
                connectors_dir=tmp,
                config_path="__nonexistent__.yaml",
            )
            # Discovered purely from the dropped manifest.
            self.assertIn("echo", registry.get_catalog())
            self.assertIsNotNone(registry.resolve("echo_say"))
            names = {d["function"]["name"] for d in registry.get_tool_definitions()}
            self.assertIn("echo_say", names)
        finally:
            # 'then removed' — nothing persists in the real connectors/ tree.
            os.remove(os.path.join(echo_dir, "manifest.yaml"))
            os.rmdir(echo_dir)
            os.rmdir(tmp)


if __name__ == "__main__":
    unittest.main()
