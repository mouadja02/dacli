""" (Context Constructor) tests — one per exit criterion.

Run with:  python -m unittest tests.test_context_phase3
"""

import asyncio
import os
import tempfile
import unittest
from dataclasses import dataclass

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.connectors.registry import ConnectorRegistry
from dacli.connectors.system.connector import SystemConnector
from dacli.core.kernel import Kernel

from dacli.context.assembler import build_context, Context
from dacli.context.budget import Budget, PINNED, PRIORS
from dacli.context.compaction import compact, needs_compaction
from dacli.context.disclosure import disclose
from dacli.context.spill import ResultStore, summarize_or_inline
from dacli.context.tokenizer import make_counter
from dacli.prompts.system_prompt import compose_system_prompt


# ---------------------------------------------------------------------------
# Mock connectors (distinct names + chunky schemas so full defs are expensive)
# ---------------------------------------------------------------------------
class _MockConnector(Connector):
    def __init__(self, idx: int, ops: int = 4):
        super().__init__(None)
        self.name = f"mock{idx}"
        self._ops = ops

    def operations(self) -> list[OperationSpec]:
        return [
            OperationSpec(
                name=f"{self.name}_op{j}",
                description=("A reasonably verbose operation description that exists "
                             "to make full tool schemas expensive relative to the digest. " * 2),
                parameters={
                    "type": "object",
                    "properties": {
                        f"param_{k}": {"type": "string", "description": "x" * 80}
                        for k in range(6)
                    },
                    "required": [f"param_{k}" for k in range(3)],
                },
                capability=f"{self.name}.op{j}",
                risk=Risk.SAFE,
            )
            for j in range(self._ops)
        ]

    async def invoke(self, op, args):
        return ToolResult(tool_name=op, status=ToolStatus.SUCCESS, data={"ok": True})

    async def health(self):
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS, data={"ready": True})


def _registry_with(n: int) -> ConnectorRegistry:
    """Registry with ``n`` discovered mock connectors + the system built-in."""
    empty = tempfile.mkdtemp(prefix="dacli_ctx_")
    system = SystemConnector(settings=None, memory=None)
    reg = ConnectorRegistry(None, connectors_dir=empty, config_path="__nope__.yaml",
                            extra_connectors=[system])
    os.rmdir(empty)
    for i in range(n):
        c = _MockConnector(i)
        reg._connectors[c.name] = c
        reg._manifests[c.name] = {"name": f"Mock {i}", "description": f"Mock connector number {i}", "enabled": True}
    reg._build_index()
    system.bind_registry(reg)
    return reg


def _digest_block_tokens(reg, counter) -> int:
    # The base context cost that grows with connectors = compose core + digest.
    base = compose_system_prompt("", [])
    digest = reg.get_tool_digest()
    lines = "\n".join(f"- {e['id']} ({e['name']}): {e['description']} [{e['operations']} ops]" for e in digest)
    return counter.count(base + "\n\n" + lines)


def _full_defs_tokens(reg, counter) -> int:
    import json
    return counter.count(json.dumps(reg.get_tool_definitions()))


# ---------------------------------------------------------------------------
# Fakes for the kernel new-path test
# ---------------------------------------------------------------------------
@dataclass
class _Msg:
    role: str
    content: str


class _FakeMemory:
    def __init__(self):
        self._history: list[_Msg] = []
        self.catalog = None
        self.finals: list[str] = []

    def add_user_message(self, content):
        self._history.append(_Msg("user", content))

    def add_assistant_message(self, content):
        self.finals.append(content)
        self._history.append(_Msg("assistant", content))

    def get_full_history(self):
        return list(self._history)

    def get_context_messages(self):
        return [{"role": m.role, "content": m.content} for m in self._history]

    def retrieve(self, query, top_k=5):
        return []

    def log_tool_execution(self, **kw):
        pass


class _RecordingLLM:
    """Replays scripted responses and records the tools/system it was offered."""

    def __init__(self, script):
        self.script = list(script)
        self.seen_tools: list[set] = []
        self.seen_system: list[str] = []

    async def generate(self, messages, tools=None, system_prompt=None, on_text=None):
        self.seen_tools.append({t["function"]["name"] for t in (tools or [])})
        self.seen_system.append(system_prompt or "")
        return self.script.pop(0)


def _tc(cid, name, args):
    return {"id": cid, "name": name, "arguments": args}


# ===========================================================================
# Exit criterion: progressive disclosure keeps the base prompt roughly flat
# ===========================================================================
class DisclosureFlatCostTest(unittest.TestCase):
    def test_digest_grows_far_slower_than_full_defs(self):
        counter = make_counter(None)
        d3 = _digest_block_tokens(_registry_with(3), counter)
        d12 = _digest_block_tokens(_registry_with(12), counter)
        f3 = _full_defs_tokens(_registry_with(3), counter)
        f12 = _full_defs_tokens(_registry_with(12), counter)

        digest_growth = d12 - d3      # +9 connectors, digest only
        defs_growth = f12 - f3        # +9 connectors, full schemas

        # Adding 9 connectors costs a handful of tokens each in the digest...
        self.assertLess(digest_growth, 300)
        # ...while loading their full schemas would cost an order of magnitude more.
        self.assertGreater(defs_growth, 5 * digest_growth)

    def test_disclosed_tools_only_when_selected(self):
        reg = _registry_with(5)
        # Nothing disclosed -> only the always-on system tools.
        names = {d["function"]["name"] for d in reg.get_tool_definitions(connector_ids=set())}
        self.assertNotIn("mock0_op0", names)
        self.assertIn("load_connector_tools", names)
        # Disclose one -> its ops appear; others stay hidden.
        names = {d["function"]["name"] for d in reg.get_tool_definitions(connector_ids={"mock0"})}
        self.assertIn("mock0_op0", names)
        self.assertNotIn("mock1_op0", names)


# ===========================================================================
# Exit criterion: a 10k-row result adds a bounded, small number of tokens
# ===========================================================================
class SpillBoundedTest(unittest.TestCase):
    def test_large_result_spills_to_bounded_summary_and_round_trips(self):
        counter = make_counter(None)
        store = ResultStore(root=tempfile.mkdtemp(), session_id="t")
        data = [{"id": i, "name": f"row{i}", "val": i * 2} for i in range(10000)]
        big = ToolResult(tool_name="q", status=ToolStatus.SUCCESS, data=data)

        msg = summarize_or_inline(big, counter, threshold_tokens=1000, store=store)
        # The model-facing message is tiny relative to the raw result.
        self.assertLess(counter.count(msg), 500)
        self.assertLess(counter.count(msg), counter.count(big.to_message()) / 50)
        self.assertIn("handle:", msg)

        handle = msg.split("handle:")[1].split(")")[0].strip()
        full = store.read(handle)
        self.assertEqual(full["total_rows"], 10000)
        self.assertEqual(full["data"][0], {"id": 0, "name": "row0", "val": 0})

    def test_small_result_and_errors_stay_inline(self):
        counter = make_counter(None)
        store = ResultStore(root=tempfile.mkdtemp(), session_id="t2")
        small = ToolResult(tool_name="q", status=ToolStatus.SUCCESS, data=[{"x": 1}])
        self.assertNotIn("handle:", summarize_or_inline(small, counter, 1000, store))
        err = ToolResult(tool_name="q", status=ToolStatus.ERROR, error="boom")
        self.assertIn("boom", summarize_or_inline(err, counter, 1000, store))


# ===========================================================================
# Exit criterion: compaction by budget pressure; decisions survive
# ===========================================================================
class CompactionTest(unittest.TestCase):
    def _long_session(self):
        msgs = []
        for i in range(20):
            msgs.append({"role": "user", "content": f"step {i}: create table T{i}"})
            msgs.append({"role": "assistant", "content": "x" * 400})
        msgs.append({"role": "user", "content": "summarize the build"})
        return msgs

    def test_triggers_by_pressure_not_turn_count(self):
        counter = make_counter(None)
        msgs = self._long_session()
        tok = counter.count_messages(msgs)
        # Same (many) turns: triggers under a tight budget, not under a generous one.
        self.assertTrue(needs_compaction(msgs, counter, token_budget=int(tok / 0.9) - 50))
        self.assertFalse(needs_compaction(msgs, counter, token_budget=tok * 10))

    def test_compaction_preserves_decisions_and_does_not_destroy(self):
        msgs = self._long_session()

        class FakeLLM:
            async def generate(self, messages, tools=None, system_prompt=None, on_text=None):
                return ("- Created tables T0..T19\n- Open TODO: summarize build", [])

        stored = []
        res = asyncio.run(compact(msgs, FakeLLM(), keep_recent=4, store_fn=stored.append))

        self.assertEqual(res.compacted_count, len(msgs) - 4)
        self.assertEqual(len(res.messages), 5)  # 1 note + 4 recent
        self.assertIn("Created tables", res.messages[0]["content"])
        self.assertTrue(stored)                 # note persisted to memory
        self.assertEqual(res.messages[-1]["content"], "summarize the build")  # task at tail
        self.assertEqual(len(msgs), 41)         # original NOT mutated (raw history kept)


# ===========================================================================
# 3.1 assembler: provenance, budget, pinning, disclosure-gated tools
# ===========================================================================
class AssemblerTest(unittest.TestCase):
    def test_provenance_budget_and_pinning(self):
        reg = _registry_with(2)
        counter = make_counter(None)

        class Mem:
            catalog = None
            def retrieve(self, q, top_k=5):
                return []

        msgs = [
            {"role": "user", "content": "first"},
            {"role": "assistant", "content": "ok"},
            {"role": "tool", "content": "some result"},
            {"role": "user", "content": "the current task"},
        ]
        ctx = build_context(
            "the current task", memory=Mem(), registry=reg, recent_messages=msgs,
            counter=counter, budget=Budget(total=4000), disclosed={"mock0"},
            base_system_prompt="CORE", priors_text="# Priors\nUse FQNs.",
        )
        self.assertIsInstance(ctx, Context)
        # Provenance rows expose source/timestamp/tokens (the --explain contract).
        for row in ctx.explain():
            self.assertIn(row["source"], {"priors", "memory", "live", "skills", "history", "task"})
            self.assertIn("tokens", row)
        # Priors are pinned; the current task is pinned and present at the tail.
        self.assertTrue(any(c.source == PRIORS and c.pinned for c in ctx.chunks))
        self.assertEqual(ctx.messages[-1]["content"], "the current task")
        self.assertGreater(ctx.budget[PINNED]["used"], 0)
        # Tools are gated by the disclosed set (+ always-on system tools).
        names = {t["function"]["name"] for t in ctx.tools}
        self.assertIn("mock0_op0", names)
        self.assertNotIn("mock1_op0", names)
        self.assertIn("load_connector_tools", names)


# ===========================================================================
# Integration: kernel new path — window gone, disclosure flows, assembler used
# ===========================================================================
class KernelNewPathTest(unittest.TestCase):
    def _spine(self, script, task):
        empty = tempfile.mkdtemp(prefix="dacli_kn_")
        # Drop an echo manifest so it is *discovered* (not a built-in) and thus
        # gated by disclosure.
        echo_dir = os.path.join(empty, "echo")
        os.makedirs(echo_dir)
        with open(os.path.join(echo_dir, "manifest.yaml"), "w", encoding="utf-8") as f:
            f.write("id: echo\nname: Echo\ndescription: Echo back a string\nicon: E\n"
                    "class: tests.golden_echo.EchoConnector\nrequired_config: []\nenabled: true\n")

        memory = _FakeMemory()
        system = SystemConnector(settings=None, memory=memory)
        reg = ConnectorRegistry(None, connectors_dir=empty, config_path="__nope__.yaml",
                                extra_connectors=[system])
        system.bind_registry(reg)

        from dacli.connectors.dispatcher import Dispatcher
        dispatcher = Dispatcher(reg, memory=memory)
        counter = make_counter(None)
        budget = Budget(total=8000)

        def builder(t, working, disclosed):
            effective = disclose(t, reg, already_disclosed=disclosed)
            base = compose_system_prompt(t, effective)
            return build_context(t, memory=memory, registry=reg, recent_messages=working,
                                 counter=counter, budget=budget, disclosed=effective,
                                 base_system_prompt=base)

        llm = _RecordingLLM(script)
        kernel = Kernel(
            llm=llm, dispatcher=dispatcher, memory=memory,
            tools=reg.get_tool_definitions(), system_prompt="UNUSED",
            max_iterations=10, context_builder=builder,
        )
        return kernel, llm, memory

    def test_load_connector_tools_discloses_on_next_iteration(self):
        # Task avoids the word 'echo' so the connector is NOT auto-disclosed; the
        # model must call load_connector_tools to make echo_say available.
        script = [
            ("", [_tc("c1", "load_connector_tools", {"connector_id": "echo"})]),
            ("", [_tc("c2", "echo_say", {"text": "hi"})]),
            ("done", []),
        ]
        kernel, llm, _memory = self._spine(script, "perform the requested operation")
        resp = asyncio.run(kernel.orchestrate("perform the requested operation"))

        self.assertEqual(resp.content, "done")
        # echo_say was gated on the first call, disclosed (and usable) afterwards.
        self.assertNotIn("echo_say", llm.seen_tools[0])
        self.assertIn("echo_say", llm.seen_tools[1])
        # The assembler built the system prompt (core + connectors digest),
        # proving the fixed window/static prompt path is not in use.
        self.assertIn("Available connectors", llm.seen_system[0])

    def test_assembler_seeds_from_full_history_not_a_window(self):
        script = [("answer", [])]
        kernel, llm, _memory = self._spine(script, "just answer")
        asyncio.run(kernel.orchestrate("just answer"))
        # The single generation saw the assembled context; system prompt carries
        # the connectors digest rather than the legacy static prompt.
        self.assertIn("Available connectors", llm.seen_system[0])


if __name__ == "__main__":
    unittest.main()
