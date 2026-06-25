"""Regression tests for the P08 TUI/UX upgrades that cross module seams.

Covers the optional tool-progress callback (dispatcher → connector →
``emit_progress``) and the honest ctx % (assembler budget snapshot).
Pure-rendering cases live in test_tui.py.
"""

import asyncio
import types


from dacli.connectors.base import Connector, OperationSpec, ToolResult, ToolStatus
from dacli.connectors.dispatcher import Dispatcher


# ---------------------------------------------------------------------------
# emit_progress (U-4): connector-side contract
# ---------------------------------------------------------------------------
class _ProgressConnector(Connector):
    name = "prog"

    def __init__(self):
        super().__init__(settings=None)
        self.progress_during_invoke = None

    def operations(self):
        return [OperationSpec(name="prog_op", description="d", parameters={},
                              capability="test")]

    async def invoke(self, op, args):
        self.progress_during_invoke = self._on_progress
        self.emit_progress("step 1 of 2")
        self.emit_progress("step 2 of 2")
        return ToolResult(tool_name="prog_op", status=ToolStatus.SUCCESS, data="ok")

    async def health(self):
        return ToolResult(tool_name="health", status=ToolStatus.SUCCESS)


class _FakeRegistry:
    def __init__(self, connector):
        self._connector = connector

    def resolve(self, tool_name):
        return (self._connector, "prog_op")

    def get_operation_spec(self, tool_name):
        return None

    def is_builtin(self, name):
        return True


def test_emit_progress_is_a_noop_without_callback():
    connector = _ProgressConnector()
    connector.emit_progress("nobody listening")  # must not raise


def test_emit_progress_swallows_callback_exceptions():
    connector = _ProgressConnector()
    connector._on_progress = lambda msg: 1 / 0
    connector.emit_progress("boom")  # a UI hiccup must never break the op
    connector._on_progress = None


def test_dispatcher_binds_progress_callback_around_invoke():
    connector = _ProgressConnector()
    events = []
    dispatcher = Dispatcher(
        _FakeRegistry(connector),
        on_tool_progress=lambda tool, msg: events.append((tool, msg)),
    )
    result = asyncio.run(dispatcher.execute("prog_op", {}))
    assert result.success
    assert events == [("prog_op", "step 1 of 2"), ("prog_op", "step 2 of 2")]
    # Bound only for the duration of the call.
    assert connector.progress_during_invoke is not None
    assert connector._on_progress is None


def test_dispatcher_without_progress_callback_is_unchanged():
    connector = _ProgressConnector()
    dispatcher = Dispatcher(_FakeRegistry(connector))
    result = asyncio.run(dispatcher.execute("prog_op", {}))
    assert result.success
    assert connector.progress_during_invoke is None
    assert connector._on_progress is None


# ---------------------------------------------------------------------------
# Honest ctx % (U-6)
# ---------------------------------------------------------------------------
def _agent_with_budget(budget):
    ctx = types.SimpleNamespace(budget=budget) if budget is not None else None
    return types.SimpleNamespace(_context={"last_context": lambda: ctx})


def _memory(window=10, history_len=3):
    return types.SimpleNamespace(
        memory_window=window,
        get_full_history=lambda: list(range(history_len)),
    )


def test_ctx_pct_uses_assembler_budget_snapshot():
    from dacli.scripts.cli import _ctx_pct
    agent = _agent_with_budget({
        "HISTORY": {"used": 50, "cap": 100},
        "MEMORY": {"used": 0, "cap": 100},
    })
    assert _ctx_pct(_memory(), agent) == 25


def test_ctx_pct_clamps_to_100():
    from dacli.scripts.cli import _ctx_pct
    agent = _agent_with_budget({"HISTORY": {"used": 500, "cap": 100}})
    assert _ctx_pct(_memory(), agent) == 100


def test_ctx_pct_falls_back_to_window_proxy_before_first_turn():
    from dacli.scripts.cli import _ctx_pct
    assert _ctx_pct(_memory(window=10, history_len=3)) == 30
    assert _ctx_pct(_memory(window=10, history_len=3), _agent_with_budget(None)) == 30


def test_ctx_pct_survives_malformed_budget():
    from dacli.scripts.cli import _ctx_pct
    agent = _agent_with_budget({"HISTORY": {}})  # cap 0 -> fall back
    assert _ctx_pct(_memory(window=10, history_len=3), agent) == 30
    broken = types.SimpleNamespace(_context={})  # no last_context key
    assert _ctx_pct(_memory(window=10, history_len=3), broken) == 30


def test_pipeline_caches_last_context(tmp_path):
    from dacli.config.settings import Settings
    from dacli.connectors.registry import ConnectorRegistry
    from dacli.connectors.system.connector import SystemConnector
    from dacli.context.pipeline import build_context_pipeline

    class _Memory:
        session_id = "p08-test"

        def get_full_history(self):
            return []

        def get_context_messages(self):
            return []

        def retrieve(self, query, top_k=5):
            return []

    settings = Settings()
    memory = _Memory()
    system = SystemConnector(settings=settings, memory=memory)
    registry = ConnectorRegistry(
        settings, connectors_dir=str(tmp_path), config_path="__nope__.yaml",
        extra_connectors=[system],
    )
    hooks = build_context_pipeline(settings, memory, registry, llm=None,
                                   system_connector=system)
    assert hooks["last_context"]() is None  # nothing assembled yet
    ctx = hooks["build"]("list the tables", [], set())
    assert hooks["last_context"]() is ctx
    assert ctx.budget  # the snapshot the toolbar reads
