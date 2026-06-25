"""M09 — the thin host is the live path.

``core.host.DacliHost`` replaces the DACLI god object on the interactive turn
loop: a plain turn runs through the kernel, and the one dispatcher serves both
the register(api) seeds (snowflake/github/shell) and the still-connector path
(built-ins + the 13 platforms) behind one tool list. The seed connectors are
excluded from the connector path so the seeds own their tool names.
"""

import asyncio
import tempfile
from pathlib import Path

from dacli.config.settings import Settings
from dacli.connectors.base import ToolStatus
from dacli.core.host import DacliHost
from dacli.ai.scripted import ScriptedLLM


def _settings(tmp: Path) -> Settings:
    # Offline: scripted LLM, terminal + sandbox off so construction spawns nothing.
    return Settings.model_validate({
        "llm": {"provider": "scripted", "model": "scripted",
                "api_key": "scripted", "base_url": "https://api.test.local"},
        "agent": {"state_path": str(tmp / "state") + "/",
                  "history_path": str(tmp / "history") + "/"},
        "terminal": {"enabled": False},
        "sandbox": {"enabled": False},
    })


def _host(tmp: Path, script) -> DacliHost:
    return DacliHost(settings=_settings(tmp), llm=ScriptedLLM(script))


def test_plain_turn_runs_through_host():
    with tempfile.TemporaryDirectory() as tmp:
        host = _host(Path(tmp), [{"text": "all set", "tool_calls": []}])
        resp = asyncio.run(host.process_message("hello"))
        assert resp.content == "all set"
        assert resp.error is None
        assert resp.tool_calls == []


def test_one_tool_list_serves_seeds_and_builtins():
    with tempfile.TemporaryDirectory() as tmp:
        host = _host(Path(tmp), [])
        names = {d["function"]["name"] for d in host._combined.get_tool_definitions()}
        # A seed tool (extension) and a built-in connector tool, in one list.
        assert "execute_snowflake_query" in names
        assert "request_user_input" in names


def test_seed_owns_its_tool_name_not_the_connector():
    with tempfile.TemporaryDirectory() as tmp:
        host = _host(Path(tmp), [])
        # Resolves to the extension (its .name is the extension id), and the
        # snowflake/github connectors were excluded from the connector path.
        resolved = host._combined.resolve("execute_snowflake_query")
        assert resolved is not None
        tool, _op = resolved
        assert tool.name == "snowflake"
        assert host.registry.get_connector("snowflake") is None
        assert host.registry.get_connector("github") is None


def test_host_exposes_the_slash_surface():
    with tempfile.TemporaryDirectory() as tmp:
        host = _host(Path(tmp), [])
        # The handles chat_session + tui.slash read off the agent.
        assert host.registry is not None
        assert host.dispatcher is not None
        assert host.governor is not None
        assert callable(host._get_pricing)
        assert "build" in host._context


def test_safe_seed_tool_dispatches_through_the_host(monkeypatch):
    import snowflake.connector

    class _Cur:
        description = [("COLUMN_NAME",), ("DATA_TYPE",)]
        rowcount = 1

        def execute(self, sql):
            self._sql = sql

        def fetchall(self):
            return [("id", "NUMBER")]

    class _Conn:
        def cursor(self):
            return _Cur()

    monkeypatch.setattr(snowflake.connector, "connect", lambda **kw: _Conn(), raising=False)

    with tempfile.TemporaryDirectory() as tmp:
        host = _host(Path(tmp), [])
        # introspect is risk=safe -> no approval needed; proves a seed runs the
        # host's governed dispatcher end to end.
        res = asyncio.run(host.dispatcher.execute(
            "introspect_snowflake_object",
            {"object_type": "table", "database": "D", "schema": "S", "object": "T"},
        ))
        assert res.status is ToolStatus.SUCCESS
        assert res.data["exists"] is True
