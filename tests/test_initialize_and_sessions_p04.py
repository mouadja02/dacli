"""P04 — honest ``DACLI.initialize()`` + correct session listing.

* Fix A: ``initialize()`` returns ``False`` when the LLM fails to initialize,
  ``True`` otherwise. Connector ``connect()`` failures stay non-fatal (one bad
  connector can't take the agent down).
* Fix B: ``list_sessions`` no longer reports a misleading always-zero
  ``tables_created`` (the legacy ``created_tables`` state field is gone; table
  facts live in the catalog cache), and ``dacli sessions`` drops the column.

Offline + deterministic. Run with:
    python -m unittest tests.test_initialize_and_sessions_p04
"""

import asyncio
import contextlib
import os
import shutil
import tempfile
import unittest
from unittest import mock

from click.testing import CliRunner

_TEMP_DIRS = []


def tearDownModule():
    for d in _TEMP_DIRS:
        shutil.rmtree(d, ignore_errors=True)


def _settings():
    """Hermetic settings with placeholder creds + isolated state (mirrors
    tests/test_orchestration_wiring_p08._settings)."""
    from dacli.config.settings import Settings

    settings = Settings(
        llm={"provider": "scripted", "model": "scripted",
             "api_key": "scripted", "base_url": "https://api.test.local"},
        github={"token": "x"},
        snowflake={"account": "a", "user": "u", "password": "p",
                   "warehouse": "w", "role": "r", "database": "d"},
        pinecone={"api_key": "k", "index_name": "i", "environment": "e"},
        embeddings={"provider": "openai", "api_key": "k", "model": "m"},
    )
    root = tempfile.mkdtemp(prefix="dacli_p04_test_")
    _TEMP_DIRS.append(root)
    settings.agent.state_path = os.path.join(root, "state.json")
    settings.agent.history_path = os.path.join(root, "history.json")
    with contextlib.suppress(Exception):
        settings.sandbox.enabled = False
    with contextlib.suppress(Exception):
        settings.terminal.enabled = False
    return settings


class _OkLLM:
    """LLM double whose initialize succeeds."""
    async def initialize(self):
        return None


class _FailingLLM:
    """LLM double whose initialize raises (provider unreachable)."""
    async def initialize(self):
        raise ConnectionError("provider unreachable")


class _FailingConnector:
    """Connector double whose connect raises."""
    name = "boom"

    async def connect(self):
        raise RuntimeError("connector down")


class InitializeHonestyTest(unittest.TestCase):
    """Acceptance: initialize() reports LLM failure; connector failures stay non-fatal."""

    def setUp(self):
        self._pricing = mock.patch("dacli.core.host.fetch_pricing", return_value=None)
        self._pricing.start()
        self.addCleanup(self._pricing.stop)

    def _agent(self, llm):
        from dacli.core.host import DacliHost
        return DacliHost(settings=_settings(), llm=llm)

    def test_llm_failure_returns_false(self):
        agent = self._agent(_FailingLLM())
        self.assertFalse(asyncio.run(agent.initialize()))

    def test_llm_success_returns_true(self):
        agent = self._agent(_OkLLM())
        self.assertTrue(asyncio.run(agent.initialize()))

    def test_connector_failure_stays_nonfatal(self):
        agent = self._agent(_OkLLM())
        with mock.patch.object(agent.registry, "enabled_connectors",
                               return_value=[_FailingConnector()]), \
             mock.patch.object(agent.registry, "get_catalog", return_value={}):
            self.assertTrue(asyncio.run(agent.initialize()))

    def test_llm_failure_wins_even_when_connectors_succeed(self):
        agent = self._agent(_FailingLLM())
        with mock.patch.object(agent.registry, "enabled_connectors", return_value=[]), \
             mock.patch.object(agent.registry, "get_catalog", return_value={}):
            self.assertFalse(asyncio.run(agent.initialize()))


class ListSessionsTest(unittest.TestCase):
    """Acceptance: list_sessions drops the always-zero ``tables_created``."""

    def _memory(self):
        from dacli.core.memory import AgentMemory
        root = tempfile.mkdtemp(prefix="dacli_p04_mem_")
        _TEMP_DIRS.append(root)
        return AgentMemory(
            state_path=os.path.join(root, "state"),
            history_path=os.path.join(root, "history"),
            memory_path=os.path.join(root, "memory"),
        )

    def test_no_tables_created_key(self):
        memory = self._memory()
        memory.set_todos([{"content": "load CRM", "status": "in_progress"}])
        sessions = memory.list_sessions()
        self.assertEqual(len(sessions), 1)
        self.assertNotIn("tables_created", sessions[0])

    def test_session_summary_fields_survive(self):
        memory = self._memory()
        memory.set_todos([{"content": "load CRM", "status": "in_progress"}])
        (session,) = memory.list_sessions()
        self.assertEqual(session["session_id"], memory.session_id)
        self.assertEqual(session["active_task"], "load CRM")
        self.assertEqual(session["errors_count"], 0)


class SessionsCommandTest(unittest.TestCase):
    """`dacli sessions` renders without error and without a Tables column."""

    def test_sessions_command_runs_clean(self):
        from dacli.core.memory import AgentMemory
        from dacli.scripts.cli import cli

        runner = CliRunner()
        with runner.isolated_filesystem():
            AgentMemory().set_todos([{"content": "x", "status": "in_progress"}])
            result = runner.invoke(cli, ["sessions"], obj={})
            self.assertEqual(result.exit_code, 0, result.output)
            self.assertIn("Session", result.output)
            self.assertNotIn("Tables", result.output)


if __name__ == "__main__":
    unittest.main()
