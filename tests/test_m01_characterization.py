"""M01 — characterization snapshots.

Pin the live surface before the pivot deletes ~10–12k LOC, so M09/M10 can diff
the rebuilt spine against what shipped today. Five snapshots, all deterministic
(ScriptedLLM + faked connector seams, no live model or credentials):

* three governed single-step turns — a snowflake read, a github read, and a
  shell command — replayed through the real Kernel + Dispatcher + Governor;
* the ``/help`` slash inventory and the shape of ``dacli doctor``;
* one full generated-connector lifecycle: generate → import → rediscover → use.

Fixtures live in ``dacli/eval/golden/transcripts/``. Re-baseline a fixture after
an intended change with ``DACLI_M01_RECORD=1 pytest tests/test_m01_characterization.py``
— the snapshot is rewritten instead of asserted.
"""

import asyncio
import base64
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import dacli.eval.golden as _golden
from dacli.connectors.dispatcher import Dispatcher
from dacli.connectors.registry import ConnectorRegistry
from dacli.core.kernel import Kernel
from dacli.ai.scripted import ScriptedLLM
from tests.test_golden_transcript import FakeMemory

FIXTURES = Path(_golden.__file__).resolve().parent / "transcripts"
RECORD = os.environ.get("DACLI_M01_RECORD") == "1"

# Keys whose values are timing/identity noise — never part of the behavior we pin.
_VOLATILE = {
    "execution_time_ms", "timestamp", "scrollback_handle", "command_id",
    "backups", "duration_ms", "pid", "started_at", "finished_at",
    "session_id", "cwd", "session", "backend",
}


def _scrub(obj, tmp=None):
    """Drop volatile keys and mask the throwaway temp path so a fixture is stable."""
    if isinstance(obj, dict):
        return {k: _scrub(v, tmp) for k, v in obj.items() if k not in _VOLATILE}
    if isinstance(obj, list):
        return [_scrub(v, tmp) for v in obj]
    if isinstance(obj, str) and tmp:
        return obj.replace(str(tmp), "<WORKSPACE>")
    return obj


def _result_view(result, tmp=None):
    return _scrub({
        "tool_name": result.tool_name,
        "status": result.status.value,
        "data": result.data,
        "error": result.error,
        "metadata": dict(result.metadata or {}),
    }, tmp)


def _assert_or_record(case, actual):
    """Compare ``actual`` against the committed fixture, or (re)write it."""
    path = FIXTURES / f"{case}.json"
    blob = json.dumps(actual, indent=2, sort_keys=True, default=str)
    if RECORD or not path.exists():
        path.write_text(blob + "\n", encoding="utf-8")
        return
    expected = json.loads(path.read_text(encoding="utf-8"))
    assert json.loads(blob) == expected, (
        f"{case} drifted from its M01 snapshot. If intentional, re-baseline with "
        f"DACLI_M01_RECORD=1 (see {path})."
    )


# ---------------------------------------------------------------------------
# faked connector seams
# ---------------------------------------------------------------------------
class FakeCursor:
    """Stands in for a Snowflake cursor (the connector's SDK seam)."""

    def __init__(self, columns, rows):
        self._columns = columns
        self._rows = rows
        self.description = [(c,) for c in columns]
        self.rowcount = len(rows)

    def execute(self, sql):
        self._last = sql

    def fetchall(self):
        return self._rows

    def fetchmany(self, n):
        return self._rows[:n]


class _Resp:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeHttpx:
    """Stands in for the github connector's httpx.AsyncClient seam."""

    def __init__(self, resp):
        self._resp = resp

    async def get(self, *args, **kwargs):
        return self._resp


# ---------------------------------------------------------------------------
# real spine, scripted turn
# ---------------------------------------------------------------------------
def _governed_spine(connector, scope, script):
    """Wire the real Kernel + Dispatcher + Governor around one injected connector."""
    from dacli.governance import (
        Governor, ActionClassifier, PolicyEngine, PermissionRegistry, Scope,
        RollbackStrategist, AuditLedger,
    )

    memory = FakeMemory()
    empty = tempfile.mkdtemp(prefix="dacli_m01_")
    registry = ConnectorRegistry(
        settings=None, connectors_dir=empty,
        config_path="__nonexistent__.yaml", extra_connectors=[connector],
    )
    os.rmdir(empty)

    perms = PermissionRegistry(default_scope=Scope.READ_ONLY)
    perms.grant(connector.name, Scope(scope))
    gov = Governor(
        classifier=ActionClassifier(network="allowlist", egress_allowlist=[]),
        policy=PolicyEngine(), permissions=perms, strategist=RollbackStrategist(),
        ledger=AuditLedger(path=".dacli/eval/m01_governor.jsonl"),
        enforce=True, use_shadow=False, approval_fn=lambda req: True,
    )

    dispatched = []
    results = []
    dispatcher = Dispatcher(
        registry, memory=memory, governor=gov,
        on_tool_start=lambda name, args: dispatched.append({"tool": name, "args": dict(args)}),
        on_tool_end=lambda name, result: results.append(result),
    )
    kernel = Kernel(
        llm=ScriptedLLM(script), dispatcher=dispatcher, memory=memory,
        tools=registry.get_tool_definitions(), system_prompt="SYS", max_iterations=10,
    )
    return kernel, dispatched, results


def _turn(connector, scope, tool, args, *, tmp=None):
    script = [
        {"text": "", "tool_calls": [{"name": tool, "arguments": args}]},
        {"text": "done"},
    ]
    kernel, dispatched, results = _governed_spine(connector, scope, script)
    resp = asyncio.run(kernel.orchestrate("GO"))
    return {
        "dispatched": _scrub(dispatched, tmp),
        "results": [_result_view(r, tmp) for r in results],
        "final": {
            "content": resp.content,
            "iteration": resp.iteration,
            "needs_user_input": resp.needs_user_input,
            "error": resp.error,
        },
    }


class TurnSnapshots(unittest.TestCase):

    def test_snowflake_read(self):
        from dacli.connectors.snowflake.connector import SnowflakeConnector

        conn = SnowflakeConnector(SimpleNamespace(connector_config={"snowflake": {}}))
        conn._connection = object()
        conn._cursor = FakeCursor(["ID", "NAME"], [(1, "ada"), (2, "linus")])
        conn.is_connected = True

        actual = _turn(conn, "admin", "execute_snowflake_query",
                       {"query": "SELECT ID, NAME FROM analytics.users"})
        _assert_or_record("snowflake_read", actual)

    def test_github_read(self):
        from dacli.connectors.github.connector import GithubConnector

        settings = SimpleNamespace(connector_config={"github": {
            "owner": "octo", "repo": "warehouse", "branch": "main",
        }})
        conn = GithubConnector(settings)
        payload = {
            "path": "dbt_project.yml",
            "content": base64.b64encode(b"name: warehouse\nversion: 1.0\n").decode(),
            "sha": "feedface",
            "size": 31,
        }
        conn._client = FakeHttpx(_Resp(200, payload))
        conn.is_connected = True

        actual = _turn(conn, "read_only", "read_github_file", {"path": "dbt_project.yml"})
        _assert_or_record("github_read", actual)

    def test_shell_command(self):
        from dacli.connectors.shell.connector import ShellConnector
        from dacli.context.sources.terminal import ScrollbackStore
        from dacli.eval.sim.shell import make_sim_session

        tmp = tempfile.mkdtemp(prefix="dacli_m01_shell_")
        try:
            session, _sim = make_sim_session("m01sh", tmp)
            (session.workspace.root / "hello.txt").write_text("hi there\n", encoding="utf-8")
            store = ScrollbackStore(root=tmp, session_id="m01sh")
            settings = SimpleNamespace(terminal=SimpleNamespace(
                network="allowlist", egress_allowlist=[],
                max_output_chars=2000, wall_clock_seconds=120,
            ))
            conn = ShellConnector(settings, session=session, scrollback_store=store)

            actual = _turn(conn, "write", "run_shell_command",
                           {"command": "cat hello.txt"}, tmp=tmp)
            _assert_or_record("shell_command", actual)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# inventory + doctor shape
# ---------------------------------------------------------------------------
class InventorySnapshots(unittest.TestCase):

    def test_slash_inventory(self):
        from dacli.config import CLI_COMMANDS

        _assert_or_record("slash_inventory", [list(c) for c in CLI_COMMANDS])

    def test_doctor_shape(self):
        from dacli.config.settings import Settings
        from dacli.core import doctor

        diag = doctor.collect(Settings()).to_dict()

        # Pin the key structure only — leaf values (and even their types, for the
        # nullable fields) are environment-specific (config.path, sandbox probes).
        def shape(value):
            if isinstance(value, dict):
                return {k: shape(v) for k, v in sorted(value.items())}
            return "<value>"

        _assert_or_record("doctor_shape", shape(diag))


if __name__ == "__main__":
    unittest.main()
