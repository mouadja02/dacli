"""F-2: diff-before-promote — the data-diff skill and `dacli diff` CLI.

The diff is read-only (row counts + per-column null rates + a bounded sample
comparison, all via the connector's governed query op). The promote variant
dispatches its mutation through the governed dispatcher, so the approval +
rollback machinery applies — a promote is impossible without the diff and a
governance gate.

Deterministic and offline: driven by the simulated BigQuery platform.
"""

import asyncio
import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from dacli.connectors.base import ToolResult, ToolStatus
from dacli.connectors.bigquery.connector import BigQueryConnector
from dacli.connectors.cli_base import CliResult
from dacli.connectors.dispatcher import Dispatcher
from dacli.core.verify import VerificationContext, run_postconditions
from dacli.eval.sim.cli import SimCli
from dacli.eval.sim.platforms import sim_settings
from dacli.governance.audit import AuditLedger
from dacli.governance.governor import Governor
from dacli.governance.permissions import PermissionRegistry, Scope
from dacli.scripts.cli import cli
from dacli.skills.data_diff.skill import DataDiffSkill
from dacli.skills.spec import SkillContext


# ---------------------------------------------------------------------------
# A sim responder that knows two tables and records every executed statement.
# ---------------------------------------------------------------------------
_TABLES = {
    "ds.a": {"count": 100, "rows": [{"id": 1, "v": "x"}, {"id": 2, "v": None}]},
    "ds.b": {"count": 97, "rows": [{"id": 1, "v": "x"}, {"id": 2, "v": "y"}]},
}


def _make_responder(executed: list):
    def respond(argv: list[str]) -> CliResult:
        if "show" in argv:
            return CliResult(0, json.dumps({"schema": {"fields": []}}), "", argv)
        sql = argv[-1]
        executed.append(sql)
        m = re.search(r"COUNT\(\*\)\s+AS\s+(\w+)\s+FROM\s+([\w.$:]+)", sql, re.IGNORECASE)
        if m:
            table = _TABLES.get(m.group(2))
            n = table["count"] if table else 0
            return CliResult(0, json.dumps([{m.group(1): n}]), "", argv)
        m = re.search(r"FROM\s+([\w.$:]+)", sql, re.IGNORECASE)
        if m and m.group(1) in _TABLES:
            return CliResult(0, json.dumps(_TABLES[m.group(1)]["rows"]), "", argv)
        return CliResult(0, json.dumps([]), "", argv)
    return respond


class _SimRegistry:
    """Just enough registry for the dispatcher + skill to resolve bigquery."""

    def __init__(self, connector):
        self._conn = connector
        self._specs = {op.name: op for op in connector.operations()}

    def resolve(self, tool_name):
        if tool_name in self._specs:
            return self._conn, tool_name
        return None

    def get_operation_spec(self, tool_name):
        return self._specs.get(tool_name)

    def get_connector(self, connector_id):
        return self._conn if connector_id == "bigquery" else None

    def is_builtin(self, connector_id):
        return False


def _make_context(executed: list, *, approve: bool | None = None) -> SkillContext:
    conn = BigQueryConnector(
        sim_settings("bigquery"), runner=SimCli(_make_responder(executed))
    )
    registry = _SimRegistry(conn)
    permissions = PermissionRegistry(default_scope=Scope.ADMIN)
    ledger_path = Path(tempfile.mkdtemp()) / "audit.jsonl"
    governor = Governor(
        permissions=permissions,
        ledger=AuditLedger(path=str(ledger_path)),
        approval_fn=(None if approve is None else (lambda _req: approve)),
    )
    from dacli.core.verify import Verifier

    dispatcher = Dispatcher(
        registry=registry, verifier=Verifier(), governor=governor
    )
    return SkillContext(registry=registry, dispatcher=dispatcher)


def _run(coro):
    return asyncio.run(coro)


class DataDiffTest(unittest.TestCase):
    def test_diff_reports_row_count_and_null_rate_deltas(self):
        executed: list = []
        ctx = _make_context(executed)
        result = _run(DataDiffSkill().execute(
            {"connector": "bigquery", "table_a": "ds.a", "table_b": "ds.b"}, ctx
        ))
        self.assertTrue(result.success, result.error)
        data = result.data
        self.assertEqual(data["row_count_a"], 100)
        self.assertEqual(data["row_count_b"], 97)
        self.assertEqual(data["row_delta"], -3)
        # Column "v" is null in a's sample but not b's → a non-zero delta.
        v = next(c for c in data["columns"] if c["name"] == "v")
        self.assertEqual(v["null_rate_a"], 0.5)
        self.assertEqual(v["null_rate_b"], 0.0)
        # One of the two compared sample rows differs.
        self.assertEqual(data["sample"]["rows_differing"], 1)

    def test_diff_is_read_only(self):
        executed: list = []
        ctx = _make_context(executed)
        _run(DataDiffSkill().execute(
            {"connector": "bigquery", "table_a": "ds.a", "table_b": "ds.b"}, ctx
        ))
        for sql in executed:
            self.assertTrue(
                sql.upper().startswith("SELECT"), f"non-SELECT executed: {sql}"
            )

    def test_diff_rejects_unsafe_identifiers(self):
        executed: list = []
        ctx = _make_context(executed)
        result = _run(DataDiffSkill().execute(
            {"connector": "bigquery", "table_a": "ds.a; DROP TABLE x", "table_b": "ds.b"},
            ctx,
        ))
        self.assertFalse(result.success)
        self.assertEqual(executed, [])

    def test_postconditions_pass_on_a_well_formed_diff(self):
        executed: list = []
        ctx = _make_context(executed)
        skill = DataDiffSkill()
        result = _run(skill.execute(
            {"connector": "bigquery", "table_a": "ds.a", "table_b": "ds.b"}, ctx
        ))
        vctx = VerificationContext(args={}, result=result)
        report = _run(run_postconditions(skill.spec.postconditions, vctx))
        self.assertTrue(report.passed, report.summary())

    def test_promote_is_denied_without_approval(self):
        # Headless deny mode: no approval callback → fail-closed.
        executed: list = []
        ctx = _make_context(executed, approve=None)
        result = _run(DataDiffSkill().execute(
            {"connector": "bigquery", "table_a": "ds.a", "table_b": "ds.b",
             "mode": "promote"},
            ctx,
        ))
        self.assertFalse(result.success)
        self.assertFalse((result.data or {}).get("promoted"))
        # The mutation must never have reached the platform.
        for sql in executed:
            self.assertTrue(
                sql.upper().startswith("SELECT"), f"promote executed unapproved: {sql}"
            )

    def test_promote_runs_after_diff_and_approval(self):
        executed: list = []
        ctx = _make_context(executed, approve=True)
        result = _run(DataDiffSkill().execute(
            {"connector": "bigquery", "table_a": "ds.a", "table_b": "ds.b",
             "mode": "promote"},
            ctx,
        ))
        self.assertTrue(result.success, result.error)
        self.assertTrue(result.data["promoted"])
        # The diff ran (and rode along in the result) before the promote.
        self.assertEqual(result.data["row_count_a"], 100)
        promotes = [s for s in executed if not s.upper().startswith("SELECT")]
        self.assertEqual(len(promotes), 1)
        self.assertIn("ds.b", promotes[0])


class DiffCommandTest(unittest.TestCase):
    """The `dacli diff` CLI surface renders the skill's result."""

    class _StubDispatcher:
        async def execute(self, tool_name, arguments):
            return ToolResult(
                tool_name=tool_name,
                status=ToolStatus.SUCCESS,
                data={
                    "table_a": "ds.a", "table_b": "ds.b",
                    "row_count_a": 100, "row_count_b": 97, "row_delta": -3,
                    "columns": [{"name": "v", "null_rate_a": 0.5,
                                 "null_rate_b": 0.0, "delta": -0.5}],
                    "sample": {"size_a": 2, "size_b": 2,
                               "rows_compared": 2, "rows_differing": 1},
                    "method": "row-count + null-rate + sampled comparison",
                    "mode": "diff",
                },
            )

    class _StubAgent:
        def __init__(self, **kwargs):
            self.dispatcher = DiffCommandTest._StubDispatcher()

    def test_diff_command_renders_deltas(self):
        runner = CliRunner()
        with (
            runner.isolated_filesystem(),
            patch("dacli.scripts.cli.DACLI", self._StubAgent),
            patch.dict("os.environ", {"COLUMNS": "300"}),
        ):
            result = runner.invoke(cli, ["diff", "bigquery", "ds.a", "ds.b"], obj={})
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("100", result.output)
        self.assertIn("97", result.output)
        self.assertIn("-3", result.output)


if __name__ == "__main__":
    unittest.main()
