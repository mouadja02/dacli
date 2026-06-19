"""P14 slice A: data-quality assertions with governed remediation.

An assertion is a read-only metric query (null_rate / row_count) dispatched
through the governed path; a breach proposes a remediation that is never executed
unless the caller opts in, and even then it runs through the governance gate.

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

from dacli.connectors.bigquery.connector import BigQueryConnector
from dacli.connectors.cli_base import CliResult
from dacli.connectors.dispatcher import Dispatcher
from dacli.core.paths import STATE_PATH_ENV
from dacli.core.quality import (
    Assertion,
    delete_assertion,
    evaluate,
    load_assertions,
    propose_remediation,
    save_assertion,
)
from dacli.core.verify import VerificationContext, Verifier, run_postconditions
from dacli.eval.sim.cli import SimCli
from dacli.eval.sim.platforms import sim_settings
from dacli.governance.audit import AuditLedger
from dacli.governance.governor import Governor
from dacli.governance.permissions import PermissionRegistry, Scope
from dacli.scripts.cli import cli
from dacli.skills.quality_assert.skill import QualityAssertSkill
from dacli.skills.spec import SkillContext


# ds.orders: 100 rows, amount null in 5 of them → null_rate 0.05.
def _make_responder(executed: list):
    def respond(argv: list[str]) -> CliResult:
        if "show" in argv:
            return CliResult(0, json.dumps({"schema": {"fields": []}}), "", argv)
        sql = argv[-1]
        executed.append(sql)
        if re.search(r"COUNT\(\*\)\s+AS\s+TOTAL", sql, re.IGNORECASE):
            return CliResult(0, json.dumps([{"TOTAL": 100, "NONNULL": 95}]), "", argv)
        if re.search(r"COUNT\(\*\)\s+AS\s+N\b", sql, re.IGNORECASE):
            return CliResult(0, json.dumps([{"N": 100}]), "", argv)
        return CliResult(0, json.dumps([]), "", argv)
    return respond


class _SimRegistry:
    def __init__(self, connector):
        self._conn = connector
        self._specs = {op.name: op for op in connector.operations()}

    def resolve(self, tool_name):
        return (self._conn, tool_name) if tool_name in self._specs else None

    def get_operation_spec(self, tool_name):
        return self._specs.get(tool_name)

    def get_connector(self, connector_id):
        return self._conn if connector_id == "bigquery" else None

    def is_builtin(self, connector_id):
        return False


def _dispatcher(executed: list, *, approve=None):
    conn = BigQueryConnector(sim_settings("bigquery"), runner=SimCli(_make_responder(executed)))
    governor = Governor(
        permissions=PermissionRegistry(default_scope=Scope.ADMIN),
        ledger=AuditLedger(path=str(Path(tempfile.mkdtemp()) / "audit.jsonl")),
        approval_fn=(None if approve is None else (lambda _r: approve)),
    )
    return Dispatcher(registry=_SimRegistry(conn), verifier=Verifier(), governor=governor)


def _run(coro):
    return asyncio.run(coro)


class PersistenceTest(unittest.TestCase):
    def test_define_load_delete_roundtrip(self):
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.dict("os.environ", {STATE_PATH_ENV: str(Path(tmp) / "state")}),
        ):
            a = Assertion(name="orders_amount", connector="bigquery",
                          table="ds.orders", metric="null_rate", op=">",
                          threshold=0.01, column="amount")
            save_assertion(a)
            loaded = load_assertions()
            self.assertIn("orders_amount", loaded)
            self.assertEqual(loaded["orders_amount"].column, "amount")
            self.assertTrue(delete_assertion("orders_amount"))
            self.assertEqual(load_assertions(), {})


class EvaluateTest(unittest.TestCase):
    def test_null_rate_breach_proposes_unapplied_fix(self):
        a = Assertion(name="amt", connector="bigquery", table="ds.orders",
                      metric="null_rate", op=">", threshold=0.01, column="amount")
        executed: list = []
        outcome = _run(evaluate(a, _dispatcher(executed)))
        self.assertAlmostEqual(outcome.value, 0.05)
        self.assertTrue(outcome.breached)
        self.assertIsNotNone(outcome.proposed_fix)
        self.assertFalse(outcome.proposed_fix.applied)
        self.assertEqual(outcome.exit_code, 1)
        # The measurement read only — nothing was mutated.
        self.assertTrue(all(s.upper().startswith("SELECT") for s in executed), executed)

    def test_null_rate_within_threshold_passes(self):
        a = Assertion(name="amt", connector="bigquery", table="ds.orders",
                      metric="null_rate", op=">", threshold=0.10, column="amount")
        outcome = _run(evaluate(a, _dispatcher([])))
        self.assertFalse(outcome.breached)
        self.assertIsNone(outcome.proposed_fix)
        self.assertEqual(outcome.exit_code, 0)

    def test_row_count_metric(self):
        a = Assertion(name="rows", connector="bigquery", table="ds.orders",
                      metric="row_count", op="<", threshold=1000)
        outcome = _run(evaluate(a, _dispatcher([])))
        self.assertEqual(outcome.value, 100)
        self.assertTrue(outcome.breached)

    def test_invalid_assertion_errors_before_any_query(self):
        a = Assertion(name="bad", connector="bigquery", table="ds.orders",
                      metric="null_rate", op=">", threshold=0.01)  # no column
        executed: list = []
        outcome = _run(evaluate(a, _dispatcher(executed)))
        self.assertFalse(outcome.ok)
        self.assertEqual(executed, [])

    def test_applied_remediation_is_governed_and_fail_closed(self):
        # An irreversible remediation with no approver is blocked — the gate is
        # not bypassed and the platform is never touched.
        a = Assertion(name="amt", connector="bigquery", table="ds.orders",
                      metric="null_rate", op=">", threshold=0.01, column="amount",
                      remediation_tool="execute_bigquery_query",
                      remediation_args={"query": "DROP TABLE ds.orders"})
        executed: list = []
        outcome = _run(evaluate(a, _dispatcher(executed, approve=None), apply=True))
        self.assertTrue(outcome.breached)
        self.assertFalse(outcome.proposed_fix.applied)
        self.assertTrue(outcome.blocked)
        self.assertEqual(outcome.exit_code, 2)
        self.assertFalse(any("DROP" in s.upper() for s in executed), executed)


class SkillTest(unittest.TestCase):
    def test_skill_runs_and_postconditions_pass(self):
        executed: list = []
        ctx = SkillContext(dispatcher=_dispatcher(executed))
        skill = QualityAssertSkill()
        result = _run(skill.execute({
            "connector": "bigquery", "table": "ds.orders", "metric": "null_rate",
            "op": ">", "threshold": 0.01, "column": "amount",
        }, ctx))
        self.assertTrue(result.success, result.error)
        self.assertTrue(result.data["breached"])
        vctx = VerificationContext(args={}, result=result)
        report = _run(run_postconditions(skill.spec.postconditions, vctx))
        self.assertTrue(report.passed, report.summary())

    def test_proposed_remediation_defaults_to_dbt_model(self):
        a = Assertion(name="x", connector="bigquery", table="analytics.orders",
                      metric="row_count", op=">", threshold=0)
        fix = propose_remediation(a)
        self.assertEqual(fix.tool_name, "dbt_run")
        self.assertEqual(fix.args, {"select": "orders"})


class AssertCliTest(unittest.TestCase):
    def test_define_then_list(self):
        runner = CliRunner()
        with (
            runner.isolated_filesystem() as fs,
            patch.dict("os.environ", {STATE_PATH_ENV: str(Path(fs) / "state")}),
        ):
            r = runner.invoke(cli, [
                "assert", "define", "amt", "--connector", "bigquery",
                "--table", "ds.orders", "--metric", "null_rate", "--op", ">",
                "--threshold", "0.01", "--column", "amount",
            ], obj={})
            self.assertEqual(r.exit_code, 0, r.output)
            r = runner.invoke(cli, ["assert", "list"], obj={})
            self.assertEqual(r.exit_code, 0, r.output)
            self.assertIn("amt", r.output)


if __name__ == "__main__":
    unittest.main()
