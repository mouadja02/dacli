"""P12: object lineage as blast-radius evidence.

A best-effort lineage store (dbt manifest + warehouse view deps + orchestrator
DAGs) feeds the governor: dropping or replacing an object with known downstream
consumers names them in the audit decision and raises the tier. Absence of
lineage never errors and never invents a "safe" signal.
"""

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path

from click.testing import CliRunner

from dacli.connectors.base import (
    Connector, OperationSpec, Risk, ToolResult, ToolStatus,
)
from dacli.governance.audit import AuditLedger
from dacli.governance.classifier import Tier
from dacli.governance.governor import Governor
from dacli.governance.permissions import PermissionRegistry, Scope
from dacli.memory.catalog import CatalogEntry
from dacli.memory.graph.lineage import (
    LineageNode,
    LineageStore,
    action_targets,
    destructive_targets,
    edges_from_catalog,
    edges_from_dbt_manifest,
    edges_from_orchestrator,
)

FIXTURE = Path(__file__).parent / "fixtures" / "dbt_manifest.json"


def _run(coro):
    return asyncio.run(coro)


def _dbt_store():
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    store = LineageStore()
    store.ingest(edges_from_dbt_manifest(data))
    return store


# ---------------------------------------------------------------------------
# Store + adapters
# ---------------------------------------------------------------------------
class LineageStoreTest(unittest.TestCase):
    def test_dbt_downstream_lists_consuming_models(self):
        names = {n.label for n in _dbt_store().downstream("marts.orders")}
        self.assertEqual(names, {"customers"})

    def test_dbt_upstream_lists_producers(self):
        names = {n.label for n in _dbt_store().upstream("marts.orders")}
        self.assertEqual(names, {"stg_orders"})

    def test_bare_leaf_match_is_fail_safe_and_may_be_broad(self):
        # A bare identifier matches every object with that leaf (here both the
        # marts model and the raw source share "orders"). Over-citing is the safe
        # default; a qualified name disambiguates.
        broad = {n.label for n in _dbt_store().downstream("orders")}
        self.assertIn("customers", broad)

    def test_unqualified_name_matches_qualified_relation(self):
        # The dbt model relation is analytics.marts.orders; a bare "orders"
        # (or marts.orders) resolves to it.
        store = _dbt_store()
        self.assertTrue(store.downstream("orders"))
        self.assertTrue(store.downstream("marts.orders"))
        self.assertTrue(store.downstream("analytics.marts.orders"))

    def test_dbt_source_relation_has_model_consumer(self):
        # Dropping the warehouse table behind a dbt source breaks the staging model.
        consumers = _dbt_store().downstream("raw.public.orders")
        self.assertEqual({n.label for n in consumers}, {"stg_orders"})

    def test_catalog_view_dependency(self):
        view = CatalogEntry(
            connector="snowflake", object_type="view",
            scope={"database": "d", "schema": "marts", "object": "active_users"},
            extra={"view_dependencies": ["marts.users"]},
        )
        store = LineageStore()
        store.ingest(edges_from_catalog([view]))
        consumers = store.downstream("marts.users")
        self.assertEqual([n.label for n in consumers], ["active_users"])

    def test_orchestrator_dag_consumer(self):
        store = LineageStore()
        store.ingest(edges_from_orchestrator(
            [{"object": "marts.orders", "dag": "daily_revenue"}]
        ))
        consumers = store.downstream("marts.orders")
        self.assertEqual([n.display() for n in consumers],
                         ["airflow DAG daily_revenue"])

    def test_unknown_object_returns_empty(self):
        self.assertEqual(_dbt_store().downstream("nope.not_here"), [])

    def test_persistence_round_trip(self):
        path = Path(tempfile.mkdtemp()) / "lineage.json"
        store = _dbt_store()
        store.path = path
        store.save()
        reloaded = LineageStore.load(path)
        self.assertEqual(
            {n.label for n in reloaded.downstream("orders")},
            {n.label for n in store.downstream("orders")},
        )


class DestructiveTargetTest(unittest.TestCase):
    def test_drop_table(self):
        self.assertEqual(destructive_targets("DROP TABLE analytics.marts.orders"),
                         ["analytics.marts.orders"])

    def test_drop_if_exists_quoted(self):
        self.assertEqual(destructive_targets('DROP TABLE IF EXISTS "marts"."orders"'),
                         ['marts.orders'])

    def test_delete_from(self):
        self.assertEqual(destructive_targets("DELETE FROM marts.orders WHERE x"),
                         ["marts.orders"])

    def test_create_or_replace(self):
        self.assertEqual(
            destructive_targets("CREATE OR REPLACE TABLE marts.orders AS SELECT 1"),
            ["marts.orders"])

    def test_select_is_not_destructive(self):
        self.assertEqual(destructive_targets("SELECT * FROM marts.orders"), [])

    def test_action_targets_from_args_for_destructive_op(self):
        self.assertEqual(
            action_targets("overwrite_table", {"table": "marts.orders"}),
            ["marts.orders"])

    def test_action_targets_ignores_args_for_read_op(self):
        self.assertEqual(action_targets("read_table", {"table": "marts.orders"}), [])


# ---------------------------------------------------------------------------
# Governance: lineage as blast-radius evidence
# ---------------------------------------------------------------------------
class _SqlConnector(Connector):
    name = "snowflake"

    def operations(self):
        return [self.spec()]

    def spec(self, risk=Risk.SAFE, op="execute_query"):
        return OperationSpec(
            name=op, description="q",
            parameters={"type": "object", "properties": {}},
            capability="snowflake.query", risk=risk,
        )

    async def invoke(self, op, args):
        return ToolResult(tool_name=op, status=ToolStatus.SUCCESS, data=[])

    async def health(self):
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS)


def _governor(store, *, approvals=None, approve=True):
    def approval_fn(request):
        approvals.append(request)
        return approve

    return Governor(
        permissions=PermissionRegistry(default_scope=Scope.ADMIN),
        ledger=AuditLedger(path=str(Path(tempfile.mkdtemp()) / "audit.jsonl")),
        approval_fn=approval_fn if approvals is not None else None,
        lineage=store,
        use_shadow=False,
    )


class LineageGovernanceTest(unittest.TestCase):
    def _consumer_store(self):
        store = LineageStore()
        store.add(LineageNode("analytics.marts.orders", "table"),
                  LineageNode("analytics.marts.customers", "dbt model", label="customers"),
                  source="dbt")
        return store

    def test_drop_with_consumers_names_them(self):
        conn = _SqlConnector(settings=None)
        gov = _governor(self._consumer_store(), approvals=None)
        decision = _run(gov.review(
            "execute_query", conn.spec(), {"query": "DROP TABLE analytics.marts.orders"}, conn,
        ))
        self.assertEqual(decision.classification.tier, Tier.IRREVERSIBLE)
        self.assertTrue(any("customers" in r for r in decision.classification.reasons))

    def test_drop_records_a_lineage_audit_event(self):
        conn = _SqlConnector(settings=None)
        gov = _governor(self._consumer_store(), approvals=None)
        _run(gov.review(
            "execute_query", conn.spec(), {"query": "DROP TABLE analytics.marts.orders"}, conn,
        ))
        events = gov.ledger.all_events()
        lineage_events = [e for e in events if e.get("kind") == "lineage"]
        self.assertEqual(len(lineage_events), 1)
        self.assertIn("customers", lineage_events[0]["summary"])

    def test_overwrite_promotes_write_to_risky_and_reaches_approval(self):
        conn = _SqlConnector(settings=None)
        approvals = []
        gov = _governor(self._consumer_store(), approvals=approvals)
        decision = _run(gov.review(
            "overwrite_table", conn.spec(risk=Risk.WRITE, op="overwrite_table"),
            {"table": "analytics.marts.orders"}, conn,
        ))
        self.assertEqual(decision.classification.tier, Tier.RISKY)
        self.assertEqual(len(approvals), 1)
        self.assertIn("customers", "; ".join(approvals[0].classification.reasons))

    def test_absence_of_lineage_does_not_promote_or_error(self):
        conn = _SqlConnector(settings=None)
        gov = _governor(self._consumer_store(), approvals=None)
        # Object nobody consumes — write stays write, no lineage reason added.
        decision = _run(gov.review(
            "overwrite_table", conn.spec(risk=Risk.WRITE, op="overwrite_table"),
            {"table": "analytics.marts.unconsumed"}, conn,
        ))
        self.assertEqual(decision.classification.tier, Tier.WRITE)
        self.assertFalse(any("lineage" in r for r in decision.classification.reasons))

    def test_no_lineage_store_is_a_noop(self):
        conn = _SqlConnector(settings=None)
        gov = _governor(None, approvals=None)
        decision = _run(gov.review(
            "execute_query", conn.spec(), {"query": "DROP TABLE analytics.marts.orders"}, conn,
        ))
        self.assertEqual(decision.classification.tier, Tier.IRREVERSIBLE)
        self.assertFalse(any("downstream" in r for r in decision.classification.reasons))


class LineageCliTest(unittest.TestCase):
    """`dacli lineage <object>` lists dbt + warehouse + orchestrator consumers."""

    def test_command_lists_all_three_source_kinds(self):
        from dacli.scripts.cli import cli

        runner = CliRunner()
        with runner.isolated_filesystem() as fs:
            home = Path(fs)
            store = LineageStore(home / "memory" / "lineage.json")
            target = "analytics.marts.orders"
            store.add(LineageNode(target, "table"),
                      LineageNode("analytics.marts.customers", "dbt model", label="customers"),
                      source="dbt")
            store.add(LineageNode(target, "table"),
                      LineageNode("analytics.marts.active_orders", "view", label="active_orders"),
                      source="warehouse")
            store.add(LineageNode(target, "table"),
                      LineageNode("daily_revenue", "airflow DAG", label="daily_revenue"),
                      source="airflow")
            store.save()

            env = {**os.environ, "DACLI_HOME": str(home)}
            result = runner.invoke(cli, ["lineage", target, "--json"], env=env)

        self.assertEqual(result.exit_code, 0, result.output)
        # The group prints an "outside a project" state banner before the
        # command's JSON; parse from the first object brace.
        payload = json.loads(result.output[result.output.index("{"):])
        labels = {n["label"] for n in payload["downstream"]}
        self.assertEqual(labels, {"customers", "active_orders", "daily_revenue"})


if __name__ == "__main__":
    unittest.main()
