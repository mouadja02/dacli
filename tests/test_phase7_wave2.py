"""Wave 2 connector golden tests (offline, CLI runner injected).

Wave 2 reaches operational databases — relational (Postgres, MySQL) and NoSQL
(MongoDB, DynamoDB). The NoSQL pair is the point: they prove the connector
contract is not secretly SQL-only (schema *inference*, count/acknowledgement
oracles, non-SQL native rollback primitives).

Run with:
    python -m unittest tests.test_phase7_wave2
"""

import asyncio
import json
import types
import unittest

from dacli.connectors.base import Risk, ToolStatus
from dacli.connectors.cli_base import CliResult
from dacli.core.verify import VerificationContext, run_postconditions

from dacli.connectors.postgres.connector import PostgresConnector, postgres_ddl_object_exists
from dacli.connectors.mysql.connector import MySQLConnector, mysql_ddl_object_exists
from dacli.connectors.mongodb.connector import MongoDBConnector
from dacli.connectors.dynamodb.connector import DynamoDBConnector

from dacli.governance.rollback import RollbackStrategist
from dacli.governance.classifier import Classification, Tier


def _run(coro):
    return asyncio.run(coro)


def _settings(**sections):
    return types.SimpleNamespace(**sections)


def _runner(responder):
    async def run(argv, *, cwd=None, env=None, timeout=None, stdin=None):
        return responder(list(argv), stdin)
    return run


# ===========================================================================
# Postgres — information_schema is the oracle for a CREATE
# ===========================================================================
class PostgresConnectorTest(unittest.TestCase):
    def _conn(self, responder):
        cfg = {"host": "h", "port": 5432, "database": "db", "user": "u",
               "password": "p", "sslmode": "", "psql_binary": "psql", "timeout": 300}
        return PostgresConnector(_settings(connector_config={"postgres": cfg}), runner=_runner(responder))

    def test_create_confirmed_by_information_schema(self):
        def responder(argv, stdin):
            sql = argv[-1]
            if "information_schema.columns" in sql:
                return CliResult(0, "column_name,data_type\nid,integer\n", "", argv)
            return CliResult(0, "CREATE TABLE\n", "", argv)
        conn = self._conn(responder)
        q = "CREATE TABLE public.customers (id int)"
        res = _run(conn.invoke("execute_postgres_query", {"query": q}))
        self.assertEqual(res.status, ToolStatus.SUCCESS)
        ctx = VerificationContext(args={"query": q}, result=res, target=conn)
        report = _run(run_postconditions([postgres_ddl_object_exists()], ctx))
        self.assertTrue(report.passed, report.summary())

    def test_create_missing_relation_caught(self):
        def responder(argv, stdin):
            sql = argv[-1]
            if "information_schema.columns" in sql:
                return CliResult(0, "column_name,data_type\n", "", argv)  # no rows
            return CliResult(0, "CREATE TABLE\n", "", argv)
        conn = self._conn(responder)
        q = "CREATE TABLE public.customers (id int)"
        res = _run(conn.invoke("execute_postgres_query", {"query": q}))
        ctx = VerificationContext(args={"query": q}, result=res, target=conn)
        report = _run(run_postconditions([postgres_ddl_object_exists()], ctx))
        self.assertFalse(report.passed)

    def test_select_rows_parsed(self):
        conn = self._conn(lambda argv, stdin: CliResult(0, "id,name\n1,alice\n2,bob\n", "", argv))
        res = _run(conn.invoke("execute_postgres_query", {"query": "SELECT id,name FROM t"}))
        self.assertEqual(res.data, [{"id": "1", "name": "alice"}, {"id": "2", "name": "bob"}])

    def test_verify_rollback_transaction_true(self):
        conn = self._conn(lambda argv, stdin: CliResult(0, "", "", argv))
        plan = types.SimpleNamespace(primitive="transaction")
        ok, _ = _run(conn.verify_rollback(plan, {"query": "DELETE FROM t"}))
        self.assertTrue(ok)

    def test_verify_rollback_pgdump_requires_existing_relation(self):
        def responder(argv, stdin):
            return CliResult(0, "column_name,data_type\n", "", argv)  # introspect → no rows
        conn = self._conn(responder)
        plan = types.SimpleNamespace(primitive="pg_dump_snapshot")
        ok, _ = _run(conn.verify_rollback(plan, {"query": "DROP TABLE public.gone"}))
        self.assertFalse(ok)


# ===========================================================================
# MySQL — DDL is not transactional; introspection via information_schema
# ===========================================================================
class MySQLConnectorTest(unittest.TestCase):
    def _conn(self, responder):
        cfg = {"host": "h", "port": 3306, "database": "db", "user": "u",
               "password": "p", "mysql_binary": "mysql", "timeout": 300}
        return MySQLConnector(_settings(connector_config={"mysql": cfg}), runner=_runner(responder))

    def test_create_confirmed(self):
        def responder(argv, stdin):
            sql = argv[-1]
            if "information_schema.columns" in sql:
                return CliResult(0, "column_name\tdata_type\nid\tint\n", "", argv)
            return CliResult(0, "", "", argv)
        conn = self._conn(responder)
        q = "CREATE TABLE customers (id int)"
        res = _run(conn.invoke("execute_mysql_query", {"query": q}))
        ctx = VerificationContext(args={"query": q}, result=res, target=conn)
        report = _run(run_postconditions([mysql_ddl_object_exists()], ctx))
        self.assertTrue(report.passed, report.summary())

    def test_drop_rollback_is_mysqldump(self):
        cls = Classification(tool_name="q", tier=Tier.IRREVERSIBLE,
                             declared_risk=Risk.RISKY, sql_verb="DROP")
        plan = RollbackStrategist().plan_for("mysql", cls)
        self.assertEqual(plan.primitive, "mysqldump_snapshot")


# ===========================================================================
# MongoDB — schema inference + count/acknowledgement oracles (no SQL)
# ===========================================================================
class MongoDBConnectorTest(unittest.TestCase):
    def _conn(self, responder):
        cfg = {"uri": "mongodb://h", "database": "app", "sample_size": 50,
               "mongosh_binary": "mongosh", "timeout": 300}
        return MongoDBConnector(_settings(connector_config={"mongodb": cfg}), runner=_runner(responder))

    def test_introspect_infers_schema(self):
        payload = {"exists": True, "count": 3,
                   "fields": {"_id": {"object": 3}, "name": {"string": 3}, "age": {"number": 2}}}
        conn = self._conn(lambda argv, stdin: CliResult(0, json.dumps(payload), "", argv))
        res = _run(conn.invoke("introspect_mongodb_collection", {"collection": "users"}))
        self.assertTrue(res.data["exists"])
        self.assertEqual(res.data["count"], 3)
        op = next(o for o in conn.operations() if o.name == "introspect_mongodb_collection")
        ctx = VerificationContext(args={}, result=res, target=conn)
        self.assertTrue(_run(run_postconditions(op.postconditions, ctx)).passed)
        # catalog effect carries inferred columns
        self.assertTrue(res.metadata["catalog_effects"])

    def test_insert_acknowledged_count_checked(self):
        payload = {"acknowledged": True, "insertedCount": 2}
        conn = self._conn(lambda argv, stdin: CliResult(0, json.dumps(payload), "", argv))
        res = _run(conn.invoke("insert_mongodb_documents",
                               {"collection": "c", "documents": [{"a": 1}, {"a": 2}]}))
        op = next(o for o in conn.operations() if o.name == "insert_mongodb_documents")
        ctx = VerificationContext(args={"documents": [{"a": 1}, {"a": 2}]}, result=res, target=conn)
        self.assertTrue(_run(run_postconditions(op.postconditions, ctx)).passed)

    def test_insert_count_mismatch_caught(self):
        payload = {"acknowledged": True, "insertedCount": 1}  # asked for 2
        conn = self._conn(lambda argv, stdin: CliResult(0, json.dumps(payload), "", argv))
        res = _run(conn.invoke("insert_mongodb_documents",
                               {"collection": "c", "documents": [{"a": 1}, {"a": 2}]}))
        op = next(o for o in conn.operations() if o.name == "insert_mongodb_documents")
        ctx = VerificationContext(args={"documents": [{"a": 1}, {"a": 2}]}, result=res, target=conn)
        self.assertFalse(_run(run_postconditions(op.postconditions, ctx)).passed)

    def test_delete_is_irreversible_and_dump_backed(self):
        conn = self._conn(lambda argv, stdin: CliResult(0, "{}", "", argv))
        op = next(o for o in conn.operations() if o.name == "delete_mongodb_documents")
        self.assertEqual(op.risk, Risk.IRREVERSIBLE)
        # verify_rollback needs the collection to exist (dumpable)
        def responder(argv, stdin):
            return CliResult(0, json.dumps({"exists": True, "count": 5, "fields": {}}), "", argv)
        conn2 = self._conn(responder)
        plan = types.SimpleNamespace(primitive="mongodump_snapshot")
        ok, _ = _run(conn2.verify_rollback(plan, {"collection": "users"}))
        self.assertTrue(ok)


# ===========================================================================
# DynamoDB — live get-item / describe-table oracle; PITR rollback
# ===========================================================================
class DynamoDBConnectorTest(unittest.TestCase):
    def _conn(self, responder):
        cfg = {"region": "us-east-1", "profile": "", "aws_binary": "aws", "timeout": 300}
        return DynamoDBConnector(_settings(connector_config={"dynamodb": cfg}), runner=_runner(responder))

    def test_put_then_present(self):
        def responder(argv, stdin):
            if "describe-table" in argv:
                return CliResult(0, json.dumps({"Table": {"KeySchema": [
                    {"AttributeName": "id", "KeyType": "HASH"}]}}), "", argv)
            if "get-item" in argv:
                return CliResult(0, json.dumps({"Item": {"id": {"S": "1"}}}), "", argv)
            return CliResult(0, "{}", "", argv)  # put-item
        conn = self._conn(responder)
        res = _run(conn.invoke("put_dynamodb_item",
                               {"table": "t", "item": {"id": {"S": "1"}, "v": {"N": "9"}}}))
        self.assertTrue(res.data["exists"])
        op = next(o for o in conn.operations() if o.name == "put_dynamodb_item")
        ctx = VerificationContext(args={}, result=res, target=conn)
        self.assertTrue(_run(run_postconditions(op.postconditions, ctx)).passed)

    def test_delete_item_then_absent(self):
        def responder(argv, stdin):
            if "get-item" in argv:
                return CliResult(0, "{}", "", argv)  # gone (no Item)
            return CliResult(0, "{}", "", argv)
        conn = self._conn(responder)
        res = _run(conn.invoke("delete_dynamodb_item", {"table": "t", "key": {"id": {"S": "1"}}}))
        self.assertFalse(res.data["exists"])
        op = next(o for o in conn.operations() if o.name == "delete_dynamodb_item")
        ctx = VerificationContext(args={}, result=res, target=conn)
        self.assertTrue(_run(run_postconditions(op.postconditions, ctx)).passed)

    def test_delete_table_irreversible_and_pitr_verified(self):
        conn = self._conn(lambda argv, stdin: CliResult(0, "{}", "", argv))
        op = next(o for o in conn.operations() if o.name == "delete_dynamodb_table")
        self.assertEqual(op.risk, Risk.IRREVERSIBLE)

        def enabled(argv, stdin):
            return CliResult(0, json.dumps({"ContinuousBackupsDescription": {
                "PointInTimeRecoveryDescription": {"PointInTimeRecoveryStatus": "ENABLED"}}}), "", argv)
        conn_ok = self._conn(enabled)
        plan = types.SimpleNamespace(primitive="dynamodb_pitr")
        ok, _ = _run(conn_ok.verify_rollback(plan, {"table": "t"}))
        self.assertTrue(ok)

        def disabled(argv, stdin):
            return CliResult(0, json.dumps({"ContinuousBackupsDescription": {
                "PointInTimeRecoveryDescription": {"PointInTimeRecoveryStatus": "DISABLED"}}}), "", argv)
        conn_no = self._conn(disabled)
        ok2, _ = _run(conn_no.verify_rollback(plan, {"table": "t"}))
        self.assertFalse(ok2)

    def test_introspect_describes_keys(self):
        payload = {"Table": {"KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                             "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}]}}
        conn = self._conn(lambda argv, stdin: CliResult(0, json.dumps(payload), "", argv))
        res = _run(conn.invoke("introspect_dynamodb_table", {"table": "t"}))
        self.assertTrue(res.data["exists"])
        self.assertEqual(res.data["keys"][0]["name"], "id")


# ===========================================================================
# Rollback parity — Wave 2 native primitives
# ===========================================================================
class Wave2RollbackParityTest(unittest.TestCase):
    def _cls(self, tier, verb=None):
        return Classification(tool_name="q", tier=tier, declared_risk=Risk.RISKY, sql_verb=verb)

    def test_native_primitives(self):
        strat = RollbackStrategist()
        cases = [
            ("postgres", self._cls(Tier.IRREVERSIBLE, "DROP"), "pg_dump_snapshot"),
            ("postgres", self._cls(Tier.RISKY, "DELETE"), "transaction"),
            ("mysql", self._cls(Tier.IRREVERSIBLE, "TRUNCATE"), "mysqldump_snapshot"),
            ("mysql", self._cls(Tier.RISKY, "UPDATE"), "transaction"),
            ("mongodb", self._cls(Tier.IRREVERSIBLE, None), "mongodump_snapshot"),
            ("dynamodb", self._cls(Tier.IRREVERSIBLE, None), "dynamodb_pitr"),
        ]
        for cid, cls, expected in cases:
            self.assertEqual(strat.plan_for(cid, cls).primitive, expected,
                             f"{cid} expected {expected}")


if __name__ == "__main__":
    unittest.main()
