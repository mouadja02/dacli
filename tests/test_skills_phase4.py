""" (𝒮 Skill Routing & Verification) test suite.

Each test maps to an exit criterion in the plan. Run with:
    python -m unittest tests.test_skills_phase4
"""

import asyncio
import tempfile
import unittest

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.connectors.registry import ConnectorRegistry
from dacli.connectors.dispatcher import Dispatcher

from dacli.core.verify import (
    VerificationContext, Verifier, PipelineVerifier,
    require_postconditions, MissingPostConditionError, run_postconditions,
    result_succeeded,
)
from dacli.connectors.snowflake.connector import (
    parse_create_table, create_table_matches_intent,
)
from dacli.skills.registry import SkillRegistry
from dacli.skills.spec import Skill, SkillSpec, SkillContext
from dacli.skills.diagram_mermaid.skill import (
    MermaidSkill, entities_exist_in_catalog, mermaid_parses,
)
from dacli.memory.catalog import CatalogCache


def _run(coro):
    return asyncio.run(coro)


def _tmp(name):
    return tempfile.mkdtemp(prefix="dacli_p4_") + "/" + name


def _empty_dir():
    return tempfile.mkdtemp(prefix="dacli_p4_nc_")


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------
class _UncheckedConnector(Connector):
    """A connector whose op declares NO post-condition (should be rejected)."""

    name = "unchecked"

    def __init__(self):
        super().__init__(settings=None)
        self._is_connected = True

    def operations(self) -> list[OperationSpec]:
        return [OperationSpec(
            name="do_thing", description="x", parameters={"type": "object", "properties": {}},
            capability="unchecked.do", risk=Risk.SAFE,
        )]  # no postconditions

    async def invoke(self, op, args):
        return ToolResult(tool_name=op, status=ToolStatus.SUCCESS, data={})

    async def health(self):
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS)


class _FakeSnowflake(Connector):
    """Stands in for the real Snowflake connector with a controllable oracle.

    ``execute_snowflake_query`` always 'succeeds'; ``introspect_snowflake_object``
    returns whatever column set we configure — so we can simulate a CREATE TABLE
    whose live schema does (or does not) match what was declared.
    """

    name = "snowflake"

    def __init__(self, live_columns):
        super().__init__(settings=None)
        self._is_connected = True
        self._live_columns = live_columns  # list[{name,type}] or None for "missing"

    def operations(self) -> list[OperationSpec]:
        return [
            OperationSpec(
                name="execute_snowflake_query", description="run sql",
                parameters={"type": "object", "properties": {"query": {"type": "string"}}},
                capability="snowflake.query", risk=Risk.RISKY,
                postconditions=[result_succeeded(), create_table_matches_intent()],
            ),
            OperationSpec(
                name="introspect_snowflake_object", description="introspect",
                parameters={"type": "object", "properties": {}},
                capability="snowflake.introspection", risk=Risk.SAFE,
                postconditions=[result_succeeded()],
            ),
        ]

    async def invoke(self, op, args) -> ToolResult:
        if op == "execute_snowflake_query":
            return ToolResult(tool_name=op, status=ToolStatus.SUCCESS, data=None,
                              metadata={"query": args.get("query")})
        if op == "introspect_snowflake_object":
            exists = self._live_columns is not None
            return ToolResult(
                tool_name=op, status=ToolStatus.SUCCESS,
                data={"exists": exists, "object_type": "table",
                      "scope": args, "columns": self._live_columns},
            )
        return ToolResult(tool_name=op, status=ToolStatus.ERROR, error="unknown op")

    async def health(self):
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS)


class _FakeMemory:
    def __init__(self, catalog):
        self.catalog = catalog
        self.logged = []

    def log_tool_execution(self, **kwargs):
        self.logged.append(kwargs)


# ===========================================================================
# Exit criterion 1: the registry REJECTS an op/skill lacking a post-condition
# ===========================================================================
class MandatoryPostConditionsTest(unittest.TestCase):
    def test_require_postconditions_raises_on_empty(self):
        with self.assertRaises(MissingPostConditionError):
            require_postconditions("x", [])
        require_postconditions("x", [result_succeeded()])  # does not raise

    def test_connector_registry_rejects_unchecked_op(self):
        with self.assertRaises(MissingPostConditionError):
            ConnectorRegistry(
                settings=None,
                connectors_dir=_empty_dir(),
                config_path="__nonexistent__.yaml",
                extra_connectors=[_UncheckedConnector()],
                enforce_postconditions=True,
            )

    def test_connector_registry_allows_checked_op(self):
        reg = ConnectorRegistry(
            settings=None,
            connectors_dir=_empty_dir(),
            config_path="__nonexistent__.yaml",
            extra_connectors=[_FakeSnowflake(live_columns=[])],
            enforce_postconditions=True,
        )
        self.assertIsNotNone(reg.resolve("execute_snowflake_query"))

    def test_skill_registry_rejects_unchecked_skill(self):
        class _BadSkill(Skill):
            spec = SkillSpec(name="bad", description="no checks")  # no postconditions

            async def execute(self, args, context):
                return ToolResult(tool_name="bad", status=ToolStatus.SUCCESS)

        with self.assertRaises(MissingPostConditionError):
            SkillRegistry(skills_dir=_empty_dir(), extra_skills=[_BadSkill()])


# ===========================================================================
# Exit criterion 2: a deliberately-wrong CREATE TABLE is caught
# ===========================================================================
class CreateTablePostConditionTest(unittest.TestCase):
    QUERY = "CREATE TABLE BRONZE.RAW.CUSTOMERS (ID NUMBER, NAME VARCHAR)"

    def test_parser_extracts_columns(self):
        parsed = parse_create_table(self.QUERY)
        self.assertEqual(parsed["scope"]["object"], "CUSTOMERS")
        self.assertEqual(parsed["columns"], ["ID", "NAME"])

    def _verify(self, live_columns):
        target = _FakeSnowflake(live_columns=live_columns)
        ctx = VerificationContext(
            args={"query": self.QUERY},
            result=ToolResult(tool_name="execute_snowflake_query", status=ToolStatus.SUCCESS),
            target=target,
        )
        return _run(run_postconditions([create_table_matches_intent()], ctx))

    def test_matching_columns_pass(self):
        report = self._verify([{"name": "ID", "type": "NUMBER"}, {"name": "NAME", "type": "VARCHAR"}])
        self.assertTrue(report.passed, report.summary())

    def test_wrong_column_set_is_caught(self):
        # Live table is missing NAME and has a bogus EMAIL column.
        report = self._verify([{"name": "ID", "type": "NUMBER"}, {"name": "EMAIL", "type": "VARCHAR"}])
        self.assertFalse(report.passed)
        self.assertIn("column set mismatch", report.summary())

    def test_missing_object_is_caught(self):
        report = self._verify(None)  # object not in information_schema
        self.assertFalse(report.passed)
        self.assertIn("not found", report.summary())

    def test_dispatcher_downgrades_unverified_create(self):
        # End-to-end: a wrong CREATE TABLE goes through the real dispatch path and
        # is downgraded from SUCCESS to ERROR by the verifier.
        target = _FakeSnowflake(live_columns=[{"name": "ID", "type": "NUMBER"}])  # missing NAME
        reg = ConnectorRegistry(
            settings=None, connectors_dir=_empty_dir(), config_path="__nonexistent__.yaml",
            extra_connectors=[target], enforce_postconditions=True,
        )
        disp = Dispatcher(reg, memory=_FakeMemory(None), verifier=Verifier(enforce=True))
        res = _run(disp.execute("execute_snowflake_query", {"query": self.QUERY}))
        self.assertEqual(res.status, ToolStatus.ERROR)
        self.assertIn("verification", res.metadata)
        self.assertFalse(res.metadata["verification"]["passed"])


# ===========================================================================
# Exit criterion 5: Mermaid renders a valid ER diagram; rejects ghost tables
# ===========================================================================
class MermaidSkillTest(unittest.TestCase):
    def _memory(self, objects):
        cat = CatalogCache(path=_tmp("cat.json"))
        for obj in objects:
            cat.record_object("snowflake", "table",
                              {"database": "B", "schema": "RAW", "object": obj},
                              columns=[{"name": "ID", "type": "NUMBER"}])
        return _FakeMemory(cat)

    def test_renders_valid_er_from_catalog(self):
        mem = self._memory(["CUSTOMERS", "ORDERS"])
        skill = MermaidSkill()
        res = _run(skill.execute({"diagram_type": "er"}, SkillContext(memory=mem)))
        self.assertTrue(res.data["mermaid"].startswith("erDiagram"))
        self.assertEqual(set(res.data["entities"]), {"CUSTOMERS", "ORDERS"})
        # Its own post-conditions accept it.
        ctx = VerificationContext(result=res, memory=mem)
        report = _run(run_postconditions(MermaidSkill.spec.postconditions, ctx))
        self.assertTrue(report.passed, report.summary())

    def test_postcondition_rejects_nonexistent_table(self):
        mem = self._memory(["CUSTOMERS"])  # catalog has only CUSTOMERS
        # A doctored diagram that references a table not in the catalog.
        bad = ToolResult(
            tool_name="diagram-mermaid", status=ToolStatus.SUCCESS,
            data={"mermaid": "erDiagram\n    GHOST {\n        STRING X\n    }\n",
                  "entities": ["GHOST"]},
        )
        ctx = VerificationContext(result=bad, memory=mem)
        report = _run(run_postconditions([entities_exist_in_catalog()], ctx))
        self.assertFalse(report.passed)
        self.assertIn("GHOST", report.summary())

    def test_mermaid_parses_rejects_garbage(self):
        bad = ToolResult(tool_name="d", status=ToolStatus.SUCCESS,
                         data={"mermaid": "not a diagram {", "entities": []})
        ctx = VerificationContext(result=bad, memory=None)
        report = _run(run_postconditions([mermaid_parses()], ctx))
        self.assertFalse(report.passed)


# ===========================================================================
# Exit criterion 6: a two-skill chain aborts when the handoff schema mismatches
# ===========================================================================
class HandoffVerificationTest(unittest.TestCase):
    DOWNSTREAM_SCHEMA = {
        "type": "object",
        "properties": {"rows": {"type": "array"}},
        "required": ["rows"],
    }

    def test_matching_handoff_passes(self):
        pv = PipelineVerifier()
        ok = pv.verify_handoff({"rows": [1, 2, 3]}, self.DOWNSTREAM_SCHEMA)
        self.assertTrue(ok.ok)
        self.assertEqual(ok.errors, [])

    def test_mismatched_handoff_aborts_chain(self):
        pv = PipelineVerifier()
        # Upstream produced {mermaid, entities} — no 'rows' the downstream needs.
        upstream = {"mermaid": "erDiagram\n", "entities": ["A"]}

        # Simulate a 2-step chain that stops cleanly on a bad handoff.
        steps_run = []

        def run_chain():
            steps_run.append("step1")
            handoff = pv.verify_handoff(upstream, self.DOWNSTREAM_SCHEMA)
            if not handoff.ok:
                return {"aborted_at": 0, "error": handoff.errors}
            steps_run.append("step2")  # must NOT happen
            return {"aborted_at": None}

        outcome = run_chain()
        self.assertEqual(outcome["aborted_at"], 0)
        self.assertEqual(steps_run, ["step1"])  # step2 never ran
        self.assertTrue(any("rows" in e for e in outcome["error"]))


if __name__ == "__main__":
    unittest.main()
