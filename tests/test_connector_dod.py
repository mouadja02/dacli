""" — the mechanical *Definition of Done* gate.

This is the CI gate that keeps capability (𝒮) and governance (𝒢) scaling
*together*: every shipped connector must carry its post-conditions, rollback
strategy, permission scope, introspection op, SKILL.md, and a golden task, or the
build fails. It is the structural cure for governance debt.

Run with:
    python -m unittest tests.test_connector_dod
"""

import unittest

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.connectors.dod import audit_connectors, check_connector_dod


class AllShippedConnectorsPassDod(unittest.TestCase):
    def test_every_connector_meets_the_dod(self):
        report = audit_connectors(settings=None)
        # We actually discovered the shipped connectors (not a vacuous pass).
        # After M11 only the seeds' old Connector classes remain on disk.
        self.assertTrue(report.checked, "no connectors were discovered")
        for expected in ("snowflake", "github"):
            self.assertIn(expected, report.checked,
                          f"{expected} connector not discovered")
        self.assertTrue(report.passed, "\n" + report.summary())


# ---------------------------------------------------------------------------
# The gate must have teeth: a non-compliant connector is rejected.
# ---------------------------------------------------------------------------
class _BadConnector(Connector):
    """No introspection op, a mutating op with only result_succeeded, no rollback."""

    name = "bad"

    def __init__(self):
        super().__init__(settings=None)

    def operations(self) -> list[OperationSpec]:
        from dacli.core.verify import result_succeeded
        return [
            OperationSpec(
                name="bad_write", description="writes something",
                parameters={"type": "object", "properties": {}},
                capability="bad.write", risk=Risk.WRITE,
                category="write",
                postconditions=[result_succeeded()],  # no anchored-beyond-success check
            ),
        ]

    async def invoke(self, op, args):
        return ToolResult(tool_name=op, status=ToolStatus.SUCCESS)

    async def health(self):
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS)


class DodHasTeeth(unittest.TestCase):
    def test_bad_connector_collects_violations(self):
        manifest = {  # missing default_scope + golden_task on purpose
            "id": "bad", "name": "Bad", "description": "x",
            "class": "tests.x.Bad", "enabled": False, "required_config": [],
        }
        violations = check_connector_dod("bad", manifest, _BadConnector(), connector_dir=None)
        rules = {v.rule for v in violations}
        self.assertIn("permission_scope", rules)   # no default_scope
        self.assertIn("introspection", rules)       # no read/introspection op
        self.assertIn("postconditions", rules)      # mutating op lacks anchored check
        self.assertIn("rollback", rules)            # mutating op, no planner registered
        self.assertIn("golden_task", rules)         # no golden task

    def test_compliant_manifest_shape_clears_manifest_rules(self):
        # A fully-formed manifest for a read-only connector with introspection
        # should not raise the manifest/scope/golden rules.
        from dacli.core.verify import data_has_keys

        class _Good(Connector):
            name = "good"

            def __init__(self):
                super().__init__(settings=None)

            def operations(self):
                return [OperationSpec(
                    name="introspect_good", description="read",
                    parameters={"type": "object", "properties": {}},
                    capability="good.introspection", risk=Risk.SAFE,
                    category="introspection",
                    postconditions=[data_has_keys("exists", name="ok")],
                )]

            async def invoke(self, op, args):
                return ToolResult(tool_name=op, status=ToolStatus.SUCCESS, data={"exists": True})

            async def health(self):
                return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS)

        manifest = {
            "id": "good", "name": "Good", "description": "x",
            "class": "tests.x.Good", "enabled": False, "required_config": [],
            "default_scope": "read_only",
            "golden_task": {"name": "n", "op": "introspect_good", "description": "d"},
        }
        violations = check_connector_dod("good", manifest, _Good(), connector_dir=None)
        rules = {v.rule for v in violations}
        # Only SKILL.md is unverifiable here (connector_dir=None skips it).
        self.assertNotIn("permission_scope", rules)
        self.assertNotIn("introspection", rules)
        self.assertNotIn("golden_task", rules)
        self.assertNotIn("postconditions", rules)
        self.assertNotIn("rollback", rules)  # no mutating op → no planner required


if __name__ == "__main__":
    unittest.main()
