"""P14 slice B: governed runbooks + a policy envelope.

A runbook is a saved, parameterized headless task whose envelope pre-grants
approval *only within* a tool set and a tier ceiling. In-envelope actions
auto-approve; out-of-envelope actions still prompt — which on the headless path
(no interactive approver) means they block, fail-closed. Every decision and the
envelope itself land in the audit ledger.

Offline: a fake risky connector exercises the governance gate; a ScriptedLLM
drives the end-to-end headless path.
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.core.paths import STATE_PATH_ENV
from dacli.core.runbooks import (
    PolicyEnvelope,
    Runbook,
    delete_runbook,
    list_runbooks,
    load_runbook,
    run_runbook,
    save_runbook,
)
from dacli.governance.audit import AuditLedger
from dacli.governance.governor import Governor
from dacli.governance.permissions import PermissionRegistry, Scope
from dacli.governance.vocab import Tier

_TEMP_DIRS: list[str] = []


def tearDownModule():
    for d in _TEMP_DIRS:
        shutil.rmtree(d, ignore_errors=True)


def _hermetic_settings():
    from dacli.config.settings import Settings

    settings = Settings.model_validate({
        "llm": {"provider": "scripted", "model": "scripted",
                "api_key": "scripted", "base_url": "https://api.test.local"},
    })
    root = tempfile.mkdtemp(prefix="dacli_runbook_test_")
    _TEMP_DIRS.append(root)
    settings.agent.state_path = os.path.join(root, "state.json")
    settings.agent.history_path = os.path.join(root, "history.json")
    settings.sandbox.enabled = False
    return settings


def _run(coro):
    return asyncio.run(coro)


class _RiskyConnector(Connector):
    name = "warehouse"

    def __init__(self):
        super().__init__(settings=None)

    def operations(self):
        return [self.spec()]

    def spec(self):
        return OperationSpec(
            name="do_risky_thing", description="x",
            parameters={"type": "object", "properties": {}},
            capability="warehouse.thing", risk=Risk.RISKY,
            postconditions=[],
        )

    async def invoke(self, op, args):
        return ToolResult(tool_name=op, status=ToolStatus.SUCCESS, data=[])

    async def health(self):
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS)


def _governor(envelope: PolicyEnvelope) -> Governor:
    return Governor(
        permissions=PermissionRegistry(default_scope=Scope.ADMIN),
        ledger=AuditLedger(path=str(Path(tempfile.mkdtemp()) / "audit.jsonl")),
        approval_fn=lambda req: envelope.permits(req)[0],
        use_shadow=False,
    )


class PolicyEnvelopeTest(unittest.TestCase):
    def _req(self, tool, tier):
        return mock.Mock(tool_name=tool, tier=tier)

    def test_in_envelope_permitted(self):
        env = PolicyEnvelope(tools=["do_risky_thing"], max_tier="risky")
        ok, _ = env.permits(self._req("do_risky_thing", Tier.RISKY))
        self.assertTrue(ok)

    def test_tool_outside_envelope_refused(self):
        env = PolicyEnvelope(tools=["other"], max_tier="risky")
        ok, reason = env.permits(self._req("do_risky_thing", Tier.RISKY))
        self.assertFalse(ok)
        self.assertIn("outside the envelope", reason)

    def test_tier_above_ceiling_refused(self):
        env = PolicyEnvelope(tools=["do_risky_thing"], max_tier="write")
        ok, reason = env.permits(self._req("do_risky_thing", Tier.RISKY))
        self.assertFalse(ok)
        self.assertIn("exceeds", reason)

    def test_wildcard_allows_any_tool(self):
        env = PolicyEnvelope(tools=["*"], max_tier="irreversible")
        ok, _ = env.permits(self._req("anything", Tier.IRREVERSIBLE))
        self.assertTrue(ok)


class GovernanceGateTest(unittest.TestCase):
    """The envelope, wired as the approver, scopes auto-approval at the gate."""

    def _review(self, envelope):
        conn = _RiskyConnector()
        gov = _governor(envelope)
        return _run(gov.review("do_risky_thing", conn.spec(), {}, conn))

    def test_in_envelope_action_is_auto_approved(self):
        decision = self._review(PolicyEnvelope(tools=["do_risky_thing"], max_tier="risky"))
        self.assertTrue(decision.allowed)

    def test_out_of_envelope_tool_is_refused(self):
        decision = self._review(PolicyEnvelope(tools=["something_else"], max_tier="risky"))
        self.assertFalse(decision.allowed)

    def test_out_of_envelope_tier_is_refused(self):
        decision = self._review(PolicyEnvelope(tools=["do_risky_thing"], max_tier="write"))
        self.assertFalse(decision.allowed)


class PersistenceTest(unittest.TestCase):
    def test_save_load_list_delete(self):
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.dict(os.environ, {STATE_PATH_ENV: str(Path(tmp) / "state")}),
        ):
            rb = Runbook(name="nightly", turns=["check {table}"],
                         params={"table": "orders"},
                         envelope=PolicyEnvelope(tools=["dbt_run"], max_tier="write"))
            save_runbook(rb)
            self.assertEqual(list_runbooks(), ["nightly"])
            loaded = load_runbook("nightly")
            self.assertEqual(loaded.turns, ["check {table}"])
            self.assertEqual(loaded.envelope.tools, ["dbt_run"])
            self.assertTrue(delete_runbook("nightly"))
            self.assertEqual(list_runbooks(), [])


class RenderTest(unittest.TestCase):
    def test_param_substitution(self):
        rb = Runbook(name="r", turns=["run {model} in {env}"], params={"env": "dev"})
        self.assertEqual(rb.render({"model": "orders"}), ["run orders in dev"])

    def test_missing_param_raises(self):
        rb = Runbook(name="r", turns=["run {model}"])
        with self.assertRaises(ValueError):
            rb.render()


class EndToEndTest(unittest.TestCase):
    def setUp(self):
        self._pricing = mock.patch("dacli.core.host.fetch_pricing", return_value=None)
        self._pricing.start()
        self.addCleanup(self._pricing.stop)

    def test_out_of_envelope_action_blocks_and_envelope_is_audited(self):
        from dacli.ai.scripted import ScriptedLLM

        # run_shell_command is NOT in the envelope, so a destructive command is
        # refused before it runs (exit 2), proving the envelope doesn't widen
        # the secure defaults.
        llm = ScriptedLLM([
            {"text": "wiping",
             "tool_calls": [{"name": "run_shell_command",
                             "arguments": {"command": "rm -rf /tmp/zz"}}]},
            {"text": "I was blocked."},
        ])
        rb = Runbook(name="safe_only", turns=["clean up"],
                     envelope=PolicyEnvelope(tools=["update_plan"], max_tier="write"))
        result = _run(run_runbook(rb, settings=_hermetic_settings(), llm=llm))
        self.assertEqual(result.exit_code, 2)

        events = AuditLedger(path=result.audit_path).all_events()
        runbook_events = [e for e in events if e.get("kind") == "runbook"]
        self.assertEqual(len(runbook_events), 1)
        self.assertEqual(runbook_events[0]["detail"]["envelope"]["tools"], ["update_plan"])


if __name__ == "__main__":
    unittest.main()
