"""F-7 (P11) — opt-in MCP client bridge.

With a stubbed in-process MCP session (the SDK is never imported): tools are
discovered and mapped to OperationSpecs with a conservative default risk, and
a proxied call routes through Dispatcher → Governor (denied under deny mode
for a risky-classified tool). Default install stays inert.
"""

import asyncio
import tempfile
import unittest
from typing import Any

from dacli.config.settings import Settings
from dacli.connectors.base import Risk, ToolStatus
from dacli.connectors.dispatcher import Dispatcher
from dacli.connectors.mcp_bridge.connector import McpBridgeConnector
from dacli.connectors.registry import ConnectorRegistry
from dacli.governance.audit import AuditLedger
from dacli.governance.classifier import ActionClassifier
from dacli.governance.governor import Governor
from dacli.governance.permissions import PermissionRegistry, Scope
from dacli.governance.policy_engine import PolicyEngine
from dacli.governance.rollback import RollbackStrategist
from dacli.governance.shadow import ShadowExecutor


def _run(coro):
    return asyncio.run(coro)


def _tmp(name):
    return tempfile.mkdtemp(prefix="dacli_p11_mcp_") + "/" + name


_TOOLS = [
    {
        "name": "do_thing",
        "description": "Does a thing on the remote system.",
        "inputSchema": {
            "type": "object",
            "properties": {"q": {"type": "string"}},
        },
    },
    {
        "name": "list_models",
        "description": "Read-only model listing.",
        "inputSchema": {"type": "object", "properties": {}},
    },
]


class _StubSession:
    """In-process stand-in for an MCP ClientSession."""

    def __init__(self, tools=None):
        self.tools = list(tools if tools is not None else _TOOLS)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def list_tools(self):
        return {"tools": self.tools}

    async def call_tool(self, name, arguments):
        self.calls.append((name, dict(arguments or {})))
        return {"content": [{"text": f"{name} ok"}], "isError": False}


def _bridge(*, overrides=None, tools=None):
    settings = Settings()
    settings.mcp.command = "stub-mcp-server"
    if overrides:
        settings.mcp.risk_overrides = dict(overrides)
    bridge = McpBridgeConnector(settings)
    session = _StubSession(tools=tools)

    async def factory():
        return session

    bridge._client_factory = factory
    return bridge, session


class InertByDefaultTest(unittest.TestCase):
    """A default install never speaks MCP."""

    def test_unconfigured_bridge_exposes_only_introspection(self):
        bridge = McpBridgeConnector(Settings())
        ops = bridge.operations()
        self.assertEqual([o.name for o in ops], ["mcp_list_tools"])
        self.assertFalse(_run(bridge.connect()))
        self.assertFalse(bridge.is_connected)
        health = _run(bridge.health())
        self.assertEqual(health.status, ToolStatus.ERROR)
        self.assertIn("not configured", health.error)

    def test_settings_default_is_unconfigured(self):
        settings = Settings()
        self.assertEqual(settings.mcp.command, "")
        self.assertEqual(settings.mcp.url, "")
        self.assertEqual(settings.mcp.default_risk, "risky")


class DiscoveryTest(unittest.TestCase):
    def test_tools_map_to_specs_with_conservative_default_risk(self):
        bridge, _ = _bridge()
        self.assertTrue(_run(bridge.connect()))
        specs = {s.name: s for s in bridge.operations()}
        self.assertIn("mcp_do_thing", specs)
        self.assertIn("mcp_list_models", specs)

        spec = specs["mcp_do_thing"]
        self.assertEqual(spec.risk, Risk.RISKY)               # default: risky
        self.assertEqual(spec.parameters["properties"]["q"]["type"], "string")
        self.assertTrue(spec.postconditions)                  # generic gate
        self.assertIn("MCP", spec.description)

    def test_user_pinned_risk_override_and_bogus_value(self):
        bridge, _ = _bridge(overrides={"list_models": "safe", "do_thing": "bogus"})
        self.assertTrue(_run(bridge.connect()))
        specs = {s.name: s for s in bridge.operations()}
        self.assertEqual(specs["mcp_list_models"].risk, Risk.SAFE)
        # An unknown/typo'd risk value falls back to risky (deny-by-default).
        self.assertEqual(specs["mcp_do_thing"].risk, Risk.RISKY)

    def test_list_tools_op_reports_inventory(self):
        bridge, _ = _bridge()
        self.assertTrue(_run(bridge.connect()))
        result = _run(bridge.invoke("mcp_list_tools", {}))
        self.assertEqual(result.status, ToolStatus.SUCCESS)
        names = {t["name"] for t in result.data["tools"]}
        self.assertEqual(names, {"do_thing", "list_models"})
        self.assertEqual(result.data["count"], 2)

    def test_invoke_proxies_to_the_remote_tool_name(self):
        bridge, session = _bridge()
        self.assertTrue(_run(bridge.connect()))
        result = _run(bridge.invoke("mcp_do_thing", {"q": "hello"}))
        self.assertEqual(result.status, ToolStatus.SUCCESS)
        self.assertEqual(result.data, "do_thing ok")
        self.assertEqual(session.calls, [("do_thing", {"q": "hello"})])

    def test_registry_index_resolves_after_connect_and_rebuild(self):
        bridge, _ = _bridge()
        empty = tempfile.mkdtemp(prefix="dacli_p11_mcp_nc_")
        registry = ConnectorRegistry(
            None, connectors_dir=empty, config_path="__nope__.yaml",
            extra_connectors=[bridge],
        )
        # Pre-connect: only the static introspection op resolves.
        self.assertIsNotNone(registry.resolve("mcp_list_tools"))
        self.assertIsNone(registry.resolve("mcp_do_thing"))
        self.assertTrue(_run(bridge.connect()))
        registry.rebuild_index()
        self.assertIsNotNone(registry.resolve("mcp_do_thing"))


def _governed_dispatcher(bridge, *, approval):
    empty = tempfile.mkdtemp(prefix="dacli_p11_mcp_gov_")
    registry = ConnectorRegistry(
        None, connectors_dir=empty, config_path="__nope__.yaml",
        extra_connectors=[bridge],
    )
    registry.rebuild_index()
    perms = PermissionRegistry(default_scope=Scope.READ_ONLY)
    perms.grant(bridge.name, Scope.RISKY)
    governor = Governor(
        classifier=ActionClassifier(),
        policy=PolicyEngine(),
        permissions=perms,
        strategist=RollbackStrategist(),
        shadow_executor=ShadowExecutor(),
        ledger=AuditLedger(path=_tmp("audit.jsonl")),
        session_id="mcp-sess",
        approval_fn=approval,
    )
    return Dispatcher(registry, memory=None, governor=governor), governor


class GovernedRoutingTest(unittest.TestCase):
    """The bridge is not a governance bypass."""

    def test_risky_proxied_tool_is_denied_under_deny_mode(self):
        bridge, session = _bridge()
        self.assertTrue(_run(bridge.connect()))
        disp, _ = _governed_dispatcher(bridge, approval=lambda r: False)
        result = _run(disp.execute("mcp_do_thing", {"q": "hello"}))
        self.assertEqual(result.status, ToolStatus.DENIED)
        # The remote tool was NEVER called — denial happened before the proxy.
        self.assertEqual(session.calls, [])

    def test_risky_proxied_tool_runs_with_approval_and_is_audited(self):
        bridge, session = _bridge()
        self.assertTrue(_run(bridge.connect()))
        disp, governor = _governed_dispatcher(bridge, approval=lambda r: True)
        result = _run(disp.execute("mcp_do_thing", {"q": "hello"}))
        self.assertEqual(result.status, ToolStatus.SUCCESS)
        self.assertEqual(session.calls, [("do_thing", {"q": "hello"})])
        decisions = governor.ledger.decisions(session_id="mcp-sess")
        self.assertTrue(decisions)
        self.assertEqual(decisions[-1]["tool_name"], "mcp_do_thing")

    def test_safe_introspection_op_runs_without_approval(self):
        bridge, _ = _bridge()
        self.assertTrue(_run(bridge.connect()))
        disp, _ = _governed_dispatcher(bridge, approval=lambda r: False)
        result = _run(disp.execute("mcp_list_tools", {}))
        self.assertEqual(result.status, ToolStatus.SUCCESS)


if __name__ == "__main__":
    unittest.main()
