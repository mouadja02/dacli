""" (𝒢 Governance & the code-execution sandbox) test suite.

Each test class maps to an exit criterion in the roadmap. Run with:
    python -m unittest tests.test_governance_phase5
"""

import asyncio
import tempfile
import unittest
from typing import Any

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.connectors.registry import ConnectorRegistry
from dacli.connectors.dispatcher import Dispatcher

from dacli.governance.classifier import ActionClassifier, Tier, classify_sql, detect_prod
from dacli.governance.policy_engine import (
    PolicyEngine, PolicyConfig, PolicyDecision,
)
from dacli.governance.permissions import PermissionRegistry, Scope
from dacli.governance.rollback import RollbackStrategist
from dacli.governance.shadow import ShadowExecutor
from dacli.governance.audit import AuditLedger
from dacli.governance.governor import Governor

from dacli.sandbox.policy import SandboxPolicy
from dacli.sandbox.sdk import ConnectorSDK
from dacli.sandbox.runtime import SandboxRuntime
from dacli.sandbox.connector import SandboxConnector


def _run(coro):
    return asyncio.run(coro)


def _tmp(name):
    return tempfile.mkdtemp(prefix="dacli_p5_") + "/" + name


def _empty_dir():
    return tempfile.mkdtemp(prefix="dacli_p5_nc_")


def _spec(name, risk):
    return OperationSpec(name=name, description="x",
                         parameters={"type": "object", "properties": {}},
                         capability=f"{name}.cap", risk=risk)


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------
class _Conn(Connector):
    """A controllable connector that records every invoke."""

    def __init__(self, name="snowflake", *, rollback_ok=None, rows=None,
                 supports_shadow=False):
        super().__init__(settings=None)
        self.name = name
        self._is_connected = True
        self.invoked: list[dict[str, Any]] = []
        self._rollback_ok = rollback_ok          # None=no hook, True/False=verdict
        self._rows = rows
        self.supports_shadow = supports_shadow
        self.clone_calls: list[str] = []

    def operations(self) -> list[OperationSpec]:
        return [
            _spec("execute_snowflake_query", Risk.RISKY),
            _spec("read_thing", Risk.SAFE),
            _spec("push_thing", Risk.WRITE),
        ]

    async def invoke(self, op, args) -> ToolResult:
        self.invoked.append({"op": op, "args": dict(args or {})})
        data = self._rows if self._rows is not None else {"ok": True}
        return ToolResult(tool_name=op, status=ToolStatus.SUCCESS, data=data)

    async def health(self):
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS)

    # rollback verification hook
    async def verify_rollback(self, plan, args):
        if self._rollback_ok is None:
            raise AttributeError("no hook")  # not reached; absence tested separately
        return self._rollback_ok, ("retention ok" if self._rollback_ok else "retention 0")

    # shadow protocol
    async def create_clone(self, args):
        self.clone_calls.append("create")
        return "CLONE_REF"

    async def run_on_clone(self, clone_ref, args):
        self.clone_calls.append("run")
        return {"ok": True}

    async def diff_clone(self, clone_ref, args):
        self.clone_calls.append("diff")
        return {"rows_before": 100, "rows_after": 98, "row_delta": -2, "checksum_changed": True}

    async def promote_clone(self, clone_ref, args):
        self.clone_calls.append("promote")
        return {"ok": True}

    async def drop_clone(self, clone_ref):
        self.clone_calls.append("drop")


class _NoHookConn(_Conn):
    """Connector with NO verify_rollback hook (rollback can't be verified)."""
    def __init__(self, **kw):
        super().__init__(**kw)
    # shadow the attribute away
    verify_rollback = None  # type: ignore


def _governor(*, connector, scope=Scope.ADMIN, policy=None, approval=None,
              env=None, ledger=None, use_shadow=True):
    perms = PermissionRegistry(default_scope=Scope.READ_ONLY)
    perms.grant(connector.name, scope)
    return Governor(
        classifier=ActionClassifier(),
        policy=policy or PolicyEngine(),
        permissions=perms,
        strategist=RollbackStrategist(),
        shadow_executor=ShadowExecutor(),
        ledger=ledger or AuditLedger(path=_tmp("audit.jsonl")),
        session_id="sess-1",
        approval_fn=approval,
        env_resolver=(lambda *_a: env) if env else None,
        use_shadow=use_shadow,
    )


# ===========================================================================
# Unit: classifier
# ===========================================================================
class ClassifierTest(unittest.TestCase):
    def test_select_is_safe_even_through_risky_op(self):
        c = ActionClassifier().classify(
            "execute_snowflake_query", {"query": "SELECT * FROM T"}, declared_risk=Risk.RISKY)
        self.assertEqual(c.tier, Tier.SAFE)

    def test_drop_is_irreversible(self):
        c = ActionClassifier().classify(
            "execute_snowflake_query", {"query": "DROP TABLE T"}, declared_risk=Risk.RISKY)
        self.assertEqual(c.tier, Tier.IRREVERSIBLE)

    def test_update_is_risky(self):
        c = ActionClassifier().classify(
            "execute_snowflake_query", {"query": "UPDATE T SET x=1"}, declared_risk=Risk.RISKY)
        self.assertEqual(c.tier, Tier.RISKY)

    def test_insert_is_write(self):
        c = ActionClassifier().classify(
            "execute_snowflake_query", {"query": "INSERT INTO T VALUES (1)"}, declared_risk=Risk.RISKY)
        self.assertEqual(c.tier, Tier.WRITE)

    def test_hidden_delete_in_cte_is_caught(self):
        # A destructive keyword anywhere promotes the tier (defense in depth).
        v = classify_sql("WITH x AS (SELECT 1) DELETE FROM T WHERE id IN (SELECT id FROM x)")
        self.assertEqual(v.tier, Tier.RISKY)

    def test_unparseable_sql_defaults_to_risky(self):
        v = classify_sql("@@@ not sql @@@")
        self.assertEqual(v.tier, Tier.RISKY)
        self.assertTrue(v.ambiguous)

    def test_string_literal_drop_does_not_trigger(self):
        # 'DROP' inside a string literal must not classify a SELECT as irreversible.
        c = ActionClassifier().classify(
            "execute_snowflake_query",
            {"query": "SELECT 'please DROP this' AS note FROM T"}, declared_risk=Risk.RISKY)
        self.assertEqual(c.tier, Tier.SAFE)

    def test_prod_insert_promoted_to_risky(self):
        c = ActionClassifier().classify(
            "execute_snowflake_query", {"query": "INSERT INTO GOLD.SALES VALUES (1)"},
            declared_risk=Risk.RISKY)
        self.assertTrue(c.is_prod)
        self.assertEqual(c.tier, Tier.RISKY)  # WRITE promoted by prod marker

    def test_detect_prod_in_env_hint(self):
        self.assertIsNotNone(detect_prod({}, env_hint="ANALYTICS_PROD"))
        self.assertIsNone(detect_prod({"x": "dev_table"}))


# ===========================================================================
# Unit: policy engine (config-overridable)
# ===========================================================================
class PolicyEngineTest(unittest.TestCase):
    def test_locked_posture(self):
        pe = PolicyEngine()
        self.assertEqual(pe.decide(Tier.SAFE).decision, PolicyDecision.AUTO)
        self.assertEqual(pe.decide(Tier.WRITE).decision, PolicyDecision.VERIFY)
        self.assertEqual(pe.decide(Tier.RISKY).decision, PolicyDecision.CONFIRM)
        self.assertEqual(pe.decide(Tier.IRREVERSIBLE).decision, PolicyDecision.DRY_RUN_APPROVE)

    def test_irreversible_requires_verified_rollback(self):
        r = PolicyEngine().decide(Tier.IRREVERSIBLE)
        self.assertTrue(r.requires_verified_rollback)
        self.assertTrue(r.requires_dry_run)
        self.assertTrue(r.requires_human)

    def test_env_override_auto_write(self):
        cfg = PolicyConfig(connectors={
            "snowflake": {"environments": {"dev": {"write": "auto"}}}})
        pe = PolicyEngine(cfg)
        dev = pe.decide(Tier.WRITE, connector_id="snowflake", environment="dev")
        prod = pe.decide(Tier.WRITE, connector_id="snowflake", environment="prod")
        self.assertEqual(dev.decision, PolicyDecision.AUTO)
        self.assertEqual(prod.decision, PolicyDecision.VERIFY)  # locked default


# ===========================================================================
# Unit: permissions (least privilege)
# ===========================================================================
class PermissionsTest(unittest.TestCase):
    def test_read_only_blocks_write(self):
        reg = PermissionRegistry(default_scope=Scope.READ_ONLY)
        self.assertTrue(reg.check("snowflake", Tier.SAFE).allowed)
        self.assertFalse(reg.check("snowflake", Tier.WRITE).allowed)

    def test_granted_scope_permits_up_to_ceiling(self):
        reg = PermissionRegistry()
        reg.grant("snowflake", Scope.RISKY)
        self.assertTrue(reg.check("snowflake", Tier.RISKY).allowed)
        self.assertFalse(reg.check("snowflake", Tier.IRREVERSIBLE).allowed)

    def test_from_policy_config(self):
        cfg = PolicyConfig(connectors={"github": {"scope": "write"}})
        reg = PermissionRegistry.from_policy_config(cfg)
        self.assertEqual(reg.scope_for("github"), Scope.WRITE)
        self.assertEqual(reg.scope_for("unknown"), Scope.READ_ONLY)  # least-privilege default


# ===========================================================================
# Exit criterion 1: DROP -> irreversible, dry-run+approval, BLOCKED w/o rollback
# ===========================================================================
class DropTableGatingTest(unittest.TestCase):
    DROP = {"query": "DROP TABLE BRONZE.RAW.CUSTOMERS"}

    def test_blocked_when_no_verified_rollback(self):
        conn = _Conn(rollback_ok=False)  # Time Travel retention is 0
        gov = _governor(connector=conn, approval=lambda r: True)  # would approve...
        d = _run(gov.review("execute_snowflake_query", _spec("execute_snowflake_query", Risk.RISKY),
                            self.DROP, conn))
        self.assertFalse(d.allowed)
        self.assertEqual(d.short_circuit.status, ToolStatus.BLOCKED)
        self.assertEqual(d.classification.tier, Tier.IRREVERSIBLE)

    def test_blocked_when_connector_has_no_rollback_hook(self):
        conn = _NoHookConn()
        gov = _governor(connector=conn, approval=lambda r: True)
        d = _run(gov.review("execute_snowflake_query", _spec("execute_snowflake_query", Risk.RISKY),
                            self.DROP, conn))
        self.assertFalse(d.allowed)
        self.assertEqual(d.short_circuit.status, ToolStatus.BLOCKED)

    def test_allowed_with_verified_rollback_and_approval(self):
        conn = _Conn(rollback_ok=True)  # retention > 0
        approvals = []
        gov = _governor(connector=conn, approval=lambda r: approvals.append(r) or True)
        d = _run(gov.review("execute_snowflake_query", _spec("execute_snowflake_query", Risk.RISKY),
                            self.DROP, conn))
        self.assertTrue(d.allowed)
        self.assertTrue(d.rollback_plan.verified)
        self.assertEqual(len(approvals), 1)               # a human WAS asked
        self.assertEqual(d.policy.decision, PolicyDecision.DRY_RUN_APPROVE)

    def test_blocked_through_dispatcher(self):
        conn = _Conn(rollback_ok=False)
        reg = ConnectorRegistry(settings=None, connectors_dir=_empty_dir(),
                                config_path="__nonexistent__.yaml", extra_connectors=[conn])
        gov = _governor(connector=conn, approval=lambda r: True)
        disp = Dispatcher(reg, memory=None, governor=gov)
        res = _run(disp.execute("execute_snowflake_query", self.DROP))
        self.assertEqual(res.status, ToolStatus.BLOCKED)
        self.assertEqual(conn.invoked, [])  # the DROP NEVER executed


# ===========================================================================
# Exit criterion 2: prod INSERT -> risky+confirm; dev INSERT -> auto (config)
# ===========================================================================
class ProdPromotionTest(unittest.TestCase):
    DEV_CFG = PolicyConfig(connectors={
        "snowflake": {"scope": "admin", "environments": {"dev": {"write": "auto"}}}})

    def test_dev_insert_auto_runs(self):
        conn = _Conn()
        gov = _governor(connector=conn, policy=PolicyEngine(self.DEV_CFG),
                        env="dev", approval=None)  # no approver — must NOT be needed
        d = _run(gov.review("execute_snowflake_query", _spec("execute_snowflake_query", Risk.RISKY),
                            {"query": "INSERT INTO STAGING.T VALUES (1)"}, conn))
        self.assertTrue(d.allowed)
        self.assertEqual(d.policy.decision, PolicyDecision.AUTO)

    def test_prod_insert_asks_confirmation(self):
        conn = _Conn()
        asked = []
        gov = _governor(connector=conn, policy=PolicyEngine(self.DEV_CFG),
                        env="prod", approval=lambda r: asked.append(r) or True)
        d = _run(gov.review("execute_snowflake_query", _spec("execute_snowflake_query", Risk.RISKY),
                            {"query": "INSERT INTO GOLD.SALES VALUES (1)"}, conn))
        self.assertEqual(d.classification.tier, Tier.RISKY)  # promoted from write
        self.assertEqual(d.policy.decision, PolicyDecision.CONFIRM)
        self.assertEqual(len(asked), 1)
        self.assertTrue(d.allowed)

    def test_prod_insert_denied_fails_closed_without_approver(self):
        conn = _Conn()
        gov = _governor(connector=conn, policy=PolicyEngine(self.DEV_CFG),
                        env="prod", approval=None)
        d = _run(gov.review("execute_snowflake_query", _spec("execute_snowflake_query", Risk.RISKY),
                            {"query": "INSERT INTO GOLD.SALES VALUES (1)"}, conn))
        self.assertFalse(d.allowed)  # fail-closed


# ===========================================================================
# Exit criterion 3: risky transform runs on a clone, presents a diff, promotes
# only on approval
# ===========================================================================
class ShadowExecutionTest(unittest.TestCase):
    UPDATE = {"query": "UPDATE BRONZE.T SET status='x' WHERE id<10"}

    def test_runs_on_clone_and_presents_diff(self):
        conn = _Conn(supports_shadow=True)
        seen = {}
        def approve(req):
            seen["diff"] = req.shadow.diff if req.shadow else None
            return True
        gov = _governor(connector=conn, approval=approve)
        d = _run(gov.review("execute_snowflake_query", _spec("execute_snowflake_query", Risk.RISKY),
                            self.UPDATE, conn))
        # The clone was created, the transform ran on it, and a diff was produced.
        self.assertIn("create", conn.clone_calls)
        self.assertIn("run", conn.clone_calls)
        self.assertIn("diff", conn.clone_calls)
        self.assertEqual(seen["diff"]["row_delta"], -2)
        self.assertTrue(d.allowed)

    def test_denial_does_not_promote_and_discards_clone(self):
        conn = _Conn(supports_shadow=True)
        reg = ConnectorRegistry(settings=None, connectors_dir=_empty_dir(),
                                config_path="__nonexistent__.yaml", extra_connectors=[conn])
        gov = _governor(connector=conn, approval=lambda r: False)  # human declines
        disp = Dispatcher(reg, memory=None, governor=gov)
        res = _run(disp.execute("execute_snowflake_query", self.UPDATE))
        self.assertEqual(res.status, ToolStatus.DENIED)
        self.assertEqual(conn.invoked, [])            # real op never promoted
        self.assertIn("drop", conn.clone_calls)       # clone cleaned up

    def test_approval_promotes_real_action(self):
        conn = _Conn(supports_shadow=True)
        reg = ConnectorRegistry(settings=None, connectors_dir=_empty_dir(),
                                config_path="__nonexistent__.yaml", extra_connectors=[conn])
        gov = _governor(connector=conn, approval=lambda r: True)
        disp = Dispatcher(reg, memory=None, governor=gov)
        res = _run(disp.execute("execute_snowflake_query", self.UPDATE))
        self.assertEqual(res.status, ToolStatus.SUCCESS)
        self.assertEqual(len(conn.invoked), 1)        # real op executed after approval


# ===========================================================================
# Exit criterion 4: `dacli audit` reconstructs a full session
# ===========================================================================
class AuditReconstructionTest(unittest.TestCase):
    def test_full_decision_chain_is_reconstructable(self):
        ledger = AuditLedger(path=_tmp("audit.jsonl"))
        conn = _Conn()
        reg = ConnectorRegistry(settings=None, connectors_dir=_empty_dir(),
                                config_path="__nonexistent__.yaml", extra_connectors=[conn])
        gov = _governor(connector=conn, ledger=ledger, approval=lambda r: True)
        disp = Dispatcher(reg, memory=None, governor=gov)

        _run(disp.execute("read_thing", {}))                              # safe -> auto
        _run(disp.execute("execute_snowflake_query", {"query": "INSERT INTO T VALUES (1)"}))  # write

        decisions = ledger.decisions(session_id="sess-1")
        self.assertGreaterEqual(len(decisions), 2)
        kinds = {e["kind"] for d in decisions for e in d["events"]}
        # The 'why' chain is present: classification -> policy -> execution.
        self.assertIn("classification", kinds)
        self.assertIn("policy", kinds)
        self.assertIn("permission", kinds)
        self.assertIn("execution", kinds)
        # Each decision groups its own events under one decision_id.
        for d in decisions:
            self.assertTrue(d["decision_id"])
            self.assertTrue(any(e["kind"] == "classification" for e in d["events"]))


# ===========================================================================
# Unit: the sandbox SDK is governed + off-context + secret-free
# ===========================================================================
class SandboxSdkTest(unittest.TestCase):
    def test_large_result_spills_and_returns_bounded_summary(self):
        # The "1M rows" property: regardless of N, the summary returned to context
        # is bounded to preview_rows; the full set lands on disk.
        N = 100_000
        rows = [{"id": i} for i in range(N)]

        async def fake_execute(tool, args):
            return ToolResult(tool_name=tool, status=ToolStatus.SUCCESS, data=rows)

        sdk = ConnectorSDK(fake_execute, workdir=_empty_dir(), preview_rows=20)
        summary = _run(sdk.run("big_query", {"sql": "SELECT * FROM huge"}))
        self.assertEqual(summary["row_count"], N)
        self.assertEqual(len(summary["preview"]), 20)        # bounded
        self.assertIn("saved_path", summary)                 # full set on disk
        # The returned summary is small + carries NO credentials.
        import json
        blob = json.dumps(summary)
        self.assertNotIn("password", blob.lower())
        self.assertNotIn("token", blob.lower())
        self.assertLess(len(blob), 4000)

    def test_sdk_drop_is_governed_not_executed(self):
        # Exit criterion 6 (SDK layer): a DROP via the SDK still hits the policy
        # engine and is blocked — the connector's invoke is never reached.
        conn = _Conn(rollback_ok=False)
        reg = ConnectorRegistry(settings=None, connectors_dir=_empty_dir(),
                                config_path="__nonexistent__.yaml", extra_connectors=[conn])
        gov = _governor(connector=conn, approval=lambda r: True)
        disp = Dispatcher(reg, memory=None, governor=gov)
        sdk = ConnectorSDK(disp.execute, registry=reg, workdir=_empty_dir())
        summary = _run(sdk.run("execute_snowflake_query", {"query": "DROP TABLE T"}))
        self.assertEqual(summary["status"], "blocked")
        self.assertEqual(conn.invoked, [])


# ===========================================================================
# Exit criteria 5 & 6 (end to end through the real subprocess runtime)
# ===========================================================================
class SandboxRuntimeTest(unittest.TestCase):
    def _runtime(self, dispatcher, registry):
        policy = SandboxPolicy(workdir=_empty_dir(), wall_clock_seconds=60,
                               max_output_chars=20000, network="off")
        return SandboxRuntime(policy, dispatcher.execute, registry=registry)

    def test_query_write_disk_return_summary(self):
        # Exit criterion 5, full path: the script fetches rows via the governed
        # SDK bridge, writes them to disk, and returns only a 20-row summary.
        rows = [{"id": i} for i in range(5000)]
        conn = _Conn(name="snowflake", rows=rows)
        reg = ConnectorRegistry(settings=None, connectors_dir=_empty_dir(),
                                config_path="__nonexistent__.yaml", extra_connectors=[conn])
        gov = _governor(connector=conn, approval=lambda r: True)
        disp = Dispatcher(reg, memory=None, governor=gov)
        runtime = self._runtime(disp, reg)

        script = (
            "res = sdk.run('read_thing', table='huge')\n"
            "rows = sdk.read_rows(res['handle']) if res.get('saved_path') else res.get('preview', [])\n"
            "sdk.save_rows('summary.jsonl', rows[:20])\n"
            "sdk.finish({'total': res['row_count'], 'sample': len(rows[:20])})\n"
        )
        result = _run(runtime.run_script(script))
        self.assertTrue(result.ok, result.error or result.stderr)
        self.assertEqual(result.returned["total"], 5000)
        self.assertEqual(result.returned["sample"], 20)
        self.assertGreaterEqual(result.calls, 1)
        self.assertIn("summary.jsonl", result.artifacts)
        # Nothing secret leaked into the model-visible transcript.
        self.assertNotIn("password", (result.output + str(result.returned)).lower())

    def test_sandbox_drop_still_triggers_policy(self):
        # Exit criterion 6, full path: a DROP issued from inside the sandbox is
        # blocked by the policy engine; the connector never executes it.
        conn = _Conn(name="snowflake", rollback_ok=False)
        reg = ConnectorRegistry(settings=None, connectors_dir=_empty_dir(),
                                config_path="__nonexistent__.yaml", extra_connectors=[conn])
        gov = _governor(connector=conn, approval=lambda r: True)
        disp = Dispatcher(reg, memory=None, governor=gov)
        runtime = self._runtime(disp, reg)

        script = (
            "res = sdk.run('execute_snowflake_query', query='DROP TABLE T')\n"
            "sdk.finish({'status': res['status']})\n"
        )
        result = _run(runtime.run_script(script))
        self.assertTrue(result.ok, result.error or result.stderr)
        self.assertEqual(result.returned["status"], "blocked")
        self.assertEqual(conn.invoked, [])  # the DROP never ran


# ===========================================================================
# The built-in sandbox connector enforces a structured (post-conditioned) result
# ===========================================================================
class SandboxConnectorTest(unittest.TestCase):
    def test_run_sandbox_code_returns_structured_result(self):
        conn = _Conn(name="snowflake")
        reg = ConnectorRegistry(settings=None, connectors_dir=_empty_dir(),
                                config_path="__nonexistent__.yaml", extra_connectors=[conn])
        gov = _governor(connector=conn, approval=lambda r: True)
        disp = Dispatcher(reg, memory=None, governor=gov)
        runtime = SandboxRuntime(SandboxPolicy(workdir=_empty_dir(), wall_clock_seconds=60,
                                               network="off"), disp.execute, registry=reg)
        sandbox_conn = SandboxConnector()
        sandbox_conn.bind_runtime(runtime)
        res = _run(sandbox_conn.invoke("run_sandbox_code", {"code": "sdk.finish({'hi': 1})"}))
        self.assertEqual(res.status, ToolStatus.SUCCESS)
        self.assertIn("ok", res.data)
        self.assertEqual(res.data["returned"], {"hi": 1})


if __name__ == "__main__":
    unittest.main()
