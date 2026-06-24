"""M04 — governed dispatch for extension tools.

An extension tool registered through M03's ``register(api)`` must run the *same*
spine a connector op does: ``Governor.review()`` → execute → ``Verifier`` → audit
ledger. These tests dispatch the M03 sample tool through the real Dispatcher and
check the ledger shape matches a connector op's, and that a risky tool with no
approver is denied fail-closed.
"""

import asyncio
from textwrap import dedent

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.connectors.dispatcher import Dispatcher
from dacli.connectors.registry import ConnectorRegistry
from dacli.core.extensions import ExtensionDispatchRegistry, load_extensions
from dacli.core.verify import Verifier, result_succeeded
from dacli.governance.audit import AuditLedger
from dacli.governance.governor import Governor
from dacli.governance.permissions import PermissionRegistry, Scope


def _run(coro):
    return asyncio.run(coro)


def _write_ext(root, name, body):
    pkg = root / name
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text(dedent(body), encoding="utf-8")


SAMPLE = """
    def register(api):
        @api.tool(
            name="sample_list",
            description="List objects under a prefix",
            parameters={"prefix": {"type": "string"}},
            risk="safe",
            postconditions=["result_succeeded"],
        )
        async def sample_list(args, ctx):
            return ctx.ok([{"key": args.get("prefix", "") + "/a"}])
"""

DESTRUCTIVE = """
    def register(api):
        @api.tool(
            name="wipe",
            description="Delete everything",
            risk="risky",
            postconditions=["result_succeeded"],
        )
        async def wipe(args, ctx):
            wipe.ran = True
            return ctx.ok("gone")
        wipe.ran = False
"""


def _governor(extension_id, *, ledger, approval=None):
    perms = PermissionRegistry(default_scope=Scope.READ_ONLY)
    perms.grant(extension_id, Scope.ADMIN)
    return Governor(
        permissions=perms,
        ledger=ledger,
        session_id="m04",
        approval_fn=approval,
        use_shadow=False,
    )


def _kinds(ledger):
    return {e["kind"] for d in ledger.decisions(session_id="m04") for e in d["events"]}


def test_extension_tool_runs_the_full_governed_spine(tmp_path):
    _write_ext(tmp_path, "sample", SAMPLE)
    reg = ExtensionDispatchRegistry(load_extensions(tmp_path))
    ledger = AuditLedger(path=str(tmp_path / "audit.jsonl"))
    disp = Dispatcher(
        reg, memory=None, verifier=Verifier(),
        governor=_governor("sample", ledger=ledger),
    )

    res = _run(disp.execute("sample_list", {"prefix": "p"}))

    assert res.status is ToolStatus.SUCCESS
    assert res.data == [{"key": "p/a"}]
    # The post-condition ran and passed (verified success is the only success).
    assert res.metadata["verification"]["passed"] is True
    # Same audit chain a connector op produces.
    assert {"classification", "permission", "policy", "execution", "post_condition"} <= _kinds(ledger)


def test_ledger_shape_matches_a_connector_op(tmp_path):
    _write_ext(tmp_path, "sample", SAMPLE)

    ext_ledger = AuditLedger(path=str(tmp_path / "ext.jsonl"))
    ext_disp = Dispatcher(
        ExtensionDispatchRegistry(load_extensions(tmp_path)),
        memory=None, verifier=Verifier(),
        governor=_governor("sample", ledger=ext_ledger),
    )
    _run(ext_disp.execute("sample_list", {"prefix": "p"}))

    conn = _SafeConnector()
    conn_reg = ConnectorRegistry(
        settings=None, connectors_dir=str(tmp_path / "none"),
        config_path="__nonexistent__.yaml", extra_connectors=[conn],
    )
    conn_ledger = AuditLedger(path=str(tmp_path / "conn.jsonl"))
    conn_disp = Dispatcher(
        conn_reg, memory=None, verifier=Verifier(),
        governor=_governor("safe_conn", ledger=conn_ledger),
    )
    _run(conn_disp.execute("read_thing", {}))

    assert _kinds(ext_ledger) == _kinds(conn_ledger)


def test_risky_tool_denied_fail_closed_without_approver(tmp_path):
    _write_ext(tmp_path, "danger", DESTRUCTIVE)
    registry = load_extensions(tmp_path)
    ledger = AuditLedger(path=str(tmp_path / "audit.jsonl"))
    disp = Dispatcher(
        ExtensionDispatchRegistry(registry), memory=None, verifier=Verifier(),
        governor=_governor("danger", ledger=ledger),  # no approval_fn
    )

    res = _run(disp.execute("wipe", {}))

    assert res.status is ToolStatus.DENIED
    assert "approv" in (res.error or "").lower()
    # The handler never ran — denial short-circuits before execute.
    assert "execution" not in _kinds(ledger)


def test_risky_tool_runs_when_approved(tmp_path):
    _write_ext(tmp_path, "danger", DESTRUCTIVE)
    ledger = AuditLedger(path=str(tmp_path / "audit.jsonl"))
    disp = Dispatcher(
        ExtensionDispatchRegistry(load_extensions(tmp_path)),
        memory=None, verifier=Verifier(),
        governor=_governor("danger", ledger=ledger, approval=lambda req: True),
    )

    res = _run(disp.execute("wipe", {}))

    assert res.status is ToolStatus.SUCCESS
    assert res.data == "gone"
    assert "execution" in _kinds(ledger)


class _SafeConnector(Connector):
    """Minimal connector exposing one safe op, to compare ledger shape."""

    name = "safe_conn"

    def __init__(self):
        super().__init__(settings=None)

    def operations(self):
        return [OperationSpec(
            name="read_thing", description="read",
            parameters={"type": "object", "properties": {}},
            capability="safe_conn.read", risk=Risk.SAFE,
            postconditions=[result_succeeded()],
        )]

    async def invoke(self, op, args):
        return ToolResult(tool_name=op, status=ToolStatus.SUCCESS, data=[{"key": "a"}])

    async def health(self):
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS)
