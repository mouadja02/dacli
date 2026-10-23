"""Generated extension source must be approved before it reaches disk."""

import asyncio
import hashlib
from textwrap import dedent

import pytest

from dacli.connectors.registry import ConnectorRegistry
from dacli.connectors.system.connector import SystemConnector, _AutoUI
from dacli.connectors.base import Risk, ToolStatus
from dacli.connectors.dispatcher import Dispatcher
from dacli.core import extensions
from dacli.core.extensions import ExtensionHost
from dacli.core.verify import Verifier
from dacli.governance.audit import AuditLedger
from dacli.governance.governor import Governor
from dacli.governance.permissions import PermissionRegistry, Scope
from dacli.governance.policy_engine import PolicyConfig, PolicyEngine


def _run(coro):
    return asyncio.run(coro)


class ScriptedLLM:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = 0

    async def generate(self, **_):
        self.calls += 1
        return self.responses.pop(0), []


def _module_block(source):
    return f"### FILE: __init__.py\n```python\n{source}```\n"


MODULE_V1 = '''\
def register(api):
    @api.tool(name="sample_list", description="List sample", risk="safe",
              postconditions=["result_succeeded"])
    async def sample_list(args, ctx):
        return ctx.ok("v1")
'''

MODULE_V2 = '''\
def register(api):
    @api.tool(name="sample_list", description="List sample items", risk="safe",
              postconditions=["result_succeeded"])
    async def sample_list(args, ctx):
        return ctx.ok("v2")
'''


def _system_dispatcher(tmp_path, llm, host, *, approval=None, policy=None):
    system = SystemConnector()
    system.bind_llm(llm)
    system.bind_extension_host(host)
    registry = ConnectorRegistry(
        settings=None,
        connectors_dir=str(tmp_path / "connectors"),
        config_path=str(tmp_path / "connectors.yaml"),
        extra_connectors=[system],
    )
    permissions = PermissionRegistry(default_scope=Scope.READ_ONLY)
    permissions.grant("system", Scope.ADMIN)
    governor = Governor(
        permissions=permissions,
        ledger=AuditLedger(path=str(tmp_path / "audit.jsonl")),
        session_id="extension-approval",
        approval_fn=approval,
        policy=policy,
        use_shadow=False,
    )
    return system, Dispatcher(
        registry, memory=None, verifier=Verifier(), governor=governor
    )


def _disable_context_docs(monkeypatch):
    async def no_docs(*_args, **_kwargs):
        return None

    monkeypatch.setattr("dacli.core.generate._fetch_context7_docs_silent", no_docs)


def test_generation_is_denied_without_writing_or_reloading(tmp_path, monkeypatch):
    _disable_context_docs(monkeypatch)
    host = ExtensionHost(tmp_path / "extensions")
    llm = ScriptedLLM(_module_block(MODULE_V1))
    system, dispatcher = _system_dispatcher(tmp_path, llm, host)
    validation_calls = []

    def validate(*_args):
        validation_calls.append(True)
        return True, "valid"

    monkeypatch.setattr(
        "dacli.core.extensions._validate_extension_in_subprocess", validate
    )

    result = _run(dispatcher.execute(
        "generate_connector", {"name": "sample", "description": "sample service"}
    ))

    assert result.status is ToolStatus.DENIED
    assert llm.calls == 1
    assert not (tmp_path / "extensions" / "sample" / "__init__.py").exists()
    assert validation_calls == []
    assert system._pending_previews == {}
    assert next(op for op in system.operations() if op.name == "generate_connector").risk is Risk.RISKY


def test_generation_approval_previews_and_applies_the_same_hashed_source(
    tmp_path, monkeypatch
):
    _disable_context_docs(monkeypatch)
    host = ExtensionHost(tmp_path / "extensions")
    llm = ScriptedLLM(_module_block(MODULE_V1))
    requests = []
    system, dispatcher = _system_dispatcher(
        tmp_path,
        llm,
        host,
        approval=lambda request: requests.append(request) or True,
        policy=PolicyEngine(PolicyConfig(defaults={"risky": "auto"})),
    )
    validation_calls = []

    def validate(*_args):
        validation_calls.append(True)
        return True, "valid"

    monkeypatch.setattr(
        "dacli.core.extensions._validate_extension_in_subprocess", validate
    )

    result = _run(dispatcher.execute(
        "generate_connector", {"name": "sample", "description": "sample service"}
    ))

    expected_hash = hashlib.sha256(MODULE_V1.strip().encode("utf-8")).hexdigest()
    source_path = tmp_path / "extensions" / "sample" / "__init__.py"
    assert result.status is ToolStatus.SUCCESS
    assert len(requests) == 1
    assert "Generated source" in requests[0].dry_run_preview
    assert MODULE_V1.strip() in requests[0].dry_run_preview
    assert requests[0].preview_metadata["source_sha256"] == expected_hash
    assert result.data["source_sha256"] == expected_hash
    assert hashlib.sha256(source_path.read_text(encoding="utf-8").encode("utf-8")).hexdigest() == expected_hash
    assert host.source_hash("sample") == expected_hash
    assert validation_calls == [True]
    assert system._pending_previews == {}
    approval_event = dispatcher._governor.ledger.events(
        session_id="extension-approval", kind="approval"
    )[0]
    assert approval_event["detail"]["preview_metadata"]["source_sha256"] == expected_hash


def test_edit_is_denied_without_changing_source_or_reloading(tmp_path, monkeypatch):
    extension_dir = tmp_path / "extensions" / "sample"
    extension_dir.mkdir(parents=True)
    source_path = extension_dir / "__init__.py"
    source_path.write_text(dedent(MODULE_V1), encoding="utf-8")
    original = source_path.read_bytes()
    host = ExtensionHost(tmp_path / "extensions")
    host.load()
    llm = ScriptedLLM(_module_block(MODULE_V2))
    system, dispatcher = _system_dispatcher(tmp_path, llm, host)
    validation_calls = []

    def validate(*_args):
        validation_calls.append(True)
        return True, "valid"

    monkeypatch.setattr(
        "dacli.core.extensions._validate_extension_in_subprocess", validate
    )

    result = _run(dispatcher.execute(
        "edit_extension", {"name": "sample", "instruction": "Improve description"}
    ))

    assert result.status is ToolStatus.DENIED
    assert source_path.read_bytes() == original
    assert validation_calls == []
    assert system._pending_previews == {}
    assert next(op for op in system.operations() if op.name == "edit_extension").risk is Risk.RISKY


def test_edit_approval_previews_diff_and_applies_matching_hash(tmp_path, monkeypatch):
    extension_dir = tmp_path / "extensions" / "sample"
    extension_dir.mkdir(parents=True)
    source_path = extension_dir / "__init__.py"
    source_path.write_text(dedent(MODULE_V1), encoding="utf-8")
    host = ExtensionHost(tmp_path / "extensions")
    host.load()
    llm = ScriptedLLM(_module_block(MODULE_V2))
    requests = []
    system, dispatcher = _system_dispatcher(
        tmp_path, llm, host, approval=lambda request: requests.append(request) or True
    )
    validation_calls = []

    def validate(*_args):
        validation_calls.append(True)
        return True, "valid"

    monkeypatch.setattr(
        "dacli.core.extensions._validate_extension_in_subprocess", validate
    )

    result = _run(dispatcher.execute(
        "edit_extension", {"name": "sample", "instruction": "Improve description"}
    ))

    expected_hash = hashlib.sha256(dedent(MODULE_V2).strip().encode("utf-8")).hexdigest()
    assert result.status is ToolStatus.SUCCESS
    assert len(requests) == 1
    assert "Unified diff" in requests[0].dry_run_preview
    assert '-    @api.tool(name="sample_list", description="List sample", risk="safe",' in requests[0].dry_run_preview
    assert '-    @api.tool(name="sample_list", description="List sample items", risk="safe",' not in requests[0].dry_run_preview
    assert '+    @api.tool(name="sample_list", description="List sample items", risk="safe",' in requests[0].dry_run_preview
    assert requests[0].preview_metadata["source_sha256"] == expected_hash
    assert result.data["source_sha256"] == expected_hash
    assert hashlib.sha256(source_path.read_text(encoding="utf-8").encode("utf-8")).hexdigest() == expected_hash
    assert host.source_hash("sample") == expected_hash
    assert validation_calls == [True]
    assert system._pending_previews == {}


def test_autonomous_clarification_requires_explicit_input():
    assert _AutoUI(None).confirm("Install dependency?") is False
    answers = iter(["yes", "http"])
    ui = _AutoUI(lambda _question: next(answers))
    assert ui.confirm("Install dependency?") is True
    assert ui.select("Transport?", ["http", "cli"]) == "http"


@pytest.mark.parametrize("tool_name", ["generate_connector", "edit_extension"])
@pytest.mark.parametrize("when", ["before_validation", "after_validation"])
def test_replaced_source_never_executes(tmp_path, monkeypatch, tool_name, when):
    _disable_context_docs(monkeypatch)
    host = ExtensionHost(tmp_path / "extensions")
    source_path = host.base_dir() / "sample" / "__init__.py"
    if tool_name == "edit_extension":
        source_path.parent.mkdir(parents=True)
        source_path.write_text(MODULE_V1, encoding="utf-8")
        host.load()
    original_hash = host.source_hash("sample")
    marker = tmp_path / "unreviewed.txt"
    replacement = (
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).write_text('ran')\n"
        + MODULE_V1
    )
    system, dispatcher = _system_dispatcher(
        tmp_path, ScriptedLLM(_module_block(MODULE_V2)), host,
        approval=lambda request: True,
    )
    validation_calls = []
    validate = extensions._validate_extension_in_subprocess
    reload = host.reload

    def validate_then_replace(*args):
        result = validate(*args)
        validation_calls.append(result[0])
        source_path.write_text(replacement, encoding="utf-8")
        return result

    def replace_then_reload(*args, **kwargs):
        source_path.write_text(replacement, encoding="utf-8")
        return reload(*args, **kwargs)

    if when == "before_validation":
        monkeypatch.setattr(host, "reload", replace_then_reload)
    else:
        monkeypatch.setattr(extensions, "_validate_extension_in_subprocess", validate_then_replace)

    args = {"name": "sample", "description": "sample service", "instruction": "Improve description"}
    result = _run(dispatcher.execute(tool_name, args))
    message = result.error or result.data.get("validation_message") or result.data.get("message", "")

    assert (
        marker.exists(), host.source_hash("sample"),
        "changed" in message, system._pending_previews, validation_calls,
    ) == (False, original_hash, True, {}, [True] if when == "after_validation" else [])


@pytest.mark.parametrize("tool_name,name,response,expected", [
    ("generate_connector", "sample", "", "already exists"),
    ("edit_extension", "missing", "", "not found"),
    ("generate_connector", " ", "", "'name' and 'description' are required"),
    ("generate_connector", "new", "no module", "neither a module nor a clarification"),
    ("generate_connector", "new", _module_block(
        'def register(api):\n    api.config_field("token", secret=True, default="literal")\n'
    ), "secret field 'token'"),
])
def test_preview_errors_keep_the_cause(tmp_path, monkeypatch, tool_name, name, response, expected):
    _disable_context_docs(monkeypatch)
    host = ExtensionHost(tmp_path / "extensions")
    source_path = host.base_dir() / "sample" / "__init__.py"
    source_path.parent.mkdir(parents=True)
    source_path.write_text(MODULE_V1, encoding="utf-8")
    original = source_path.read_bytes()
    approvals = []
    system, dispatcher = _system_dispatcher(
        tmp_path, ScriptedLLM(response), host,
        approval=lambda request: approvals.append(request) or True,
    )
    result = _run(dispatcher.execute(tool_name, {
        "name": name, "description": "sample service", "instruction": "Improve description",
    }))

    assert (
        result.status, expected in result.error, approvals,
        system._pending_previews, source_path.read_bytes(),
        (host.base_dir() / "new").exists(),
    ) == (ToolStatus.BLOCKED, True, [], {}, original, False)
