"""M06 — generalize the generator to any extension type.

The generator now emits one ``register(api)`` module per reporting/03, writes it
under ``paths.resource_dir("extensions")`` (never the package tree), clarifies
ambiguous forks through a ``ctx.ui``-style dialog, refuses a module that inlines a
secret literal, and ends in a hot reload — no restart, no ``/import``, no ``/push``.
"""

import asyncio
from textwrap import dedent

import pytest

from dacli.connectors.base import ToolStatus
from dacli.connectors.dispatcher import Dispatcher
from dacli.core.extensions import ExtensionDispatchRegistry, ExtensionHost
from dacli.core.generate import (
    Clarification,
    ExtensionGenerator,
    GenerationError,
    SecretInlineError,
    extract_secret_fields,
    find_inlined_secret,
    generate_extension,
    parse_generation_response,
)
from dacli.core.verify import Verifier
from dacli.governance.audit import AuditLedger
from dacli.governance.governor import Governor
from dacli.governance.permissions import PermissionRegistry, Scope


def _run(coro):
    return asyncio.run(coro)


class ScriptedLLM:
    """Returns the queued ``text`` for each ``generate`` call, ``tool_calls`` empty."""

    def __init__(self, texts):
        self._texts = list(texts)
        self._i = 0

    async def generate(self, messages=None, system_prompt=None, **_):
        text = self._texts[self._i]
        self._i += 1
        return text, []


class ScriptedUI:
    """A ctx.ui double: pops a queued answer per call, records what was asked."""

    def __init__(self, *, selects=(), confirms=(), inputs=()):
        self._selects = list(selects)
        self._confirms = list(confirms)
        self._inputs = list(inputs)
        self.asked = []

    def select(self, question, options):
        self.asked.append(("select", question, tuple(options)))
        return self._selects.pop(0)

    def confirm(self, question):
        self.asked.append(("confirm", question))
        return self._confirms.pop(0)

    def input(self, prompt, *, secret=False):
        self.asked.append(("input", prompt, secret))
        return self._inputs.pop(0)


TRIVIAL_MODULE = '''\
def register(api):
    @api.tool(
        name="echo_prefix",
        description="Echo a prefix back",
        parameters={"prefix": {"type": "string"}},
        risk="safe",
        postconditions=["result_succeeded"],
    )
    async def echo_prefix(args, ctx):
        return ctx.ok({"prefix": args.get("prefix", "")})
'''


def _module_block(source):
    return f"### FILE: __init__.py\n```python\n{source}```\n"


# --- parsing -------------------------------------------------------------

def test_parse_returns_module_source():
    clars, source = parse_generation_response(_module_block(TRIVIAL_MODULE))
    assert clars == []
    assert "def register(api)" in source
    assert "```" not in source


def test_parse_returns_clarification():
    text = (
        '### CLARIFY\n'
        '{"kind": "select", "question": "AWS CLI or credentials?", '
        '"options": ["aws-cli", "credentials"]}\n'
    )
    clars, source = parse_generation_response(text)
    assert source is None
    assert clars == [
        Clarification(kind="select", question="AWS CLI or credentials?",
                      options=["aws-cli", "credentials"])
    ]


# --- secret-inline detection ---------------------------------------------

SECRET_DECL = '''\
def register(api):
    api.config_field("bucket", required=True)
    api.config_field("access_key", secret=True)
'''


def test_extract_secret_fields_reads_declarations():
    assert extract_secret_fields(SECRET_DECL) == {"access_key"}


def test_find_inlined_secret_flags_literal_assignment():
    source = SECRET_DECL + '    access_key = "AKIAREALKEY123"\n'
    assert find_inlined_secret(source, {"access_key"}) == "access_key"


def test_find_inlined_secret_flags_config_field_default():
    source = 'def register(api):\n    api.config_field("token", secret=True, default="sk-live-xyz")\n'
    assert find_inlined_secret(source, extract_secret_fields(source)) == "token"


def test_find_inlined_secret_clean_module_passes():
    source = SECRET_DECL + '    cfg = api.config()\n    key = cfg["access_key"]\n'
    assert find_inlined_secret(source, {"access_key"}) is None


# --- prompt contract ------------------------------------------------------

def test_generation_prompt_targets_register_api():
    from dacli.core.generate import _GENERATION_PROMPT

    assert "register(api)" in _GENERATION_PROMPT
    assert "postconditions" in _GENERATION_PROMPT
    # Not the old connector base-class contract.
    assert "subclass" not in _GENERATION_PROMPT.lower()


# --- clarification loop ---------------------------------------------------

def test_generate_clarifies_then_emits_module():
    llm = ScriptedLLM([
        '### CLARIFY\n{"kind": "select", "question": "transport?", '
        '"options": ["aws-cli", "credentials"]}\n',
        _module_block(TRIVIAL_MODULE),
    ])
    ui = ScriptedUI(selects=["aws-cli"])

    source = _run(ExtensionGenerator(llm).generate("echo", "echo a prefix", ui))

    assert "def register(api)" in source
    assert ui.asked == [("select", "transport?", ("aws-cli", "credentials"))]


def test_generate_raises_when_no_module_after_rounds():
    llm = ScriptedLLM(['### CLARIFY\n{"kind": "confirm", "question": "ok?"}\n'] * 8)
    ui = ScriptedUI(confirms=[True] * 8)
    with pytest.raises(GenerationError):
        _run(ExtensionGenerator(llm).generate("x", "y", ui))


# --- end to end -----------------------------------------------------------

def _governor(extension_id, ledger):
    perms = PermissionRegistry(default_scope=Scope.READ_ONLY)
    perms.grant(extension_id, Scope.ADMIN)
    return Governor(permissions=perms, ledger=ledger, session_id="m06",
                    approval_fn=None, use_shadow=False)


def test_add_trivial_extension_end_to_end(tmp_path):
    llm = ScriptedLLM([
        '### CLARIFY\n{"kind": "select", "question": "transport?", '
        '"options": ["aws-cli", "credentials"]}\n',
        _module_block(TRIVIAL_MODULE),
    ])
    ui = ScriptedUI(selects=["aws-cli"])
    host = ExtensionHost(tmp_path)

    result = _run(generate_extension("echo", "echo a prefix", llm=llm, ui=ui, host=host))

    # Landed under the extensions dir, not src/dacli.
    assert result.path == tmp_path / "echo" / "__init__.py"
    assert result.path.exists()
    assert result.validated and result.reloaded
    assert ui.asked  # at least one clarification dialog exercised

    # Live and governed: dispatch the generated tool through the real spine.
    ledger = AuditLedger(path=str(tmp_path / "audit.jsonl"))
    disp = Dispatcher(
        ExtensionDispatchRegistry(host.registry), memory=None,
        verifier=Verifier(), governor=_governor("echo", ledger),
    )
    res = _run(disp.execute("echo_prefix", {"prefix": "p"}))
    assert res.status is ToolStatus.SUCCESS
    assert res.data == {"prefix": "p"}
    assert res.metadata["verification"]["passed"] is True


def test_inlined_secret_is_rejected(tmp_path):
    bad = dedent('''\
        def register(api):
            api.config_field("api_key", secret=True)

            @api.tool(name="call", description="d", risk="safe",
                      postconditions=["result_succeeded"])
            async def call(args, ctx):
                api_key = "sk-live-HARDCODED"
                return ctx.ok(api_key)
    ''')
    llm = ScriptedLLM([_module_block(bad)])
    ui = ScriptedUI()
    host = ExtensionHost(tmp_path)

    with pytest.raises(SecretInlineError) as exc:
        _run(generate_extension("leaky", "d", llm=llm, ui=ui, host=host))

    assert "api_key" in str(exc.value)
    # Nothing adopted, nothing written into the live tree.
    assert not (tmp_path / "leaky" / "__init__.py").exists()
    assert "leaky" not in host.registry.extension_ids()


def test_existing_extension_is_not_overwritten(tmp_path):
    (tmp_path / "echo").mkdir(parents=True)
    (tmp_path / "echo" / "__init__.py").write_text(TRIVIAL_MODULE, encoding="utf-8")
    llm = ScriptedLLM([_module_block(TRIVIAL_MODULE)])
    host = ExtensionHost(tmp_path)

    with pytest.raises(FileExistsError):
        _run(generate_extension("echo", "d", llm=llm, ui=ScriptedUI(), host=host))
