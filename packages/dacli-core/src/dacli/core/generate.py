"""LLM-driven extension generator (M06).

Generalizes ``core/connector_generator.py``: the agent describes any extension —
tool, command, provider, connector — and the LLM writes a single ``register(api)``
module (reporting/03), not a ``Connector`` subclass. The module is written under
``paths.resource_dir("extensions")`` (project overlay or global ``~/.dacli/``),
never into the package tree.

The flow ends in a hot reload, not a restart: generate (clarifying ambiguous forks
through a ``ctx.ui``-style dialog) -> reject any inlined secret -> write ->
``ExtensionHost.reload`` (which validates the module in a child process before
adopting it, M05). No ``/import``, no ``/push``.
"""

from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol


class GenerationError(RuntimeError):
    """The model never produced a usable module (e.g. only clarifications)."""


class SecretInlineError(ValueError):
    """A generated module inlined a literal for a ``secret=True`` field."""


# ---------------------------------------------------------------------------
# Clarification dialog (reporting/03: ask at an ambiguous fork, don't guess)
# ---------------------------------------------------------------------------
class ClarifyUI(Protocol):
    """The ``ctx.ui`` surface the generator drives when the model asks."""

    def select(self, question: str, options: list[str]) -> str: ...
    def confirm(self, question: str) -> bool: ...
    def input(self, prompt: str, *, secret: bool = False) -> str: ...


@dataclass
class Clarification:
    kind: str  # "select" | "confirm" | "input"
    question: str
    options: list[str] = field(default_factory=list)
    secret: bool = False


# ---------------------------------------------------------------------------
# Generation prompt — targets the register(api) contract, not the old base classes
# ---------------------------------------------------------------------------
_SYSTEM_PROMPT = (
    "You generate dacli extension modules. Output only clarifications or the final "
    "module, in the exact format requested."
)

# NOTE: filled via str.replace, not str.format — the example code carries literal
# ``{ ... }`` (dict literals, schema maps) that str.format would read as fields.
_GENERATION_PROMPT = """\
Write one Python module for the dacli extension host. The module exports
`register(api)` and runs nothing else at import time.

## The register(api) contract

```python
def register(api):
    # Config the user supplies later via /connect. Secrets are entered hidden and
    # stored encrypted; NEVER write a secret value as a literal in the code.
    api.config_field("bucket", required=True, description="Target bucket")
    api.config_field("access_key", secret=True)

    @api.tool(
        name="thing_list",                          # LLM-facing tool name
        description="List things under a prefix",
        parameters={"prefix": {"type": "string"}},  # JSON-schema property map
        risk="safe",                                # safe | write | risky | irreversible
        postconditions=["result_succeeded"],        # at least one, always
    )
    async def thing_list(args, ctx):
        cfg = api.config()              # {"bucket": ..., decrypted secrets ...}
        ...
        return ctx.ok(rows)            # success; or ctx.fail("reason")
```

Rules:
- Every `@api.tool` declares `risk` and at least one post-condition. A tool with no
  post-condition is refused at load.
- Read every credential through `api.config()` at call time. NEVER inline a secret
  value — a module that hardcodes a `secret=True` field is rejected.
- Post-condition names you can use: result_succeeded, data_is_list, shell_exit_zero,
  shell_writes_observed, shell_deletes_observed. Use "result_succeeded" if unsure.
- Each handler is `async def handler(args, ctx)` returning `ctx.ok(...)` / `ctx.fail(...)`.
- `api.command(name, handler)`, `api.shortcut(key, handler)`, `api.provider(name, cfg)`
  register the other extension kinds.

## Ask when a decision is ambiguous

If a design fork needs the user's input (e.g. "drive S3 through the AWS CLI runner
or direct access-key credentials?"), emit a clarification instead of the module:

### CLARIFY
{"kind": "select", "question": "...", "options": ["...", "..."]}

`kind` is "select" (with options), "confirm" (yes/no), or "input" (free text; add
"secret": true to enter the value hidden). One `### CLARIFY` block per question.
You get the answers back, then output the module.

## Output

When you have what you need, output exactly the module and nothing else:

### FILE: __init__.py
<python source>

## Task

Extension name: {name}
What it should do: {description}
"""


_FILE_RE = re.compile(r"###\s*FILE:\s*__init__\.py\s*\n([\s\S]*?)(?=\n###\s|\Z)", re.I)
_CLARIFY_RE = re.compile(r"###\s*CLARIFY\b[^\n]*\n([\s\S]*?)(?=\n###\s|\Z)", re.I)


def _strip_fence(text: str) -> str:
    text = text.strip()
    text = re.sub(r"^```\w*\n?", "", text)
    text = re.sub(r"\n?```$", "", text)
    return text.strip()


def parse_generation_response(text: str) -> tuple[list[Clarification], str | None]:
    """Split a model turn into clarifications and/or the final module source.

    A turn carrying a ``### FILE: __init__.py`` block is final — its clarifications,
    if any, are ignored. Otherwise every ``### CLARIFY`` block is a question to ask.
    """
    m = _FILE_RE.search(text)
    if m:
        return [], _strip_fence(m.group(1))

    clars: list[Clarification] = []
    for cm in _CLARIFY_RE.finditer(text):
        data = json.loads(_strip_fence(cm.group(1)))
        clars.append(
            Clarification(
                kind=data["kind"],
                question=data["question"],
                options=list(data.get("options") or []),
                secret=bool(data.get("secret", False)),
            )
        )
    return clars, None


# ---------------------------------------------------------------------------
# Secret-inline rejection (reporting/03: generated code never holds a secret)
# ---------------------------------------------------------------------------
def _is_config_field(func: ast.AST) -> bool:
    return isinstance(func, ast.Attribute) and func.attr == "config_field"


def _const_true(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value is True


def _nonempty_str(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and isinstance(node.value, str) and bool(
        node.value.strip()
    )


def _field_name(call: ast.Call) -> str | None:
    if call.args and isinstance(call.args[0], ast.Constant):
        val = call.args[0].value
        return val if isinstance(val, str) else None
    for kw in call.keywords:
        if kw.arg == "name" and isinstance(kw.value, ast.Constant):
            return kw.value.value if isinstance(kw.value.value, str) else None
    return None


def _target_name(target: ast.AST) -> str | None:
    if isinstance(target, ast.Name):
        return target.id
    if isinstance(target, ast.Attribute):
        return target.attr
    return None


def extract_secret_fields(source: str) -> set[str]:
    """Names declared ``api.config_field(..., secret=True)`` in the module."""
    secrets: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if not (isinstance(node, ast.Call) and _is_config_field(node.func)):
            continue
        if not any(kw.arg == "secret" and _const_true(kw.value) for kw in node.keywords):
            continue
        name = _field_name(node)
        if name:
            secrets.add(name)
    return secrets


def find_inlined_secret(source: str, secret_fields: set[str]) -> str | None:
    """The first secret field the module hardcodes a literal for, or None.

    Catches the three ways a key leaks into source: a ``secret=True`` field given a
    non-empty ``default=`` literal, an assignment to a name matching the field, and
    a dict entry keyed by the field name with a string-literal value. The legit path
    is always ``api.config()`` at call time.
    """
    if not secret_fields:
        return None
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and _is_config_field(node.func):
            name = _field_name(node)
            if name in secret_fields and any(
                kw.arg == "default" and _nonempty_str(kw.value) for kw in node.keywords
            ):
                return name
        elif isinstance(node, ast.Assign) and _nonempty_str(node.value):
            for tgt in node.targets:
                if _target_name(tgt) in secret_fields:
                    return _target_name(tgt)
        elif (
            isinstance(node, ast.AnnAssign)
            and node.value is not None
            and _nonempty_str(node.value)
            and _target_name(node.target) in secret_fields
        ):
            return _target_name(node.target)
        elif isinstance(node, ast.Dict):
            for key, val in zip(node.keys, node.values, strict=True):
                if (
                    isinstance(key, ast.Constant)
                    and key.value in secret_fields
                    and _nonempty_str(val)
                ):
                    return key.value
    return None


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------
_MAX_ROUNDS = 6


def _ask_all(clars: list[Clarification], ui: ClarifyUI) -> str:
    """Answer each clarification through the UI, render the answers for the model.

    A secret entered via ``input(secret=True)`` is never echoed back — feeding it to
    the model would put it one step from being inlined into the generated code.
    """
    lines: list[str] = []
    for c in clars:
        if c.kind == "select":
            shown = ui.select(c.question, c.options)
        elif c.kind == "confirm":
            shown = "yes" if ui.confirm(c.question) else "no"
        elif c.kind == "input":
            ans = ui.input(c.question, secret=c.secret)
            shown = "(provided)" if c.secret else ans
        else:
            raise GenerationError(f"unknown clarification kind: {c.kind!r}")
        lines.append(f"- {c.question} -> {shown}")
    return "Answers:\n" + "\n".join(lines) + "\nNow output the extension module."


class ExtensionGenerator:
    def __init__(self, llm: Any):
        self.llm = llm

    async def generate(self, name: str, description: str, ui: ClarifyUI) -> str:
        prompt = _GENERATION_PROMPT.replace("{name}", name).replace(
            "{description}", description
        )
        messages = [{"role": "user", "content": prompt}]
        for _ in range(_MAX_ROUNDS):
            text, _ = await self.llm.generate(
                messages=messages, system_prompt=_SYSTEM_PROMPT
            )
            clars, source = parse_generation_response(text)
            if source is not None:
                return source
            if not clars:
                raise GenerationError(
                    "model produced neither a module nor a clarification"
                )
            messages.append({"role": "assistant", "content": text})
            messages.append({"role": "user", "content": _ask_all(clars, ui)})
        raise GenerationError(f"no module after {_MAX_ROUNDS} clarification rounds")


@dataclass
class GenerationResult:
    name: str
    path: Path
    validated: bool
    reloaded: bool
    message: str


def _normalize_name(name: str) -> str:
    norm = re.sub(r"[^a-z0-9_]", "_", (name or "").lower().strip())
    if not norm:
        raise ValueError("invalid extension name")
    return norm


async def generate_extension(
    name: str, description: str, *, llm: Any, ui: ClarifyUI, host: Any
) -> GenerationResult:
    """Generate an extension end to end and hot-reload it into ``host``.

    Generate (clarifying through ``ui``) -> reject any inlined secret -> write under
    ``host.base_dir()`` -> ``host.reload()`` (child-process validation, then adopt).
    Raises ``FileExistsError`` if the extension exists, ``SecretInlineError`` if the
    module hardcodes a secret, ``GenerationError`` if the model never emits a module.
    A module that fails validation is left on disk (``reloaded=False``) so it can be
    edited and reloaded.
    """
    norm = _normalize_name(name)
    ext_dir = host.base_dir() / norm
    init_file = ext_dir / "__init__.py"
    if init_file.exists():
        raise FileExistsError(f"extension '{norm}' already exists at {ext_dir}")

    source = await ExtensionGenerator(llm).generate(norm, description, ui)

    try:
        offending = find_inlined_secret(source, extract_secret_fields(source))
    except SyntaxError:
        # Unparseable source can't hide a working secret; child validation rejects it.
        offending = None
    if offending is not None:
        raise SecretInlineError(
            f"generated module inlines a literal for secret field '{offending}'; "
            "read it via api.config() instead"
        )

    ext_dir.mkdir(parents=True, exist_ok=True)
    init_file.write_text(source, encoding="utf-8")

    result = host.reload()
    if norm in result.failed:
        return GenerationResult(norm, init_file, False, False, result.failed[norm])
    return GenerationResult(norm, init_file, True, True, result.report())


# ---------------------------------------------------------------------------
# Console driver
# ---------------------------------------------------------------------------
class ConsoleUI:
    """``ClarifyUI`` backed by rich prompts."""

    def __init__(self, console: Any):
        self._console = console

    def select(self, question: str, options: list[str]) -> str:
        from rich.prompt import Prompt

        self._console.print(question)
        for i, opt in enumerate(options, 1):
            self._console.print(f"  {i}. {opt}")
        choices = [str(i + 1) for i in range(len(options))]
        return options[int(Prompt.ask("Choose", choices=choices, default="1")) - 1]

    def confirm(self, question: str) -> bool:
        from rich.prompt import Confirm

        return Confirm.ask(question, default=True)

    def input(self, prompt: str, *, secret: bool = False) -> str:
        from rich.prompt import Prompt

        return Prompt.ask(prompt, password=secret)


async def run_new_extension_flow(console: Any, llm: Any, host: Any) -> str | None:
    """Interactive ``/new-extension``: prompt, generate, hot-reload."""
    from rich.prompt import Prompt

    name = Prompt.ask("Extension name (lowercase, no spaces)", default="")
    if not name:
        console.print("[dim]Cancelled.[/dim]")
        return None
    description = Prompt.ask("What should it do?", default="")
    if not description:
        console.print("[dim]Cancelled.[/dim]")
        return None

    console.print("[dim]Generating extension with the LLM…[/dim]")
    try:
        result = await generate_extension(
            name, description, llm=llm, ui=ConsoleUI(console), host=host
        )
    except FileExistsError as exc:
        console.print(f"[red]{exc}[/red]")
        return None
    except SecretInlineError as exc:
        console.print(f"[red]Rejected: {exc}[/red]")
        return None
    except (GenerationError, ValueError) as exc:
        console.print(f"[red]Generation failed: {exc}[/red]")
        return None

    if result.reloaded:
        console.print(f"[green]✓ {result.name} is live — {result.message}[/green]")
    else:
        console.print(f"[yellow]⚠ written but not loaded: {result.message}[/yellow]")
        console.print("[dim]Edit the module and /reload to retry.[/dim]")
    return result.name
