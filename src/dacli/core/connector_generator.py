"""LLM-driven connector code generator.

Given a connector name and a natural-language description, uses the agent's
configured LLM to produce a working connector (manifest.yaml + connector.py +
__init__.py) in the ``connectors/<name>/`` folder.

The generated connector must:
- Subclass ``Connector`` or ``CliConnector``
- Declare ``OperationSpec`` entries with parameters, risk, and postconditions
- Follow the manifest.yaml convention for registry discovery

This is how dacli extends itself: the user describes what a connector should do,
and the agent writes the code.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt

from dacli.connectors.registry import (
    ConnectorRegistry,
    CONNECTORS_CONFIG_PATH,
    load_connectors_config,
    save_connectors_config,
)
from dacli.core.store import DacliStore

_CONNECTORS_DIR = Path(__file__).resolve().parent.parent / "connectors"

_GENERATION_PROMPT = """\
You are a code generator for the DACLI data-agent connector system.

Given a connector name and description, produce THREE files for a new connector
that integrates with the described platform/tool.

## Base Classes (you MUST subclass one of these)

### connectors.base.Connector (abstract base)
```python
class Connector(ABC):
    name: str = ""  # stable id like "mytool"
    def __init__(self, settings): ...
    @abstractmethod
    def operations(self) -> List[OperationSpec]: ...
    @abstractmethod
    async def invoke(self, op: str, args: Dict[str, Any]) -> ToolResult: ...
    @abstractmethod
    async def health(self) -> ToolResult: ...
```

### connectors.cli_base.CliConnector (for CLI-driven connectors)
```python
class CliConnector(Connector):
    binary: str = ""  # CLI binary name e.g. "aws", "gcloud"
    def __init__(self, settings, runner=None): ...
    async def _run(self, argv, *, cwd=None, env=None, timeout=None, stdin=None) -> CliResult: ...
    def _ok(self, op, data, started, **metadata) -> ToolResult: ...
    def _fail(self, op, error, started, **metadata) -> ToolResult: ...
```

### OperationSpec
```python
@dataclass
class OperationSpec:
    name: str           # LLM-facing tool name e.g. "query_mydb"
    description: str    # What this operation does
    parameters: Dict    # JSON Schema for arguments
    capability: str     # e.g. "mydb.query"
    risk: Risk          # Risk.SAFE | Risk.WRITE | Risk.RISKY | Risk.IRREVERSIBLE
    display_name: Optional[str] = None
    category: Optional[str] = None
    postconditions: List[Any] = field(default_factory=list)
```

### ToolResult and ToolStatus
```python
class ToolStatus(Enum):
    SUCCESS, ERROR, TIMEOUT, CANCELLED, PENDING_APPROVAL = ...
    DENIED, BLOCKED = ...

@dataclass
class ToolResult:
    tool_name: str
    status: ToolStatus
    data: Any = None
    error: Optional[str] = None
    execution_time_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
```

### Risk levels
```python
class Risk(str, Enum):
    SAFE = "safe"            # read-only / no side effects
    WRITE = "write"          # creates or mutates state, recoverable
    RISKY = "risky"          # arbitrary or hard-to-predict side effects
    IRREVERSIBLE = "irreversible"  # destructive, not easily undone
```

## Example: manifest.yaml
```yaml
id: s3
name: Amazon S3
description: List, read, upload, and delete S3 objects with versioned rollback
icon: "🪣"
class: dacli.connectors.s3.connector.S3Connector
required_config: [bucket]
enabled: false
default_scope: read_only
# config_fields drives the /connect prompts. Mark credentials secret: true so
# they are entered with a hidden prompt and stored encrypted.
config_fields:
  - name: bucket
    type: str
    required: true
    description: Target S3 bucket name
  - name: access_key
    type: str
    required: true
    secret: true
    description: AWS access key id
  - name: secret_key
    type: str
    required: true
    secret: true
    description: AWS secret access key
```

## Reading configuration at runtime

Generated connectors have NO Settings section, so do NOT read
`settings.<id>`. Instead, in `__init__`, load the values the user entered via
`/connect` (decrypted automatically):

```python
from dacli.connectors.base import Connector
from dacli.core.connector_config import load_connector_config

class MyToolConnector(Connector):
    name = "mytool"
    def __init__(self, settings):
        super().__init__(settings)
        self.cfg = load_connector_config("mytool", settings=settings)  # dict of config_fields
        # e.g. self.cfg.get("api_key")
```

## Example connector pattern (PostgreSQL — CLI-first via psql)
- Subclasses CliConnector with binary="psql"
- Operations: execute_query, introspect_object
- Each operation declares postconditions (e.g. result_succeeded())
- Uses self._run() to execute psql commands
- Uses self._ok() / self._fail() for uniform ToolResult construction

## YOUR TASK

Connector name: {name}
Description: {description}

Generate exactly these three files. Output them in this format:

### FILE: manifest.yaml
(your manifest content here)

### FILE: connector.py
(your connector python code here)

### FILE: __init__.py
(your init file content here — typically empty or a one-line docstring)

Rules:
1. The manifest `class` field MUST be `connectors.{name}.connector.{ClassName}`
2. The connector MUST subclass either `Connector` or `CliConnector`
3. Every operation MUST declare at least one postcondition
4. Use `from core.verify import PostCondition, VerificationContext, result_succeeded` for postconditions
5. Import from `connectors.base` and `connectors.cli_base` as needed
6. The `name` attribute MUST match the connector id in the manifest
7. If the connector uses a CLI binary, subclass CliConnector and set `binary`
8. If the connector uses a REST API or SDK, subclass Connector directly
9. Add NO comments to the generated code
10. The connector id in manifest MUST be `{name}`
11. The manifest MUST include a `config_fields` list for every credential/setting
    the connector needs, marking secrets with `secret: true`
12. Read configuration via `load_connector_config("{name}", settings=settings)` in
    `__init__` (NEVER `settings.{name}` — generated connectors have no Settings section)
"""


@dataclass
class GeneratedConnector:
    name: str
    manifest: str
    connector_py: str
    init_py: str
    path: Path | None = None


def _extract_files(llm_response: str) -> dict[str, str]:
    files: dict[str, str] = {}
    pattern = r"###\s*FILE:\s*(\S+)\s*\n([\s\S]*?)(?=###\s*FILE:|\Z)"
    for match in re.finditer(pattern, llm_response):
        filename = match.group(1).strip()
        content = match.group(2).strip()
        if content.startswith("```"):
            content = re.sub(r"^```\w*\n?", "", content)
            content = re.sub(r"\n?```$", "", content)
        files[filename] = content.strip()
    return files


#: Wall-clock budget for the sandboxed validation import of generated code.
_VALIDATION_TIMEOUT_S = 30.0

# Runs in a child python process (security: the connector module is
# LLM-authored, so the validation import must never execute in the agent's
# process). Loads the module by file path under its canonical dotted name,
# locates the Connector subclass, instantiates it, and checks operations() +
# post-conditions. Reports the verdict as a single JSON line on stdout; the
# parent only ever parses that data. argv: <connector.py path> <module name>.
_VALIDATION_SCRIPT = """\
import importlib.util
import json
import sys


def _verdict(ok, message):
    print(json.dumps({"ok": ok, "message": message}))
    sys.exit(0)


path, module_name = sys.argv[1], sys.argv[2]

from dacli.connectors.base import Connector, OperationSpec

try:
    spec = importlib.util.spec_from_file_location(module_name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
except Exception as exc:
    _verdict(False, f"Import failed: {exc}")

connector_cls = None
for attr_name in dir(mod):
    attr = getattr(mod, attr_name)
    if (
        isinstance(attr, type)
        and issubclass(attr, Connector)
        and attr is not Connector
        and attr.__module__ == mod.__name__
    ):
        connector_cls = attr
        break
if connector_cls is None:
    _verdict(False, "No Connector subclass found in generated code")

try:
    from dacli.config.settings import Settings, load_config

    try:
        settings = load_config()
    except Exception:
        settings = Settings()
    instance = connector_cls(settings)
except Exception as exc:
    _verdict(False, f"Connector failed to instantiate: {exc}")

try:
    ops = instance.operations()
except Exception as exc:
    _verdict(False, f"operations() raised: {exc}")
if not isinstance(ops, list) or not ops:
    _verdict(False, "operations() must return a non-empty list of OperationSpec")

for op_spec in ops:
    if not isinstance(op_spec, OperationSpec):
        _verdict(
            False,
            f"operations() returned a {type(op_spec).__name__}, expected OperationSpec",
        )
    if not getattr(op_spec, "postconditions", None):
        _verdict(
            False,
            f"operation '{op_spec.name}' declares no post-condition "
            "(every operation must have at least one)",
        )

_verdict(True, f"Valid: {len(ops)} operation(s), manifest + import OK")
"""


def _validate_in_subprocess(name: str, connector_file: Path) -> tuple[bool, str]:
    """Run the import/instantiate/operations checks in a child python process.

    The connector module is untrusted LLM-generated code; executing its
    module-level statements in-process would run it with the agent's
    privileges. The child reports its verdict as JSON on stdout, so a
    malicious or broken generation can at worst fail its own process.
    """
    module_name = f"dacli.connectors.{name}.connector"
    # Make `dacli` importable in the child regardless of how the parent was
    # launched (installed package or src layout).
    package_root = str(Path(__file__).resolve().parent.parent.parent)
    env = dict(os.environ)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        package_root + os.pathsep + existing if existing else package_root
    )
    try:
        proc = subprocess.run(  # fixed argv, no shell
            [sys.executable, "-c", _VALIDATION_SCRIPT, str(connector_file), module_name],
            capture_output=True,
            text=True,
            timeout=_VALIDATION_TIMEOUT_S,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return False, (
            f"Validation timed out after {_VALIDATION_TIMEOUT_S:.0f}s "
            "(module-level code may be blocking)"
        )
    except Exception as exc:
        return False, f"Validation subprocess could not run: {exc}"

    # The verdict is the last stdout line; module-level prints may precede it.
    for line in reversed((proc.stdout or "").strip().splitlines()):
        try:
            verdict = json.loads(line)
            return bool(verdict.get("ok")), str(verdict.get("message", ""))
        except (json.JSONDecodeError, ValueError):
            continue
    detail = (proc.stderr or proc.stdout or "").strip()
    return False, f"Validation subprocess failed (exit {proc.returncode}): {detail[-500:]}"


def validate_connector(
    name: str, settings: Any, connectors_dir: Path | None = None
) -> tuple[bool, str]:
    """Structurally validate the connector at ``connectors/<name>/``.

    The single validator shared by generation, ``/import-connector``, and the
    ``/debug-connector`` re-check so all three enforce the same bar. Checks, in
    order: the manifest parses and declares a matching id/name/class; the module
    imports; it defines a ``Connector`` subclass (from this module) that
    instantiates; ``operations()`` returns a non-empty list of ``OperationSpec``;
    and every operation declares at least one post-condition ("no post-condition,
    no acceptance"). Returns ``(ok, message)`` with a specific reason on failure.

    SECURITY: the import/instantiate/operations checks run in a child python
    subprocess (see :func:`_validate_in_subprocess`), never in this process —
    the module is LLM-authored and must not execute with the agent's
    privileges during validation. It only enters the host process via the
    registry after the user runs ``/import-connector`` + restart.
    """
    import yaml

    connector_dir = (connectors_dir or _CONNECTORS_DIR) / name
    connector_file = connector_dir / "connector.py"
    manifest_file = connector_dir / "manifest.yaml"
    if not connector_file.exists():
        return False, f"connector.py not found at {connector_file}"

    # 1. Manifest sanity.
    if not manifest_file.exists():
        return False, "manifest.yaml not found"
    try:
        manifest = yaml.safe_load(manifest_file.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        return False, f"manifest.yaml is not valid YAML: {exc}"
    for key in ("id", "name", "class"):
        if not manifest.get(key):
            return False, f"manifest.yaml is missing required key '{key}'"
    if manifest.get("id") != name:
        return False, (
            f"manifest id '{manifest.get('id')}' must match connector name '{name}'"
        )

    # 2+3. Import, instantiate, and introspect — sandboxed in a subprocess.
    return _validate_in_subprocess(name, connector_file)


_TEMPLATES_DIR = _CONNECTORS_DIR / "templates"


def _reference_section() -> str:
    """Few-shot reference templates appended to the generation prompt.

    Read from ``connectors/templates/`` so the examples never drift from the
    real base-class contracts. Appended *after* ``_GENERATION_PROMPT.format`` so
    the templates' literal ``{`` / ``}`` (dict literals) aren't seen by
    ``str.format``.
    """
    files = [
        ("manifest.yaml (reference)", "yaml", "manifest_template.yaml"),
        ("connector.py — REST/SDK pattern", "python", "rest_connector_template.py"),
        ("connector.py — CLI pattern", "python", "cli_connector_template.py"),
    ]
    blocks: list = [
        "## Reference templates (study these — match their structure)\n"
    ]
    for title, lang, fname in files:
        try:
            body = (_TEMPLATES_DIR / fname).read_text(encoding="utf-8")
        except Exception:
            continue
        blocks.append(f"### {title}\n```{lang}\n{body}\n```")
    return "\n\n".join(blocks)


class ConnectorGenerator:
    def __init__(self, settings: Any, llm_client: Any):
        self.settings = settings
        self.llm = llm_client

    async def generate(self, name: str, description: str) -> GeneratedConnector:
        # NOTE: explicit .replace, not str.format — the prompt contains a literal
        # ``{ClassName}`` instruction for the model that str.format would treat as
        # a missing field (KeyError).
        prompt = _GENERATION_PROMPT.replace("{name}", name).replace(
            "{description}", description
        )
        reference = _reference_section()
        if reference:
            prompt = f"{prompt}\n\n{reference}"
        messages = [
            {"role": "user", "content": prompt},
        ]
        response_text, _ = await self.llm.generate(
            messages=messages,
            system_prompt="You are an expert Python developer generating DACLI connector code. Output only the three files in the specified format.",
        )
        files = _extract_files(response_text)
        manifest = files.get("manifest.yaml", "")
        connector_py = files.get("connector.py", "")
        init_py = files.get("__init__.py", "")
        if not manifest or not connector_py:
            raise ValueError(
                f"LLM did not produce required files. Got: {list(files.keys())}"
            )
        return GeneratedConnector(
            name=name,
            manifest=manifest,
            connector_py=connector_py,
            init_py=init_py,
        )

    def write_to_disk(self, generated: GeneratedConnector) -> Path:
        connector_dir = _CONNECTORS_DIR / generated.name
        connector_dir.mkdir(parents=True, exist_ok=True)
        (connector_dir / "manifest.yaml").write_text(
            generated.manifest, encoding="utf-8"
        )
        (connector_dir / "connector.py").write_text(
            generated.connector_py, encoding="utf-8"
        )
        (connector_dir / "__init__.py").write_text(
            generated.init_py or "", encoding="utf-8"
        )
        generated.path = connector_dir
        return connector_dir

    def validate(self, generated: GeneratedConnector) -> tuple[bool, str]:
        """Structurally validate a generated connector (see :func:`validate_connector`)."""
        return validate_connector(generated.name, self.settings)


@dataclass
class GenerationResult:
    name: str
    path: Path
    validated: bool
    message: str


async def generate_connector_files(
    name: str,
    description: str,
    settings: Any,
    llm: Any,
    config_path: str = CONNECTORS_CONFIG_PATH,
) -> GenerationResult:
    """Generate, write, validate, and register (disabled) a connector.

    The non-interactive core shared by the ``/new-connector`` flow and the in-chat
    ``generate_connector`` capability. Always registers the connector **disabled**
    so unvalidated code never auto-loads. Raises ``ValueError`` for an invalid
    name and ``FileExistsError`` if the connector already exists.
    """
    norm = re.sub(r"[^a-z0-9_]", "_", (name or "").lower().strip())
    if not norm:
        raise ValueError("invalid connector name")
    existing = _CONNECTORS_DIR / norm
    if existing.exists() and (existing / "manifest.yaml").exists():
        raise FileExistsError(f"Connector '{norm}' already exists at {existing}")

    gen = ConnectorGenerator(settings, llm)
    generated = await gen.generate(norm, description)
    path = gen.write_to_disk(generated)
    ok, msg = gen.validate(generated)

    config = load_connectors_config(config_path)
    config.setdefault("connectors", {})[norm] = {"enabled": False, "operations": {}}
    save_connectors_config(config, config_path)
    return GenerationResult(name=norm, path=path, validated=ok, message=msg)


async def run_new_connector_flow(
    console: Console,
    settings: Any,
    llm: Any,
    registry: ConnectorRegistry,
    store: DacliStore,
    config_path: str = CONNECTORS_CONFIG_PATH,
) -> str | None:
    """Interactive flow for /new-connector. Returns the connector name or None."""
    console.print(
        Panel(
            "[bold]Create a new connector[/bold]\n\n"
            "Describe what the connector should do in natural language.\n"
            "dacli will generate the code using the LLM.",
            title="[accent]New Connector[/accent]",
            border_style="border",
            padding=(1, 2),
        )
    )
    console.print()

    name = Prompt.ask("Connector name (lowercase, no spaces)", default="")
    if not name:
        console.print("[dim]Cancelled.[/dim]")
        return None

    description = Prompt.ask("Description (what does this connector do?)", default="")
    if not description:
        console.print("[dim]Cancelled.[/dim]")
        return None

    console.print()
    console.print("[dim]Generating connector code with LLM…[/dim]")

    try:
        result = await generate_connector_files(
            name, description, settings, llm, config_path
        )
    except FileExistsError as exc:
        console.print(f"[red]{exc}[/red]")
        return None
    except ValueError as exc:
        console.print(f"[red]Invalid name: {exc}[/red]")
        return None
    except Exception as exc:
        console.print(f"[red]Generation failed: {exc}[/red]")
        return None

    name = result.name
    console.print(f"[green]✓ Wrote connector to {result.path}[/green]")
    if result.validated:
        console.print("[green]✓ Validation passed[/green]")
    else:
        console.print(f"[yellow]⚠ Validation warning: {result.message}[/yellow]")
        console.print(
            "[dim]The connector was written but may need fixes. Use /debug-connector to iterate.[/dim]"
        )

    console.print()
    console.print(
        "[dim]Registered as disabled — it won't load into the agent until you import it.[/dim]"
    )
    console.print("[bold]Next steps:[/bold]")
    console.print(f"  1. [cyan]/connect {name}[/cyan] — configure credentials")
    console.print(f"  2. [cyan]/import-connector {name}[/cyan] — validate + enable (then restart to load it)")
    console.print(f"  3. [cyan]/testmode {name}[/cyan] — stage it, then ask the agent to use it safely")
    console.print(f"  4. [cyan]/push-connector {name}[/cyan] — git commit once it works")

    return name
