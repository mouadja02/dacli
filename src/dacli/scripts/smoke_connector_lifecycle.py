"""Offline smoke test for the connector lifecycle (TEST.md §7).

Drives **generate → validate → stage → import** with a stub LLM — no network,
no credentials, no live platform. It exercises the real code paths:

* ``core.connector_generator.generate_connector_files`` (LLM → files on disk,
  validate, register disabled),
* ``core.connector_generator.validate_connector`` (manifest + import + ops +
  post-conditions),
* ``connectors.dispatcher.Dispatcher`` staging (health gate, ``test_mode`` tag,
  catalog-effect suppression) via ``core.test_mode.StagingMode``,
* ``core.connector_workflow.import_connector`` (validate + enable).

It writes a throwaway connector into ``connectors/<name>/`` (required for a real
import) and a *temporary* connectors.yaml (so your real config is untouched), and
cleans both up in ``finally``.

Run from the repo root::

    python scripts/smoke_connector_lifecycle.py

Exit code 0 and a final ``SMOKE PASSED`` line means everything worked.
"""

from __future__ import annotations

# This script bootstraps sys.path before importing the dacli package, so the
# dacli imports intentionally come after that setup (E402 doesn't apply here).

import asyncio
import sys
import shutil
import tempfile
from pathlib import Path

# Make the `dacli` package importable when run as a script (add the `src/` root).
_SRC_ROOT = Path(__file__).resolve().parents[2]
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from rich.console import Console

from dacli.config.settings import load_config
from dacli.connectors.registry import ConnectorRegistry
from dacli.connectors.dispatcher import Dispatcher
from dacli.core.test_mode import StagingMode
from dacli.core.connector_generator import (
    generate_connector_files,
    validate_connector,
    _CONNECTORS_DIR,
)
from dacli.core.connector_workflow import import_connector
import contextlib

CONNECTOR_NAME = "smoketest_conn"
CLASS_NAME = "SmoketestConnConnector"
console = Console()


# ---------------------------------------------------------------------------
# Stub LLM — returns a valid connector in the generator's expected file format.
# ---------------------------------------------------------------------------
_MANIFEST = f"""\
id: {CONNECTOR_NAME}
name: Smoke Test Connector
description: A throwaway connector used by the offline lifecycle smoke test.
icon: "🧪"
class: dacli.connectors.{CONNECTOR_NAME}.connector.{CLASS_NAME}
enabled: false
default_scope: read_only
config_fields:
  - name: api_key
    type: str
    required: true
    secret: true
    description: API key for the (fake) service
  - name: base_url
    type: str
    required: false
    default: "https://api.example.com"
    description: API base URL
"""

# Built with placeholder substitution (not str.format) to avoid escaping the
# many ``{`` / ``}`` in the dict literals below.
_CONNECTOR_PY = """\
from __future__ import annotations

import time
from typing import Any, Dict, List

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.core.connector_config import load_connector_config
from dacli.core.verify import result_succeeded


class __CLS__(Connector):
    name = "__NAME__"

    def __init__(self, settings: Any):
        super().__init__(settings)
        self.cfg = load_connector_config("__NAME__", settings=settings)

    def operations(self) -> List[OperationSpec]:
        return [
            OperationSpec(
                name="__NAME___echo",
                description="Echo a message back.",
                parameters={
                    "type": "object",
                    "properties": {"message": {"type": "string"}},
                    "required": ["message"],
                },
                capability="__NAME__.echo",
                risk=Risk.SAFE,
                display_name="Echo",
                category="__NAME__",
                postconditions=[result_succeeded()],
            )
        ]

    async def invoke(self, op: str, args: Dict[str, Any]) -> ToolResult:
        if op == "__NAME___echo":
            return ToolResult(
                tool_name=op,
                status=ToolStatus.SUCCESS,
                data={"echo": args.get("message")},
                # Declares a catalog effect so we can prove staging suppresses it.
                metadata={"catalog_effects": [{"action": "create", "object": "smoke"}]},
            )
        return ToolResult(tool_name=op, status=ToolStatus.ERROR, error=f"unknown op '{op}'")

    async def health(self) -> ToolResult:
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS, data={"ready": True})
""".replace("__CLS__", CLASS_NAME).replace("__NAME__", CONNECTOR_NAME)


class StubLLM:
    """Minimal stand-in for reasoning.llm.LLMClient — returns canned files."""

    async def generate(self, messages=None, system_prompt=None, **kwargs):
        text = (
            "### FILE: manifest.yaml\n" + _MANIFEST + "\n"
            "### FILE: connector.py\n" + _CONNECTOR_PY + "\n"
            "### FILE: __init__.py\n\n"
        )
        return text, []


# ---------------------------------------------------------------------------
# Fakes for the dispatcher
# ---------------------------------------------------------------------------
class FakeMemory:
    def __init__(self):
        self.catalog_applied = []

    def log_tool_execution(self, **kwargs):
        pass

    def apply_catalog_effects(self, connector_name, effects):
        self.catalog_applied.append((connector_name, effects))


def ok(msg: str) -> None:
    console.print(f"[green]✓[/green] {msg}")


def fail(msg: str) -> None:
    console.print(f"[red]✗ {msg}[/red]")
    raise AssertionError(msg)


def _purge_modules() -> None:
    for mod in list(sys.modules):
        if mod.startswith(f"connectors.{CONNECTOR_NAME}"):
            del sys.modules[mod]


def _cleanup(tmp_yaml: Path) -> None:
    _purge_modules()
    shutil.rmtree(_CONNECTORS_DIR / CONNECTOR_NAME, ignore_errors=True)
    with contextlib.suppress(Exception):
        tmp_yaml.unlink(missing_ok=True)


async def main() -> int:
    settings = load_config()
    tmp_yaml = Path(tempfile.gettempdir()) / "dacli_smoke_connectors.yaml"
    # Start clean even if a previous run aborted.
    _cleanup(tmp_yaml)

    try:
        # ---- 1. GENERATE ----------------------------------------------------
        console.rule("[bold]1. Generate")
        result = await generate_connector_files(
            CONNECTOR_NAME, "echo service", settings, StubLLM(), config_path=str(tmp_yaml)
        )
        if result.name != CONNECTOR_NAME:
            fail(f"generated name {result.name!r} != {CONNECTOR_NAME!r}")
        conn_dir = _CONNECTORS_DIR / CONNECTOR_NAME
        for fname in ("manifest.yaml", "connector.py", "__init__.py"):
            if not (conn_dir / fname).exists():
                fail(f"missing generated file: {fname}")
        ok(f"wrote {conn_dir} (3 files)")
        if not result.validated:
            fail(f"generation validation failed: {result.message}")
        ok(f"generation validated: {result.message}")

        import yaml

        cfg = yaml.safe_load(tmp_yaml.read_text(encoding="utf-8")) or {}
        entry = cfg.get("connectors", {}).get(CONNECTOR_NAME, {})
        if entry.get("enabled") is not False:
            fail(f"expected registered DISABLED, got enabled={entry.get('enabled')!r}")
        ok("registered as disabled (won't auto-load)")

        # ---- 2. VALIDATE (standalone) --------------------------------------
        console.rule("[bold]2. Validate")
        valid, msg = validate_connector(CONNECTOR_NAME, settings)
        if not valid:
            fail(f"validate_connector failed: {msg}")
        ok(f"validate_connector: {msg}")

        # ---- 3. STAGE (test mode) ------------------------------------------
        console.rule("[bold]3. Stage (test mode)")
        registry = ConnectorRegistry(settings, config_path=str(tmp_yaml))
        if registry.resolve(f"{CONNECTOR_NAME}_echo") is None:
            fail("registry did not discover the generated op")
        if CONNECTOR_NAME in registry.failed_connectors():
            fail(f"connector failed to load: {registry.failed_connectors()[CONNECTOR_NAME]}")
        ok("registry discovered the connector (fault-isolation intact)")

        op = f"{CONNECTOR_NAME}_echo"

        # 3a. test mode OFF -> normal: no test tag, catalog effect applied.
        tm_off = StagingMode()
        mem_off = FakeMemory()
        disp_off = Dispatcher(registry, memory=mem_off, test_mode=tm_off)
        r_off = await disp_off.execute(op, {"message": "hi"})
        if not r_off.success:
            fail(f"off-mode call failed: {r_off.error}")
        if r_off.metadata.get("test_mode"):
            fail("off-mode result should NOT carry a test_mode tag")
        if not mem_off.catalog_applied:
            fail("off-mode should apply catalog effects")
        ok("test mode OFF: normal success, catalog effect applied, no [TEST] tag")

        # 3b. test mode ON -> staged: health-gated, tagged, catalog suppressed.
        tm_on = StagingMode()
        tm_on.activate(connector_name=CONNECTOR_NAME)
        mem_on = FakeMemory()
        disp_on = Dispatcher(registry, memory=mem_on, test_mode=tm_on)
        r_on = await disp_on.execute(op, {"message": "hi"})
        if not r_on.success:
            fail(f"staged call failed: {r_on.error}")
        if r_on.metadata.get("test_mode") != CONNECTOR_NAME:
            fail(f"staged result not tagged: {r_on.metadata!r}")
        if mem_on.catalog_applied:
            fail("staged call must NOT apply catalog effects")
        if not tm_on.is_verified(CONNECTOR_NAME):
            fail("connector should be marked health-verified after first staged call")
        ok("test mode ON: health-gated, result tagged test_mode, catalog suppressed")

        # ---- 4. IMPORT (validate + enable) ---------------------------------
        console.rule("[bold]4. Import")
        imported, imsg = await import_connector(
            name=CONNECTOR_NAME, console=console, config_path=str(tmp_yaml), settings=settings
        )
        if not imported:
            fail(f"import_connector failed: {imsg}")
        cfg2 = yaml.safe_load(tmp_yaml.read_text(encoding="utf-8")) or {}
        if cfg2.get("connectors", {}).get(CONNECTOR_NAME, {}).get("enabled") is not True:
            fail("import did not enable the connector in connectors.yaml")
        ok(f"import_connector: {imsg}  (now enabled)")

        console.print()
        console.print("[bold green]SMOKE PASSED[/bold green] — generate → validate → stage → import all OK")
        return 0

    except AssertionError:
        console.print("\n[bold red]SMOKE FAILED[/bold red]")
        return 1
    except Exception as exc:
        console.print(f"\n[bold red]SMOKE ERROR:[/bold red] {exc!r}")
        import traceback

        traceback.print_exc()
        return 1
    finally:
        _cleanup(tmp_yaml)
        console.print("[dim]cleaned up temp connector + config[/dim]")


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
