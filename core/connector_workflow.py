"""Connector import, push, and debug workflows.

These are the post-generation operations:

- **import**: validate and finalize a generated connector for local use
- **push**: git-commit and optionally push the connector to a remote
- **debug**: send a failing connector's code + error to the LLM for a fix
"""

from __future__ import annotations

import importlib
import subprocess
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt

from connectors.registry import (
    CONNECTORS_CONFIG_PATH,
    load_connectors_config,
    save_connectors_config,
)

_CONNECTORS_DIR = Path(__file__).resolve().parent.parent / "connectors"


def _available_connectors() -> list:
    if not _CONNECTORS_DIR.exists():
        return []
    return sorted(
        d.name
        for d in _CONNECTORS_DIR.iterdir()
        if d.is_dir() and (d / "manifest.yaml").exists()
    )


def _match_ci(name: str, available: list) -> str | None:
    """Return the canonical connector id matching ``name`` case-insensitively."""
    for cid in available:
        if cid.lower() == name.lower():
            return cid
    return None


def _resolve_name(name: str | None, console: Console) -> str | None:
    available = _available_connectors()
    if name:
        # Resolve case-insensitively to the canonical id (so `/import-connector S3`
        # finds `s3`), and validate it actually exists rather than trusting input.
        matched = _match_ci(name, available)
        if matched is None:
            console.print(f"[red]Unknown connector: {name}[/red]")
        return matched
    if not available:
        console.print("[dim]No connectors found.[/dim]")
        return None
    console.print("[bold]Available connectors:[/bold]")
    for idx, cid in enumerate(available, 1):
        console.print(f"  {idx}. {cid}")
    choice = Prompt.ask("Enter number or name", default="")
    if not choice:
        return None
    if choice.isdigit():
        num = int(choice)
        if 1 <= num <= len(available):
            return available[num - 1]
    matched = _match_ci(choice, available)
    if matched is None:
        console.print(f"[red]Unknown connector: {choice}[/red]")
    return matched


async def import_connector(
    name: str | None = None,
    console: Console = None,
    config_path: str = CONNECTORS_CONFIG_PATH,
    settings: Any = None,
) -> tuple[bool, str]:
    """Validate and finalize a connector for local use.

    Runs the same structural validation as generation (manifest + import +
    operations + post-conditions, via :func:`validate_connector`); only a
    connector that passes is enabled in ``connectors.yaml``.
    """
    con = console or Console()
    name = _resolve_name(name, con)
    if not name:
        return False, "No connector specified."

    connector_dir = _CONNECTORS_DIR / name
    if not connector_dir.exists():
        return False, f"Connector directory not found: {connector_dir}"

    con.print(f"[dim]Validating connector '{name}'…[/dim]")

    from core.connector_generator import validate_connector

    ok, msg = validate_connector(name, settings)
    if not ok:
        return False, f"Validation failed: {msg}"

    config = load_connectors_config(config_path)
    connectors = config.setdefault("connectors", {})
    entry = connectors.setdefault(name, {})
    entry["enabled"] = True
    entry.setdefault("operations", {})
    save_connectors_config(config, config_path)

    con.print(f"[green]✓ Connector '{name}' imported and enabled.[/green]")
    con.print("[dim]Restart dacli to load the new connector.[/dim]")
    return True, f"Connector '{name}' imported successfully."


async def push_connector(
    name: str | None = None,
    console: Console = None,
) -> tuple[bool, str]:
    """Git-commit and optionally push a connector.

    Steps:
    1. Stage the connector directory
    2. Commit with a descriptive message
    3. Optionally push to remote
    """
    con = console or Console()
    name = _resolve_name(name, con)
    if not name:
        return False, "No connector specified."

    connector_dir = _CONNECTORS_DIR / name
    if not connector_dir.exists():
        return False, f"Connector directory not found: {connector_dir}"

    con.print(f"[dim]Staging connector '{name}'…[/dim]")

    try:
        subprocess.run(
            ["git", "add", str(connector_dir)],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        return False, f"git add failed: {exc.stderr}"
    except FileNotFoundError:
        return False, "git not found on PATH."

    message = f"feat: add {name} connector"
    try:
        result = subprocess.run(
            ["git", "commit", "-m", message],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return False, f"git commit failed: {result.stderr}"
    except FileNotFoundError:
        return False, "git not found on PATH."

    con.print(f"[green]✓ Committed: {message}[/green]")

    if Confirm.ask("Push to remote?", default=False, console=con):
        try:
            result = subprocess.run(
                ["git", "push"],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                con.print(f"[yellow]⚠ Push failed: {result.stderr}[/yellow]")
                return True, f"Committed locally. Push failed: {result.stderr}"
            con.print("[green]✓ Pushed to remote.[/green]")
            return True, f"Connector '{name}' committed and pushed."
        except Exception as exc:
            return True, f"Committed locally. Push failed: {exc}"

    return True, f"Connector '{name}' committed locally."


async def debug_connector(
    name: str | None = None,
    console: Console = None,
    settings: Any = None,
    llm: Any = None,
) -> None:
    """Iterate on a failing connector with LLM assistance.

    Steps:
    1. Read the connector code
    2. Try to import it and capture any error
    3. Send the code + error to the LLM
    4. Apply the LLM's fix to the connector file
    5. Re-validate
    """
    con = console or Console()
    name = _resolve_name(name, con)
    if not name:
        con.print("[dim]No connector specified.[/dim]")
        return

    connector_dir = _CONNECTORS_DIR / name
    connector_py = connector_dir / "connector.py"
    manifest_yaml = connector_dir / "manifest.yaml"

    if not connector_py.exists():
        con.print(f"[red]connector.py not found at {connector_py}[/red]")
        return

    con.print("[dim]Reading connector code…[/dim]")
    code = connector_py.read_text(encoding="utf-8")
    manifest = (
        manifest_yaml.read_text(encoding="utf-8") if manifest_yaml.exists() else ""
    )

    error_msg = ""
    module_name = f"connectors.{name}.connector"
    try:
        mod = importlib.import_module(module_name)
        importlib.reload(mod)
        from connectors.base import Connector

        found = False
        for attr_name in dir(mod):
            attr = getattr(mod, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, Connector)
                and attr is not Connector
            ):
                found = True
                break
        if not found:
            error_msg = "No Connector subclass found in the module."
    except Exception as exc:
        error_msg = str(exc)

    if not error_msg:
        error_msg = Prompt.ask(
            "No import errors detected. Describe the issue",
            default="",
        )
        if not error_msg:
            con.print("[dim]No issue to debug.[/dim]")
            return

    con.print(
        Panel(
            f"[bold]Error:[/bold]\n{error_msg}",
            title=f"[accent]Debug: {name}[/accent]",
            border_style="border",
            padding=(1, 2),
        )
    )

    if llm is None:
        con.print("[red]LLM client not available.[/red]")
        return

    con.print("[dim]Sending to LLM for a fix…[/dim]")

    debug_prompt = f"""\
You are debugging a DACLI connector. Here is the current code and the error.

## manifest.yaml
```yaml
{manifest}
```

## connector.py
```python
{code}
```

## Error
{error_msg}

Fix the connector code. Output ONLY the corrected connector.py content,
wrapped in a single ```python ... ``` block. Rules:
1. Keep the same class name and module structure
2. The connector must subclass Connector or CliConnector
3. Every operation must declare at least one postcondition
4. Add NO comments to the code
"""
    try:
        response_text, _ = await llm.generate(
            messages=[{"role": "user", "content": debug_prompt}],
            system_prompt="You are an expert Python developer fixing a DACLI connector.",
        )
    except Exception as exc:
        con.print(f"[red]LLM call failed: {exc}[/red]")
        return

    import re

    # Only accept a real fenced code block. Writing a bare reply (e.g. the model
    # explaining itself in prose) straight over connector.py would corrupt it, so
    # we refuse rather than overwrite when no ```python block is present.
    code_match = re.search(r"```python\s*\n([\s\S]*?)```", response_text)
    if not code_match:
        con.print(
            "[yellow]⚠ The model did not return a Python code block — leaving the "
            "connector untouched. Run /debug-connector again to retry.[/yellow]"
        )
        return
    fixed_code = code_match.group(1).strip()
    if not fixed_code:
        con.print("[yellow]⚠ The proposed fix was empty — not applied.[/yellow]")
        return

    if not Confirm.ask("Apply the fix?", default=True, console=con):
        con.print("[dim]Fix not applied.[/dim]")
        return

    # Back up the current file before overwriting so a bad fix is recoverable.
    backup = connector_py.with_suffix(".py.bak")
    try:
        backup.write_text(code, encoding="utf-8")
    except Exception:
        backup = None  # best-effort; proceed even if the backup can't be written
    connector_py.write_text(fixed_code, encoding="utf-8")
    con.print(f"[green]✓ Updated {connector_py}[/green]")
    if backup is not None:
        con.print(f"[dim]Backup saved to {backup.name}[/dim]")

    # Re-validate with the same bar as generation/import.
    from core.connector_generator import validate_connector

    ok, msg = validate_connector(name, settings)
    if ok:
        con.print(f"[green]✓ Re-validation passed — {msg}[/green]")
    else:
        con.print(f"[yellow]⚠ Re-validation failed: {msg}[/yellow]")
        con.print("[dim]Run /debug-connector again to iterate.[/dim]")
