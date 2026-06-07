"""Interactive ``/connect`` flow — prompt for connector credentials.

The flow:
1. List available connectors (or jump to a specific one via ``/connect <tool>``).
2. For each config field in the connector's settings model:
   - Required fields (no usable default): **mandatory** prompt, cannot skip.
   - Optional fields: prompt with ``[skip]`` default.
   - Secret fields (password, token, api_key, ...): password-mode prompt.
3. Validate via ``connector.health()`` after collection.
4. Store encrypted via ``DacliStore.set_secret()``.
5. Enable the connector in ``config/connectors.yaml``.
"""

from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich import box

from dacli.connectors.registry import (
    ConnectorRegistry,
    CONNECTORS_CONFIG_PATH,
    load_connectors_config,
    save_connectors_config,
    ConfigField,
)
from dacli.core.store import DacliStore


def _pick_connector(console: Console, registry: ConnectorRegistry) -> str | None:
    catalog = registry.get_catalog()
    ids = registry.get_connector_ids()
    if not ids:
        console.print("[dim]No connectors available.[/dim]")
        return None

    table = Table(
        title="Available Connectors",
        box=box.ROUNDED,
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("#", style="dim", width=3, justify="right")
    table.add_column("Connector", style="cyan")
    table.add_column("Description")
    table.add_column("Status", justify="center")

    for idx, cid in enumerate(ids, 1):
        info = catalog.get(cid, {})
        enabled = registry.is_connector_enabled(cid)
        status = "[green]enabled[/green]" if enabled else "[dim]not configured[/dim]"
        table.add_row(
            str(idx),
            f"{info.get('icon', '')} {info.get('name', cid)}",
            info.get("description", ""),
            status,
        )

    console.print(table)
    console.print()

    choice = Prompt.ask(
        "Enter connector number or name",
        default="",
    )
    if not choice:
        return None

    if choice.isdigit():
        num = int(choice)
        if 1 <= num <= len(ids):
            return ids[num - 1]
        console.print(f"[red]Invalid number: {num}[/red]")
        return None

    if choice in ids:
        return choice
    for cid in ids:
        if cid.lower() == choice.lower():
            return cid
    console.print(f"[red]Unknown connector: {choice}[/red]")
    return None


def _prompt_field(
    console: Console, connector_name: str, field: ConfigField
) -> str | None:
    label = field.name.replace("_", " ").title()
    hint = f" ({field.description})" if field.description else ""

    if field.is_secret:
        if field.required:
            val = Prompt.ask(
                f"  [bold]{label}[/bold]{hint} [red](required)[/red]",
                password=True,
            )
            if not val:
                console.print(f"  [red]{label} is required.[/red]")
                val = Prompt.ask(
                    f"  [bold]{label}[/bold]{hint} [red](required)[/red]",
                    password=True,
                )
            return val or None
        val = Prompt.ask(
            f"  {label}{hint} [dim](optional, press Enter to skip)[/dim]",
            password=True,
            default="",
        )
        return val or None
    if field.required:
        default = str(field.default) if field.default else ""
        val = Prompt.ask(
            f"  [bold]{label}[/bold]{hint} [red](required)[/red]",
            default=default,
        )
        if not val and not default:
            console.print(f"  [red]{label} is required.[/red]")
            val = Prompt.ask(
                f"  [bold]{label}[/bold]{hint} [red](required)[/red]",
            )
        return val or None
    default = str(field.default) if field.default else ""
    val = Prompt.ask(
        f"  {label}{hint} [dim](optional)[/dim]",
        default=default,
    )
    return val or None


async def run_connect_flow(
    console: Console,
    registry: ConnectorRegistry,
    settings: Any,
    store: DacliStore,
    connector_id: str | None = None,
    config_path: str = CONNECTORS_CONFIG_PATH,
) -> tuple[bool, str]:
    """Run the interactive /connect flow.

    Returns ``(success, message)``.
    """
    from dacli.config.settings import load_config

    if connector_id is None:
        connector_id = _pick_connector(console, registry)
        if connector_id is None:
            return False, "No connector selected."

    catalog = registry.get_catalog()
    info = catalog.get(connector_id)
    if info is None:
        return False, f"Unknown connector: {connector_id}"

    console.print(
        Panel(
            f"{info.get('icon', '')} [bold]{info.get('name', connector_id)}[/bold]\n"
            f"{info.get('description', '')}",
            title="[accent]Connect[/accent]",
            border_style="border",
            padding=(1, 2),
        )
    )
    console.print()

    fields = registry.get_config_fields(connector_id)
    if not fields:
        return False, f"No config fields found for '{connector_id}'."

    collected: dict[str, str] = {}
    required_fields = [f for f in fields if f.required]
    optional_fields = [f for f in fields if not f.required]

    if required_fields:
        console.print("[bold]Required fields:[/bold]")
        for field in required_fields:
            val = _prompt_field(console, info.get("name", connector_id), field)
            if val:
                collected[field.name] = val

    missing_required = [f for f in required_fields if f.name not in collected]
    if missing_required:
        names = ", ".join(f.name for f in missing_required)
        return False, f"Missing required fields: {names}"

    if optional_fields:
        console.print()
        console.print("[bold]Optional fields [dim](press Enter to skip):[/dim][/bold]")
        for field in optional_fields:
            val = _prompt_field(console, info.get("name", connector_id), field)
            if val:
                collected[field.name] = val

    if not collected:
        return False, "No values provided."

    console.print()
    console.print("[dim]Saving credentials (encrypted)…[/dim]")
    for field_name, value in collected.items():
        store.set_secret(connector_id, field_name, value)
    store.save()

    config = load_connectors_config(config_path)
    connectors = config.setdefault("connectors", {})
    entry = connectors.setdefault(connector_id, {})
    entry["enabled"] = True
    entry.setdefault("operations", {})
    connector = registry.get_connector(connector_id)
    if connector is not None:
        for spec in connector.operations():
            entry["operations"].setdefault(spec.name, True)
    save_connectors_config(config, config_path)

    console.print("[dim]Validating connection…[/dim]")
    settings = load_config()
    registry2 = ConnectorRegistry(settings, config_path=config_path)
    connector2 = registry2.get_connector(connector_id)
    if connector2 is not None:
        try:
            result = await connector2.health()
            if result.success:
                console.print(
                    f"[green]✓ {info.get('name', connector_id)}: Connected successfully[/green]"
                )
            else:
                console.print(
                    f"[yellow]⚠ {info.get('name', connector_id)}: {result.error or 'Health check failed'}[/yellow]"
                )
                console.print(
                    "[dim]Credentials saved. Fix the issue and re-run /connect to retry.[/dim]"
                )
        except Exception as exc:
            console.print(f"[yellow]⚠ Validation error: {exc}[/yellow]")
            console.print(
                "[dim]Credentials saved. Fix the issue and re-run /connect to retry.[/dim]"
            )

    return True, f"{info.get('name', connector_id)} configured and enabled."
