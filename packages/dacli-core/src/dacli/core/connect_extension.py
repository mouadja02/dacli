"""``/connect <extension>`` — collect an extension's credentials (M07).

Reads an extension's ``config_field`` declarations off the live registry, prompts
for each (secrets entered hidden), and writes them to a :class:`SecretStore` —
encrypted where the field is a secret. This is the one connect path now; the old
connector flow went with the fleet (M11/M12).

:func:`connect_extension` is the testable core (prompting injected); the console
driver builds a rich prompter and the extension picker around it.
"""

from __future__ import annotations

from typing import Any, Protocol
from collections.abc import Callable

from dacli.connectors.registry import ConfigField
from dacli.core.secrets import SecretStore


class _Registry(Protocol):
    def config_fields(self, extension: str) -> list[ConfigField]: ...


def connect_extension(
    registry: _Registry,
    store: SecretStore,
    extension: str,
    *,
    prompt: Callable[[ConfigField], str | None],
) -> tuple[bool, str]:
    """Collect and store one extension's config. Returns ``(ok, message)``.

    ``prompt`` is called once per field and returns the entered value, or None to
    skip it. Nothing is saved when no value is provided, so a cancelled flow
    leaves the store untouched.
    """
    fields = registry.config_fields(extension)
    if not fields:
        return False, f"'{extension}' has nothing to configure."

    stored = 0
    for field in fields:
        value = prompt(field)
        if value:
            store.set(extension, field.name, value, secret=field.is_secret)
            stored += 1

    if not stored:
        return False, "No values provided."
    store.save()
    return True, f"{extension} configured."


# ---------------------------------------------------------------------------
# Console driver
# ---------------------------------------------------------------------------
def _rich_prompt() -> Callable[[ConfigField], str | None]:
    from rich.prompt import Prompt

    def prompt(field: ConfigField) -> str | None:
        label = field.name.replace("_", " ").title()
        hint = f" ({field.description})" if field.description else ""
        tag = "required" if field.required else "optional, Enter to skip"
        val = Prompt.ask(
            f"  {label}{hint} [dim]({tag})[/dim]",
            password=field.is_secret,
            default="",
        )
        return val or None

    return prompt


def _pick_extension(console: Any, ids: list[str]) -> str | None:
    from rich.prompt import Prompt

    if not ids:
        console.print("[dim]No extensions loaded.[/dim]")
        return None
    for i, ext in enumerate(ids, 1):
        console.print(f"  {i}. {ext}")
    choice = Prompt.ask("Extension number or name", default="")
    if not choice:
        return None
    if choice.isdigit() and 1 <= int(choice) <= len(ids):
        return ids[int(choice) - 1]
    return choice if choice in ids else None


def run_connect_extension_flow(
    console: Any,
    registry: Any,
    store: SecretStore,
    extension: str | None = None,
) -> tuple[bool, str]:
    """Interactive ``/connect``: pick an extension, prompt its fields, store them.

    If the given extension isn't loaded, suggests ``/new-extension`` or generation.
    """
    known_ids = registry.extension_ids()

    if extension is None:
        extension = _pick_extension(console, known_ids)
        if extension is None:
            return False, "No extension selected."

    if extension not in known_ids:
        console.print(
            f"[yellow]'{extension}' is not a loaded extension.[/yellow]\n"
            f"[dim]Run /new-extension to generate one, or check the name.[/dim]"
        )
        return False, f"Unknown extension '{extension}'."

    console.print(f"[dim]Enter config for {extension} (secrets stay hidden)…[/dim]")
    ok, message = connect_extension(registry, store, extension, prompt=_rich_prompt())
    style = "green" if ok else "yellow"
    console.print(f"[{style}]{message}[/{style}]")

    # After creds, offer scope change so users don't have to edit policy.yaml.
    if ok:
        _prompt_scope(console, extension)

    return ok, message


# ---------------------------------------------------------------------------
# Scope prompting (writes to the project-local or user-level policy.yaml)
# ---------------------------------------------------------------------------

_SCOPE_CHOICES = ("read_only", "write", "risky", "admin")


def _current_scope(connector_id: str) -> str:
    """Read the effective scope for a connector from the resolved policy."""
    from dacli.core import paths
    from dacli.governance.policy_engine import load_policy_config

    policy_path = paths.resolve_policy_path(None)
    config = load_policy_config(str(policy_path))
    connectors = getattr(config, "connectors", {}) or {}
    block = connectors.get(connector_id, {})
    if isinstance(block, dict):
        return block.get("scope", "read_only")
    return "read_only"


def _write_scope(connector_id: str, scope: str) -> None:
    """Set the connector scope in the project-level config/policy.yaml."""
    from pathlib import Path

    import yaml

    from dacli.core import paths

    # Write to project-level policy; create if absent.
    root = paths.project_root()
    if root is not None:
        target = root / "config" / "policy.yaml"
    else:
        target = paths.user_config_dir() / "policy.yaml"

    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists():
        data = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
    else:
        data = {}

    connectors = data.setdefault("connectors", {})
    block = connectors.setdefault(connector_id, {})
    if not isinstance(block, dict):
        block = {}
        connectors[connector_id] = block
    block["scope"] = scope

    target.write_text(yaml.dump(data, default_flow_style=False), encoding="utf-8")


def _prompt_scope(console: Any, connector_id: str) -> None:
    """Show current scope and offer to change it."""
    from rich.prompt import Prompt

    current = _current_scope(connector_id)
    console.print(
        f"  [dim]Permission scope:[/dim] [bold]{current}[/bold] "
        f"[dim](read_only | write | risky | admin)[/dim]"
    )
    choice = Prompt.ask(
        "  Change scope? Enter new value or press Enter to keep",
        default="",
    )
    if not choice or choice.strip() == current:
        return
    choice = choice.strip().lower()
    if choice not in _SCOPE_CHOICES:
        console.print(
            f"  [yellow]Unknown scope '{choice}', keeping {current}.[/yellow]"
        )
        return
    _write_scope(connector_id, choice)
    console.print(f"  [green]Scope set to {choice}.[/green]")
