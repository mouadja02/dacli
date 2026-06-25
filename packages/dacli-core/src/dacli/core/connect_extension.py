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
    """Interactive ``/connect``: pick an extension, prompt its fields, store them."""
    if extension is None:
        extension = _pick_extension(console, registry.extension_ids())
        if extension is None:
            return False, "No extension selected."

    console.print(f"[dim]Enter config for {extension} (secrets stay hidden)…[/dim]")
    ok, message = connect_extension(
        registry, store, extension, prompt=_rich_prompt()
    )
    style = "green" if ok else "yellow"
    console.print(f"[{style}]{message}[/{style}]")
    return ok, message
