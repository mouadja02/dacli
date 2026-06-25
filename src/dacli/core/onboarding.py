"""First-run onboarding — conversational, not a fixed wizard (M12).

The fixed setup wizard and per-connector selection went with M11/M12. Onboarding
is now two small steps: configure the LLM if it isn't, then guide the user to a
first working connection through the *same* ``/connect`` path they'd use later.
There is no connector-selection step — the seeds are always present; a connector
is "set up" the moment the user connects it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from rich.prompt import Confirm, Prompt


def collect_llm_credentials(console: Any, settings: Any, *, store_base_dir: str) -> Any:
    """Prompt for provider/model/api_key when the LLM is unconfigured; persist + return reloaded settings.

    The API key goes to the encrypted store (.dacli/dacli.json) only; the
    non-secret provider/model/base_url are written to config.yaml so they
    persist across runs.
    """
    import yaml

    from dacli.config.settings import load_config
    from dacli.core.store import DacliStore

    console.print(
        "[warning]No LLM is configured yet.[/warning] "
        "Let's set it up (the key is stored encrypted in .dacli/dacli.json)."
    )
    known_providers = ["openai", "anthropic", "openrouter"]
    default_provider = (
        settings.llm.provider if settings.llm.provider in known_providers else "openai"
    )
    provider = Prompt.ask(
        "LLM provider", choices=known_providers, default=default_provider, console=console
    )
    default_base = {
        "openai": "https://api.openai.com/v1",
        "anthropic": "https://api.anthropic.com",
        "openrouter": "https://openrouter.ai/api/v1",
    }[provider]
    model = Prompt.ask(
        "Model id (e.g. gpt-4o-mini, claude-3-5-sonnet-latest)", console=console
    )
    api_key = Prompt.ask("API key", password=True, console=console)
    base_url = Prompt.ask("Base URL", default=default_base, console=console)

    store = DacliStore(base_dir=store_base_dir)
    store.set_secret("llm", "api_key", api_key)
    store.save()
    # Provider/model/base_url aren't secrets; persist them to config.yaml so
    # the next run loads a configured LLM without re-prompting.
    cfg_path = Path("config.yaml")
    existing = {}
    if cfg_path.exists():
        existing = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    existing.setdefault("llm", {})
    existing["llm"].update({"provider": provider, "model": model, "base_url": base_url})
    cfg_path.write_text(yaml.dump(existing, default_flow_style=False), encoding="utf-8")
    return load_config(str(cfg_path))


def run_first_connection(ui: Any, console: Any, ext_registry: Any, secrets: Any) -> None:
    """Guide a new user to a first connection. Optional and skippable.

    Lists the bundled extensions and offers to ``/connect`` one now; declining
    leaves an empty ``~/.dacli`` untouched — the user can connect later, or just
    start talking and let the agent walk them through it.
    """
    ids = sorted(ext_registry.extension_ids())
    if not ids:
        return
    ui.panel(
        "I'm ready. Bundled connections: "
        + ", ".join(ids)
        + ".\nConfigure one now, or just ask — I can guide you. "
        "Generate more with /new-extension.",
        title="[accent]Getting started[/accent]",
    )
    if not Confirm.ask("Connect one now?", console=console, default=True):
        ui.notice("You can connect anytime with /connect.", style="muted")
        return

    from dacli.core.connect_extension import run_connect_extension_flow

    run_connect_extension_flow(console, ext_registry, secrets)
