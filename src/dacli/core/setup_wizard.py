# Setup Wizard - Interactive CLI for configuring DACLI connectors.
#
# Fully manifest/registry-driven: it knows nothing about specific platforms. It
# iterates over whatever connectors the ConnectorRegistry discovered and writes
# the user's selections to config/connectors.yaml.
from pathlib import Path
from typing import Any, ClassVar
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Confirm, Prompt
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.markdown import Markdown
from rich import box

from dacli.connectors.registry import ConnectorRegistry, CONNECTORS_CONFIG_PATH


class SetupWizard:
    """
    Interactive setup wizard for DACLI connector configuration.

    Guides users through:
    1. Selecting which connectors to enable
    2. Choosing specific operations within each connector
    3. Validating credentials for enabled connectors
    4. Producing a config/connectors.yaml document
    """

    def __init__(self, settings: Any, registry: ConnectorRegistry, config_path: str = CONNECTORS_CONFIG_PATH):
        self.settings = settings
        self.registry = registry
        self.config_path = config_path
        self.console = Console()
        self.catalog = registry.get_catalog()
        # {connector_id: {"enabled": bool, "operations": {op: bool}}}
        self.selections: dict[str, dict[str, Any]] = {}

    def print_welcome(self):
        """Print welcome banner"""
        welcome_text = """
# 🚀 Welcome to DACLI Setup Wizard

This wizard will help you configure which connectors and capabilities
you want to use with your agent.

You can always re-run this wizard later by using: `dacli --setup`
        """
        self.console.print(Panel(
            Markdown(welcome_text),
            title="[bold blue]DACLI - Data Agent CLI[/bold blue]",
            border_style="blue",
            box=box.DOUBLE
        ))

    def needs_setup(self) -> bool:
        """Check if setup wizard should run"""
        return not self.registry.setup_completed

    async def run(self) -> dict[str, Any]:
        """Run the complete setup wizard. Returns a connectors-config dict.

        Leads with the quick profiles (one question, immediate value); the
        granular per-connector / per-operation flow is the "custom" path.
        """
        self.print_welcome()
        self.console.print()

        if Confirm.ask(
            "Quick start with a profile? (recommended — choose 'n' to customize)",
            default=True,
        ) and self._apply_quick_profile():
            self.console.print("\n[bold cyan]Validating Credentials[/bold cyan]\n")
            self._collect_secrets()
            validation_results = await self._validate_credentials()
            self._show_summary(validation_results)
            return {"setup_completed": True, "connectors": self.selections}

        # Step 1: Connector selection
        self.console.print("[bold cyan]Step 1/3:[/bold cyan] Select Connectors\n")
        self._select_connectors()

        # Step 2: Operation selection for each enabled connector
        self.console.print("\n[bold cyan]Step 2/3:[/bold cyan] Select Operations\n")
        self._select_operations()

        # Step 3: Collect any missing secrets, then validate credentials
        self.console.print("\n[bold cyan]Step 3/3:[/bold cyan] Validating Credentials\n")
        self._collect_secrets()
        validation_results = await self._validate_credentials()

        # Show summary
        self._show_summary(validation_results)

        return {"setup_completed": True, "connectors": self.selections}

    def _apply_quick_profile(self) -> bool:
        """Pick a QuickSetup profile and adopt its selections.

        Returns True when a profile was applied; False (fall through to the
        custom flow) when the chosen profile is unknown.
        """
        QuickSetup.show_profiles(self.console, self.registry)
        choices = list(QuickSetup.list_profiles(self.registry).keys())
        name = Prompt.ask("Profile", choices=choices, default="full")
        config = QuickSetup.get_profile(name, self.registry)
        if config is None:
            self.console.print(f"[yellow]Unknown profile: {name}[/yellow]")
            return False
        self.selections = config["connectors"]
        self.console.print(f"[green]✓ Applied profile: {name}[/green]")
        return True

    def _select_connectors(self):
        """Interactive connector selection"""
        table = Table(
            title="Available Connectors",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold magenta"
        )
        table.add_column("#", style="dim", width=3)
        table.add_column("Connector", style="cyan")
        table.add_column("Description")
        table.add_column("Operations", justify="center")

        for idx, (_connector_id, info) in enumerate(self.catalog.items(), 1):
            table.add_row(
                str(idx),
                f"{info['icon']} {info['name']}",
                info['description'],
                str(len(info['operations']))
            )

        self.console.print(table)
        self.console.print()

        for connector_id, info in self.catalog.items():
            enabled = Confirm.ask(
                f"Enable {info['icon']} [bold]{info['name']}[/bold]?",
                default=True
            )
            operations = dict.fromkeys(info['operations'], True) if enabled else {}
            self.selections[connector_id] = {"enabled": enabled, "operations": operations}

    def _select_operations(self):
        """Let user select specific operations for enabled connectors"""
        enabled_ids = [cid for cid, sel in self.selections.items() if sel["enabled"]]

        if not enabled_ids:
            self.console.print("[yellow]No connectors enabled. You can re-run setup later.[/yellow]")
            return

        for connector_id in enabled_ids:
            info = self.catalog[connector_id]
            self.console.print(f"\n{info['icon']} [bold cyan]{info['name']} Operations:[/bold cyan]")

            ops_table = Table(box=box.SIMPLE, show_header=True)
            ops_table.add_column("#", style="dim", width=3)
            ops_table.add_column("Operation", style="green")
            ops_table.add_column("Description")
            ops_table.add_column("Category", style="dim")

            operations = list(info['operations'].items())
            for idx, (_op_name, op_info) in enumerate(operations, 1):
                ops_table.add_row(
                    str(idx),
                    op_info['name'],
                    op_info['description'],
                    op_info['category']
                )

            self.console.print(ops_table)

            customize = Confirm.ask(
                "Would you like to customize which operations to enable?",
                default=False
            )

            if customize:
                new_operations = {}
                for op_name, op_info in operations:
                    op_enabled = Confirm.ask(
                        f"  Enable [green]{op_info['name']}[/green]?",
                        default=True
                    )
                    new_operations[op_name] = op_enabled
                self.selections[connector_id]["operations"] = new_operations
            else:
                self.selections[connector_id]["operations"] = dict.fromkeys(info['operations'], True)

    # Secrets to collect per connector id: (settings section, field, label).
    # The LLM key is always required (not tied to an optional connector).
    _ALWAYS_SECRETS: ClassVar[list[tuple[str, str, str]]] = [("llm", "api_key", "LLM API key")]
    _SECRET_FIELDS: ClassVar[dict[str, list[tuple[str, str, str]]]] = {
        "snowflake": [("snowflake", "password", "Snowflake password")],
        "github": [("github", "token", "GitHub personal access token")],
        "pinecone": [
            ("pinecone", "api_key", "Pinecone API key"),
            ("embeddings", "api_key", "Embeddings API key"),
        ],
    }

    def _current_secret(self, section: str, field: str):
        sec = getattr(self.settings, section, None)
        return getattr(sec, field, None) if sec is not None else None

    @staticmethod
    def _is_missing(value) -> bool:
        return not value or value == "" or (isinstance(value, str) and value.startswith("${"))

    def _collect_secrets(self):
        """Prompt for any missing secret of an enabled connector and persist it.

        Secrets are written to ``.dacli/dacli.json`` (gitignored) and the config
        is reloaded so the credentials resolve for validation and at runtime —
        no ``.env`` required.
        """
        from dacli.core.store import DacliStore
        from dacli.config.settings import load_config

        enabled_ids = [cid for cid, sel in self.selections.items() if sel.get("enabled")]
        candidates = list(self._ALWAYS_SECRETS)
        for cid in enabled_ids:
            candidates.extend(self._SECRET_FIELDS.get(cid, []))
        # Only prompt for what's actually missing (a value from .env/config wins).
        needed = [
            (section, field, label)
            for (section, field, label) in candidates
            if self._is_missing(self._current_secret(section, field))
        ]
        if not needed:
            return

        self.console.print(
            "[bold]Some credentials are missing.[/bold] They will be stored in "
            "[cyan].dacli/dacli.json[/cyan] (gitignored). Press Enter to skip any.\n"
        )
        state_path = getattr(getattr(self.settings, "agent", None), "state_path", ".dacli/state/")
        store = DacliStore(base_dir=str(Path(state_path).parent))
        changed = False
        for section, field, label in needed:
            value = Prompt.ask(f"  Enter {label}", password=True, default="")
            if value:
                store.set_secret(section, field, value)
                changed = True

        if changed:
            store.save()
            # Reload settings so the just-saved secrets are overlaid for validation.
            self.settings = load_config()
            self.console.print("[green]✓ Credentials saved.[/green]\n")

    async def _validate_credentials(self) -> dict[str, tuple[bool, str]]:
        """Validate credentials for enabled connectors"""
        results: dict[str, tuple[bool, str]] = {}
        enabled_ids = [cid for cid, sel in self.selections.items() if sel["enabled"]]

        if not enabled_ids:
            return results

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console
        ) as progress:
            for connector_id in enabled_ids:
                info = self.catalog[connector_id]
                task = progress.add_task(
                    f"Validating {info['icon']} {info['name']}...",
                    total=None
                )
                try:
                    results[connector_id] = await self._validate_connector(connector_id)
                except Exception as e:
                    results[connector_id] = (False, str(e))
                progress.remove_task(task)

        return results

    async def _validate_connector(self, connector_id: str) -> tuple[bool, str]:
        """Validate a connector: required config present, then a live health check."""
        info = self.catalog[connector_id]

        # Check required config fields on the matching settings section.
        section = getattr(self.settings, connector_id, None)
        if section is None:
            return (False, f"{info['name']} configuration missing in config.yaml")

        missing = []
        for field in info.get("required_config", []):
            value = getattr(section, field, None)
            if not value or (isinstance(value, str) and (value == "" or value.startswith("${"))):
                missing.append(field)

        if missing:
            return (False, f"Missing config: {', '.join(missing)}")

        # Live connection / health check.
        connector = self.registry.get_connector(connector_id)
        if connector is None:
            return (True, "Config appears valid (connection test skipped)")
        try:
            result = await connector.health()
            if result.success:
                return (True, "Connected successfully")
            return (False, result.error or "Connection failed")
        except Exception as e:
            return (False, f"Connection failed: {e!s}")

    def _show_summary(self, validation_results: dict[str, tuple[bool, str]]):
        """Show configuration summary"""
        self.console.print("\n")

        summary_table = Table(
            title="📋 Configuration Summary",
            box=box.DOUBLE,
            show_header=True,
            header_style="bold white on blue"
        )
        summary_table.add_column("Connector", style="cyan")
        summary_table.add_column("Status", justify="center")
        summary_table.add_column("Operations", justify="center")
        summary_table.add_column("Validation")

        for connector_id, info in self.catalog.items():
            sel = self.selections.get(connector_id, {"enabled": False, "operations": {}})

            if sel["enabled"]:
                status = "[green]✅ Enabled[/green]"
                ops_count = sum(1 for v in sel["operations"].values() if v)

                if connector_id in validation_results:
                    success, message = validation_results[connector_id]
                    validation = f"[green]✓ {message}[/green]" if success else f"[red]✗ {message}[/red]"
                else:
                    validation = "[dim]Not validated[/dim]"
            else:
                status = "[dim]⊘ Disabled[/dim]"
                ops_count = 0
                validation = "[dim]—[/dim]"

            summary_table.add_row(
                f"{info['icon']} {info['name']}",
                status,
                str(ops_count) if ops_count > 0 else "—",
                validation
            )

        self.console.print(summary_table)

        warnings = []
        for connector_id, (success, message) in validation_results.items():
            if not success:
                info = self.catalog[connector_id]
                warnings.append(f"⚠️  {info['name']}: {message}")

        if warnings:
            self.console.print("\n[bold yellow]Warnings:[/bold yellow]")
            for warning in warnings:
                self.console.print(f"  {warning}")
            self.console.print(
                "\n[dim]Connectors with validation errors will be skipped during agent initialization.[/dim]"
            )

        self.console.print("\n[green]✓ Setup complete![/green] Your preferences will be saved.\n")


class QuickSetup:
    """
    Quick setup for common profiles without going through the full wizard.

    Profiles are derived from whatever the registry discovered:
    - ``full``   : enable every connector and all its operations
    - ``none``   : disable everything
    - ``<id>_only`` : enable only that connector (e.g. ``<connector_id>_only``)
    """

    @staticmethod
    def _all_enabled(registry: ConnectorRegistry, only: str | None = None) -> dict[str, Any]:
        catalog = registry.get_catalog()
        connectors: dict[str, Any] = {}
        for connector_id, info in catalog.items():
            enabled = (only is None) or (connector_id == only)
            connectors[connector_id] = {
                "enabled": enabled,
                "operations": dict.fromkeys(info["operations"], enabled),
            }
        return {"setup_completed": True, "connectors": connectors}

    @classmethod
    def list_profiles(cls, registry: ConnectorRegistry) -> dict[str, dict[str, str]]:
        profiles = {
            "full": {"name": "Full Stack", "description": "All connectors enabled"},
            "none": {"name": "None", "description": "All connectors disabled"},
        }
        for connector_id, info in registry.get_catalog().items():
            profiles[f"{connector_id}_only"] = {
                "name": f"{info['name']} Only",
                "description": f"Only {info['name']} enabled",
            }
        return profiles

    @classmethod
    def get_profile(cls, profile_name: str, registry: ConnectorRegistry) -> dict[str, Any] | None:
        if profile_name == "full":
            return cls._all_enabled(registry)
        if profile_name == "none":
            return cls._all_enabled(registry, only="__none__")
        if profile_name.endswith("_only"):
            connector_id = profile_name[: -len("_only")]
            if connector_id in registry.get_catalog():
                return cls._all_enabled(registry, only=connector_id)
        return None

    @classmethod
    def show_profiles(cls, console: Console, registry: ConnectorRegistry):
        table = Table(title="Quick Setup Profiles", box=box.ROUNDED, show_header=True)
        table.add_column("Profile", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Description")
        for key, profile in cls.list_profiles(registry).items():
            table.add_row(key, profile["name"], profile["description"])
        console.print(table)


def collect_llm_credentials(console: Console, settings: Any, *, store_base_dir: str) -> Any:
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
