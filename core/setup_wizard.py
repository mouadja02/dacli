# Setup Wizard - Interactive CLI for configuring DACLI tools
from typing import Dict, Optional, Tuple, Any
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Confirm
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.markdown import Markdown
from rich import box

from config.tool_registry import ToolCategory, ToolConfig, ToolsSettings, TOOL_CATALOG


class SetupWizard:
    """
    Interactive setup wizard for DACLI tool configuration.

    Guides users through:
    1. Selecting which tool categories to enable
    2. Choosing specific operations within each category
    3. Validating credentials for enabled tools
    4. Saving configuration to YAML
    """

    def __init__(self, settings: Any, config_path: str = "config.yaml"):
        self.settings = settings
        self.config_path = config_path
        self.console = Console()
        self.tools_settings = ToolsSettings()

        # Try to load existing tools settings if present
        if hasattr(settings, "tools") and settings.tools:
            self.tools_settings = settings.tools

    def print_welcome(self):
        """Print welcome banner"""
        welcome_text = """
# 🚀 Welcome to DACLI Setup Wizard

This wizard will help you configure which tools and capabilities 
you want to use with your agent.

You can always re-run this wizard later by using: `dacli --setup`
        """
        self.console.print(
            Panel(
                Markdown(welcome_text),
                title="[bold blue]DACLI - Data Agent CLI[/bold blue]",
                border_style="blue",
                box=box.DOUBLE,
            )
        )

    def needs_setup(self) -> bool:
        """Check if setup wizard should run"""
        return not self.tools_settings.setup_completed

    async def run(self) -> ToolsSettings:
        """Run the complete setup wizard"""
        self.print_welcome()
        self.console.print()

        # Step 1: Tool category selection
        self.console.print("[bold cyan]Step 1/3:[/bold cyan] Select Tool Categories\n")
        await self._select_tool_categories()

        # Step 2: Operation selection for each enabled tool
        self.console.print("\n[bold cyan]Step 2/3:[/bold cyan] Select Operations\n")
        await self._select_operations()

        # Step 3: Validate credentials
        self.console.print(
            "\n[bold cyan]Step 3/3:[/bold cyan] Validating Credentials\n"
        )
        validation_results = await self._validate_credentials()

        # Show summary
        self._show_summary(validation_results)

        # Mark setup as completed
        self.tools_settings.setup_completed = True

        return self.tools_settings

    async def _select_tool_categories(self):
        """Interactive tool category selection"""
        categories_table = Table(
            title="Available Tool Categories",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold magenta",
        )
        categories_table.add_column("#", style="dim", width=3)
        categories_table.add_column("Tool", style="cyan")
        categories_table.add_column("Description")
        categories_table.add_column("Operations", justify="center")

        for idx, (category, info) in enumerate(TOOL_CATALOG.items(), 1):
            categories_table.add_row(
                str(idx),
                f"{info['icon']} {info['name']}",
                info["description"],
                str(len(info["operations"])),
            )

        self.console.print(categories_table)
        self.console.print()

        # Ask about each tool category
        for category in ToolCategory:
            info = TOOL_CATALOG[category]
            enabled = Confirm.ask(
                f"Enable {info['icon']} [bold]{info['name']}[/bold]?", default=True
            )

            config = ToolConfig(enabled=enabled)
            if enabled:
                # Enable all operations by default
                config.operations = {op: True for op in info["operations"].keys()}

            self.tools_settings.set_tool_config(category, config)

    async def _select_operations(self):
        """Let user select specific operations for enabled tools"""
        enabled_categories = self.tools_settings.get_enabled_tools()

        if not enabled_categories:
            self.console.print(
                "[yellow]No tools enabled. You can re-run setup later.[/yellow]"
            )
            return

        for category in enabled_categories:
            info = TOOL_CATALOG[category]
            self.console.print(
                f"\n{info['icon']} [bold cyan]{info['name']} Operations:[/bold cyan]"
            )

            # Show operations table
            ops_table = Table(box=box.SIMPLE, show_header=True)
            ops_table.add_column("#", style="dim", width=3)
            ops_table.add_column("Operation", style="green")
            ops_table.add_column("Description")
            ops_table.add_column("Category", style="dim")

            operations = list(info["operations"].items())
            for idx, (op_name, op_info) in enumerate(operations, 1):
                ops_table.add_row(
                    str(idx),
                    op_info["name"],
                    op_info["description"],
                    op_info["category"],
                )

            self.console.print(ops_table)

            # Ask if user wants to customize
            customize = Confirm.ask(
                "Would you like to customize which operations to enable?", default=False
            )

            config = self.tools_settings.get_tool_config(category)

            if customize:
                new_operations = {}
                for op_name, op_info in operations:
                    enabled = Confirm.ask(
                        f"  Enable [green]{op_info['name']}[/green]?", default=True
                    )
                    new_operations[op_name] = enabled
                config.operations = new_operations
            else:
                # Keep all enabled
                config.operations = {op: True for op in info["operations"].keys()}

            self.tools_settings.set_tool_config(category, config)

    async def _validate_credentials(self) -> Dict[ToolCategory, Tuple[bool, str]]:
        """Validate credentials for enabled tools"""
        results: Dict[ToolCategory, Tuple[bool, str]] = {}
        enabled_categories = self.tools_settings.get_enabled_tools()

        if not enabled_categories:
            return results

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console,
        ) as progress:
            for category in enabled_categories:
                info = TOOL_CATALOG[category]
                task = progress.add_task(
                    f"Validating {info['icon']} {info['name']}...", total=None
                )

                try:
                    success, message = await self._validate_tool(category)
                    results[category] = (success, message)
                except Exception as e:
                    results[category] = (False, str(e))

                progress.remove_task(task)

        return results

    async def _validate_tool(self, category: ToolCategory) -> Tuple[bool, str]:
        """Validate a specific tool's credentials"""
        try:
            if category == ToolCategory.SNOWFLAKE:
                return await self._validate_snowflake()
            elif category == ToolCategory.GITHUB:
                return await self._validate_github()
            elif category == ToolCategory.PINECONE:
                return await self._validate_pinecone()
        except Exception as e:
            return (False, f"Validation error: {str(e)}")

        return (True, "No validation implemented")

    async def _validate_snowflake(self) -> Tuple[bool, str]:
        """Validate Snowflake credentials"""
        if not hasattr(self.settings, "snowflake"):
            return (False, "Snowflake configuration missing in config.yaml")

        sf = self.settings.snowflake

        # Check required fields
        missing = []
        for field in ["account", "user", "password", "warehouse", "database"]:
            value = getattr(sf, field, None)
            if not value or value == "" or value.startswith("${"):
                missing.append(field)

        if missing:
            return (False, f"Missing config: {', '.join(missing)}")

        # Try actual connection
        try:
            from tools.snowflake_tools import SnowflakeTool

            tool = SnowflakeTool(self.settings)
            result = await tool.validate()
            if result.success:
                return (True, "Connected successfully")
            else:
                return (False, result.error or "Connection failed")
        except ImportError:
            return (True, "Config appears valid (connection test skipped)")
        except Exception as e:
            return (False, f"Connection failed: {str(e)}")

    async def _validate_github(self) -> Tuple[bool, str]:
        """Validate GitHub credentials"""
        if not hasattr(self.settings, "github"):
            return (False, "GitHub configuration missing in config.yaml")

        gh = self.settings.github

        # Check required fields
        missing = []
        for field in ["token", "owner", "repo"]:
            value = getattr(gh, field, None)
            if not value or value == "" or value.startswith("${"):
                missing.append(field)

        if missing:
            return (False, f"Missing config: {', '.join(missing)}")

        # Try actual connection
        try:
            from tools.github_tools import GithubTool

            tool = GithubTool(self.settings)
            result = await tool.validate()
            if result.success:
                return (True, f"Connected to {gh.owner}/{gh.repo}")
            else:
                return (False, result.error or "Connection failed")
        except ImportError:
            return (True, "Config appears valid (connection test skipped)")
        except Exception as e:
            return (False, f"Connection failed: {str(e)}")

    async def _validate_pinecone(self) -> Tuple[bool, str]:
        """Validate Pinecone credentials"""
        if not hasattr(self.settings, "pinecone"):
            return (False, "Pinecone configuration missing in config.yaml")

        pc = self.settings.pinecone

        # Check required fields
        missing = []
        for field in ["api_key", "index_name"]:
            value = getattr(pc, field, None)
            if not value or value == "" or value.startswith("${"):
                missing.append(field)

        if missing:
            return (False, f"Missing config: {', '.join(missing)}")

        # Check embeddings config too
        if hasattr(self.settings, "embeddings"):
            emb = self.settings.embeddings
            if not getattr(emb, "api_key", None) or emb.api_key.startswith("${"):
                return (False, "Missing embeddings API key")

        # Try actual connection
        try:
            from tools.pinecone_tools import PineconeTool

            tool = PineconeTool(self.settings)
            result = await tool.validate()
            if result.success:
                return (True, f"Connected to index: {pc.index_name}")
            else:
                return (False, result.error or "Connection failed")
        except ImportError:
            return (True, "Config appears valid (connection test skipped)")
        except Exception as e:
            return (False, f"Connection failed: {str(e)}")

    def _show_summary(self, validation_results: Dict[ToolCategory, Tuple[bool, str]]):
        """Show configuration summary"""
        self.console.print("\n")

        # Summary table
        summary_table = Table(
            title="📋 Configuration Summary",
            box=box.DOUBLE,
            show_header=True,
            header_style="bold white on blue",
        )
        summary_table.add_column("Tool", style="cyan")
        summary_table.add_column("Status", justify="center")
        summary_table.add_column("Operations", justify="center")
        summary_table.add_column("Validation")

        for category in ToolCategory:
            info = TOOL_CATALOG[category]
            config = self.tools_settings.get_tool_config(category)

            if config.enabled:
                status = "[green]✅ Enabled[/green]"
                ops_count = len(config.get_enabled_operations())

                # Validation result
                if category in validation_results:
                    success, message = validation_results[category]
                    if success:
                        validation = f"[green]✓ {message}[/green]"
                    else:
                        validation = f"[red]✗ {message}[/red]"
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
                validation,
            )

        self.console.print(summary_table)

        # Warnings for failed validations
        warnings = []
        for category, (success, message) in validation_results.items():
            if not success:
                info = TOOL_CATALOG[category]
                warnings.append(f"⚠️  {info['name']}: {message}")

        if warnings:
            self.console.print("\n[bold yellow]Warnings:[/bold yellow]")
            for warning in warnings:
                self.console.print(f"  {warning}")
            self.console.print(
                "\n[dim]Tools with validation errors will be skipped during agent initialization.[/dim]"
            )

        self.console.print(
            "\n[green]✓ Setup complete![/green] Your preferences will be saved.\n"
        )


class QuickSetup:
    """
    Quick setup for common profiles without going through the full wizard.
    """

    PROFILES: Dict[str, Any] = {
        "full": {
            "name": "Full Stack",
            "description": "All tools enabled (Snowflake + GitHub + Pinecone)",
            "config": lambda: ToolsSettings(
                setup_completed=True,
                snowflake=ToolConfig(
                    enabled=True,
                    operations={
                        op: True
                        for op in TOOL_CATALOG[ToolCategory.SNOWFLAKE]["operations"]
                    },
                ),
                github=ToolConfig(
                    enabled=True,
                    operations={
                        op: True
                        for op in TOOL_CATALOG[ToolCategory.GITHUB]["operations"]
                    },
                ),
                pinecone=ToolConfig(
                    enabled=True,
                    operations={
                        op: True
                        for op in TOOL_CATALOG[ToolCategory.PINECONE]["operations"]
                    },
                ),
            ),
        },
        "github_only": {
            "name": "GitHub Only",
            "description": "Only GitHub tools (file management, workflows)",
            "config": lambda: ToolsSettings(
                setup_completed=True,
                snowflake=ToolConfig(enabled=False),
                github=ToolConfig(
                    enabled=True,
                    operations={
                        op: True
                        for op in TOOL_CATALOG[ToolCategory.GITHUB]["operations"]
                    },
                ),
                pinecone=ToolConfig(enabled=False),
            ),
        },
        "snowflake_only": {
            "name": "Snowflake Only",
            "description": "Only Snowflake tools (SQL queries)",
            "config": lambda: ToolsSettings(
                setup_completed=True,
                snowflake=ToolConfig(
                    enabled=True,
                    operations={
                        op: True
                        for op in TOOL_CATALOG[ToolCategory.SNOWFLAKE]["operations"]
                    },
                ),
                github=ToolConfig(enabled=False),
                pinecone=ToolConfig(enabled=False),
            ),
        },
        "datawarehouse": {
            "name": "Data Warehouse Builder",
            "description": "Snowflake + GitHub (for dbt projects)",
            "config": lambda: ToolsSettings(
                setup_completed=True,
                snowflake=ToolConfig(
                    enabled=True,
                    operations={
                        op: True
                        for op in TOOL_CATALOG[ToolCategory.SNOWFLAKE]["operations"]
                    },
                ),
                github=ToolConfig(
                    enabled=True,
                    operations={
                        op: True
                        for op in TOOL_CATALOG[ToolCategory.GITHUB]["operations"]
                    },
                ),
                pinecone=ToolConfig(enabled=False),
            ),
        },
    }

    @classmethod
    def list_profiles(cls) -> Dict[str, Dict]:
        """List all available quick profiles"""
        return {
            name: {"name": p["name"], "description": p["description"]}
            for name, p in cls.PROFILES.items()
        }

    @classmethod
    def get_profile(cls, profile_name: str) -> Optional[ToolsSettings]:
        """Get a specific profile configuration"""
        if profile_name in cls.PROFILES:
            return cls.PROFILES[profile_name]["config"]()
        return None

    @classmethod
    def show_profiles(cls, console: Console):
        """Display available profiles"""
        table = Table(title="Quick Setup Profiles", box=box.ROUNDED, show_header=True)
        table.add_column("Profile", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Description")

        for key, profile in cls.PROFILES.items():
            table.add_row(key, profile["name"], profile["description"])

        console.print(table)
