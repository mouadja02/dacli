import asyncio
import os
import sys
import click
from typing import Optional

from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown
from rich.table import Table
from rich.live import Live
from rich.spinner import Spinner
from rich.text import Text
from rich.syntax import Syntax
from rich.prompt import Prompt, Confirm
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.theme import Theme
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.styles import Style as PTStyle

from core import __author__,__version__
from config import CLI_COMMANDS
from config.settings import load_config, Settings, save_config, save_tools_config
from config.tool_registry import ToolsSettings, ToolRegistry, TOOL_CATALOG, ToolCategory
from tools import DACLI_tools, get_available_tools
from core.agent import DACLI
from core.memory import AgentMemory
from core.setup_wizard import SetupWizard, QuickSetup
from prompts.system_prompt import load_system_prompt, save_system_prompt, SYSTEM_PROMPT_FILE
from prompts.user_prompt import load_user_prompt, save_user_prompt, USER_PROMPT_FILE

# -----------------------------------------
#  CUSTOMIZE CONSOLE THEME
# -----------------------------------------
CUSTOM_THEME = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "success": "bold green",
    "prompt": "bold magenta",
    "tool": "blue",
    "sql": "green",
    "user": "bold white",
    "assistant": "cyan",
    "phase": "bold yellow",
    "step": "dim white",
})

console = Console(theme=CUSTOM_THEME)


# -----------------------------------------
#  UI components
# -----------------------------------------
def print_banner():
    banner_in_box = """
╔════════════════════════════════════════════════════════════════════╗
║                                                                    ║
║               ██████╗   █████╗   ██████╗ ██╗      ██╗              ║
║               ██╔══██╗ ██╔══██╗ ██╔════╝ ██║      ██║              ║
║               ██║  ██║ ███████║ ██║      ██║      ██║              ║
║               ██║  ██║ ██╔══██║ ██║      ██║      ██║              ║
║               ██████╔╝ ██║  ██║ ╚██████╗ ███████╗ ██║              ║
║               ╚═════╝  ╚═╝  ╚═╝  ╚═════╝ ╚══════╝ ╚═╝              ║
║              Your Autonomous Data Engineering CLI Agent            ║
║                                                                    ║
╚════════════════════════════════════════════════════════════════════╝    
"""
    console.print(banner_in_box, style="dim")
    console.print(f"Version: {__version__}", style="dim")
    console.print(f"Author: {__author__}", style="dim")

def print_help_commands():
    # Print available tolls
    table = Table(title="Available Tools", show_header=True, header_style="bold magenta")
    table.add_column("Tool Name", style="cyan")
    table.add_column("Description")

    for cmd, desc in CLI_COMMANDS:
        table.add_row(cmd, desc)

    console.print(table)


def print_status(memory: AgentMemory):
    # Print current agent status
    summary = memory.get_progress_summary()
    
    # Main status panel
    status_text = Text()
    status_text.append("Session: ", style="dim")
    status_text.append(f"{summary['session_id']}\n", style="cyan")
    status_text.append("Current Phase: ", style="dim")
    status_text.append(f"{summary['current_phase']}\n", style="phase")
    status_text.append("Infrastructure: ", style="dim")
    status_text.append(
        "✅ Ready" if summary['infrastructure_ready'] else "⏳ Pending",
        style="success" if summary['infrastructure_ready'] else "warning"
    )
    
    console.print(Panel(status_text, title="Agent Status", border_style="cyan"))
    
    # Progress table
    table = Table(title="Phase Progress", show_header=True)
    table.add_column("Phase", style="cyan")
    table.add_column("Status")
    table.add_column("Progress")
    
    for phase, info in summary.get('phases', {}).items():
        status = info.get('status', 'not_started')
        status_icon = {
            "not_started": "⬜",
            "in_progress": "🔄",
            "completed": "✅",
            "failed": "❌",
            "paused": "⏸️"
        }.get(status, "⬜")
        
        table.add_row(
            phase.replace("_", " ").title(),
            f"{status_icon} {status}",
            info.get('progress', '0/0')
        )
    
    console.print(table)
    
    # Stats
    stats_table = Table(show_header=False, box=None)
    stats_table.add_column("Metric", style="dim")
    stats_table.add_column("Value", style="cyan")
    
    stats_table.add_row("Schemas Created", str(len(summary.get('schemas_created', []))))
    stats_table.add_row("Tables Created", str(summary.get('tables_created', 0)))
    stats_table.add_row("Tables Loaded", str(summary.get('tables_loaded', 0)))
    stats_table.add_row("Total Rows", str(summary.get('total_rows_loaded', 0)))
    stats_table.add_row("Files Discovered", str(summary.get('files_discovered', 0)))
    stats_table.add_row("Errors", str(summary.get('errors_count', 0)))
    
    console.print(Panel(stats_table, title="Statistics", border_style="blue"))
    
    if summary.get('last_error'):
        console.print(f"[error]Last Error:[/error] {summary['last_error']}")



def format_sql(sql: str) -> Panel:
    # Format SQL for display.
    syntax = Syntax(sql, "sql", theme="monokai", line_numbers=True)
    return Panel(syntax, title="SQL Query", border_style="green")


def format_response(content: str) -> Panel:
    # Format agent response for display.
    md = Markdown(content)
    return Panel(md, title="🤖 Agent", border_style="cyan")


def format_tool_result(tool_name: str, result) -> Panel:
    # Format tool result for display.
    from tools.Base import ToolResult, ToolStatus
    
    if isinstance(result, ToolResult):
        if result.success:
            content = Text()
            content.append("✅ Success\n", style="success")
            content.append(f"Time: {result.execution_time_ms:.0f}ms\n", style="dim")
            
            if isinstance(result.data, list):
                content.append(f"Rows: {len(result.data)}\n", style="dim")
            elif isinstance(result.data, dict):
                for k, v in result.data.items():
                    content.append(f"{k}: {v}\n")
            else:
                content.append(str(result.data))
            
            return Panel(content, title=f"🔧 {tool_name}", border_style="green")
        else:
            content = Text()
            content.append("❌ Error\n", style="error")
            content.append(str(result.error), style="dim red")
            return Panel(content, title=f"🔧 {tool_name}", border_style="red")
    else:
        return Panel(str(result), title=f"🔧 {tool_name}", border_style="blue")


# ============================================================
# CLI Commands
# ============================================================

@click.group(invoke_without_command=True)
@click.option('--config', '-c', type=click.Path(), help='Path to config.yaml file')
@click.option('--session', '-s', type=str, help='Session ID to resume')
@click.option('--version', '-v', is_flag=True, help='Show version')
@click.option('--setup', is_flag=True, help='Run the setup wizard')
@click.pass_context
def cli(ctx, config, session, version, setup):
    # DACLI: AI-powered Data Engineering Assistant
    if version:
        console.print(f"DACLI version {__version__}")
        return
    
    ctx.ensure_object(dict)
    ctx.obj['config_path'] = config
    ctx.obj['session_id'] = session
    ctx.obj['run_setup'] = setup
    
    if ctx.invoked_subcommand is None:
        # Default to chat mode
        ctx.invoke(chat, config=config, session=session, run_setup=setup)


@cli.command()
@click.option('--config', '-c', type=click.Path(), help='Path to config.yaml file')
@click.option('--session', '-s', type=str, help='Session ID to resume')
@click.option('--setup', 'run_setup', is_flag=True, help='Force run setup wizard')
@click.pass_context
def chat(ctx, config, session, run_setup):
    """Start interactive chat with the agent."""
    config_path = config or ctx.obj.get('config_path')
    session_id = session or ctx.obj.get('session_id')
    force_setup = run_setup or ctx.obj.get('run_setup', False)
    
    asyncio.run(_run_chat(config_path, session_id, force_setup=force_setup))


@cli.command()
@click.option('--config', '-c', type=click.Path(), help='Path to config.yaml file')
@click.option('--profile', '-p', type=str, help='Quick profile: full, github_only, snowflake_only, datawarehouse')
def setup(config, profile):
    """Run the interactive tool setup wizard."""
    config_path = config or "config.yaml"
    settings = load_config(config_path)
    
    if profile:
        # Use quick profile
        QuickSetup.show_profiles(console)
        tools_settings = QuickSetup.get_profile(profile)
        if tools_settings:
            save_tools_config(tools_settings, config_path)
            console.print(f"[success]✓ Applied profile: {profile}[/success]")
        else:
            console.print(f"[error]Unknown profile: {profile}[/error]")
            console.print("Available profiles: full, github_only, snowflake_only, datawarehouse")
    else:
        # Run full wizard
        asyncio.run(_run_setup_wizard(config_path, settings))


async def _run_setup_wizard(config_path: str, settings: Settings) -> ToolsSettings:
    """Run the setup wizard and save results."""
    wizard = SetupWizard(settings, config_path)
    tools_settings = await wizard.run()
    save_tools_config(tools_settings, config_path)
    return tools_settings


@cli.command()
@click.option('--config', '-c', type=click.Path(), help='Path to config.yaml file')
def init(config):
    # Initialize a new config.yaml file.
    target_path = Path(config) if config else Path("config.yaml")
    
    if target_path.exists():
        if not Confirm.ask(f"[warning]{target_path} already exists. Overwrite?[/warning]"):
            console.print("Cancelled.")
            return
    
    # Create default config
    from config.settings import Settings
    settings = Settings()
    
    # Save to file
    import yaml
    with open(target_path, 'w') as f:
        yaml.dump(settings.model_dump(), f, default_flow_style=False)
    
    console.print(f"[success]Created {target_path}[/success]")
    console.print("Edit this file to configure your credentials and settings.")


@cli.command()
@click.option('--config', '-c', type=click.Path(), help='Path to config.yaml file')
def validate(config):
    # Validate configuration and test connections.
    settings = load_config(config)
    
    console.print("[info]Validating configuration...[/info]")
    
    asyncio.run(_validate_connections(settings))


@cli.command()
def sessions():
    # List available sessions.
    memory = AgentMemory()
    session_list = memory.list_sessions()
    
    if not session_list:
        console.print("[dim]No sessions found.[/dim]")
        return
    
    table = Table(title="Available Sessions", show_header=True)
    table.add_column("Session ID", style="cyan")
    table.add_column("Created")
    table.add_column("Phase")
    table.add_column("Tables")
    table.add_column("Errors")
    
    for s in session_list:
        table.add_row(
            s['session_id'],
            s['created_at'][:19] if s.get('created_at') else "?",
            s.get('current_phase', 'unknown'),
            str(s.get('tables_created', 0)),
            str(s.get('errors_count', 0))
        )
    
    console.print(table)


@cli.command()
@click.argument('session_id')
@click.option('--config', '-c', type=click.Path(), help='Path to config.yaml file')
def load(session_id, config):
    """Load and resume a previous session."""
    asyncio.run(_run_chat(config, session_id))


@cli.command()
@click.option('--output', '-o', type=click.Path(), help='Output file path')
def prompt(output):
    # View or edit the system prompt.
    current_prompt = load_system_prompt()
    
    if output:
        # Save to file
        save_system_prompt(current_prompt, output)
        console.print(f"[success]Saved system prompt to {output}[/success]")
    else:
        # Display current prompt
        md = Markdown(current_prompt)
        console.print(Panel(md, title="System Prompt", border_style="cyan"))
        console.print(f"\n[dim]Prompt file: {SYSTEM_PROMPT_FILE}[/dim]")
        console.print("[dim]Edit this file to customize the agent's behavior.[/dim]")


# ============================================================
# Async Functions
# ============================================================

async def _validate_connections(settings: Settings):
    # Validate all connections.
    from tools.snowflake_tool import SnowflakeTool
    from tools.pinecone_tool import PineconeTool
    
    results = []
    
    # Snowflake
    with console.status("[bold green]Testing Snowflake connection..."):
        try:
            sf = SnowflakeTool(settings)
            result = await sf.validate()
            if result.success:
                console.print("[success]✅ Snowflake: Connected[/success]")
                console.print(f"   {result.data}")
            else:
                console.print(f"[error]❌ Snowflake: {result.error}[/error]")
            await sf.disconnect()
        except Exception as e:
            console.print(f"[error]❌ Snowflake: {e}[/error]")
    
    # Pinecone
    with console.status("[bold green]Testing Pinecone connection..."):
        try:
            pc = PineconeTool(settings)
            result = await pc.validate()
            if result.success:
                console.print("[success]✅ Pinecone: Connected[/success]")
                console.print(f"   Index: {result.data.get('index_name')}, Vectors: {result.data.get('total_vectors')}")
            else:
                console.print(f"[error]❌ Pinecone: {result.error}[/error]")
            await pc.disconnect()
        except Exception as e:
            console.print(f"[warning]⚠️ Pinecone: {e}[/warning]")
    
async def _run_chat(config_path: Optional[str], session_id: Optional[str], force_setup: bool = False):
    # Run the interactive chat session.
    print_banner()
    
    # Load configuration
    console.print("[dim]Loading configuration...[/dim]")
    settings = load_config(config_path)
    
    # Check if setup wizard should run
    tools_settings = settings.tools if settings.tools else ToolsSettings()
    
    if force_setup or not tools_settings.setup_completed:
        console.print()
        if not tools_settings.setup_completed:
            console.print("[yellow]First time setup detected![/yellow]")
            run_wizard = Confirm.ask(
                "Would you like to configure which tools to use?",
                default=True
            )
        else:
            run_wizard = True
        
        if run_wizard:
            wizard = SetupWizard(settings, config_path or "config.yaml")
            tools_settings = await wizard.run()
            save_tools_config(tools_settings, config_path or "config.yaml")
            # Reload settings with new tools config
            settings = load_config(config_path)
            tools_settings = settings.tools if settings.tools else tools_settings
        else:
            # Enable all tools by default if user skips wizard
            console.print("[dim]Using default configuration (all tools enabled)[/dim]")
            tools_settings = QuickSetup.get_profile("full")
            save_tools_config(tools_settings, config_path or "config.yaml")
    
    # Initialize memory
    memory = AgentMemory(
        state_path=settings.agent.state_path,
        history_path=settings.agent.history_path,
        memory_window=settings.agent.memory_window
    )
    
    # Load session if specified
    if session_id:
        if memory.load_session(session_id):
            console.print(f"[success]Loaded session: {session_id}[/success]")
        else:
            console.print(f"[error]Session not found: {session_id}[/error]")
            return
    
    # Status update callback
    def on_status_update(message: str):
        console.print(f"[dim]{message}[/dim]")
    
    # Tool callbacks
    def on_tool_start(tool_name: str, args: dict):
        console.print(f"[tool]🔧 Calling {tool_name}...[/tool]")
        if "query" in args:
            console.print(format_sql(args["query"]))
    
    def on_tool_end(tool_name: str, result):
        console.print(format_tool_result(tool_name, result))
    
    # Mutable container for the status spinner so callbacks can stop it
    spinner_ctx = {"status": None}

    # User input callback
    def on_user_input_needed(question: str) -> str:
        # Stop the spinner so the terminal is free for user input
        if spinner_ctx["status"]:
            spinner_ctx["status"].stop()
        console.print(Panel(question, title="❓ Input Needed", border_style="yellow"))
        response = Prompt.ask("[prompt]Your response[/prompt]")
        # Restart the spinner after user responds
        if spinner_ctx["status"]:
            spinner_ctx["status"].start()
        return response
    
    # Initialize agent
    agent = DACLI(
        settings=settings,
        tools_settings=tools_settings,
        memory=memory,
        on_status_update=on_status_update,
        on_tool_start=on_tool_start,
        on_tool_end=on_tool_end,
        on_user_input_needed=on_user_input_needed
    )
    
    # Initialize connections
    console.print("\n[info]Initializing agent...[/info]")
    
    try:
        if not await agent.initialize():
            console.print("[error]Failed to initialize agent. Check your configuration.[/error]")
            return
    except Exception as e:
        console.print(f"[error]Initialization error: {e}[/error]")
        console.print("[dim]Check your config.yaml and ensure credentials are set.[/dim]")
        return
    
    console.print("[success]✅ Agent ready![/success]")
    print_help_commands()
    console.print()
    
    # Set up prompt toolkit for better input
    history_file = Path(settings.agent.history_path) / "input_history.txt"
    history_file.parent.mkdir(parents=True, exist_ok=True)
    
    pt_session = PromptSession(
        history=FileHistory(str(history_file)),
        auto_suggest=AutoSuggestFromHistory(),
    )
    
    try:
        while True:
            try:
                # Get user input
                user_input = await asyncio.to_thread(
                    pt_session.prompt,
                    "You > "
                )
                
                if not user_input.strip():
                    continue
                
                # Handle commands
                if user_input.startswith("/"):
                    cmd = user_input.lower().split()[0]
                    args = user_input.split()[1:] if len(user_input.split()) > 1 else []
                    
                    if cmd == "/exit" or cmd == "/quit":
                        break
                    
                    elif cmd == "/help":
                        print_help_commands()
                    
                    elif cmd == "/status":
                        print_status(memory)
                    
                    elif cmd == "/history":
                        for msg in memory.get_full_history()[-20:]:
                            role_style = "user" if msg.role == "user" else "assistant"
                            console.print(f"[{role_style}]{msg.role.upper()}:[/{role_style}] {msg.content[:200]}...")
                    
                    elif cmd == "/sessions":
                        session_list = memory.list_sessions()
                        table = Table(title="Sessions")
                        table.add_column("ID", style="cyan")
                        table.add_column("Phase")
                        table.add_column("Tables")
                        for s in session_list[:10]:
                            table.add_row(s['session_id'], s.get('current_phase', '?'), str(s.get('tables_created', 0)))
                        console.print(table)
                    
                    elif cmd == "/load" and args:
                        if memory.load_session(args[0]):
                            console.print(f"[success]Loaded session: {args[0]}[/success]")
                        else:
                            console.print(f"[error]Session not found: {args[0]}[/error]")
                    
                    elif cmd == "/export":
                        console.print(memory.export_state())
                    
                    elif cmd == "/config":
                        table = Table(title="Configuration")
                        table.add_column("Setting", style="cyan")
                        table.add_column("Value")
                        table.add_row("LLM Provider", settings.llm.provider)
                        table.add_row("LLM Model", settings.llm.model)
                        table.add_row("Snowflake Account", settings.snowflake.account[:10] + "..." if settings.snowflake.account else "Not set")
                        table.add_row("Snowflake Database", settings.snowflake.database)
                        table.add_row("Pinecone Index", settings.pinecone.index_name)
                        console.print(table)
                    
                    elif cmd == "/prompt":
                        prompt_content = load_system_prompt()
                        console.print(Panel(Markdown(prompt_content[:2000] + "..."), title="System Prompt", border_style="cyan"))
                    
                    elif cmd == "/clear":
                        memory.clear_messages()
                        console.print("[success]Conversation cleared.[/success]")
                    
                    elif cmd == "/reset":
                        if Confirm.ask("Are you sure you want to reset the agent state?"):
                            memory._state = memory._create_new_state()
                            memory.clear_messages()
                            console.print("[success]Agent state reset.[/success]")
                    
                    elif cmd == "/tools":
                        # Show enabled tools
                        table = Table(title="🔧 Enabled Tools", show_header=True)
                        table.add_column("Category", style="cyan")
                        table.add_column("Status", justify="center")
                        table.add_column("Operations", justify="center")
                        
                        for category in ToolCategory:
                            info = TOOL_CATALOG[category]
                            config = tools_settings.get_tool_config(category)
                            if config.enabled:
                                ops = config.get_enabled_operations()
                                table.add_row(
                                    f"{info['icon']} {info['name']}",
                                    "[green]✅ Enabled[/green]",
                                    str(len(ops))
                                )
                            else:
                                table.add_row(
                                    f"{info['icon']} {info['name']}",
                                    "[dim]⊘ Disabled[/dim]",
                                    "—"
                                )
                        console.print(table)
                        console.print("[dim]Use /setup to reconfigure tools[/dim]")
                    
                    elif cmd == "/setup":
                        # Run setup wizard
                        console.print("[info]Running setup wizard...[/info]")
                        wizard = SetupWizard(settings, config_path or "config.yaml")
                        tools_settings = await wizard.run()
                        save_tools_config(tools_settings, config_path or "config.yaml")
                        console.print("[success]Tools reconfigured. Restart the agent to apply changes.[/success]")
                    
                    else:
                        console.print(f"[warning]Unknown command: {cmd}[/warning]")
                    
                    continue
                
                # Process message with agent
                console.print()
                
                status = console.status("[bold cyan]Thinking...", spinner="dots")
                spinner_ctx["status"] = status
                with status:
                    response = await agent.process_message(user_input)
                spinner_ctx["status"] = None
                
                if response.error:
                    console.print(f"[error]Error: {response.error}[/error]")
                else:
                    console.print(format_response(response.content))
                
                if response.needs_user_input:
                    console.print("[warning]⏳ Agent is waiting for your input to continue.[/warning]")
                
                console.print()
            
            except KeyboardInterrupt:
                console.print("\n[dim]Use /exit to quit[/dim]")
                continue
            
            except EOFError:
                break
    
    finally:
        console.print("\n[dim]Cleaning up...[/dim]")
        await agent.shutdown()
        console.print("[success]Goodbye! 👋[/success]")


# ============================================================
# Entry Point
# ============================================================

def main():
    """CLI entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
