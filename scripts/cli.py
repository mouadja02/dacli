import asyncio
import click
from typing import Optional

from pathlib import Path
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.prompt import Prompt, Confirm
from prompt_toolkit import PromptSession
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.key_binding import KeyBindings

from core import __author__, __version__
from config import CLI_COMMANDS
from config.settings import load_config, Settings
from connectors.registry import (
    ConnectorRegistry,
    save_connectors_config,
    CONNECTORS_CONFIG_PATH,
)
from core.agent import DACLI
from core.memory import AgentMemory
from core.setup_wizard import SetupWizard, QuickSetup
from prompts.system_prompt import load_system_prompt, save_system_prompt, SYSTEM_PROMPT_FILE
from tui import DacliUI, THEMES

# Module-level UI for the standalone (non-chat) click commands. The interactive
# chat in ``_run_chat`` builds its own themed instance once settings are loaded.
ui = DacliUI(version=__version__, author=__author__)
console = ui.console


class SlashCommandCompleter(Completer):
    """Autocomplete chat slash-commands (e.g. ``/st`` -> ``/status``).

    Only activates while the line starts with ``/`` so it never interferes with
    normal prompts to the agent.
    """

    def __init__(self, commands):
        # commands: list of (cmd, description); ``cmd`` may include args like "/load <id>".
        self._commands = [(c.split()[0], desc) for c, desc in commands]

    @staticmethod
    def _subsequence(needle: str, haystack: str) -> bool:
        # Fuzzy match: are needle's chars an in-order subsequence of haystack?
        it = iter(haystack)
        return all(ch in it for ch in needle)

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor.lstrip()
        if not text.startswith("/") or " " in text:
            return  # not a command, or already past the command word
        query = text[1:].lower()  # chars after the leading slash
        prefix, fuzzy = [], []
        for cmd, desc in self._commands:
            name = cmd[1:].lower()
            if name.startswith(query):
                prefix.append((cmd, desc))
            elif query and self._subsequence(query, name):
                fuzzy.append((cmd, desc))
        # Prefix matches first (most expected), then looser fuzzy matches.
        for cmd, desc in prefix + fuzzy:
            yield Completion(cmd, start_position=-len(text), display=cmd, display_meta=desc)


def build_completion_keybindings() -> KeyBindings:
    """Tab opens/advances the completion menu; Shift-Tab steps back."""
    kb = KeyBindings()

    @kb.add("tab")
    def _(event):
        buff = event.app.current_buffer
        if buff.complete_state:
            buff.complete_next()          # menu open -> next suggestion
        else:
            buff.start_completion(select_first=True)  # open menu, pick first

    @kb.add("s-tab")  # Shift-Tab
    def _(event):
        buff = event.app.current_buffer
        if buff.complete_state:
            buff.complete_previous()
        else:
            buff.start_completion(select_last=True)

    return kb


# -----------------------------------------
#  UI components
# -----------------------------------------
def print_status(memory: AgentMemory, target: Optional[DacliUI] = None):
    # Print current agent status through the active themed UI.
    out = target or ui
    con = out.console
    summary = memory.get_progress_summary()

    # Main status panel
    status_text = Text()
    status_text.append("Session     ", style="muted")
    status_text.append(f"{summary['session_id']}\n", style="accent")
    status_text.append("Active task ", style="muted")
    status_text.append(f"{summary.get('active_task') or '—'}", style="phase")
    con.print(Panel(status_text, title="[accent]Status[/accent]", border_style="border", padding=(1, 2)))

    # Plan (todo list)
    if summary.get('todos'):
        table = Table(title="[accent]Plan[/accent]", show_header=True,
                      header_style="muted", border_style="border", box=None, padding=(0, 2, 0, 0))
        table.add_column("#", style="muted", justify="right")
        table.add_column("Status")
        table.add_column("Task", style="info")
        for i, todo in enumerate(summary.get('todos', []), 1):
            status = todo.get('status', 'pending')
            status_icon = {
                "pending": "○",
                "in_progress": "◐",
                "completed": "●",
            }.get(status, "○")
            table.add_row(str(i), f"{status_icon} {status}", todo.get('content', ''))
        con.print(table)

    # Stats
    stats_table = Table(show_header=False, box=None, padding=(0, 2, 0, 0))
    stats_table.add_column("Metric", style="muted")
    stats_table.add_column("Value", style="info", justify="right")
    stats_table.add_row("Schemas created", str(summary.get('schemas_created', 0)))
    stats_table.add_row("Tables created", str(summary.get('tables_created', 0)))
    stats_table.add_row("Tables loaded", str(summary.get('tables_loaded', 0)))
    stats_table.add_row("Total rows", str(summary.get('total_rows_loaded', 0)))
    stats_table.add_row("Files discovered", str(summary.get('files_discovered', 0)))
    stats_table.add_row("Errors", str(summary.get('errors_count', 0)))
    con.print(Panel(stats_table, title="[accent]Statistics[/accent]", border_style="border", padding=(1, 2)))

    if summary.get('last_error'):
        con.print(f"[error]Last error:[/error] {summary['last_error']}")



def _init_dacli_md(settings: Settings) -> None:
    """`/init`: draft a DACLI.md priors file from config (no secrets)."""
    from memory.priors import generate_dacli_md, DACLI_PRIORS_FILE

    target = Path(DACLI_PRIORS_FILE.name)
    if target.exists():
        if not Confirm.ask(f"[warning]{target} already exists. Overwrite?[/warning]", default=False):
            console.print("[dim]Cancelled — DACLI.md left unchanged.[/dim]")
            return

    content = generate_dacli_md(settings)
    target.write_text(content, encoding="utf-8")
    console.print(f"[success]✓ Wrote {target}[/success] from config (secrets excluded).")
    console.print("[dim]Review/edit it — it loads as the top layer of context next session.[/dim]")


# Tool-call and agent-response rendering now lives in :mod:`tui.ui` (DacliUI).


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
@click.option('--profile', '-p', type=str, help='Quick profile: full, none, or <connector>_only (e.g. github_only)')
def setup(config, profile):
    """Run the interactive connector setup wizard."""
    config_path = config or "config.yaml"
    settings = load_config(config_path)
    registry = ConnectorRegistry(settings, config_path=CONNECTORS_CONFIG_PATH)

    if profile:
        # Use quick profile
        QuickSetup.show_profiles(console, registry)
        connectors_config = QuickSetup.get_profile(profile, registry)
        if connectors_config:
            save_connectors_config(connectors_config, CONNECTORS_CONFIG_PATH)
            console.print(f"[success]✓ Applied profile: {profile}[/success]")
        else:
            console.print(f"[error]Unknown profile: {profile}[/error]")
            console.print("Available profiles: " + ", ".join(QuickSetup.list_profiles(registry).keys()))
    else:
        # Run full wizard
        asyncio.run(_run_setup_wizard(config_path, settings))


async def _run_setup_wizard(config_path: str, settings: Settings) -> dict:
    """Run the setup wizard and save results."""
    registry = ConnectorRegistry(settings, config_path=CONNECTORS_CONFIG_PATH)
    wizard = SetupWizard(settings, registry, CONNECTORS_CONFIG_PATH)
    connectors_config = await wizard.run()
    save_connectors_config(connectors_config, CONNECTORS_CONFIG_PATH)
    return connectors_config


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
    table.add_column("Active task")
    table.add_column("Tables")
    table.add_column("Errors")

    for s in session_list:
        table.add_row(
            s['session_id'],
            s['created_at'][:19] if s.get('created_at') else "?",
            s.get('active_task') or "—",
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
@click.option('--config', '-c', type=click.Path(), help='Path to config.yaml file')
@click.option('--session', '-s', type=str, help='Session ID to inspect')
@click.option('--task', '-t', type=str, help='Task to assemble context for (defaults to the last user message)')
@click.option('--explain', is_flag=True, help='Print each context chunk with its source, timestamp and token cost')
def context(config, session, task, explain):
    """Inspect the assembled context (Context Constructor, Phase 3)."""
    settings = load_config(config)
    memory = AgentMemory(
        state_path=settings.agent.state_path,
        history_path=settings.agent.history_path,
        memory_window=settings.agent.memory_window,
    )
    if session and not memory.load_session(session):
        console.print(f"[error]Session not found: {session}[/error]")
        return

    # Build the agent (no initialize() -> no network) just for its context pipeline.
    agent = DACLI(settings=settings, memory=memory)
    build = agent._context["build"]

    working = [{"role": m.role, "content": m.content} for m in memory.get_full_history()]
    if not task:
        task = next(
            (m["content"] for m in reversed(working) if m.get("role") == "user"),
            "(no task — provide --task)",
        )

    ctx = build(task, working, set())
    _print_context_explain(ctx, task, explain)


def _print_context_explain(ctx, task, explain: bool) -> None:
    # Render the assembled context for `dacli context --explain`.
    from context.tokenizer import make_counter

    console.print(Panel(Text(task, style="info"), title="[accent]Task[/accent]", border_style="border", padding=(0, 2)))

    if explain:
        table = Table(title="[accent]Context chunks[/accent]", show_header=True,
                      header_style="muted", border_style="border", box=None, padding=(0, 2, 0, 0))
        table.add_column("Source", style="info")
        table.add_column("Label", style="step")
        table.add_column("Timestamp", style="muted")
        table.add_column("Tokens", justify="right", style="accent")
        table.add_column("Pinned", justify="center")
        for row in ctx.explain():
            table.add_row(
                row["source"],
                str(row["label"])[:48],
                (row["timestamp"] or "")[:19],
                str(row["tokens"]),
                "📌" if row["pinned"] else "",
            )
        console.print(table)

    # Per-source budget usage.
    budget_table = Table(title="[accent]Budget usage[/accent]", show_header=True,
                         header_style="muted", border_style="border", box=None, padding=(0, 2, 0, 0))
    budget_table.add_column("Source", style="info")
    budget_table.add_column("Used", justify="right", style="accent")
    budget_table.add_column("Cap", justify="right", style="muted")
    for source, vals in ctx.budget.items():
        budget_table.add_row(source, str(vals["used"]), str(vals["cap"]))
    console.print(budget_table)

    sys_tokens = make_counter().count(ctx.system_prompt)
    console.print(
        f"[muted]System prompt:[/muted] {sys_tokens} tokens  ·  "
        f"[muted]Messages:[/muted] {len(ctx.messages)}  ·  "
        f"[muted]Tools disclosed:[/muted] {len(ctx.tools)}"
    )


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
    # Validate all discovered connectors via a live health check.
    registry = ConnectorRegistry(settings, config_path=CONNECTORS_CONFIG_PATH)
    catalog = registry.get_catalog()

    for connector_id, info in catalog.items():
        label = f"{info['icon']} {info['name']}"
        with console.status(f"[bold green]Testing {info['name']} connection..."):
            connector = registry.get_connector(connector_id)
            try:
                result = await connector.health()
                if result.success:
                    console.print(f"[success]✅ {label}: Connected[/success]")
                    console.print(f"   {result.data}")
                else:
                    console.print(f"[error]❌ {label}: {result.error}[/error]")
                await connector.disconnect()
            except Exception as e:
                console.print(f"[warning]⚠️ {label}: {e}[/warning]")

def _enabled_connector_names(registry) -> list:
    # Short connector names for the welcome card / status bar.
    names = []
    catalog = registry.get_catalog()
    for connector_id in catalog:
        if registry.is_connector_enabled(connector_id):
            names.append(connector_id)
    return names


def _ctx_pct(memory) -> int:
    # Context-window fill: how full the rolling message window is (0-100).
    window = max(getattr(memory, "memory_window", 0) or 1, 1)
    used = min(len(memory.get_full_history()), window)
    return int(round(used / window * 100))


async def _run_chat(config_path: Optional[str], session_id: Optional[str], force_setup: bool = False):
    # Run the interactive chat session.
    # Load configuration first so the UI is themed from the user's settings.
    settings = load_config(config_path)
    chat_ui = DacliUI(settings=settings, version=__version__, author=__author__)
    con = chat_ui.console

    chat_ui.banner()
    chat_ui.status("Loading configuration…")

    # Check if setup wizard should run
    registry = ConnectorRegistry(settings, config_path=CONNECTORS_CONFIG_PATH)

    if force_setup or not registry.setup_completed:
        con.print()
        if not registry.setup_completed:
            chat_ui.notice("First-time setup detected.", style="warning")
            run_wizard = Confirm.ask(
                "Would you like to configure which connectors to use?",
                default=True, console=con,
            )
        else:
            run_wizard = True

        if run_wizard:
            wizard = SetupWizard(settings, registry, CONNECTORS_CONFIG_PATH)
            connectors_config = await wizard.run()
            save_connectors_config(connectors_config, CONNECTORS_CONFIG_PATH)
        else:
            chat_ui.status("Using default configuration (all connectors enabled)")
            connectors_config = QuickSetup.get_profile("full", registry)
            if connectors_config:
                save_connectors_config(connectors_config, CONNECTORS_CONFIG_PATH)

    # Initialize memory
    memory = AgentMemory(
        state_path=settings.agent.state_path,
        history_path=settings.agent.history_path,
        memory_window=settings.agent.memory_window
    )

    # Load session if specified
    if session_id:
        if memory.load_session(session_id):
            chat_ui.notice(f"Loaded session: {session_id}", style="success")
        else:
            chat_ui.error(f"Session not found: {session_id}")
            return

    def on_user_input_needed(question: str) -> str:
        # Asked mid-loop (system connector); the stream is already torn down.
        con.print(Panel(question, title="[warning]input needed[/warning]", border_style="warning", padding=(1, 2)))
        return Prompt.ask("[prompt]your response[/prompt]", console=con)

    # Initialize agent — UI methods wired directly as kernel callbacks.
    agent = DACLI(
        settings=settings,
        memory=memory,
        on_status_update=chat_ui.status,
        on_tool_start=chat_ui.tool_start,
        on_tool_end=chat_ui.tool_end,
        on_user_input_needed=on_user_input_needed,
        on_stream_start=chat_ui.on_stream_start,
        on_text=chat_ui.on_text,
        on_stream_end=chat_ui.on_stream_end,
        connectors_config_path=CONNECTORS_CONFIG_PATH,
    )

    # Initialize connections (the agent emits its own progress via on_status).
    con.print()
    try:
        if not await agent.initialize():
            chat_ui.error("Failed to initialize agent. Check your configuration.")
            return
    except Exception as e:
        chat_ui.error(f"Initialization error: {e}")
        chat_ui.notice("Check your config.yaml and ensure credentials are set.", style="muted")
        return

    con.print()
    chat_ui.welcome(
        model=settings.llm.model,
        provider=settings.llm.provider,
        connectors=_enabled_connector_names(agent.registry),
        cwd=str(Path.cwd()),
    )

    # Set up prompt toolkit for better input
    history_file = Path(settings.agent.history_path) / "input_history.txt"
    history_file.parent.mkdir(parents=True, exist_ok=True)

    def bottom_toolbar():
        return chat_ui.bottom_toolbar(
            provider=settings.llm.provider,
            model=settings.llm.model,
            connectors=_enabled_connector_names(agent.registry),
            ctx_pct=_ctx_pct(memory),
            session=memory.session_id,
        )

    pt_session = PromptSession(
        history=FileHistory(str(history_file)),
        auto_suggest=AutoSuggestFromHistory(),
        completer=SlashCommandCompleter(CLI_COMMANDS),
        complete_while_typing=True,
        key_bindings=build_completion_keybindings(),
        bottom_toolbar=bottom_toolbar,
    )

    try:
        while True:
            try:
                user_input = await asyncio.to_thread(
                    pt_session.prompt,
                    HTML('<b>❯</b> '),
                )

                if not user_input.strip():
                    continue

                # Handle commands
                if user_input.startswith("/"):
                    cmd = user_input.lower().split()[0]
                    args = user_input.split()[1:] if len(user_input.split()) > 1 else []

                    if cmd in ("/exit", "/quit"):
                        break

                    elif cmd == "/help":
                        chat_ui.help(CLI_COMMANDS)

                    elif cmd == "/init":
                        _init_dacli_md(settings)

                    elif cmd == "/status":
                        print_status(memory, chat_ui)

                    elif cmd == "/context":
                        working = [{"role": m.role, "content": m.content} for m in memory.get_full_history()]
                        task_arg = " ".join(args) if args else next(
                            (m["content"] for m in reversed(working) if m.get("role") == "user"),
                            "(no task yet)",
                        )
                        ctx = agent._context["build"](task_arg, working, set())
                        _print_context_explain(ctx, task_arg, explain=True)

                    elif cmd == "/history":
                        chat_ui.history(memory.get_full_history())

                    elif cmd == "/sessions":
                        chat_ui.sessions_table(memory.list_sessions())

                    elif cmd == "/load" and args:
                        if memory.load_session(args[0]):
                            chat_ui.notice(f"Loaded session: {args[0]}", style="success")
                        else:
                            chat_ui.error(f"Session not found: {args[0]}")

                    elif cmd == "/export":
                        con.print(memory.export_state())

                    elif cmd == "/config":
                        chat_ui.config_table(settings)

                    elif cmd == "/theme":
                        if args and args[0].lower() in THEMES:
                            chat_ui.set_theme(args[0])
                            chat_ui.notice(f"Theme set to '{args[0].lower()}'.", style="success")
                        else:
                            available = ", ".join(THEMES.keys())
                            chat_ui.notice(f"Usage: /theme <name>  ·  available: {available}", style="muted")

                    elif cmd == "/prompt":
                        prompt_content = load_system_prompt()
                        chat_ui.panel(Markdown(prompt_content[:2000] + "…"), title="[accent]System prompt[/accent]")

                    elif cmd == "/clear":
                        memory.clear_messages()
                        chat_ui.notice("Conversation cleared.", style="success")

                    elif cmd == "/reset":
                        if Confirm.ask("Reset the agent state?", console=con, default=False):
                            memory._state = memory._create_new_state()
                            memory.clear_messages()
                            chat_ui.notice("Agent state reset.", style="success")

                    elif cmd == "/tools":
                        chat_ui.connectors_table(agent.registry)

                    elif cmd == "/setup":
                        chat_ui.status("Running setup wizard…")
                        setup_registry = ConnectorRegistry(settings, config_path=CONNECTORS_CONFIG_PATH)
                        wizard = SetupWizard(settings, setup_registry, CONNECTORS_CONFIG_PATH)
                        connectors_config = await wizard.run()
                        save_connectors_config(connectors_config, CONNECTORS_CONFIG_PATH)
                        chat_ui.notice("Connectors reconfigured. Restart to apply.", style="success")

                    else:
                        chat_ui.notice(f"Unknown command: {cmd}", style="warning")

                    continue

                # Process the message with the agent. The kernel streams text +
                # tool calls to the UI as it runs, so there is nothing to print
                # here on success — only errors / hand-offs need a notice.
                # (prompt_toolkit already leaves the typed "❯ …" line in the
                # scrollback, so we don't re-echo it.)
                con.print()

                try:
                    response = await agent.process_message(user_input)
                except KeyboardInterrupt:
                    chat_ui.stream.abort()
                    chat_ui.notice("Interrupted.", style="warning")
                    continue

                if response.error:
                    chat_ui.error(f"Error: {response.error}")

                if response.needs_user_input:
                    chat_ui.notice("⏳ Agent is waiting for your input to continue.", style="warning")

                con.print()

            except KeyboardInterrupt:
                chat_ui.stream.abort()
                con.print("\n[muted]Use /exit to quit[/muted]")
                continue

            except EOFError:
                break

    finally:
        chat_ui.stream.abort()
        con.print("\n[muted]Cleaning up…[/muted]")
        await agent.shutdown()
        chat_ui.notice("Goodbye 👋", style="success")


# ============================================================
# Entry Point
# ============================================================

def main():
    """CLI entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
