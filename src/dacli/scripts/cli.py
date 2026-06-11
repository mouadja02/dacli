import asyncio
import click

from pathlib import Path
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.prompt import Prompt, Confirm
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.key_binding import KeyBindings

from dacli.core import __author__, __version__
from dacli.config import CLI_COMMANDS
from dacli.config.settings import load_config, Settings, is_llm_configured
from dacli.connectors.registry import (
    ConnectorRegistry,
    save_connectors_config,
    CONNECTORS_CONFIG_PATH,
)
from dacli.core.agent import DACLI
from dacli.core.logging_setup import setup_logging
from dacli.core.memory import AgentMemory
from dacli.core.store import DacliStore
from dacli.governance.audit import AuditLedger
from dacli.core.setup_wizard import SetupWizard, QuickSetup, collect_llm_credentials
from dacli.prompts.system_prompt import (
    get_default_system_prompt,
    save_system_prompt,
    SYSTEM_PROMPT_FILE,
)
from dacli.tui import DacliUI, THEMES

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
        # Dedupe by the base command token so two entries that share a word
        # (e.g. "/connect" and "/connect <tool>") don't show as two menu rows.
        seen = set()
        self._commands = []
        for c, desc in commands:
            base = c.split()[0]
            if base in seen:
                continue
            seen.add(base)
            self._commands.append((base, desc))

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
            yield Completion(
                cmd, start_position=-len(text), display=cmd, display_meta=desc
            )


def build_completion_keybindings() -> KeyBindings:
    """Tab opens/advances the completion menu; Shift-Tab steps back."""
    kb = KeyBindings()

    @kb.add("tab")
    def _(event):
        buff = event.app.current_buffer
        if buff.complete_state:
            buff.complete_next()  # menu open -> next suggestion
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
def print_status(memory: AgentMemory, target: DacliUI | None = None):
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
    con.print(
        Panel(
            status_text,
            title="[accent]Status[/accent]",
            border_style="border",
            padding=(1, 2),
        )
    )

    # Plan (todo list)
    if summary.get("todos"):
        table = Table(
            title="[accent]Plan[/accent]",
            show_header=True,
            header_style="muted",
            border_style="border",
            box=None,
            padding=(0, 2, 0, 0),
        )
        table.add_column("#", style="muted", justify="right")
        table.add_column("Status")
        table.add_column("Task", style="info")
        for i, todo in enumerate(summary.get("todos", []), 1):
            status = todo.get("status", "pending")
            status_icon = {
                "pending": "○",
                "in_progress": "◐",
                "completed": "●",
            }.get(status, "○")
            table.add_row(str(i), f"{status_icon} {status}", todo.get("content", ""))
        con.print(table)

    # Stats
    stats_table = Table(show_header=False, box=None, padding=(0, 2, 0, 0))
    stats_table.add_column("Metric", style="muted")
    stats_table.add_column("Value", style="info", justify="right")
    stats_table.add_row("Schemas created", str(summary.get("schemas_created", 0)))
    stats_table.add_row("Tables created", str(summary.get("tables_created", 0)))
    stats_table.add_row("Tables loaded", str(summary.get("tables_loaded", 0)))
    stats_table.add_row("Total rows", str(summary.get("total_rows_loaded", 0)))
    stats_table.add_row("Files discovered", str(summary.get("files_discovered", 0)))
    stats_table.add_row("Errors", str(summary.get("errors_count", 0)))
    con.print(
        Panel(
            stats_table,
            title="[accent]Statistics[/accent]",
            border_style="border",
            padding=(1, 2),
        )
    )

    if summary.get("last_error"):
        con.print(f"[error]Last error:[/error] {summary['last_error']}")


def _fmt_int(n) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def _fmt_cost(c) -> str:
    try:
        c = float(c)
    except Exception:
        return "n/a"
    if c <= 0:
        return "$0.00"
    return f"${c:.4f}" if c < 0.01 else f"${c:.2f}"


def print_usage(
    store: DacliStore,
    session_id: str | None = None,
    target: DacliUI | None = None,
    pricing=None,
):
    # Token/cost usage through the themed UI: all-time totals, by model, this session.
    con = (target or ui).console
    summary = store.usage_summary(session_id)
    totals = summary["totals"]
    by_model = summary["byModel"]
    session = summary.get("session")

    t = Text()
    t.append("Startups    ", style="muted")
    t.append(f"{summary.get('numStartups', 0)}\n", style="accent")
    t.append("Requests    ", style="muted")
    t.append(f"{_fmt_int(totals.get('requests', 0))}\n", style="accent")
    t.append("Input       ", style="muted")
    t.append(f"{_fmt_int(totals.get('input', 0))} tok\n", style="info")
    t.append("Output      ", style="muted")
    t.append(f"{_fmt_int(totals.get('output', 0))} tok\n", style="info")
    t.append("Cache read  ", style="muted")
    t.append(f"{_fmt_int(totals.get('cache_read', 0))} tok\n", style="info")
    t.append("Cache write ", style="muted")
    t.append(f"{_fmt_int(totals.get('cache_creation', 0))} tok\n", style="info")
    t.append("Total cost  ", style="muted")
    t.append(_fmt_cost(totals.get("costUSD", 0)), style="success")
    con.print(
        Panel(
            t,
            title="[accent]Usage — all time[/accent]",
            border_style="border",
            padding=(1, 2),
        )
    )

    if by_model:
        table = Table(
            title="[accent]By model[/accent]",
            show_header=True,
            header_style="muted",
            border_style="border",
            box=None,
            padding=(0, 2, 0, 0),
        )
        table.add_column("Model", style="info")
        table.add_column("Reqs", justify="right")
        table.add_column("Input", justify="right")
        table.add_column("Output", justify="right")
        table.add_column("Cache R/W", justify="right")
        table.add_column("Cost", justify="right", style="success")
        for model, b in sorted(
            by_model.items(), key=lambda kv: kv[1].get("costUSD", 0), reverse=True
        ):
            table.add_row(
                model,
                _fmt_int(b.get("requests", 0)),
                _fmt_int(b.get("input", 0)),
                _fmt_int(b.get("output", 0)),
                f"{_fmt_int(b.get('cache_read', 0))}/{_fmt_int(b.get('cache_creation', 0))}",
                _fmt_cost(b.get("costUSD", 0)),
            )
        con.print(table)

    if session:
        s = Text()
        s.append("Model ", style="muted")
        s.append(f"{session.get('model', '?')}   ", style="accent")
        s.append("Reqs ", style="muted")
        s.append(f"{_fmt_int(session.get('requests', 0))}   ", style="info")
        s.append("In/Out ", style="muted")
        s.append(
            f"{_fmt_int(session.get('input', 0))}/{_fmt_int(session.get('output', 0))} tok   ",
            style="info",
        )
        s.append("Cost ", style="muted")
        s.append(_fmt_cost(session.get("costUSD", 0)), style="success")
        con.print(
            Panel(
                s,
                title=f"[accent]This session ({session_id})[/accent]",
                border_style="border",
                padding=(1, 2),
            )
        )

    if pricing is not None and getattr(pricing, "is_fuzzy", False):
        con.print(
            f"[muted]Priced as [info]{pricing.resolved_provider}/{pricing.resolved_model}[/info] "
            f"— closest models.dev match for [info]{pricing.model}[/info] "
            f"({pricing.match}, similarity {pricing.similarity}).[/muted]"
        )

    if totals.get("requests") and not totals.get("costUSD"):
        con.print(
            "[muted]Cost shows $0.00 — models.dev pricing unavailable for this model (offline or unlisted).[/muted]"
        )


def _init_dacli_md(settings: Settings) -> None:
    """`/init`: draft a DACLI.md priors file from config (no secrets)."""
    from dacli.memory.priors import generate_dacli_md, DACLI_PRIORS_FILE

    target = Path(DACLI_PRIORS_FILE.name)
    if target.exists() and not Confirm.ask(
        f"[warning]{target} already exists. Overwrite?[/warning]", default=False
    ):
        console.print("[dim]Cancelled — DACLI.md left unchanged.[/dim]")
        return

    content = generate_dacli_md(settings)
    target.write_text(content, encoding="utf-8")
    console.print(
        f"[success]✓ Wrote {target}[/success] from config (secrets excluded)."
    )
    console.print(
        "[dim]Review/edit it — it loads as the top layer of context next session.[/dim]"
    )


# Tool-call and agent-response rendering now lives in :mod:`tui.ui` (DacliUI).


# ============================================================
# CLI Commands
# ============================================================


@click.group(invoke_without_command=True)
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--session", "-s", type=str, help="Session ID to resume")
@click.option("--version", "-v", is_flag=True, help="Show version")
@click.option("--setup", is_flag=True, help="Run the setup wizard")
@click.option("--debug", is_flag=True, help="Verbose DEBUG logging to .dacli/dacli.log "
                                            "(re-raises unexpected kernel errors)")
@click.pass_context
def cli(ctx, config, session, version, setup, debug):
    # DACLI: AI-powered Data Engineering Assistant
    # P06: configure the logging tree once, at the single CLI entry point.
    # --debug (or DACLI_DEBUG=1) flips the whole tree to DEBUG.
    setup_logging(debug=True if debug else None)

    if version:
        console.print(f"DACLI version {__version__}")
        return

    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config
    ctx.obj["session_id"] = session
    ctx.obj["run_setup"] = setup
    ctx.obj["debug"] = debug

    if ctx.invoked_subcommand is None:
        # Default to chat mode
        ctx.invoke(chat, config=config, session=session, run_setup=setup)


@cli.command()
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--session", "-s", type=str, help="Session ID to resume")
@click.option("--setup", "run_setup", is_flag=True, help="Force run setup wizard")
@click.pass_context
def chat(ctx, config, session, run_setup):
    """Start interactive chat with the agent."""
    config_path = config or ctx.obj.get("config_path")
    session_id = session or ctx.obj.get("session_id")
    force_setup = run_setup or ctx.obj.get("run_setup", False)

    asyncio.run(_run_chat(config_path, session_id, force_setup=force_setup))


@cli.command()
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option(
    "--profile",
    "-p",
    type=str,
    help="Quick profile: full, none, or <connector>_only (e.g. github_only)",
)
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
            console.print(
                "Available profiles: "
                + ", ".join(QuickSetup.list_profiles(registry).keys())
            )
    else:
        # Run full wizard
        asyncio.run(_run_setup_wizard(config_path, settings))


async def _run_setup_wizard(_config_path: str, settings: Settings) -> dict:
    """Run the setup wizard and save results."""
    registry = ConnectorRegistry(settings, config_path=CONNECTORS_CONFIG_PATH)
    wizard = SetupWizard(settings, registry, CONNECTORS_CONFIG_PATH)
    connectors_config = await wizard.run()
    save_connectors_config(connectors_config, CONNECTORS_CONFIG_PATH)
    return connectors_config


def _find_config_template() -> Path | None:
    # The commented template ships at the repo root; prefer it over a bare
    # defaults dump. Check the cwd first, then the source checkout root.
    candidates = [
        Path("config_template.yaml"),
        Path(__file__).resolve().parents[3] / "config_template.yaml",
    ]
    return next((p for p in candidates if p.exists()), None)


@cli.command()
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
def init(config):
    # Initialize a new config.yaml file.
    target_path = Path(config) if config else Path("config.yaml")

    if target_path.exists() and not Confirm.ask(
        f"[warning]{target_path} already exists. Overwrite?[/warning]"
    ):
        console.print("Cancelled.")
        return

    template = _find_config_template()
    if template is not None:
        target_path.write_text(template.read_text(encoding="utf-8"), encoding="utf-8")
    else:
        import yaml

        target_path.write_text(
            yaml.dump(Settings().model_dump(), default_flow_style=False),
            encoding="utf-8",
        )

    console.print(f"[success]Created {target_path}[/success]")
    console.print(
        "Edit it to set your provider/model, or just run `dacli` to use the setup wizard."
    )


@cli.command()
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
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
    table.add_column("Errors")

    for s in session_list:
        table.add_row(
            s["session_id"],
            s["created_at"][:19] if s.get("created_at") else "?",
            s.get("active_task") or "—",
            str(s.get("errors_count", 0)),
        )

    console.print(table)


@cli.command()
@click.argument("session_id")
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
def load(session_id, config):
    """Load and resume a previous session."""
    asyncio.run(_run_chat(config, session_id))


@cli.command()
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--session", "-s", type=str, help="Session ID to inspect")
@click.option(
    "--task",
    "-t",
    type=str,
    help="Task to assemble context for (defaults to the last user message)",
)
@click.option(
    "--explain",
    is_flag=True,
    help="Print each context chunk with its source, timestamp and token cost",
)
def context(config, session, task, explain):
    """Inspect the assembled context (Context Constructor)."""
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

    working = [
        {"role": m.role, "content": m.content} for m in memory.get_full_history()
    ]
    if not task:
        task = next(
            (m["content"] for m in reversed(working) if m.get("role") == "user"),
            "(no task — provide --task)",
        )

    ctx = build(task, working, set())
    _print_context_explain(ctx, task, explain)


def _print_context_explain(ctx, task, explain: bool) -> None:
    # Render the assembled context for `dacli context --explain`.
    from dacli.context.tokenizer import make_counter

    console.print(
        Panel(
            Text(task, style="info"),
            title="[accent]Task[/accent]",
            border_style="border",
            padding=(0, 2),
        )
    )

    if explain:
        table = Table(
            title="[accent]Context chunks[/accent]",
            show_header=True,
            header_style="muted",
            border_style="border",
            box=None,
            padding=(0, 2, 0, 0),
        )
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
    budget_table = Table(
        title="[accent]Budget usage[/accent]",
        show_header=True,
        header_style="muted",
        border_style="border",
        box=None,
        padding=(0, 2, 0, 0),
    )
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
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--session", "-s", type=str, help="Only show decisions for this session")
@click.option(
    "--limit", "-n", type=int, default=20, help="Max number of decisions to show"
)
@click.option(
    "--full",
    is_flag=True,
    help="Show every event in each decision, not just the summary",
)
def audit(config, session, limit, full):
    """Reconstruct governance decisions: why the agent did (or didn't) act."""
    settings = load_config(config)
    gov = getattr(settings, "governance", None)
    state_dir = str(Path(settings.agent.state_path).parent)
    path = (
        (getattr(gov, "audit_path", None) or f"{state_dir}/audit.jsonl")
        if gov
        else f"{state_dir}/audit.jsonl"
    )

    ledger = AuditLedger(path=path)
    _print_audit(ledger, session, full=full, limit=limit, header=f"ledger: {path}")


def _print_audit(ledger, session_id, *, full=False, limit=20, header=None, target=None):
    # Render governance decisions grouped by action: classifier tier, policy
    # decision, rollback plan, approval, execution + post-condition verdict.
    con = (target or ui).console
    decisions = ledger.decisions(session_id=session_id)
    if not decisions:
        con.print("[dim]No governance decisions recorded yet.[/dim]")
        return
    decisions = decisions[-limit:]

    sub = header or f"session {session_id}"
    con.print(
        Panel(
            Text(f"{len(decisions)} decision(s)  ·  {sub}", style="info"),
            title="[accent]Audit[/accent]",
            border_style="border",
            padding=(0, 2),
        )
    )

    tier_style = {
        "safe": "success",
        "write": "info",
        "risky": "warning",
        "irreversible": "error",
    }
    icons = {
        "classification": "◆",
        "policy": "▸",
        "permission": "▸",
        "rollback": "↩",
        "shadow": "⧉",
        "approval": "?",
        "block": "✗",
        "execution": "→",
        "post_condition": "✓",
        "memory_write": "✎",
    }
    for dec in decisions:
        tier = dec.get("tier") or "?"
        head = Text()
        head.append(f"{dec.get('tool_name', '?')}  ", style="accent")
        head.append(f"[{tier}]", style=tier_style.get(tier, "muted"))
        head.append(f"   {dec.get('decision_id', '')}", style="muted")
        con.print(head)

        for ev in dec.get("events", []):
            kind = ev.get("kind", "")
            summary = ev.get("summary", "")
            style = "muted"
            if kind == "block" or (kind == "approval" and "denied" in summary):
                style = "error"
            elif kind == "post_condition":
                style = "success" if "passed" in summary else "error"
            elif kind == "approval":
                style = "success" if "approved" in summary else "warning"
            line = Text(f"   {icons.get(kind, '·')} ", style=style)
            line.append(f"{kind:<14}", style="step")
            line.append(summary, style=style)
            con.print(line)
            if full and ev.get("detail"):
                con.print(Text(f"        {ev['detail']}", style="muted"))
        con.print()


@cli.command(name="eval")
@click.option(
    "--quick", is_flag=True, help="Fast run: scale pass^k k down (destructive stays ≥2)"
)
@click.option(
    "--regression", is_flag=True, help="Diff against the previous run in history"
)
@click.option(
    "--calibrate", is_flag=True, help="Print data-driven threshold recommendations"
)
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output")
def eval_cmd(quick, regression, calibrate, as_json):
    """Run the reliability eval (pass^k) against the simulated platforms.

    Offline: deterministic simulated warehouses/object-stores, no credentials,
    no network, no cost. Reports pass^k per connector/skill, the destructive-
    action gate, and the reliability dashboard. Exits non-zero on an unguarded
    destructive execution or (with --regression) a detected regression.
    """
    from dacli.eval.__main__ import main as eval_main

    argv = []
    if quick:
        argv.append("--quick")
    if regression:
        argv.append("--regression")
    if calibrate:
        argv.append("--calibrate")
    if as_json:
        argv.append("--json")
    raise SystemExit(eval_main(argv))


def _load_settings_for_headless(config, *, offline):
    # Real runs need a configured LLM provider; an offline scripted run never
    # uses real credentials (the ScriptedLLM is injected), so when no config is
    # present fall back to a placeholder LLM config. This keeps hermetic CI and
    # AI-agent runs secret-free, which is the whole point of the scripted path.
    from dacli.config.settings import Settings

    try:
        return load_config(config)
    except Exception:
        if not offline:
            raise
        # Only the sub-settings with required fields need an explicit block; the
        # rest default. These placeholders are never used to reach a network.
        return Settings.model_validate({
            "llm": {"provider": "scripted", "model": "scripted",
                    "api_key": "scripted", "base_url": "https://api.test.local"},
            "github": {"token": "x"},
            "snowflake": {"account": "a", "user": "u", "password": "p",
                          "warehouse": "w", "role": "r", "database": "d"},
            "pinecone": {"api_key": "k", "index_name": "i", "environment": "e"},
            "embeddings": {"provider": "openai", "api_key": "k", "model": "m"},
        })


async def _run_headless_cli(
    *,
    inputs,
    config,
    session,
    approve,
    llm_script,
    no_connectors,
    max_iterations,
):
    # Shared driver for `run` and `replay`. Builds settings, optionally injects a
    # ScriptedLLM from a JSON/YAML file, and returns a HeadlessResult.
    import yaml

    from dacli.core.headless import run_headless
    from dacli.reasoning.scripted import ScriptedLLM

    settings = _load_settings_for_headless(config, offline=bool(llm_script))
    llm = None
    if llm_script:
        responses = yaml.safe_load(Path(llm_script).read_text(encoding="utf-8")) or []
        llm = ScriptedLLM(responses)
    return await run_headless(
        inputs=inputs,
        settings=settings,
        llm=llm,
        approve=approve,
        session_id=session,
        no_connectors=no_connectors,
        max_iterations=max_iterations,
    )


def _emit_headless(result, as_json):
    # Machine path: emit ONLY the JSON via click.echo (plain stdout, no Rich
    # styling/ANSI) so consumers can json.loads(stdout) safely. Human path: a
    # short themed summary.
    if as_json:
        click.echo(result.to_json())
    else:
        for i, turn in enumerate(result.turns, 1):
            console.print(f"[accent]turn {i}[/accent]: {turn.content or '(no text)'}")
            if turn.error:
                console.print(f"[error]error:[/error] {turn.error}")
        if result.scenario_error:
            console.print(f"[error]scenario error:[/error] {result.scenario_error}")
        console.print(f"[muted]exit {result.exit_code} · session {result.session_id}[/muted]")


@cli.command(name="run")
@click.argument("message")
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--session", "-s", type=str, help="Session ID to resume")
@click.option("--approve", type=click.Choice(["deny", "approve"]), default="deny",
              help="Approval policy for governed actions (default: deny = fail-safe)")
@click.option("--llm-script", type=click.Path(exists=True),
              help="JSON/YAML file of scripted LLM responses (offline, deterministic)")
@click.option("--no-connectors", is_flag=True, default=False,
              help="Disable external connectors (built-ins only) for a hermetic run")
@click.option("--max-iterations", type=int, default=None, help="Override the agent iteration cap")
@click.option("--json", "as_json", is_flag=True, help="Emit the machine-readable JSON result")
def run_cmd(message, config, session, approve, llm_script, no_connectors, max_iterations, as_json):
    """Run a single message through the agent headlessly and emit a JSON result."""
    result = asyncio.run(_run_headless_cli(
        inputs=[message], config=config, session=session, approve=approve,
        llm_script=llm_script, no_connectors=no_connectors, max_iterations=max_iterations,
    ))
    _emit_headless(result, as_json)
    raise SystemExit(result.exit_code)


@cli.command(name="replay")
@click.argument("scenario_file", type=click.Path(exists=True))
@click.option("--json", "as_json", is_flag=True, help="Emit the machine-readable JSON result")
def replay_cmd(scenario_file, as_json):
    """Replay a scenario file (ordered user turns + optional scripted LLM)."""
    import yaml

    from dacli.core.headless import run_headless
    from dacli.reasoning.scripted import ScriptedLLM

    scenario = yaml.safe_load(Path(scenario_file).read_text(encoding="utf-8")) or {}
    llm = None
    if scenario.get("llm_script"):
        llm = ScriptedLLM(scenario["llm_script"])
    settings = _load_settings_for_headless(
        scenario.get("config"), offline=bool(scenario.get("llm_script"))
    )
    result = asyncio.run(run_headless(
        inputs=list(scenario.get("turns") or []),
        settings=settings,
        llm=llm,
        approve=scenario.get("approve", "deny"),
        canned_inputs=scenario.get("inputs"),
        no_connectors=bool(scenario.get("no_connectors", True)),
        max_iterations=scenario.get("max_iterations"),
    ))
    _emit_headless(result, as_json)
    raise SystemExit(result.exit_code)


@cli.command()
@click.option("--output", "-o", type=click.Path(), help="Output file path")
def prompt(output):
    # View or edit the system prompt. Shows the exact live source (the composed
    # core.md) the agent runs on — one source of truth, no drift (07.E).
    current_prompt = get_default_system_prompt()

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
            if connector is None:
                continue
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
    catalog = registry.get_catalog()
    return [
        connector_id for connector_id in catalog
        if registry.is_connector_enabled(connector_id)
    ]


def _ctx_pct(memory) -> int:
    # Context-window fill: how full the rolling message window is (0-100).
    window = max(getattr(memory, "memory_window", 0) or 1, 1)
    used = min(len(memory.get_full_history()), window)
    return round(used / window * 100)


async def _run_chat(
    config_path: str | None, session_id: str | None, force_setup: bool = False
):
    # Run the interactive chat session.
    # Load configuration first so the UI is themed from the user's settings.
    settings = load_config(config_path)
    chat_ui = DacliUI(settings=settings, version=__version__, author=__author__)
    con = chat_ui.console

    chat_ui.banner()
    chat_ui.status("Loading configuration…")

    # First-run LLM bootstrap: with no usable provider/model/key, collect them
    # interactively (key -> encrypted store, rest -> config.yaml) before any
    # connector setup. Must run before the agent is built.
    if not is_llm_configured(settings):
        settings = collect_llm_credentials(
            con, settings, store_base_dir=str(Path(settings.agent.state_path).parent)
        )

    # Check if setup wizard should run
    registry = ConnectorRegistry(settings, config_path=CONNECTORS_CONFIG_PATH)

    if force_setup or not registry.setup_completed:
        con.print()
        if not registry.setup_completed:
            chat_ui.notice("First-time setup detected.", style="warning")
            run_wizard = Confirm.ask(
                "Would you like to configure which connectors to use?",
                default=True,
                console=con,
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

        # Reload settings so any secrets the wizard saved to dacli.json apply.
        settings = load_config(config_path)

    # Persistent project store (.dacli/dacli.json): startups, config snapshot, usage/cost
    store = DacliStore(base_dir=str(Path(settings.agent.state_path).parent))

    # Initialize memory
    memory = AgentMemory(
        state_path=settings.agent.state_path,
        history_path=settings.agent.history_path,
        memory_window=settings.agent.memory_window,
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
        con.print(
            Panel(
                question,
                title="[warning]input needed[/warning]",
                border_style="warning",
                padding=(1, 2),
            )
        )
        return Prompt.ask("[prompt]your response[/prompt]", console=con)

    def on_approval(request) -> bool:
        # Governance: a risky/irreversible action wants sign-off. Show
        # the blast radius, the classifier's reasoning, the rollback plan and any
        # dry-run / shadow diff, then ask. Default is NO (fail-safe).
        tier = getattr(getattr(request, "tier", None), "value", "?")
        border = "error" if tier == "irreversible" else "warning"
        con.print(
            Panel(
                Text(request.describe(), style="step"),
                title=f"[{border}]approval needed · {tier}[/{border}]",
                border_style=border,
                padding=(1, 2),
            )
        )
        return Confirm.ask(
            "[prompt]Proceed with this action?[/prompt]", console=con, default=False
        )

    # Initialize agent — UI methods wired directly as kernel callbacks.
    agent = DACLI(
        settings=settings,
        memory=memory,
        on_status_update=chat_ui.status,
        on_tool_start=chat_ui.tool_start,
        on_tool_end=chat_ui.tool_end,
        on_user_input_needed=on_user_input_needed,
        on_approval=on_approval,
        on_stream_start=chat_ui.on_stream_start,
        on_text=chat_ui.on_text,
        on_stream_end=chat_ui.on_stream_end,
        connectors_config_path=CONNECTORS_CONFIG_PATH,
        store=store,
    )

    # Initialize connections (the agent emits its own progress via on_status).
    con.print()
    try:
        if not await agent.initialize():
            chat_ui.error("Failed to initialize agent. Check your configuration.")
            return
    except Exception as e:
        chat_ui.error(f"Initialization error: {e}")
        chat_ui.notice(
            "Check your config.yaml and ensure credentials are set.", style="muted"
        )
        return

    # Persist startup + a secret-redacted snapshot of the effective config.
    store.record_startup()
    store.snapshot_config(settings)
    store.save()

    con.print()
    chat_ui.welcome(
        model=settings.llm.model,
        provider=settings.llm.provider,
        connectors=_enabled_connector_names(agent.registry),
        cwd=str(Path.cwd()),
    )

    # Surface any connectors the registry had to skip (bad manifest / import
    # error / failing operations()) so a broken — often freshly generated —
    # connector is visible instead of silently missing.
    failed = agent.registry.failed_connectors()
    if failed:
        for cid, reason in failed.items():
            chat_ui.notice(
                f"Connector '{cid}' was skipped: {reason}", style="warning"
            )
        chat_ui.notice(
            "Fix it with /debug-connector, then /import-connector to re-enable.",
            style="muted",
        )

    # Set up prompt toolkit for better input
    history_file = Path(settings.agent.history_path) / "input_history.txt"
    history_file.parent.mkdir(parents=True, exist_ok=True)

    def bottom_toolbar():
        from dacli.core.test_mode import test_mode as _tm

        return chat_ui.bottom_toolbar(
            provider=settings.llm.provider,
            model=settings.llm.model,
            connectors=_enabled_connector_names(agent.registry),
            ctx_pct=_ctx_pct(memory),
            session=memory.session_id,
            test_mode=_tm.toolbar_text(),
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
                    chat_ui.prompt_html(),
                )

                if not user_input.strip():
                    continue

                # Handle commands
                if user_input.startswith("/"):
                    cmd = user_input.lower().split()[0]
                    args = user_input.split()[1:] if len(user_input.split()) > 1 else []

                    if cmd in ("/exit", "/quit"):
                        break

                    if cmd == "/help":
                        chat_ui.help(CLI_COMMANDS)

                    elif cmd == "/init":
                        _init_dacli_md(settings)

                    elif cmd == "/status":
                        print_status(memory, chat_ui)

                    elif cmd == "/usage":
                        print_usage(
                            agent.store,
                            memory.session_id,
                            chat_ui,
                            pricing=agent._pricing,
                        )

                    elif cmd == "/context":
                        working = [
                            {"role": m.role, "content": m.content}
                            for m in memory.get_full_history()
                        ]
                        task_arg = (
                            " ".join(args)
                            if args
                            else next(
                                (
                                    m["content"]
                                    for m in reversed(working)
                                    if m.get("role") == "user"
                                ),
                                "(no task yet)",
                            )
                        )
                        ctx = agent._context["build"](task_arg, working, set())
                        _print_context_explain(ctx, task_arg, explain=True)

                    elif cmd == "/audit":
                        gov = getattr(agent, "governor", None)
                        ledger = getattr(gov, "ledger", None) if gov else None
                        if ledger is None:
                            chat_ui.notice(
                                "Governance is disabled — no audit ledger.",
                                style="muted",
                            )
                        else:
                            _print_audit(
                                ledger, memory.session_id, full=("full" in args)
                            )

                    elif cmd == "/history":
                        chat_ui.history(memory.get_full_history())

                    elif cmd == "/sessions":
                        chat_ui.sessions_table(memory.list_sessions())

                    elif cmd == "/load" and args:
                        if memory.load_session(args[0]):
                            chat_ui.notice(
                                f"Loaded session: {args[0]}", style="success"
                            )
                        else:
                            chat_ui.error(f"Session not found: {args[0]}")

                    elif cmd == "/export":
                        con.print(memory.export_state())

                    elif cmd == "/config":
                        chat_ui.config_table(settings)

                    elif cmd == "/theme":
                        if args and args[0].lower() in THEMES:
                            chat_ui.set_theme(args[0])
                            chat_ui.notice(
                                f"Theme set to '{args[0].lower()}'.", style="success"
                            )
                        else:
                            available = ", ".join(THEMES.keys())
                            chat_ui.notice(
                                f"Usage: /theme <name>  ·  available: {available}",
                                style="muted",
                            )

                    elif cmd == "/prompt":
                        prompt_content = get_default_system_prompt()
                        chat_ui.panel(
                            Markdown(prompt_content[:2000] + "…"),
                            title="[accent]System prompt[/accent]",
                        )

                    elif cmd == "/clear":
                        memory.clear_messages()
                        chat_ui.notice("Conversation cleared.", style="success")

                    elif cmd == "/cls":
                        # PowerShell-style screen clear: wipe the viewport but
                        # keep conversation history/state intact.
                        chat_ui.clear_screen(
                            header=f"dacli · {settings.llm.provider}·{settings.llm.model} "
                            f"· {memory.session_id}  (history kept — /clear to wipe it)"
                        )

                    elif cmd == "/reset":
                        if Confirm.ask(
                            "Reset the agent state?", console=con, default=False
                        ):
                            memory._state = memory._create_new_state()
                            memory.clear_messages()
                            chat_ui.notice("Agent state reset.", style="success")

                    elif cmd == "/tools":
                        chat_ui.connectors_table(agent.registry)

                    elif cmd == "/setup":
                        chat_ui.status("Running setup wizard…")
                        setup_registry = ConnectorRegistry(
                            settings, config_path=CONNECTORS_CONFIG_PATH
                        )
                        wizard = SetupWizard(
                            settings, setup_registry, CONNECTORS_CONFIG_PATH
                        )
                        connectors_config = await wizard.run()
                        save_connectors_config(
                            connectors_config, CONNECTORS_CONFIG_PATH
                        )
                        # Re-snapshot the (possibly updated) config into dacli.json.
                        store.snapshot_config(load_config(config_path))
                        store.save()
                        chat_ui.notice(
                            "Connectors reconfigured. Restart to apply.",
                            style="success",
                        )

                    elif cmd == "/connect":
                        from dacli.core.connect_flow import run_connect_flow

                        target_id = args[0] if args else None
                        connect_registry = ConnectorRegistry(
                            settings, config_path=CONNECTORS_CONFIG_PATH
                        )
                        ok, msg = await run_connect_flow(
                            con,
                            connect_registry,
                            settings,
                            store,
                            connector_id=target_id,
                            config_path=CONNECTORS_CONFIG_PATH,
                        )
                        style = "success" if ok else "warning"
                        chat_ui.notice(msg, style=style)
                        if ok:
                            settings = load_config(config_path)
                            store.snapshot_config(settings)
                            store.save()
                            chat_ui.notice(
                                "Restart dacli for the new credentials to take effect "
                                "in this session.",
                                style="muted",
                            )

                    elif cmd == "/new-connector":
                        from dacli.core.connector_generator import run_new_connector_flow

                        await run_new_connector_flow(
                            console=con,
                            settings=settings,
                            llm=agent.llm,
                            registry=agent.registry,
                            store=store,
                            config_path=CONNECTORS_CONFIG_PATH,
                        )
                        settings = load_config(config_path)

                    elif cmd == "/testmode":
                        from dacli.core.test_mode import test_mode

                        target = args[0] if args else None
                        if test_mode.toggle(connector_name=target):
                            scope = (
                                f"'{target}'" if target else "all non-built-in connectors"
                            )
                            chat_ui.notice(
                                f"Test mode ON — staging {scope}: calls are health-gated, "
                                "exceptions shown in full, and catalog effects suppressed.",
                                style="success",
                            )
                        else:
                            chat_ui.notice(
                                "Test mode OFF — connector calls run normally.",
                                style="warning",
                            )

                    elif cmd == "/import-connector":
                        from dacli.core.connector_workflow import import_connector

                        name = args[0] if args else None
                        ok, msg = await import_connector(
                            name=name,
                            console=con,
                            config_path=CONNECTORS_CONFIG_PATH,
                            settings=settings,
                        )
                        style = "success" if ok else "warning"
                        chat_ui.notice(msg, style=style)
                        if ok:
                            settings = load_config(config_path)

                    elif cmd == "/push-connector":
                        from dacli.core.connector_workflow import push_connector

                        name = args[0] if args else None
                        ok, msg = await push_connector(name=name, console=con)
                        style = "success" if ok else "warning"
                        chat_ui.notice(msg, style=style)

                    elif cmd == "/debug-connector":
                        from dacli.core.connector_workflow import debug_connector

                        name = args[0] if args else None
                        await debug_connector(
                            name=name,
                            console=con,
                            settings=settings,
                            llm=agent.llm,
                        )

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
                    chat_ui.notice(
                        "⏳ Agent is waiting for your input to continue.",
                        style="warning",
                    )

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
