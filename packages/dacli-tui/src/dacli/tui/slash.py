"""Chat slash-command registry.

Each ``/command`` is a small handler ``async def(ctx, args)`` registered in
``HANDLERS``; :func:`dispatch` parses a line and runs the matching handler.
``HANDLERS`` and the autocomplete list (``dacli.config.CLI_COMMANDS``) are the
same source of truth — ``test_slash_registry`` asserts every advertised command
has a handler and vice versa.

``ChatContext`` carries the live session state handlers read and mutate (a few
commands reload ``settings`` after a wizard/connect/import). The interactive loop
that builds the context lives in ``tui.chat_session``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dacli.tui.reports as reports
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.key_binding import KeyBindings
from rich.markdown import Markdown
from rich.prompt import Confirm

from dacli.config import CLI_COMMANDS
from dacli.config.settings import invalidate_config_cache, load_config
from dacli.core import paths
from dacli.prompts.system_prompt import get_default_system_prompt
from dacli.tui.theme import THEMES

# Exit aliases handled directly by the loop (no panel to render).
EXIT_COMMANDS = frozenset({"/exit", "/quit"})


@dataclass
class ChatContext:
    """Live session state shared with the slash handlers."""

    ui: Any
    console: Any
    memory: Any
    agent: Any
    store: Any
    settings: Any
    config_path: str | None
    should_exit: bool = False


Handler = Callable[[ChatContext, list[str]], Awaitable[None]]
HANDLERS: dict[str, Handler] = {}


def command(*names: str) -> Callable[[Handler], Handler]:
    """Register ``fn`` as the handler for one or more ``/command`` names."""

    def deco(fn: Handler) -> Handler:
        for name in names:
            HANDLERS[name] = fn
        return fn

    return deco


async def dispatch(ctx: ChatContext, user_input: str) -> None:
    """Run the slash command in ``user_input`` (assumes it starts with ``/``)."""
    cmd = user_input.lower().split()[0]
    args = user_input.split()[1:]
    if cmd in EXIT_COMMANDS:
        ctx.should_exit = True
        return
    handler = HANDLERS.get(cmd)
    if handler is None:
        ctx.ui.notice(f"Unknown command: {cmd}", style="warning")
        return
    await handler(ctx, args)


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------
@command("/help")
async def _help(ctx, args):
    ctx.ui.help(CLI_COMMANDS)


@command("/keys")
async def _keys(ctx, args):
    ctx.ui.keys_panel()


@command("/init")
async def _init(ctx, args):
    _init_dacli_md(ctx)


@command("/status")
async def _status(ctx, args):
    ctx.ui.status_panel(ctx.memory)


@command("/doctor")
async def _doctor(ctx, args):
    from dacli.core.doctor import collect

    ctx.ui.doctor_panel(
        collect(ctx.settings, config_path=ctx.config_path, ping="ping" in args)
    )


@command("/usage")
async def _usage(ctx, args):
    reports.print_usage(
        ctx.agent.store, ctx.memory.session_id, ctx.ui, pricing=ctx.agent._get_pricing()
    )


@command("/context")
async def _context(ctx, args):
    working = [
        {"role": m.role, "content": m.content} for m in ctx.memory.get_full_history()
    ]
    task_arg = (
        " ".join(args)
        if args
        else next(
            (m["content"] for m in reversed(working) if m.get("role") == "user"),
            "(no task yet)",
        )
    )
    built = ctx.agent._context["build"](task_arg, working, set())
    reports.print_context_explain(built, task_arg, True, ctx.ui.console)


@command("/audit")
async def _audit(ctx, args):
    gov = getattr(ctx.agent, "governor", None)
    ledger = getattr(gov, "ledger", None) if gov else None
    if ledger is None:
        ctx.ui.notice("Governance is disabled — no audit ledger.", style="muted")
        return
    reports.print_audit(ledger, ctx.memory.session_id, ctx.ui, full=("full" in args))


@command("/why-failed")
async def _why_failed(ctx, args):
    from dacli.config.settings import ConnectorConfig
    from dacli.core.why_failed import explain_failure

    dag = args[0] if args else None
    source = "airflow" if dag else "dbt"
    project_dir = ConnectorConfig(ctx.settings, "dbt").get("project_dir", "") or "."
    gov = getattr(ctx.agent, "governor", None)
    explanation = await explain_failure(
        source=source, dispatcher=ctx.agent.dispatcher,
        lineage=getattr(gov, "lineage", None), dag=dag,
        dbt_project_dir=project_dir,
    )
    ctx.ui.why_failed_panel(explanation)


@command("/history")
async def _history(ctx, args):
    ctx.ui.history(ctx.memory.get_full_history())


@command("/find")
async def _find(ctx, args):
    if not args:
        ctx.ui.notice("Usage: /find <text>", style="muted")
        return
    ctx.ui.find(" ".join(args), ctx.memory.get_full_history())


@command("/last-error")
async def _last_error(ctx, args):
    ctx.ui.last_error()


@command("/expand")
async def _expand(ctx, args):
    if not args:
        ctx.ui.notice(
            "Usage: /expand <id>  (the id shown after a tool result, e.g. t3)",
            style="muted",
        )
        return
    ctx.ui.expand(args[0])


@command("/transcript")
async def _transcript(ctx, args):
    from dacli.tui import transcript_app

    if not transcript_app.is_available():
        # \[ keeps Rich from eating [tui] as a markup tag (notice renders markup).
        ctx.ui.notice(
            "Full-screen transcript needs the Textual extra: pip install dacli\\[tui]",
            style="warning",
        )
        return
    app = transcript_app.build_app(
        ctx.memory.get_full_history(), ctx.ui.transcript_log.records()
    )
    await app.run_async()


@command("/sessions")
async def _sessions(ctx, args):
    ctx.ui.sessions_table(ctx.memory.list_sessions())


@command("/catalog")
async def _catalog(ctx, args):
    entries = ctx.memory.catalog.list_objects(connector=args[0] if args else None)
    entries.sort(key=lambda e: (e.connector, e.object_type, e.key()))
    ctx.ui.catalog_table(entries)


@command("/schema")
async def _schema(ctx, args):
    if not args:
        ctx.ui.notice(
            "Usage: /schema <object>  (e.g. /schema orders or "
            "/schema db.schema.orders)",
            style="muted",
        )
        return
    reports.print_schema(ctx.ui, ctx.memory.catalog, args[0])


@command("/load")
async def _load(ctx, args):
    if not args:
        ctx.ui.notice("Usage: /load <session-id>", style="muted")
        return
    if ctx.memory.load_session(args[0]):
        ctx.ui.notice(f"Loaded session: {args[0]}", style="success")
    else:
        ctx.ui.error(f"Session not found: {args[0]}")


@command("/export")
async def _export(ctx, args):
    ctx.console.print(ctx.memory.export_state())


@command("/config")
async def _config(ctx, args):
    ctx.ui.config_table(ctx.settings)


@command("/theme")
async def _theme(ctx, args):
    if args and args[0].lower() in THEMES:
        ctx.ui.set_theme(args[0])
        ctx.ui.notice(f"Theme set to '{args[0].lower()}'.", style="success")
    else:
        available = ", ".join(THEMES.keys())
        ctx.ui.notice(
            f"Usage: /theme <name>  ·  available: {available}", style="muted"
        )


@command("/prompt")
async def _prompt(ctx, args):
    prompt_content = get_default_system_prompt()
    ctx.ui.panel(
        Markdown(prompt_content[:2000] + "…"),
        title="[accent]System prompt[/accent]",
    )
    ctx.ui.notice(
        "Built-in and read-only. Customize via `dacli init` "
        f"(DACLI.md) or the overlay at {paths.user_prompt_overlay()} "
        "(`dacli prompt --edit`).",
        style="muted",
    )


@command("/clear")
async def _clear(ctx, args):
    ctx.memory.clear_messages()
    ctx.ui.notice("Conversation cleared.", style="success")


@command("/cls")
async def _cls(ctx, args):
    # PowerShell-style screen clear: wipe the viewport but keep history/state.
    ctx.ui.clear_screen(
        header=f"dacli · {ctx.settings.llm.provider}·{ctx.settings.llm.model} "
        f"· {ctx.memory.session_id}  (history kept — /clear to wipe it)"
    )


@command("/reset")
async def _reset(ctx, args):
    if Confirm.ask("Reset the agent state?", console=ctx.console, default=False):
        ctx.memory._state = ctx.memory._create_new_state()
        ctx.memory.clear_messages()
        ctx.ui.notice("Agent state reset.", style="success")


@command("/tools")
async def _tools(ctx, args):
    ctx.ui.connectors_table(ctx.agent.registry)


@command("/setup")
async def _setup(ctx, args):
    # Onboarding is conversational now (M12): no connector wizard. Offer a first
    # connection through the same /connect path.
    from dacli.core.onboarding import run_first_connection

    run_first_connection(ctx.ui, ctx.console, ctx.agent._ext_registry, ctx.agent.secrets)
    invalidate_config_cache()
    ctx.settings = load_config(ctx.config_path)


@command("/connect")
async def _connect(ctx, args):
    from dacli.core.connect_extension import run_connect_extension_flow

    target = args[0] if args else None
    ok, msg = run_connect_extension_flow(
        ctx.console, ctx.agent._ext_registry, ctx.agent.secrets, extension=target
    )
    ctx.ui.notice(msg, style="success" if ok else "warning")
    if ok:
        invalidate_config_cache()
        ctx.settings = load_config(ctx.config_path)
        ctx.store.snapshot_config(ctx.settings)
        ctx.store.save()
        ctx.ui.notice(
            "Run /reload (or restart) for the new credentials to take effect.",
            style="muted",
        )


@command("/new-extension")
async def _new_extension(ctx, args):
    from dacli.core.generate import run_new_extension_flow

    await run_new_extension_flow(ctx.console, ctx.agent.llm, ctx.agent.ext_host)
    ctx.ui.notice("Tools refreshed.", style="success")


@command("/reload")
async def _reload(ctx, args):
    result = ctx.agent.ext_host.reload()
    ctx.ui.notice(f"Extensions: {result.report()}", style="success")


@command("/testmode")
async def _testmode(ctx, args):
    from dacli.core.test_mode import test_mode

    target = args[0] if args else None
    if test_mode.toggle(connector_name=target):
        scope = f"'{target}'" if target else "all non-built-in connectors"
        ctx.ui.notice(
            f"Test mode ON — staging {scope}: calls are health-gated, "
            "exceptions shown in full, and catalog effects suppressed.",
            style="success",
        )
    else:
        ctx.ui.notice("Test mode OFF — connector calls run normally.", style="warning")


def _init_dacli_md(ctx) -> None:
    """`/init`: draft a DACLI.md priors file from config (no secrets)."""
    from dacli.memory.priors import DACLI_PRIORS_FILE, generate_dacli_md

    target = Path(DACLI_PRIORS_FILE.name)
    if target.exists() and not Confirm.ask(
        f"[warning]{target} already exists. Overwrite?[/warning]", default=False
    ):
        ctx.console.print("[dim]Cancelled — DACLI.md left unchanged.[/dim]")
        return
    target.write_text(generate_dacli_md(ctx.settings), encoding="utf-8")
    ctx.console.print(
        f"[success]✓ Wrote {target}[/success] from config (secrets excluded)."
    )
    ctx.console.print(
        "[dim]Review/edit it — it loads as the top layer of context next session.[/dim]"
    )


# ---------------------------------------------------------------------------
# Autocomplete
# ---------------------------------------------------------------------------
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

    def get_completions(self, document, _complete_event):
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
