"""Interactive chat REPL.

Builds the agent/memory/UI, runs first-run bootstrap, then loops on input —
delegating ``/commands`` to :func:`dacli.tui.slash.dispatch` and everything else
to the agent. Extracted from ``scripts/cli.py`` so the Click surface stays thin.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from prompt_toolkit import PromptSession
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.history import FileHistory
from rich.panel import Panel
from rich.prompt import Confirm, Prompt

from dacli.config import CLI_COMMANDS
from dacli.config.settings import (
    invalidate_config_cache,
    is_llm_configured,
    load_config,
)
from dacli.connectors.registry import CONNECTORS_CONFIG_PATH
from dacli.core import __author__, __version__, paths
from dacli.core.host import DacliHost
from dacli.core.logging_setup import get_logger
from dacli.core.memory import AgentMemory
from dacli.core.onboarding import collect_llm_credentials, run_first_connection
from dacli.core.store import DacliStore
import dacli.tui.reports as reports
import dacli.tui.slash as slash
from dacli.tui import DacliUI

log = get_logger(__name__)


def _enabled_connector_names(registry) -> list:
    # Short connector names for the welcome card / status bar.
    catalog = registry.get_catalog()
    return [
        connector_id
        for connector_id in catalog
        if registry.is_connector_enabled(connector_id)
    ]


def _ctx_pct(memory, agent=None) -> int:
    # Context fill for the toolbar (0-100). Prefer the assembler's real budget
    # snapshot (cached once per turn by the context pipeline) so the number
    # reflects true token pressure; before the first turn assembles anything,
    # fall back to the rolling message-window proxy.
    try:
        last = agent._context["last_context"]() if agent is not None else None
        budget = getattr(last, "budget", None)
        if budget:
            used = sum(v.get("used", 0) for v in budget.values())
            cap = sum(v.get("cap", 0) for v in budget.values())
            if cap > 0:
                return min(100, max(0, round(used / cap * 100)))
    except Exception:
        # a toolbar glitch must never break the input loop
        log.debug("context-percent toolbar calc failed; falling back to window", exc_info=True)
    window = max(getattr(memory, "memory_window", 0) or 1, 1)
    used = min(len(memory.get_full_history()), window)
    return round(used / window * 100)


async def run_chat(
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

    # Persistent project store (.dacli/dacli.json): startups, config snapshot, usage/cost
    store = DacliStore(base_dir=str(Path(settings.agent.state_path).parent))
    # Onboarding is conversational now (M12): no connector wizard. Offer a first
    # connection once, after the host is built — it needs the live extension
    # registry and secret store. ``--setup`` forces the offer on any run.
    want_onboarding = force_setup or store.is_first_run()

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
        chat_ui.approval_panel(request)
        return Confirm.ask(
            "[prompt]Proceed with this action?[/prompt]", console=con, default=False
        )

    # Initialize the host (M09) — UI methods wired directly as kernel callbacks.
    agent = DacliHost(
        settings=settings,
        memory=memory,
        on_status_update=chat_ui.status,
        on_tool_start=chat_ui.tool_start,
        on_tool_end=chat_ui.tool_end,
        on_tool_progress=chat_ui.tool_progress,
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

    # Point the transcript log at the session spill store so /expand can fetch
    # an elided result back off-context instead of re-running the tool (P11).
    spill_store = getattr(agent, "_context", {}).get("store")
    if spill_store is not None:
        chat_ui.bind_result_store(spill_store)

    # Persist startup + a secret-redacted snapshot of the effective config.
    store.record_startup()
    store.snapshot_config(settings)
    store.save()

    # Conversational first-connection onboarding (M12). Skippable; declining
    # leaves an empty ~/.dacli untouched.
    if want_onboarding:
        con.print()
        run_first_connection(chat_ui, con, agent._ext_registry, agent.secrets)
        invalidate_config_cache()
        settings = load_config(config_path)

    con.print()
    resolved_config = paths.resolve_config_path(config_path)
    chat_ui.welcome(
        model=settings.llm.model,
        provider=settings.llm.provider,
        connectors=_enabled_connector_names(agent.registry),
        cwd=str(Path.cwd()),
        config=str(resolved_config) if resolved_config else None,
        state=str(paths.state_dir()),
    )

    # Surface any connectors the registry had to skip (bad manifest / import
    # error / failing operations()) so a broken — often freshly generated —
    # connector is visible instead of silently missing.
    failed = agent.registry.failed_connectors()
    if failed:
        for cid, reason in failed.items():
            chat_ui.notice(f"Connector '{cid}' was skipped: {reason}", style="warning")
        chat_ui.notice(
            "Fix it with /debug-connector, then /import-connector to re-enable.",
            style="muted",
        )

    # Mutable session state the slash handlers read and (for reloads) write.
    ctx = slash.ChatContext(
        ui=chat_ui,
        console=con,
        memory=memory,
        agent=agent,
        store=store,
        settings=settings,
        config_path=config_path,
    )

    # Set up prompt toolkit for better input
    history_file = Path(settings.agent.history_path) / "input_history.txt"
    history_file.parent.mkdir(parents=True, exist_ok=True)

    def _session_cost() -> str:
        # Live per-session $cost for the bottom bar; blank on any hiccup.
        # O(1) lookup — the toolbar recomputes this on every keystroke.
        try:
            return reports.fmt_cost(store.session_cost_usd(memory.session_id))
        except Exception:
            return ""

    def _warehouse_cost() -> str:
        # Live per-session warehouse $spend (P14), shown next to the LLM cost.
        # Blank until a governed warehouse action records an estimate.
        try:
            usd = store.session_warehouse_usd(memory.session_id)
            return reports.fmt_cost(usd) if usd else ""
        except Exception:
            return ""

    def bottom_toolbar():
        from dacli.core.test_mode import test_mode as _tm

        return chat_ui.bottom_toolbar(
            provider=ctx.settings.llm.provider,
            model=ctx.settings.llm.model,
            connectors=_enabled_connector_names(agent.registry),
            ctx_pct=_ctx_pct(memory, agent),
            session=memory.session_id,
            test_mode=_tm.toolbar_text(),
            cost=_session_cost(),
            wh_cost=_warehouse_cost(),
        )

    pt_session = PromptSession(
        history=FileHistory(str(history_file)),
        auto_suggest=AutoSuggestFromHistory(),
        completer=slash.SlashCommandCompleter(CLI_COMMANDS),
        complete_while_typing=True,
        key_bindings=slash.build_completion_keybindings(),
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

                # Slash commands run through the registry; everything else is a
                # message for the agent.
                if user_input.startswith("/"):
                    await slash.dispatch(ctx, user_input)
                    if ctx.should_exit:
                        break
                    continue

                # Process the message with the agent. The kernel streams text +
                # tool calls to the UI as it runs, so there is nothing to print
                # here on success — only errors / hand-offs need a notice.
                # (prompt_toolkit already leaves the typed "❯ …" line in the
                # scrollback, so we don't re-echo it.)
                con.print()
                if getattr(ctx.settings.ui, "show_header", False):
                    chat_ui.turn_header(
                        model=ctx.settings.llm.model, session=memory.session_id
                    )

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
