import asyncio
import os
import click

from pathlib import Path
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Confirm

from dacli.core import __author__, __version__, paths
from dacli.config.settings import (
    load_config,
    Settings,
)
from dacli.connectors.registry import (
    ConnectorRegistry,
    CONNECTORS_CONFIG_PATH,
)
from dacli.core.host import DacliHost
from dacli.core.logging_setup import get_logger, setup_logging
from dacli.core.memory import AgentMemory
from dacli.governance.audit import AuditLedger
from dacli.prompts.system_prompt import (
    get_default_system_prompt,
    save_system_prompt,
)
from dacli.tui import DacliUI
from dacli.tui import reports

# Re-exported so callers/tests keep importing these from here after the cli.py
# split (P10): the slash registry moved to tui.slash, the REPL to
# tui.chat_session, the renderers to tui.reports.
from dacli.tui.slash import SlashCommandCompleter  # noqa: F401
from dacli.tui.chat_session import run_chat, _ctx_pct  # noqa: F401

log = get_logger(__name__)

# Module-level UI for the standalone (non-chat) click commands. The interactive
# chat builds its own themed instance once settings are loaded.
ui = DacliUI(version=__version__, author=__author__)
console = ui.console


def _print_audit(ledger, session_id, *, full=False, limit=20, header=None, target=None):
    # Compat shim around tui.reports.print_audit (which takes an explicit UI).
    reports.print_audit(
        ledger, session_id, target or ui, full=full, limit=limit, header=header
    )


def _open_transcript(config, session):
    # `dacli --transcript`: browse a session's history full-screen. Tool records
    # are in-session only, so this view shows the persisted conversation.
    from dacli.tui import transcript_app

    if not transcript_app.is_available():
        console.print(
            "[warning]Full-screen transcript needs the Textual extra: "
            "pip install dacli\\[tui][/warning]"
        )
        return
    settings = load_config(config)
    memory = AgentMemory(
        state_path=settings.agent.state_path,
        history_path=settings.agent.history_path,
        memory_window=settings.agent.memory_window,
    )
    if session and not memory.load_session(session):
        console.print(f"[error]Session not found: {session}[/error]")
        return
    transcript_app.build_app(memory.get_full_history(), []).run()


# ============================================================
# CLI Commands
# ============================================================


@click.group(
    invoke_without_command=True,
    epilog="First time? Just run `dacli` — the setup wizard walks you through "
           "provider, model, key, and connectors.",
)
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--session", "-s", type=str, help="Session ID to resume")
@click.option("--version", "-v", is_flag=True, help="Show version")
@click.option("--setup", is_flag=True, help="Run the setup wizard")
@click.option("--debug", is_flag=True, help="Verbose DEBUG logging to .dacli/dacli.log "
                                            "(re-raises unexpected kernel errors)")
@click.option("--transcript", is_flag=True, help="Open the full-screen transcript "
                                                 "viewer for a session (needs dacli[tui])")
@click.pass_context
def cli(ctx, config, session, version, setup, debug, transcript):
    # DACLI: AI-powered Data Engineering Assistant
    # P02: --version short-circuits before any state setup so it touches no FS.
    # (--help is Click's eager option — it exits before this body runs.)
    if version:
        console.print(f"DACLI version {__version__}")
        return

    if transcript:
        _open_transcript(config, session)
        return

    # P06: configure the logging tree once, at the single CLI entry point.
    # --debug (or DACLI_DEBUG=1) flips the whole tree to DEBUG. The handler is
    # lazy, so nothing lands on disk until a WARNING+ record actually emits.
    setup_logging(debug=True if debug else None)

    # P02: one muted notice when we fall back to the global state dir — no
    # project here and no DACLI_STATE_PATH override pinning it elsewhere.
    if not os.environ.get(paths.STATE_PATH_ENV) and paths.project_root() is None:
        console.print(
            f"[muted]running outside a project — using global state at "
            f"{paths.user_config_dir()}[/muted]"
        )

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

    asyncio.run(run_chat(config_path, session_id, force_setup=force_setup))


@cli.command()
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--session", "-s", type=str, help="Session ID to resume")
def setup(config, session):
    """Start chat and walk through a first connection.

    Onboarding is conversational now (M12): there's no connector wizard. This is
    ``dacli chat --setup`` — it opens the agent and offers to /connect a seed.
    """
    asyncio.run(run_chat(config, session, force_setup=True))


def _find_config_template() -> Path | None:
    # The commented template ships in the wheel; prefer it over a bare defaults
    # dump. A cwd copy still wins so a checkout can edit it in place.
    from dacli.core.paths import packaged_asset

    candidates = [
        Path("config_template.yaml"),
        packaged_asset("scripts", "config_template.yaml"),
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
@click.option("--ping", is_flag=True, help="Probe the LLM with a bounded models/list call (off by default)")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def doctor(config, ping, as_json):
    """Diagnose config/state/log resolution, the LLM key, governance, sandbox,
    terminal and connector status. Offline unless --ping. Exits non-zero on a
    hard problem (no LLM key, config not found)."""
    from dacli.core.doctor import collect

    settings = load_config(config)
    diag = collect(settings, config_path=config, ping=ping)
    if as_json:
        import json

        click.echo(json.dumps(diag.to_dict(), indent=2, default=str))
    else:
        ui.doctor_panel(diag)
    raise SystemExit(0 if diag.ok else 1)


@cli.command()
@click.argument("object_name", metavar="OBJECT")
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def lineage(object_name, config, as_json):
    """Show known upstream producers and downstream consumers of OBJECT.

    Best-effort from the dbt manifest, cached view dependencies and orchestrator
    DAGs. Unknown lineage is not a "safe" signal — it just means nothing is
    recorded yet."""
    from dacli.memory.graph.lineage import build_project_lineage

    settings = load_config(config)
    store = build_project_lineage(settings)
    down = store.downstream(object_name)
    up = store.upstream(object_name)
    if as_json:
        import json

        click.echo(json.dumps({
            "object": object_name,
            "downstream": [n.to_dict() for n in down],
            "upstream": [n.to_dict() for n in up],
        }, indent=2))
    else:
        ui.lineage_panel(object_name, up, down)


@cli.command(name="why-failed")
@click.option("--source", type=click.Choice(["dbt", "airflow"]), default=None,
              help="Which platform's failure to explain (default: airflow if --dag, else dbt)")
@click.option("--dag", "dag_id", type=str, default=None, help="Airflow DAG id to inspect")
@click.option("--run", "run_id", type=str, default=None,
              help="A specific run id (default: the most recent failed run)")
@click.option("--apply", "apply_fix", is_flag=True,
              help="Route the proposed fix through the governance gate (approval-gated)")
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def why_failed(source, dag_id, run_id, apply_fix, config, as_json):
    """Explain why the most recent pipeline run failed, and propose a governed fix.

    Locates the failure (dbt run-results or an orchestrator connector), reads its
    logs read-only through the governed dispatcher, correlates it to the failing
    object via lineage, and prints a root cause plus a proposed fix. The fix is
    never applied unless --apply is given, and even then it runs through the
    normal classify → approve → verify → rollback gate.
    """
    from dacli.config.settings import ConnectorConfig
    from dacli.core.why_failed import explain_failure

    settings = load_config(config)
    source = source or ("airflow" if dag_id else "dbt")

    memory = AgentMemory(
        state_path=settings.agent.state_path,
        history_path=settings.agent.history_path,
        memory_window=settings.agent.memory_window,
    )
    # Build the agent (no initialize() -> no network) for its governed dispatcher
    # and lineage store — the same pattern as `diff` and `context`.
    agent = DacliHost(settings=settings, memory=memory)
    project_dir = ConnectorConfig(settings, "dbt").get("project_dir", "") or "."

    explanation = asyncio.run(explain_failure(
        source=source, dispatcher=agent.dispatcher,
        lineage=getattr(agent.governor, "lineage", None),
        dag=dag_id, run=run_id,
        dbt_project_dir=project_dir, apply=apply_fix,
    ))

    if as_json:
        click.echo(explanation.to_json())
    else:
        ui.why_failed_panel(explanation)
        if explanation.error and explanation.finding is None:
            console.print(f"[muted]{explanation.error}[/muted]")
    raise SystemExit(explanation.exit_code)


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
    asyncio.run(run_chat(config, session_id))


def _open_catalog(config):
    # The catalog cache the agent reads/writes (no agent construction needed).
    from dacli.memory.catalog import CatalogCache

    load_config(config)  # surface config warnings exactly like other commands
    return CatalogCache()


@cli.command()
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--connector", type=str, default=None, help="Filter by connector id")
def catalog(config, connector):
    """List known data objects from the catalog cache (F-6)."""
    entries = _open_catalog(config).list_objects(connector=connector)
    entries.sort(key=lambda e: (e.connector, e.object_type, e.key()))
    ui.catalog_table(entries)


@cli.command()
@click.argument("object_name")
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--connector", type=str, default=None, help="Filter by connector id")
def schema(object_name, config, connector):
    """Show cached columns/row-count/last-verified for one object (F-6)."""
    reports.print_schema(ui, _open_catalog(config), object_name, connector)


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
    agent = DacliHost(settings=settings, memory=memory)
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
    reports.print_context_explain(ctx, task, explain, console)


@cli.command(name="diff")
@click.argument("connector")
@click.argument("table_a")
@click.argument("table_b")
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--sample", "-n", type=int, default=100,
              help="Rows sampled per side for the null-rate / value comparison")
def diff_cmd(connector, table_a, table_b, config, sample):
    """Read-only data diff between TABLE_A and TABLE_B on CONNECTOR.

    Row-count delta, per-column null-rate delta over a bounded sample, and a
    sampled value comparison — all via the connector's governed query op.
    Never mutates anything (promotion is the agent-side `data_diff` skill with
    mode=promote, which is approval-gated).
    """
    settings = load_config(config)
    memory = AgentMemory(
        state_path=settings.agent.state_path,
        history_path=settings.agent.history_path,
        memory_window=settings.agent.memory_window,
    )
    # Build the agent (no initialize() -> no network) just for its governed
    # dispatcher — the same pattern as the `context` command.
    agent = DacliHost(settings=settings, memory=memory)
    result = asyncio.run(agent.dispatcher.execute("data_diff", {
        "connector": connector,
        "table_a": table_a,
        "table_b": table_b,
        "sample_size": sample,
        "mode": "diff",
    }))
    if not result.success:
        ui.error(f"diff failed: {result.error}")
        raise SystemExit(1)
    ui.diff_panel(result.data)


@cli.command(name="cost")
@click.argument("connector")
@click.option("--estimate", "estimate_sql", default=None,
              help="Estimate the cost of this SQL before running it")
@click.option("--session/--no-session", "want_session", default=True,
              help="Report recent warehouse spend from the platform history view")
@click.option("--limit", type=int, default=200, help="Rows of query history to aggregate")
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def cost_cmd(connector, estimate_sql, want_session, limit, config, as_json):
    """Estimate a query's cost and report this session's warehouse spend.

    Pre-run estimate reuses the connector's native estimator (BigQuery's
    dry-run); session cost reads the platform's history view (Snowflake
    QUERY_HISTORY / BigQuery INFORMATION_SCHEMA.JOBS / Databricks system tables)
    read-only through the governed dispatcher. Supports snowflake, bigquery,
    databricks.
    """
    from dacli.core import cost_advisor

    settings = load_config(config)
    memory = AgentMemory(
        state_path=settings.agent.state_path,
        history_path=settings.agent.history_path,
        memory_window=settings.agent.memory_window,
    )
    # Build the agent (no initialize() -> no network) for its governed dispatcher
    # — the same pattern as `diff` and `why-failed`.
    agent = DacliHost(settings=settings, memory=memory)

    estimate = None
    if estimate_sql:
        conn = agent.registry.get_connector(connector)
        if conn is None:
            ui.error(f"connector '{connector}' is not available")
            raise SystemExit(1)
        op = next((o.name for o in conn.operations()
                   if o.capability == f"{connector}.query"), f"execute_{connector}_query")
        estimate = asyncio.run(cost_advisor.estimate(conn, op, {"query": estimate_sql}))

    session = None
    if want_session:
        session = asyncio.run(cost_advisor.session_cost(connector, agent.dispatcher, limit=limit))

    if as_json:
        import json

        click.echo(json.dumps({
            "connector": connector,
            "estimate": estimate.to_dict() if estimate else None,
            "session": session.to_dict() if session else None,
        }, indent=2, default=str))
    else:
        ui.cost_panel(connector, estimate, session)


@cli.group(name="assert")
def assert_grp():
    """Author and run data-quality assertions with governed remediation (P14)."""


@assert_grp.command(name="define")
@click.argument("name")
@click.option("--connector", required=True, help="Connector id (e.g. bigquery, snowflake)")
@click.option("--table", required=True, help="Qualified table name")
@click.option("--metric", type=click.Choice(["null_rate", "row_count"]), required=True)
@click.option("--op", type=click.Choice([">", ">=", "<", "<=", "==", "!="]), required=True,
              help="Breach predicate: the condition that, when true, is a breach")
@click.option("--threshold", type=float, required=True)
@click.option("--column", default=None, help="Column for null_rate")
@click.option("--remediation-tool", default=None,
              help="Tool to propose on breach (default: dbt_run on the table's model)")
def assert_define(name, connector, table, metric, op, threshold, column, remediation_tool):
    """Save an assertion under the state dir (does not run it)."""
    from dacli.core.quality import Assertion, save_assertion

    a = Assertion(name=name, connector=connector, table=table, metric=metric,
                  op=op, threshold=threshold, column=column,
                  remediation_tool=remediation_tool)
    problem = a.validate()
    if problem:
        console.print(f"[error]{problem}[/error]")
        raise SystemExit(1)
    save_assertion(a)
    console.print(f"[success]Saved assertion '{name}'[/success]  [muted]{a.describe()}[/muted]")


@assert_grp.command(name="list")
def assert_list():
    """List saved assertions."""
    from dacli.core.quality import load_assertions

    store = load_assertions()
    if not store:
        console.print("[muted]No assertions saved. Define one with `dacli assert define`.[/muted]")
        return
    table = Table(show_header=True, header_style="muted", box=None)
    table.add_column("name", style="accent")
    table.add_column("connector", style="info")
    table.add_column("predicate", style="step")
    for a in store.values():
        table.add_row(a.name, a.connector, a.describe())
    console.print(table)


@assert_grp.command(name="delete")
@click.argument("name")
def assert_delete(name):
    """Delete a saved assertion."""
    from dacli.core.quality import delete_assertion

    if delete_assertion(name):
        console.print(f"[success]Deleted '{name}'[/success]")
    else:
        console.print(f"[muted]No assertion named '{name}'[/muted]")
        raise SystemExit(1)


@assert_grp.command(name="run")
@click.argument("name", required=False)
@click.option("--apply", "apply_fix", is_flag=True,
              help="Route a breach's proposed fix through the governance gate")
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def assert_run(name, apply_fix, config, as_json):
    """Run one saved assertion (or all), measuring its metric through the governed path.

    A breach proposes a governed remediation; with --apply it runs through the
    normal classify → approve → verify → rollback gate. Exits non-zero on a
    breach (or a read error) so CI can gate on data quality.
    """
    from dacli.core.quality import evaluate, load_assertions

    settings = load_config(config)
    store = load_assertions()
    chosen = [store[name]] if name and name in store else (
        [] if name else list(store.values()))
    if name and name not in store:
        console.print(f"[error]No assertion named '{name}'[/error]")
        raise SystemExit(1)
    if not chosen:
        console.print("[muted]No assertions to run. Define one with `dacli assert define`.[/muted]")
        return

    memory = AgentMemory(
        state_path=settings.agent.state_path,
        history_path=settings.agent.history_path,
        memory_window=settings.agent.memory_window,
    )
    # Build the agent (no initialize() -> no network) just for its governed
    # dispatcher — the same pattern as `diff` and `why-failed`.
    agent = DacliHost(settings=settings, memory=memory)
    outcomes = [
        asyncio.run(evaluate(a, agent.dispatcher, apply=apply_fix)) for a in chosen
    ]
    if as_json:
        import json

        click.echo(json.dumps([o.to_dict() for o in outcomes], indent=2, default=str))
    else:
        ui.assertion_panel(outcomes)
    raise SystemExit(max(o.exit_code for o in outcomes))


def _parse_kv(pairs: tuple[str, ...]) -> dict[str, str]:
    out: dict[str, str] = {}
    for pair in pairs:
        key, _, value = pair.partition("=")
        out[key.strip()] = value
    return out


@cli.group(name="runbook")
def runbook_grp():
    """Save and run governed, parameterized headless tasks (P14)."""


@runbook_grp.command(name="save")
@click.argument("name")
@click.option("--turn", "turns", multiple=True, required=True,
              help="A user message (repeatable). Use {param} placeholders.")
@click.option("--tool", "tools", multiple=True,
              help="Tool name pre-approved within the envelope (repeatable; '*' = any)")
@click.option("--max-tier", type=click.Choice(["safe", "write", "risky", "irreversible"]),
              default="write", help="Blast-radius ceiling for the envelope")
@click.option("--param", "params", multiple=True, help="Default param k=v (repeatable)")
@click.option("--connectors/--no-connectors", "connectors", default=False,
              help="Allow external connectors (default: built-ins only)")
def runbook_save(name, turns, tools, max_tier, params, connectors):
    """Persist a runbook with its policy envelope."""
    from dacli.core.runbooks import PolicyEnvelope, Runbook, save_runbook

    rb = Runbook(
        name=name, turns=list(turns), params=_parse_kv(params),
        envelope=PolicyEnvelope(tools=list(tools), max_tier=max_tier),
        no_connectors=not connectors,
    )
    save_runbook(rb)
    console.print(
        f"[success]Saved runbook '{name}'[/success]  [muted]{len(rb.turns)} turn(s), "
        f"envelope tools={list(tools) or '∅'} max_tier={max_tier}[/muted]")


@runbook_grp.command(name="list")
def runbook_list():
    """List saved runbooks."""
    from dacli.core.runbooks import list_runbooks

    names = list_runbooks()
    if not names:
        console.print("[muted]No runbooks saved. Create one with `dacli runbook save`.[/muted]")
        return
    for n in names:
        console.print(f"  [accent]{n}[/accent]")


@runbook_grp.command(name="show")
@click.argument("name")
def runbook_show(name):
    """Show a runbook's turns and policy envelope."""
    from dacli.core.runbooks import load_runbook

    rb = load_runbook(name)
    if rb is None:
        console.print(f"[error]No runbook named '{name}'[/error]")
        raise SystemExit(1)
    env = rb.envelope
    console.print(f"[accent]{rb.name}[/accent]")
    for i, t in enumerate(rb.turns, 1):
        console.print(f"  [muted]turn {i}:[/muted] {t}")
    if rb.params:
        console.print(f"  [muted]params:[/muted] {rb.params}")
    console.print(f"  [muted]envelope:[/muted] tools={env.tools or '∅'} max_tier={env.max_tier}")


@runbook_grp.command(name="run")
@click.argument("name")
@click.option("--param", "params", multiple=True, help="Override param k=v (repeatable)")
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--llm-script", type=click.Path(exists=True),
              help="JSON/YAML scripted LLM responses (offline, deterministic)")
@click.option("--json", "as_json", is_flag=True, help="Emit the machine-readable JSON result")
def runbook_run(name, params, config, llm_script, as_json):
    """Run a saved runbook headlessly under its policy envelope.

    In-envelope actions auto-approve; anything outside the envelope still
    prompts (and on this non-interactive path, blocks — fail-closed). The
    envelope and every decision land in the audit ledger.
    """
    from dacli.core.runbooks import load_runbook, run_runbook
    from dacli.ai.scripted import ScriptedLLM

    rb = load_runbook(name)
    if rb is None:
        console.print(f"[error]No runbook named '{name}'[/error]")
        raise SystemExit(1)
    settings = _load_settings_for_headless(config, offline=bool(llm_script))
    llm = None
    if llm_script:
        import yaml

        llm = ScriptedLLM(yaml.safe_load(Path(llm_script).read_text(encoding="utf-8")) or [])
    result = asyncio.run(run_runbook(rb, settings=settings, params=_parse_kv(params), llm=llm))
    _emit_headless(result, as_json)
    raise SystemExit(result.exit_code)


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


@cli.group()
def connector():
    """Manage shared connectors from a community index (F-8)."""


@connector.command(name="install")
@click.argument("name")
@click.option("--index", "index_source", required=True,
              help="Connector index: a local path or http(s) URL to a "
                   "JSON/YAML file with a 'connectors' mapping")
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--force", is_flag=True, help="Overwrite an existing connectors/<name>/")
def connector_install(name, index_source, config, force):
    """Fetch a connector from an index into connectors/<name>/ (disabled).

    The download is validated in a sandboxed subprocess and registered
    disabled; run /connect <name> and restart dacli to enable it.
    """
    from dacli.core.connector_index import install_connector

    settings = load_config(config)
    ok, msg = install_connector(name, settings, index_source, force=force)
    console.print(f"[{'success' if ok else 'error'}]{msg}[/{'success' if ok else 'error'}]")
    if not ok:
        raise SystemExit(1)


@cli.command(name="export-run")
@click.option("--config", "-c", type=click.Path(), help="Path to config.yaml file")
@click.option("--session", "-s", type=str, default=None,
              help="Session ID to export (defaults to the most recent session)")
@click.option("--out", "-o", type=click.Path(), default=None,
              help="Output zip path (defaults to dacli_run_<session>.zip)")
def export_run(config, session, out):
    """Export a session as a compliance bundle (transcript + audit + usage).

    The zip contains history.json, state.json, the session's audit-ledger
    slice, the usage summary and a manifest. Secret-keyed values are redacted.
    """
    from dacli.core.export_run import export_run_bundle

    settings = load_config(config)
    try:
        manifest = export_run_bundle(settings, session, out)
    except FileNotFoundError as exc:
        console.print(f"[error]{exc}[/error]")
        raise SystemExit(1) from exc
    console.print(
        f"[success]Exported session {manifest['session_id']} → "
        f"{manifest['path']}[/success]"
    )
    console.print(
        f"[muted]contents: {', '.join(manifest['contents'])}  ·  "
        f"{manifest['counts']['messages']} message(s), "
        f"{manifest['counts']['audit_events']} audit event(s)[/muted]"
    )


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
@click.option("--report", "report_path", type=click.Path(), default=None,
              help="Write a shareable reliability report (.md or .html, inferred from the extension)")
def eval_cmd(quick, regression, calibrate, as_json, report_path):
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
    if report_path:
        argv.extend(["--report", report_path])
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
    from dacli.ai.scripted import ScriptedLLM

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
    from dacli.ai.scripted import ScriptedLLM

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
@click.option("--output", "-o", type=click.Path(), help="Export the composed prompt to a file")
@click.option("--edit", is_flag=True, help="Create the editable overlay (if missing) and open it")
def prompt(output, edit):
    # View or customize the system prompt. The composed prompt (core.md + overlay)
    # is the live source the agent runs on — one source of truth, no drift (07.E).
    current_prompt = get_default_system_prompt()
    overlay = paths.user_prompt_overlay()

    if edit:
        if not overlay.exists():
            save_system_prompt(current_prompt)
            console.print(f"[success]Created overlay {overlay}[/success]")
        else:
            console.print(f"[info]Overlay already exists: {overlay}[/info]")
        editor = os.environ.get("VISUAL") or os.environ.get("EDITOR")
        if editor:
            click.edit(filename=str(overlay))
        else:
            console.print("[dim]Set $EDITOR to open it automatically.[/dim]")
        return

    if output:
        save_system_prompt(current_prompt, output)
        console.print(f"[success]Saved system prompt to {output}[/success]")
    else:
        md = Markdown(current_prompt)
        console.print(Panel(md, title="System Prompt", border_style="cyan"))
        console.print("\n[dim]The prompt is built-in and read-only.[/dim]")
        console.print("[dim]To customize: run `dacli init` for editable DACLI.md priors,[/dim]")
        console.print(f"[dim]or `dacli prompt --edit` to edit the overlay at {overlay}.[/dim]")


# ============================================================
# Async Functions
# ============================================================


async def _check_one_connection(registry, connector_id: str, info: dict) -> str | None:
    # Health-check one connector; return its rendered result line(s).
    label = f"{info['icon']} {info['name']}"
    connector = registry.get_connector(connector_id)
    if connector is None:
        return None
    try:
        result = await connector.health()
        if result.success:
            line = f"[success]✅ {label}: Connected[/success]\n   {result.data}"
        else:
            line = f"[error]❌ {label}: {result.error}[/error]"
        await connector.disconnect()
    except Exception as e:
        return f"[warning]⚠️ {label}: {e}[/warning]"
    return line


async def _validate_connections(settings: Settings):
    # Validate all discovered connectors via a live health check, run
    # concurrently so total wait is the slowest check, not the sum. Results
    # are printed in catalog order after the gather.
    registry = ConnectorRegistry(settings, config_path=CONNECTORS_CONFIG_PATH)
    catalog = registry.get_catalog()

    with console.status("[bold green]Testing connections..."):
        lines = await asyncio.gather(
            *(_check_one_connection(registry, cid, info) for cid, info in catalog.items())
        )
    for line in lines:
        if line is not None:
            console.print(line)



# ============================================================
# Entry Point
# ============================================================


def main():
    """CLI entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
