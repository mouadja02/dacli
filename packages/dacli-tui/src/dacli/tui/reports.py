"""Console renderers shared by the Click commands and the chat slash commands.

These were inline in ``scripts/cli.py``; they take an explicit ``DacliUI`` /
console so the same renderer serves both the standalone commands (cli's module
UI) and the interactive chat (its themed UI).
"""

from __future__ import annotations

from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from dacli.tui.design import ASCII as ASCII_GLYPHS
from dacli.tui.design import tier_legend
from dacli.tui.ui import TIER_STYLE


def fmt_int(n) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def fmt_cost(c) -> str:
    try:
        c = float(c)
    except Exception:
        return "n/a"
    if c <= 0:
        return "$0.00"
    return f"${c:.4f}" if c < 0.01 else f"${c:.2f}"


def print_usage(store, session_id, target, pricing=None) -> None:
    # Token/cost usage through the themed UI: all-time totals, by model, this session.
    con = target.console
    summary = store.usage_summary(session_id)
    totals = summary["totals"]
    by_model = summary["byModel"]
    session = summary.get("session")

    t = Text()
    t.append("Startups    ", style="muted")
    t.append(f"{summary.get('numStartups', 0)}\n", style="accent")
    t.append("Requests    ", style="muted")
    t.append(f"{fmt_int(totals.get('requests', 0))}\n", style="accent")
    t.append("Input       ", style="muted")
    t.append(f"{fmt_int(totals.get('input', 0))} tok\n", style="info")
    t.append("Output      ", style="muted")
    t.append(f"{fmt_int(totals.get('output', 0))} tok\n", style="info")
    t.append("Cache read  ", style="muted")
    t.append(f"{fmt_int(totals.get('cache_read', 0))} tok\n", style="info")
    t.append("Cache write ", style="muted")
    t.append(f"{fmt_int(totals.get('cache_creation', 0))} tok\n", style="info")
    t.append("Total cost  ", style="muted")
    t.append(fmt_cost(totals.get("costUSD", 0)), style="success")
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
                fmt_int(b.get("requests", 0)),
                fmt_int(b.get("input", 0)),
                fmt_int(b.get("output", 0)),
                f"{fmt_int(b.get('cache_read', 0))}/{fmt_int(b.get('cache_creation', 0))}",
                fmt_cost(b.get("costUSD", 0)),
            )
        con.print(table)

    if session:
        s = Text()
        s.append("Model ", style="muted")
        s.append(f"{session.get('model', '?')}   ", style="accent")
        s.append("Reqs ", style="muted")
        s.append(f"{fmt_int(session.get('requests', 0))}   ", style="info")
        s.append("In/Out ", style="muted")
        s.append(
            f"{fmt_int(session.get('input', 0))}/{fmt_int(session.get('output', 0))} tok   ",
            style="info",
        )
        s.append("Cost ", style="muted")
        s.append(fmt_cost(session.get("costUSD", 0)), style="success")
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


def print_schema(target_ui, cache, object_name, connector=None) -> None:
    # Shared by `dacli schema` and the in-chat /schema command.
    matches = cache.find(object_name, connector=connector)
    if not matches:
        target_ui.notice(
            f"No catalog entry for '{object_name}'. The catalog fills in as the "
            "agent introspects — ask it about the object, or run /catalog to "
            "see what is known.",
            style="warning",
        )
        return
    for entry in matches:
        target_ui.schema_panel(entry)


def print_context_explain(ctx, task, explain: bool, con) -> None:
    # Render the assembled context for `dacli context --explain` and /context.
    from dacli.context.tokenizer import make_counter

    con.print(
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
        con.print(table)

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
    con.print(budget_table)

    sys_tokens = make_counter().count(ctx.system_prompt)
    con.print(
        f"[muted]System prompt:[/muted] {sys_tokens} tokens  ·  "
        f"[muted]Messages:[/muted] {len(ctx.messages)}  ·  "
        f"[muted]Tools disclosed:[/muted] {len(ctx.tools)}"
    )


def print_audit(
    ledger, session_id, target, *, full=False, limit=20, header=None
) -> None:
    # Render governance decisions grouped by action: classifier tier, policy
    # decision, rollback plan, approval, execution + post-condition verdict.
    con = target.console
    decisions = ledger.decisions(session_id=session_id)
    if not decisions:
        con.print(
            "[muted]No governance decisions yet — they appear after the "
            "agent's first state-changing action (reads don't need "
            "sign-off).[/muted]"
        )
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

    glyphs = getattr(target, "glyphs", None)
    con.print(tier_legend(getattr(glyphs, "dot", "·")))
    con.print()

    tier_style = TIER_STYLE  # shared with the approval panel (tui.ui)
    if glyphs is ASCII_GLYPHS:
        icons = {
            "classification": "*",
            "policy": ">",
            "permission": ">",
            "rollback": "<",
            "shadow": "#",
            "approval": "?",
            "block": "x",
            "execution": "->",
            "post_condition": "+",
            "memory_write": "w",
        }
    else:
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
