"""Panel surfaces: approval, plan, diff, catalog, status, schema, sessions, help."""
from __future__ import annotations

from typing import Any
from collections.abc import Iterable

from rich.console import Group, RenderableType
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from dacli.tui.design import ASCII as ASCII_GLYPHS
from dacli.tui.design import SPACING, TIER_STYLE, tier_legend
from dacli.tui.tables import Col, responsive_table
from dacli.tui.render_util import _unified_diff_text


class PanelsMixin:
    """All the panel/table renderers composed onto DacliUI."""

    def approval_panel(self, request) -> None:
        """Render a governance approval request (or a DAG plan) for sign-off.

        Structured when the request carries a dry-run preview / shadow diff or
        is a plan; plain text otherwise. A malformed request can never raise —
        it falls back to ``describe()`` text, then to ``str()``.
        """
        tier = getattr(getattr(request, "tier", None), "value", "?")
        # Border weight follows blast radius: irreversible screams, a safe
        # action stays lightweight. The strongest tier always wins visually.
        border = {
            "irreversible": "error",
            "risky": "warning",
            "write": "info",
            "safe": "border",
        }.get(tier, "warning")
        try:
            body = self._approval_body(request, tier)
        except Exception:
            describe = getattr(request, "describe", None)
            try:
                text = str(describe()) if callable(describe) else str(request)
            except Exception:
                text = str(request)
            body = Text(text, style="step")

        decision = Text()
        decision.append("Proceed?  ", style="bold" if tier == "irreversible" else "prompt")
        decision.append("[y]es / [N]o", style="accent")
        decision.append("  (No is the safe default)", style="muted")

        legend = Text("tiers  ", style="muted")
        legend.append_text(tier_legend(self.glyphs.dot))

        tier_style = TIER_STYLE.get(tier, "muted")
        self.console.print(
            Panel(
                Group(body, Text(), legend, Text(), decision),
                title=(
                    f"[{border}]approval needed[/{border}] {self.glyphs.dot} "
                    f"[{tier_style}]{tier}[/{tier_style}]"
                ),
                title_align="left",
                box=self.glyphs.box,
                border_style=border,
                padding=SPACING["panel_pad"],
            )
        )

    def _approval_body(self, request, tier: str) -> RenderableType:
        describe = getattr(request, "describe", None)
        if not callable(describe):
            # A DAG plan (plan-approve-execute) renders its inspectable text.
            render = getattr(request, "render", None)
            if callable(render):
                return Text(str(render()), style="step")
            return Text(str(request), style="step")

        grid = Table.grid(padding=(0, 2, 0, 0))
        grid.add_column(style="muted", no_wrap=True)
        grid.add_column()
        grid.add_row(
            "Action", Text(str(getattr(request, "tool_name", "?")), style="accent")
        )
        tier_text = Text(tier, style=TIER_STYLE.get(tier, "muted"))
        cls = getattr(request, "classification", None)
        if getattr(cls, "is_prod", False):
            tier_text.append(f"  (PROD: {cls.prod_marker})", style="error")
        grid.add_row("Blast radius", tier_text)
        reasons = "; ".join(getattr(cls, "reasons", None) or [])
        if reasons:
            grid.add_row("Why", Text(reasons, style="step"))
        policy = getattr(request, "policy", None)
        if policy is not None:
            decision = getattr(getattr(policy, "decision", None), "value", "?")
            source = getattr(policy, "source", "?")
            grid.add_row("Decision", Text(f"{decision}  [{source}]", style="step"))
        plan = getattr(request, "rollback_plan", None)
        if plan is not None:
            rollback = Text(str(getattr(plan, "strategy", "?")), style="step")
            if getattr(plan, "primitive", None) not in ("noop", "none", None):
                rollback.append(
                    f"  (verified: {getattr(plan, 'verify_detail', '')})",
                    style="muted",
                )
            grid.add_row("Rollback", rollback)
        estimate = getattr(request, "cost_estimate", None)
        if estimate:
            bits = []
            if estimate.get("bytes") is not None:
                bits.append(f"{estimate['bytes']:,} bytes scanned")
            if estimate.get("credits") is not None:
                bits.append(f"{estimate['credits']} credits")
            if estimate.get("usd") is not None:
                bits.append(f"≈ ${estimate['usd']:,.2f}")
            grid.add_row("Est. cost", Text("  ·  ".join(bits), style="warning"))

        parts: list[RenderableType] = [grid]
        preview = getattr(request, "dry_run_preview", None)
        if preview:
            parts.append(Text("Dry-run preview", style="muted"))
            parts.append(
                Syntax(
                    str(preview).strip(),
                    "sql",
                    theme=self.theme.code_theme,
                    word_wrap=True,
                    background_color="default",
                )
            )
        shadow = getattr(request, "shadow", None)
        if shadow is not None and getattr(shadow, "ran", False):
            diff = getattr(shadow, "diff", None) or {}
            if "before" in diff or "after" in diff:
                # Textual before/after from the shadow run → red/green diff
                # (the same renderer `dacli diff` and dry-run previews use).
                parts.append(Text("Shadow diff (on a clone)", style="muted"))
                parts.append(
                    _unified_diff_text(
                        str(diff.get("before", "")), str(diff.get("after", ""))
                    )
                )
            elif "rows_before" in diff and "rows_after" in diff:
                parts.append(self._shadow_delta_table(diff))
            else:
                parts.append(Text(f"Shadow: {shadow.summary()}", style="step"))
        return Group(*parts) if len(parts) > 1 else parts[0]

    def _shadow_delta_table(self, diff: dict[str, Any]) -> Table:
        # Tiny before/after table for a shadow row-count delta.
        delta = diff.get("row_delta")
        if delta is None:
            try:
                delta = diff["rows_after"] - diff["rows_before"]
            except Exception:
                delta = "?"
        table = Table(
            title="[muted]Shadow run (on a clone)[/muted]",
            title_justify="left",
            show_header=True,
            header_style="muted",
            box=None,
            padding=(0, 2, 0, 0),
        )
        table.add_column("rows before", justify="right", style="info")
        table.add_column("rows after", justify="right", style="info")
        table.add_column(self.glyphs.delta, justify="right", style="accent")
        table.add_row(str(diff["rows_before"]), str(diff["rows_after"]), str(delta))
        return table

    # ------------------------------------------------------------------
    # Long-running-work feedback (M4): progress, plan tree, text diff
    # ------------------------------------------------------------------
    def plan_tree(self, dag: Any) -> None:
        """Render a :class:`TaskDAG` as a status tree (reused by `dacli plan`).

        Per node: a status icon (pending/running/done/paused/failed, color
        paired with the glyph), its dependencies, and the irreversible /
        breadth-first markers in the shared tier palette. Malformed DAGs
        render best-effort — never raise.
        """
        from rich.tree import Tree

        g = self.glyphs
        try:
            goal = str(getattr(dag, "goal", "") or "plan")
            nodes = list(getattr(dag, "nodes", None) or [])
        except Exception:
            self.console.print(Text(str(dag), style="step"))
            return

        icon_style = {
            "pending": (g.pending, "muted"),
            "running": (g.running, "info"),
            "completed": (g.ok, "success"),
            "failed": (g.err, "error"),
            "paused": (g.paused, "warning"),
        }
        tree = Tree(Text(goal, style="accent"), guide_style="border")
        branches: dict[str, Any] = {}
        for node in nodes:
            try:
                status = getattr(getattr(node, "status", None), "value", None) or str(
                    getattr(node, "status", "pending")
                )
                icon, style = icon_style.get(status, (g.pending, "muted"))
                label = Text()
                label.append(f"{icon} ", style=style)
                label.append(str(getattr(node, "description", node)), style="step")
                deps = list(getattr(node, "depends_on", None) or [])
                if getattr(node, "irreversible", False):
                    label.append("  irreversible", style=TIER_STYLE["irreversible"])
                if getattr(node, "breadth_first", False):
                    items = list(getattr(node, "items", None) or [])
                    mult = "x" if g is ASCII_GLYPHS else "×"
                    label.append(
                        f"  [breadth-first {mult}{len(items) or '?'}]", style="info"
                    )
                if len(deps) > 1:
                    label.append(
                        f"  (also after {', '.join(deps[1:])})", style="muted"
                    )
                parent = branches.get(deps[0]) if deps else tree
                branch = (parent if parent is not None else tree).add(label)
                node_id = str(getattr(node, "id", "") or "")
                if node_id:
                    branches[node_id] = branch
            except Exception:
                continue
        self.console.print(tree)
        self.console.print()

    def text_diff(
        self,
        before: str,
        after: str,
        *,
        title: str = "diff",
        from_label: str = "before",
        to_label: str = "after",
    ) -> None:
        """Render a red/green unified diff panel (shadow previews, `dacli diff`)."""
        body = _unified_diff_text(
            before, after, from_label=from_label, to_label=to_label
        )
        self.console.print(
            Panel(
                body,
                title=f"[accent]{title}[/accent]",
                title_align="left",
                box=self.glyphs.box,
                border_style="border",
                padding=SPACING["panel_pad"],
            )
        )

    def diff_panel(self, data: dict[str, Any]) -> None:
        """Render a data-diff result (`dacli diff` / the data-diff skill)."""
        data = data or {}
        a, b = data.get("row_count_a", "?"), data.get("row_count_b", "?")
        delta = data.get("row_delta", "?")

        counts = Table(
            show_header=True, header_style="muted", box=None, padding=(0, 2, 0, 0)
        )
        counts.add_column(data.get("table_a", "a"), justify="right", style="info")
        counts.add_column(data.get("table_b", "b"), justify="right", style="info")
        counts.add_column(f"{self.glyphs.delta} rows", justify="right", style="accent")
        counts.add_row(str(a), str(b), str(delta))

        parts: list[RenderableType] = [counts]
        changed = [
            c for c in (data.get("columns") or []) if c.get("delta")
        ]
        if changed:
            cols = Table(
                title="[muted]null-rate deltas (sampled)[/muted]",
                title_justify="left",
                show_header=True, header_style="muted", box=None,
                padding=(0, 2, 0, 0),
            )
            cols.add_column("column", style="step")
            cols.add_column("null% a", justify="right", style="info")
            cols.add_column("null% b", justify="right", style="info")
            cols.add_column(self.glyphs.delta, justify="right", style="warning")
            for c in changed:
                cols.add_row(
                    str(c.get("name")),
                    f"{c.get('null_rate_a', 0):.1%}",
                    f"{c.get('null_rate_b', 0):.1%}",
                    f"{c.get('delta', 0):+.1%}",
                )
            parts.append(cols)

        sample = data.get("sample") or {}
        summary = Text()
        summary.append(
            f"sample: {sample.get('rows_compared', 0)} row(s) compared, ",
            style="muted",
        )
        differing = sample.get("rows_differing", 0)
        summary.append(
            f"{differing} differing",
            style="warning" if differing else "success",
        )
        parts.append(summary)
        if data.get("method"):
            parts.append(Text(str(data["method"]), style="muted"))

        self.console.print(
            Panel(
                Group(*parts),
                title=(
                    f"[accent]data diff[/accent] {self.glyphs.dot} "
                    "[muted]read-only[/muted]"
                ),
                title_align="left",
                box=self.glyphs.box,
                border_style="border",
                padding=SPACING["panel_pad"],
            )
        )

    # ------------------------------------------------------------------
    # Slash-command tables
    # ------------------------------------------------------------------
    def help(self, commands: Iterable) -> None:
        table = Table(
            title="[accent]Commands[/accent]",
            show_header=True,
            header_style="muted",
            border_style="border",
            box=None,
            padding=(0, 2, 0, 0),
        )
        table.add_column("Command", style="accent", no_wrap=True)
        table.add_column("Description", style="step")
        for cmd, desc in commands:
            table.add_row(cmd, desc)
        self.console.print(table)
        self.console.print()

    def keys_panel(self) -> None:
        """`/keys`: the TUI keybinding map, so shortcuts are discoverable."""
        table = Table(show_header=False, box=None, padding=(0, 2, 0, 0))
        table.add_column("Key", style="accent", no_wrap=True)
        table.add_column("Action", style="step")
        for key, action in (
            ("Tab / Shift-Tab", "Open / step through slash-command completions"),
            (self.glyphs.arrows, "Browse input history"),
            ("Ctrl-R", "Reverse-search input history"),
            ("Ctrl-C", "Interrupt the running turn"),
            ("Enter", "Send the message"),
            ("paste", "Pasted text keeps its newlines (multiline message)"),
            ("/help", "List all slash commands"),
        ):
            table.add_row(key, action)
        self.console.print(
            Panel(
                table,
                title="[accent]Keyboard shortcuts[/accent]",
                title_align="left",
                box=self.glyphs.box,
                border_style="border",
                padding=SPACING["panel_pad"],
            )
        )
        self.console.print()

    def connectors_table(self, registry) -> None:
        table = Table(
            title="[accent]Connectors[/accent]",
            show_header=True,
            header_style="muted",
            border_style="border",
            box=None,
            padding=(0, 2, 0, 0),
        )
        table.add_column("Connector", style="info")
        table.add_column("Status", justify="left")
        table.add_column("Operations", justify="right", style="step")
        for connector_id, info in registry.get_catalog().items():
            if registry.is_connector_enabled(connector_id):
                ops = [
                    op for op in info["operations"] if registry.is_operation_enabled(op)
                ]
                table.add_row(
                    f"{info['icon']} {info['name']}",
                    f"[ok]{self.glyphs.enabled} enabled[/ok]",
                    str(len(ops)),
                )
            else:
                table.add_row(
                    f"{info['icon']} {info['name']}",
                    f"[muted]{self.glyphs.disabled} disabled[/muted]",
                    self.glyphs.dash,
                )
        self.console.print(table)
        self.console.print("[muted]Use /setup to reconfigure connectors[/muted]\n")

    def config_table(self, settings) -> None:
        table = Table(
            title="[accent]Configuration[/accent]",
            show_header=False,
            border_style="border",
            box=None,
            padding=(0, 2, 0, 0),
        )
        table.add_column("Setting", style="muted")
        table.add_column("Value", style="info")
        table.add_row("LLM provider", settings.llm.provider)
        table.add_row("LLM model", settings.llm.model)
        table.add_row("Theme", self.theme.name)
        table.add_row("Memory window", str(settings.agent.memory_window))
        table.add_row("Max iterations", str(settings.agent.max_iterations))
        self.console.print(table)
        self.console.print()

    def doctor_panel(self, diag) -> None:
        """`/doctor` / `dacli doctor`: resolved paths + provider/governance/
        sandbox/terminal/connector posture. Stacked key/value rows so it stays
        legible down to narrow widths."""
        ok, bad = self.glyphs.ok, self.glyphs.err
        d = diag

        def mark(flag: bool) -> str:
            return f"[ok]{ok}[/ok]" if flag else f"[bad]{bad}[/bad]"

        table = Table(show_header=False, box=None, padding=(0, 2, 0, 0))
        table.add_column("Field", style="muted", no_wrap=True)
        table.add_column("Value", style="info", overflow="fold")

        cfg = d.config
        table.add_row(
            "config",
            f"{cfg['path']}" if cfg["found"]
            else "[warning]not found → using defaults[/warning]",
        )
        table.add_row("state dir", f"{d.state_dir['path']}  [muted]({d.state_dir['kind']})[/muted]")
        table.add_row("log", d.log["path"])

        llm = d.llm
        src = f" [muted](source: {llm['source']})[/muted]" if llm["source"] else ""
        ping = {"ok": f"[ok]{ok}[/ok]", "failed": f"[bad]{bad}[/bad]"}.get(
            llm["ping"], "[muted]skipped[/muted]"
        )
        model = llm["model"] or "[warning](unset)[/warning]"
        table.add_row(
            "llm",
            f"{llm['provider']}/{model}   key: {mark(llm['key'])}{src}   ping: {ping}",
        )

        gov = d.governance
        policy = "built-in defaults" if gov["policy_default"] else gov["policy"]
        table.add_row(
            "governance",
            f"{'enabled' if gov['enabled'] else 'disabled'}  [muted]·[/muted]  "
            f"policy: {policy}",
        )

        sb = d.sandbox
        if not sb["enabled"]:
            sandbox_val = "[muted]disabled[/muted]"
        elif sb["runtime"] == "docker":
            img = "image present" if sb["image_present"] else "[warning]image absent[/warning]"
            sandbox_val = f"docker  [muted]({img})[/muted]"
        else:
            note = "docker unavailable → fallback" if sb["fallback"] else "no docker"
            sandbox_val = f"subprocess  [muted]({note})[/muted]"
        table.add_row("sandbox", sandbox_val)

        term = d.terminal
        term_val = f"{term['shell']}  [muted]· scope {term['scope']}[/muted]"
        if not term["enabled"]:
            term_val = f"[muted]disabled[/muted]  ({term_val})"
        table.add_row("terminal", term_val)

        conn = d.connectors
        table.add_row(
            "connectors",
            f"{conn['enabled']} enabled  [muted]·[/muted]  {conn['skipped']} skipped",
        )
        for cid, reason in conn["skipped_detail"].items():
            table.add_row("", f"[warning]{cid}[/warning]: {reason}")

        cost = getattr(d, "cost", None) or {}
        gate = cost.get("confirm_usd")
        gate_txt = f"confirm above ${gate:g}" if gate is not None else "off"
        advisors = ", ".join(cost.get("advisors") or []) or "none enabled"
        table.add_row("cost", f"gate: {gate_txt}  [muted]·[/muted]  advisor: {advisors}")

        border = "border" if d.ok else "warning"
        self.console.print(
            Panel(
                table,
                title="[accent]doctor[/accent]",
                title_align="left",
                box=self.glyphs.box,
                border_style=border,
                padding=SPACING["panel_pad"],
            )
        )
        if not d.ok:
            for p in d.problems:
                self.notice(p, style="warning")
        self.console.print()

    def sessions_table(self, sessions: list[dict[str, Any]], limit: int = 10) -> None:
        if not sessions:
            self.console.print(
                "[muted]No sessions yet — one is created the first time you "
                "chat. `dacli chat --session <id>` resumes a saved one.[/muted]\n"
            )
            return
        table = Table(
            title="[accent]Sessions[/accent]",
            show_header=True,
            header_style="muted",
            border_style="border",
            box=None,
            padding=(0, 2, 0, 0),
        )
        table.add_column("Session", style="accent", no_wrap=True)
        table.add_column("Updated", style="step")
        table.add_column("Active task", style="info")
        table.add_column("Errors", justify="right", style="step")
        for s in sessions[:limit]:
            updated = (s.get("updated_at") or s.get("created_at") or "")[:19]
            table.add_row(
                str(s.get("session_id", "?")),
                updated,
                str(s.get("active_task") or self.glyphs.dash),
                str(s.get("errors_count", 0)),
            )
        self.console.print(table)
        self.console.print()

    def catalog_table(self, entries: list[Any]) -> None:
        """Known objects from the catalog cache (F-6: `dacli catalog`)."""
        if not entries:
            self.console.print(
                "[muted]Catalog cache is empty — objects appear here once the "
                "agent introspects or creates them.[/muted]\n"
            )
            return
        cols = [
            Col("Connector", style="info", drop_rank=1),
            Col("Type", style="step", drop_rank=3),
            Col("Object", style="accent", ratio=1, primary=True),
            Col("~Rows", style="step", justify="right", drop_rank=2),
            Col("Verified", style="muted", drop_rank=4),
            Col("", style="warning", drop_rank=5),
        ]
        rows: list[list[Any]] = []
        for entry in entries:
            scope = getattr(entry, "scope", {}) or {}
            name = ".".join(
                str(scope[k]) for k in ("database", "schema", "object") if scope.get(k)
            ) or "(unscoped)"
            rce = getattr(entry, "row_count_estimate", None)
            verified = getattr(entry, "last_verified", None)
            stale = entry.is_stale() if hasattr(entry, "is_stale") else False
            rows.append([
                getattr(entry, "connector", "?"),
                getattr(entry, "object_type", "?"),
                name,
                str(rce) if rce is not None else self.glyphs.dash,
                verified.isoformat(timespec="seconds")
                if hasattr(verified, "isoformat") else self.glyphs.dash,
                "stale" if stale else "",
            ])
        self.console.print(Text("Catalog", style="accent"))
        self.console.print(responsive_table(self.console, cols, rows))
        self.console.print(
            "[muted]Stale entries are hints — the agent re-verifies them before "
            "acting. /schema <object> shows columns.[/muted]\n"
        )

    def schema_panel(self, entry: Any) -> None:
        """Columns/types/row-count/last-verified for one object (F-6)."""
        scope = getattr(entry, "scope", {}) or {}
        name = ".".join(
            str(scope[k]) for k in ("database", "schema", "object") if scope.get(k)
        ) or "(unscoped)"
        header = Text()
        header.append(f"{name}\n", style="accent")
        header.append("Connector   ", style="muted")
        header.append(f"{getattr(entry, 'connector', '?')}\n", style="info")
        header.append("Type        ", style="muted")
        header.append(f"{getattr(entry, 'object_type', '?')}\n", style="info")
        rce = getattr(entry, "row_count_estimate", None)
        header.append("~Rows       ", style="muted")
        header.append(f"{rce if rce is not None else self.glyphs.dash}\n", style="info")
        verified = getattr(entry, "last_verified", None)
        header.append("Verified    ", style="muted")
        header.append(
            verified.isoformat(timespec="seconds")
            if hasattr(verified, "isoformat") else self.glyphs.dash,
            style="info",
        )
        if hasattr(entry, "is_stale") and entry.is_stale():
            header.append("  (stale — re-verify before relying on it)", style="warning")
        self.console.print(
            Panel(header, title="[accent]Schema[/accent]", box=self.glyphs.box,
                  border_style="border", padding=SPACING["panel_pad"])
        )

        columns = getattr(entry, "columns", None) or []
        if not columns:
            self.console.print(
                "[muted]No cached columns for this object — ask the agent to "
                "introspect it to fill them in.[/muted]\n"
            )
            return
        cols = [
            Col("Column", style="info", primary=True),
            Col("Type", style="step", drop_rank=2),
            Col("Description", style="muted", ratio=1, drop_rank=1),
        ]
        rows = [
            [
                str(col.get("name", "?")),
                str(col.get("type") or col.get("data_type") or ""),
                str(col.get("description", "")),
            ]
            for col in columns
        ]
        self.console.print(responsive_table(self.console, cols, rows))
        self.console.print()

    def lineage_panel(self, obj: str, upstream: list[Any], downstream: list[Any]) -> None:
        """Upstream producers + downstream consumers for one object (P12)."""
        header = Text()
        header.append(f"{obj}\n", style="accent")
        header.append("Downstream  ", style="muted")
        header.append(f"{len(downstream)} consumer(s)", style="warning" if downstream else "muted")
        header.append("   Upstream  ", style="muted")
        header.append(f"{len(upstream)} source(s)", style="info")
        self.console.print(
            Panel(header, title="[accent]Lineage[/accent]", box=self.glyphs.box,
                  border_style="border", padding=SPACING["panel_pad"])
        )
        if not downstream and not upstream:
            self.console.print(
                "[muted]No lineage known for this object — lineage is best-effort "
                "(dbt manifest, view deps, orchestrator DAGs) and fills in as the "
                "agent works. No lineage does not mean no consumers.[/muted]\n"
            )
            return

        cols = [
            Col("Kind", style="step", drop_rank=1),
            Col("Object", style="accent", ratio=1, primary=True),
            Col("Via", style="muted", drop_rank=2),
        ]

        def _rows(nodes):
            return [[n.kind, n.label or n.name, n.name] for n in nodes]

        if downstream:
            self.console.print(Text("Downstream (blast radius)", style="warning"))
            self.console.print(responsive_table(self.console, cols, _rows(downstream)))
            self.console.print()
        if upstream:
            self.console.print(Text("Upstream (reads from)", style="info"))
            self.console.print(responsive_table(self.console, cols, _rows(upstream)))
            self.console.print()

    def why_failed_panel(self, explanation: Any) -> None:
        """Root cause + log excerpt + the proposed (governed, unapplied) fix."""
        finding = explanation.finding
        if finding is None:
            self.console.print(
                f"[muted]{explanation.error or 'no failure located'}[/muted]")
            return

        body = Text()
        body.append("Source      ", style="muted")
        body.append(f"{finding.source}\n", style="accent")
        body.append("Failing     ", style="muted")
        body.append(f"{finding.failing_node}  ({finding.status})\n", style="warning")
        if finding.dag_id:
            body.append("DAG / run   ", style="muted")
            body.append(f"{finding.dag_id} / {finding.run_id}\n", style="step")
        body.append("Root cause  ", style="muted")
        body.append(f"{explanation.root_cause}", style="phase")
        self.console.print(
            Panel(body, title="[error]Why failed[/error]", box=self.glyphs.box,
                  border_style="border", padding=SPACING["panel_pad"])
        )

        if finding.log_excerpt:
            self.console.print(Text("Log excerpt", style="muted"))
            self.console.print(Panel(finding.log_excerpt.strip()[:1500],
                                     box=self.glyphs.box, border_style="muted"))

        if explanation.downstream:
            names = ", ".join(n.get("label") or n.get("name")
                              for n in explanation.downstream[:8])
            self.console.print(
                f"[warning]Blast radius[/warning]  {len(explanation.downstream)} "
                f"downstream consumer(s): {names}")

        fix = explanation.proposed_fix
        if fix is not None:
            verb = "applied" if fix.applied else "proposed (not applied)"
            self.console.print(
                f"\n[accent]Proposed fix[/accent] [{verb}]  "
                f"{fix.tool_name} {fix.args}\n[muted]{fix.rationale}[/muted]")
            if not fix.applied:
                self.console.print(
                    "[muted]Re-run with --apply to route it through the governance "
                    "gate (classify → approve → verify → rollback).[/muted]")

    def assertion_panel(self, outcomes: list[Any]) -> None:
        """Render data-quality assertion outcomes (`dacli assert run`)."""
        for outcome in outcomes:
            if outcome.error:
                self.console.print(
                    f"[error]{outcome.name}[/error]  {outcome.predicate}\n"
                    f"[muted]{outcome.error}[/muted]")
                continue
            verdict = ("[error]BREACH[/error]" if outcome.breached
                       else "[success]ok[/success]")
            value = f"{outcome.value:.4g}" if outcome.value is not None else "?"
            self.console.print(
                f"{verdict}  [accent]{outcome.name}[/accent]  "
                f"{outcome.predicate}  [muted](measured {value})[/muted]")
            fix = outcome.proposed_fix
            if fix is not None:
                verb = "applied" if fix.applied else "proposed (not applied)"
                self.console.print(
                    f"  [accent]fix[/accent] [{verb}]  {fix.tool_name} {fix.args}\n"
                    f"  [muted]{fix.rationale}[/muted]")
                if not fix.applied:
                    self.console.print(
                        "  [muted]Re-run with --apply to route it through the "
                        "governance gate.[/muted]")

    def cost_panel(self, connector: str, estimate: Any, session: Any) -> None:
        """Render a warehouse cost estimate and/or session spend (`dacli cost`)."""
        body = Text()
        body.append("Connector   ", style="muted")
        body.append(f"{connector}\n", style="accent")
        if estimate is not None:
            body.append("Estimate    ", style="muted")
            body.append(f"{estimate.detail}\n", style="warning")
        if session is not None:
            body.append("Session     ", style="muted")
            if session.error:
                body.append(f"{session.error}\n", style="muted")
            else:
                bits = [f"{session.queries} query(ies)"]
                if session.credits is not None:
                    bits.append(f"{session.credits:g} credits")
                if session.usd is not None:
                    bits.append(f"≈ ${session.usd:,.2f}")
                body.append("  ·  ".join(bits) + "\n", style="info")
        self.console.print(
            Panel(body, title="Warehouse cost", box=self.glyphs.box,
                  border_style="border", padding=SPACING["panel_pad"]))

    def status_panel(self, memory) -> None:
        # Render the current agent status: session panel, plan and statistics.
        summary = memory.get_progress_summary()

        # Main status panel
        status_text = Text()
        status_text.append("Session     ", style="muted")
        status_text.append(f"{summary['session_id']}\n", style="accent")
        status_text.append("Active task ", style="muted")
        status_text.append(
            f"{summary.get('active_task') or self.glyphs.dash}", style="phase"
        )
        self.console.print(
            Panel(
                status_text,
                title="[accent]Status[/accent]",
                box=self.glyphs.box,
                border_style="border",
                padding=SPACING["panel_pad"],
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
                    "pending": self.glyphs.pending,
                    "in_progress": self.glyphs.running,
                    "completed": self.glyphs.ok,
                }.get(status, self.glyphs.pending)
                table.add_row(str(i), f"{status_icon} {status}", todo.get("content", ""))
            self.console.print(table)

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
        self.console.print(
            Panel(
                stats_table,
                title="[accent]Statistics[/accent]",
                box=self.glyphs.box,
                border_style="border",
                padding=SPACING["panel_pad"],
            )
        )

        if summary.get("last_error"):
            self.console.print(f"[error]Last error:[/error] {summary['last_error']}")

    def history(self, messages: list[Any], limit: int = 20) -> None:
        for msg in messages[-limit:]:
            role = getattr(msg, "role", "?")
            content = getattr(msg, "content", "")
            is_user = role == "user"
            marker = self.glyphs.caret if is_user else self.glyphs.agent
            marker_style = "accent" if is_user else "gutter"
            text_style = "user" if is_user else "step"
            preview = (
                content
                if len(content) <= 200
                else content[:200] + self.glyphs.ellipsis
            )
            self.console.print(
                self._guttered(marker, marker_style, Text(preview, style=text_style))
            )
        self.console.print()

