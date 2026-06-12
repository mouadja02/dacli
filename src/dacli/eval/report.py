"""Exportable reliability report (F-3) — `dacli eval --report <path>`.

Renders the same numbers the text dashboard prints — per-connector
pass@1/pass^k, the destructive-action gate record, tokens/latency, and the
optional regression diff — as a shareable artifact: Markdown (easy to paste in
a PR) or a self-contained HTML page (``string.Template``, no dependencies, no
external assets). The format is inferred from the file extension.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from string import Template

from dacli.eval.dashboard import ConnectorRow, Dashboard
from dacli.eval.regression import RegressionReport

_COLUMNS = (
    ("connector", "connector"), ("tasks", "tasks"), ("pass@1", "pass_at_1"),
    ("pass^k", "pass_k"), ("success", "success_rate"),
    ("escalation", "escalation_rate"), ("correction", "correction_rate"),
    ("gov-interrupt", "governance_interrupt_rate"), ("unguarded", "unguarded"),
    ("avg tokens", "avg_tokens"), ("avg ms", "avg_latency_ms"),
)


def _cell(row: ConnectorRow, attr: str) -> str:
    value = getattr(row, attr)
    if attr == "connector":
        return str(value)
    if attr in ("tasks", "unguarded"):
        return str(value)
    if attr == "avg_tokens":
        return f"{value:.0f}"
    if attr == "avg_latency_ms":
        return f"{value:.1f}"
    return f"{value:.2f}"


def _gate_line(dashboard: Dashboard) -> str:
    n = dashboard.overall.unguarded
    if n:
        return f"⚠ {n} UNGUARDED destructive execution(s) — this must be zero."
    return "✓ zero unguarded destructive executions."


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


# ---------------------------------------------------------------------------
# Markdown
# ---------------------------------------------------------------------------
def render_markdown(
    dashboard: Dashboard,
    regression: RegressionReport | None = None,
) -> str:
    lines = [
        f"# dacli reliability report — suite `{dashboard.suite}`",
        "",
        f"Generated {_timestamp()} by `dacli eval` (offline, deterministic "
        "simulated platforms — no credentials, no network).",
        "",
        "## Per-connector scorecard",
        "",
        "| " + " | ".join(h for h, _ in _COLUMNS) + " |",
        "|" + "---|" * len(_COLUMNS),
    ]
    lines.extend(
        "| " + " | ".join(_cell(row, a) for _, a in _COLUMNS) + " |"
        for row in dashboard.rows
    )
    overall = [_cell(dashboard.overall, a) for _, a in _COLUMNS]
    overall[0] = f"**{overall[0]}**"
    lines.append("| " + " | ".join(overall) + " |")
    lines += ["", "## Destructive-action gate", "", _gate_line(dashboard)]

    if regression is not None:
        lines += [
            "",
            "## Regression vs. previous run",
            "",
            regression.summary(),
        ]
        for kind, items in (
            ("New failures", regression.new_failures),
            ("Earlier-failure recurrences", regression.earlier_failures),
            ("Unguarded executions", regression.unguarded),
        ):
            if items:
                lines.append("")
                lines.append(f"### {kind}")
                lines.extend(f"- `{r.task_id}`: {r.detail}" for r in items)
        if regression.fixed:
            lines.append("")
            lines.append("### Fixed")
            lines.extend(f"- `{t}`" for t in regression.fixed)

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Self-contained HTML (stdlib string.Template — no new deps, no external assets)
# ---------------------------------------------------------------------------
_HTML_PAGE = Template("""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>dacli reliability report — $suite</title>
<style>
  body { font: 14px/1.5 system-ui, sans-serif; margin: 2rem auto; max-width: 64rem; color: #1c2330; }
  h1 { font-size: 1.4rem; } h2 { font-size: 1.1rem; margin-top: 2rem; }
  table { border-collapse: collapse; width: 100%; margin-top: .75rem; }
  th, td { padding: .35rem .6rem; text-align: right; border-bottom: 1px solid #e2e6ee; }
  th:first-child, td:first-child { text-align: left; }
  th { color: #5a6478; font-weight: 600; }
  tr.overall td { font-weight: 700; border-top: 2px solid #1c2330; }
  .gate-ok { color: #176e3b; font-weight: 600; }
  .gate-bad { color: #a01818; font-weight: 700; }
  .muted { color: #5a6478; }
</style>
</head>
<body>
<h1>dacli reliability report — suite <code>$suite</code></h1>
<p class="muted">Generated $timestamp by <code>dacli eval</code> (offline, deterministic simulated platforms).</p>
<h2>Per-connector scorecard</h2>
<table>
<thead><tr>$header</tr></thead>
<tbody>
$rows
</tbody>
</table>
<h2>Destructive-action gate</h2>
<p class="$gate_class">$gate_line</p>
$regression
</body>
</html>
""")


def _escape(text: str) -> str:
    return (str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def render_html(
    dashboard: Dashboard,
    regression: RegressionReport | None = None,
) -> str:
    header = "".join(f"<th>{_escape(h)}</th>" for h, _ in _COLUMNS)
    body_rows = [
        "<tr>" + "".join(f"<td>{_escape(_cell(row, a))}</td>" for _, a in _COLUMNS) + "</tr>"
        for row in dashboard.rows
    ]
    body_rows.append(
        '<tr class="overall">'
        + "".join(f"<td>{_escape(_cell(dashboard.overall, a))}</td>" for _, a in _COLUMNS)
        + "</tr>"
    )

    regression_html = ""
    if regression is not None:
        regression_html = (
            "<h2>Regression vs. previous run</h2>"
            f"<p>{_escape(regression.summary())}</p>"
        )

    return _HTML_PAGE.substitute(
        suite=_escape(dashboard.suite),
        timestamp=_escape(_timestamp()),
        header=header,
        rows="\n".join(body_rows),
        gate_class="gate-bad" if dashboard.overall.unguarded else "gate-ok",
        gate_line=_escape(_gate_line(dashboard)),
        regression=regression_html,
    )


# ---------------------------------------------------------------------------
def write_report(
    path: str,
    dashboard: Dashboard,
    regression: RegressionReport | None = None,
) -> Path:
    """Write the report to ``path``; the extension picks the format (md default)."""
    target = Path(path)
    if target.suffix.lower() in (".html", ".htm"):
        content = render_html(dashboard, regression)
    else:
        content = render_markdown(dashboard, regression)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return target
