"""F-3: exportable reliability report — `dacli eval --report <path>`.

A shareable scorecard (Markdown or self-contained HTML) rendered from the same
numbers the text dashboard prints: per-connector pass@1/pass^k, the
destructive-action gate record, tokens/latency, and (when available) the
regression diff. Offline and deterministic — driven by the sim suite.
"""

import tempfile
import unittest
from pathlib import Path

from dacli.eval.dashboard import ConnectorRow, Dashboard
from dacli.eval.report import render_html, render_markdown, write_report


def _row(connector, *, unguarded=0):
    return ConnectorRow(
        connector=connector, tasks=3, pass_at_1=1.0, pass_k=0.97,
        success_rate=0.98, escalation_rate=0.0, correction_rate=0.1,
        governance_interrupt_rate=0.33, unguarded=unguarded,
        avg_tokens=120.0, avg_latency_ms=8.5,
    )


def _dashboard(*, unguarded=0):
    rows = [_row("bigquery"), _row("s3")]
    return Dashboard(suite="sim", rows=rows, overall=_row("OVERALL", unguarded=unguarded))


class MarkdownReportTest(unittest.TestCase):
    def test_markdown_contains_connector_table_and_overall_row(self):
        md = render_markdown(_dashboard())
        self.assertIn("| bigquery |", md)
        self.assertIn("| s3 |", md)
        self.assertIn("| **OVERALL** |", md)
        self.assertIn("pass^k", md)

    def test_markdown_reports_the_destructive_gate(self):
        clean = render_markdown(_dashboard(unguarded=0))
        self.assertIn("zero unguarded destructive executions", clean)
        dirty = render_markdown(_dashboard(unguarded=2))
        self.assertIn("2 UNGUARDED", dirty)


class HtmlReportTest(unittest.TestCase):
    def test_html_is_self_contained_and_carries_the_rows(self):
        html = render_html(_dashboard())
        self.assertTrue(html.lstrip().lower().startswith("<!doctype html"))
        self.assertIn("bigquery", html)
        self.assertIn("OVERALL", html)
        # Self-contained: no external scripts or stylesheets.
        self.assertNotIn("<script src", html)
        self.assertNotIn("<link", html)


class WriteReportTest(unittest.TestCase):
    def test_format_is_inferred_from_the_extension(self):
        tmp = Path(tempfile.mkdtemp())
        md_path, html_path = tmp / "r.md", tmp / "r.html"
        write_report(str(md_path), _dashboard())
        write_report(str(html_path), _dashboard())
        self.assertIn("| **OVERALL** |", md_path.read_text(encoding="utf-8"))
        self.assertIn("<!doctype html", html_path.read_text(encoding="utf-8").lower())


class EvalReportEndToEndTest(unittest.TestCase):
    def test_eval_quick_writes_a_markdown_report(self):
        from dacli.eval.__main__ import main as eval_main

        tmp = Path(tempfile.mkdtemp())
        out = tmp / "report.md"
        rc = eval_main([
            "--quick", "--no-persist",
            "--history", str(tmp / "history.jsonl"),
            "--report", str(out),
        ])
        self.assertEqual(rc, 0)
        self.assertTrue(out.exists())
        content = out.read_text(encoding="utf-8")
        self.assertIn("| **OVERALL** |", content)
        self.assertIn("zero unguarded destructive executions", content)
        self.assertIn("| spine |", content)


if __name__ == "__main__":
    unittest.main()
