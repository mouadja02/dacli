"""F-6 (P11) — `dacli catalog` / `dacli schema` and the /catalog /schema views.

Seeds a CatalogCache with known objects and asserts the commands render them.
No network, no agent construction.
"""

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from click.testing import CliRunner

from dacli.memory.catalog import CatalogCache


def _seeded_cache(tmpdir: str) -> CatalogCache:
    cache = CatalogCache(path=str(Path(tmpdir) / "catalog.json"))
    cache.record_object(
        "snowflake",
        "table",
        {"database": "ANALYTICS", "schema": "MARTS", "object": "ORDERS"},
        row_count_estimate=1234,
        columns=[
            {"name": "ORDER_ID", "type": "NUMBER", "description": "pk"},
            {"name": "AMOUNT", "type": "NUMBER", "description": "USD total"},
        ],
    )
    cache.record_object(
        "postgres",
        "table",
        {"database": "app", "schema": "public", "object": "users"},
        row_count_estimate=42,
    )
    cache.record_object("snowflake", "schema", {"database": "ANALYTICS", "schema": "MARTS"})
    return cache


class CatalogFindTest(unittest.TestCase):
    def setUp(self):
        self.cache = _seeded_cache(tempfile.mkdtemp(prefix="dacli_p11_cat_"))

    def test_find_bare_name_case_insensitive(self):
        matches = self.cache.find("orders")
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].scope["object"], "ORDERS")

    def test_find_qualified_name(self):
        self.assertEqual(len(self.cache.find("analytics.marts.orders")), 1)
        self.assertEqual(self.cache.find("other.marts.orders"), [])

    def test_find_with_connector_filter(self):
        self.assertEqual(self.cache.find("users", connector="postgres")[0].connector, "postgres")
        self.assertEqual(self.cache.find("users", connector="snowflake"), [])


class CatalogCommandTest(unittest.TestCase):
    def _invoke(self, args, cache):
        from dacli.scripts.cli import cli

        with mock.patch("dacli.memory.catalog.CatalogCache", return_value=cache):
            return CliRunner().invoke(cli, args)

    def test_catalog_lists_known_objects(self):
        cache = _seeded_cache(tempfile.mkdtemp(prefix="dacli_p11_cmd_"))
        res = self._invoke(["catalog"], cache)
        self.assertEqual(res.exit_code, 0, msg=res.output)
        self.assertIn("ANALYTICS.MARTS.ORDERS", res.output)
        self.assertIn("app.public.users", res.output)
        self.assertIn("1234", res.output)

    def test_catalog_connector_filter(self):
        cache = _seeded_cache(tempfile.mkdtemp(prefix="dacli_p11_cmd2_"))
        res = self._invoke(["catalog", "--connector", "postgres"], cache)
        self.assertEqual(res.exit_code, 0, msg=res.output)
        self.assertIn("users", res.output)
        self.assertNotIn("ORDERS", res.output)

    def test_schema_shows_columns(self):
        cache = _seeded_cache(tempfile.mkdtemp(prefix="dacli_p11_cmd3_"))
        res = self._invoke(["schema", "orders"], cache)
        self.assertEqual(res.exit_code, 0, msg=res.output)
        self.assertIn("ORDER_ID", res.output)
        self.assertIn("AMOUNT", res.output)
        self.assertIn("USD total", res.output)
        self.assertIn("1234", res.output)

    def test_schema_unknown_object_notices(self):
        cache = _seeded_cache(tempfile.mkdtemp(prefix="dacli_p11_cmd4_"))
        res = self._invoke(["schema", "nope"], cache)
        self.assertEqual(res.exit_code, 0, msg=res.output)
        self.assertIn("No catalog entry", res.output)


class CatalogUiRenderTest(unittest.TestCase):
    """The DacliUI renderers used by the in-chat /catalog and /schema."""

    def _ui(self):
        from rich.console import Console
        from dacli.tui import DacliUI

        console = Console(record=True, width=120, force_terminal=False)
        return DacliUI(version="9.9.9", author="tester", console=console)

    def test_catalog_table_renders_entries_and_stale_marker(self):
        cache = _seeded_cache(tempfile.mkdtemp(prefix="dacli_p11_ui_"))
        cache.invalidate_scope(
            "postgres", {"database": "app", "schema": "public", "object": "users"}
        )
        ui = self._ui()
        ui.catalog_table(cache.list_objects())
        out = ui.console.export_text()
        self.assertIn("ANALYTICS.MARTS.ORDERS", out)
        self.assertIn("stale", out)

    def test_catalog_table_empty_message(self):
        ui = self._ui()
        ui.catalog_table([])
        self.assertIn("Catalog cache is empty", ui.console.export_text())

    def test_schema_panel_renders_columns(self):
        cache = _seeded_cache(tempfile.mkdtemp(prefix="dacli_p11_ui2_"))
        ui = self._ui()
        ui.schema_panel(cache.find("orders")[0])
        out = ui.console.export_text()
        self.assertIn("ANALYTICS.MARTS.ORDERS", out)
        self.assertIn("ORDER_ID", out)
        self.assertIn("snowflake", out)


if __name__ == "__main__":
    unittest.main()
