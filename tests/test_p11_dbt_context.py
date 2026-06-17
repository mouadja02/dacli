"""F-5 (P11) — dbt manifest ingestion into the live-env context layer.

Parses a fixture ``manifest.json`` and asserts models + lineage + column docs
are extracted, and that a task mentioning a model surfaces it in the assembled
context (the same ``build`` hook ``dacli context --explain`` uses).
"""

import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from dacli.config.settings import Settings
from dacli.connectors.registry import ConnectorRegistry
from dacli.connectors.system.connector import SystemConnector
from dacli.context.assembler import build_context
from dacli.context.budget import Budget, LIVE
from dacli.context.pipeline import build_context_pipeline
from dacli.context.sources.dbt_manifest import (
    DbtManifestSource,
    parse_manifest,
)
from dacli.context.tokenizer import make_counter

FIXTURE = Path(__file__).parent / "fixtures" / "dbt_manifest.json"


def _project_with_manifest(tmpdir: str) -> str:
    """Lay out ``<project>/target/manifest.json`` from the fixture."""
    target = Path(tmpdir) / "target"
    target.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(FIXTURE, target / "manifest.json")
    return tmpdir


class _FakeMemory:
    catalog = None
    session_id = "p11_dbt"

    def retrieve(self, query, top_k=5):
        return []


def _empty_registry() -> ConnectorRegistry:
    empty = tempfile.mkdtemp(prefix="dacli_p11_")
    system = SystemConnector(settings=None, memory=None)
    reg = ConnectorRegistry(None, connectors_dir=empty, config_path="__nope__.yaml",
                            extra_connectors=[system])
    os.rmdir(empty)
    system.bind_registry(reg)
    return reg


class ParseManifestTest(unittest.TestCase):
    def setUp(self):
        self.entries = parse_manifest(json.loads(FIXTURE.read_text(encoding="utf-8")))
        self.by_name = {e.name: e for e in self.entries}

    def test_models_extracted_not_tests_or_sources(self):
        self.assertEqual(
            sorted(self.by_name), ["customers", "orders", "stg_orders"]
        )

    def test_model_docs_and_column_docs(self):
        orders = self.by_name["orders"]
        self.assertIn("Orders fact table", orders.description)
        cols = {c["name"]: c for c in orders.columns}
        self.assertEqual(cols["amount"]["description"], "Order total in USD.")
        self.assertEqual(cols["amount"]["type"], "numeric")

    def test_lineage_maps_to_model_names_only(self):
        # orders depends on stg_orders; stg_orders' source.* dep is dropped.
        self.assertEqual(self.by_name["orders"].depends_on, ["stg_orders"])
        self.assertEqual(self.by_name["stg_orders"].depends_on, [])

    def test_tests_attached_to_their_model(self):
        self.assertEqual(
            self.by_name["orders"].tests,
            ["not_null_orders_order_id", "unique_orders_order_id"],
        )
        self.assertEqual(self.by_name["customers"].tests, [])

    def test_context_line_carries_docs_lineage_and_tests(self):
        line = self.by_name["orders"].context_line()
        self.assertIn("dbt model orders", line)
        self.assertIn("Orders fact table", line)
        self.assertIn("amount (Order total in USD.)", line)
        self.assertIn("depends_on: stg_orders", line)
        self.assertIn("unique_orders_order_id", line)


class DbtManifestSourceTest(unittest.TestCase):
    def test_missing_manifest_is_a_noop(self):
        source = DbtManifestSource(tempfile.mkdtemp(prefix="dacli_p11_empty_"))
        self.assertEqual(source.entries(), [])

    def test_parse_is_cached_and_refreshes_on_mtime_change(self):
        project = _project_with_manifest(tempfile.mkdtemp(prefix="dacli_p11_proj_"))
        source = DbtManifestSource(project)
        first = source.entries()
        self.assertEqual(len(first), 3)
        # Same mtime -> the exact cached list (no re-parse).
        self.assertIs(source.entries(), first)

        # Rewrite the manifest with one model and bump the mtime -> refresh.
        manifest_path = Path(project) / "target" / "manifest.json"
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        data["nodes"] = {
            "model.jaffle.orders": data["nodes"]["model.jaffle.orders"]
        }
        manifest_path.write_text(json.dumps(data), encoding="utf-8")
        st = manifest_path.stat()
        os.utime(manifest_path, (st.st_atime, st.st_mtime + 5))
        refreshed = source.entries()
        self.assertEqual([e.name for e in refreshed], ["orders"])

    def test_corrupt_manifest_keeps_last_good_parse(self):
        project = _project_with_manifest(tempfile.mkdtemp(prefix="dacli_p11_bad_"))
        source = DbtManifestSource(project)
        good = source.entries()
        manifest_path = Path(project) / "target" / "manifest.json"
        manifest_path.write_text("{not json", encoding="utf-8")
        st = manifest_path.stat()
        os.utime(manifest_path, (st.st_atime, st.st_mtime + 5))
        self.assertEqual(source.entries(), good)


class AssembledContextTest(unittest.TestCase):
    """A task mentioning a model surfaces its docs in the system prompt."""

    def _build(self, task: str):
        project = _project_with_manifest(tempfile.mkdtemp(prefix="dacli_p11_ctx_"))
        source = DbtManifestSource(project)
        return build_context(
            task,
            memory=_FakeMemory(),
            registry=_empty_registry(),
            recent_messages=[{"role": "user", "content": task}],
            counter=make_counter(None),
            budget=Budget(total=12000),
            priors_text="",
            live_provider=lambda _t: source.entries(),
        )

    def test_mentioned_model_surfaces_in_live_section(self):
        ctx = self._build("add a margin column to the orders model")
        self.assertIn("dbt model orders", ctx.system_prompt)
        self.assertIn("Orders fact table", ctx.system_prompt)
        live_chunks = [c for c in ctx.chunks if c.source == LIVE]
        self.assertTrue(any("orders" in c.text for c in live_chunks))

    def test_mentioned_model_outranks_unmentioned(self):
        ctx = self._build("compute customer lifetime value in customers")
        live = [c.text for c in ctx.chunks if c.source == LIVE]
        rank = {line.split(" · ")[0]: i for i, line in enumerate(live)}
        # The mentioned model ranks above a model the task never references.
        self.assertLess(
            rank["dbt model customers"], rank["dbt model stg_orders"]
        )


class PipelineWiringTest(unittest.TestCase):
    """The agent's context pipeline feeds dbt models through live_provider."""

    def _pipeline(self, project_dir: str):
        settings = Settings(connector_config={"dbt": {"project_dir": project_dir}})
        system = SystemConnector(settings=None, memory=None)
        registry = _empty_registry()
        return build_context_pipeline(
            settings, _FakeMemory(), registry, llm=None, system_connector=system
        )

    def test_configured_project_surfaces_models(self):
        project = _project_with_manifest(tempfile.mkdtemp(prefix="dacli_p11_pipe_"))
        pipeline = self._pipeline(project)
        ctx = pipeline["build"]("backfill the orders model", [], set())
        self.assertIn("dbt model orders", ctx.system_prompt)

    def test_no_project_configured_is_a_noop(self):
        pipeline = self._pipeline("")
        ctx = pipeline["build"]("backfill the orders model", [], set())
        self.assertNotIn("dbt model", ctx.system_prompt)


if __name__ == "__main__":
    unittest.main()
