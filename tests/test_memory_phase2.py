""" (ℳ Trustworthy Memory) test suite.

Each test maps to an exit criterion in the plan. Run with:
    python -m unittest tests.test_memory_phase2
"""

import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta

from dacli.memory.store import (
    MemoryStore, MemoryEntry, MemoryKind, VerificationStatus,
    confidence_for_source, MAX_CONFIDENCE,
)
from dacli.memory.catalog import CatalogCache
from dacli.memory.retrieval import rank, retrieve, staleness_penalty
from dacli.memory.verify import verify, build_catalog_verifier
from dacli.connectors.snowflake.connector import parse_catalog_effects
from dacli.connectors.base import ToolResult, ToolStatus, Risk, OperationSpec
from dacli.connectors.dispatcher import Dispatcher
from dacli.core.memory import AgentMemory, AgentState


def _tmp(name):
    return tempfile.mkdtemp(prefix="dacli_p2_") + "/" + name


# ---------------------------------------------------------------------------
# Exit criterion: no Snowflake/dbt-specific field names remain in memory module
# ---------------------------------------------------------------------------
class NoPipelineFieldsTest(unittest.TestCase):
    def test_agentstate_has_no_pipeline_fields(self):
        fields = set(AgentState.__dataclass_fields__.keys())
        forbidden = {
            "created_tables", "schemas_created", "file_formats_created",
            "inferred_schemas", "loaded_tables", "dbt_sources_registered",
            "dbt_models_created",
        }
        self.assertEqual(fields & forbidden, set(),
                         f"pipeline-specific fields still present: {fields & forbidden}")


# ---------------------------------------------------------------------------
# Exit criterion: typed store — confidence priors, append-only supersession
# ---------------------------------------------------------------------------
class StoreTest(unittest.TestCase):
    def test_confidence_priors(self):
        self.assertEqual(confidence_for_source("snowflake.information_schema"), 0.95)
        self.assertEqual(confidence_for_source("user"), 0.90)
        self.assertEqual(confidence_for_source("inference"), 0.50)

    def test_confidence_capped(self):
        store = MemoryStore(path=_tmp("store.jsonl"))
        e = store.remember("x", source="user", confidence=1.0)
        self.assertLessEqual(e.confidence, MAX_CONFIDENCE)

    def test_append_only_supersession_preserves_history(self):
        path = _tmp("store.jsonl")
        store = MemoryStore(path=path)
        old = store.remember("BRONZE.CRM has 3 columns", source="inference")
        new = MemoryEntry(content="BRONZE.CRM has 5 columns", source="snowflake.information_schema")
        store.supersede(old.id, new)

        # Old entry is preserved (audit trail) but no longer active.
        self.assertIsNotNone(store.get(old.id))
        self.assertEqual(store.get(old.id).superseded_by, new.id)
        self.assertNotIn(old.id, [e.id for e in store.active()])
        self.assertIn(new.id, [e.id for e in store.active()])

        # Reload from disk: supersession survives the round-trip.
        store2 = MemoryStore(path=path)
        self.assertEqual(store2.get(old.id).superseded_by, new.id)
        self.assertNotIn(old.id, [e.id for e in store2.active()])


# ---------------------------------------------------------------------------
# Exit criterion: every catalog entry exposes last_verified; a DROP/write op
# invalidates the matching scope.
# ---------------------------------------------------------------------------
class CatalogTest(unittest.TestCase):
    def test_entry_exposes_last_verified(self):
        cat = CatalogCache(path=_tmp("catalog.json"))
        e = cat.record_object("snowflake", "table", {"database": "DW", "schema": "BRONZE", "object": "CRM"})
        self.assertIsInstance(e.last_verified, datetime)

    def test_write_invalidates_matching_scope(self):
        cat = CatalogCache(path=_tmp("catalog.json"))
        cat.record_object("snowflake", "table", {"database": "DW", "schema": "BRONZE", "object": "CRM"})
        self.assertTrue(cat.is_known("snowflake", "table", {"database": "DW", "schema": "BRONZE", "object": "CRM"}))

        # A DROP/write touching the object invalidates it -> now a hint, not a fact.
        affected = cat.invalidate_scope("snowflake", {"database": "DW", "schema": "BRONZE", "object": "CRM"})
        self.assertEqual(len(affected), 1)
        self.assertFalse(cat.is_known("snowflake", "table", {"database": "DW", "schema": "BRONZE", "object": "CRM"}))
        self.assertTrue(cat.get("snowflake", "table", {"database": "DW", "schema": "BRONZE", "object": "CRM"}).is_stale())

    def test_schema_invalidation_cascades_to_children(self):
        cat = CatalogCache(path=_tmp("catalog.json"))
        cat.record_object("snowflake", "schema", {"database": "DW", "schema": "BRONZE"})
        cat.record_object("snowflake", "table", {"database": "DW", "schema": "BRONZE", "object": "CRM"})
        cat.record_object("snowflake", "table", {"database": "DW", "schema": "SILVER", "object": "DIM"})

        affected = cat.invalidate_scope("snowflake", {"database": "DW", "schema": "BRONZE"})
        # The BRONZE schema entry + the BRONZE.CRM table, but not SILVER.DIM.
        scopes = {(a.object_type, a.scope.get("object")) for a in affected}
        self.assertIn(("schema", None), scopes)
        self.assertIn(("table", "CRM"), scopes)
        self.assertTrue(cat.is_known("snowflake", "table", {"database": "DW", "schema": "SILVER", "object": "DIM"}))

    def test_ttl_staleness(self):
        cat = CatalogCache(path=_tmp("catalog.json"))
        e = cat.record_object("snowflake", "table", {"schema": "BRONZE", "object": "CRM"}, ttl_seconds=0)
        self.assertTrue(e.is_stale())  # TTL 0 -> immediately a hint


# ---------------------------------------------------------------------------
# Exit criterion: retrieval down-ranks an aged entry below a fresh, lower-
# relevance one (synthetic ages).
# ---------------------------------------------------------------------------
class RetrievalTest(unittest.TestCase):
    def test_aged_high_relevance_sinks_below_fresh_low_relevance(self):
        now = datetime.now()
        # Highly relevant to the query, but 60 days stale.
        aged = MemoryEntry(
            content="bronze crm table columns id name email",
            confidence=0.9, last_verified=now - timedelta(days=60),
        )
        # Less relevant (one shared token), but fresh.
        fresh = MemoryEntry(
            content="bronze schema overview",
            confidence=0.9, last_verified=now,
        )
        ranked = rank("bronze crm table columns", [aged, fresh], now=now)
        self.assertEqual(ranked[0].entry.id, fresh.id,
                         "stale-but-relevant entry must not outrank the fresh one")
        self.assertGreater(ranked[0].staleness_penalty, -1)  # sanity

    def test_staleness_penalty_monotonic(self):
        now = datetime.now()
        young = MemoryEntry(content="x", last_verified=now - timedelta(days=1))
        old = MemoryEntry(content="x", last_verified=now - timedelta(days=20))
        self.assertLess(staleness_penalty(young, now=now), staleness_penalty(old, now=now))

    def test_superseded_excluded(self):
        e = MemoryEntry(content="bronze crm", superseded_by="other")
        self.assertEqual(retrieve("bronze crm", [e]), [])


# ---------------------------------------------------------------------------
# Exit criterion: verify() re-checks against a (mock) live system and updates
# trust fields + supersession.
# ---------------------------------------------------------------------------
class VerifyTest(unittest.TestCase):
    def test_confirmed_refreshes_trust(self):
        store = MemoryStore(path=_tmp("store.jsonl"))
        old_time = datetime.now() - timedelta(days=10)
        e = store.add(MemoryEntry(content="BRONZE.CRM exists", confidence=0.7, last_verified=old_time))

        verifier = build_catalog_verifier(lambda entry: {"exists": True, "content": entry.content})
        result = verify(e, verifier, store=store)

        self.assertEqual(result.verification_status, VerificationStatus.VERIFIED.value)
        self.assertGreater(result.confidence, 0.7)          # bumped toward cap
        self.assertGreater(result.last_verified, old_time)  # recency refreshed

    def test_contradiction_supersedes(self):
        store = MemoryStore(path=_tmp("store.jsonl"))
        e = store.add(MemoryEntry(content="BRONZE.CRM has 3 columns", confidence=0.8))

        # Live system says the object is gone / different.
        verifier = build_catalog_verifier(lambda entry: {"exists": False})
        result = verify(e, verifier, store=store)

        self.assertNotEqual(result.id, e.id)                # a new, superseding fact
        self.assertEqual(result.supersedes, e.id)
        self.assertEqual(store.get(e.id).verification_status, VerificationStatus.CONTRADICTED.value)
        self.assertEqual(store.get(e.id).superseded_by, result.id)
        self.assertNotIn(e.id, [a.id for a in store.active()])


# ---------------------------------------------------------------------------
# Exit criterion: the regex side-effects are replaced by structured catalog
# updates (parse_catalog_effects), robust to non-standard SQL.
# ---------------------------------------------------------------------------
class CatalogEffectParseTest(unittest.TestCase):
    def test_create_or_replace_schema(self):
        effects = parse_catalog_effects("CREATE OR REPLACE SCHEMA DATA_WAREHOUSE.BRONZE;")
        self.assertEqual(effects, [{"action": "create", "object_type": "schema",
                                    "scope": {"database": "DATA_WAREHOUSE", "schema": "BRONZE"}}])

    def test_create_table_if_not_exists_multiline_qualified(self):
        sql = "CREATE TABLE IF NOT EXISTS\n  DATA_WAREHOUSE.BRONZE.CRM (\n   id INT,\n   name STRING\n)"
        effects = parse_catalog_effects(sql)
        self.assertEqual(effects[0]["action"], "create")
        self.assertEqual(effects[0]["object_type"], "table")
        self.assertEqual(effects[0]["scope"], {"database": "DATA_WAREHOUSE", "schema": "BRONZE", "object": "CRM"})

    def test_create_file_format(self):
        effects = parse_catalog_effects("CREATE OR REPLACE FILE FORMAT BRONZE.CSV_FORMAT TYPE = CSV")
        self.assertEqual(effects[0]["object_type"], "file_format")

    def test_drop_invalidates(self):
        effects = parse_catalog_effects("DROP TABLE IF EXISTS DATA_WAREHOUSE.BRONZE.CRM")
        self.assertEqual(effects[0]["action"], "invalidate")
        self.assertEqual(effects[0]["object_type"], "table")

    def test_copy_into_invalidates_table(self):
        effects = parse_catalog_effects("COPY INTO DATA_WAREHOUSE.BRONZE.CRM FROM @STAGING/source_crm/")
        self.assertEqual(effects[0], {"action": "invalidate", "object_type": "table",
                                      "scope": {"database": "DATA_WAREHOUSE", "schema": "BRONZE", "object": "CRM"}})

    def test_copy_into_stage_is_unload_no_effect(self):
        self.assertEqual(parse_catalog_effects("COPY INTO @STAGING/out FROM BRONZE.CRM"), [])

    def test_select_has_no_effect(self):
        self.assertEqual(parse_catalog_effects("SELECT $1, $2 FROM @STAGING (FILE_FORMAT => 'CSV_FORMAT')"), [])


# ---------------------------------------------------------------------------
# Exit criterion: the dispatcher applies effects (create / write-invalidation)
# from structured results, gated on risk — replacing the deleted regex path.
# ---------------------------------------------------------------------------
class _FakeConnector:
    name = "snowflake"

    def __init__(self, result):
        self._result = result

    async def invoke(self, op, args):
        return self._result


class _StubRegistry:
    def __init__(self, connector, spec):
        self._connector = connector
        self._spec = spec

    def resolve(self, tool_name):
        return (self._connector, tool_name)

    def get_operation_spec(self, tool_name):
        return self._spec


def _spec(risk):
    return OperationSpec(name="execute_snowflake_query", description="", parameters={}, capability="x", risk=risk)


class DispatcherPostconditionTest(unittest.TestCase):
    def _dispatch(self, effects, risk):
        memory = AgentMemory(state_path=_tmp("s"), history_path=_tmp("h"), memory_path=_tmp("m"))
        result = ToolResult(tool_name="snowflake", status=ToolStatus.SUCCESS,
                            metadata={"catalog_effects": effects})
        registry = _StubRegistry(_FakeConnector(result), _spec(risk))
        dispatcher = Dispatcher(registry, memory=memory)
        asyncio.run(dispatcher.execute("execute_snowflake_query", {"query": "..."}))
        return memory

    def test_risky_create_effect_records_catalog(self):
        memory = self._dispatch(
            [{"action": "create", "object_type": "table",
              "scope": {"database": "DW", "schema": "BRONZE", "object": "CRM"}}],
            Risk.RISKY,
        )
        self.assertTrue(memory.catalog.is_known("snowflake", "table",
                        {"database": "DW", "schema": "BRONZE", "object": "CRM"}))

    def test_risky_invalidate_effect_marks_stale(self):
        memory = AgentMemory(state_path=_tmp("s"), history_path=_tmp("h"), memory_path=_tmp("m"))
        memory.catalog.record_object("snowflake", "table", {"database": "DW", "schema": "BRONZE", "object": "CRM"})
        result = ToolResult(tool_name="snowflake", status=ToolStatus.SUCCESS,
                            metadata={"catalog_effects": [{"action": "invalidate", "object_type": "table",
                             "scope": {"database": "DW", "schema": "BRONZE", "object": "CRM"}}]})
        registry = _StubRegistry(_FakeConnector(result), _spec(Risk.IRREVERSIBLE))
        asyncio.run(Dispatcher(registry, memory=memory).execute("execute_snowflake_query", {}))
        self.assertFalse(memory.catalog.is_known("snowflake", "table",
                         {"database": "DW", "schema": "BRONZE", "object": "CRM"}))

    def test_safe_op_cannot_invalidate(self):
        memory = AgentMemory(state_path=_tmp("s"), history_path=_tmp("h"), memory_path=_tmp("m"))
        memory.catalog.record_object("snowflake", "table", {"schema": "BRONZE", "object": "CRM"})
        result = ToolResult(tool_name="snowflake", status=ToolStatus.SUCCESS,
                            metadata={"catalog_effects": [{"action": "invalidate", "object_type": "table",
                             "scope": {"schema": "BRONZE", "object": "CRM"}}]})
        registry = _StubRegistry(_FakeConnector(result), _spec(Risk.SAFE))
        asyncio.run(Dispatcher(registry, memory=memory).execute("introspect", {}))
        # SAFE op must NOT invalidate -> still trusted.
        self.assertTrue(memory.catalog.is_known("snowflake", "table", {"schema": "BRONZE", "object": "CRM"}))


# ---------------------------------------------------------------------------
# Exit criterion: add_loaded_table bug gone, replaced by catalog updates.
# ---------------------------------------------------------------------------
class LegacyApiTest(unittest.TestCase):
    def test_add_loaded_table_records_catalog_rowcount(self):
        memory = AgentMemory(state_path=_tmp("s"), history_path=_tmp("h"), memory_path=_tmp("m"))
        memory.add_loaded_table("DATA_WAREHOUSE.BRONZE.CRM", row_count=42)
        entry = memory.catalog.get("snowflake", "table",
                                   {"database": "DATA_WAREHOUSE", "schema": "BRONZE", "object": "CRM"})
        self.assertIsNotNone(entry)
        self.assertEqual(entry.row_count_estimate, 42)

    def test_progress_summary_derived_from_catalog(self):
        memory = AgentMemory(state_path=_tmp("s"), history_path=_tmp("h"), memory_path=_tmp("m"))
        memory.add_created_schema("BRONZE")
        memory.add_created_table("BRONZE.CRM")
        memory.add_loaded_table("BRONZE.CRM", row_count=10)
        summary = memory.get_progress_summary()
        self.assertEqual(summary["schemas_created"], 1)
        self.assertEqual(summary["tables_created"], 1)
        self.assertEqual(summary["total_rows_loaded"], 10)


# ---------------------------------------------------------------------------
# Exit criterion: episodic capture stores a task trace.
# ---------------------------------------------------------------------------
class EpisodicTest(unittest.TestCase):
    def test_capture_episode(self):
        memory = AgentMemory(state_path=_tmp("s"), history_path=_tmp("h"), memory_path=_tmp("m"))
        memory.capture_episode("build bronze", [{"tool": "execute_snowflake_query", "status": "success"}])
        episodes = memory.episodic.all()
        self.assertEqual(len(episodes), 1)
        self.assertEqual(episodes[0].kind, MemoryKind.EPISODIC.value)


if __name__ == "__main__":
    unittest.main()
