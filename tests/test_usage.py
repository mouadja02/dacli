"""Offline tests for usage/cost tracking (no network).

Covers models.dev pricing parsing + cost math (core/pricing.py) and the
persistent store's secret redaction + usage accumulation (core/store.py).
"""

import json
import tempfile
import unittest

from dacli.config.settings import _overlay_secrets
from dacli.ai.pricing import TokenUsage, pricing_from_payload
from dacli.core.store import DacliStore, _redact


# A trimmed models.dev-shaped payload.
PAYLOAD = {
    "anthropic": {
        "models": {
            "claude-opus-4-8": {
                "cost": {"input": 3, "output": 15, "cache_read": 0.3, "cache_write": 3.75},
                "limit": {"context": 200000, "output": 4096},
            }
        }
    },
    "openai": {
        "models": {
            "gpt-x": {"cost": {"input": 2.5, "output": 10}}  # no cache fields
        }
    },
}


class PricingTest(unittest.TestCase):
    def test_parse_and_cost_with_cache(self):
        p = pricing_from_payload(PAYLOAD, "anthropic", "claude-opus-4-8")
        self.assertIsNotNone(p)
        usage = TokenUsage(input=1_000_000, output=1_000_000, cache_read=1_000_000, cache_creation=1_000_000)
        # 3 + 15 + 0.3 + 3.75 per 1M each
        self.assertAlmostEqual(p.cost_for(usage), 22.05, places=4)

    def test_missing_cache_fields_default_zero(self):
        p = pricing_from_payload(PAYLOAD, "openai", "gpt-x")
        self.assertEqual((p.cache_read, p.cache_write), (0.0, 0.0))
        usage = TokenUsage(input=2_000_000, output=1_000_000, cache_read=5_000_000)
        # cache_read priced at 0 -> only input*2.5 + output*10
        self.assertAlmostEqual(p.cost_for(usage), 2 * 2.5 + 1 * 10, places=4)

    def test_provider_fallback_scan(self):
        # Wrong provider name still resolves by scanning all providers for the model id.
        p = pricing_from_payload(PAYLOAD, "openrouter", "claude-opus-4-8")
        self.assertIsNotNone(p)
        self.assertEqual(p.input, 3.0)

    def test_unknown_model_returns_none(self):
        self.assertIsNone(pricing_from_payload(PAYLOAD, "anthropic", "does-not-exist"))

    def test_exact_match_is_not_fuzzy(self):
        p = pricing_from_payload(PAYLOAD, "openai", "gpt-x")
        self.assertEqual(p.match, "exact")
        self.assertFalse(p.is_fuzzy)


# A models.dev-shaped payload with an OpenRouter provider carrying routed ids.
ROUTED_PAYLOAD = {
    "openrouter": {
        "models": {
            "openai/gpt-oss-120b": {"cost": {"input": 0.05, "output": 0.25}},
            "anthropic/claude-sonnet-4.6": {"cost": {"input": 3, "output": 15}},
        }
    },
    "openai": {
        "models": {"gpt-oss-120b": {"cost": {"input": 0.1, "output": 0.4}}}
    },
}


class SimilarityPricingTest(unittest.TestCase):
    def test_variant_suffix_resolves_to_base_in_same_provider(self):
        # The reported bug: ':nitro' variant priced as the base model, and from
        # the configured provider (OpenRouter), not the openai-native entry.
        p = pricing_from_payload(ROUTED_PAYLOAD, "openrouter", "openai/gpt-oss-120b:nitro")
        self.assertIsNotNone(p)
        self.assertEqual(p.resolved_provider, "openrouter")
        self.assertEqual(p.resolved_model, "openai/gpt-oss-120b")
        self.assertEqual(p.input, 0.05)            # OpenRouter price, not openai's 0.10
        self.assertTrue(p.is_fuzzy)
        self.assertEqual(p.match, "normalized")

    def test_basename_only_query_matches(self):
        p = pricing_from_payload(ROUTED_PAYLOAD, "openrouter", "gpt-oss-120b")
        self.assertIsNotNone(p)
        self.assertEqual(p.resolved_model, "gpt-oss-120b")  # exact id in 'openai'

    def test_close_but_distinct_model_matches_by_similarity(self):
        p = pricing_from_payload(ROUTED_PAYLOAD, "openrouter", "anthropic/claude-sonnet-4.5:floor")
        self.assertIsNotNone(p)
        self.assertEqual(p.resolved_model, "anthropic/claude-sonnet-4.6")
        self.assertGreaterEqual(p.similarity, 0.62)

    def test_unrelated_model_stays_none(self):
        # Better an honest "unknown" than a confidently wrong price.
        self.assertIsNone(
            pricing_from_payload(ROUTED_PAYLOAD, "openrouter", "mistral/totally-unrelated-xyz"))

    def test_token_usage_roundtrip_and_add(self):
        a = TokenUsage(input=10, output=5, cache_read=2, cache_creation=1)
        a.add(TokenUsage.from_dict({"input": 1, "output": 1}))
        self.assertEqual(a.input, 11)
        self.assertEqual(a.total, 11 + 6 + 2 + 1)
        self.assertEqual(TokenUsage.from_dict(a.as_dict()).output, 6)


class _StubSettings:
    """Mimics pydantic Settings.model_dump() for redaction testing."""

    def __init__(self, data):
        self._data = data

    def model_dump(self, mode=None):
        return self._data


class StoreTest(unittest.TestCase):
    def _store(self):
        d = tempfile.mkdtemp(prefix="dacli_store_")
        return DacliStore(base_dir=d), d

    def test_redact_secrets(self):
        red = _redact({"api_key": "sk-x", "model": "m", "nested": {"password": "p", "keep": 1, "token": "t"}})
        self.assertEqual(red["api_key"], "***")
        self.assertEqual(red["model"], "m")
        self.assertEqual(red["nested"]["password"], "***")
        self.assertEqual(red["nested"]["token"], "***")
        self.assertEqual(red["nested"]["keep"], 1)

    def test_snapshot_config_redacts_and_structures(self):
        store, _ = self._store()
        settings = _StubSettings({
            "llm": {"provider": "anthropic", "model": "claude-x", "api_key": "sk-secret", "base_url": "u"},
            "snowflake": {"account": "ACC", "password": "pw", "database": "DB", "db_schema": "PUBLIC", "user": "U"},
            "github": {"owner": "o", "repo": "r", "branch": "main", "token": "ghp_secret"},
            "pinecone": {"index_name": "idx", "environment": "env", "api_key": "pc_secret"},
            "embeddings": {"provider": "openai", "model": "emb", "api_key": "e_secret"},
            "agent": {"max_iterations": 100},
            "ui": {"theme": "dark"},
        })
        store.snapshot_config(settings)
        cfg = store.data["config"]
        # No secret value appears anywhere in the serialized snapshot.
        blob = json.dumps(cfg)
        for secret in ("sk-secret", "pw", "ghp_secret", "pc_secret", "e_secret"):
            self.assertNotIn(secret, blob)
        self.assertEqual(cfg["llm"]["api_key"], "***")
        self.assertEqual(cfg["connectors"]["snowflake"]["account"], "ACC")
        self.assertEqual(cfg["connectors"]["github"]["owner"], "o")

    def test_record_usage_accumulates(self):
        store, _ = self._store()
        u = TokenUsage(input=100, output=50, cache_read=10, cache_creation=5)
        store.record_usage("sess1", "claude-x", u, cost=0.5, first_prompt="hello world")
        store.record_usage("sess1", "claude-x", u, cost=0.5)
        totals = store.usage_summary("sess1")
        self.assertEqual(totals["totals"]["requests"], 2)
        self.assertEqual(totals["totals"]["input"], 200)
        self.assertAlmostEqual(totals["totals"]["costUSD"], 1.0, places=6)
        self.assertIn("claude-x", totals["byModel"])
        self.assertEqual(totals["session"]["requests"], 2)
        self.assertEqual(totals["session"]["firstPrompt"], "hello world")

    def test_secrets_roundtrip(self):
        store, base = self._store()
        store.set_secret("snowflake", "password", "PW")
        store.set_secret("github", "token", "TOK")
        store.save()
        reloaded = DacliStore(base_dir=base)
        self.assertEqual(reloaded.get_secrets()["snowflake"]["password"], "PW")
        self.assertEqual(reloaded.get_secrets()["github"]["token"], "TOK")

    def test_snapshot_keeps_secrets_block_separate(self):
        # Redacted snapshot must not leak the real secrets stored alongside it.
        store, _ = self._store()
        store.set_secret("snowflake", "password", "REAL_PW")
        store.snapshot_config(_StubSettings({"snowflake": {"account": "ACC", "password": "REAL_PW"}}))
        self.assertNotIn("REAL_PW", json.dumps(store.data["config"]))
        self.assertEqual(store.get_secrets()["snowflake"]["password"], "REAL_PW")

    def test_startup_and_roundtrip(self):
        store, base = self._store()
        store.record_startup()
        store.record_startup()
        store.record_usage("s", "m", TokenUsage(input=7, output=3), cost=0.01)
        store.save()

        reloaded = DacliStore(base_dir=base)
        self.assertEqual(reloaded.data["numStartups"], 2)
        self.assertIsNotNone(reloaded.data["firstStartTime"])
        self.assertEqual(reloaded.usage_summary()["totals"]["input"], 7)


class OverlayTest(unittest.TestCase):
    def test_overlay_fills_placeholders_only(self):
        # llm is a typed harness section; the connectors are on the manifest-config
        # pattern, so their secrets land under connector_config.<id> (09/A-4).
        cfg = {
            "llm": {"api_key": ""},
            "connector_config": {
                "snowflake": {"account": "ACC", "password": "${SF_PW}"},
                "github": {"token": "explicit-real-token"},
            },
        }
        secrets = {
            "snowflake": {"password": "PW"},
            "github": {"token": "should-not-override"},
            "llm": {"api_key": "LLM"},
        }
        out = _overlay_secrets(cfg, secrets)
        cc = out["connector_config"]
        self.assertEqual(cc["snowflake"]["password"], "PW")        # ${...} filled
        self.assertEqual(out["llm"]["api_key"], "LLM")             # typed section filled
        self.assertEqual(cc["github"]["token"], "explicit-real-token")  # explicit kept
        self.assertEqual(cc["snowflake"]["account"], "ACC")

    def test_overlay_materializes_connector_secret(self):
        # A /connect-stored secret with no prior config.yaml entry still reaches
        # the connector by materializing connector_config.<id>.
        out = _overlay_secrets({}, {"snowflake": {"password": "PW"}})
        self.assertEqual(out["connector_config"]["snowflake"]["password"], "PW")


if __name__ == "__main__":
    unittest.main()
