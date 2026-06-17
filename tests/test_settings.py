import json
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path

from cryptography.fernet import Fernet

from dacli.config.settings import (
    Settings,
    _substitute_env_vars,
    invalidate_config_cache,
    is_llm_configured,
    load_config,
)
from dacli.core.crypto import encrypt_value


class SettingsEnvSubstitutionTest(unittest.TestCase):
    def test_substitutes_braced_environment_variables(self):
        os.environ["DACLI_TEST_TOKEN"] = "token-value"
        os.environ["DACLI_TEST_PASSWORD"] = "password-value"

        try:
            config = {
                "github": {"token": "${DACLI_TEST_TOKEN}"},
                "snowflake": {"password": "${DACLI_TEST_PASSWORD}"},
                "literal": "prefix-${DACLI_TEST_TOKEN}-suffix",
            }

            self.assertEqual(
                _substitute_env_vars(config),
                {
                    "github": {"token": "token-value"},
                    "snowflake": {"password": "password-value"},
                    "literal": "prefix-token-value-suffix",
                },
            )
        finally:
            os.environ.pop("DACLI_TEST_TOKEN", None)
            os.environ.pop("DACLI_TEST_PASSWORD", None)


class SettingsConstructibleTest(unittest.TestCase):
    """First-run keystone: Settings() must never raise (no config at all)."""

    def test_settings_constructs_with_no_arguments(self):
        settings = Settings()
        self.assertEqual(settings.llm.provider, "openai")
        self.assertEqual(settings.llm.model, "")
        self.assertEqual(settings.llm.api_key, "")
        self.assertEqual(settings.llm.base_url, "")

    def test_unconfigured_connectors_default_empty(self):
        # Connectors read via ConnectorConfig (manifest-config pattern, 09/A-4),
        # which is fail-soft: a missing connector/field returns the default.
        from dacli.config.settings import ConnectorConfig

        settings = Settings()
        self.assertEqual(settings.connector_config, {})
        self.assertEqual(ConnectorConfig(settings, "snowflake").get("account", ""), "")
        self.assertEqual(ConnectorConfig(settings, "github").get("token", ""), "")
        self.assertEqual(ConnectorConfig(settings, "pinecone").get("api_key", ""), "")


class IsLlmConfiguredTest(unittest.TestCase):
    def _settings(self, **llm):
        return Settings(llm=llm)

    def test_blank_settings_are_unconfigured(self):
        self.assertFalse(is_llm_configured(Settings()))

    def test_blank_api_key_is_unconfigured(self):
        self.assertFalse(is_llm_configured(self._settings(model="gpt-4o-mini")))

    def test_unresolved_env_placeholder_key_is_unconfigured(self):
        settings = self._settings(model="gpt-4o-mini", api_key="${OPENAI_API_KEY}")
        self.assertFalse(is_llm_configured(settings))

    def test_missing_model_is_unconfigured(self):
        self.assertFalse(is_llm_configured(self._settings(api_key="sk-real-key")))

    def test_real_key_and_model_is_configured(self):
        settings = self._settings(model="gpt-4o-mini", api_key="sk-real-key")
        self.assertTrue(is_llm_configured(settings))


class LoadConfigCacheTest(unittest.TestCase):
    """load_config caches on unchanged mtime and reloads on change/invalidation.

    Stale config is a reliability bug (a saved secret the next turn can't see),
    so invalidation correctness matters more than the cache hit.
    """

    def setUp(self):
        invalidate_config_cache()
        self._dir = tempfile.mkdtemp(prefix="dacli_cfg_")
        self._cfg = Path(self._dir, "config.yaml")

    def tearDown(self):
        invalidate_config_cache()
        shutil.rmtree(self._dir, ignore_errors=True)

    def _state_path(self) -> str:
        return Path(self._dir, "state").as_posix() + "/"

    def _write(self, body: str) -> None:
        self._cfg.write_text(
            body + f"agent:\n  state_path: {self._state_path()}\n", encoding="utf-8"
        )

    def test_returns_cached_on_unchanged_mtime(self):
        self._write("llm:\n  model: gpt-a\n")
        s1 = load_config(str(self._cfg))
        s2 = load_config(str(self._cfg))
        self.assertIs(s1, s2)

    def test_invalidate_forces_reload(self):
        self._write("llm:\n  model: gpt-a\n")
        s1 = load_config(str(self._cfg))
        invalidate_config_cache()
        s2 = load_config(str(self._cfg))
        self.assertIsNot(s1, s2)
        self.assertEqual(s2.llm.model, "gpt-a")

    def test_file_change_reloads(self):
        self._write("llm:\n  model: gpt-a\n")
        s1 = load_config(str(self._cfg))
        self._write("llm:\n  model: gpt-b\n")
        # Bump mtime forward so the cache sees the change on coarse-clock filesystems.
        future = time.time() + 10
        os.utime(self._cfg, (future, future))
        s2 = load_config(str(self._cfg))
        self.assertEqual(s1.llm.model, "gpt-a")
        self.assertEqual(s2.llm.model, "gpt-b")

    def test_wizard_secret_visible_after_invalidation(self):
        # snowflake is on the manifest-config pattern: its secret is stored under
        # secrets.snowflake.* and _overlay_secrets routes it into
        # connector_config.snowflake, where ConnectorConfig reads it.
        from dacli.config.settings import ConnectorConfig

        self._write("connector_config:\n  snowflake:\n    account: acct\n")
        s1 = load_config(str(self._cfg))
        self.assertEqual(ConnectorConfig(s1, "snowflake").get("password", ""), "")

        # The wizard writes an encrypted secret into dacli.json at the base dir
        # (parent of state_path). A .key there makes encryption deterministic.
        Path(self._dir, ".key").write_bytes(Fernet.generate_key())
        enc = encrypt_value("hunter2", base_dir=self._dir)
        Path(self._dir, "dacli.json").write_text(
            json.dumps({"secrets": {"snowflake": {"password": enc}}}), encoding="utf-8"
        )
        invalidate_config_cache()
        s2 = load_config(str(self._cfg))
        self.assertEqual(ConnectorConfig(s2, "snowflake").get("password"), "hunter2")


if __name__ == "__main__":
    unittest.main()
