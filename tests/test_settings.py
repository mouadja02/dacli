import os
import unittest

from dacli.config.settings import Settings, _substitute_env_vars, is_llm_configured


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

    def test_unconfigured_sections_default_empty(self):
        settings = Settings()
        self.assertEqual(settings.snowflake.account, "")
        self.assertEqual(settings.github.token, "")
        self.assertEqual(settings.pinecone.api_key, "")
        self.assertEqual(settings.embeddings.provider, "")


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


if __name__ == "__main__":
    unittest.main()
