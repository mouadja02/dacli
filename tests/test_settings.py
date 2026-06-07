import os
import unittest

from dacli.config.settings import _substitute_env_vars


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


if __name__ == "__main__":
    unittest.main()
