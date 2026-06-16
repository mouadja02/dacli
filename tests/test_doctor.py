"""P04: `dacli doctor` + `/doctor` — one collector, two surfaces.

Covers configured vs. unconfigured LLM, the key-source detection (never the
value), a skipped connector surfaced with its reason, and the json/exit-code
contract. Drives the Click group through CliRunner in an isolated filesystem so
nothing touches the real config/state dirs.
"""

import json
import os
import unittest
from pathlib import Path

from click.testing import CliRunner

from dacli.config.settings import Settings, load_config
from dacli.core import doctor
from dacli.scripts.cli import cli

_ENV_KEYS = ("DACLI_HOME", "DACLI_STATE_PATH", "DACLI_ENCRYPTION_KEY", "OPENAI_API_KEY")

# A config.yaml makes the cwd a project (so paths resolve project-local and the
# group's "running outside a project" notice never pollutes --json stdout).
_CONFIGURED = """\
llm:
  provider: openai
  model: gpt-4o
  api_key: sk-test-not-a-real-key
"""

_ENV_REF = """\
llm:
  provider: openai
  model: gpt-4o
  api_key: ${OPENAI_API_KEY}
"""


class DoctorTest(unittest.TestCase):
    def setUp(self):
        self._saved = {k: os.environ.get(k) for k in _ENV_KEYS}
        for k in _ENV_KEYS:
            os.environ.pop(k, None)

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    # ---- collect() ------------------------------------------------------

    def test_unconfigured_llm_is_a_problem(self):
        diag = doctor.collect(Settings())
        self.assertFalse(diag.llm["key"])
        self.assertFalse(diag.ok)
        self.assertEqual(diag.llm["ping"], "skipped")

    def test_configured_llm_key_present_no_problem(self):
        s = Settings.model_validate({"llm": {"provider": "openai", "model": "gpt-4o",
                                             "api_key": "sk-x"}})
        diag = doctor.collect(s)
        self.assertTrue(diag.llm["key"])
        self.assertTrue(diag.ok)

    def test_key_source_env_vs_config(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("config.yaml").write_text(_ENV_REF, encoding="utf-8")
            os.environ["OPENAI_API_KEY"] = "sk-from-env"
            diag = doctor.collect(load_config("config.yaml"), config_path="config.yaml")
            self.assertEqual(diag.llm["source"], "env")

            Path("config.yaml").write_text(_CONFIGURED, encoding="utf-8")
            diag = doctor.collect(load_config("config.yaml"), config_path="config.yaml")
            self.assertEqual(diag.llm["source"], "config")

    # ---- secret hygiene -------------------------------------------------

    def test_no_secret_value_ever_printed(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("config.yaml").write_text(_CONFIGURED, encoding="utf-8")
            result = runner.invoke(cli, ["doctor", "--config", "config.yaml"])
            self.assertNotIn("sk-test-not-a-real-key", result.output)
            result = runner.invoke(cli, ["doctor", "--config", "config.yaml", "--json"])
            self.assertNotIn("sk-test-not-a-real-key", result.output)

    # ---- json / exit-code contract -------------------------------------

    def test_json_parses_and_exit_nonzero_without_key(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Project dir but no LLM key -> hard problem -> exit 1, clean JSON.
            Path("config.yaml").write_text("llm:\n  provider: openai\n", encoding="utf-8")
            result = runner.invoke(cli, ["doctor", "--config", "config.yaml", "--json"])
            self.assertEqual(result.exit_code, 1, result.output)
            payload = json.loads(result.output)
            self.assertFalse(payload["ok"])
            self.assertFalse(payload["llm"]["key"])
            self.assertIn("state_dir", payload)

    def test_json_exit_zero_when_configured(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("config.yaml").write_text(_CONFIGURED, encoding="utf-8")
            result = runner.invoke(cli, ["doctor", "--config", "config.yaml", "--json"])
            self.assertEqual(result.exit_code, 0, result.output)
            self.assertTrue(json.loads(result.output)["ok"])

    # ---- connectors -----------------------------------------------------

    def test_skipped_connector_surfaced_with_reason(self):
        s = Settings()

        class _Reg:
            def get_catalog(self):
                return {"good": {}}

            def is_connector_enabled(self, _cid):
                return True

            def failed_connectors(self):
                return {"broken": "load error: boom"}

        import dacli.core.doctor as mod
        from dacli.connectors import registry as reg_mod

        orig = reg_mod.ConnectorRegistry
        mod_orig = getattr(mod, "ConnectorRegistry", None)
        reg_mod.ConnectorRegistry = lambda *a, **k: _Reg()
        try:
            diag = doctor.collect(s)
        finally:
            reg_mod.ConnectorRegistry = orig
            if mod_orig is not None:
                mod.ConnectorRegistry = mod_orig

        self.assertEqual(diag.connectors["enabled"], 1)
        self.assertEqual(diag.connectors["skipped"], 1)
        self.assertEqual(diag.connectors["skipped_detail"]["broken"], "load error: boom")

    # ---- help surfaces --------------------------------------------------

    def test_doctor_listed_in_help(self):
        result = CliRunner().invoke(cli, ["--help"])
        self.assertIn("doctor", result.output)


if __name__ == "__main__":
    unittest.main()
