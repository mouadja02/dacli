"""P06 security hardening tests.

* Fix A — generated-connector validation runs in a child subprocess, so an
  LLM-authored module that misbehaves at import time never executes in the
  agent's (or this test's) process.
* Fix C — unset ``${VAR}`` references in config.yaml surface one clear warning
  and still load with the field empty.
* Fix E — MySQL introspection identifiers are escaped as data (backslash +
  quote) before being embedded in the information_schema literal.
"""

import io
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest import mock

from dacli.config import settings as settings_module
from dacli.config.settings import Settings, _substitute_env_vars, load_config
from dacli.core import connector_generator
from dacli.core.connector_generator import validate_connector


def _write_connector(root: Path, name: str, connector_py: str) -> None:
    d = root / name
    d.mkdir(parents=True)
    (d / "manifest.yaml").write_text(
        f"id: {name}\nname: {name}\nclass: dacli.connectors.{name}.connector.X\n",
        encoding="utf-8",
    )
    (d / "__init__.py").write_text("", encoding="utf-8")
    (d / "connector.py").write_text(connector_py, encoding="utf-8")


_GOOD_CONNECTOR = '''\
from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.core.verify import result_succeeded


class OkGenConnector(Connector):
    name = "okgen_p06"

    def operations(self):
        return [OperationSpec(
            name="noop", description="no-op",
            parameters={"type": "object", "properties": {}},
            capability="okgen.noop", risk=Risk.SAFE,
            postconditions=[result_succeeded()],
        )]

    async def invoke(self, op, args):
        return ToolResult(tool_name=op, status=ToolStatus.SUCCESS)

    async def health(self):
        return ToolResult(tool_name="health", status=ToolStatus.SUCCESS)
'''


class SandboxedValidationTest(unittest.TestCase):
    """Fix A: the validation import must never run in this process."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        patcher = mock.patch.object(connector_generator, "_CONNECTORS_DIR", self.root)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_import_time_raise_is_a_validation_failure_not_a_parent_import(self):
        _write_connector(
            self.root, "evilgen_p06",
            'raise RuntimeError("malicious module-level code ran")\n',
        )
        ok, msg = validate_connector("evilgen_p06", Settings())
        self.assertFalse(ok)
        self.assertIn("Import failed", msg)
        self.assertIn("malicious module-level code ran", msg)
        # The untrusted module was only ever imported in the child process.
        self.assertNotIn("dacli.connectors.evilgen_p06.connector", sys.modules)

    def test_valid_connector_passes_via_subprocess(self):
        _write_connector(self.root, "okgen_p06", _GOOD_CONNECTOR)
        ok, msg = validate_connector("okgen_p06", Settings())
        self.assertTrue(ok, msg)
        self.assertIn("1 operation(s)", msg)
        self.assertNotIn("dacli.connectors.okgen_p06.connector", sys.modules)

    def test_missing_postcondition_fails_in_subprocess(self):
        _write_connector(
            self.root, "nopc_p06",
            _GOOD_CONNECTOR.replace("postconditions=[result_succeeded()],",
                                    "postconditions=[],"),
        )
        ok, msg = validate_connector("nopc_p06", Settings())
        self.assertFalse(ok)
        self.assertIn("no post-condition", msg)

    def test_manifest_checks_still_run_before_the_subprocess(self):
        d = self.root / "nomanifest_p06"
        d.mkdir()
        (d / "connector.py").write_text("x = 1\n", encoding="utf-8")
        ok, msg = validate_connector("nomanifest_p06", Settings())
        self.assertFalse(ok)
        self.assertIn("manifest.yaml not found", msg)


class UnsetEnvVarWarningTest(unittest.TestCase):
    """Fix C: a missing ${VAR} still substitutes "" but is reported once."""

    def test_substitute_records_unresolved_names(self):
        unresolved: set = set()
        out = _substitute_env_vars(
            {"github": {"token": "${DACLI_P06_DEFINITELY_UNSET}"}}, unresolved
        )
        self.assertEqual(out, {"github": {"token": ""}})
        self.assertEqual(unresolved, {"DACLI_P06_DEFINITELY_UNSET"})

    def test_load_config_warns_once_and_still_loads(self):
        settings_module._warned_env_vars.discard("DACLI_P06_DEFINITELY_UNSET")
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "config.yaml"
            cfg.write_text(
                'connector_config:\n  github:\n    token: "${DACLI_P06_DEFINITELY_UNSET}"\n',
                encoding="utf-8",
            )
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                settings = load_config(str(cfg))
        self.assertEqual(settings.connector_config.get("github", {}).get("token"), "")
        self.assertIn("DACLI_P06_DEFINITELY_UNSET", stderr.getvalue())
        self.assertIn("Unset environment variables", stderr.getvalue())

    def test_warning_is_not_repeated_for_the_same_name(self):
        settings_module._warned_env_vars.add("DACLI_P06_ALREADY_WARNED")
        msg = settings_module._warn_unresolved_env_vars({"DACLI_P06_ALREADY_WARNED"})
        self.assertIsNone(msg)


if __name__ == "__main__":
    unittest.main()
