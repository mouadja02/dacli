"""F-8 (P11) — connector index / `dacli connector install`.

Installs from a local fixture "index" (no network): asserts the connector
folder is written, registered disabled, validated in the sandboxed subprocess,
and that a broken download is fault-isolated by the registry.
"""

import tempfile
import unittest
from pathlib import Path

import yaml

from dacli.config.settings import Settings
from dacli.connectors.registry import ConnectorRegistry, load_connectors_config
from dacli.core.connector_index import install_connector, load_index

_GOOD_CONNECTOR = '''\
from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.core.verify import result_succeeded


class WeatherixConnector(Connector):
    name = "weatherix"

    def operations(self):
        return [OperationSpec(
            name="weatherix_ping", description="no-op ping",
            parameters={"type": "object", "properties": {}},
            capability="weatherix.ping", risk=Risk.SAFE,
            postconditions=[result_succeeded()],
        )]

    async def invoke(self, op, args):
        return ToolResult(tool_name=op, status=ToolStatus.SUCCESS)

    async def health(self):
        return ToolResult(tool_name="health", status=ToolStatus.SUCCESS)
'''

_MANIFEST = """\
id: weatherix
name: Weatherix
class: dacli.connectors.weatherix.connector.WeatherixConnector
enabled: false
description: Example shared connector.
"""


def _build_index(root: Path, *, connector_py: str = _GOOD_CONNECTOR) -> str:
    """Lay out <root>/index.yaml + <root>/weatherix/{manifest,connector}."""
    folder = root / "weatherix"
    folder.mkdir(parents=True)
    (folder / "manifest.yaml").write_text(_MANIFEST, encoding="utf-8")
    (folder / "connector.py").write_text(connector_py, encoding="utf-8")
    index = root / "index.yaml"
    index.write_text(yaml.safe_dump({
        "connectors": {
            "weatherix": {
                "description": "Example shared connector.",
                "files": ["manifest.yaml", "connector.py"],
            },
        },
    }), encoding="utf-8")
    return str(index)


class InstallConnectorTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        self.index = _build_index(root / "remote")
        self.dest = root / "installed"
        self.dest.mkdir()
        self.config_path = str(root / "connectors.yaml")

    def _install(self, name="weatherix", **kw):
        return install_connector(
            name, Settings(), self.index,
            connectors_dir=self.dest, config_path=self.config_path, **kw,
        )

    def test_install_writes_folder_disabled_and_validates(self):
        ok, msg = self._install()
        self.assertTrue(ok, msg)
        self.assertTrue((self.dest / "weatherix" / "manifest.yaml").exists())
        self.assertTrue((self.dest / "weatherix" / "connector.py").exists())
        self.assertTrue((self.dest / "weatherix" / "__init__.py").exists())
        # Registered disabled (deny-by-default), and the user is told the path.
        config = load_connectors_config(self.config_path)
        self.assertFalse(config["connectors"]["weatherix"]["enabled"])
        self.assertIn("/connect weatherix", msg)

    def test_installed_folder_is_discovered_without_crashing_startup(self):
        ok, _ = self._install()
        self.assertTrue(ok)
        # The canonical class path is not importable from a temp dir, so the
        # registry must fault-isolate it (recorded in _failed) — a bad or
        # unimportable download never takes the agent down.
        registry = ConnectorRegistry(
            Settings(), connectors_dir=str(self.dest), config_path=self.config_path
        )
        self.assertIn("weatherix", registry.failed_connectors())
        self.assertIsNone(registry.get_connector("weatherix"))

    def test_validation_failure_reports_and_stays_disabled(self):
        index = _build_index(
            Path(self._tmp.name) / "remote_bad",
            connector_py="x = 1  # no Connector subclass\n",
        )
        ok, msg = install_connector(
            "weatherix", Settings(), index,
            connectors_dir=self.dest, config_path=self.config_path,
        )
        self.assertFalse(ok)
        self.assertIn("validation FAILED", msg)
        config = load_connectors_config(self.config_path)
        self.assertFalse(config["connectors"]["weatherix"]["enabled"])

    def test_unknown_name_and_bad_names_are_rejected(self):
        ok, msg = self._install("nope")
        self.assertFalse(ok)
        self.assertIn("not in the index", msg)
        for bad in ("../evil", "Evil", "a/b", ""):
            ok, msg = self._install(bad)
            self.assertFalse(ok, msg)
            self.assertIn("invalid connector name", msg)

    def test_existing_folder_requires_force(self):
        ok, _ = self._install()
        self.assertTrue(ok)
        ok, msg = self._install()
        self.assertFalse(ok)
        self.assertIn("--force", msg)
        ok, msg = self._install(force=True)
        self.assertTrue(ok, msg)

    def test_suspicious_file_names_are_refused(self):
        index_path = Path(self.index)
        data = yaml.safe_load(index_path.read_text(encoding="utf-8"))
        data["connectors"]["weatherix"]["files"] = [
            "manifest.yaml", "connector.py", "..\\..\\evil.py",
        ]
        index_path.write_text(yaml.safe_dump(data), encoding="utf-8")
        ok, msg = self._install()
        self.assertFalse(ok)
        self.assertIn("suspicious file name", msg)

    def test_index_must_list_manifest_and_connector(self):
        index_path = Path(self.index)
        data = yaml.safe_load(index_path.read_text(encoding="utf-8"))
        data["connectors"]["weatherix"]["files"] = ["connector.py"]
        index_path.write_text(yaml.safe_dump(data), encoding="utf-8")
        ok, msg = self._install()
        self.assertFalse(ok)
        self.assertIn("manifest.yaml", msg)


class LoadIndexTest(unittest.TestCase):
    def test_load_index_rejects_shapeless_files(self):
        root = Path(tempfile.mkdtemp(prefix="dacli_p11_idx_"))
        bad = root / "index.yaml"
        bad.write_text("just a string", encoding="utf-8")
        with self.assertRaises(ValueError):
            load_index(str(bad))


if __name__ == "__main__":
    unittest.main()
