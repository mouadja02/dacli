"""F-10 (P11) — `dacli export-run` compliance bundle.

Seeds a session (history + state + audit slice + usage with secrets) and
asserts the zip contains every artifact with no secret values inside.
"""

import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from click.testing import CliRunner

from dacli.config.settings import Settings
from dacli.core.export_run import export_run_bundle

SECRET = "tok-hunter2-do-not-leak"
SID = "20260612_010203"
OTHER_SID = "20260611_000000"


def _seed(base: Path) -> Settings:
    state_dir = base / "state"
    history_dir = base / "history"
    state_dir.mkdir(parents=True)
    history_dir.mkdir(parents=True)

    (history_dir / f"history_{SID}.json").write_text(json.dumps([
        {"role": "user", "content": "load the orders file", "timestamp": "t"},
        {"role": "assistant", "content": "done", "timestamp": "t"},
    ]), encoding="utf-8")
    (state_dir / f"state_{SID}.json").write_text(json.dumps({
        "session_id": SID, "created_at": "t", "updated_at": "t",
        "todos": [], "discovered_files": {},
        "tool_history": [
            # A tool log entry whose params carry a secret-keyed value.
            {"tool_name": "github_op", "timestamp": "t", "status": "success",
             "input_params": {"token": SECRET}, "result": None, "error": None,
             "execution_time_ms": 1.0},
        ],
    }), encoding="utf-8")

    # Audit ledger with this session's events plus another session's.
    audit = base / "audit.jsonl"
    lines = [
        {"kind": "classification", "tool_name": "execute_query",
         "session_id": SID, "decision_id": "d1", "tier": "safe",
         "summary": "read-only", "detail": {"api_key": SECRET}, "timestamp": "t"},
        {"kind": "execution", "tool_name": "execute_query",
         "session_id": OTHER_SID, "decision_id": "d9", "tier": "safe",
         "summary": "other session", "detail": {}, "timestamp": "t"},
    ]
    audit.write_text(
        "\n".join(json.dumps(line) for line in lines) + "\n", encoding="utf-8"
    )

    # Usage store (dacli.json) with a per-session bucket.
    (base / "dacli.json").write_text(json.dumps({
        "version": 1,
        "usage": {
            "totals": {"input": 10, "output": 5, "requests": 1, "costUSD": 0.01},
            "byModel": {},
            "sessions": {SID: {"input": 10, "output": 5, "requests": 1,
                               "costUSD": 0.01, "model": "m"}},
        },
    }), encoding="utf-8")

    settings = Settings()
    settings.agent.state_path = str(state_dir)
    settings.agent.history_path = str(history_dir)
    return settings


class ExportRunBundleTest(unittest.TestCase):
    def setUp(self):
        self.base = Path(tempfile.mkdtemp(prefix="dacli_p11_export_"))
        self.settings = _seed(self.base)
        self.out = self.base / "bundle.zip"

    def _export(self, session=SID):
        return export_run_bundle(self.settings, session, str(self.out))

    def test_bundle_contains_all_artifacts(self):
        manifest = self._export()
        with zipfile.ZipFile(self.out) as zf:
            names = set(zf.namelist())
        self.assertEqual(
            names,
            {"manifest.json", "history.json", "state.json",
             "audit.jsonl", "usage.json"},
        )
        self.assertEqual(manifest["session_id"], SID)
        self.assertEqual(manifest["counts"]["messages"], 2)
        self.assertEqual(manifest["counts"]["audit_events"], 1)

    def test_audit_slice_is_session_scoped(self):
        self._export()
        with zipfile.ZipFile(self.out) as zf:
            audit = zf.read("audit.jsonl").decode("utf-8")
        self.assertIn("d1", audit)
        self.assertNotIn(OTHER_SID, audit)

    def test_no_secret_values_anywhere_in_the_bundle(self):
        self._export()
        with zipfile.ZipFile(self.out) as zf:
            for name in zf.namelist():
                body = zf.read(name).decode("utf-8")
                self.assertNotIn(SECRET, body, msg=f"secret leaked into {name}")
        # The redaction kept the keys (auditability), masking only values.
        with zipfile.ZipFile(self.out) as zf:
            state = json.loads(zf.read("state.json"))
        self.assertEqual(state["tool_history"][0]["input_params"]["token"], "***")

    def test_defaults_to_latest_session(self):
        manifest = export_run_bundle(self.settings, None, str(self.out))
        self.assertEqual(manifest["session_id"], SID)

    def test_unknown_session_raises(self):
        with self.assertRaises(FileNotFoundError):
            self._export(session="nope")


class ExportRunCliTest(unittest.TestCase):
    def test_cli_exports_and_reports(self):
        base = Path(tempfile.mkdtemp(prefix="dacli_p11_exportcli_"))
        settings = _seed(base)
        out = base / "cli_bundle.zip"
        from unittest import mock
        from dacli.scripts.cli import cli

        with mock.patch("dacli.scripts.cli.load_config", return_value=settings):
            res = CliRunner().invoke(
                cli, ["export-run", "--session", SID, "--out", str(out)]
            )
        self.assertEqual(res.exit_code, 0, msg=res.output)
        self.assertIn("Exported session", res.output)
        self.assertTrue(out.exists())

    def test_cli_unknown_session_exits_1(self):
        base = Path(tempfile.mkdtemp(prefix="dacli_p11_exportcli2_"))
        settings = _seed(base)
        from unittest import mock
        from dacli.scripts.cli import cli

        with mock.patch("dacli.scripts.cli.load_config", return_value=settings):
            res = CliRunner().invoke(cli, ["export-run", "--session", "missing"])
        self.assertEqual(res.exit_code, 1)


if __name__ == "__main__":
    unittest.main()
