"""P09: key-file location/ACL hardening + the cwd .env trust boundary.

Offline, no real keyring or platform calls — keyring is faked via sys.modules and
the Windows path is forced by monkeypatching os.name + subprocess.
"""

import io
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from cryptography.fernet import Fernet

from dacli.core import crypto


class _EnvSandbox(unittest.TestCase):
    """Isolate the env vars and cwd the key/dotenv resolution reads."""

    _KEYS = (
        "DACLI_HOME",
        "DACLI_STATE_PATH",
        "DACLI_ENCRYPTION_KEY",
        "DACLI_KEY_BACKEND",
        "DACLI_USE_DOTENV",
    )

    def setUp(self):
        self._saved = {k: os.environ.get(k) for k in self._KEYS}
        for k in self._KEYS:
            os.environ.pop(k, None)
        self._home = tempfile.mkdtemp(prefix="dacli_home_")
        os.environ["DACLI_HOME"] = self._home
        crypto.reset_key_cache()

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        crypto.reset_key_cache()


class BaseDirDefaultTest(_EnvSandbox):
    def test_fresh_default_lands_in_per_user_dir(self):
        # Outside a project, no override -> the per-user config dir (DACLI_HOME).
        with tempfile.TemporaryDirectory() as cwd:
            prev = os.getcwd()
            os.chdir(cwd)
            try:
                self.assertEqual(crypto.resolve_base_dir(), Path(self._home))
            finally:
                os.chdir(prev)

    def test_legacy_cwd_key_still_used(self):
        # An existing cwd .dacli/.key predates the move and must keep working.
        with tempfile.TemporaryDirectory() as cwd:
            prev = os.getcwd()
            os.chdir(cwd)
            try:
                legacy = Path(cwd) / ".dacli"
                legacy.mkdir()
                (legacy / ".key").write_bytes(Fernet.generate_key())
                self.assertEqual(crypto.resolve_base_dir().resolve(), legacy.resolve())
            finally:
                os.chdir(prev)

    def test_explicit_state_path_wins(self):
        self.assertEqual(
            crypto.resolve_base_dir("/somewhere/state/"), Path("/somewhere")
        )


class WindowsAclTest(_EnvSandbox):
    # _secure_key_file is exercised directly on a path built *before* the os.name
    # patch: patching the global os.name to "nt" makes pathlib build a WindowsPath
    # from a string, which a POSIX CI runner can't instantiate.
    def test_icacls_invoked_on_windows_key_creation(self):
        with tempfile.TemporaryDirectory() as base:
            key_file = Path(base) / ".key"
            key_file.write_bytes(Fernet.generate_key())
            with mock.patch.object(crypto.os, "name", "nt"), mock.patch(
                "subprocess.run"
            ) as run:
                crypto._secure_key_file(key_file)
            self.assertTrue(run.called)
            argv = run.call_args.args[0]
            self.assertEqual(argv[0], "icacls")
            self.assertIn("/inheritance:r", argv)

    def test_broad_acl_warning_when_icacls_fails(self):
        crypto._warned_broad_acl.clear()
        stderr = io.StringIO()
        with tempfile.TemporaryDirectory() as base:
            key_file = Path(base) / ".key"
            key_file.write_bytes(Fernet.generate_key())
            with mock.patch.object(crypto.os, "name", "nt"), mock.patch(
                "subprocess.run", side_effect=OSError("no icacls")
            ), mock.patch.object(sys, "stderr", stderr):
                crypto._secure_key_file(key_file)
            self.assertIn("ACL", stderr.getvalue())
            # A second key in the same dir doesn't re-spam.
            self.assertIn(str(key_file.parent), crypto._warned_broad_acl)


class _FakeKeyring:
    """Minimal in-memory stand-in for the optional `keyring` package."""

    def __init__(self):
        self.store: dict[tuple[str, str], str] = {}

    def get_password(self, service, account):
        return self.store.get((service, account))

    def set_password(self, service, account, value):
        self.store[(service, account)] = value


class KeyringBackendTest(_EnvSandbox):
    def setUp(self):
        super().setUp()
        os.environ["DACLI_KEY_BACKEND"] = "keyring"
        self._fake = _FakeKeyring()
        self._mod = types.ModuleType("keyring")
        self._mod.get_password = self._fake.get_password
        self._mod.set_password = self._fake.set_password
        self._saved_mod = sys.modules.get("keyring")
        sys.modules["keyring"] = self._mod

    def tearDown(self):
        if self._saved_mod is None:
            sys.modules.pop("keyring", None)
        else:
            sys.modules["keyring"] = self._saved_mod
        super().tearDown()

    def test_key_round_trips_through_keyring_no_file(self):
        with tempfile.TemporaryDirectory() as base:
            token = crypto.encrypt_value("hunter2", base_dir=base)
            self.assertEqual(crypto.decrypt_value(token, base_dir=base), "hunter2")
            self.assertFalse((Path(base) / ".key").exists(), "no file under keyring")
            self.assertTrue(self._fake.store, "key stored in the keyring")

    def test_missing_keyring_falls_back_to_file(self):
        crypto._warned_no_keyring = False
        sys.modules.pop("keyring", None)
        with tempfile.TemporaryDirectory() as base, mock.patch.dict(
            sys.modules, {"keyring": None}
        ):
            # keyring=None makes `import keyring` raise ImportError.
            token = crypto.encrypt_value("secret", base_dir=base)
            self.assertEqual(crypto.decrypt_value(token, base_dir=base), "secret")
            self.assertTrue((Path(base) / ".key").exists(), "fell back to the file")


class DotenvTrustBoundaryTest(_EnvSandbox):
    def _write_env(self, d: Path) -> None:
        (d / ".env").write_text("DACLI_P09_PROBE=loaded\n", encoding="utf-8")

    def setUp(self):
        super().setUp()
        os.environ.pop("DACLI_P09_PROBE", None)

    def tearDown(self):
        os.environ.pop("DACLI_P09_PROBE", None)
        super().tearDown()

    def test_disabled_by_env(self):
        from dacli.config import settings

        with tempfile.TemporaryDirectory() as d:
            (Path(d) / ".git").mkdir()
            self._write_env(Path(d))
            prev = os.getcwd()
            os.chdir(d)
            os.environ["DACLI_USE_DOTENV"] = "0"
            try:
                settings._load_env_files()
            finally:
                os.chdir(prev)
            self.assertIsNone(os.environ.get("DACLI_P09_PROBE"))

    def test_loads_from_project_root_not_raw_cwd(self):
        from dacli.config import settings

        # An attacker .env sits in a non-project cwd; without a project marker it
        # is NOT loaded (per-user config dir is empty here).
        with tempfile.TemporaryDirectory() as cwd:
            self._write_env(Path(cwd))
            prev = os.getcwd()
            os.chdir(cwd)
            try:
                settings._load_env_files()
            finally:
                os.chdir(prev)
            self.assertIsNone(
                os.environ.get("DACLI_P09_PROBE"), "raw cwd .env must be ignored"
            )

    def test_loads_project_env_when_marked(self):
        from dacli.config import settings

        with tempfile.TemporaryDirectory() as d:
            (Path(d) / ".git").mkdir()
            self._write_env(Path(d))
            prev = os.getcwd()
            os.chdir(d)
            try:
                settings._load_env_files()
            finally:
                os.chdir(prev)
            self.assertEqual(os.environ.get("DACLI_P09_PROBE"), "loaded")


if __name__ == "__main__":
    unittest.main()
