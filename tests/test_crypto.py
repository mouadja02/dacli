"""Offline tests for credential encryption (core/crypto.py).

Focus: a wrong/rotated encryption key must fail loudly instead of silently
returning the ciphertext as if it were the plaintext value (P04).
"""

import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from cryptography.fernet import Fernet

from dacli.core import crypto
from dacli.core.crypto import (
    CredentialDecryptionError,
    decrypt_value,
    encrypt_value,
    surface_decryption_failures,
)


def _write_key(base_dir: str) -> None:
    Path(base_dir, ".key").write_bytes(Fernet.generate_key())


class DecryptValueTest(unittest.TestCase):
    def setUp(self):
        # The env key takes priority over the .key file; clear it so each test
        # controls the key purely through its temp base dir.
        self._saved_env = os.environ.pop("DACLI_ENCRYPTION_KEY", None)

    def tearDown(self):
        if self._saved_env is not None:
            os.environ["DACLI_ENCRYPTION_KEY"] = self._saved_env

    def test_round_trip_with_correct_key(self):
        with tempfile.TemporaryDirectory() as base:
            _write_key(base)
            token = encrypt_value("hunter2", base_dir=base)
            self.assertNotEqual(token, "hunter2")
            self.assertEqual(decrypt_value(token, base_dir=base), "hunter2")

    def test_plaintext_passes_through_unchanged(self):
        with tempfile.TemporaryDirectory() as base:
            _write_key(base)
            # A never-encrypted value is not a Fernet token: returned as-is.
            self.assertEqual(decrypt_value("plain-password", base_dir=base), "plain-password")

    def test_wrong_key_raises_naming_the_secret(self):
        with tempfile.TemporaryDirectory() as base_a, tempfile.TemporaryDirectory() as base_b:
            _write_key(base_a)
            _write_key(base_b)  # a different key
            token = encrypt_value("topsecret", base_dir=base_a)

            with self.assertRaises(CredentialDecryptionError) as ctx:
                decrypt_value(token, base_dir=base_b, name="snowflake.password")
            self.assertIn("snowflake.password", str(ctx.exception))

    def test_empty_value_passes_through(self):
        self.assertEqual(decrypt_value("", base_dir=None), "")


class ResolveBaseDirTest(unittest.TestCase):
    """resolve_base_dir now delegates to paths.resource_dir('secrets'); the
    legacy cwd .dacli/.key must still win so existing stores stay readable."""

    def test_state_path_parent_wins(self):
        self.assertEqual(
            crypto.resolve_base_dir("/srv/app/.dacli/state/"),
            Path("/srv/app/.dacli"),
        )

    def test_legacy_cwd_key_detected(self):
        saved = os.environ.pop("DACLI_STATE_PATH", None)
        with tempfile.TemporaryDirectory() as d:
            cwd = os.getcwd()
            os.chdir(d)
            try:
                Path(".dacli").mkdir()
                Path(".dacli", ".key").touch()
                self.assertEqual(crypto.resolve_base_dir(), Path(".dacli"))
            finally:
                os.chdir(cwd)
                if saved is not None:
                    os.environ["DACLI_STATE_PATH"] = saved


class DerivedKeyCacheTest(unittest.TestCase):
    """A password key source must run PBKDF2 once, not once per decrypt."""

    def setUp(self):
        # A passphrase (not a 44-char Fernet key) forces the PBKDF2 path.
        self._saved_env = os.environ.get("DACLI_ENCRYPTION_KEY")
        os.environ["DACLI_ENCRYPTION_KEY"] = "a-passphrase-not-a-fernet-key"
        crypto.reset_key_cache()

    def tearDown(self):
        if self._saved_env is None:
            os.environ.pop("DACLI_ENCRYPTION_KEY", None)
        else:
            os.environ["DACLI_ENCRYPTION_KEY"] = self._saved_env
        crypto.reset_key_cache()

    def _count_derivations(self):
        calls = {"n": 0}
        real = crypto._derive_key

        def counting(password):
            calls["n"] += 1
            return real(password)

        return calls, counting

    def test_password_key_derived_once_across_many_decrypts(self):
        calls, counting = self._count_derivations()
        with tempfile.TemporaryDirectory() as base, mock.patch.object(
            crypto, "_derive_key", side_effect=counting
        ):
            token = encrypt_value("secret", base_dir=base)
            for _ in range(5):
                self.assertEqual(decrypt_value(token, base_dir=base), "secret")
        self.assertEqual(calls["n"], 1)

    def test_reset_key_cache_forces_rederivation(self):
        calls, counting = self._count_derivations()
        with tempfile.TemporaryDirectory() as base, mock.patch.object(
            crypto, "_derive_key", side_effect=counting
        ):
            encrypt_value("secret", base_dir=base)
            crypto.reset_key_cache()
            encrypt_value("secret", base_dir=base)
        self.assertEqual(calls["n"], 2)


class SurfaceFailuresTest(unittest.TestCase):
    def setUp(self):
        crypto._warned_secrets.clear()

    def test_surfaces_one_clear_message_for_all_names(self):
        stream = io.StringIO()
        msg = surface_decryption_failures(["snowflake.password", "github.token"], stream=stream)
        self.assertIsNotNone(msg)
        out = stream.getvalue()
        self.assertIn("snowflake.password", out)
        self.assertIn("github.token", out)
        self.assertIn(".key", out)  # tells the user how to recover

    def test_deduplicates_repeated_names(self):
        stream = io.StringIO()
        first = surface_decryption_failures(["snowflake.password"], stream=stream)
        self.assertIsNotNone(first)
        # A second load of the same broken secret must not re-spam the user.
        second = surface_decryption_failures(["snowflake.password"], stream=stream)
        self.assertIsNone(second)

    def test_no_message_for_empty_list(self):
        self.assertIsNone(surface_decryption_failures([]))


if __name__ == "__main__":
    unittest.main()
