"""Per-extension secret store for the new path (M07).

One JSON file — ``secrets.json`` under :func:`paths.resource_dir("secrets")`
(``~/.dacli/secrets.json`` or a project ``.dacli/secrets.json``) — keyed by
extension, then field. Secret fields are Fernet tokens; non-secret fields sit in
the clear. :meth:`SecretStore.config` is the ``config_provider`` the ExtensionHost
hands to ``api.config()``: it decrypts at call time and surfaces a rotated key
once via :func:`crypto.surface_decryption_failures`, the same path the legacy
store uses.

The base dir is resolved through :mod:`core.paths`, which carries the legacy
``.key`` detection, so a token written by the old store stays readable.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dacli.core import paths
from dacli.core.atomicio import write_json_atomic
from dacli.core.crypto import (
    CredentialDecryptionError,
    decrypt_value,
    encrypt_value,
    is_encrypted,
    surface_decryption_failures,
)

_STORE_FILE = "secrets.json"


class SecretStore:
    """Load/update/persist ``<secrets base>/secrets.json``."""

    def __init__(self, base_dir: str | Path | None = None):
        self.base_dir = Path(base_dir) if base_dir else paths.resource_dir("secrets")
        self.path = self.base_dir / _STORE_FILE
        self._data: dict[str, dict[str, Any]] = self._load()

    def _load(self) -> dict[str, dict[str, Any]]:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            # Missing or corrupt -> start empty; never wipe what's there on save
            # until the user re-enters, but don't crash a session over it.
            return {}
        return data if isinstance(data, dict) else {}

    def set(self, extension: str, field: str, value: str, *, secret: bool) -> None:
        """Store one field. A secret value is Fernet-encrypted before it lands;
        a non-secret value is kept as-is. An already-encrypted value passes
        through unchanged (idempotent re-save)."""
        if secret and value and not is_encrypted(value):
            value = encrypt_value(value, base_dir=str(self.base_dir))
        self._data.setdefault(extension, {})[field] = value

    def save(self) -> None:
        write_json_atomic(self.path, self._data, indent=2)

    def config(self, extension: str) -> dict[str, Any]:
        """This extension's config, secrets decrypted at call time.

        Plaintext (non-secret) fields pass through; a field whose key was
        rotated is omitted and reported once, rather than handing ciphertext to
        the caller as if it were the credential.
        """
        raw = self._data.get(extension)
        if not isinstance(raw, dict):
            return {}
        out: dict[str, Any] = {}
        undecryptable: list[str] = []
        for field, val in raw.items():
            if not isinstance(val, str):
                out[field] = val
                continue
            try:
                out[field] = decrypt_value(
                    val, base_dir=str(self.base_dir), name=f"{extension}.{field}"
                )
            except CredentialDecryptionError:
                undecryptable.append(f"{extension}.{field}")
        if undecryptable:
            surface_decryption_failures(undecryptable)
        return out
