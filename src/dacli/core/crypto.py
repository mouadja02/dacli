"""Fernet-based credential encryption for dacli.

Key resolution priority:
1. ``DACLI_ENCRYPTION_KEY`` environment variable (raw Fernet key or password)
2. ``.dacli/.key`` file (raw Fernet key or password)
3. Auto-generate a Fernet key and persist it to ``.dacli/.key``

When the key source looks like a password (not a valid 32-byte base64 Fernet
key), it is derived into one via PBKDF2 with a stable salt so the same password
always produces the same Fernet key.

Stored values are Fernet tokens (URL-safe base64). :func:`is_encrypted`
detects the ``gAAAAA`` prefix so plaintext migration is transparent.
"""

from __future__ import annotations

import base64
import binascii
import os
import sys
from pathlib import Path
from typing import TextIO
from collections.abc import Iterable

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from dacli.core.logging_setup import get_logger

log = get_logger(__name__)


class CredentialDecryptionError(Exception):
    """Raised when an encrypted value can't be decrypted (wrong/rotated key)."""


_KEY_FILE = ".key"
_STABLE_SALT = b"dacli-credential-encryption-v1"
#: Fernet token = 0x80 version byte + 8-byte timestamp + 16-byte IV + ciphertext
# + 32-byte HMAC, so the smallest possible token decodes to 1+8+16+0+32 bytes.
_FERNET_VERSION = 0x80
_FERNET_MIN_LEN = 57


def resolve_base_dir(state_path: str | None = None) -> Path:
    """The single source of truth for the credential base directory.

    The encryption key (``.key``) and the secrets store (``dacli.json``) both
    live here, so encrypt and decrypt always agree on the key location. Priority:
    an explicit ``state_path`` (e.g. the resolved ``settings.agent.state_path``) >
    the ``DACLI_STATE_PATH`` env var > the ``.dacli/state/`` default. The base dir
    is the *parent* of the state path (``.dacli`` for ``.dacli/state/``).

    All callers — :mod:`core.store`, :func:`config.settings._load_dacli_secrets`,
    and this module — resolve through here so the three never drift apart.

    The whole resolution — env var, legacy cwd ``.dacli/.key`` detection, and
    the project-vs-user default — lives in :mod:`core.paths` (the one resolver);
    this delegates to it via the ``secrets`` resource kind. Back-compat is
    preserved: an existing cwd ``.dacli/.key`` is detected and kept, so installs
    that predate the per-user move keep decrypting their store unchanged.
    """
    from dacli.core import paths

    return paths.resource_dir("secrets", state_path=state_path)


def _resolve_base_dir() -> Path:
    return resolve_base_dir()


def _try_load_fernet_key(raw: bytes) -> bytes | None:
    if len(raw) == 44 and raw.endswith(b"="):
        try:
            decoded = base64.urlsafe_b64decode(raw)
            if len(decoded) == 32:
                return raw
        except Exception:
            # Not a raw Fernet key -> caller falls back to PBKDF2 derivation.
            log.debug("value is not a raw Fernet key; will derive", exc_info=True)
    return None


def _derive_key(password: bytes) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        iterations=480_000,
        salt=_STABLE_SALT,
        length=32,
    )
    return base64.urlsafe_b64encode(kdf.derive(password))


#: Derived keys keyed by (base_dir, sha256(password)). The derivation is
#: deterministic for a given source, so caching it is correctness-neutral; what
#: it saves is 480k PBKDF2 iterations on every encrypt/decrypt for a passphrase
#: key source (load_config decrypts once per stored secret, repeatedly).
_key_cache: dict[tuple[str, str], bytes] = {}


def reset_key_cache() -> None:
    """Drop cached derived keys. Call on key rotation (the source changed)."""
    _key_cache.clear()


def _cached_derive(base_dir: str, password: bytes) -> bytes:
    import hashlib

    key = (base_dir, hashlib.sha256(password).hexdigest())
    derived = _key_cache.get(key)
    if derived is None:
        derived = _derive_key(password)
        _key_cache[key] = derived
    return derived


#: Optional OS-keyring backend. Selected by ``DACLI_KEY_BACKEND=keyring`` (env,
#: not config, so the choice never depends on decrypting the very store the key
#: protects). Stores one Fernet key per OS user under this service/account.
_KEYRING_BACKEND_ENV = "DACLI_KEY_BACKEND"
_KEYRING_SERVICE = "dacli"
_KEYRING_ACCOUNT = "encryption-key"
_warned_no_keyring = False
_warned_broad_acl: set = set()


def _keyring_selected() -> bool:
    return os.environ.get(_KEYRING_BACKEND_ENV, "file").strip().lower() == "keyring"


def _keyring_get_or_create(key_file: Path) -> bytes | None:
    """Fetch the Fernet key from the OS keyring, generating+storing one if absent.

    Returns ``None`` to fall back to the file backend when keyring isn't
    installed/reachable, or when a ``.key`` file already exists (don't silently
    mint a second key that can't decrypt the old store — migrate by hand).
    """
    global _warned_no_keyring
    try:
        import keyring
    except ImportError:
        if not _warned_no_keyring:
            _warned_no_keyring = True
            log.warning(
                "DACLI_KEY_BACKEND=keyring but the 'keyring' package isn't "
                "installed; using the .key file. Install with: pip install dacli[keyring]"
            )
        return None
    try:
        existing = keyring.get_password(_KEYRING_SERVICE, _KEYRING_ACCOUNT)
    except Exception:
        log.debug("keyring read failed; using file backend", exc_info=True)
        return None
    if existing:
        return existing.encode("utf-8")
    if key_file.exists():
        return None
    key = Fernet.generate_key()
    try:
        keyring.set_password(_KEYRING_SERVICE, _KEYRING_ACCOUNT, key.decode("utf-8"))
    except Exception:
        log.debug("keyring write failed; using file backend", exc_info=True)
        return None
    reset_key_cache()
    return key


def _secure_key_file(key_file: Path) -> None:
    """Restrict the new key file to the current user.

    POSIX: ``chmod 600``. Windows: ``os.chmod`` only flips the read-only bit, so
    strip inherited ACEs and grant the current user explicitly via ``icacls``.
    If that can't run, warn once that the key sits in a broadly-readable dir.
    """
    if os.name != "nt":
        try:
            os.chmod(str(key_file), 0o600)
        except OSError:
            log.debug("could not chmod 600 the key file %s", key_file, exc_info=True)
        return

    import getpass
    import subprocess

    try:
        user = getpass.getuser()
        subprocess.run(
            ["icacls", str(key_file), "/inheritance:r", "/grant:r", f"{user}:F"],
            check=True,
            capture_output=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        _warn_broad_key_acl(key_file)


def _warn_broad_key_acl(key_file: Path) -> None:
    parent = str(key_file.parent)
    if parent in _warned_broad_acl:
        return
    _warned_broad_acl.add(parent)
    msg = (
        f"Couldn't tighten the encryption key's ACL ({key_file}); it inherits the "
        f"directory's permissions. Anyone who can read {parent} can read the key. "
        "Move it to a per-user dir (set DACLI_HOME) or restrict the folder."
    )
    log.warning("broad ACL on key file %s", key_file)
    print(msg, file=sys.stderr)


def get_encryption_key(base_dir: str | None = None) -> bytes:
    base = Path(base_dir) if base_dir else _resolve_base_dir()
    key_file = base / _KEY_FILE

    env_key = os.environ.get("DACLI_ENCRYPTION_KEY")
    if env_key:
        raw = env_key.encode("utf-8")
        return _try_load_fernet_key(raw) or _cached_derive(str(base), raw)

    if _keyring_selected():
        key = _keyring_get_or_create(key_file)
        if key is not None:
            return key

    if key_file.exists():
        raw = key_file.read_bytes().strip()
        if raw:
            return _try_load_fernet_key(raw) or _cached_derive(str(base), raw)

    key = Fernet.generate_key()
    base.mkdir(parents=True, exist_ok=True)
    key_file.write_bytes(key)
    # A freshly generated key supersedes any derivation cached for this dir.
    reset_key_cache()
    _secure_key_file(key_file)
    return key


def _fernet(base_dir: str | None = None) -> Fernet:
    return Fernet(get_encryption_key(base_dir))


def encrypt_value(plaintext: str, base_dir: str | None = None) -> str:
    if not plaintext:
        return plaintext
    return _fernet(base_dir).encrypt(plaintext.encode("utf-8")).decode("utf-8")


def decrypt_value(
    token: str, base_dir: str | None = None, name: str | None = None
) -> str:
    """Decrypt a stored credential.

    A value that is *not* a Fernet token is genuine plaintext (pre-encryption
    store) and is returned unchanged. A value that *is* a Fernet token but
    fails to decrypt means the key was lost/rotated (``.key`` deleted or
    ``DACLI_ENCRYPTION_KEY`` changed): rather than silently returning the
    ciphertext — which a connector would then use as the password and fail
    opaquely platform-side — raise :class:`CredentialDecryptionError` naming the
    secret so the real cause is surfaced at startup.
    """
    if not token:
        return token
    if not is_encrypted(token):
        return token
    try:
        return _fernet(base_dir).decrypt(token.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        label = name or "a stored credential"
        raise CredentialDecryptionError(
            f"wrong/rotated encryption key for {label}"
        ) from exc


def is_encrypted(value: str) -> bool:
    """True if ``value`` is structurally a Fernet token.

    Decodes the URL-safe base64 and checks the version byte and minimum length,
    rather than matching the ``gAAAAA`` prefix. The prefix check produced false
    positives for any plaintext starting with those characters, which would make
    a real secret be treated as already-encrypted and stored in the clear.
    """
    if not isinstance(value, str) or not value:
        return False
    try:
        decoded = base64.urlsafe_b64decode(value.encode("utf-8"))
    except (binascii.Error, ValueError):
        return False
    return len(decoded) >= _FERNET_MIN_LEN and decoded[0] == _FERNET_VERSION


#: Names already reported by :func:`surface_decryption_failures`, so repeated
#: secret loads (one per connector) don't re-spam the same warning.
_warned_secrets: set = set()


def surface_decryption_failures(
    names: Iterable[str], *, stream: TextIO | None = None
) -> str | None:
    """Report, exactly once, secrets that couldn't be decrypted.

    Aggregates the affected credential names into a single clear message so the
    user sees the real cause once at startup — *"the encryption key changed"* —
    instead of N connectors each failing later with an opaque platform-side auth
    error. Returns the emitted message, or ``None`` if there is nothing new to
    report (empty input or every name already warned about this process).
    """
    new: list[str] = [n for n in names if n and n not in _warned_secrets]
    if not new:
        return None
    _warned_secrets.update(new)
    msg = (
        "Encryption key changed; stored credentials for "
        + ", ".join(sorted(new))
        + " can't be read. Re-enter them or restore `.dacli/.key`."
    )
    # Intentional user-facing surface (a real reliability issue the operator must
    # see at once) — kept as a print, but also recorded so it lands in dacli.log.
    log.warning("undecryptable credentials: %s", ", ".join(sorted(new)))
    print(msg, file=stream or sys.stderr)
    return msg
