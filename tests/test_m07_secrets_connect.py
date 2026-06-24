"""M07 — secrets + /connect on the new path.

A secret entered via /connect is a Fernet token at rest; api.config() decrypts it
at call time; a rotated key surfaces the named credential once; a non-secret field
is stored in the clear; and an extension reads its key only through api.config().
"""

import asyncio
import json
from textwrap import dedent

from dacli.core import crypto
from dacli.core.connect_extension import connect_extension
from dacli.core.extensions import ExtensionHost, ToolContext
from dacli.connectors.registry import ConfigField
from dacli.core.secrets import SecretStore


def _fields(*specs):
    return [ConfigField(**s) for s in specs]


class _Registry:
    """Minimal stand-in for the surface connect_extension reads."""

    def __init__(self, fields):
        self._fields = fields

    def config_fields(self, extension):
        return list(self._fields)


def _scripted(answers):
    return lambda field: answers.get(field.name)


def _run(coro):
    return asyncio.run(coro)


# --- SecretStore -----------------------------------------------------------

def test_secret_at_rest_is_fernet_token(tmp_path):
    store = SecretStore(tmp_path)
    store.set("aws", "access_key", "hunter2", secret=True)
    store.save()

    raw = json.loads((tmp_path / "secrets.json").read_text())["aws"]["access_key"]
    assert crypto.is_encrypted(raw)
    assert raw != "hunter2"


def test_non_secret_stored_in_clear(tmp_path):
    store = SecretStore(tmp_path)
    store.set("aws", "bucket", "my-bucket", secret=False)
    store.save()

    raw = json.loads((tmp_path / "secrets.json").read_text())["aws"]["bucket"]
    assert raw == "my-bucket"


def test_config_decrypts_secret_at_call_time(tmp_path):
    store = SecretStore(tmp_path)
    store.set("aws", "access_key", "hunter2", secret=True)
    store.set("aws", "bucket", "my-bucket", secret=False)
    store.save()

    assert SecretStore(tmp_path).config("aws") == {
        "access_key": "hunter2",
        "bucket": "my-bucket",
    }


def test_config_unknown_extension_is_empty(tmp_path):
    assert SecretStore(tmp_path).config("nope") == {}


def test_rotated_key_surfaces_named_credential_once(tmp_path, capsys, monkeypatch):
    # Force the file backend so rotating == deleting .key, regardless of any
    # DACLI_ENCRYPTION_KEY/keyring in the ambient environment.
    monkeypatch.delenv("DACLI_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("DACLI_KEY_BACKEND", raising=False)
    crypto._warned_secrets.discard("vault.token")
    store = SecretStore(tmp_path)
    store.set("vault", "token", "s3cr3t", secret=True)
    store.save()

    (tmp_path / ".key").unlink()  # rotate: the token no longer decrypts
    crypto.reset_key_cache()

    rotated = SecretStore(tmp_path)
    assert "token" not in rotated.config("vault")  # omitted, not handed on as ciphertext
    assert "vault.token" in capsys.readouterr().err

    rotated.config("vault")
    assert "vault.token" not in capsys.readouterr().err  # surfaced once, not per read


# --- /connect on the new path ---------------------------------------------

def test_connect_stores_secret_encrypted_and_nonsecret_clear(tmp_path):
    registry = _Registry(_fields(
        {"name": "token", "is_secret": True, "required": True},
        {"name": "region", "is_secret": False},
    ))
    store = SecretStore(tmp_path)

    ok, _ = connect_extension(
        registry, store, "vault",
        prompt=_scripted({"token": "s3cr3t", "region": "eu-west-1"}),
    )
    assert ok

    blob = json.loads((tmp_path / "secrets.json").read_text())["vault"]
    assert crypto.is_encrypted(blob["token"])
    assert blob["region"] == "eu-west-1"


def test_connect_skips_blank_answers(tmp_path):
    registry = _Registry(_fields({"name": "token", "is_secret": True}))
    store = SecretStore(tmp_path)

    ok, _ = connect_extension(registry, store, "vault", prompt=_scripted({}))
    assert not ok
    assert not (tmp_path / "secrets.json").exists()


def test_connect_reports_nothing_to_configure(tmp_path):
    ok, msg = connect_extension(
        _Registry([]), SecretStore(tmp_path), "bare", prompt=_scripted({})
    )
    assert not ok
    assert "bare" in msg


# --- end to end: an extension reads its key only via api.config() ----------

EXT = """
    def register(api):
        api.config_field("token", secret=True)

        @api.tool(
            name="whoami",
            description="Echo the configured token",
            risk="safe",
            postconditions=["result_succeeded"],
        )
        async def whoami(args, ctx):
            return ctx.ok(api.config().get("token"))
"""


def test_extension_reads_secret_via_config(tmp_path):
    secrets_dir = tmp_path / "secrets"
    secrets_dir.mkdir()
    store = SecretStore(secrets_dir)
    store.set("vault", "token", "s3cr3t", secret=True)
    store.save()

    ext_dir = tmp_path / "extensions" / "vault"
    ext_dir.mkdir(parents=True)
    (ext_dir / "__init__.py").write_text(dedent(EXT), encoding="utf-8")

    host = ExtensionHost(tmp_path / "extensions", config_provider=store.config)
    host.load()

    tool = host.registry.resolve("whoami")
    result = _run(tool.handler({}, ToolContext("whoami")))
    assert result.data == "s3cr3t"
