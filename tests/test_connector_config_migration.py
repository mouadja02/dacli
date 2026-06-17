"""Regression tests for the per-connector manifest config pattern (09/A-4).

S3 is the end-to-end proven migration: it declares its config in
``manifest.yaml`` (``config_fields``), users write values under
``connector_config.s3`` in config.yaml, and the connector reads them through the
fail-soft :class:`ConnectorConfig` accessor instead of a typed ``Settings.s3``
section. These tests pin that contract so the next connector can follow it.
"""
import asyncio

import pytest

from dacli.config.settings import ConnectorConfig, Settings


# ---------------------------------------------------------------------------
# ConnectorConfig accessor
# ---------------------------------------------------------------------------
def test_connector_config_accessor_dict_access():
    s = Settings(connector_config={"s3": {"bucket": "my-bucket", "timeout": 60}})
    cfg = ConnectorConfig(s, "s3")
    assert cfg.get("bucket") == "my-bucket"
    assert cfg.get("timeout") == 60
    assert cfg.get("region", "") == ""


def test_connector_config_accessor_attribute_access():
    s = Settings(connector_config={"s3": {"bucket": "my-bucket"}})
    cfg = ConnectorConfig(s, "s3")
    # Attribute-style access works for present keys …
    assert cfg.bucket == "my-bucket"
    # … and raises AttributeError for missing ones (like a pydantic model).
    with pytest.raises(AttributeError):
        _ = cfg.region


def test_connector_config_accessor_missing_connector():
    s = Settings()
    cfg = ConnectorConfig(s, "s3")
    assert cfg.get("bucket", "") == ""  # no crash, safe default


# ---------------------------------------------------------------------------
# Settings no longer carries a typed s3 section
# ---------------------------------------------------------------------------
def test_s3_settings_removed_from_settings():
    s = Settings()
    assert not hasattr(s, "s3"), "S3Settings was not removed from Settings"
    assert "s3" not in Settings.model_fields
    # The generic store is present instead.
    assert "connector_config" in Settings.model_fields


# ---------------------------------------------------------------------------
# Registry sources s3 config fields from the manifest now
# ---------------------------------------------------------------------------
def test_registry_get_config_fields_uses_manifest():
    """``get_config_fields('s3')`` falls through to the manifest's config_fields
    (the typed Settings.s3 section is gone)."""
    from dacli.connectors.registry import ConnectorRegistry

    # Default connectors_dir is the real package directory (cwd-independent).
    registry = ConnectorRegistry(Settings())
    fields = registry.get_config_fields("s3")
    names = {f.name for f in fields}
    assert {"bucket", "prefix", "region", "profile", "aws_binary", "timeout"} <= names
    required = [f for f in fields if f.required]
    assert any(f.name == "bucket" for f in required)


# ---------------------------------------------------------------------------
# S3Connector reads its config from ConnectorConfig
# ---------------------------------------------------------------------------
def test_s3_connector_uses_connector_config():
    from dacli.connectors.s3.connector import S3Connector

    s = Settings(connector_config={
        "s3": {"bucket": "test-bucket", "region": "eu-west-1", "aws_binary": "aws"}
    })
    conn = S3Connector(s)
    cfg = conn._cfg()
    assert cfg.get("bucket") == "test-bucket"
    assert cfg.get("region") == "eu-west-1"
    assert conn.binary == "aws"


def test_s3_connector_global_flags_from_connector_config():
    from dacli.connectors.s3.connector import S3Connector

    s = Settings(connector_config={
        "s3": {"bucket": "b", "region": "us-east-1", "profile": "prod"}
    })
    conn = S3Connector(s)
    flags = conn._global_flags()
    assert flags == ["--profile", "prod", "--region", "us-east-1"]


# ---------------------------------------------------------------------------
# Setup wizard validates a manifest-config connector against connector_config
# ---------------------------------------------------------------------------
def test_setup_wizard_validates_manifest_config_connector():
    """With no ``connector_config.s3``, the wizard reports the specific missing
    field — not the old 'configuration missing in config.yaml' (which assumed a
    typed section)."""
    from dacli.connectors.registry import ConnectorRegistry
    from dacli.core.setup_wizard import SetupWizard

    registry = ConnectorRegistry(Settings())
    wizard = SetupWizard(Settings(), registry)
    ok, msg = asyncio.run(wizard._validate_connector("s3"))
    assert ok is False
    assert "bucket" in msg
    assert "missing in config.yaml" not in msg
