"""Regression tests for the per-connector manifest config pattern (09/A-4).

A connector declares its config in ``manifest.yaml`` (``config_fields``), users
write values under ``connector_config.<id>`` in config.yaml, and the connector
reads them through the fail-soft :class:`ConnectorConfig` accessor instead of a
typed ``Settings.<id>`` section. These tests pin that contract.
"""

from dacli.config.settings import ConnectorConfig, Settings
import pytest


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
    assert cfg.bucket == "my-bucket"
    with pytest.raises(AttributeError):
        _ = cfg.region


def test_connector_config_accessor_missing_connector():
    s = Settings()
    cfg = ConnectorConfig(s, "s3")
    assert cfg.get("bucket", "") == ""  # no crash, safe default


# ---------------------------------------------------------------------------
# Settings carries no typed connector section
# ---------------------------------------------------------------------------
def test_no_typed_connector_section_on_settings():
    s = Settings()
    assert not hasattr(s, "s3")
    assert "s3" not in Settings.model_fields
    # The generic store is present instead.
    assert "connector_config" in Settings.model_fields


# ---------------------------------------------------------------------------
# Registry sources config fields from the manifest only (M12)
# ---------------------------------------------------------------------------
def test_registry_get_config_fields_uses_manifest():
    """``get_config_fields`` reads the manifest's config_fields — there is no
    typed Settings section path anymore."""
    from dacli.connectors.registry import ConnectorRegistry

    # Default connectors_dir is the real package directory (cwd-independent); the
    # github seed's old manifest still ships its config_fields.
    registry = ConnectorRegistry(Settings())
    fields = registry.get_config_fields("github")
    names = {f.name for f in fields}
    assert {"token", "repository_url", "branch", "timeout"} <= names
    token = next(f for f in fields if f.name == "token")
    assert token.required and token.is_secret
