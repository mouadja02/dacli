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
def test_registry_get_config_fields_returns_empty_for_unknown():
    """``get_config_fields`` returns empty list for a connector with no manifest
    (all packaged connectors removed; extensions own their config now)."""
    from dacli.connectors.registry import ConnectorRegistry

    registry = ConnectorRegistry(Settings())
    fields = registry.get_config_fields("nonexistent")
    assert fields == []
