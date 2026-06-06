"""Tests for the central logging setup (P06)."""

import logging
from logging.handlers import RotatingFileHandler

import pytest

from core.logging_setup import get_logger, is_debug, setup_logging


def test_setup_creates_rotating_file_handler(tmp_path):
    logger = setup_logging(debug=False, base_dir=str(tmp_path), force=True)
    handlers = [h for h in logger.handlers if isinstance(h, RotatingFileHandler)]
    assert handlers, "expected a RotatingFileHandler on the dacli logger"
    assert logger.level == logging.WARNING


def test_debug_flag_raises_level(tmp_path):
    logger = setup_logging(debug=True, base_dir=str(tmp_path), force=True)
    assert logger.level == logging.DEBUG
    assert is_debug() is True


def test_default_is_warning(tmp_path):
    logger = setup_logging(debug=False, base_dir=str(tmp_path), force=True)
    assert logger.level == logging.WARNING
    assert is_debug() is False


def test_logged_record_lands_in_file(tmp_path):
    setup_logging(debug=True, base_dir=str(tmp_path), force=True)
    log = get_logger("core.test_thing")
    log.debug("hello breadcrumb 12345")
    for h in logging.getLogger("dacli").handlers:
        h.flush()
    content = (tmp_path / "dacli.log").read_text(encoding="utf-8")
    assert "hello breadcrumb 12345" in content


def test_env_var_enables_debug(tmp_path, monkeypatch):
    monkeypatch.setenv("DACLI_DEBUG", "1")
    logger = setup_logging(base_dir=str(tmp_path), force=True)
    assert logger.level == logging.DEBUG
    assert is_debug() is True


def test_warning_record_does_not_land_at_default_level(tmp_path):
    # At WARNING level a debug breadcrumb is filtered out (kept off the disk).
    setup_logging(debug=False, base_dir=str(tmp_path), force=True)
    log = get_logger("core.quiet")
    log.debug("should not appear quietly")
    log.warning("should appear loudly")
    for h in logging.getLogger("dacli").handlers:
        h.flush()
    content = (tmp_path / "dacli.log").read_text(encoding="utf-8")
    assert "should not appear quietly" not in content
    assert "should appear loudly" in content
