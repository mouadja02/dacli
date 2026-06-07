"""Tests for the central logging setup (P06). Pure unittest (no pytest)."""

import logging
import os
import tempfile
import unittest
from logging.handlers import RotatingFileHandler

from dacli.core.logging_setup import get_logger, is_debug, setup_logging


def _flush():
    for h in logging.getLogger("dacli").handlers:
        h.flush()


def _read_log(base_dir):
    with open(os.path.join(base_dir, "dacli.log"), encoding="utf-8") as f:
        return f.read()


class LoggingSetupTest(unittest.TestCase):
    def _dir(self):
        return tempfile.mkdtemp(prefix="dacli_log_")

    def test_setup_creates_rotating_file_handler(self):
        d = self._dir()
        logger = setup_logging(debug=False, base_dir=d, force=True)
        handlers = [h for h in logger.handlers if isinstance(h, RotatingFileHandler)]
        self.assertTrue(handlers, "expected a RotatingFileHandler on the dacli logger")
        self.assertEqual(logger.level, logging.WARNING)

    def test_debug_flag_raises_level(self):
        d = self._dir()
        logger = setup_logging(debug=True, base_dir=d, force=True)
        self.assertEqual(logger.level, logging.DEBUG)
        self.assertTrue(is_debug())

    def test_default_is_warning(self):
        d = self._dir()
        logger = setup_logging(debug=False, base_dir=d, force=True)
        self.assertEqual(logger.level, logging.WARNING)
        self.assertFalse(is_debug())

    def test_logged_record_lands_in_file(self):
        d = self._dir()
        setup_logging(debug=True, base_dir=d, force=True)
        get_logger("core.test_thing").debug("hello breadcrumb 12345")
        _flush()
        self.assertIn("hello breadcrumb 12345", _read_log(d))

    def test_env_var_enables_debug(self):
        d = self._dir()
        prev = os.environ.get("DACLI_DEBUG")
        os.environ["DACLI_DEBUG"] = "1"
        try:
            logger = setup_logging(base_dir=d, force=True)
            self.assertEqual(logger.level, logging.DEBUG)
            self.assertTrue(is_debug())
        finally:
            if prev is None:
                os.environ.pop("DACLI_DEBUG", None)
            else:
                os.environ["DACLI_DEBUG"] = prev

    def test_warning_record_filtered_at_default_level(self):
        # At WARNING level a debug breadcrumb is filtered out (kept off the disk).
        d = self._dir()
        setup_logging(debug=False, base_dir=d, force=True)
        log = get_logger("core.quiet")
        log.debug("should not appear quietly")
        log.warning("should appear loudly")
        _flush()
        content = _read_log(d)
        self.assertNotIn("should not appear quietly", content)
        self.assertIn("should appear loudly", content)


if __name__ == "__main__":
    unittest.main()
