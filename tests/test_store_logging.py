"""P06: a failed DacliStore.save() leaves a breadcrumb (and still never raises).

Pure unittest (no pytest): assert against the written dacli.log file.
"""

import logging
import os
import tempfile
import unittest

import dacli.core.store as store_mod
from dacli.core.store import DacliStore
from dacli.core.logging_setup import setup_logging


class StoreLoggingTest(unittest.TestCase):
    def test_save_failure_is_logged_and_does_not_raise(self):
        d = tempfile.mkdtemp(prefix="dacli_storelog_")
        setup_logging(debug=True, base_dir=d, force=True)
        store = DacliStore(base_dir=d)

        original = store_mod.write_json_atomic

        def boom(*args, **kwargs):
            raise OSError("disk gone")

        store_mod.write_json_atomic = boom
        try:
            store.save()  # fail-soft: must not raise
        finally:
            store_mod.write_json_atomic = original

        for h in logging.getLogger("dacli").handlers:
            h.flush()
        with open(os.path.join(d, "dacli.log"), encoding="utf-8") as f:
            content = f.read()
        self.assertIn("store save failed", content)
        # The traceback (exc_info) is recorded, not just the one-line message.
        self.assertIn("Traceback", content)
        self.assertIn("disk gone", content)


if __name__ == "__main__":
    unittest.main()
