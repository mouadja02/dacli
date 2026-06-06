"""P06: a failed DacliStore.save() now leaves a breadcrumb (and still never raises)."""

import logging

from core.store import DacliStore
from core.logging_setup import setup_logging


def test_save_failure_is_logged_and_does_not_raise(tmp_path, caplog, monkeypatch):
    setup_logging(debug=True, base_dir=str(tmp_path), force=True)
    store = DacliStore(base_dir=str(tmp_path))

    def boom(*args, **kwargs):
        raise OSError("disk gone")

    monkeypatch.setattr("core.store.write_json_atomic", boom)

    with caplog.at_level(logging.DEBUG, logger="dacli"):
        store.save()  # fail-soft: must not raise

    assert any(r.exc_info for r in caplog.records), "expected a logged breadcrumb"
