"""P07 — code-quality cleanup regressions.

Pins the cleanup batch: dead ``RetrySettings`` stays deleted, the formerly
silent exception swallows now leave a ``log.debug`` breadcrumb, persisted
session timestamps are tz-aware UTC (while the human-facing session-id format
is deliberately unchanged), and the status renderer lives on ``DacliUI``.
"""

import logging
import re
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from rich.console import Console

from dacli.config.settings import Settings, _load_dacli_secrets
from dacli.core.memory import AgentMemory, Message
from dacli.tui import DacliUI


def _memory(tmp_path) -> AgentMemory:
    return AgentMemory(
        state_path=str(tmp_path / "state"),
        history_path=str(tmp_path / "history"),
        memory_path=str(tmp_path / "memory"),
    )


def _is_aware(iso_string: str) -> bool:
    return datetime.fromisoformat(iso_string).tzinfo is not None


# ---------------------------------------------------------------------------
# Q-1 — dead RetrySettings stays deleted
# ---------------------------------------------------------------------------
def test_settings_retry_field_removed():
    assert "retry" not in Settings.model_fields


# ---------------------------------------------------------------------------
# Q-3 — silent swallows leave a debug breadcrumb (control flow unchanged)
# ---------------------------------------------------------------------------
def test_secrets_overlay_failure_logs_debug(tmp_path, caplog):
    (tmp_path / "dacli.json").write_text("{not valid json", encoding="utf-8")
    with caplog.at_level(logging.DEBUG, logger="dacli"):
        assert _load_dacli_secrets(str(tmp_path)) == {}
    assert any("secrets overlay" in r.getMessage() for r in caplog.records)


def test_workspace_cleanup_failure_logs_debug(tmp_path, monkeypatch, caplog):
    from dacli.sandbox import workspace as ws_mod

    ws = ws_mod.SessionWorkspace("p07", workspace_root=str(tmp_path / "sessions"))

    def _boom(*args, **kwargs):
        raise OSError("boom")

    monkeypatch.setattr(ws_mod.shutil, "rmtree", _boom)
    with caplog.at_level(logging.DEBUG, logger="dacli"):
        ws.cleanup()  # must swallow, not raise
    assert any("workspace cleanup failed" in r.getMessage() for r in caplog.records)


def test_terminal_journal_failure_logs_debug(tmp_path, caplog):
    from dacli.sandbox.terminal import TerminalSession

    term = TerminalSession.__new__(TerminalSession)
    term._journal_on = True
    term.workspace = SimpleNamespace(journal_dir=tmp_path)

    class BadResult:
        def to_dict(self):
            raise RuntimeError("boom")

    with caplog.at_level(logging.DEBUG, logger="dacli"):
        term._journal(BadResult())  # must swallow, not raise
    assert any("journal write failed" in r.getMessage() for r in caplog.records)


def test_guidelines_read_failure_logs_debug(monkeypatch, caplog):
    import dacli.memory.priors as priors_mod
    from dacli.prompts.system_prompt import load_system_prompt

    monkeypatch.setattr(priors_mod, "load_priors", lambda: "")

    real_exists = Path.exists
    real_read_text = Path.read_text

    def fake_exists(self):
        if self.name == "GUIDELINES.md":
            return True
        return real_exists(self)

    def fake_read_text(self, *args, **kwargs):
        if self.name == "GUIDELINES.md":
            raise OSError("boom")
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "exists", fake_exists)
    monkeypatch.setattr(Path, "read_text", fake_read_text)
    with caplog.at_level(logging.DEBUG, logger="dacli"):
        prompt = load_system_prompt()  # must swallow, not raise
    assert prompt
    assert any("GUIDELINES.md" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# Q-5 — persisted session timestamps are tz-aware UTC
# ---------------------------------------------------------------------------
def test_message_default_timestamp_is_aware():
    assert _is_aware(Message(role="user", content="hi").timestamp)


def test_persisted_session_timestamps_are_aware(tmp_path):
    mem = _memory(tmp_path)
    mem.add_message("user", "hello")
    assert _is_aware(mem.get_full_history()[0].timestamp)
    assert _is_aware(mem.state.created_at)
    assert _is_aware(mem.state.updated_at)
    mem.set_todos([{"content": "x", "status": "pending"}])
    assert _is_aware(mem.state.updated_at)


def test_session_id_format_unchanged(tmp_path):
    # Intentional exception: the session id stays a human-facing local-time
    # filename key (see roadmap Q-5) — not an ISO timestamp.
    mem = _memory(tmp_path)
    assert re.fullmatch(r"\d{8}_\d{6}", mem.session_id)


# ---------------------------------------------------------------------------
# Q-6 — status renderer lives on DacliUI
# ---------------------------------------------------------------------------
def test_status_panel_renders_on_ui(tmp_path):
    mem = _memory(tmp_path)
    mem.set_todos([{"content": "load data", "status": "in_progress"}])
    console = Console(record=True, width=100, force_terminal=False)
    ui = DacliUI(version="9.9.9", author="tester", console=console)
    ui.status_panel(mem)
    out = console.export_text()
    assert "Status" in out
    assert "Plan" in out
    assert "Statistics" in out
    assert "load data" in out
