"""P03 — editable system-prompt overlay.

The packaged ``core.md`` stays read-only; an overlay at
``paths.user_prompt_overlay()`` is the writable layer. ``compose_system_prompt``
folds it in when present, ``save_system_prompt`` targets it (never site-packages),
and ``dacli prompt`` points users there instead of the wheel.
"""

import pytest
from click.testing import CliRunner

from dacli.prompts import system_prompt as sp


@pytest.fixture
def overlay(monkeypatch, tmp_path):
    # DACLI_STATE_PATH's parent is the state dir; overlay = <state_dir>/system_prompt.md.
    monkeypatch.setenv("DACLI_STATE_PATH", str(tmp_path / "state" / "state"))
    (tmp_path / "state").mkdir()
    return tmp_path / "state" / "system_prompt.md"


def test_compose_unchanged_without_overlay(overlay):
    assert not overlay.exists()
    assert sp.compose_system_prompt() == sp.CORE_FRAGMENT.read_text(encoding="utf-8").strip()


def test_compose_includes_overlay(overlay):
    overlay.write_text("OVERLAY MARKER LINE", encoding="utf-8")
    out = sp.compose_system_prompt()
    assert "OVERLAY MARKER LINE" in out
    # core comes first, overlay after it.
    core = sp.CORE_FRAGMENT.read_text(encoding="utf-8").strip()
    assert out.index(core[:30]) < out.index("OVERLAY MARKER LINE")


def test_overlay_before_connector_fragments(overlay):
    overlay.write_text("OVERLAY MARKER", encoding="utf-8")
    # pick any shipped connector fragment to disclose.
    frags = [p.stem for p in sp.FRAGMENTS_DIR.glob("*.md") if p.name != "core.md"]
    if not frags:
        pytest.skip("no connector fragments shipped")
    out = sp.compose_system_prompt(disclosed_connectors=[frags[0]])
    frag_text = (sp.FRAGMENTS_DIR / f"{frags[0]}.md").read_text(encoding="utf-8").strip()
    assert out.index("OVERLAY MARKER") < out.index(frag_text[:20])


def test_save_default_targets_overlay(overlay):
    written = sp.save_system_prompt("hello", None)
    assert written == overlay
    assert overlay.read_text(encoding="utf-8") == "hello"


def test_save_refuses_packaged_path(overlay):
    with pytest.raises(ValueError, match="read-only"):
        sp.save_system_prompt("x", str(sp.CORE_FRAGMENT))


def test_save_refuses_anywhere_in_package(overlay):
    with pytest.raises(ValueError, match="read-only"):
        sp.save_system_prompt("x", str(sp.PACKAGE_DIR / "prompts" / "fragments" / "evil.md"))


def test_save_allows_arbitrary_export(overlay, tmp_path):
    out = tmp_path / "export.md"
    assert sp.save_system_prompt("exported", str(out)) == out
    assert out.read_text(encoding="utf-8") == "exported"


def test_prompt_command_points_at_overlay_not_sitepackages(overlay):
    from dacli.scripts.cli import cli

    result = CliRunner().invoke(cli, ["prompt"])
    assert result.exit_code == 0, result.output
    assert "read-only" in result.output
    assert "site-packages" not in result.output
    assert "Edit this file to customize" not in result.output


def test_prompt_edit_creates_overlay(overlay, monkeypatch):
    from dacli.scripts.cli import cli

    monkeypatch.delenv("EDITOR", raising=False)
    monkeypatch.delenv("VISUAL", raising=False)
    assert not overlay.exists()
    result = CliRunner().invoke(cli, ["prompt", "--edit"])
    assert result.exit_code == 0, result.output
    assert overlay.exists()
    assert "Created overlay" in result.output
