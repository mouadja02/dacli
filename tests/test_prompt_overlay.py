"""M14 — user SYSTEM.md override + AGENTS.md operating notes.

The packaged ``core.md`` stays read-only. A ``SYSTEM.md`` (``paths.system_md_target``)
*replaces* it as the base prompt; ``AGENTS.md`` files are *appended* on top,
hierarchically merged. ``save_system_prompt`` targets SYSTEM.md (never
site-packages) and ``dacli prompt`` points users there.
"""

import pytest
from click.testing import CliRunner

from dacli.prompts import system_prompt as sp


@pytest.fixture
def override(monkeypatch, tmp_path):
    # DACLI_STATE_PATH's parent is the state dir; SYSTEM.md = <state_dir>/SYSTEM.md.
    monkeypatch.setenv("DACLI_STATE_PATH", str(tmp_path / "state" / "state"))
    (tmp_path / "state").mkdir()
    return tmp_path / "state" / "SYSTEM.md"


def test_compose_is_core_without_override(override):
    assert not override.exists()
    assert sp.compose_system_prompt() == sp.CORE_FRAGMENT.read_text(encoding="utf-8").strip()


def test_system_md_replaces_core(override):
    override.write_text("MY OWN PROMPT", encoding="utf-8")
    out = sp.compose_system_prompt()
    assert out == "MY OWN PROMPT"
    # the packaged core is gone, not merely appended-to.
    core_head = sp.CORE_FRAGMENT.read_text(encoding="utf-8").strip()[:30]
    assert core_head not in out


def test_agents_md_appended_after_base(override, monkeypatch, tmp_path):
    # AGENTS.md is read from user_config_dir (DACLI_HOME) and the project overlay.
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    (tmp_path / "home" / "AGENTS.md").write_text("GLOBAL NOTE", encoding="utf-8")
    out = sp.compose_system_prompt()
    core = sp.CORE_FRAGMENT.read_text(encoding="utf-8").strip()
    assert out.startswith(core)
    assert "GLOBAL NOTE" in out
    assert out.index(core[:20]) < out.index("GLOBAL NOTE")


def test_agents_before_connector_fragments(override):
    frags = [p.stem for p in sp.FRAGMENTS_DIR.glob("*.md") if p.name != "core.md"]
    if not frags:
        pytest.skip("no connector fragments shipped")
    override.write_text("BASE", encoding="utf-8")
    out = sp.compose_system_prompt(disclosed_connectors=[frags[0]])
    frag_text = (sp.FRAGMENTS_DIR / f"{frags[0]}.md").read_text(encoding="utf-8").strip()
    assert out.index("BASE") < out.index(frag_text[:20])


def test_save_default_targets_override(override):
    written = sp.save_system_prompt("hello", None)
    assert written == override
    assert override.read_text(encoding="utf-8") == "hello"


def test_save_refuses_packaged_path(override):
    with pytest.raises(ValueError, match="read-only"):
        sp.save_system_prompt("x", str(sp.CORE_FRAGMENT))


def test_save_refuses_anywhere_in_package(override):
    with pytest.raises(ValueError, match="read-only"):
        sp.save_system_prompt("x", str(sp.PACKAGE_DIR / "prompts" / "fragments" / "evil.md"))


def test_save_allows_arbitrary_export(override, tmp_path):
    out = tmp_path / "export.md"
    assert sp.save_system_prompt("exported", str(out)) == out
    assert out.read_text(encoding="utf-8") == "exported"


def test_prompt_command_points_at_override_not_sitepackages(override):
    from dacli.scripts.cli import cli

    result = CliRunner().invoke(cli, ["prompt"])
    assert result.exit_code == 0, result.output
    assert "read-only" in result.output
    assert "site-packages" not in result.output


def test_prompt_edit_creates_override(override, monkeypatch):
    from dacli.scripts.cli import cli

    monkeypatch.delenv("EDITOR", raising=False)
    monkeypatch.delenv("VISUAL", raising=False)
    assert not override.exists()
    result = CliRunner().invoke(cli, ["prompt", "--edit"])
    assert result.exit_code == 0, result.output
    assert override.exists()
    assert "Created override" in result.output
