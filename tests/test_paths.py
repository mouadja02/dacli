"""Tests for core/paths.py — the one path & resource resolver (P01).

Covers project detection, the POSIX/Windows split for the user config dir, the
``DACLI_HOME``/``DACLI_STATE_PATH`` overrides, and ``packaged_asset`` resolving a
file shipped in the wheel.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from dacli.core import paths


# ---- project_root ---------------------------------------------------------

@pytest.mark.parametrize("marker", [".dacli", "config.yaml", ".git"])
def test_project_root_finds_marker(tmp_path, marker):
    (tmp_path / marker).mkdir() if marker != "config.yaml" else (tmp_path / marker).touch()
    sub = tmp_path / "a" / "b"
    sub.mkdir(parents=True)
    assert paths.project_root(sub) == tmp_path


def test_project_root_none_without_marker(tmp_path):
    assert paths.project_root(tmp_path) is None


# ---- user_config_dir ------------------------------------------------------

def test_user_config_dir_honors_dacli_home(monkeypatch, tmp_path):
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "custom"))
    assert paths.user_config_dir() == tmp_path / "custom"


# Patching os.name to a foreign value makes pathlib.Path() try to build the wrong
# flavour and raise, so each branch is asserted on its native platform — the CI
# matrix (Linux) covers POSIX, Windows runners cover nt.
posix_only = pytest.mark.skipif(os.name != "posix", reason="POSIX user-dir branch")
nt_only = pytest.mark.skipif(os.name != "nt", reason="Windows user-dir branch")


@posix_only
def test_user_config_dir_posix_xdg(monkeypatch, tmp_path):
    monkeypatch.delenv("DACLI_HOME", raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    assert paths.user_config_dir() == tmp_path / "xdg" / "dacli"


@posix_only
def test_user_config_dir_posix_default(monkeypatch, tmp_path):
    monkeypatch.delenv("DACLI_HOME", raising=False)
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path / "h"))
    assert paths.user_config_dir() == tmp_path / "h" / ".config" / "dacli"


@nt_only
def test_user_config_dir_windows_appdata(monkeypatch, tmp_path):
    monkeypatch.delenv("DACLI_HOME", raising=False)
    monkeypatch.setenv("APPDATA", str(tmp_path / "Roaming"))
    assert paths.user_config_dir() == tmp_path / "Roaming" / "dacli"


# ---- state_dir ------------------------------------------------------------

def test_state_dir_state_path_override_wins(monkeypatch, tmp_path):
    # DACLI_STATE_PATH is the highest-priority override; base = parent of it.
    monkeypatch.setenv("DACLI_STATE_PATH", str(tmp_path / "s" / "state"))
    assert paths.state_dir() == tmp_path / "s"


def test_state_dir_project_local(monkeypatch, tmp_path):
    monkeypatch.delenv("DACLI_STATE_PATH", raising=False)
    (tmp_path / ".git").mkdir()
    monkeypatch.chdir(tmp_path)
    assert paths.state_dir() == tmp_path / ".dacli"


def test_state_dir_falls_back_to_user_config(monkeypatch, tmp_path):
    monkeypatch.delenv("DACLI_STATE_PATH", raising=False)
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    monkeypatch.chdir(tmp_path)  # no project marker here
    assert paths.state_dir() == tmp_path / "home"


# ---- resolve_config_path --------------------------------------------------

def test_resolve_config_path_explicit_wins(tmp_path):
    explicit = tmp_path / "my.yaml"
    explicit.touch()
    assert paths.resolve_config_path(str(explicit)) == explicit


def test_resolve_config_path_project(monkeypatch, tmp_path):
    cfg = tmp_path / "config.yaml"
    cfg.touch()
    monkeypatch.chdir(tmp_path)
    assert paths.resolve_config_path(None) == cfg


def test_resolve_config_path_none_when_absent(monkeypatch, tmp_path):
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "empty"))
    monkeypatch.chdir(tmp_path)
    assert paths.resolve_config_path(None) is None


# ---- resolve_policy_path --------------------------------------------------

class _Gov:
    def __init__(self, policy_path):
        self.policy_path = policy_path


class _Settings:
    def __init__(self, policy_path=None):
        self.governance = _Gov(policy_path)


def test_resolve_policy_path_explicit(tmp_path):
    p = tmp_path / "policy.yaml"
    p.touch()
    assert paths.resolve_policy_path(_Settings(str(p))) == p


def test_resolve_policy_path_packaged_fallback(monkeypatch, tmp_path):
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "empty"))
    monkeypatch.chdir(tmp_path)
    resolved = paths.resolve_policy_path(_Settings(None))
    assert resolved == paths.packaged_asset("config", "policy.yaml")
    assert resolved.exists()


# ---- packaged_asset / overlay ---------------------------------------------

def test_packaged_asset_resolves_shipped_policy():
    p = paths.packaged_asset("config", "policy.yaml")
    assert p.exists()
    assert p.name == "policy.yaml"


def test_user_prompt_overlay_under_state_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("DACLI_STATE_PATH", str(tmp_path / "s" / "state"))
    assert paths.user_prompt_overlay() == tmp_path / "s" / "system_prompt.md"


# ---- project_overlay_dir --------------------------------------------------

def test_project_overlay_dir(tmp_path):
    assert paths.project_overlay_dir(tmp_path) == tmp_path / ".dacli"


# ---- resource_dir ---------------------------------------------------------

def _project(tmp_path, monkeypatch):
    (tmp_path / ".git").mkdir()
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_resource_dir_project_overlay_wins_over_global(monkeypatch, tmp_path):
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    root = _project(tmp_path, monkeypatch)
    proj = root / ".dacli" / "extensions"
    proj.mkdir(parents=True)
    (tmp_path / "home" / "extensions").mkdir(parents=True)
    assert paths.resource_dir("extensions") == proj


def test_resource_dir_global_wins_over_bundled(monkeypatch, tmp_path):
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    monkeypatch.chdir(tmp_path)  # no project marker
    glob = tmp_path / "home" / "extensions"
    glob.mkdir(parents=True)
    bundled = tmp_path / "bundled"
    (bundled / "extensions").mkdir(parents=True)
    monkeypatch.setattr(paths, "bundled_seeds_dir", lambda kind: bundled / kind)
    assert paths.resource_dir("extensions") == glob


def test_resource_dir_falls_through_to_bundled(monkeypatch, tmp_path):
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    monkeypatch.chdir(tmp_path)
    bundled = tmp_path / "bundled"
    (bundled / "themes").mkdir(parents=True)
    monkeypatch.setattr(paths, "bundled_seeds_dir", lambda kind: bundled / kind)
    assert paths.resource_dir("themes") == bundled / "themes"


def test_resource_dir_create_makes_writable_default(monkeypatch, tmp_path):
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    monkeypatch.chdir(tmp_path)  # no project -> global is the writable target
    monkeypatch.setattr(paths, "bundled_seeds_dir", lambda kind: tmp_path / "nope" / kind)
    d = paths.resource_dir("skills", create=True)
    assert d == tmp_path / "home" / "skills"
    assert d.is_dir()


def test_resource_dir_create_in_project_targets_overlay(monkeypatch, tmp_path):
    root = _project(tmp_path, monkeypatch)
    monkeypatch.setattr(paths, "bundled_seeds_dir", lambda kind: tmp_path / "nope" / kind)
    d = paths.resource_dir("workspaces", create=True)
    assert d == root / ".dacli" / "workspaces"
    assert d.is_dir()


def test_resource_dir_unknown_kind_raises():
    with pytest.raises(ValueError):
        paths.resource_dir("widgets")


def test_resource_dir_secrets_is_project_base(monkeypatch, tmp_path):
    monkeypatch.delenv("DACLI_STATE_PATH", raising=False)
    root = _project(tmp_path, monkeypatch)
    assert paths.resource_dir("secrets") == root / ".dacli"


def test_resource_dir_secrets_finds_legacy_key(monkeypatch, tmp_path):
    monkeypatch.delenv("DACLI_STATE_PATH", raising=False)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".dacli").mkdir()
    (tmp_path / ".dacli" / ".key").touch()
    assert paths.resource_dir("secrets") == Path(".dacli")


# ---- packaging: resolves from the installed package, not __file__ math ----

def test_packaged_policy_resolves_in_fresh_interpreter():
    code = (
        "from dacli.core.paths import resolve_policy_path, packaged_asset; "
        "p = packaged_asset('config', 'policy.yaml'); "
        "assert p.exists(), p; "
        "print('ok')"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert proc.returncode == 0, proc.stderr
    assert "ok" in proc.stdout
