"""The one path & resource resolver (P01).

Three location kinds dacli has to keep apart, and where each lives:

- **packaged read-only assets** (prompts, templates, the default policy) — shipped
  in the wheel, resolved via :func:`packaged_asset` over the installed ``dacli``
  package (``importlib.resources``, not ``__file__`` math).
- **per-user global config/state** — :func:`user_config_dir` (XDG on POSIX,
  ``%APPDATA%`` on Windows), overridable with ``DACLI_HOME``.
- **project-local state** — ``<project_root>/.dacli`` when cwd is inside a project,
  via :func:`state_dir` / :func:`project_root`.

This module is the single source of truth. Callers that historically did their own
resolution (``core.crypto.resolve_base_dir``, ``config.settings.load_config``,
``scripts.cli._find_config_template``) route through here.
"""

from __future__ import annotations

import os
from importlib import resources
from pathlib import Path

#: Override for the per-user config/state root (highest priority in that resolver).
DACLI_HOME_ENV = "DACLI_HOME"
#: Back-compat override pointing at the session ``state/`` dir; its parent is the base.
STATE_PATH_ENV = "DACLI_STATE_PATH"
#: Legacy cwd-relative default for the session state dir. P02 retires this in favor
#: of :func:`state_dir`; kept here so the one definition is shared, not duplicated.
DEFAULT_STATE_PATH = ".dacli/state/"

#: A directory is a dacli project if it (or an ancestor) holds one of these.
_PROJECT_MARKERS = (".dacli", "config.yaml", ".git")


def packaged_asset(*parts: str) -> Path:
    """Read-only asset shipped in the wheel (prompts, templates, default policy)."""
    return Path(str(resources.files("dacli").joinpath(*parts)))


def user_config_dir() -> Path:
    """Per-user global config dir. ``DACLI_HOME`` overrides; else ``%APPDATA%\\dacli``
    on Windows, ``$XDG_CONFIG_HOME/dacli`` or ``~/.config/dacli`` on POSIX."""
    home = os.environ.get(DACLI_HOME_ENV)
    if home:
        return Path(home)
    if os.name == "nt":
        appdata = os.environ.get("APPDATA")
        base = Path(appdata) if appdata else Path.home() / "AppData" / "Roaming"
        return base / "dacli"
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg else Path.home() / ".config"
    return base / "dacli"


def project_root(start: Path | None = None) -> Path | None:
    """Walk up from ``start`` (default cwd) for a project marker; the dir, or None."""
    cur = (start or Path.cwd()).resolve()
    for d in (cur, *cur.parents):
        if any((d / m).exists() for m in _PROJECT_MARKERS):
            return d
    return None


def state_dir() -> Path:
    """Project-local ``<project_root>/.dacli`` when in a project, else
    :func:`user_config_dir`. ``DACLI_STATE_PATH`` (its parent) overrides both."""
    override = os.environ.get(STATE_PATH_ENV)
    if override:
        return Path(override).parent
    root = project_root()
    return root / ".dacli" if root is not None else user_config_dir()


def resolve_config_path(explicit: str | None) -> Path | None:
    """explicit > project_root/config.yaml > user_config_dir()/config.yaml > None."""
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    root = project_root()
    if root is not None:
        candidates.append(root / "config.yaml")
    candidates.append(user_config_dir() / "config.yaml")
    return next((p for p in candidates if p.exists()), None)


def resolve_policy_path(settings) -> Path:
    """explicit (settings.governance.policy_path) > project/config/policy.yaml
    > user_config_dir()/policy.yaml > packaged default."""
    explicit = getattr(getattr(settings, "governance", None), "policy_path", None)
    if explicit and Path(explicit).exists():
        return Path(explicit)
    root = project_root()
    if root is not None and (root / "config" / "policy.yaml").exists():
        return root / "config" / "policy.yaml"
    user = user_config_dir() / "policy.yaml"
    if user.exists():
        return user
    return packaged_asset("config", "policy.yaml")


def user_prompt_overlay() -> Path:
    """Editable system-prompt overlay (used by P03): ``<state_dir>/system_prompt.md``."""
    return state_dir() / "system_prompt.md"
