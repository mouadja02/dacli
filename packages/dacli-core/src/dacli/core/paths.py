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

#: Name of the per-project overlay dir. The one place the literal lives.
OVERLAY_DIRNAME = ".dacli"

#: A directory is a dacli project if it (or an ancestor) holds one of these.
_PROJECT_MARKERS = (OVERLAY_DIRNAME, "config.yaml", ".git")

#: Resource kinds the agent owns under the ``.dacli`` overlay (reporting/03).
RESOURCE_KINDS = ("extensions", "skills", "themes", "secrets", "workspaces")


def packaged_asset(*parts: str) -> Path:
    """Read-only asset shipped in one of the wheels (prompts, templates, policy).

    ``dacli`` is a PEP 420 namespace split across the four wheels, so
    ``resources.files("dacli")`` is a ``MultiplexedPath`` spanning every installed
    portion. Fold ``joinpath`` one segment at a time: its multi-arg form on a
    MultiplexedPath only landed in 3.12, and the first real segment drops us onto a
    plain path anyway. A segment absent from every portion (e.g. ``seeds`` with the
    assembler wheel not installed — a valid headless-core state) resolves to a
    non-existent path the caller already guards with ``.exists()``.
    """
    asset = resources.files("dacli")
    for part in parts:
        asset = asset.joinpath(part)
    return Path(str(asset))


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


def project_overlay_dir(root: Path) -> Path:
    """The ``.dacli`` overlay dir under a given project (or repo) root."""
    return root / OVERLAY_DIRNAME


def state_dir() -> Path:
    """Project-local ``<project_root>/.dacli`` when in a project, else
    :func:`user_config_dir`. ``DACLI_STATE_PATH`` (its parent) overrides both."""
    override = os.environ.get(STATE_PATH_ENV)
    if override:
        return Path(override).parent
    root = project_root()
    return project_overlay_dir(root) if root is not None else user_config_dir()


def bundled_seeds_dir(kind: str) -> Path:
    """The wheel-shipped seed dir for ``kind`` — lowest precedence in the overlay.

    Populated in M08; until then the dir may not exist, which just means the
    overlay falls through to the writable default.
    """
    return packaged_asset("seeds", kind)


def _secrets_base_dir(state_path: str | None = None) -> Path:
    """Base dir for the encryption key and the secrets store.

    Priority: an explicit ``state_path`` > ``DACLI_STATE_PATH`` (both are a
    ``state/`` dir whose *parent* is the base) > an existing legacy cwd
    ``.dacli/.key`` > :func:`state_dir`. Kept legacy-compatible so an install
    that predates the per-user move keeps decrypting its store unchanged.
    """
    sp = state_path or os.environ.get(STATE_PATH_ENV)
    if sp:
        return Path(sp).parent
    legacy = Path(DEFAULT_STATE_PATH).parent
    if (legacy / ".key").exists():
        return legacy
    return state_dir()


def resource_dir(
    kind: str, *, state_path: str | None = None, create: bool = False
) -> Path:
    """Resolve the directory for a resource ``kind``, project overlay first.

    Precedence: project ``<root>/.dacli/<kind>`` > global ``<user_config_dir>/<kind>``
    > the bundled seed dir. Returns the first that exists; with none present, the
    writable default (the project overlay inside a project, else global), created
    when ``create`` is set.

    ``secrets`` is special: it resolves the legacy-compatible key/store base via
    :func:`_secrets_base_dir` (``state_path`` applies only here), not a ``secrets/``
    subdir, so an existing store stays readable.
    """
    if kind not in RESOURCE_KINDS:
        raise ValueError(f"unknown resource kind: {kind!r}")
    if kind == "secrets":
        return _secrets_base_dir(state_path)

    root = project_root()
    project = project_overlay_dir(root) / kind if root is not None else None
    glob = user_config_dir() / kind
    for cand in (project, glob, bundled_seeds_dir(kind)):
        if cand is not None and cand.exists():
            return cand
    target = project if project is not None else glob
    if create:
        target.mkdir(parents=True, exist_ok=True)
    return target


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
