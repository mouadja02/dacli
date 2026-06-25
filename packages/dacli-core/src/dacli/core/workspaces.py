"""Workspaces (M15) — thin helpers over the paths resolver.

A workspace is a ``~/.dacli/workspaces/<name>/`` overlay with its own
extensions, secrets, state, history and audit ledger; the global ``~/.dacli/``
is the default. Selection is process-wide (``core.paths``), so the live host
re-resolves through the precedence chain on switch with no restart. The actual
host rewire lives at the call site (``tui.chat_session``); this module only owns
the on-disk side: create the overlay, list what exists, select the active one.
"""

from __future__ import annotations

from pathlib import Path

from dacli.core import paths


def current() -> str | None:
    """The active workspace name, or None for the default (global) one."""
    return paths.active_workspace()


def list_names() -> list[str]:
    """Named workspaces on disk (the default isn't one of them)."""
    return paths.list_workspaces()


def create(name: str) -> Path:
    """Create the overlay dir for ``name`` and return it. The name must be a
    single safe path segment; ``default`` is reserved for the global workspace."""
    if not name or name.strip().lower() == "default":
        raise ValueError("'default' is the global workspace; pick another name")
    if not paths._WORKSPACE_NAME.match(name.strip()):
        raise ValueError(f"invalid workspace name: {name!r}")
    root = paths.workspaces_dir() / name.strip()
    root.mkdir(parents=True, exist_ok=True)
    return root


def select(name: str | None) -> Path:
    """Make ``name`` the active workspace and return its root. A named workspace
    is created on first selection; ``None``/``default`` returns to the global
    default."""
    paths.set_active_workspace(name)
    root = paths.workspace_root()
    if paths.active_workspace() is not None:
        root.mkdir(parents=True, exist_ok=True)
    return root
