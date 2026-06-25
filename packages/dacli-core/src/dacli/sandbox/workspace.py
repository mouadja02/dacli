"""The per-session agent workspace — the agent's "desk", jailed.

Each data-role session gets ``.dacli/sessions/<id>/workspace/`` that the agent
owns: it reads, writes, stages query results and scratch files here. The jail is
the safety boundary the shell tier runs inside — a command's working directory
**cannot escape** this root (enforced here + flagged by the command classifier),
writes are confined here plus any declared mounts, and the directory is
inspectable in the TUI (P2) and journaled for resume (P6).
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from dacli.core.logging_setup import get_logger

log = get_logger(__name__)


class WorkspaceJailError(PermissionError):
    """Raised when a path resolves outside the session workspace jail."""


class SessionWorkspace:
    """A jailed, per-session directory the agent owns.

    ``root`` is ``<workspace_root>/<session_id>/workspace``. ``mounts`` are extra
    absolute paths the session is explicitly allowed to read/write (declared,
    not discovered) — e.g. a project the user pointed the agent at.
    """

    def __init__(
        self,
        session_id: str,
        *,
        workspace_root: str = ".dacli/sessions",
        mounts: list[str] | None = None,
    ):
        self.session_id = session_id or "default"
        self.session_dir = Path(workspace_root) / self.session_id
        self.root = (self.session_dir / "workspace").resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        # Sibling dirs for rollback copy-asides and the command journal.
        self.backups_dir = self.session_dir / "backups"
        self.journal_dir = self.session_dir / "journal"
        self.backups_dir.mkdir(parents=True, exist_ok=True)
        self.journal_dir.mkdir(parents=True, exist_ok=True)
        self.mounts = [Path(m).resolve() for m in (mounts or [])]

    # ------------------------------------------------------------------
    # jail checks
    # ------------------------------------------------------------------
    def is_within_jail(self, path: os.PathLike | str) -> bool:
        """True if ``path`` is inside the workspace root or a declared mount."""
        try:
            target = Path(path).resolve()
        except Exception:
            return False
        for base in (self.root, *self.mounts):
            try:
                target.relative_to(base)
                return True
            except ValueError:
                continue
        return False

    def resolve(self, relative: str) -> Path:
        """Resolve a workspace-relative path, refusing anything that escapes.

        Guards against ``..`` traversal and absolute-path escapes — the cwd
        cannot leave the jail.
        """
        candidate = (self.root / relative).resolve() if not os.path.isabs(relative) else Path(relative).resolve()
        if not self.is_within_jail(candidate):
            raise WorkspaceJailError(
                f"path '{relative}' resolves outside the session workspace jail "
                f"({self.root})"
            )
        return candidate

    # ------------------------------------------------------------------
    # rollback support (copy-aside before an overwrite)
    # ------------------------------------------------------------------
    def backup(self, target: os.PathLike | str) -> Path | None:
        """Copy ``target`` aside into the backups dir; return the backup path.

        Returns ``None`` when there is nothing to back up (the target does not
        yet exist — i.e. a *new* file, which is recoverable by deletion). This is
        the native "undo" primitive the shell rollback plan relies on.
        """
        src = Path(target)
        if not src.exists():
            return None
        import time

        stamp = f"{int(time.time() * 1000)}"
        safe = "".join(c for c in src.name if c.isalnum() or c in "._-") or "file"
        dest = self.backups_dir / f"{safe}.{stamp}.bak"
        try:
            if src.is_dir():
                shutil.copytree(src, dest)
            else:
                shutil.copy2(src, dest)
            return dest
        except Exception:
            return None

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def cleanup(self, *, keep_journal: bool = True) -> None:
        """Remove the session's scratch workspace (optionally keep the journal)."""
        try:
            shutil.rmtree(self.root, ignore_errors=True)
            shutil.rmtree(self.backups_dir, ignore_errors=True)
            if not keep_journal:
                shutil.rmtree(self.journal_dir, ignore_errors=True)
        except Exception:
            log.debug("workspace cleanup failed for %s", self.root, exc_info=True)

    def __str__(self) -> str:
        return str(self.root)
