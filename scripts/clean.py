"""Repo hygiene task — the ``make clean`` equivalent (P01 / step 6).

Removes build artifacts and accumulated caches so the stale-tree problem can't
silently reaccumulate:

* ``build/`` and ``*.egg-info/``   (packaging output — re-created on demand)
* every ``__pycache__/`` directory (compiled bytecode)
* ``.dacli/sandbox/run_*/``        (sandbox run workspaces)

Usage::

    python -m scripts.clean          # from the repo root
    python scripts/clean.py

It is intentionally dependency-free and safe to run anytime.
"""

from __future__ import annotations

import shutil
import sys

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SANDBOX_WORKDIR = REPO_ROOT / ".dacli" / "sandbox"


def _rmtree(path: Path) -> bool:
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
        return True
    return False


def clean() -> int:
    """Run the sweep, printing what was removed. Returns the count removed."""
    removed = 0

    # Packaging output.
    for name in ("build", "dacli.egg-info"):
        if _rmtree(REPO_ROOT / name):
            print(f"removed {name}/")
            removed += 1
    for egg in REPO_ROOT.glob("*.egg-info"):
        if _rmtree(egg):
            print(f"removed {egg.name}/")
            removed += 1

    # Bytecode caches (anywhere in the tree, but skip the virtualenv).
    for cache in REPO_ROOT.rglob("__pycache__"):
        if ".venv" in cache.parts:
            continue
        if _rmtree(cache):
            removed += 1
    print(f"removed __pycache__ dirs (cumulative count: {removed})")

    # Sandbox run workspaces.
    if SANDBOX_WORKDIR.exists():
        runs = [p for p in SANDBOX_WORKDIR.iterdir()
                if p.is_dir() and p.name.startswith("run_")]
        for run in runs:
            _rmtree(run)
        if runs:
            print(f"pruned {len(runs)} sandbox run_* workspace(s)")
            removed += len(runs)

    print("clean: done")
    return removed


if __name__ == "__main__":
    sys.exit(0 if clean() >= 0 else 1)
