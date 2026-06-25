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

from dacli.core import paths

def _repo_root() -> Path:
    # Walk up for the .git marker. __file__ math is brittle after the M13 split
    # (this script is buried under packages/dacli/src/dacli/scripts/).
    here = Path(__file__).resolve()
    for d in (here, *here.parents):
        if (d / ".git").exists():
            return d
    return Path.cwd()


REPO_ROOT = _repo_root()
SANDBOX_WORKDIR = paths.project_overlay_dir(REPO_ROOT) / "sandbox"


def _rmtree(path: Path) -> bool:
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
        return True
    return False


def clean() -> int:
    """Run the sweep, printing what was removed. Returns the count removed."""
    removed = 0

    # Packaging output. After M13 each wheel builds under packages/<pkg>/, so
    # build/ and *.egg-info live there too; dist/ collects the built wheels.
    artifact_dirs = [REPO_ROOT / "build", REPO_ROOT / "dist"]
    artifact_dirs += list((REPO_ROOT / "packages").glob("*/build"))
    egg_globs = [REPO_ROOT.glob("*.egg-info"), (REPO_ROOT / "packages").glob("*/*.egg-info")]
    for d in artifact_dirs:
        if _rmtree(d):
            print(f"removed {d.relative_to(REPO_ROOT)}/")
            removed += 1
    for eggs in egg_globs:
        for egg in eggs:
            if _rmtree(egg):
                print(f"removed {egg.relative_to(REPO_ROOT)}/")
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
