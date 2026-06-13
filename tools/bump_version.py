#!/usr/bin/env python3
"""Bump dacli's version — the single source of truth.

The version lives as one literal in ``src/dacli/__init__.py``; ``pyproject.toml``
reads it dynamically at build time and ``dacli.core`` re-exports it, so this is
the only file to touch. Usage::

    python tools/bump_version.py 0.2.0      # set an exact version
    python tools/bump_version.py patch      # 0.1.0 -> 0.1.1
    python tools/bump_version.py minor      # 0.1.3 -> 0.2.0
    python tools/bump_version.py major      # 0.2.1 -> 1.0.0
    python tools/bump_version.py --show     # print the current version

It prints the next steps (commit + annotated tag) but never commits or tags for
you — releasing is a deliberate act.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

INIT = Path(__file__).resolve().parent.parent / "src" / "dacli" / "__init__.py"
_PATTERN = re.compile(r'^__version__\s*=\s*["\'](?P<v>[^"\']+)["\']', re.MULTILINE)
_SEMVER = re.compile(r"^\d+\.\d+\.\d+$")


def current_version(text: str) -> str:
    match = _PATTERN.search(text)
    if not match:
        sys.exit(f"error: no __version__ literal found in {INIT}")
    return match.group("v")


def next_version(current: str, spec: str) -> str:
    if _SEMVER.match(spec):
        return spec
    if spec in ("major", "minor", "patch"):
        parts = current.split(".")
        if len(parts) != 3 or not all(p.isdigit() for p in parts):
            sys.exit(
                f"error: current version {current!r} is not X.Y.Z; pass an exact version."
            )
        major, minor, patch = (int(p) for p in parts)
        if spec == "major":
            return f"{major + 1}.0.0"
        if spec == "minor":
            return f"{major}.{minor + 1}.0"
        return f"{major}.{minor}.{patch + 1}"
    sys.exit(f"error: {spec!r} is not a valid version or one of major|minor|patch.")


def main(argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0

    text = INIT.read_text(encoding="utf-8")
    current = current_version(text)

    if argv[0] == "--show":
        print(current)
        return 0

    new = next_version(current, argv[0])
    if new == current:
        print(f"version already at {new}; nothing to do.")
        return 0

    INIT.write_text(_PATTERN.sub(f'__version__ = "{new}"', text, count=1), encoding="utf-8")
    print(f"bumped {current} -> {new}  ({INIT})")
    print()
    print("Next:")
    print(f"  git commit -am 'release: v{new}'")
    print(f"  git tag -a v{new} -m 'v{new}' && git push --follow-tags")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
