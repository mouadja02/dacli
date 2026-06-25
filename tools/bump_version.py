#!/usr/bin/env python3
"""Bump dacli's version — the four wheels release in lockstep.

Since M13 the tree is four wheels, each single-sourcing its own ``__version__``
literal; ``pyproject.toml`` reads it dynamically at build time. They release
together, so this bumps all four to the same version at once. Usage::

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

_ROOT = Path(__file__).resolve().parent.parent
# The four version literals, in dependency order. The assembler (last) is the
# canonical read for --show; a bump writes all four.
INITS = [
    _ROOT / "packages" / "dacli-ai" / "src" / "dacli" / "ai" / "__init__.py",
    _ROOT / "packages" / "dacli-core" / "src" / "dacli" / "core" / "__init__.py",
    _ROOT / "packages" / "dacli-tui" / "src" / "dacli" / "tui" / "__init__.py",
    _ROOT / "packages" / "dacli" / "src" / "dacli" / "scripts" / "__init__.py",
]
_PATTERN = re.compile(r'^__version__\s*=\s*["\'](?P<v>[^"\']+)["\']', re.MULTILINE)
_SEMVER = re.compile(r"^\d+\.\d+\.\d+$")


def current_version(text: str) -> str:
    match = _PATTERN.search(text)
    if not match:
        sys.exit("error: no __version__ literal found")
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

    texts = {p: p.read_text(encoding="utf-8") for p in INITS}
    versions = {p: current_version(t) for p, t in texts.items()}
    drift = {p.name: v for p, v in versions.items() if v != versions[INITS[-1]]}
    current = versions[INITS[-1]]
    if drift:
        sys.exit(f"error: version drift across wheels: {drift} (canonical {current})")

    if argv[0] == "--show":
        print(current)
        return 0

    new = next_version(current, argv[0])
    if new == current:
        print(f"version already at {new}; nothing to do.")
        return 0

    for p, text in texts.items():
        p.write_text(_PATTERN.sub(f'__version__ = "{new}"', text, count=1), encoding="utf-8")
    print(f"bumped {current} -> {new} across {len(INITS)} wheels")
    print()
    print("Next:")
    print(f"  git commit -am 'release: v{new}'")
    print(f"  git tag -a v{new} -m 'v{new}' && git push --follow-tags")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
