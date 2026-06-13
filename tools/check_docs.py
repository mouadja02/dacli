"""Docs drift gate (P12/D-7) — fail when the README's numbers rot.

The README once claimed "200 tests" while the suite ran 368; numbers that are
not enforced go stale. This script checks the working tree against the docs:

1. the tests badge count == the number of tests pytest collects,
2. the `dacli eval` sample's OVERALL task count == the live golden suite size,
3. every slash command in ``CLI_COMMANDS`` and every click subcommand is
   mentioned in the README command reference.

Run it from the repo root (CI runs it after the test suite):

    python tools/check_docs.py
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
README = REPO / "README.md"


def collected_test_count() -> int:
    """The number of tests pytest collects (the suite's ground truth)."""
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "tests", "-q", "--collect-only"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    )
    match = re.search(r"(\d+) tests? collected", proc.stdout)
    if match is None:
        raise SystemExit(f"could not parse pytest collection summary:\n{proc.stdout[-500:]}")
    return int(match.group(1))


def badge_test_count(readme: str) -> int:
    """The count claimed by the shields.io tests badge."""
    match = re.search(r"img\.shields\.io/badge/tests-(\d+)", readme)
    if match is None:
        raise SystemExit("README is missing the tests badge (img.shields.io/badge/tests-<N>...)")
    return int(match.group(1))


def eval_sample_task_count(readme: str) -> int:
    """The OVERALL task count shown in the README's `dacli eval` sample."""
    match = re.search(r"^OVERALL\s+(\d+)", readme, re.MULTILINE)
    if match is None:
        raise SystemExit("README is missing the `dacli eval` sample's OVERALL line")
    return int(match.group(1))


def golden_suite_task_count() -> int:
    """The live golden suite's size (what `dacli eval` actually runs)."""
    from dacli.eval.golden import build_golden_suite

    return len(build_golden_suite())


def missing_commands(readme: str) -> list[str]:
    """Slash commands / click subcommands that the README never mentions."""
    from dacli.config import CLI_COMMANDS
    from dacli.scripts.cli import cli

    missing = [
        entry.split()[0]
        for entry, _ in CLI_COMMANDS
        if entry.split()[0] not in readme
    ]
    missing.extend(
        f"dacli {name}" for name in cli.commands if f"dacli {name}" not in readme
    )
    return missing


def main() -> int:
    readme = README.read_text(encoding="utf-8")
    failures: list[str] = []

    collected = collected_test_count()
    badge = badge_test_count(readme)
    if badge != collected:
        failures.append(
            f"tests badge says {badge} but pytest collects {collected} — "
            f"update the badge in README.md"
        )

    suite = golden_suite_task_count()
    sample = eval_sample_task_count(readme)
    if sample != suite:
        failures.append(
            f"`dacli eval` sample shows OVERALL {sample} tasks but the golden "
            f"suite has {suite} — regenerate the sample from `dacli eval --quick`"
        )

    failures.extend(
        f"command {cmd!r} is not mentioned in README.md"
        for cmd in missing_commands(readme)
    )

    if failures:
        print("docs drift detected:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    print(f"docs in sync: {collected} tests, {suite} eval tasks, all commands documented.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
