"""POSIX ``zsh`` (or ``bash``) backend for macOS / Linux."""

from __future__ import annotations


from dacli.sandbox.shells.base import SENTINEL, ShellBackend


class ZshBackend(ShellBackend):
    name = "zsh"
    binary = "zsh"

    def launch_argv(self) -> list[str]:
        # No -i: an interactive shell wants a tty and prints prompts/job-control
        # noise. Reading the command stream from stdin keeps capture clean; the
        # sentinel echo provides the "command finished" signal a prompt would.
        return [self.binary]

    def _sentinel_echo(self, nonce: str) -> str:
        return f"printf '%s\\n' \"{SENTINEL}:{nonce}:$?\""
