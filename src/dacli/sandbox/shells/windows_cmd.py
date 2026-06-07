"""Windows ``cmd.exe`` backend."""

from __future__ import annotations


from dacli.sandbox.shells.base import SENTINEL, ShellBackend


class WindowsCmdBackend(ShellBackend):
    name = "cmd"
    binary = "cmd"

    def launch_argv(self) -> list[str]:
        # /Q disables command echo (we still strip echoes defensively); the
        # shell reads further commands from its stdin pipe / PTY.
        return [self.binary if self.binary.lower().endswith(".exe") else f"{self.binary}.exe", "/Q"]

    def _sentinel_echo(self, nonce: str) -> str:
        # %ERRORLEVEL% expands to the previous command's exit code in cmd.exe.
        return f"echo {SENTINEL}:{nonce}:%ERRORLEVEL%"
