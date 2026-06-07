"""WSL ``bash`` backend (Windows Subsystem for Linux)."""

from __future__ import annotations


from dacli.sandbox.shells.base import SENTINEL, ShellBackend


class WslBackend(ShellBackend):
    name = "wsl"
    binary = "wsl"

    def launch_argv(self) -> list[str]:
        # `wsl bash` runs a Linux bash inside the default distro, reading the
        # command stream from stdin (the PTY/pipe the transport owns).
        exe = self.binary if self.binary.lower().endswith(".exe") else f"{self.binary}.exe"
        return [exe, "bash"]

    def _sentinel_echo(self, nonce: str) -> str:
        # printf is more predictable than echo across bash configs; $? is the
        # previous command's exit status.
        return f"printf '%s\\n' \"{SENTINEL}:{nonce}:$?\""
