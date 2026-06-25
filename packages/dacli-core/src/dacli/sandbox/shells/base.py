"""``ShellBackend`` protocol + the shared sentinel idle/exit-code strategy.

Every backend launches a long-lived interactive shell and answers two questions
the :class:`~sandbox.terminal.TerminalSession` needs after writing a command:

1. **Has it finished?** (idle detection) — and
2. **What was its exit code?**

Both are answered with one trick that works across every shell: after the user
command, write a second command that echoes a **sentinel line** carrying a
per-command nonce and the last exit code. The reader streams output until it
sees ``<SENTINEL>:<nonce>:<rc>`` — at which point the command is done and ``rc``
is authoritative. This avoids the brittle "guess from a prompt regex" approach
and gives a faithful exit code even for shells (PowerShell) where that is fiddly.
"""

from __future__ import annotations

import os
import re
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


#: Stable, unlikely-to-collide marker that opens every command-completion line.
SENTINEL = "__DACLI_DONE__"


@dataclass
class RawExec:
    """The raw outcome of running one command line in a shell session."""

    output: str
    exit_code: int
    timed_out: bool = False
    # The literal lines the transport produced (pre-sentinel-stripping) — kept
    # for provenance / debugging; the session tags these with command ids.
    raw_lines: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and not self.timed_out


class ShellBackend(ABC):
    """How to launch a shell and wrap a command for sentinel-based capture."""

    #: Stable id (cmd | powershell | wsl | zsh).
    name: str = ""

    #: The executable this backend drives (resolved on PATH for availability).
    binary: str = ""

    def __init__(self, *, binary: str | None = None):
        if binary:
            self.binary = binary

    # ------------------------------------------------------------------
    # launch
    # ------------------------------------------------------------------
    @abstractmethod
    def launch_argv(self) -> list[str]:
        """argv that starts the shell in a persistent, interactive-ish mode."""
        raise NotImplementedError

    def available(self) -> bool:
        """True if the shell binary is reachable on PATH."""
        return shutil.which(self.binary) is not None

    # ------------------------------------------------------------------
    # the sentinel protocol
    # ------------------------------------------------------------------
    @abstractmethod
    def _sentinel_echo(self, nonce: str) -> str:
        """A single shell statement that prints ``<SENTINEL>:<nonce>:<rc>``."""
        raise NotImplementedError

    def format_command(self, command: str, nonce: str) -> str:
        """The exact bytes to write to the shell for one governed command.

        The user command runs first; the sentinel echo runs next and reports the
        *previous* command's exit code. A trailing newline submits each line.
        """
        return f"{command}\n{self._sentinel_echo(nonce)}\n"

    def sentinel_pattern(self, nonce: str) -> re.Pattern[str]:
        """Matches the completion line and captures the exit code (group 1)."""
        return re.compile(
            re.escape(f"{SENTINEL}:{nonce}:") + r"(-?\d+)"
        )

    def is_sentinel_line(self, line: str, nonce: str) -> int | None:
        """Return the parsed exit code if ``line`` is this command's sentinel."""
        m = self.sentinel_pattern(nonce).search(line)
        if not m:
            return None
        try:
            return int(m.group(1))
        except (TypeError, ValueError):
            return None

    def is_echo_of(self, line: str, command: str, nonce: str) -> bool:
        """True if ``line`` is just the shell echoing back what we typed.

        Interactive shells (and PTYs) echo the typed command and the sentinel
        statement; those lines are noise, not output, so the session drops them.
        """
        stripped = line.strip()
        if not stripped:
            return False
        if SENTINEL in stripped and nonce in stripped and "echo" in stripped.lower():
            return True
        if SENTINEL in stripped and nonce in stripped and "Write-Output" in stripped:
            return True
        if SENTINEL in stripped and nonce in stripped and "printf" in stripped:
            return True
        return stripped == command.strip()

    # ------------------------------------------------------------------
    # interrupt
    # ------------------------------------------------------------------
    def interrupt_bytes(self) -> bytes:
        """The control sequence that interrupts a running command (Ctrl-C)."""
        return b"\x03"


def select_backend(name: str = "auto") -> ShellBackend:
    """Resolve a backend by name, with ``auto`` picking the platform default.

    ``auto`` → PowerShell on Windows, ``zsh`` if present else ``bash``-via-wsl
    semantics on POSIX. Unknown names fall back to ``auto`` rather than raising,
    so a typo in config degrades to a working shell instead of a crash.
    """
    # Imported lazily to avoid an import cycle at module load.
    from dacli.sandbox.shells.windows_cmd import WindowsCmdBackend
    from dacli.sandbox.shells.powershell import PowerShellBackend
    from dacli.sandbox.shells.wsl import WslBackend
    from dacli.sandbox.shells.zsh import ZshBackend

    table = {
        "cmd": WindowsCmdBackend,
        "powershell": PowerShellBackend,
        "pwsh": PowerShellBackend,
        "wsl": WslBackend,
        "bash": WslBackend,
        "zsh": ZshBackend,
    }
    key = (name or "auto").strip().lower()
    if key in table:
        return table[key]()

    if os.name == "nt":
        return PowerShellBackend()
    # POSIX default: prefer zsh when installed, else a bash login shell.
    z = ZshBackend()
    if z.available():
        return z
    return ZshBackend(binary="bash")
