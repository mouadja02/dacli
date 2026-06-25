"""Shell backends for the governed terminal tier (Era 2, Phase 1).

A :class:`~sandbox.shells.base.ShellBackend` knows how to launch one kind of
shell and how to wrap a command so the terminal can detect *idle* (the command
finished) and capture its *exit code* reliably — the sentinel-marker strategy:
echo a unique nonce + ``$?`` after every command and read until that line
appears.

Four backends ship (the maintainer's Windows 11 native + WSL environment is the
conformance target):

* :class:`~sandbox.shells.windows_cmd.WindowsCmdBackend` — ``cmd.exe``
* :class:`~sandbox.shells.powershell.PowerShellBackend` — Windows PowerShell / pwsh
* :class:`~sandbox.shells.wsl.WslBackend` — ``wsl bash``
* :class:`~sandbox.shells.zsh.ZshBackend` — ``zsh`` (POSIX/macOS)

The PTY layer (``pywinpty`` on Windows, ``ptyprocess`` on POSIX) is optional;
:func:`~sandbox.shells.transports.make_transport` degrades to a pure-stdlib
line-buffered ``subprocess`` transport when no PTY library is present.
"""

from dacli.sandbox.shells.base import (
    SENTINEL,
    RawExec,
    ShellBackend,
    select_backend,
)
from dacli.sandbox.shells.windows_cmd import WindowsCmdBackend
from dacli.sandbox.shells.powershell import PowerShellBackend
from dacli.sandbox.shells.wsl import WslBackend
from dacli.sandbox.shells.zsh import ZshBackend

__all__ = [
    "SENTINEL",
    "PowerShellBackend",
    "RawExec",
    "ShellBackend",
    "WindowsCmdBackend",
    "WslBackend",
    "ZshBackend",
    "select_backend",
]
