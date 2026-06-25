"""Windows PowerShell / PowerShell Core (``pwsh``) backend."""

from __future__ import annotations

import shutil

from dacli.sandbox.shells.base import SENTINEL, ShellBackend


class PowerShellBackend(ShellBackend):
    name = "powershell"
    binary = "powershell"

    def __init__(self, *, binary: str | None = None):
        # Prefer cross-platform pwsh when available; fall back to Windows
        # PowerShell. An explicit ``binary`` always wins.
        if binary:
            resolved = binary
        elif shutil.which("pwsh"):
            resolved = "pwsh"
        else:
            resolved = "powershell"
        super().__init__(binary=resolved)

    def launch_argv(self) -> list[str]:
        # ``-Command -`` reads a command stream from stdin; -NoProfile keeps the
        # session deterministic (no user profile side effects).
        return [self.binary, "-NoLogo", "-NoProfile", "-Command", "-"]

    def _sentinel_echo(self, nonce: str) -> str:
        # $LASTEXITCODE is set by native programs; for pure cmdlets it is null,
        # so fall back to $? (success boolean). This yields a faithful rc for
        # both native commands and cmdlets.
        # IMPORTANT: capture $? FIRST — any subsequent assignment (even
        # $__c=$LASTEXITCODE) resets $? to $true, losing the user command's
        # success/failure status.
        return (
            "$__ok=$?; $__c=$LASTEXITCODE; "
            "if($null -eq $__c){$__c=if($__ok){0}else{1}}; "
            f"Write-Output \"{SENTINEL}:{nonce}:$__c\""
        )
