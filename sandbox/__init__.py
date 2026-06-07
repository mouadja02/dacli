"""Code-execution sandbox — the "complex op" half of the hybrid model.

The agent writes Python against a capability-gated, secret-free SDK; it runs in
an isolated subprocess under resource + egress limits; large results stay on disk
(only a bounded summary returns to context); and **every SDK call still flows
through the Governor** — the sandbox is not a governance bypass.
"""

from sandbox.policy import SandboxPolicy
from sandbox.sdk import ConnectorSDK
from sandbox.runtime import SandboxRuntime, SandboxRunResult
from sandbox.workspace import SessionWorkspace, WorkspaceJailError
from sandbox.terminal import TerminalSession, CommandResult, ScrollbackLine

__all__ = [
    "CommandResult",
    "ConnectorSDK",
    "SandboxPolicy",
    "SandboxRunResult",
    "SandboxRuntime",
    "ScrollbackLine",
    "SessionWorkspace",
    "TerminalSession",
    "WorkspaceJailError",
]
