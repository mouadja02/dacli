"""The shell connector — the third execution tier (Era 2).

Surfaces the governed :class:`~sandbox.terminal.TerminalSession` as a single
connector op (``run_shell_command``) so terminal execution flows through the
*same* dispatch + governance path as every tool: classify → policy →
rollback → approval → execute → post-condition → audit. It is injected by the
agent (like the ``system`` connector) because it needs a live session, so it has
no ``manifest.yaml`` and is never auto-discovered.
"""

from dacli.connectors.shell.connector import ShellConnector

__all__ = ["ShellConnector"]
