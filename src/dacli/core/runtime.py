"""Process-level handles the host hands to seeds that need more than config.

``register(api)`` gives a seed its config and session log. The shell seed needs
one more thing the seed can't own: the live, governed terminal session. The old
``ShellConnector`` got it by late-bind (``bind_session``); a seed has no instance
to bind, so the host stashes the session here at startup and the shell seed reads
it per command. One process, one session — tests set it directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Terminal:
    session: Any
    store: Any = None
    settings: Any = None


_terminal: Terminal | None = None


def set_terminal(session: Any, store: Any = None, settings: Any = None) -> None:
    global _terminal
    _terminal = Terminal(session=session, store=store, settings=settings)


def terminal() -> Terminal | None:
    return _terminal


def clear_terminal() -> None:
    global _terminal
    _terminal = None
