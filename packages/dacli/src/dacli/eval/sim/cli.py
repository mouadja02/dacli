"""A programmable, recording, optionally-flaky fake CLI runner.

Connectors call ``await runner(argv, ...)`` for every platform subprocess. The
real runner shells out; :class:`SimCli` answers from a pure responder function so
the suite never touches a real platform. Calls are recorded so a golden task can
assert *what the harness did* (e.g. "the destructive `rm` was never reached").
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from collections.abc import Callable

from dacli.connectors.cli_base import CliResult

# A responder maps an argv to a canned CliResult (pure, deterministic).
Responder = Callable[[list[str]], CliResult]


@dataclass
class Call:
    argv: list[str]


class SimCli:
    """An async, recording fake runner built from a synchronous ``responder``.

    ``failure_rate`` (with ``seed``) injects seeded transient failures so pass^k
    is measured against genuine flakiness; deterministic responders (rate 0.0)
    yield a clean pass^k of 1.0, which is exactly what the destructive-gate task
    must demonstrate. ``inject_error`` forces a hard failure response (used by
    the regression exit criterion to plant a degradation).
    """

    def __init__(
        self,
        responder: Responder,
        *,
        failure_rate: float = 0.0,
        seed: int | None = None,
        inject_error: str | None = None,
    ):
        self._responder = responder
        self._failure_rate = failure_rate
        self._rng = random.Random(seed)
        self._inject_error = inject_error
        self.calls: list[Call] = []

    async def __call__(self, argv, *args, **kwargs) -> CliResult:
        argv = list(argv)
        self.calls.append(Call(argv=argv))
        if self._inject_error is not None:
            return CliResult(1, "", self._inject_error, argv)
        if self._failure_rate and self._rng.random() < self._failure_rate:
            return CliResult(1, "", "simulated transient platform error", argv)
        return self._responder(argv)

    # -- introspection helpers for task assertions ----------------------
    def called_with(self, *needles: str) -> bool:
        """True if any recorded call's argv contains all the given substrings."""
        for call in self.calls:
            joined = " ".join(call.argv)
            if all(n in joined for n in needles):
                return True
        return False

    def reset(self) -> None:
        self.calls.clear()
