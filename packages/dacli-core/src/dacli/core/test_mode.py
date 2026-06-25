"""Test mode toggle for new connector validation.

When activated via ``/testmode``, connector calls are routed through the
existing Docker sandbox runtime instead of running locally. This lets users
test newly generated connectors in isolation before importing them.

The test mode:
- Toggles on/off via ``/testmode``
- Routes connector dispatch through the governed sandbox bridge
- Shows a visual ``[TEST]`` indicator in the TUI bottom toolbar
- The generated connector code is available inside the container via
  the bind-mounted workspace

The singleton :data:`test_mode` is imported by the CLI and the agent to
coordinate state.
"""

from __future__ import annotations

from typing import Any


class StagingMode:
    """Global test mode state.

    When active, :class:`connectors.dispatcher.Dispatcher` runs the
    connector-under-test in **staging mode**: every call is health-gated,
    exceptions are captured with full diagnostics, catalog/state side effects are
    suppressed, and the result is tagged so the UI can mark it ``[TEST]``. This
    lets a freshly generated connector be exercised on the host without trusting
    its outputs or letting it mutate session state.

    If ``connector_name`` is set, staging applies only to that connector;
    otherwise it applies to every non-built-in connector.
    """

    def __init__(self) -> None:
        self._active = False
        self._runtime: Any = None
        self._connector_name: str | None = None
        #: connectors whose ``health()`` already passed this test-mode session,
        # so we health-gate only on the first staged call (not every call).
        self._verified: set[str] = set()

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def connector_name(self) -> str | None:
        return self._connector_name

    def applies_to(self, connector_name: str) -> bool:
        """True if staging should wrap calls to ``connector_name``.

        Active + (no specific target, or this connector is the target). The
        dispatcher additionally excludes built-in connectors.
        """
        if not self._active:
            return False
        if self._connector_name is None:
            return True
        return connector_name == self._connector_name

    def is_verified(self, connector_name: str) -> bool:
        return connector_name in self._verified

    def mark_verified(self, connector_name: str) -> None:
        self._verified.add(connector_name)

    def toggle(self, connector_name: str | None = None) -> bool:
        """Toggle test mode. Returns the new state (True = on)."""
        self._active = not self._active
        if self._active:
            self._connector_name = connector_name
        else:
            self._connector_name = None
            self._runtime = None
        self._verified.clear()
        return self._active

    def activate(self, connector_name: str | None = None) -> None:
        self._active = True
        self._connector_name = connector_name
        self._verified.clear()

    def deactivate(self) -> None:
        self._active = False
        self._connector_name = None
        self._runtime = None
        self._verified.clear()

    def bind_runtime(self, runtime: Any) -> None:
        self._runtime = runtime

    @property
    def runtime(self) -> Any | None:
        return self._runtime

    def toolbar_text(self) -> str:
        if not self._active:
            return ""
        label = f"TEST · {self._connector_name}" if self._connector_name else "TEST"
        return f"[{label}]"


test_mode = StagingMode()
