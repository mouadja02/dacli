"""HTTP-API connector base.

Some platforms are reached over an HTTP control plane rather than a CLI — an
orchestrator's REST API (Airflow) or GraphQL endpoint (Dagster). This base gives
those connectors the same testability the CLI base gives the shell ones: an
**injectable transport** so golden tests drive the connector with canned HTTP
responses and never touch the network. The live path lazily builds an httpx
client.

A subclass sets the base URL + auth headers, implements ``operations`` and
``invoke``, and calls :meth:`_request`.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any
from collections.abc import Awaitable, Callable

from dacli.connectors.base import Connector, ToolResult, ToolStatus


@dataclass
class HttpResult:
    status: int
    data: Any = None
    text: str = ""
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


# A transport takes (method, path, *, params, json, headers) and returns an
# HttpResult, sync or async.
Transport = Callable[..., Any]


class HttpConnector(Connector):
    def __init__(self, settings: Any, transport: Transport | None = None):
        super().__init__(settings)
        self._transport = transport

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------
    def _base_url(self) -> str:
        return ""

    def _default_headers(self) -> dict[str, str]:
        return {}

    def _timeout(self) -> int:
        return 60

    # ------------------------------------------------------------------
    # Request plumbing
    # ------------------------------------------------------------------
    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> HttpResult:
        headers = self._default_headers()
        if self._transport is not None:
            outcome = self._transport(method, path, params=params, json=json, headers=headers)
            if isinstance(outcome, Awaitable):
                outcome = await outcome
            return outcome

        import httpx

        url = self._base_url().rstrip("/") + path
        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            resp = await client.request(method, url, params=params, json=json, headers=headers)
        data = None
        try:
            data = resp.json()
        except Exception:
            data = None
        return HttpResult(resp.status_code, data, resp.text, dict(resp.headers))

    def _reachable(self) -> bool:
        return self._transport is not None or bool(self._base_url())

    # ------------------------------------------------------------------
    # ToolResult helpers (uniform timing + provenance)
    # ------------------------------------------------------------------
    def _ok(self, op: str, data: Any, started: float, **metadata: Any) -> ToolResult:
        return ToolResult(
            tool_name=self.name, status=ToolStatus.SUCCESS, data=data,
            execution_time_ms=(time.time() - started) * 1000,
            metadata={"operation": op, **metadata},
        )

    def _fail(self, op: str, error: str, started: float, **metadata: Any) -> ToolResult:
        return ToolResult(
            tool_name=self.name, status=ToolStatus.ERROR, error=error,
            execution_time_ms=(time.time() - started) * 1000,
            metadata={"operation": op, **metadata},
        )

    def _unknown_op(self, op: str) -> ToolResult:
        return ToolResult(
            tool_name=op, status=ToolStatus.ERROR,
            error=f"Unknown operation '{op}' for connector '{self.name}'",
        )

    # ------------------------------------------------------------------
    async def connect(self) -> bool:
        self.is_connected = self._reachable()
        return self.is_connected

    async def health(self) -> ToolResult:
        started = time.time()
        if not self._reachable():
            return self._fail("health", f"{self.name} has no base URL configured.", started)
        return self._ok("health", {"base_url": self._base_url(), "reachable": True}, started)
