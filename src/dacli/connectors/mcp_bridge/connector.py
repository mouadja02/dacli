"""Opt-in MCP *client* bridge (F-7).

dacli deliberately does not adopt MCP internally — tools-as-code keeps context
lean and auditable. This connector is the one sanctioned crossing point: it
wraps a single external MCP server, maps each server tool to an
:class:`OperationSpec` at ``connect()`` time, and proxies ``invoke()`` to the
MCP tool call. Every proxied call still flows through ``Dispatcher.execute``
→ ``Governor`` → audit — **the bridge is not a governance bypass**.

Trust posture:

* Proxied tools default to ``Risk.RISKY`` unless the user pins them via
  ``settings.mcp.risk_overrides`` (deny-by-default: an unknown override value
  also falls back to risky).
* MCP tools cannot declare environment-anchored post-conditions, so proxied
  calls are held to the generic ``result_succeeded`` gate (documented in
  SKILL.md).
* The MCP SDK is an optional extra (``pip install "dacli[mcp]"``), lazy-imported
  on first connect — a default install never imports (or speaks) MCP.
"""

from __future__ import annotations

import re
from typing import Any

from dacli.connectors.base import (
    Connector,
    OperationSpec,
    Risk,
    ToolResult,
    ToolStatus,
)
from dacli.core.logging_setup import get_logger
from dacli.core.verify import data_has_keys, result_succeeded

log = get_logger(__name__)

_OP_PREFIX = "mcp_"
_LIST_TOOLS_OP = "mcp_list_tools"
_SANITIZE_RE = re.compile(r"[^A-Za-z0-9_]")

_RISK_VALUES = {r.value: r for r in Risk}


def _get(tool: Any, key: str, default: Any = None) -> Any:
    """Read ``key`` from an SDK tool object or a plain dict alike."""
    if isinstance(tool, dict):
        return tool.get(key, default)
    return getattr(tool, key, default)


class McpBridgeConnector(Connector):
    """Bridge one MCP server's tools into the governed dispatch path."""

    name = "mcp_bridge"

    def __init__(self, settings: Any):
        super().__init__(settings)
        #: Proxied specs discovered at connect(); empty until then (and forever
        # when no server is configured — the inert default).
        self._proxied: list[OperationSpec] = []
        #: op name -> remote MCP tool name (sanitization is not invertible).
        self._remote_names: dict[str, str] = {}
        self._session: Any = None
        self._stack: Any = None  # AsyncExitStack owning the transport
        #: Test seam: an async factory returning a session-like object with
        # ``list_tools()`` and ``call_tool(name, arguments)``. When set, the
        # MCP SDK is never imported.
        self._client_factory: Any = None

    # ------------------------------------------------------------------
    # configuration
    # ------------------------------------------------------------------
    def _cfg(self) -> Any:
        return getattr(self.settings, "mcp", None)

    def _configured(self) -> bool:
        cfg = self._cfg()
        return bool(cfg and (getattr(cfg, "command", "") or getattr(cfg, "url", "")))

    def _risk_for(self, remote_name: str) -> Risk:
        cfg = self._cfg()
        overrides = getattr(cfg, "risk_overrides", None) or {}
        raw = overrides.get(remote_name) or getattr(cfg, "default_risk", "risky")
        # Deny-by-default: an unknown/typo'd risk value is treated as risky.
        return _RISK_VALUES.get(str(raw).lower(), Risk.RISKY)

    # ------------------------------------------------------------------
    # contract surface
    # ------------------------------------------------------------------
    def operations(self) -> list[OperationSpec]:
        return [self._list_tools_spec(), *self._proxied]

    def _list_tools_spec(self) -> OperationSpec:
        return OperationSpec(
            name=_LIST_TOOLS_OP,
            description=(
                "List the tools exposed by the configured MCP server (name, "
                "description, assigned risk tier). Live introspection of what "
                "the bridge proxies."
            ),
            parameters={"type": "object", "properties": {}},
            capability="mcp_bridge.introspection",
            risk=Risk.SAFE,
            category="introspection",
            postconditions=[data_has_keys("tools", name="lists_tools")],
        )

    def _spec_for(self, tool: Any) -> OperationSpec | None:
        remote = str(_get(tool, "name", "") or "")
        if not remote:
            return None
        op_name = _OP_PREFIX + _SANITIZE_RE.sub("_", remote)[:64]
        if op_name in self._remote_names and self._remote_names[op_name] != remote:
            # Two remote tools sanitized to the same op name: keep the first,
            # skip the collision rather than silently mis-routing.
            log.debug("mcp tool name collision: %s vs %s", remote,
                      self._remote_names[op_name])
            return None
        schema = _get(tool, "inputSchema") or _get(tool, "input_schema") or {}
        if not isinstance(schema, dict) or schema.get("type") != "object":
            schema = {"type": "object", "properties": {}}
        risk = self._risk_for(remote)
        self._remote_names[op_name] = remote
        return OperationSpec(
            name=op_name,
            description=(
                f"[MCP:{remote}] {_get(tool, 'description', '') or ''} "
                "(proxied MCP tool — held to the generic verification gate)"
            ).strip(),
            parameters=schema,
            capability=f"mcp.{remote}",
            risk=risk,
            postconditions=[result_succeeded()],
        )

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    async def connect(self) -> bool:
        if not self._configured():
            # Inert by design: no server configured -> nothing to bridge.
            self.is_connected = False
            return False
        try:
            session = await self._open_session()
            tools = await self._list_remote_tools(session)
            self._proxied = []
            self._remote_names = {}
            for tool in tools:
                spec = self._spec_for(tool)
                if spec is not None:
                    self._proxied.append(spec)
            self.is_connected = True
        except Exception:
            log.debug("mcp bridge connect failed", exc_info=True)
            await self.disconnect()
            return False
        return True

    async def disconnect(self) -> None:
        stack, self._stack = self._stack, None
        self._session = None
        self.is_connected = False
        if stack is not None:
            try:
                await stack.aclose()
            except Exception:
                log.debug("mcp transport close failed", exc_info=True)

    async def health(self) -> ToolResult:
        if not self._configured():
            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.ERROR,
                error=(
                    "MCP bridge is not configured — set mcp.command (stdio) or "
                    "mcp.url (streamable-http) in config.yaml"
                ),
            )
        return ToolResult(
            tool_name=self.name,
            status=ToolStatus.SUCCESS,
            data={
                "configured": True,
                "connected": self.is_connected,
                "tools": len(self._proxied),
            },
        )

    # ------------------------------------------------------------------
    # transport (lazy SDK import — test seam first)
    # ------------------------------------------------------------------
    async def _open_session(self) -> Any:
        if self._session is not None:
            return self._session
        if self._client_factory is not None:
            self._session = await self._client_factory()
            return self._session

        try:
            from contextlib import AsyncExitStack

            from mcp import ClientSession, StdioServerParameters
            from mcp.client.stdio import stdio_client
        except ImportError as exc:  # pragma: no cover - exercised via message
            raise RuntimeError(
                "the MCP SDK is not installed — pip install \"dacli[mcp]\""
            ) from exc

        cfg = self._cfg()
        self._stack = AsyncExitStack()
        if getattr(cfg, "command", ""):
            params = StdioServerParameters(
                command=cfg.command, args=list(getattr(cfg, "args", []) or [])
            )
            read, write = await self._stack.enter_async_context(stdio_client(params))
        else:
            from mcp.client.streamable_http import streamablehttp_client

            read, write, _ = await self._stack.enter_async_context(
                streamablehttp_client(cfg.url)
            )
        session = await self._stack.enter_async_context(ClientSession(read, write))
        await session.initialize()
        self._session = session
        return session

    @staticmethod
    async def _list_remote_tools(session: Any) -> list[Any]:
        result = await session.list_tools()
        return list(_get(result, "tools", None) or (result if isinstance(result, list) else []))

    # ------------------------------------------------------------------
    # invoke
    # ------------------------------------------------------------------
    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        if op == _LIST_TOOLS_OP:
            return await self._invoke_list_tools()
        remote = self._remote_names.get(op)
        if remote is None:
            return ToolResult(
                tool_name=op,
                status=ToolStatus.ERROR,
                error=f"unknown MCP-proxied operation '{op}' (server not connected?)",
            )
        try:
            session = await self._open_session()
            result = await session.call_tool(remote, dict(args or {}))
        except Exception as exc:
            return ToolResult(tool_name=op, status=ToolStatus.ERROR, error=str(exc))
        return self._to_tool_result(op, result)

    async def _invoke_list_tools(self) -> ToolResult:
        try:
            session = await self._open_session()
            tools = await self._list_remote_tools(session)
        except Exception as exc:
            return ToolResult(
                tool_name=_LIST_TOOLS_OP, status=ToolStatus.ERROR, error=str(exc)
            )
        inventory = [
            {
                "name": str(_get(t, "name", "")),
                "description": str(_get(t, "description", "") or ""),
                "risk": self._risk_for(str(_get(t, "name", ""))).value,
            }
            for t in tools
        ]
        return ToolResult(
            tool_name=_LIST_TOOLS_OP,
            status=ToolStatus.SUCCESS,
            data={"tools": inventory, "count": len(inventory)},
        )

    @staticmethod
    def _to_tool_result(op: str, result: Any) -> ToolResult:
        """Normalize an MCP CallToolResult (or stub) into a ToolResult."""
        is_error = bool(_get(result, "isError", False) or _get(result, "is_error", False))
        data = _get(result, "structuredContent", None)
        if data is None:
            content = _get(result, "content", None)
            if isinstance(content, list):
                texts = [str(_get(c, "text", "") or "") for c in content]
                data = "\n".join(t for t in texts if t)
            else:
                data = content if content is not None else result
        if is_error:
            return ToolResult(
                tool_name=op,
                status=ToolStatus.ERROR,
                error=str(data) or "MCP tool reported an error",
            )
        return ToolResult(tool_name=op, status=ToolStatus.SUCCESS, data=data)
