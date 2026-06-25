"""Reference template: a REST/SDK-driven connector.

Copy this when the platform is reached through a Python SDK or an HTTP API
(no first-class CLI). Replace ``mytool`` throughout, implement the operations,
and ship a matching ``manifest.yaml`` (see ``manifest_template.yaml``).

This file is a *pattern*, not a live connector — it has no manifest at a
connector dir, so the registry never loads it.
"""

from __future__ import annotations

import time
from typing import Any

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.core.connector_config import load_connector_config
from dacli.core.verify import result_succeeded, data_has_keys


class MyToolConnector(Connector):
    # MUST equal the manifest `id`.
    name = "mytool"

    def __init__(self, settings: Any):
        super().__init__(settings)
        # Generated connectors have no Settings section: read the values the user
        # entered via /connect (decrypted automatically). Pass settings so a
        # custom state_path resolves correctly.
        self.cfg = load_connector_config("mytool", settings=settings)
        self._client = None  # lazily built from self.cfg in _ensure_client()

    def operations(self) -> list[OperationSpec]:
        return [
            OperationSpec(
                name="mytool_list_items",
                description="List items from the platform.",
                parameters={
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Max number of items to return.",
                        },
                    },
                    "required": [],
                },
                capability="mytool.read",
                risk=Risk.SAFE,
                display_name="List Items",
                category="mytool",
                # A SAFE read: assert the call succeeded and returned a list.
                postconditions=[data_has_keys("items", name="returned_items")],
            ),
            OperationSpec(
                name="mytool_create_item",
                description="Create a new item on the platform.",
                parameters={
                    "type": "object",
                    "properties": {
                        "title": {"type": "string", "description": "Item title."},
                    },
                    "required": ["title"],
                },
                capability="mytool.write",
                # WRITE: creates state. Governance gates writes before they run.
                risk=Risk.WRITE,
                display_name="Create Item",
                category="mytool",
                postconditions=[result_succeeded()],
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        try:
            if op == "mytool_list_items":
                items = await self._list_items(int(args.get("limit") or 50))
                return self._ok(op, {"items": items}, started)
            if op == "mytool_create_item":
                created = await self._create_item(str(args["title"]))
                return self._ok(op, created, started)
        except Exception as exc:
            return self._fail(op, str(exc), started)
        return ToolResult(
            tool_name=op,
            status=ToolStatus.ERROR,
            error=f"Unknown operation '{op}' for connector '{self.name}'",
        )

    async def health(self) -> ToolResult:
        started = time.time()
        api_key = self.cfg.get("api_key")
        if not api_key:
            return self._fail("health", "api_key is not configured (run /connect mytool).", started)
        # Real connectors should make a cheap authenticated call here.
        return self._ok("health", {"ready": True}, started)

    # ------------------------------------------------------------------
    # Implementation details (replace with real SDK/HTTP calls)
    # ------------------------------------------------------------------
    async def _list_items(self, limit: int) -> list[dict[str, Any]]:
        return []

    async def _create_item(self, title: str) -> dict[str, Any]:
        return {"title": title, "created": True}

    def _ok(self, op: str, data: Any, started: float) -> ToolResult:
        return ToolResult(
            tool_name=self.name,
            status=ToolStatus.SUCCESS,
            data=data,
            execution_time_ms=(time.time() - started) * 1000,
            metadata={"operation": op},
        )

    def _fail(self, op: str, error: str, started: float) -> ToolResult:
        return ToolResult(
            tool_name=self.name,
            status=ToolStatus.ERROR,
            error=error,
            execution_time_ms=(time.time() - started) * 1000,
            metadata={"operation": op},
        )
