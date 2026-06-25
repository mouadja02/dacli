"""Reference template: a CLI-driven connector.

Copy this when the platform ships a first-class CLI (``aws``, ``gcloud``, ``bq``,
``psql``, ``databricks``, ...). ``CliConnector`` centralizes subprocess
execution (``self._run``), binary-availability ``health``, and uniform
``ToolResult`` construction (``self._ok`` / ``self._fail``).

This file is a *pattern*, not a live connector.
"""

from __future__ import annotations

import time
from typing import Any

from dacli.connectors.base import OperationSpec, Risk, ToolResult
from dacli.connectors.cli_base import CliConnector
from dacli.core.connector_config import load_connector_config
from dacli.core.verify import result_succeeded


class MyCliConnector(CliConnector):
    # MUST equal the manifest `id`.
    name = "mycli"
    # The CLI executable this connector drives.
    binary = "mycli"

    def __init__(self, settings: Any, runner: Any = None):
        super().__init__(settings, runner=runner)
        self.cfg = load_connector_config("mycli", settings=settings)

    def operations(self) -> list[OperationSpec]:
        return [
            OperationSpec(
                name="mycli_status",
                description="Show platform status via the CLI.",
                parameters={"type": "object", "properties": {}, "required": []},
                capability="mycli.read",
                risk=Risk.SAFE,
                display_name="Status",
                category="mycli",
                postconditions=[result_succeeded()],
            ),
            OperationSpec(
                name="mycli_delete_resource",
                description="Delete a resource by id.",
                parameters={
                    "type": "object",
                    "properties": {
                        "resource_id": {"type": "string", "description": "Resource id."},
                    },
                    "required": ["resource_id"],
                },
                capability="mycli.delete",
                # IRREVERSIBLE: destructive. Governance requires a verified
                # rollback path / human approval before this runs.
                risk=Risk.IRREVERSIBLE,
                display_name="Delete Resource",
                category="mycli",
                postconditions=[result_succeeded()],
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        if op == "mycli_status":
            res = await self._run([self.binary, "status", "--format", "json"])
            if res.ok:
                return self._ok(op, res.stdout, started, argv=res.argv)
            return self._fail(op, res.stderr or "status failed", started, argv=res.argv)
        if op == "mycli_delete_resource":
            rid = str(args["resource_id"])
            res = await self._run([self.binary, "delete", rid, "--yes"])
            if res.ok:
                return self._ok(op, {"deleted": rid}, started, argv=res.argv)
            return self._fail(op, res.stderr or "delete failed", started, argv=res.argv)
        return self._unknown_op(op)
