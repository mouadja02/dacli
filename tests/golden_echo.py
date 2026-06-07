"""Throwaway 'echo' connector used to prove that adding a connector requires
**zero** edits to ``core/`` or ``reasoning/`` (exit criterion).

It is also reused by the golden transcript test as a deterministic, side-effect
free platform connector. It deliberately lives under ``tests/`` rather than
``connectors/`` so it is not part of the shipped product.
"""

from typing import Any

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus


class EchoConnector(Connector):

    name = "echo"

    def operations(self) -> list[OperationSpec]:
        return [
            OperationSpec(
                name="echo_say",
                description="Echo back the provided text.",
                parameters={
                    "type": "object",
                    "properties": {
                        "text": {"type": "string", "description": "Text to echo back"}
                    },
                    "required": ["text"],
                },
                capability="echo.say",
                risk=Risk.SAFE,
                display_name="Echo Say",
                category="echo",
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        if op == "echo_say":
            return ToolResult(
                tool_name="echo_say",
                status=ToolStatus.SUCCESS,
                data={"echoed": args.get("text", "")},
            )
        return ToolResult(
            tool_name=op,
            status=ToolStatus.ERROR,
            error=f"Unknown operation '{op}' for connector '{self.name}'",
        )

    async def health(self) -> ToolResult:
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS, data={"ready": True})
