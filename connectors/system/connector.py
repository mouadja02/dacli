"""Built-in 'system' connector.

The two built-in tools (``request_user_input`` and ``update_progress``) used to
be special-cased inside the agent's dispatch ladder. Modelling them as a
connector means *all* tools flow through the single dispatch path.

Unlike the platform connectors, the system connector needs runtime collaborators
(the user-input callback and the agent's memory), so it is constructed and
injected by the agent rather than discovered from a manifest. It is always
enabled.
"""

from typing import Any, Callable, Dict, List, Optional

from connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus


class SystemConnector(Connector):

    name = "system"

    def __init__(
        self,
        settings: Any = None,
        memory: Any = None,
        on_user_input_needed: Optional[Callable[[str], str]] = None,
    ):
        super().__init__(settings)
        self._memory = memory
        self._on_user_input_needed = on_user_input_needed
        # Always ready; no external connection.
        self._is_connected = True

    # ------------------------------------------------------------------
    # Connector contract
    # ------------------------------------------------------------------
    def operations(self) -> List[OperationSpec]:
        return [
            OperationSpec(
                name="request_user_input",
                description="Request input from the user when stuck, encountering errors, or needing clarification. Use this to put the user in the loop.",
                parameters={
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "The question or information request for the user"
                        },
                        "context": {
                            "type": "string",
                            "description": "Context about what led to this request"
                        }
                    },
                    "required": ["question"]
                },
                capability="system.user_input",
                risk=Risk.SAFE,
                display_name="Request User Input",
                category="system",
            ),
            OperationSpec(
                name="update_progress",
                description="Update the current progress status. Use to track phases and steps completed.",
                parameters={
                    "type": "object",
                    "properties": {
                        "phase": {
                            "type": "string",
                            "description": "Current phase (phase_0_infrastructure, phase_1_discovery, etc.)"
                        },
                        "step": {
                            "type": "string",
                            "description": "Description of the step completed"
                        },
                        "status": {
                            "type": "string",
                            "enum": ["in_progress", "completed", "failed"],
                            "description": "Status of the phase"
                        }
                    },
                    "required": ["phase", "step"]
                },
                capability="system.progress",
                risk=Risk.SAFE,
                display_name="Update Progress",
                category="system",
            ),
        ]

    async def invoke(self, op: str, args: Dict[str, Any]) -> ToolResult:
        if op == "request_user_input":
            return self._request_user_input(args)
        elif op == "update_progress":
            return self._update_progress(args)
        return ToolResult(
            tool_name=op,
            status=ToolStatus.ERROR,
            error=f"Unknown operation '{op}' for connector '{self.name}'",
        )

    async def health(self) -> ToolResult:
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS, data={"ready": True})

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------
    def _request_user_input(self, args: Dict[str, Any]) -> ToolResult:
        question = args.get("question", "")
        context = args.get("context", "")

        if self._on_user_input_needed:
            user_response = self._on_user_input_needed(
                f"{context}\n\n{question}" if context else question
            )
            return ToolResult(
                tool_name="request_user_input",
                status=ToolStatus.SUCCESS,
                data={"user_response": user_response}
            )
        else:
            return ToolResult(
                tool_name="request_user_input",
                status=ToolStatus.PENDING_APPROVAL,
                data={"question": question, "context": context}
            )

    def _update_progress(self, args: Dict[str, Any]) -> ToolResult:
        # Imported lazily to avoid a core <-> connectors import cycle at load time.
        from core.memory import PhaseStatus

        phase = args.get("phase", "")
        step = args.get("step", "")
        status = args.get("status", "in_progress")

        status_enum = {
            "in_progress": PhaseStatus.IN_PROGRESS,
            "completed": PhaseStatus.COMPLETED,
            "failed": PhaseStatus.FAILED
        }.get(status, PhaseStatus.IN_PROGRESS)

        self._memory.update_phase(phase, status=status_enum, step_completed=step)

        return ToolResult(
            tool_name="update_progress",
            status=ToolStatus.SUCCESS,
            data={"phase": phase, "step": step, "status": status}
        )
