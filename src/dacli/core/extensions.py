"""Extension API + Python ``register(api)`` loader (M03).

A "feature" is a Python module exporting ``register(api)`` — no manifest, no enum,
no settings section. The module *is* the registration (reporting/03). This runs
beside ``connectors/registry.py``; the live agent stays on the connector path
until M09.

``register(api)`` receives an :class:`ExtensionAPI` and calls ``api.tool(...)``,
``api.command(...)``, etc. to declare what the extension exposes. Two rules carry
over from the connector registry: discovery is **fault-isolated** (a broken module
is recorded and skipped, never crashes the loader, mirroring
``connectors/registry.py``'s ``_discover``) and post-conditions are **mandatory**
(a tool that declares none is refused — ``core.verify.require_postconditions``,
the same guard ``ConnectorRegistry.validate_postconditions`` uses).
"""

from __future__ import annotations

import importlib.util
import inspect
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from collections.abc import Callable

from dacli.connectors.base import OperationSpec, Risk, ToolResult, ToolStatus
from dacli.connectors.registry import ConfigField
from dacli.core import paths
from dacli.core.verify import (
    PostCondition,
    data_is_list,
    require_postconditions,
    result_succeeded,
    shell_deletes_observed,
    shell_exit_zero,
    shell_writes_observed,
)


# Post-conditions an extension may name as a string in ``postconditions=[...]``
# (reporting/03 writes ``["result_succeeded"]``). Only the zero-arg factories are
# nameable; a parameterized check is passed as a PostCondition object.
_POSTCONDITION_ALIASES: dict[str, Callable[[], PostCondition]] = {
    "result_succeeded": result_succeeded,
    "data_is_list": data_is_list,
    "shell_exit_zero": shell_exit_zero,
    "shell_writes_observed": shell_writes_observed,
    "shell_deletes_observed": shell_deletes_observed,
}


def _resolve_postconditions(label: str, pcs: list[Any]) -> list[PostCondition]:
    """Turn an extension's ``postconditions`` into PostCondition objects the
    verifier runs — resolving string aliases, passing objects through."""
    resolved: list[PostCondition] = []
    for pc in pcs:
        if isinstance(pc, PostCondition):
            resolved.append(pc)
        elif isinstance(pc, str):
            factory = _POSTCONDITION_ALIASES.get(pc)
            if factory is None:
                known = ", ".join(sorted(_POSTCONDITION_ALIASES))
                raise ValueError(
                    f"{label}: unknown post-condition {pc!r}; name one of "
                    f"{known}, or pass a PostCondition object"
                )
            resolved.append(factory())
        else:
            raise TypeError(
                f"{label}: post-condition must be a name or PostCondition, "
                f"got {type(pc).__name__}"
            )
    return resolved


def _coerce_risk(risk: Any) -> Risk:
    if isinstance(risk, Risk):
        return risk
    try:
        return Risk(str(risk).lower())
    except ValueError:
        valid = ", ".join(r.value for r in Risk)
        raise ValueError(f"unknown risk {risk!r}; expected one of {valid}") from None


def _as_schema(parameters: dict[str, Any] | None) -> dict[str, Any]:
    # reporting/03 shows ``parameters={"prefix": {"type": "string"}}`` — a bare
    # property map. Wrap that into a real object schema; pass a full schema (one
    # already carrying ``type``/``properties``) through untouched.
    if not parameters:
        return {"type": "object", "properties": {}}
    if "type" in parameters or "properties" in parameters:
        return parameters
    return {"type": "object", "properties": parameters}


@dataclass
class RegisteredTool:
    extension: str
    spec: OperationSpec
    handler: Callable


class ExtensionAPI:
    """The surface ``register(api)`` writes to. One instance per extension.

    Registrations accumulate on the instance; the loader merges them into the
    shared :class:`ExtensionRegistry` only once ``register`` returns cleanly, so a
    module that registers two tools and then raises commits neither.
    """

    def __init__(self, name: str, *, config_provider: Callable[[str], dict] | None = None):
        self.name = name
        self._config_provider = config_provider
        self.tools: dict[str, RegisteredTool] = {}
        self.commands: dict[str, dict[str, Any]] = {}
        self.shortcuts: dict[str, dict[str, Any]] = {}
        self.providers: dict[str, Any] = {}
        self.config_fields: list[ConfigField] = []
        self.event_handlers: list[tuple[str, Callable]] = []

    def tool(
        self,
        *,
        name: str,
        description: str,
        parameters: dict[str, Any] | None = None,
        risk: Any = None,
        postconditions: Any = (),
        capability: str | None = None,
        display_name: str | None = None,
        category: str | None = None,
    ):
        """Decorator registering an LLM-callable tool. ``risk`` and
        ``postconditions`` are mandatory — both are refused here, at registration,
        not at dispatch."""
        if risk is None:
            raise ValueError(
                f"tool '{name}' must declare a risk (safe|write|risky|irreversible)"
            )
        pcs = list(postconditions)
        require_postconditions(f"{self.name}.{name}", pcs)
        pcs = _resolve_postconditions(f"{self.name}.{name}", pcs)
        spec = OperationSpec(
            name=name,
            description=description,
            parameters=_as_schema(parameters),
            capability=capability or self.name,
            risk=_coerce_risk(risk),
            display_name=display_name,
            category=category,
            postconditions=pcs,
        )

        def decorator(fn: Callable) -> Callable:
            self.tools[name] = RegisteredTool(self.name, spec, fn)
            return fn

        return decorator

    def command(self, name: str, handler: Callable, *, description: str = "") -> None:
        self.commands[name] = {"handler": handler, "description": description}

    def shortcut(self, key: str, handler: Callable, *, description: str = "") -> None:
        self.shortcuts[key] = {"handler": handler, "description": description}

    def provider(self, name: str, config: Any) -> None:
        self.providers[name] = config

    def config_field(
        self,
        name: str,
        *,
        secret: bool = False,
        required: bool = False,
        field_type: str = "str",
        default: Any = None,
        description: str = "",
    ) -> None:
        self.config_fields.append(
            ConfigField(
                name=name,
                field_type=field_type,
                required=required,
                default="" if secret else default,
                is_secret=secret,
                description=description,
            )
        )

    def config(self) -> dict[str, Any]:
        """This extension's config, secrets decrypted at call time. The provider
        is wired in M07; with none set this is an empty dict."""
        if self._config_provider is None:
            return {}
        return self._config_provider(self.name)

    def on(self, event: str, handler: Callable) -> None:
        self.event_handlers.append((event, handler))


class ExtensionRegistry:
    """Merged view of every loaded extension, plus the ones that failed to load."""

    def __init__(self):
        self._tools: dict[str, RegisteredTool] = {}
        self._commands: dict[str, dict[str, Any]] = {}
        self._shortcuts: dict[str, dict[str, Any]] = {}
        self._providers: dict[str, Any] = {}
        self._config_fields: dict[str, list[ConfigField]] = {}
        self._event_handlers: dict[str, list[tuple[str, Callable]]] = {}
        self._extensions: list[str] = []
        self._failed: dict[str, str] = {}

    def _merge(self, api: ExtensionAPI) -> None:
        self._tools.update(api.tools)
        self._commands.update(api.commands)
        self._shortcuts.update(api.shortcuts)
        self._providers.update(api.providers)
        self._config_fields[api.name] = list(api.config_fields)
        for event, handler in api.event_handlers:
            self._event_handlers.setdefault(event, []).append((api.name, handler))
        self._extensions.append(api.name)

    def record_failure(self, name: str, reason: str) -> None:
        self._failed[name] = reason

    def get_tool_definitions(self) -> list[dict[str, Any]]:
        """OpenAI-style tool definitions for every registered tool."""
        return [t.spec.to_tool_definition() for t in self._tools.values()]

    def resolve(self, tool_name: str) -> RegisteredTool | None:
        return self._tools.get(tool_name)

    def get_operation_spec(self, tool_name: str) -> OperationSpec | None:
        tool = self._tools.get(tool_name)
        return tool.spec if tool else None

    def handlers_for(self, event: str) -> list[tuple[str, Callable]]:
        return list(self._event_handlers.get(event, []))

    def config_fields(self, extension: str) -> list[ConfigField]:
        return list(self._config_fields.get(extension, []))

    def commands(self) -> dict[str, dict[str, Any]]:
        return dict(self._commands)

    def shortcuts(self) -> dict[str, dict[str, Any]]:
        return dict(self._shortcuts)

    def providers(self) -> dict[str, Any]:
        return dict(self._providers)

    def extension_ids(self) -> list[str]:
        return list(self._extensions)

    def failed_extensions(self) -> dict[str, str]:
        """Extensions that failed to load, mapped to why they were skipped — so a
        bad (e.g. freshly generated) extension is visible, not silently gone."""
        return dict(self._failed)


def _import_extension(name: str, init_path: Path):
    mod_name = f"dacli_ext_{name}"
    spec = importlib.util.spec_from_file_location(
        mod_name, init_path, submodule_search_locations=[str(init_path.parent)]
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot build import spec for {init_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module  # so the package can import its own siblings
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(mod_name, None)
        raise
    return module


def load_extensions(
    extensions_dir: str | Path | None = None,
    *,
    config_provider: Callable[[str], dict] | None = None,
) -> ExtensionRegistry:
    """Discover ``<dir>/*/__init__.py``, import each, and call ``register(api)``.

    ``extensions_dir`` defaults to ``paths.resource_dir("extensions")``. Discovery
    is fault-isolated: a module that won't import, exports no ``register``, or
    raises from ``register`` (including a tool with no post-condition) is recorded
    in :meth:`ExtensionRegistry.failed_extensions` and skipped.
    """
    base = Path(extensions_dir) if extensions_dir else paths.resource_dir("extensions")
    registry = ExtensionRegistry()
    if not base.exists():
        return registry
    for init_path in sorted(base.glob("*/__init__.py")):
        name = init_path.parent.name
        try:
            module = _import_extension(name, init_path)
            register = getattr(module, "register", None)
            if not callable(register):
                registry.record_failure(name, "module exports no register(api)")
                continue
            api = ExtensionAPI(name, config_provider=config_provider)
            register(api)
        except Exception as exc:
            registry.record_failure(name, f"load error: {exc}")
            continue
        registry._merge(api)
    return registry


# ---------------------------------------------------------------------------
# Governed dispatch (M04)
#
# The Dispatcher (connectors/dispatcher.py) already wraps every call in the
# governance + post-condition spine. To run an extension tool through that exact
# spine we present the ExtensionRegistry behind the small surface the Dispatcher
# consumes — resolve / get_operation_spec / is_builtin — and wrap each tool's
# handler in a connector-shaped object. Wire, don't rebuild.
# ---------------------------------------------------------------------------
@dataclass
class ToolContext:
    """Handed to a tool handler as ``ctx``. ``ok``/``fail`` build the ToolResult
    the dispatcher governs, verifies, and audits."""

    tool_name: str

    def ok(self, data: Any = None, **metadata: Any) -> ToolResult:
        return ToolResult(
            tool_name=self.tool_name, status=ToolStatus.SUCCESS,
            data=data, metadata=dict(metadata),
        )

    def fail(self, error: Any, **metadata: Any) -> ToolResult:
        return ToolResult(
            tool_name=self.tool_name, status=ToolStatus.ERROR,
            error=str(error), metadata=dict(metadata),
        )


class _ExtensionTool:
    """One registered tool wearing the connector surface the Dispatcher and
    Governor read: ``name`` (the extension id, so it's scoped and audited like a
    connector) and ``invoke``. ``health`` is here only for the staging gate."""

    _on_progress: Any = None

    def __init__(self, tool: RegisteredTool):
        self._tool = tool
        self.name = tool.extension

    async def invoke(self, op: Any, args: dict[str, Any]) -> ToolResult:
        ctx = ToolContext(self._tool.spec.name)
        out = self._tool.handler(dict(args or {}), ctx)
        if inspect.isawaitable(out):
            out = await out
        # The contract is ctx.ok/ctx.fail; a handler returning a bare value is
        # taken as a success payload rather than failing the call.
        return out if isinstance(out, ToolResult) else ctx.ok(out)

    async def health(self) -> ToolResult:
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS)


class ExtensionDispatchRegistry:
    """Adapts an :class:`ExtensionRegistry` to the surface ``Dispatcher`` calls,
    so an extension tool runs the same review → execute → verify → audit path a
    connector op does."""

    def __init__(self, registry: ExtensionRegistry):
        self._registry = registry

    def resolve(self, tool_name: str) -> tuple[_ExtensionTool, str] | None:
        tool = self._registry.resolve(tool_name)
        if tool is None:
            return None
        return _ExtensionTool(tool), tool.spec.name

    def get_operation_spec(self, tool_name: str) -> OperationSpec | None:
        return self._registry.get_operation_spec(tool_name)

    def is_builtin(self, name: str) -> bool:
        # Staging (test mode) is a connector-under-test concern; extensions don't
        # go through it, so the dispatcher's staging gate treats them as trusted.
        return True
