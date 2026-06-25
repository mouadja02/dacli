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

import hashlib
import importlib.util
import inspect
import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass, field
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

    def __init__(
        self,
        name: str,
        *,
        config_provider: Callable[[str], dict] | None = None,
        session_log: SessionLog | None = None,
    ):
        self.name = name
        self._config_provider = config_provider
        self._session_log = session_log
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
        is :meth:`core.secrets.SecretStore.config` (M07); with none set, an empty
        dict."""
        if self._config_provider is None:
            return {}
        return self._config_provider(self.name)

    def on(self, event: str, handler: Callable) -> None:
        self.event_handlers.append((event, handler))

    def append_entry(self, entry: Any) -> None:
        """Append state to the session log so it outlives a reload (Pi's
        ``appendEntry``). The next loaded version replays it on ``session_start``."""
        if self._session_log is None:
            raise RuntimeError("append_entry needs a session log; load via ExtensionHost")
        self._session_log.append(self.name, entry)

    def entries(self) -> list[Any]:
        """This extension's session-log entries, oldest first."""
        if self._session_log is None:
            return []
        return self._session_log.entries(self.name)


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
        # Compile from source rather than spec.loader.exec_module: a reload of an
        # edit that keeps the file's size and mtime-second would otherwise reuse a
        # stale __pycache__ entry and run the old code. module_from_spec already
        # set __path__, so sibling imports still resolve.
        code = compile(init_path.read_text(encoding="utf-8"), str(init_path), "exec")
        exec(code, module.__dict__)
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


# ---------------------------------------------------------------------------
# Hot reload (M05)
#
# Pi's defining UX: edit an extension, /reload, keep working — no restart
# (reporting/01, reporting/03). The ExtensionHost owns the live registry and
# swaps it in place. A changed module is validated in a child process before it
# is imported here (the LLM-authored module must never run with the agent's
# privileges unvalidated — same rule as core/connector_generator.py); a changed
# module that fails validation keeps its previously loaded version. State
# survives the swap through an append-only SessionLog the host owns and hands to
# every module it (re)loads.
# ---------------------------------------------------------------------------
log = logging.getLogger(__name__)

_VALIDATION_TIMEOUT_S = 20.0

# Run in a child: import the module, confirm register(api) exists and runs
# cleanly against a throwaway ExtensionAPI (this is where a missing post-condition
# or a bad risk surfaces). Verdict is the last JSON line on stdout, mirroring
# core/connector_generator.py's validator. argv: <__init__.py path> <name>.
_EXT_VALIDATION_SCRIPT = """\
import importlib.util
import json
import sys
from pathlib import Path


def _verdict(ok, message):
    print(json.dumps({"ok": ok, "message": message}))
    sys.exit(0)


path, name = sys.argv[1], sys.argv[2]
from dacli.core.extensions import ExtensionAPI

spec = importlib.util.spec_from_file_location(
    f"dacli_ext_check_{name}", path, submodule_search_locations=[str(Path(path).parent)]
)
if spec is None or spec.loader is None:
    _verdict(False, "cannot build import spec")
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
try:
    spec.loader.exec_module(mod)
except Exception as exc:
    _verdict(False, f"import failed: {exc}")

register = getattr(mod, "register", None)
if not callable(register):
    _verdict(False, "module exports no register(api)")
try:
    register(ExtensionAPI(name))
except Exception as exc:
    _verdict(False, f"register(api) failed: {exc}")

_verdict(True, "valid")
"""


def _validate_extension_in_subprocess(name: str, init_path: Path) -> tuple[bool, str]:
    package_root = str(Path(__file__).resolve().parent.parent.parent)
    env = dict(os.environ)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        package_root + os.pathsep + existing if existing else package_root
    )
    try:
        proc = subprocess.run(  # fixed argv, no shell
            [sys.executable, "-c", _EXT_VALIDATION_SCRIPT, str(init_path), name],
            capture_output=True,
            text=True,
            timeout=_VALIDATION_TIMEOUT_S,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return False, f"validation timed out after {_VALIDATION_TIMEOUT_S:.0f}s"
    except Exception as exc:
        return False, f"validation subprocess could not run: {exc}"

    for line in reversed((proc.stdout or "").strip().splitlines()):
        try:
            verdict = json.loads(line)
            return bool(verdict.get("ok")), str(verdict.get("message", ""))
        except (json.JSONDecodeError, ValueError):
            continue
    detail = (proc.stderr or proc.stdout or "").strip()
    return False, f"validation failed (exit {proc.returncode}): {detail[-300:]}"


def _fingerprint(ext_dir: Path) -> str:
    """Content hash of every ``*.py`` under the extension, so an edit is detected
    by content, not mtime (which is too coarse to trust under a fast reload)."""
    h = hashlib.sha256()
    for f in sorted(ext_dir.rglob("*.py")):
        h.update(f.relative_to(ext_dir).as_posix().encode())
        h.update(b"\0")
        h.update(f.read_bytes())
    return h.hexdigest()


class SessionLog:
    """Append-only, per-extension state that outlives a module reload.

    The host owns one log per session and hands it to every ExtensionAPI it
    builds. After a reload the module is a fresh import with fresh in-memory
    state, but the log is the same object — so a ``session_start`` handler can
    replay what the prior version appended (Pi's appendEntry)."""

    def __init__(self):
        self._entries: dict[str, list[Any]] = {}

    def append(self, ext: str, entry: Any) -> None:
        self._entries.setdefault(ext, []).append(entry)

    def entries(self, ext: str) -> list[Any]:
        return list(self._entries.get(ext, []))


@dataclass
class ReloadResult:
    loaded: list[str] = field(default_factory=list)      # newly discovered
    reloaded: list[str] = field(default_factory=list)    # changed and re-adopted
    unchanged: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)     # gone from disk
    failed: dict[str, str] = field(default_factory=dict)  # name -> why; prior kept

    def report(self) -> str:
        parts: list[str] = []
        if self.loaded:
            parts.append(f"loaded {', '.join(self.loaded)}")
        if self.reloaded:
            parts.append(f"reloaded {', '.join(self.reloaded)}")
        if self.removed:
            parts.append(f"dropped {', '.join(self.removed)}")
        if self.unchanged:
            parts.append(f"{len(self.unchanged)} unchanged")
        for name, why in self.failed.items():
            parts.append(f"{name} failed, kept prior ({why})")
        return "; ".join(parts) or "no extensions"


# ---------------------------------------------------------------------------
# Legacy manifest.yaml bridge (M11 — temporary, one release)
#
# The old fleet was deleted; generated connectors are register(api) extensions
# now. But a user may still have a pre-pivot connector dir (manifest.yaml +
# connector.py) under ~/.dacli. Load it for one more release so nobody is
# stranded, instantiating the Connector by file path and exposing its operations
# as extension tools — with a deprecation warning telling them to regenerate it.
# ---------------------------------------------------------------------------
_LEGACY_NOTICE = (
    "manifest.yaml connector '{name}' is deprecated and will stop loading next "
    "release — regenerate it as a register(api) extension (/new-extension)."
)


def _legacy_api_for_manifest(
    conn_dir: Path,
    manifest: dict[str, Any],
    *,
    settings: Any,
    config_provider: Callable[[str], dict] | None,
    session_log: SessionLog | None,
) -> ExtensionAPI:
    """Adapt one legacy ``manifest.yaml`` connector into an :class:`ExtensionAPI`.

    Imports ``connector.py`` from the dir by path (the old dotted
    ``dacli.connectors.<name>`` package is gone), instantiates the class named in
    the manifest, and registers each operation as an extension tool delegating to
    ``connector.invoke``. Raises on a bad manifest/import so the host records it.
    """
    name = manifest.get("id") or conn_dir.name
    class_path = manifest.get("class") or ""
    class_name = class_path.rpartition(".")[2]
    if not class_name:
        raise ValueError("manifest missing 'class'")
    init_path = conn_dir / "connector.py"
    module = _import_extension(name, init_path)
    cls = getattr(module, class_name, None)
    if cls is None:
        raise ImportError(f"{class_name} not found in {init_path}")
    connector = cls(settings)

    api = ExtensionAPI(name, config_provider=config_provider, session_log=session_log)

    def _make_handler(op_name: str) -> Callable:
        async def handler(args: dict[str, Any], ctx: ToolContext) -> ToolResult:
            return await connector.invoke(op_name, dict(args or {}))

        return handler

    for spec in connector.operations():
        api.tools[spec.name] = RegisteredTool(name, spec, _make_handler(spec.name))
    return api


class ExtensionHost:
    """Owns the live :class:`ExtensionRegistry` and reloads it in place.

    :meth:`load` does the first discovery; :meth:`reload` is the ``/reload``
    entry — it re-discovers, validates each changed module in a child process,
    imports only what validated, and emits ``session_shutdown`` →
    ``session_start(reason="reload")`` around the swap. A changed module that
    fails validation is never imported and keeps its previously loaded version.
    """

    def __init__(
        self,
        extensions_dir: str | Path | None = None,
        *,
        config_provider: Callable[[str], dict] | None = None,
        settings: Any = None,
    ):
        self._dir = Path(extensions_dir) if extensions_dir else None
        self._config_provider = config_provider
        # Needed only to instantiate a legacy manifest.yaml connector (temp, M11).
        self._settings = settings
        self.log = SessionLog()
        self.registry = ExtensionRegistry()
        self._good: dict[str, ExtensionAPI] = {}
        self._fingerprints: dict[str, str] = {}
        self._failed: dict[str, str] = {}

    def _base(self) -> Path:
        return self._dir if self._dir else paths.resource_dir("extensions")

    def base_dir(self) -> Path:
        """Where extensions live for this host — the generator writes here before
        :meth:`reload`."""
        return self._base()

    def load(self) -> ReloadResult:
        return self._sync(reason="startup", shutdown=False)

    def reload(self) -> ReloadResult:
        return self._sync(reason="reload", shutdown=True)

    def _sync(self, *, reason: str, shutdown: bool) -> ReloadResult:
        if shutdown:
            self._emit("session_shutdown", reason)
        result = ReloadResult()

        base = self._base()
        discovered: dict[str, Path] = {}
        if base.exists():
            for init_path in sorted(base.glob("*/__init__.py")):
                discovered[init_path.parent.name] = init_path

        for name in list(self._good):
            if name not in discovered:
                del self._good[name]
                self._fingerprints.pop(name, None)
                self._failed.pop(name, None)
                result.removed.append(name)

        for name, init_path in discovered.items():
            known = name in self._good
            fp = _fingerprint(init_path.parent)
            if known and self._fingerprints.get(name) == fp:
                result.unchanged.append(name)
                continue

            ok, message = _validate_extension_in_subprocess(name, init_path)
            if not ok:
                self._failed[name] = message
                result.failed[name] = message
                continue
            try:
                api = self._import_and_register(name, init_path)
            except Exception as exc:
                self._failed[name] = f"load error: {exc}"
                result.failed[name] = self._failed[name]
                continue
            self._good[name] = api
            self._fingerprints[name] = fp
            self._failed.pop(name, None)
            (result.reloaded if known else result.loaded).append(name)

        self._rebuild()
        self._emit("session_start", reason)
        return result

    def _import_and_register(self, name: str, init_path: Path) -> ExtensionAPI:
        module = _import_extension(name, init_path)
        register = getattr(module, "register", None)
        if not callable(register):
            raise ImportError("module exports no register(api)")
        api = ExtensionAPI(
            name, config_provider=self._config_provider, session_log=self.log
        )
        register(api)
        return api

    def _rebuild(self) -> None:
        registry = ExtensionRegistry()
        for api in self._good.values():
            registry._merge(api)
        for name, why in self._failed.items():
            registry.record_failure(name, why)
        self._merge_legacy_manifests(registry)
        self.registry = registry

    def _merge_legacy_manifests(self, registry: ExtensionRegistry) -> None:
        """Fold any pre-pivot ``manifest.yaml`` connector in the base dir into the
        registry (temporary, M11). A register(api) extension owns its name over a
        legacy connector of the same name; a broken legacy dir is recorded, not
        fatal."""
        import yaml

        base = self._base()
        if not base.exists():
            return
        for manifest_path in sorted(base.glob("*/manifest.yaml")):
            conn_dir = manifest_path.parent
            name = conn_dir.name
            if name in self._good:
                continue  # a real extension already owns this name
            try:
                manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
                api = _legacy_api_for_manifest(
                    conn_dir, manifest, settings=self._settings,
                    config_provider=self._config_provider, session_log=self.log,
                )
            except Exception as exc:
                registry.record_failure(name, f"legacy manifest load error: {exc}")
                continue
            registry._merge(api)
            log.warning(_LEGACY_NOTICE.format(name=api.name))

    def _emit(self, event: str, reason: str) -> None:
        for name, handler in self.registry.handlers_for(event):
            try:
                handler(reason)
            except Exception as exc:
                # A lifecycle hook that raises must not abort the reload.
                log.warning("extension %s %s handler raised: %s", name, event, exc)
