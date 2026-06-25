"""Connector plugin registry.

Discovers connectors by scanning ``connectors/*/manifest.yaml`` (id, class path,
required config keys, enabled flag + display metadata), instantiates them, and
exposes:

- the LLM tool definitions (replacing the agent's hand-written
  ``_build_tool_definitions``),
- a name -> ``(connector, op)`` resolver for the dispatcher,
- a catalog of metadata for the setup wizard,
- enable/disable state read from ``config/connectors.yaml``.

This replaces ``config/tool_registry.py``: adding a platform is now "drop a
folder with a manifest", not "edit an enum + the agent".
"""

import importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from collections.abc import Iterable

import yaml

from dacli.connectors.base import Connector

# Default location of the user's enable/disable selections.
CONNECTORS_CONFIG_PATH = "config/connectors.yaml"

_SECRET_FIELD_NAMES = frozenset(
    {
        "api_key",
        "password",
        "token",
        "secret",
        "access_key",
        "secret_key",
        "secret_access_key",
        "private_key",
        "client_secret",
    }
)


@dataclass
class ConfigField:
    name: str
    field_type: str = "str"
    required: bool = False
    default: Any = None
    is_secret: bool = False
    description: str = ""


def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_connectors_config(config_path: str = CONNECTORS_CONFIG_PATH) -> dict[str, Any]:
    """Load the persisted enable/disable selections.

    Shape::

        setup_completed: bool
        connectors:
          <id>:
            enabled: bool
            operations: {<op_name>: bool, ...}
    """
    path = Path(config_path)
    if not path.exists():
        return {"setup_completed": False, "connectors": {}}
    data = _load_yaml(path)
    data.setdefault("setup_completed", False)
    data.setdefault("connectors", {})
    return data


def save_connectors_config(
    config: dict[str, Any], config_path: str = CONNECTORS_CONFIG_PATH
) -> None:
    """Persist the enable/disable selections to ``config/connectors.yaml``."""
    path = Path(config_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(
            config, f, default_flow_style=False, sort_keys=False, allow_unicode=True
        )


class ConnectorRegistry:
    """Discovers connectors and resolves tool calls to (connector, op)."""

    def __init__(
        self,
        settings: Any,
        connectors_dir: str | None = None,
        config_path: str = CONNECTORS_CONFIG_PATH,
        extra_connectors: list[Connector] | None = None,
        enforce_postconditions: bool = False,
        exclude: Iterable[str] | None = None,
    ):
        self._settings = settings
        self._connectors_dir = (
            Path(connectors_dir) if connectors_dir else Path(__file__).parent
        )
        self._config_path = config_path
        # Manifest ids to skip during discovery. The host excludes the seed
        # connectors (snowflake/github) so the register(api) seeds own those tool
        # names instead of colliding with the old Connector subclasses.
        self._exclude = set(exclude or ())
        #: when on, an operation that declares no post-condition cannot
        # register — "no post-condition, no acceptance" enforced at load time.
        # Default off so isolated test rigs / throwaway connectors are unaffected;
        # the live agent turns it on.
        self._enforce_postconditions = enforce_postconditions

        # id -> manifest dict
        self._manifests: dict[str, dict[str, Any]] = {}
        # id -> Connector instance
        self._connectors: dict[str, Connector] = {}
        # ids that are always-on injected connectors (e.g. system)
        self._builtin_ids: set = set()
        # connector_id/dir -> reason, for connectors that failed to load
        self._failed: dict[str, str] = {}
        # tool_name -> (connector_id, op_name)
        self._op_index: dict[str, tuple[str, str]] = {}

        self._config = load_connectors_config(config_path)

        self._discover()
        self._inject(extra_connectors or [])
        self._build_index()
        if self._enforce_postconditions:
            self.validate_postconditions()

    # ------------------------------------------------------------------
    # Discovery / construction
    # ------------------------------------------------------------------
    def _discover(self) -> None:
        # Scan immediate subdirectories for a manifest.yaml.
        #
        # Discovery is fault-isolated: a single broken connector (bad manifest,
        # import error, or an ``operations()`` that raises) is recorded in
        # ``self._failed`` and skipped, never crashing registry construction.
        # This matters because LLM-generated connectors are untrusted code that
        # may not import cleanly — one bad apple must not take the agent down.
        for manifest_path in sorted(self._connectors_dir.glob("*/manifest.yaml")):
            try:
                manifest = _load_yaml(manifest_path)
            except Exception as exc:
                self._failed[manifest_path.parent.name] = f"manifest parse error: {exc}"
                continue
            connector_id = manifest.get("id")
            if connector_id in self._exclude:
                continue
            class_path = manifest.get("class")
            if not connector_id or not class_path:
                self._failed[manifest_path.parent.name] = (
                    "manifest missing required 'id' or 'class'"
                )
                continue
            try:
                connector = self._instantiate(class_path)
                # Smoke-check operations() once at load so a connector whose
                # spec construction throws is rejected here, not mid-dispatch.
                connector.operations()
            except Exception as exc:
                self._failed[connector_id] = f"load error: {exc}"
                continue
            self._manifests[connector_id] = manifest
            self._connectors[connector_id] = connector

    def _instantiate(self, class_path: str) -> Connector:
        module_path, _, class_name = class_path.rpartition(".")
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        return cls(self._settings)

    def _inject(self, extra_connectors: list[Connector]) -> None:
        # Always-on connectors provided by the agent (e.g. the system connector).
        for connector in extra_connectors:
            self._connectors[connector.name] = connector
            self._builtin_ids.add(connector.name)

    def _build_index(self) -> None:
        self._op_index = {}
        for connector_id, connector in self._connectors.items():
            for spec in connector.operations():
                self._op_index[spec.name] = (connector_id, spec.name)

    def rebuild_index(self) -> None:
        """Re-scan every connector's operations into the tool-name index.

        Needed after startup connects: a connector that discovers its
        operations at ``connect()`` time (e.g. the MCP bridge) only knows its
        tools now. Idempotent and cheap for static connectors.
        """
        self._build_index()

    # ------------------------------------------------------------------
    # Enable/disable state
    # ------------------------------------------------------------------
    def is_connector_enabled(self, connector_id: str) -> bool:
        if connector_id in self._builtin_ids:
            return True
        override = self._config.get("connectors", {}).get(connector_id, {})
        if "enabled" in override:
            return bool(override["enabled"])
        return bool(self._manifests.get(connector_id, {}).get("enabled", False))

    def is_operation_enabled(self, tool_name: str) -> bool:
        entry = self._op_index.get(tool_name)
        if not entry:
            return False
        connector_id, op_name = entry
        if connector_id in self._builtin_ids:
            return True
        if not self.is_connector_enabled(connector_id):
            return False
        override = self._config.get("connectors", {}).get(connector_id, {})
        ops_override = override.get("operations")
        if ops_override is not None:
            return bool(ops_override.get(op_name, False))
        # Connector enabled with no op-level override -> all ops on.
        return True

    @property
    def setup_completed(self) -> bool:
        return bool(self._config.get("setup_completed", False))

    # ------------------------------------------------------------------
    # LLM-facing surface
    # ------------------------------------------------------------------
    def get_tool_definitions(
        self, connector_ids: Iterable[str] | None = None
    ) -> list[dict[str, Any]]:
        """Build OpenAI-style tool definitions for enabled operations.

        ``connector_ids`` is the progressive-disclosure selector:

        - ``None`` (default) → full schemas for *all* enabled connectors. This
          preserves every existing caller's behavior.
        - a set/iterable of ids → full schemas only for those connectors
          (plus always-on built-ins like ``system``, which are never gated so
          the disclosure meta-tool is always callable).

        The point of the selector is token economy: at 12 connectors we disclose
        ~120 schemas only when a connector is actually selected, instead of every
        turn.
        """
        selected = set(connector_ids) if connector_ids is not None else None
        tools: list[dict[str, Any]] = []
        for connector_id in self._ordered_ids():
            if not self.is_connector_enabled(connector_id):
                continue
            # Built-ins (system) are always disclosed; otherwise honor the
            # selector when one was supplied.
            if (
                selected is not None
                and connector_id not in self._builtin_ids
                and connector_id not in selected
            ):
                continue
            tools.extend(
                spec.to_tool_definition()
                for spec in self._connectors[connector_id].operations()
                if self.is_operation_enabled(spec.name)
            )
        return tools

    def get_tool_digest(self) -> list[dict[str, Any]]:
        """Cheap name + one-line description for every enabled connector.

        This is the progressive-disclosure surface: the system
        prompt lists connectors by ``id``, ``name`` and a short blurb so the
        model knows a capability *exists* without paying for its full operation
        schemas. Full schemas are fetched via :meth:`get_tool_definitions` only
        once the connector is disclosed. Built-ins are excluded — their tools are
        always live in the prompt.
        """
        digest: list[dict[str, str]] = []
        for connector_id in self._ordered_ids():
            if connector_id in self._builtin_ids:
                continue
            if not self.is_connector_enabled(connector_id):
                continue
            manifest = self._manifests.get(connector_id, {})
            op_count = sum(
                1
                for spec in self._connectors[connector_id].operations()
                if self.is_operation_enabled(spec.name)
            )
            digest.append(
                {
                    "id": connector_id,
                    "name": manifest.get("name", connector_id),
                    "description": manifest.get("description", ""),
                    "operations": op_count,
                }
            )
        return digest

    def resolve(self, tool_name: str) -> tuple[Connector, str] | None:
        """Resolve an LLM tool name to (connector instance, op name)."""
        entry = self._op_index.get(tool_name)
        if not entry:
            return None
        connector_id, op_name = entry
        return self._connectors[connector_id], op_name

    def validate_postconditions(self) -> None:
        """Reject any registered operation that declares no post-condition.

        This is the structural enforcement of the rule: a connector
        operation cannot be offered unless its outcome can be checked. Raises
        ``MissingPostConditionError`` naming the first offender.
        """
        from dacli.core.verify import require_postconditions

        for connector_id, connector in self._connectors.items():
            for spec in connector.operations():
                require_postconditions(
                    f"{connector_id}.{spec.name}", getattr(spec, "postconditions", None)
                )

    def get_operation_spec(self, tool_name: str):
        """Return the OperationSpec for a tool name (for risk-aware dispatch)."""
        entry = self._op_index.get(tool_name)
        if not entry:
            return None
        connector_id, op_name = entry
        for spec in self._connectors[connector_id].operations():
            if spec.name == op_name:
                return spec
        return None

    def _ordered_ids(self) -> list[str]:
        # Discovered connectors first (sorted for determinism), built-ins last.
        discovered = sorted(
            cid for cid in self._connectors if cid not in self._builtin_ids
        )
        builtins = sorted(self._builtin_ids)
        return discovered + builtins

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------
    def enabled_connectors(self) -> list[Connector]:
        """Connectors that should be connected at startup (excludes built-ins)."""
        return [
            self._connectors[cid]
            for cid in self._ordered_ids()
            if cid not in self._builtin_ids and self.is_connector_enabled(cid)
        ]

    def all_connectors(self) -> list[Connector]:
        return [self._connectors[cid] for cid in self._ordered_ids()]

    def get_connector(self, connector_id: str) -> Connector | None:
        return self._connectors.get(connector_id)

    def is_builtin(self, connector_id: str) -> bool:
        """True for always-on injected connectors (system, skill, sandbox, …)."""
        return connector_id in self._builtin_ids

    def failed_connectors(self) -> dict[str, str]:
        """Connectors that failed to load, mapped to the reason they were skipped.

        Surfaced so the user can see *why* a (e.g. freshly generated) connector
        didn't register, instead of it silently vanishing.
        """
        return dict(self._failed)

    # ------------------------------------------------------------------
    # Catalog for the setup wizard (metadata for ALL connectors)
    # ------------------------------------------------------------------
    def get_catalog(self) -> dict[str, dict[str, Any]]:
        catalog: dict[str, dict[str, Any]] = {}
        for connector_id in self._ordered_ids():
            if connector_id in self._builtin_ids:
                continue
            manifest = self._manifests.get(connector_id, {})
            ops_meta: dict[str, dict[str, Any]] = {}
            for spec in self._connectors[connector_id].operations():
                ops_meta[spec.name] = {
                    "name": spec.display_name or spec.name,
                    "description": spec.description,
                    "category": spec.category or "",
                    "risk": spec.risk.value,
                }
            catalog[connector_id] = {
                "id": connector_id,
                "name": manifest.get("name", connector_id),
                "description": manifest.get("description", ""),
                "icon": manifest.get("icon", ""),
                "required_config": manifest.get("required_config", []),
                "manifest_enabled": bool(manifest.get("enabled", False)),
                "operations": ops_meta,
            }
        return catalog

    def get_manifest(self, connector_id: str) -> dict[str, Any]:
        return self._manifests.get(connector_id, {})

    # ------------------------------------------------------------------
    # Config field introspection (for /connect flow)
    # ------------------------------------------------------------------
    def get_config_fields(self, connector_id: str) -> list[ConfigField]:
        """Describe a connector's config fields (name, type, required, default,
        is_secret, description) for the ``/connect`` flow.

        A connector declares its fields in ``manifest.yaml`` (``config_fields``),
        read via :meth:`_config_fields_from_manifest`. The old per-connector
        ``Settings`` section is gone (M12) — there's no typed branch anymore.
        """
        return self._config_fields_from_manifest(connector_id)

    def _config_fields_from_manifest(self, connector_id: str) -> list[ConfigField]:
        """Build config fields from a manifest's ``config_fields`` list.

        Each entry is a mapping: ``name`` (required), plus optional ``type``,
        ``required``, ``default``, ``secret``/``is_secret``, and ``description``.
        Falls back to ``required_config`` (a bare list of field names) when no
        rich ``config_fields`` is present, treating each as a required field and
        inferring secrecy from its name.
        """
        manifest = self._manifests.get(connector_id, {})
        raw = manifest.get("config_fields")
        fields: list[ConfigField] = []
        if isinstance(raw, list) and raw:
            for entry in raw:
                if not isinstance(entry, dict) or not entry.get("name"):
                    continue
                fname = str(entry["name"])
                is_secret = bool(
                    entry.get("secret", entry.get("is_secret", False))
                ) or fname.lower() in _SECRET_FIELD_NAMES
                fields.append(
                    ConfigField(
                        name=fname,
                        field_type=str(entry.get("type", "str")),
                        required=bool(entry.get("required", False)),
                        default="" if is_secret else entry.get("default", ""),
                        is_secret=is_secret,
                        description=str(entry.get("description", "")),
                    )
                )
            return fields
        # Fallback: a bare required_config name list.
        for fname in manifest.get("required_config", []) or []:
            fname = str(fname)
            is_secret = fname.lower() in _SECRET_FIELD_NAMES
            fields.append(
                ConfigField(
                    name=fname,
                    required=True,
                    is_secret=is_secret,
                )
            )
        return fields

    def get_connector_ids(self) -> list[str]:
        """All discovered connector ids (excluding built-ins)."""
        return [cid for cid in self._ordered_ids() if cid not in self._builtin_ids]
