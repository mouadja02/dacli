import re
import os
import yaml
from pathlib import Path
from typing import Any
from pydantic import BaseModel, ConfigDict, Field, field_validator
from dotenv import load_dotenv


def _load_env_files() -> None:
    """Load ``.env`` from the resolved project/config dir, not raw cwd.

    A global CLI inherits whatever dir the user ``cd``s into; a raw cwd
    ``load_dotenv()`` would let an attacker-controlled ``.env`` there inject env
    (e.g. ``OPENAI_BASE_URL`` → a credential-harvesting proxy) that then satisfies
    ``${VAR}`` substitutions. So load only from a recognised project root (P01),
    falling back to the per-user config dir — never an arbitrary cwd. Gate the
    whole thing behind ``DACLI_USE_DOTENV`` (default on, set ``0`` to disable).
    """
    if os.environ.get("DACLI_USE_DOTENV", "1").strip().lower() in ("0", "false", "no"):
        return
    from dacli.core import paths

    root = paths.project_root()
    target = (root if root is not None else paths.user_config_dir()) / ".env"
    if target.exists():
        load_dotenv(target)


_load_env_files()


def _substitute_env_vars(value: Any, unresolved: set[str] | None = None) -> Any:
    # Recursively substitute environment variables in config values. A missing
    # ${VAR} still becomes "" (unchanged behaviour); its name is recorded in
    # ``unresolved`` (when provided) so load_config can warn once.
    if isinstance(value, str):
        # Match the  ${VAR_NAME} pattern
        pattern = r"\$\{([^}]+)\}"

        def _replace(match: re.Match[str]) -> str:
            name = match.group(1)
            resolved = os.environ.get(name)
            if resolved is None:
                if unresolved is not None:
                    unresolved.add(name)
                return ""
            return resolved

        return re.sub(pattern, _replace, value)
    if isinstance(value, dict):
        return {k: _substitute_env_vars(v, unresolved) for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute_env_vars(x, unresolved) for x in value]
    return value


#: Env-var names already reported by :func:`_warn_unresolved_env_vars`, so
#: repeated load_config calls don't re-spam the same warning (mirrors
#: ``core.crypto._warned_secrets``).
_warned_env_vars: set = set()


def _warn_unresolved_env_vars(names: set[str]) -> str | None:
    """Report, exactly once, ``${VAR}`` references with no matching env var.

    Non-fatal: the fields were substituted with "" (a user may intentionally
    leave some unset), but a typo'd name would otherwise fail opaquely
    platform-side as an empty credential. Mirrors the decryption-failure UX in
    ``core.crypto.surface_decryption_failures``.
    """
    import sys

    new = sorted(n for n in names if n and n not in _warned_env_vars)
    if not new:
        return None
    _warned_env_vars.update(new)
    msg = (
        "Unset environment variables referenced in config: "
        + ", ".join(new)
        + " — those fields will be empty."
    )
    try:
        from dacli.core.logging_setup import get_logger

        get_logger(__name__).warning("unset env vars in config: %s", ", ".join(new))
    except Exception:
        pass  # silent-swallow-ok: can't log that logging failed; stderr print below still fires
    print(msg, file=sys.stderr)
    return msg


class LLMSettings(BaseModel):
    # LLM provider configuration. All fields default to clearly-empty values so
    # ``Settings()`` is always constructible; "unconfigured" is a detectable
    # value (see ``is_llm_configured``), not a ValidationError.
    provider: str = "openai"
    model: str = ""
    fallback_model: str | None = None
    api_key: str = ""
    base_url: str = ""
    max_tokens: int = Field(
        default=4096, ge=1, description="Maximum number of tokens to generate"
    )
    temperature: float = Field(
        default=0.7,
        ge=0.0,
        le=2.0,
        description="Controls randomness: Lowering results in less random completions. As the temperature approaches zero, the model will become deterministic and repetitive.",
    )
    top_p: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Controls diversity via nucleus sampling: 0.5 means half of all likelihood-weighted options are considered. We generally recommend altering this or temperature but not both.",
    )
    presence_penalty: float = Field(
        default=0.0,
        ge=-2.0,
        le=2.0,
        description="Positive values penalize new tokens based on whether they appear in the text so far, increasing the model's likelihood to talk about new topics",
    )
    frequency_penalty: float = Field(
        default=0.0,
        ge=-2.0,
        le=2.0,
        description="Positive values penalize new tokens based on their existing frequency in the text so far, decreasing the model's likelihood to repeat the same line verbatim",
    )
    timeout: int = Field(
        default=120, ge=1, description="Overall per-call timeout in seconds for LLM requests"
    )
    # Bounded retry/backoff (P05). Transient failures (429 / 5xx / dropped
    # stream) are retried with jittered exponential backoff; permanent errors
    # (auth / 4xx-validation) fail fast and are never retried.
    retry_attempts: int = Field(
        default=4,
        ge=1,
        description="Total attempts per LLM call (initial try + retries) before a transient error is surfaced.",
    )
    retry_base_delay: float = Field(
        default=0.5,
        ge=0.0,
        description="Base seconds for exponential backoff; delay ~= retry_base_delay * 2**attempt plus jitter.",
    )


class AgentSettings(BaseModel):
    # Agent configuration
    max_iterations: int = Field(default=100, ge=1)
    memory_window: int = Field(default=10, ge=1)
    auto_approve_safe_ops: bool = False
    confirm_data_loads: bool = True
    confirm_destructive_ops: bool = True
    step_by_step_mode: bool = False
    log_level: str = "INFO"
    save_history: bool = True
    history_path: str = ".dacli/history/"
    state_path: str = ".dacli/state/"

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if v.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return v.upper()


class ContextSettings(BaseModel):
    # Context Constructor configuration.
    budget_tokens: int = Field(
        default=12000,
        ge=512,
        description="Total token budget for one assembled turn of context",
    )
    spill_threshold_tokens: int = Field(
        default=1000,
        ge=0,
        description="Tool results estimated above this many tokens are spilled to the session workspace and replaced with a structured summary + fetch handle",
    )
    # Per-source fractional ceilings of the total budget (priors/memory/live/
    # skills/history). Empty -> the assembler's DEFAULT_FRACTIONS are used.
    source_fractions: dict = Field(default_factory=dict)
    # Fraction of the total at which history compaction is triggered by pressure.
    compaction_pressure: float = Field(default=0.9, ge=0.1, le=1.0)


class GovernanceSettings(BaseModel):
    # Governance (𝒢) configuration.
    enabled: bool = Field(
        default=True,
        description="Gate every state-changing action through the classifier + policy engine. Disable only for trusted offline runs.",
    )
    policy_path: str = Field(
        default="config/policy.yaml",
        description="Path to the tier->decision policy overrides (per connector/environment).",
    )
    audit_path: str | None = Field(
        default=None,
        description="Append-only audit ledger path. Defaults to <state_dir>/audit.jsonl.",
    )
    default_scope: str = Field(
        default="read_only",
        description="Least-privilege scope granted to a connector when its profile declares none: read_only | write | risky | admin.",
    )
    shadow_execution: bool = Field(
        default=True,
        description="Run risky transforms on a zero-copy clone and diff before promoting (where the connector supports it).",
    )
    cost_confirm_usd: float | None = Field(
        default=None,
        description="Cost gate: when a connector can estimate an action's cost (e.g. BigQuery dry_run bytes) and the estimate exceeds this many USD, require a human confirm. None disables the gate.",
    )


class SandboxSettings(BaseModel):
    # Code-execution sandbox configuration.
    enabled: bool = Field(
        default=True,
        description="Allow the agent to run code in the governed sandbox for complex/multi-step jobs.",
    )
    workdir: str = Field(
        default=".dacli/sandbox/",
        description="Working directory where sandbox scripts and their (off-context) data outputs live. With the docker runtime this is bind-mounted into the container at /workspace.",
    )
    wall_clock_seconds: int = Field(
        default=300, ge=1, description="Hard wall-clock limit per sandbox run."
    )
    max_memory_mb: int = Field(
        default=1024,
        ge=64,
        description="Memory ceiling per sandbox run (POSIX rlimit for the subprocess runtime; a hard --memory cap for the docker runtime).",
    )
    max_output_chars: int = Field(
        default=20000,
        ge=256,
        description="Max characters of stdout/stderr returned to model context; the rest stays on disk.",
    )
    network: str = Field(
        default="allowlist",
        description="Egress policy: 'off' (no network), 'allowlist' (only configured platform endpoints), or 'open'. Note: installing packages (pip) inside the docker runtime requires 'open' or an allowlist that covers your package index.",
    )
    egress_allowlist: list = Field(
        default_factory=list,
        description="Extra host suffixes the sandbox may reach when network='allowlist'.",
    )
    # --- runtime backend ---
    runtime: str = Field(
        default="auto",
        description="Sandbox backend: 'auto' (docker if an engine is reachable, else subprocess), 'docker' (hardened per-session container), or 'subprocess' (local in-process; weaker OS isolation).",
    )
    docker_image: str = Field(
        default="dacli-sandbox:latest",
        description="Image for the docker runtime; auto-built from sandbox/docker/Dockerfile if absent.",
    )
    docker_bin: str = Field(
        default="docker",
        description="Docker CLI binary (e.g. 'docker' or a full path / 'podman').",
    )
    docker_cpus: float = Field(
        default=2.0,
        gt=0,
        description="CPU limit (--cpus) for the per-session container.",
    )
    docker_pids_limit: int = Field(
        default=256,
        ge=16,
        description="Max process count (--pids-limit) inside the container (fork-bomb guard).",
    )
    docker_auto_build: bool = Field(
        default=True,
        description="Build the sandbox image on first use if it is not already present.",
    )


class TerminalSettings(BaseModel):
    # Governed terminal / shell tier (Era 2, Phase 1).
    #
    # A persistent PTY-wrapped shell session per data-role session, surfaced as a
    # third execution tier that flows through the *same* governance spine as the
    # tool and sandbox tiers (classify -> policy -> rollback -> audit). It is NOT
    # a parallel approval system: every command is blast-radius-classified by the
    # command classifier before it runs.
    enabled: bool = Field(
        default=True,
        description="Expose the governed shell tier (run_shell_command). Disable to forbid all terminal execution.",
    )
    shell: str = Field(
        default="auto",
        description="Backend shell: auto | cmd | powershell | wsl | zsh. 'auto' picks the platform default (PowerShell on Windows, the login shell on POSIX).",
    )
    workspace_root: str = Field(
        default=".dacli/sessions",
        description="Root under which each session gets a jailed workspace/ directory the agent owns.",
    )
    scope: str = Field(
        default="write",
        description="Least-privilege ceiling for the shell tier: read_only | write | risky | admin. Default 'write' auto-runs reads + new-file writes; widen to allow overwrite (confirm+rollback) and bring the rm -rf rollback gate into play.",
    )
    wall_clock_seconds: int = Field(
        default=120,
        ge=1,
        description="Hard per-command wall-clock limit (a hung command is interrupted).",
    )
    idle_timeout_ms: int = Field(
        default=400,
        ge=10,
        description="How long the reader waits for further output, after the command-completion sentinel, before considering a command idle/finished.",
    )
    max_output_chars: int = Field(
        default=20000,
        ge=256,
        description="Chars of a single command's output returned to model context; the full scrollback is spilled to the workspace and fetchable by command_id.",
    )
    network: str = Field(
        default="allowlist",
        description="Egress policy for shell commands: 'off' (no network egress), 'allowlist' (only listed hosts), or 'open'. Non-allowlisted egress is classified risky+ and confirmed.",
    )
    egress_allowlist: list = Field(
        default_factory=list,
        description="Host suffixes a shell command may reach when network='allowlist'.",
    )
    journal: bool = Field(
        default=True,
        description="Journal each command + outcome to the session workspace so a terminal session can be resumed (P6).",
    )


class UISettings(BaseModel):
    # UI/Display configuration
    theme: str = "dark"
    syntax_highlighting: bool = True
    show_spinners: bool = True
    show_timing: bool = True
    table_format: str = "grid"
    max_width: int = Field(default=120, ge=40)
    truncate_output: int = Field(default=5000, ge=100)
    # Cap on rows/items/fields *rendered* in the tool transcript (head + tail).
    # The underlying result data and the off-context spill are never truncated.
    max_render_rows: int = Field(default=120, ge=10)
    # Accessibility / capability knobs (P13). Safe defaults: full polish with
    # zero config; each knob degrades one aspect gracefully.
    glyphs: str = Field(default="auto", pattern="^(auto|unicode|ascii)$")
    reduced_motion: bool = False
    high_contrast: bool = False
    no_color: bool = False
    show_header: bool = False


class Settings(BaseModel):
    # Main settings container
    model_config = ConfigDict(extra="ignore")

    llm: LLMSettings = Field(default_factory=LLMSettings)
    # Connectors use the manifest-config pattern (09/A-4): non-secret config lives
    # under ``connector_config.<id>`` and is read via ``ConnectorConfig`` — no
    # typed section here. The surviving seeds (snowflake, github, shell) follow it.
    agent: AgentSettings = Field(default_factory=AgentSettings)
    context: ContextSettings = Field(default_factory=ContextSettings)
    governance: GovernanceSettings = Field(default_factory=GovernanceSettings)
    sandbox: SandboxSettings = Field(default_factory=SandboxSettings)
    terminal: TerminalSettings = Field(default_factory=TerminalSettings)
    ui: UISettings = Field(default_factory=UISettings)

    # Generic config store for manifest-declared connectors (09/A-4). Built-in
    # connectors migrate here over time; generated/third-party connectors use
    # this exclusively. Read via ``ConnectorConfig(settings, id)``; the dict is
    # untyped by design so an unknown connector never fails a config load.
    connector_config: dict[str, dict[str, Any]] = Field(default_factory=dict)

    # NOTE: connector enable/disable state lives in config/connectors.yaml
    # (see connectors.registry), not here. A legacy top-level ``tools:`` block in
    # an old config.yaml is harmlessly ignored via ``extra="ignore"`` above.


class ConnectorConfig:
    """Attribute- and dict-style read access to ``Settings.connector_config[id]``.

    The runtime counterpart of a manifest's ``config_fields`` declaration (09/A-4):
    every built-in connector reads its non-secret config from
    ``settings.connector_config.<id>`` through this thin accessor instead of a
    typed ``Settings`` section. It is fail-soft — never raising on a missing
    connector or a missing field when the caller supplies a default — mirroring
    ``core.connector_config.load_connector_config`` (the read side of the encrypted
    *secrets* store; secrets live there, non-secret config lives here).
    """

    def __init__(self, settings: Settings, connector_id: str) -> None:
        self._data: dict[str, Any] = (
            getattr(settings, "connector_config", None) or {}
        ).get(connector_id, {})

    def __getattr__(self, item: str) -> Any:
        # Guard against recursion before ``_data`` is set (e.g. during copy).
        if item == "_data":
            raise AttributeError(item)
        try:
            return self._data[item]
        except KeyError:
            raise AttributeError(item) from None

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)


def _is_secret_placeholder(v: Any) -> bool:
    # A field is "missing" if empty or still an unresolved ${ENV_VAR} reference.
    return v is None or v == "" or (isinstance(v, str) and v.startswith("${"))


def is_llm_configured(settings: Settings) -> bool:
    """True when the LLM has the minimum needed to make a call (key + model)."""
    llm = settings.llm
    return bool(llm.api_key and llm.model and not str(llm.api_key).startswith("${"))


def _dacli_base_dir(config_data: dict[str, Any]) -> str:
    # Resolve through core.crypto's single source of truth so the secrets store
    # and the encryption key always agree on their directory (see resolve_base_dir).
    from dacli.core.crypto import resolve_base_dir

    agent = config_data.get("agent")
    cfg_state_path = agent.get("state_path") if isinstance(agent, dict) else None
    # Mirror the store exactly: the store's base dir comes from
    # ``settings.agent.state_path`` (config value, else the model default
    # ``.dacli/state/``) — env-independent — so we pass the same default rather
    # than letting resolve_base_dir fall through to DACLI_STATE_PATH here.
    return str(resolve_base_dir(cfg_state_path or ".dacli/state/"))


def _load_dacli_secrets(base_dir: str) -> dict[str, Any]:
    # Read the `secrets` block from .dacli/dacli.json (written by the setup wizard).
    # Values are Fernet-encrypted; decrypt them here so the overlay fills config
    # fields with plaintext credentials.
    import json
    from dacli.core.crypto import (
        CredentialDecryptionError,
        decrypt_value,
        surface_decryption_failures,
    )

    try:
        data = json.loads((Path(base_dir) / "dacli.json").read_text(encoding="utf-8"))
        raw = data.get("secrets")
        if not isinstance(raw, dict):
            return {}
        decrypted: dict[str, Any] = {}
        undecryptable: list = []
        for section, fields in raw.items():
            if not isinstance(fields, dict):
                continue
            decrypted[section] = {}
            for field, val in fields.items():
                if not isinstance(val, str):
                    decrypted[section][field] = val
                    continue
                try:
                    decrypted[section][field] = decrypt_value(
                        val, base_dir=base_dir, name=f"{section}.{field}"
                    )
                except CredentialDecryptionError:
                    # Wrong/rotated key: leave the field out so the overlay
                    # treats it as missing, and aggregate for one clear warning
                    # instead of N opaque connector auth failures later.
                    undecryptable.append(f"{section}.{field}")
        if undecryptable:
            surface_decryption_failures(undecryptable)
        return decrypted
    except Exception:
        # Intentionally broad: a broken/missing dacli.json must never block
        # startup, but leave a breadcrumb so a lost secret overlay is debuggable.
        try:
            from dacli.core.logging_setup import get_logger

            get_logger(__name__).debug(
                "failed to load secrets overlay from %s", base_dir, exc_info=True
            )
        except Exception:
            pass  # silent-swallow-ok: can't log that logging failed
        return {}


def _overlay_secrets(
    config_data: dict[str, Any], secrets: dict[str, Any]
) -> dict[str, Any]:
    """Fill missing/placeholder config fields from the dacli.json secrets block.

    Explicit values from config.yaml / env take precedence; dacli.json only fills
    holes (empty or unresolved ``${VAR}``), so wizard-/connect-stored credentials
    make the agent work without a .env file.

    A secret whose section names a typed ``Settings`` field fills that section
    (e.g. ``llm.api_key``). A secret whose section names a *connector* on the
    manifest-config pattern (09/A-4) — i.e. anything not a typed field — lands
    under ``connector_config.<id>`` instead, where ``ConnectorConfig`` reads it.
    This is the bridge that lets ``/connect`` (which writes ``secrets.<id>.*``)
    reach a migrated connector.
    """
    typed = set(Settings.model_fields)
    for section, fields in secrets.items():
        if not isinstance(fields, dict):
            continue
        if section in typed and section != "connector_config":
            target = config_data.get(section)
            if not isinstance(target, dict):
                continue  # only fill a section the user already declared
        else:
            cc = config_data.setdefault("connector_config", {})
            if not isinstance(cc, dict):
                continue
            target = cc.setdefault(section, {})
        for field, val in fields.items():
            if val and _is_secret_placeholder(target.get(field)):
                target[field] = val
    return config_data


#: Cached Settings keyed by resolved config path. Each entry holds the file
#: mtimes the result was built from — (config_mtime, secrets_path, secrets_mtime)
#: — so a changed config.yaml *or* a wizard-written dacli.json reloads. load_config
#: runs on every startup, /connect, /setup, validate and headless run; the YAML
#: parse + secrets decrypt is otherwise repeated for no gain.
_config_cache: dict[str, tuple[float | None, str, float | None, Settings]] = {}


def invalidate_config_cache() -> None:
    """Force the next load_config to re-read from disk.

    The chat loop calls this right after a wizard/connect mutation: a sub-second
    write can land on the same mtime, and a stale Settings would hide the new
    credential from the next turn.
    """
    _config_cache.clear()


def _mtime(path: str) -> float | None:
    try:
        return os.stat(path).st_mtime
    except OSError:
        return None


def load_config(config_path: str | None = None) -> Settings:
    """
    Load configuration from YAML file with environment variable substitution.

    Search order lives in core.paths.resolve_config_path: explicit > the project
    root's config.yaml > the per-user config dir's config.yaml > built-in defaults.

    Returns:
        Settings object with all configuration
    """
    from dacli.core.paths import resolve_config_path

    config_file = resolve_config_path(config_path)
    if config_file is None:
        # No file to stat, so nothing to key a cache on; defaults are cheap.
        return Settings()

    key = str(config_file)
    config_mtime = _mtime(key)
    cached = _config_cache.get(key)
    if cached is not None:
        cfg_mtime, secrets_path, secrets_mtime, settings = cached
        if cfg_mtime == config_mtime and _mtime(secrets_path) == secrets_mtime:
            return settings

    # Load YAML
    with open(config_file, encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)
    if raw_config is None:
        return Settings()

    # Substitute environment variables, warning once about unset ${VAR} names.
    unresolved: set[str] = set()
    config_data = _substitute_env_vars(raw_config, unresolved)
    if unresolved:
        _warn_unresolved_env_vars(unresolved)

    # Overlay credentials stored in .dacli/dacli.json (the setup wizard writes
    # them there); these fill any field left missing/placeholder by config+env.
    secrets_path = ""
    if isinstance(config_data, dict):
        base_dir = _dacli_base_dir(config_data)
        secrets_path = str(Path(base_dir) / "dacli.json")
        secrets = _load_dacli_secrets(base_dir)
        config_data = _overlay_secrets(config_data, secrets)

    settings = Settings(**config_data)
    _config_cache[key] = (config_mtime, secrets_path, _mtime(secrets_path), settings)
    return settings


def save_config(settings: Settings, config_path: str = "config.yaml") -> None:
    # Save settings to YAML file
    config_dict = settings.model_dump()

    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
