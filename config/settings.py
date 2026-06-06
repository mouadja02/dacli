import re
import os
import yaml
from pathlib import Path
from urllib.parse import urlparse
from typing import Any, Dict, Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def _substitute_env_vars(value: Any) -> Any:
    # Recursively substitute environment variables in config values.
    if isinstance(value, str):
        # Match the  ${VAR_NAME} pattern
        pattern = r"\$\{([^}]+)\}"
        return re.sub(pattern, lambda match: os.environ.get(match.group(1), ""), value)
    elif isinstance(value, dict):
        return {k: _substitute_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_substitute_env_vars(x) for x in value]
    return value


class LLMSettings(BaseModel):
    # LLM provider configuration
    provider: str
    model: str
    fallback_model: Optional[str] = None
    # Model tiering (ℛ). ``cheap_model`` runs classification, planning
    # drafts, summaries and post-condition judgments; ``strong_model`` runs
    # ambiguous reasoning, error diagnosis and irreversible-action plans. Both
    # default to ``model`` so a single-model config behaves exactly as before.
    cheap_model: Optional[str] = None
    strong_model: Optional[str] = None
    api_key: str
    base_url: str
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
        default=120, ge=1, description="Timeout in seconds for LLM requests"
    )


class GithubSettings(BaseModel):
    # Github configuration
    token: str
    repository_url: Optional[str] = None
    owner: str = ""
    repo: str = ""
    branch: str = "main"
    timeout: int = Field(
        default=60, ge=1, description="Timeout in seconds for Github requests"
    )
    workflow_timeout: int = Field(
        default=600, ge=30, description="Timeout in seconds for Github workflow runs"
    )

    @model_validator(mode="before")
    def derive_owner_repo(cls, data: Any) -> Any:
        # Auto-derive the owner and repo from the repository URL if not provided
        if isinstance(data, dict):
            url = data.get("repository_url", "")
            if url:
                parts = urlparse(url).path.strip("/").split("/")
                if len(parts) >= 2:
                    if not data.get("owner"):
                        data["owner"] = parts[0]
                    if not data.get("repo"):
                        data["repo"] = parts[1].replace(".git", "")
        return data


class SnowflakeSettings(BaseModel):
    # Snowflake connection configuration
    account: str
    user: str
    password: str
    warehouse: str
    role: str
    database: str
    db_schema: str = Field(default="PUBLIC", alias="schema")
    query_timeout: int = Field(default=300, ge=1)
    login_timeout: int = Field(default=60, ge=1)
    network_timeout: int = Field(default=60, ge=1)

    model_config = ConfigDict(populate_by_name=True)


class PineconeSettings(BaseModel):
    # Pinecone vector store configuration
    api_key: str
    index_name: str
    environment: str
    top_k: int = Field(default=5, ge=1, le=100)
    include_metadata: bool = True


class EmbeddingsSettings(BaseModel):
    # Embeddings configuration for Pinecone
    provider: str
    api_key: str
    model: str


class BigQuerySettings(BaseModel):
    # BigQuery configuration (Wave 1). CLI-first: the `bq` / `gcloud`
    # CLIs are preferred. All fields optional so the connector can be discovered
    # (and the agent can boot) before BigQuery is configured.
    project: str = ""
    dataset: str = ""
    location: str = "US"
    credentials_path: str = ""  # GOOGLE_APPLICATION_CREDENTIALS service-account JSON
    bq_binary: str = "bq"
    timeout: int = Field(default=300, ge=1)


class DatabricksSettings(BaseModel):
    # Databricks configuration (Wave 1). CLI-first via the `databricks`
    # CLI; SQL runs against a SQL warehouse.
    host: str = ""  # https://<workspace>.cloud.databricks.com
    token: str = ""
    warehouse_id: str = ""
    catalog: str = ""
    db_schema: str = Field(default="default", alias="schema")
    databricks_binary: str = "databricks"
    timeout: int = Field(default=300, ge=1)

    model_config = ConfigDict(populate_by_name=True)


class S3Settings(BaseModel):
    # S3 / object-store configuration (Wave 1). CLI-first via `aws s3`.
    bucket: str = ""
    prefix: str = ""
    region: str = ""
    profile: str = ""  # named AWS profile, optional
    aws_binary: str = "aws"
    timeout: int = Field(default=300, ge=1)


class GCSSettings(BaseModel):
    # Google Cloud Storage configuration (Wave 1). CLI-first via
    # `gcloud storage` (falls back to `gsutil` shape).
    bucket: str = ""
    prefix: str = ""
    project: str = ""
    credentials_path: str = ""
    gcloud_binary: str = "gcloud"
    timeout: int = Field(default=300, ge=1)


class PostgresSettings(BaseModel):
    # PostgreSQL configuration (Wave 2). CLI-first via `psql`.
    host: str = "localhost"
    port: int = 5432
    database: str = ""
    user: str = ""
    password: str = ""
    sslmode: str = ""  # e.g. require / verify-full
    psql_binary: str = "psql"
    timeout: int = Field(default=300, ge=1)


class MySQLSettings(BaseModel):
    # MySQL configuration (Wave 2). CLI-first via `mysql`.
    host: str = "localhost"
    port: int = 3306
    database: str = ""
    user: str = ""
    password: str = ""
    mysql_binary: str = "mysql"
    mysqldump_binary: str = "mysqldump"
    timeout: int = Field(default=300, ge=1)


class MongoDBSettings(BaseModel):
    # MongoDB configuration (Wave 2). CLI-first via `mongosh`.
    uri: str = ""  # mongodb://… connection string
    database: str = ""
    sample_size: int = Field(default=100, ge=1)  # docs sampled for schema inference
    mongosh_binary: str = "mongosh"
    timeout: int = Field(default=300, ge=1)


class DynamoDBSettings(BaseModel):
    # DynamoDB configuration (Wave 2). CLI-first via `aws dynamodb`.
    region: str = ""
    profile: str = ""
    aws_binary: str = "aws"
    timeout: int = Field(default=300, ge=1)


class AirflowSettings(BaseModel):
    # Airflow configuration (Wave 3). REST API (stable v1).
    base_url: str = ""  # e.g. https://airflow.example.com
    username: str = ""
    password: str = ""
    token: str = ""  # bearer token (alternative to basic auth)
    poll_interval: int = Field(default=5, ge=1)
    timeout: int = Field(default=600, ge=1)


class DagsterSettings(BaseModel):
    # Dagster configuration (Wave 3). GraphQL API.
    base_url: str = ""  # e.g. https://dagster.example.com
    token: str = ""
    poll_interval: int = Field(default=5, ge=1)
    timeout: int = Field(default=600, ge=1)


class DbtSettings(BaseModel):
    # dbt configuration (Wave 1). CLI-first via the `dbt` CLI.
    project_dir: str = ""  # path to the dbt project (dbt_project.yml lives here)
    profiles_dir: str = ""  # path to profiles.yml (defaults to ~/.dbt)
    target: str = ""  # dbt target/profile output, optional
    dbt_binary: str = "dbt"
    timeout: int = Field(default=900, ge=1)


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
    audit_path: Optional[str] = Field(
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


class OrchestrationSettings(BaseModel):
    # Orchestration & multi-agent (𝒪 / ℛ) configuration.
    enabled: bool = Field(
        default=True,
        description="Allow the planner→act→observe→verify orchestrator for multi-step goals. When off, every message runs the single-step kernel loop.",
    )
    complexity_gate: int = Field(
        default=2,
        ge=1,
        description="A goal that decomposes into this many or more subtasks goes through the DAG planner; simpler goals run single-step (avoids planner ceremony on trivial work).",
    )
    correction_budget: int = Field(
        default=2,
        ge=0,
        description="Bounded, feedback-driven self-correction attempts on a failed post-condition before escalating to a human with the full trail.",
    )
    subagents_enabled: bool = Field(
        default=True,
        description="Allow the lead to fan breadth-first work out to isolated-context sub-agents. Opt-in per task via the planner's breadth-first detection.",
    )
    max_subagents: int = Field(
        default=6,
        ge=1,
        description="Maximum parallel sub-agents the lead spawns for one breadth-first node (caps token blow-up).",
    )
    subagent_summary_tokens: int = Field(
        default=2000,
        ge=128,
        description="Token ceiling for the condensed summary a sub-agent returns to the lead (keeps total context bounded).",
    )
    require_plan_approval: bool = Field(
        default=True,
        description="Present the DAG for human approval before executing (the plan-approve-execute posture from).",
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


class RetrySettings(BaseModel):
    # Retry and error handling configuration
    max_attempts: int = Field(default=3, ge=1)
    initial_delay: float = Field(default=1.0, ge=0.1)
    max_delay: float = Field(default=30.0, ge=1.0)
    multiplier: float = Field(default=2.0, ge=1.0)


class Settings(BaseModel):
    # Main settings container
    model_config = ConfigDict(extra="ignore")

    llm: LLMSettings = Field(default_factory=LLMSettings)
    snowflake: SnowflakeSettings = Field(default_factory=SnowflakeSettings)
    github: GithubSettings = Field(default_factory=GithubSettings)
    pinecone: PineconeSettings = Field(default_factory=PineconeSettings)
    # Wave 1 platforms (all optional; CLI-first).
    bigquery: BigQuerySettings = Field(default_factory=BigQuerySettings)
    databricks: DatabricksSettings = Field(default_factory=DatabricksSettings)
    s3: S3Settings = Field(default_factory=S3Settings)
    gcs: GCSSettings = Field(default_factory=GCSSettings)
    dbt: DbtSettings = Field(default_factory=DbtSettings)
    # Wave 2 operational databases (all optional; CLI-first).
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    mysql: MySQLSettings = Field(default_factory=MySQLSettings)
    mongodb: MongoDBSettings = Field(default_factory=MongoDBSettings)
    dynamodb: DynamoDBSettings = Field(default_factory=DynamoDBSettings)
    # Wave 3 — orchestration.
    airflow: AirflowSettings = Field(default_factory=AirflowSettings)
    dagster: DagsterSettings = Field(default_factory=DagsterSettings)
    embeddings: EmbeddingsSettings = Field(default_factory=EmbeddingsSettings)
    agent: AgentSettings = Field(default_factory=AgentSettings)
    context: ContextSettings = Field(default_factory=ContextSettings)
    governance: GovernanceSettings = Field(default_factory=GovernanceSettings)
    sandbox: SandboxSettings = Field(default_factory=SandboxSettings)
    terminal: TerminalSettings = Field(default_factory=TerminalSettings)
    orchestration: OrchestrationSettings = Field(default_factory=OrchestrationSettings)
    ui: UISettings = Field(default_factory=UISettings)
    retry: RetrySettings = Field(default_factory=RetrySettings)

    # NOTE: connector enable/disable state lives in config/connectors.yaml
    # (see connectors.registry), not here. A legacy top-level ``tools:`` block in
    # an old config.yaml is harmlessly ignored via ``extra="ignore"`` above.


def _is_secret_placeholder(v: Any) -> bool:
    # A field is "missing" if empty or still an unresolved ${ENV_VAR} reference.
    return v is None or v == "" or (isinstance(v, str) and v.startswith("${"))


def _dacli_base_dir(config_data: Dict[str, Any]) -> str:
    # Resolve through core.crypto's single source of truth so the secrets store
    # and the encryption key always agree on their directory (see resolve_base_dir).
    from core.crypto import resolve_base_dir

    agent = config_data.get("agent")
    cfg_state_path = agent.get("state_path") if isinstance(agent, dict) else None
    # Mirror the store exactly: the store's base dir comes from
    # ``settings.agent.state_path`` (config value, else the model default
    # ``.dacli/state/``) — env-independent — so we pass the same default rather
    # than letting resolve_base_dir fall through to DACLI_STATE_PATH here.
    return str(resolve_base_dir(cfg_state_path or ".dacli/state/"))


def _load_dacli_secrets(base_dir: str) -> Dict[str, Any]:
    # Read the `secrets` block from .dacli/dacli.json (written by the setup wizard).
    # Values are Fernet-encrypted; decrypt them here so the overlay fills config
    # fields with plaintext credentials.
    import json
    from core.crypto import (
        CredentialDecryptionError,
        decrypt_value,
        surface_decryption_failures,
    )

    try:
        data = json.loads((Path(base_dir) / "dacli.json").read_text(encoding="utf-8"))
        raw = data.get("secrets")
        if not isinstance(raw, dict):
            return {}
        decrypted: Dict[str, Any] = {}
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
        return {}


def _overlay_secrets(
    config_data: Dict[str, Any], secrets: Dict[str, Any]
) -> Dict[str, Any]:
    """Fill missing/placeholder config fields from the dacli.json secrets block.

    Explicit values from config.yaml / env take precedence; dacli.json only fills
    holes (empty or unresolved ``${VAR}``), so wizard-stored credentials make the
    agent work without a .env file.
    """
    for section, fields in secrets.items():
        if not isinstance(fields, dict):
            continue
        sec = config_data.get(section)
        if not isinstance(sec, dict):
            continue
        for field, val in fields.items():
            if val and _is_secret_placeholder(sec.get(field)):
                sec[field] = val
    return config_data


def load_config(config_path: Optional[str] = None) -> Settings:
    """
    Load configuration from YAML file with environment variable substitution.

    Args:
        config_path: Path to config.yaml file. If None, searches in:
                    1. ./config.yaml
                    2. ~/.dacli/config.yaml
                    3. Uses defaults

    Returns:
        Settings object with all configuration
    """
    search_paths = [
        Path("config.yaml"),
        Path.home() / ".dacli" / "config.yaml",
    ]

    if config_path:
        search_paths.insert(0, Path(config_path))

    config_file = None
    for path in search_paths:
        if path.exists():
            config_file = path
            break

    if config_file is None:
        # Return default settings
        return Settings()

    # Load YAML
    with open(config_file, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)
    if raw_config is None:
        return Settings()

    # Substitute environment variables
    config_data = _substitute_env_vars(raw_config)

    # Overlay credentials stored in .dacli/dacli.json (the setup wizard writes
    # them there); these fill any field left missing/placeholder by config+env.
    if isinstance(config_data, dict):
        secrets = _load_dacli_secrets(_dacli_base_dir(config_data))
        config_data = _overlay_secrets(config_data, secrets)

    return Settings(**config_data)


def save_config(settings: Settings, config_path: str = "config.yaml") -> None:
    # Save settings to YAML file
    config_dict = settings.model_dump()

    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
