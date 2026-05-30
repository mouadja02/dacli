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
        pattern= r"\$\{([^}]+)\}"
        return re.sub(pattern, lambda match: os.environ.get(match.group(1), ""), value)
    elif isinstance(value, dict):
        return {k: _substitute_env_vars(v) for k,v in value.items()}
    elif isinstance(value, list):
        return [_substitute_env_vars(x) for x in value]
    return value


class LLMSettings(BaseModel):
    # LLM provider configuration
    provider: str
    model: str
    fallback_model: Optional[str] = None
    api_key: str
    base_url: str
    max_tokens: int = Field(default=4096, ge=1, description="Maximum number of tokens to generate")
    temperature: float = Field(default=0.7, ge=0.0, le=2.0, description="Controls randomness: Lowering results in less random completions. As the temperature approaches zero, the model will become deterministic and repetitive.")
    top_p: float = Field(default=1.0, ge=0.0, le=1.0, description="Controls diversity via nucleus sampling: 0.5 means half of all likelihood-weighted options are considered. We generally recommend altering this or temperature but not both.")
    presence_penalty: float = Field(default=0.0, ge=-2.0, le=2.0, description="Positive values penalize new tokens based on whether they appear in the text so far, increasing the model's likelihood to talk about new topics")
    frequency_penalty: float = Field(default=0.0, ge=-2.0, le=2.0, description="Positive values penalize new tokens based on their existing frequency in the text so far, decreasing the model's likelihood to repeat the same line verbatim")
    timeout: int = Field(default=120, ge=1, description="Timeout in seconds for LLM requests")
    

class GithubSettings(BaseModel):
    # Github configuration
    token: str
    repository_url: Optional[str] = None
    owner: str = ""
    repo: str = ""
    branch: str = "main"
    timeout: int = Field(default=60, ge=1, description="Timeout in seconds for Github requests")
    workflow_timeout: int = Field(default=600, ge=30, description="Timeout in seconds for Github workflow runs")
    
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


    class Config:
        populate_by_name = True


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
    # Context Constructor (Phase 3) configuration.
    budget_tokens: int = Field(default=12000, ge=512, description="Total token budget for one assembled turn of context")
    spill_threshold_tokens: int = Field(default=1000, ge=0, description="Tool results estimated above this many tokens are spilled to the session workspace and replaced with a structured summary + fetch handle")
    # Per-source fractional ceilings of the total budget (priors/memory/live/
    # skills/history). Empty -> the assembler's DEFAULT_FRACTIONS are used.
    source_fractions: dict = Field(default_factory=dict)
    # Fraction of the total at which history compaction is triggered by pressure.
    compaction_pressure: float = Field(default=0.9, ge=0.1, le=1.0)


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
    embeddings: EmbeddingsSettings = Field(default_factory=EmbeddingsSettings)
    agent: AgentSettings = Field(default_factory=AgentSettings)
    context: ContextSettings = Field(default_factory=ContextSettings)
    ui: UISettings = Field(default_factory=UISettings)
    retry: RetrySettings = Field(default_factory=RetrySettings)

    # NOTE: connector enable/disable state lives in config/connectors.yaml
    # (see connectors.registry), not here. A legacy top-level ``tools:`` block in
    # an old config.yaml is harmlessly ignored via ``extra="ignore"`` above.


def _is_secret_placeholder(v: Any) -> bool:
    # A field is "missing" if empty or still an unresolved ${ENV_VAR} reference.
    return v is None or v == "" or (isinstance(v, str) and v.startswith("${"))


def _dacli_base_dir(config_data: Dict[str, Any]) -> str:
    state_path = ".dacli/state/"
    agent = config_data.get("agent")
    if isinstance(agent, dict) and agent.get("state_path"):
        state_path = agent["state_path"]
    return str(Path(state_path).parent)


def _load_dacli_secrets(base_dir: str) -> Dict[str, Any]:
    # Read the `secrets` block from .dacli/dacli.json (written by the setup wizard).
    import json

    try:
        data = json.loads((Path(base_dir) / "dacli.json").read_text(encoding="utf-8"))
        secrets = data.get("secrets")
        return secrets if isinstance(secrets, dict) else {}
    except Exception:
        return {}


def _overlay_secrets(config_data: Dict[str, Any], secrets: Dict[str, Any]) -> Dict[str, Any]:
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
