# Configuration

dacli reads a `config.yaml` and overlays secrets from environment variables (`${VAR}` placeholders), so
credentials never live in the config file. Connector **enablement** lives separately in
`config/connectors.yaml` (written by the setup wizard), and governance **policy** in `config/policy.yaml`.

```bash
cp config_template.yaml config.yaml
cp .env.example .env
```

## Resolution order

1. `config.yaml` (searched: `./config.yaml`, then `~/.dacli/config.yaml`).
2. `${VAR}` placeholders resolved from the environment / `.env`.
3. Any field still empty is filled from the `secrets` block of `.dacli/dacli.json` (written by the wizard).

Git-ignored (they hold local state): `config.yaml`, `.env`, `config/connectors.yaml`, and the `.dacli/`
runtime directory.

---

## Sections

### `llm` — Reasoning (ℛ)

```yaml
llm:
  provider: "openrouter"          # openai | anthropic | google | openrouter
  model: "x-ai/grok-4.1-fast"
  fallback_model: "x-ai/grok-4.1-fast"
  cheap_model: null               # optional cheap tier (classification, summaries)
  strong_model: null              # optional strong tier (diagnosis, irreversible plans)
  api_key: "${LLM_API_KEY}"
  base_url: "https://openrouter.ai/api/v1"
  temperature: 0.1
  max_tokens: 4096
  timeout: 120
```

`cheap_model` / `strong_model` enable **model tiering**; both default to `model`, so a single-model config
behaves exactly as before.

### Platform credentials

Each connector has its own section; all are optional until you enable the connector. Examples:

```yaml
snowflake:   { account: ..., user: ..., password: "${SNOWFLAKE_PASSWORD}", warehouse: ..., database: ..., schema: ..., role: ... }
github:      { token: "${GITHUB_TOKEN}", owner: ..., repo: ..., branch: main }
bigquery:    { project: ..., dataset: ..., location: US, bq_binary: bq }
databricks:  { host: ..., token: "${DATABRICKS_TOKEN}", warehouse_id: ..., catalog: ..., schema: default }
s3:          { bucket: ..., region: ..., profile: "", aws_binary: aws }
gcs:         { bucket: ..., project: ..., gcloud_binary: gcloud }
dbt:         { project_dir: ..., profiles_dir: ..., target: ..., dbt_binary: dbt }
postgres:    { host: ..., port: 5432, database: ..., user: ..., password: ..., psql_binary: psql }
mysql:       { host: ..., port: 3306, database: ..., user: ..., password: ..., mysql_binary: mysql }
mongodb:     { uri: ..., database: ..., mongosh_binary: mongosh }
dynamodb:    { region: ..., profile: "", aws_binary: aws }
airflow:     { base_url: ..., username: ..., password: ..., token: "" }
dagster:     { base_url: ..., token: "" }
pinecone:    { api_key: "${PINECONE_API_KEY}", index_name: ..., environment: ... }
embeddings:  { provider: openai, api_key: "${OPENAI_API_KEY}", model: text-embedding-3-small }
```

CLI-first connectors take a `*_binary` override if the CLI isn't named the default on your PATH.

### `agent` — session behavior

```yaml
agent:
  max_iterations: 100
  memory_window: 25
  auto_approve_safe_ops: false
  confirm_data_loads: true
  confirm_destructive_ops: true
  history_path: ".dacli/history/"
  state_path: ".dacli/state/"
```

### `context` — Context constructor (𝒞)

```yaml
context:
  budget_tokens: 12000            # total token budget per assembled turn
  spill_threshold_tokens: 1000    # tool results above this spill to disk + summary
  source_fractions: {}            # per-source ceilings (priors/memory/live/skills/history)
  compaction_pressure: 0.9        # fraction of budget that triggers history compaction
```

### `governance` — Governance (𝒢)

```yaml
governance:
  enabled: true                   # gate every state-changing action (disable only for trusted offline runs)
  policy_path: "config/policy.yaml"
  audit_path: null                # defaults to <state_dir>/audit.jsonl
  default_scope: read_only        # least-privilege scope when a profile declares none
  shadow_execution: true          # run risky transforms on a clone + diff before promoting
```

The tier → decision table and per-connector/per-environment overrides live in `config/policy.yaml`.
See [GOVERNANCE.md](GOVERNANCE.md).

### `sandbox` — code-execution tier

```yaml
sandbox:
  enabled: true
  workdir: ".dacli/sandbox/"
  wall_clock_seconds: 300
  max_memory_mb: 1024
  max_output_chars: 20000
  network: "allowlist"            # off | allowlist | open
  egress_allowlist: []
```

### `orchestration` — Orchestration & multi-agent (𝒪)

```yaml
orchestration:
  enabled: true
  complexity_gate: 2              # goals decomposing into ≥ this many subtasks use the DAG planner
  correction_budget: 2            # bounded informed self-correction attempts before escalating
  subagents_enabled: true
  max_subagents: 6
  subagent_summary_tokens: 2000
  require_plan_approval: true     # present the DAG for approval before executing
```

### `ui` / `retry`

```yaml
ui:    { theme: dark, syntax_highlighting: true, table_format: grid, max_width: 120, truncate_output: 5000 }
retry: { max_attempts: 3, initial_delay: 1.0, max_delay: 30.0, multiplier: 2.0 }
```

Themes: `dark`, `light`, `ocean`, `mono` (switch live with `/theme <name>`).

---

## Enabling connectors

Connector enable/disable state is **not** in `config.yaml` — it's in `config/connectors.yaml`, managed by the
wizard:

```bash
dacli setup                      # interactive
dacli setup --profile full       # enable everything configured
dacli setup --profile github_only
dacli setup --profile none
```

Inspect what's active at runtime with `/tools`, and the assembled context with `dacli context --explain`.
