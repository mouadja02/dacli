# Configuration

**The default path is the wizard, not files.** The first `dacli` run collects provider, model, and API
key interactively, encrypts secrets into `.dacli/dacli.json`, and writes connector enablement to
`config/connectors.yaml` — no `config.yaml` or `.env` required. Re-run it any time with `dacli setup`
(connectors) or `/connect` (credentials).

Everything below is the **advanced, file-based path** for power users and CI: dacli reads a
`config.yaml` and overlays secrets from environment variables (`${VAR}` placeholders), so credentials
never live in the config file. Connector **enablement** lives separately in `config/connectors.yaml`
(written by the setup wizard), and governance **policy** in `config/policy.yaml`.

```bash
dacli init          # write a commented config.yaml to start from
cp .env.example .env
```

## Resolution order

1. `config.yaml` (searched: the project root, then the per-user config dir); absent → built-in defaults.
2. `${VAR}` placeholders resolved from the environment / `.env`.
3. Any field still empty is filled from the `secrets` block of `.dacli/dacli.json` (written by the wizard).

Git-ignored (they hold local state): `config.yaml`, `.env`, `config/connectors.yaml`, and the `.dacli/`
runtime directory.

---

## Sections

### `llm` — Reasoning (ℛ)

```yaml
llm:
  provider: "openrouter"          # openai | anthropic | openrouter ("google" is declared but
                                  # rejected at configure time: Gemini lacks the tool calling dacli requires)
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

Connector config lives under one `connector_config` block, keyed by connector id (the manifest-config
pattern). Each connector's fields are declared in its `manifest.yaml` (`config_fields`); all are optional
until you enable the connector. Examples:

```yaml
connector_config:
  snowflake:   { account: ..., user: ..., password: "${SNOWFLAKE_PASSWORD}", warehouse: ..., database: ..., schema: ..., role: ... }
  github:      { token: "${GITHUB_TOKEN}", repository_url: "https://github.com/owner/repo", branch: main }
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
  pinecone:    { api_key: "${PINECONE_API_KEY}", index_name: ..., environment: ...,
                 embedding_provider: openai, embedding_api_key: "${OPENAI_API_KEY}", embedding_model: text-embedding-3-small }
```

CLI-first connectors take a `*_binary` override if the CLI isn't named the default on your PATH.

> **Breaking change (09/A-4).** Connector config moved from top-level sections (`snowflake:`, `github:`,
> …) into the `connector_config.<id>` block above. `github` no longer derives `owner`/`repo` from a
> separate validator — set `repository_url` (the connector derives them) or pass `owner`/`repo`
> explicitly. The `embeddings:` section folded into `connector_config.pinecone` as the `embedding_*`
> fields. A legacy top-level section is silently ignored, so move yours under `connector_config`.

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
  cost_confirm_usd: null          # cost gate: actions whose connector-estimated cost (e.g. BigQuery
                                  # dry_run bytes) exceeds this many USD require a human confirm
```

The tier → decision table and per-connector/per-environment overrides live in `config/policy.yaml`.
See [GOVERNANCE.md](GOVERNANCE.md).

### `sandbox` — code-execution tier

```yaml
sandbox:
  enabled: true
  workdir: ".dacli/sandbox/"      # with the docker runtime, bind-mounted to /workspace
  wall_clock_seconds: 300
  max_memory_mb: 1024             # hard --memory cap under docker
  max_output_chars: 20000
  network: "allowlist"            # off | allowlist | open
  egress_allowlist: []
  # --- runtime backend ---
  runtime: "auto"                 # auto | docker | subprocess
  docker_image: "dacli-sandbox:latest"
  docker_bin: "docker"            # e.g. a full path, or "podman"
  docker_cpus: 2.0                # --cpus
  docker_pids_limit: 256          # --pids-limit (fork-bomb guard)
  docker_auto_build: true         # build the image on first use if missing
```

**Runtime backends.** `subprocess` runs each script in a local child process
(no Docker dependency; weaker OS isolation — fine for trusted/offline/CI use).
`docker` gives **each session its own hardened container** the agent can
`pip install` into and run Python in, reused across runs in the session. `auto`
(default) uses Docker when an engine is reachable, else falls back to subprocess.

The Docker runtime isolates from the host: code runs **non-root** with
`--cap-drop ALL`, `--security-opt no-new-privileges`, and `--memory` / `--cpus` /
`--pids-limit` caps; the **only** host mount is `workdir` → `/workspace`. The
mandatory comms link is the **governed bridge**: the container reaches the parent
on `host.docker.internal`, authenticates with a per-session token, and every
`sdk.run(...)` is classified → policy-checked → audited exactly like a tool call
(credentials never enter the container). Installing packages needs egress, so set
`network: open` (or an allowlist covering your package index) for `pip install`.
The image (`sandbox/docker/Dockerfile`) bakes `pandas`, `numpy`, and `pyarrow`.

### `mcp` — opt-in MCP client bridge

dacli's core never speaks MCP; this section only points the **default-disabled** `mcp_bridge`
connector (`pip install -e ".[mcp]"`) at one external MCP server whose tools are then proxied through
the governed dispatch path. With no `command`/`url` the bridge is inert.

```yaml
mcp:
  command: ""                     # stdio transport: executable serving MCP on stdio
  args: []                        # argv for the stdio command
  url: ""                         # streamable-http endpoint (alternative to command)
  default_risk: risky             # tier for proxied tools unless pinned: safe | write | risky | irreversible
  risk_overrides: {}              # per-tool pins, e.g. {list_models: safe}
  timeout: 60
```

Proxied MCP tools cannot declare environment-anchored post-conditions, so they default to the
conservative `risky` tier and are held to the generic governance gate.

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
ui:
  theme: dark               # dark · light · ocean · mono · nord · gruvbox · contrast
  syntax_highlighting: true
  table_format: grid
  max_width: 120
  truncate_output: 5000
  max_render_rows: 120      # head+tail cap for rendered rows (data itself is never truncated)
  # Accessibility / capability knobs (safe defaults — full polish with zero config):
  glyphs: auto              # auto · unicode · ascii  (auto degrades to ascii on non-UTF-8/dumb terminals)
  reduced_motion: false     # true → no spinners/animation; static status lines
  high_contrast: false      # true → forces the high-contrast `contrast` theme
  no_color: false           # true → monochrome (also honors the NO_COLOR env var)
  show_header: false        # true → a slim model · session · elapsed rule above each turn
retry: { max_attempts: 3, initial_delay: 1.0, max_delay: 30.0, multiplier: 2.0 }
```

Themes: `dark`, `light`, `ocean`, `mono`, `nord`, `gruvbox`, and a WCAG-minded high-contrast
`contrast` palette (switch live with `/theme <name>`). Each theme also picks a matching Pygments
`code_theme` so SQL and fenced code blocks suit the palette.

**Accessibility.** dacli is legible without color or Unicode: `NO_COLOR=1` (or `ui.no_color: true`)
renders structured monochrome where glyphs and layout carry the meaning; `ui.glyphs: ascii` (or any
non-UTF-8 / `TERM=dumb` terminal, detected automatically) swaps the Unicode glyph set for an ASCII
one and degrades the banner to a plain wordmark; `ui.reduced_motion: true` removes all animation.
Every status color is paired with a glyph, so the interface is colorblind-safe by construction.

---

## Security

### Where the encryption key lives

Secrets in `.dacli/dacli.json` are Fernet-encrypted; the key is resolved in this order:

1. `DACLI_ENCRYPTION_KEY` — a raw Fernet key or a passphrase (PBKDF2-derived). Highest priority.
2. The OS keyring, when `DACLI_KEY_BACKEND=keyring` and the `keyring` extra is installed (see below).
3. A `.key` file next to `dacli.json`.

For a fresh install the `.key` lands in the resolved state dir: `<project>/.dacli` inside a project,
the per-user config dir (`%APPDATA%\dacli` / `~/.config/dacli`, overridable with `DACLI_HOME`) outside
one. An existing `.dacli/.key` in the cwd keeps being used, so older installs don't break.

The key and the secrets store share a directory by design (encrypt and decrypt must agree on the key
location). Read access to that directory therefore yields both — the encryption defends against casual
disclosure, not against someone who can already read the folder. On creation the key file is locked to
the current user: `chmod 600` on POSIX, `icacls /inheritance:r /grant:r <user>:F` on Windows (where
`os.chmod` only flips the read-only bit). If the ACL can't be tightened, dacli warns once that the key
inherits the directory's permissions.

### OS keyring backend

```bash
pip install "dacli[keyring]"
export DACLI_KEY_BACKEND=keyring      # default: file
```

The Fernet key is then stored in the OS keyring (Windows Credential Manager, macOS Keychain, Secret
Service on Linux) instead of the `.key` file. One key per OS user. An existing `.key` file is left in
place rather than auto-migrated — delete it by hand once the keyring holds the key. Without the extra
installed, dacli warns once and falls back to the file.

### `.env` trust boundary

dacli loads `.env` from the **resolved project root** (or the per-user config dir outside a project) —
never the raw cwd. A global CLI inherits whatever directory you `cd` into, and a raw-cwd `.env` lets an
attacker-controlled file inject env (e.g. `OPENAI_BASE_URL` → a credential-harvesting proxy) that then
satisfies `${VAR}` placeholders. Set `DACLI_USE_DOTENV=0` to disable `.env` loading entirely.

---

## System prompt layering

The agent's system prompt is built from three layers, top to bottom:

1. **`core.md`** — the packaged base (`prompts/fragments/core.md`), shipped in the wheel and
   read-only. It's overwritten on `pip install -U`; don't edit it.
2. **`DACLI.md` priors** — connection profiles, naming conventions, medallion rules. Created by
   `dacli init`, loaded as layer L1 of the context assembler (`context.assembler`). This is the
   place for *project knowledge* the agent should always have.
3. **`system_prompt.md` overlay** — an editable prompt-level override at `<state_dir>/system_prompt.md`.
   `compose_system_prompt()` appends it after `core.md` and before any connector fragments, so it can
   extend or correct the base while connector-specific rules still get the last word.

The priors (layer 2) and the overlay (layer 3) are distinct knobs: priors are *context* the assembler
pins per turn; the overlay is *prompt text* folded into the composed system prompt. Put facts about
your warehouse in `DACLI.md`; put changes to how the agent behaves in the overlay.

```bash
dacli prompt           # view the composed prompt and where to customize it
dacli prompt --edit    # create the overlay (if missing) and open it in $EDITOR
```

`dacli prompt -o FILE` exports the composed prompt for inspection; writing into the packaged prompt
dir is refused.

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
