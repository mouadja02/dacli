<div align="center">

<img src="assets/dacli-banner.svg" alt="dacli — a data-engineering agent for the terminal" width="820">

**dacli is a data-engineering agent for the terminal.**

*A thin extension host over a governed core. It ships almost nothing and writes its own tools — Python `register(api)` extensions it validates in a child process and hot-reloads with no restart.*

[![CI](https://github.com/mouadja02/dacli/actions/workflows/ci.yml/badge.svg)](https://github.com/mouadja02/dacli/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![Tests](https://img.shields.io/badge/tests-685-brightgreen.svg)](#testing)

[Quick start](#quick-start) · [Extending dacli](docs/EXTENSIONS.md) · [Governance](docs/GOVERNANCE.md) · [Evaluation](docs/EVALUATION.md)

</div>

---

## Design

dacli is not a SQL chatbot, and it is not a maximal harness with a connector per platform.
The core is small: an agent loop, a governance gate, secrets, post-condition verification, and
a generic extension API. Everything else — connectors, skills, UI tweaks — is an extension the
agent writes on demand into `~/.dacli/`, validates in a subprocess, and hot-reloads.

The hard problem with autonomous data agents is reliability, not capability. A 95%-reliable
`DROP` guard is a catastrophe waiting for its 1-in-20. So three things stay in the core and are
non-optional, even for generated code:

- **Governance.** Every tool call — bundled, generated, or installed — declares a blast-radius
  tier (`safe → write → risky → irreversible`) and passes through one governed dispatch path:
  classify → policy → (irreversible with no verified rollback? block) → dry-run → confirm.
- **The environment is the oracle.** A result is "done" only when the platform confirms it
  (row counts, `bq show`, statement state, `dbt` artifacts), never the model's say-so. The
  registry refuses to register a tool with no post-condition.
- **Secrets never touch generated code.** Credentials are entered hidden, Fernet-encrypted, and
  read through `api.config()` at call time. Validation rejects a module that inlines a secret.

No agent framework, no LangChain/LangGraph, MCP-free core: tools are plain Python the agent
composes. Grounded in *["From Model Scaling to System Scaling"](https://arxiv.org/abs/2605.26112)*.

---

## Packages

Four wheels (a consumer embedding dacli imports the lower two):

| Package | Contents |
|---|---|
| `dacli-ai` | provider LLM API (OpenAI/Anthropic/OpenRouter), token accounting |
| `dacli-core` | extension host + registry, governance gate, secrets, verify, context economy, `~/.dacli/` resolver |
| `dacli-tui` | Rich + prompt-toolkit REPL, themes, widgets — user-editable by prompt |
| `dacli` | the assembled CLI; ships the snowflake/github/shell seed extensions + the `register(api)` docs the agent reads to extend itself |

`dacli-core` runs headless with no TUI installed.

---

## Installation

**Requirements:** Python **3.10+** (CI runs 3.10–3.12).

```bash
pipx install dacli          # or: uv tool install dacli
```

Embedding dacli as a library: `pip install dacli`.

### From source

```bash
git clone https://github.com/mouadja02/dacli.git
cd dacli
python -m venv .venv
.venv\Scripts\activate            # Windows; POSIX: source .venv/bin/activate

# dacli is four wheels; install all four editable to run the working tree.
pip install -e packages/dacli-ai -e packages/dacli-core \
            -e packages/dacli-tui -e packages/dacli
```

Optional extras: `packages/dacli[all]` (Python-SDK seeds, e.g. snowflake), `[dev]` (pytest,
ruff, vulture), `[docker]` (containerized sandbox), `[pty]` (faithful TTY for the terminal).

> Use the editable install (`-e`). A plain `pip install .` copies sources into `site-packages`,
> so `dacli` then runs a frozen copy that diverges from your working tree.

Something off? `dacli doctor` reports where config, state, and logs resolve, your LLM key's
source (never the value), governance/sandbox posture, and seed health.

---

## Quick start

The point of dacli is that you don't pre-wire connectors — you ask, and it builds one.

```bash
pipx install dacli
dacli                          # first run bootstraps your LLM key (encrypted, no .env)
```

Then describe what you want. To grow a new capability:

> *"Add an S3 connector — list and read objects under a prefix."*

dacli reads the bundled [extension guide](docs/EXTENSIONS.md) and the snowflake/github/shell
seeds, asks a clarifying question at any fork it can't infer, then:

1. **Writes** `~/.dacli/extensions/s3/__init__.py` — one module exporting `register(api)`.
2. **Validates** it in a child process: every tool must declare parameters, a risk tier, and a
   post-condition. A bad generation fails its own process, never the session.
3. **Hot-reloads** — the new tools are live, no restart.
4. **Secrets:** `/connect s3` enters the access key hidden and stores it encrypted; the module
   reads it through `api.config()` and never holds a key literal.
5. **Iterate:** edit the module, `/reload`, keep working.

An extension is the whole contract — there's no manifest, no settings section, no enum to edit:

```python
# ~/.dacli/extensions/s3/__init__.py
from dacli.core.verify import result_succeeded

def register(api):
    api.config_field("bucket", required=True, description="Target S3 bucket")
    api.config_field("access_key", secret=True)
    api.config_field("secret_key", secret=True)

    @api.tool(
        name="s3_list",
        description="List objects under a prefix",
        parameters={"prefix": {"type": "string"}},
        risk="safe",                       # safe | write | risky | irreversible
        postconditions=[result_succeeded()],   # no post-condition, no registration
    )
    async def s3_list(args, ctx):
        cfg = api.config()                 # {"bucket": ..., decrypted secrets ...}
        ...
        return ctx.ok(rows)                # or ctx.fail(reason)
```

Full surface and lifecycle: **[docs/EXTENSIONS.md](docs/EXTENSIONS.md)**.

### Offline, zero-risk

```bash
dacli diff snowflake db.a db.b     # read-only data diff; no creds, no network
dacli eval --quick                 # the offline pass^k reliability suite
```

---

## `~/.dacli/` layout

The home dir is where the agent writes extensions and where you own the surface. A project-local
`.dacli/` overlays the global one (project wins); the bundled seeds are the lowest layer.

```
~/.dacli/
  config.yaml        # llm, ui, agent, governance
  SYSTEM.md          # your system-prompt override (replaces the core default)
  AGENTS.md          # always-injected operating notes (hierarchically merged)
  secrets.json       # Fernet-encrypted credentials   .key  # the key (or OS keyring)
  extensions/<id>/__init__.py    # generated extensions
  skills/<name>/SKILL.md         # progressive-disclosure skill docs
  themes/<name>.py               # user themes
  workspaces/<name>/             # isolated overlay: extensions, secrets, history, audit
```

Discovery precedence for any resource: project `.dacli/` → global `~/.dacli/` → bundled seeds.
`dacli workspace <name>` switches the active overlay live, no restart.

---

## Skills

A skill is a `SKILL.md` doc — a method the agent reads when a task calls for it (Pi-style
progressive disclosure). The prompt carries only name + one-liner; the agent reads the file on
demand. Drop a `SKILL.md` under `~/.dacli/skills/<name>/` to teach a method without touching the
agent. The seeds ship `data-diff`, `quality-assert`, and `diagram-mermaid`.

---

## Command reference

### CLI subcommands

| Command | Description |
|---|---|
| `dacli` · `dacli chat` | Start the interactive chat (default). |
| `dacli diff <connector> <a> <b> [--sample N]` | Read-only data diff: row-count delta, per-column null rates over a bounded sample, sampled value comparison. |
| `dacli setup [--profile <name>]` | Configure which seed connectors/operations are enabled. |
| `dacli validate` | Live-test every enabled connector's credentials. |
| `dacli doctor [--ping] [--json]` | Diagnose where config/state/log resolve, the LLM key + its source, governance/sandbox/terminal posture, seed status. Offline by default; non-zero exit on a hard problem. |
| `dacli eval [--quick] [--regression] [--calibrate] [--json] [--report <path>]` | Run the pass^k reliability suite + dashboard. |
| `dacli audit [--session <id>] [--full]` | Reconstruct governance decisions ("why did it act?"). |
| `dacli context [--task <t>] [--explain]` | Inspect the assembled context (sources, tokens, budget). |
| `dacli catalog [--connector <id>]` | List known data objects from the catalog cache. |
| `dacli schema <object>` | Show cached columns / row count for one object. |
| `dacli lineage <object> [--json]` | Show known upstream producers / downstream consumers. |
| `dacli why-failed [--source dbt\|airflow] [--dag <id>] [--run <id>] [--apply] [--json]` | Explain the most recent pipeline failure and propose a governed fix; `--apply` routes it through classify → approve → verify → rollback. |
| `dacli assert define\|list\|run\|delete` | Author and run connector-agnostic data-quality assertions. `run` exits non-zero on a breach so CI can gate. |
| `dacli runbook save\|list\|show\|run` | Save and run a parameterized headless task under a policy envelope (approvals pre-granted within a tool set + tier ceiling). |
| `dacli cost <connector> [--estimate "<sql>"] [--session] [--json]` | Warehouse cost advisor: pre-run estimate + post-hoc session spend, read-only. |
| `dacli run "<message>" [--json] [--approve approve\|deny]` | One headless agent turn with a machine-readable result and a stable exit-code contract. |
| `dacli replay <scenario.json> [--json]` | Replay a scenario file headlessly — what the CI gate runs. |
| `dacli connector install <name> --index <path\|url> [--force]` | Fetch a shared extension from an index, validate it in a sandboxed subprocess, register it disabled. |
| `dacli export-run [--session <id>] [--out <zip>]` | Export a session as a compliance bundle (transcript + audit slice + usage, secrets redacted). |
| `dacli sessions` · `dacli load <id>` | List / resume previous sessions. |
| `dacli init` | Write a fresh default `config.yaml`. |
| `dacli prompt` | View the active system prompt. |
| `dacli --version` | Show the version. |

### In-chat slash commands

`/help` · `/keys` · `/init` · `/status` · `/doctor` · `/usage` · `/context` · `/audit` · `/why-failed [dag]` · `/tools` · `/connect [ext]` ·
`/new-extension` · `/reload` · `/extensions` · `/scope [ext] [level]` · `/creds [ext] [--delete]` · `/workspace [name]` · `/testmode [tool]` ·
`/setup` · `/history` · `/find <text>` · `/last-error` · `/expand <id>` · `/transcript` · `/sessions` ·
`/catalog [connector]` · `/schema <object>` · `/load <id>` · `/export` ·
`/config` · `/theme <name>` · `/prompt` · `/clear` · `/cls` · `/reset` · `/exit`

---

## Reliability: the environment is the oracle

dacli refuses to ask the model *"did that work?"* — it asks the platform. Pre-conditions
(`EXPLAIN`, BigQuery `dry_run`, `dbt compile`) validate before anything runs; post-conditions
(`bq show`, `head-object`, `run_results.json`) confirm after; irreversible actions are blocked
unless a native undo path is *verified to exist*. Reliability is measured as `pass^k` — success
across *k repeated* rollouts, not a single lucky run.

```text
$ dacli eval --quick
Reliability dashboard — suite: sim
----------------------------------------------------------------------------------------------
connector          tasks  pass@1  pass^k   succ    esc   corr    gov  unguard     tok       ms
----------------------------------------------------------------------------------------------
github                 1    1.00    1.00   1.00   0.00   0.00   0.00        0       0      0.1
snowflake              1    1.00    1.00   1.00   0.00   0.00   0.00        0       0      0.1
shell                  5    1.00    1.00   1.00   0.00   0.00   0.20        0       0     15.0
spine                  3    1.00    1.00   1.00   0.00   0.20   0.20        0       0      1.0
OVERALL               10    1.00    1.00   1.00   0.00   0.03   0.09        0       0      8.0
----------------------------------------------------------------------------------------------
✓ zero unguarded destructive executions.
```

Details: **[docs/GOVERNANCE.md](docs/GOVERNANCE.md)** and **[docs/EVALUATION.md](docs/EVALUATION.md)**.

---

## Testing

```bash
pytest tests -q                         # full suite
python tools/check_docs.py              # docs drift gate (badge / eval sample / command reference)
```

CI runs ruff, the full suite (Python 3.10–3.12), the pass^k sim suite, a headless end-to-end
smoke, and the docs drift gate on every pull request.

---

## Documentation

| Doc | What's inside |
|---|---|
| [docs/EXTENSIONS.md](docs/EXTENSIONS.md) | The `register(api)` contract: the ExtensionAPI surface, lifecycle events, the self-build loop, and validation rules. |
| [docs/GOVERNANCE.md](docs/GOVERNANCE.md) | Blast-radius tiers, policy, rollback, the audit ledger, permissions, and the sandbox. |
| [docs/EVALUATION.md](docs/EVALUATION.md) | pass^k, golden suites, regression detection, and the dashboard. |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | The `config.yaml` reference, env vars, and the `~/.dacli/` overlay. |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development setup and the contribution workflow. |
| [RELEASING.md](RELEASING.md) | The tag-driven release process. |

---

## License

This project was created by **Mouad Jaouhari**. If a `LICENSE` file is not yet present in the
repository, please contact the author before reuse or redistribution.
