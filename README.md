# dacli

**dacli** (pronounced *"DACK-lee"*) is an autonomous **data-engineering CLI agent**. You talk to it in plain
language and it operates your data stack for you — running SQL on your warehouse, managing files and CI
workflows on GitHub, and looking up documentation — all from your terminal, with your approval where it matters.

It is built for **data architects, engineers, analysts, and modelers** who want an agent that reasons over a
real, connected toolset instead of a chat window that only gives advice.

> **Status:** early (v0.1.0). The agent runs today against Snowflake, GitHub, and Pinecone. The connector
> layer is a plugin system designed so that adding a new platform is "drop in a folder", not "rewrite the agent".

---

## Features

- **Conversational data ops** — describe what you want; the agent plans, calls tools, observes results, and iterates.
- **Pluggable connectors** — each platform is a self-describing plugin discovered from a `manifest.yaml`. Ships with:
  - ❄️ **Snowflake** — run SQL, inspect context (warehouse / database / schema / role).
  - 🐙 **GitHub** — read/write repo files, trigger and monitor Actions workflows, pull failure logs.
  - 📚 **Pinecone** — semantic search over your documentation / knowledge base.
- **Multi-provider LLM** — OpenAI, Anthropic, Google, or OpenRouter, selected in config.
- **Setup wizard** — pick which connectors and operations to enable; credentials are validated with a live health check.
- **Risk-aware operations** — every operation declares a risk tier (`safe` / `write` / `risky` / `irreversible`),
  and the agent can pause to ask you before acting.
- **Session memory** — conversation history and progress are persisted so you can resume a session later.

---

## Installation

Requires **Python 3.9+**.

```bash
git clone https://github.com/mouadja02/dacli.git
cd dacli

python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
# optional: install the `dacli` command on your PATH
pip install -e .
```

---

## Configuration

dacli reads a `config.yaml` and substitutes secrets from environment variables (`${VAR}` placeholders), which
you can supply via a `.env` file.

1. **Create your config** from the template:

   ```bash
   cp config_template.yaml config.yaml
   ```

2. **Create your `.env`** from the example and fill in your secrets:

   ```bash
   cp .env.example .env
   ```

   ```dotenv
   LLM_API_KEY=...
   GITHUB_TOKEN=...
   SNOWFLAKE_PASSWORD=...
   PINECONE_API_KEY=...
   OPENAI_API_KEY=...        # used for Pinecone embeddings
   ```

3. **Edit `config.yaml`** — set your LLM provider/model and your account identifiers (Snowflake account,
   GitHub owner/repo, Pinecone index, etc.). Anything written as `${VAR}` is pulled from your environment, so
   secrets never live in the file.

> `config.yaml`, `.env`, and the wizard-generated `config/connectors.yaml` are git-ignored — they hold your
> local credentials and choices.

---

## Quick start

```bash
# Using the installed command:
dacli                 # first run launches the setup wizard, then starts chatting
dacli setup           # (re)configure which connectors/operations are enabled
dacli validate        # live-test every enabled connector's credentials

# …or without installing:
python run.py
```

On first run, the **setup wizard** asks which connectors to enable and validates each one. After that, you drop
straight into an interactive chat. Just tell the agent what you want, e.g.:

> *"List the tables in my analytics schema, then create a `stg_orders` view selecting from `raw.orders`."*

---

## Commands

### CLI subcommands

| Command | Description |
|---|---|
| `dacli` / `dacli chat` | Start the interactive chat (default). |
| `dacli setup [--profile <name>]` | Run the connector setup wizard. Profiles: `full`, `none`, `<connector>_only`. |
| `dacli validate` | Test connections for all enabled connectors. |
| `dacli init` | Write a fresh default `config.yaml`. |
| `dacli sessions` | List previous sessions. |
| `dacli load <session_id>` | Resume a previous session. |
| `dacli prompt` | View the active system prompt. |
| `dacli --version` | Show the version. |

### In-chat slash commands

`/help` · `/status` · `/tools` · `/setup` · `/history` · `/sessions` · `/load <id>` · `/export` · `/config` ·
`/prompt` · `/clear` · `/reset` · `/exit`

---

## Project layout

```
dacli/
├── scripts/cli.py        # CLI entry point (the `dacli` command)
├── core/
│   ├── agent.py          # thin wiring object — wires the components together
│   ├── kernel.py         # the orchestration loop (generate → act → observe)
│   ├── memory.py         # session state, history, progress tracking
│   └── setup_wizard.py   # interactive connector configuration
├── reasoning/llm.py      # multi-provider LLM client
├── connectors/
│   ├── base.py           # Connector contract: Connector / OperationSpec / ToolResult / Risk
│   ├── registry.py       # manifest discovery + LLM tool definitions + name→connector resolver
│   ├── dispatcher.py     # generic tool dispatch (timing, callbacks, logging)
│   ├── snowflake/        # connector.py + manifest.yaml
│   ├── github/
│   ├── pinecone/
│   └── system/           # built-in tools (request user input, update progress)
├── config/settings.py    # typed settings (pydantic) + env-var substitution
├── prompts/              # system_message.md + GUIDELINES.md + loaders
├── config_template.yaml  # copy to config.yaml
├── .env.example          # copy to .env
└── run.py                # dev entry point (python run.py)
```

The design separates concerns deliberately: the **kernel** owns the loop and knows nothing platform-specific;
**connectors** own all platform behavior; the **registry/dispatcher** route tool calls between them.

---

## Adding a new connector

No edits to `core/` or `reasoning/` are required. To add a platform:

1. Create `connectors/<platform>/connector.py` with a class that subclasses `Connector` and implements
   `operations()`, `invoke()`, and `health()`.
2. Add `connectors/<platform>/manifest.yaml` describing it:

   ```yaml
   id: myplatform
   name: My Platform
   description: What it does
   icon: "🔌"
   class: connectors.myplatform.connector.MyPlatformConnector
   required_config: [api_key]
   enabled: true
   ```

The registry discovers it on startup, the wizard offers it, and its operations become tools the agent can call.

---

## Development

```bash
python -m unittest discover -s tests -p "test_*.py"
```

The golden-transcript test replays a recorded multi-tool session through the real kernel + dispatcher +
registry, acting as a behavior-equivalence regression net.

---

## License

See the repository for license details.
