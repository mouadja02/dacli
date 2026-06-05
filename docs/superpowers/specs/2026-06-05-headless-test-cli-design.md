# Headless, auditable test surface for dacli

**Date:** 2026-06-05
**Branch:** `feature/headless-test-cli`
**Status:** Approved design, pending implementation

## Problem

`dacli` can only be driven through its interactive chat REPL (`_run_chat`, built on
`prompt_toolkit` + Rich). `agent.process_message` — the real end-to-end path through
reasoning, dispatch, and governance — has no non-interactive entry point. The existing
`python -m eval` harness proves reliability against *simulated* platforms; it does not let
you feed the *actual agent* a prompt and assert on what it did.

That gap blocks two concrete needs:

1. **CI e2e/smoke tests** in GitHub workflows — run a fixed scenario, assert the agent took
   the right actions and governance held, with no API keys, no cost, and deterministic output.
2. **An AI agent (or human) driving dacli from a terminal** — issue a prompt, get structured
   JSON back, branch on the exit code.

## Goal

Add an **additive, non-interactive test surface**: a headless runner plus two CLI commands
(`dacli run`, `dacli replay`) that drive the real agent and emit a stable JSON result covering
the four assertable dimensions: **final answer text, tool calls made, governance decisions,
and token/cost usage**. Support both a **real LLM** (manual/nightly e2e) and a **deterministic
scripted LLM** (offline, free CI). No behavior change to the interactive chat.

## Non-goals

- No declarative scenario/assertion DSL. We ship the primitive (`run`/`replay` + JSON out);
  assertions live in pytest / bash / workflow steps that consume the JSON.
- No `--expect`/strictness matcher in v1 (exit codes are the contract; a matcher can come later).
- No change to connectors, governance internals, or the eval harness.

## Architecture

### 1. The seam: LLM injection (only change to existing core)

`DACLI.__init__` currently hard-constructs `self.llm = LLMClient(settings)`. Add one optional
parameter:

```python
def __init__(self, settings, memory=None, ..., llm=None):
    ...
    self.llm = llm or LLMClient(settings)
```

Constructor injection chosen over an env/settings switch (keeps a test concern out of
production `LLMClient`) and over monkeypatching (fragile, gives no CLI surface). This is the
entire footprint in `core/agent.py`; everything else is new files.

The injected object must satisfy the contract the kernel already depends on:

- `async initialize() -> None`
- `async generate(messages, tools, system_prompt, on_text=None, model=None) -> (content, tool_calls)`
- attribute `last_usage: Dict[str, int]`, refreshed each `generate()`

where `tool_calls` is a list of dicts shaped exactly as the kernel parses them
(`core/kernel.py:226`):

```python
{"id": "call_1", "name": "bigquery_query", "arguments": {"sql": "..."}}
```

An empty `tool_calls` list signals "final answer", ending the iteration loop.

### 2. `reasoning/scripted.py` — `ScriptedLLM`

A deterministic, offline stand-in. Constructed from an ordered list of scripted *responses*;
each `generate()` call pops the next one.

A scripted response is:

```python
{
  "text": "optional assistant text",
  "tool_calls": [                     # omit / [] -> final answer, loop ends
    {"name": "bigquery_query", "arguments": {"sql": "SELECT 1"}}
  ],
  "usage": {"input": 100, "output": 20}   # optional; defaults to zeros
}
```

`ScriptedLLM` assigns each tool call a synthetic `id` (`call_1`, `call_2`, …) so callers
don't have to. It sets `last_usage` from the response's `usage`. If `generate()` is called
after the script is exhausted, it raises `ScriptExhausted` — a real signal that the agent
looped more than the scenario anticipated (surfaced as exit code 3).

### 3. `core/headless.py` — `HeadlessRunner`

Drives the agent with no interactive I/O. Responsibilities:

- Build a `DACLI` with **recording, non-interactive callbacks**:
  - `on_tool_start(name, args)` / `on_tool_end(name, result)` append to an in-memory list per
    turn (this is how "tool calls made" is captured, independent of the LLM layer).
  - `on_status_update`, `on_text`, stream hooks → no-ops (or buffered, unused in v1).
  - `on_approval(request) -> bool`: configurable policy — `"deny"` (default, fail-safe),
    `"approve"`, or a scripted sequence of booleans.
  - `on_user_input_needed(question) -> str`: returns canned answers in order; raises (→ exit 3)
    if none remain, so a scenario that unexpectedly blocks on input fails loudly.
- `await agent.initialize()`, run each input message through `await agent.process_message(msg)`,
  collecting an `AgentResponse` per turn.
- After each turn, gather:
  - **content / error / needs_user_input / iterations** from `AgentResponse`.
  - **tool_calls** from the recorded list (name, args, status, error).
  - **governance** from `agent.governor.ledger.decisions(session_id=...)` (None when governance
    disabled → empty list).
  - **usage** (run-level) from `agent.store.usage_summary(session_id)["session"]`.
- Always `await agent.shutdown()` in `finally`.
- Return a `HeadlessResult` dataclass with `to_dict()` / `to_json()` and a computed `exit_code`.

**Offline guarantee.** `LLMClient.initialize()` opens no socket (network is in `generate`),
which `ScriptedLLM` replaces. `fetch_pricing` already degrades to zero-cost offline. Connectors
default to a **built-in-only / minimal** set so `initialize()` makes no external calls; the
runner accepts `--config` to point at a test config, and a `--no-connectors` flag to force the
built-in-only set. (Built-ins = system, skills, sandbox, shell.)

### 4. CLI commands (`scripts/cli.py`)

```
dacli run "<message>" [--json] [--config PATH] [--session ID]
        [--approve deny|approve] [--llm-script FILE] [--no-connectors]
        [--max-iterations N]
```

One-shot. With `--llm-script` it loads scripted responses (JSON/YAML) and injects
`ScriptedLLM` — fully offline. Without it, the real configured provider runs (the e2e path).
`--json` prints the JSON result; default prints a short human summary plus sets the exit code.

```
dacli replay FILE [--json] [--config PATH] [--no-connectors]
```

Runs a **scenario file**: an ordered list of user turns plus an optional embedded LLM script
and approval/input policy. Emits one aggregated JSON result for the whole conversation. This is
the CI / AI-agent workhorse.

Scenario file schema:

```yaml
config: null            # optional path override
no_connectors: true     # default for offline scenarios
approve: deny           # deny | approve | [true, false, ...]
inputs: ["answer 1"]    # canned responses for on_user_input_needed, in order
llm_script:             # optional; omit to use the real LLM
  - { text: "Running the query.", tool_calls: [ { name: bigquery_query, arguments: { sql: "SELECT 1" } } ] }
  - { text: "Done — 1 row." }
turns:                  # user messages, run in order
  - "load the sales table into bigquery"
```

### 5. JSON output contract (stable)

```json
{
  "ok": true,
  "exit_code": 0,
  "session_id": "hl-20260605-...",
  "turns": [
    {
      "input": "...",
      "content": "final assistant text",
      "error": null,
      "needs_user_input": false,
      "iterations": 2,
      "tool_calls": [
        {"name": "bigquery_query", "args": {"sql": "SELECT 1"}, "status": "success", "error": null}
      ],
      "governance": [
        {"decision_id": "...", "tool_name": "bigquery_query", "tier": "risky", "events": [ ... ]}
      ]
    }
  ],
  "usage": {"requests": 1, "input": 100, "output": 20, "costUSD": 0.0},
  "audit_path": ".dacli/audit.jsonl"
}
```

### 6. Exit codes (the CI/agent contract)

| Code | Meaning |
|------|---------|
| 0 | All turns completed without error or governance block. |
| 1 | An `AgentResponse.error` was returned, or a turn ended with `needs_user_input` set. |
| 2 | An action was blocked/denied by governance (a `block` event, or an approval denied). |
| 3 | Scenario/script error (`ScriptExhausted`, malformed scenario, or `on_user_input_needed` called with no canned input left). |

`ok` is `true` iff `exit_code == 0`. When multiple conditions apply, the highest code wins
(3 > 2 > 1).

## Testing the feature (TDD)

`tests/test_headless.py` (unittest, matching repo style), built test-first:

- `ScriptedLLM` returns scripted `(content, tool_calls)` in kernel shape; assigns ids; sets
  `last_usage`; raises `ScriptExhausted` when over-pulled.
- `HeadlessRunner` with a scripted LLM + a built-in echo/no-op tool produces the documented
  JSON: captured tool calls, final content, usage.
- A scripted scenario that triggers a governance block yields `exit_code == 2` and a `block`
  event in `governance`.
- An over-short script yields `exit_code == 3`.
- A turn returning an error yields `exit_code == 1`.
- `dacli run --llm-script ...` and `dacli replay ...` invoked via Click's `CliRunner` emit valid
  JSON and the right process exit code.

## CI / GitHub workflow

- Commit an offline scenario at `scenarios/smoke_headless.json` (scripted LLM, `no_connectors`).
- Add a workflow step: `python run.py replay scenarios/smoke_headless.json --json`, assert exit 0.
  No secrets required; runs alongside the existing offline `python -m eval --quick`.

## Docs

`docs/TESTING.md`: the JSON contract, exit codes, the CI pattern, and the "AI agent drives dacli
from the terminal" pattern (issue `dacli run "<task>" --json`, parse, branch on exit code).

## File-level summary

| File | Change |
|------|--------|
| `core/agent.py` | Add optional `llm=None` param; `self.llm = llm or LLMClient(settings)`. |
| `reasoning/scripted.py` | New — `ScriptedLLM`, `ScriptExhausted`. |
| `core/headless.py` | New — `HeadlessRunner`, `HeadlessResult`. |
| `scripts/cli.py` | New — `run` and `replay` Click commands. |
| `tests/test_headless.py` | New — TDD coverage of the above. |
| `scenarios/smoke_headless.json` | New — committed offline CI scenario. |
| `.github/workflows/ci.yml` | New step running the offline scenario. |
| `docs/TESTING.md` | New — contract + usage patterns. |
