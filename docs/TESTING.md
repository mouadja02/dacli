# Testing dacli headlessly

`dacli` can be driven non-interactively for CI e2e/smoke tests and for an AI agent
operating the terminal. Two commands return a stable JSON result.

## Commands

- `dacli run "<message>" [--json] [--config PATH] [--session ID] [--approve deny|approve] [--llm-script FILE] [--no-connectors] [--max-iterations N]`
  — one message through the agent. With `--llm-script` the run is fully offline and
  deterministic; without it, the configured provider is used (real e2e).
- `dacli replay FILE [--json]` — a scenario file: ordered user turns plus an optional
  embedded `llm_script` and approval/input policy.

(Invoke via `python run.py run ...` / `python run.py replay ...` from the repo root.)

## JSON contract

```json
{
  "ok": true,
  "exit_code": 0,
  "session_id": "…",
  "turns": [
    {
      "input": "…",
      "content": "final assistant text",
      "error": null,
      "needs_user_input": false,
      "iterations": 2,
      "tool_calls": [{"name": "update_plan", "args": {}, "status": "success", "error": null}],
      "governance": [{"decision_id": "…", "tool_name": "…", "tier": "risky", "events": []}]
    }
  ],
  "usage": {"requests": 1, "input": 100, "output": 20, "costUSD": 0.0},
  "audit_path": ".dacli/audit.jsonl"
}
```

(`tool_calls[].status` mirrors the engine's `ToolStatus` value — prefer matching
the enum over the literal if you assert on it.)

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | All turns completed without error or governance block. |
| 1 | An agent error, or a turn ended waiting on user input. |
| 2 | An action was blocked/denied by governance. |
| 3 | Scenario/script error (scripted LLM ran dry, malformed scenario, missing canned input). |

When several conditions apply, the highest code wins (3 > 2 > 1).

## Scenario file

A scenario file may be JSON or YAML (`yaml.safe_load` parses both):

```yaml
config: null
no_connectors: true        # built-ins only; no external network
approve: deny              # deny | approve | [true, false, ...]
inputs: ["answer 1"]       # canned answers for request_user_input, in order
llm_script:                # omit to use the real provider
  - { text: "Running it.", tool_calls: [ { name: update_plan, arguments: { todos: [] } } ] }
  - { text: "Done." }
turns:
  - "do the thing"
```

A scripted `llm_script` entry is `{text?, tool_calls?, usage?}`. Omitting `tool_calls`
(or giving `[]`) ends the agent loop — that entry is the final answer. If the agent
requests another generation past the end of the script, the run is a scenario error
(exit 3): script your final answer entry.

## CI pattern

```yaml
- name: Headless e2e smoke (scripted LLM, offline)
  run: python run.py replay scenarios/smoke_headless.json --json
```

The step fails (non-zero exit) on any agent error, governance block, or script error.
No secrets are required — the scripted LLM makes the run free and deterministic.

## AI agent driving the terminal

```bash
python run.py run "list the connectors and load the sales table" --json
# parse stdout JSON; branch on .exit_code; inspect .turns[].tool_calls and .governance
```

`--json` prints **only** the JSON object on stdout (no Rich styling), so
`json.loads(stdout)` is safe. Use `--no-connectors` for a hermetic run that touches no
external platform, and `--approve deny` (the default) to keep governed actions fail-safe.
