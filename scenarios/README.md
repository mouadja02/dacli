# Scenarios — headless, replayable agent runs

A scenario file drives `dacli replay <file>` (see `core/headless.py`): ordered
user turns, an optional scripted LLM (offline, deterministic, no secrets), an
approval policy, and connector toggles.

```jsonc
{
  "no_connectors": true,          // built-ins only — hermetic, no network
  "approve": "deny",              // fail-safe: governed actions are denied
  "llm_script": [ ... ],          // scripted LLM responses (omit to use the real provider)
  "inputs": [ ... ],              // canned answers if the agent asks the user
  "turns": ["do the thing"]       // the user messages, in order
}
```

## Exit-code contract

| exit | meaning |
|---|---|
| 0 | scenario passed |
| 1 | agent error, or the agent needed user input |
| 2 | **governance block** — an action was denied/blocked |
| 3 | scenario error (LLM script exhausted / unanswerable input request) |

`--json` emits the machine-readable result (turns, tool calls with statuses,
governance decisions, usage) on stdout.

## Files

- `smoke_headless.json` — happy-path smoke: scripted plan + answer, exits 0.
- `ci_governance_gate.json` — proves deny-by-default holds: a scripted
  destructive command is blocked, the replay exits **2**.

## CI gating (F-9)

Use the composite action to fail a build when a governed run misbehaves:

```yaml
jobs:
  dacli-gate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: ./.github/actions/dacli-gate
        with:
          scenario: scenarios/smoke_headless.json
```

Or inline, without the action:

```yaml
      - run: pip install . && dacli replay scenarios/smoke_headless.json --json
```

A real gating scenario would point `turns` at an assertion over your data
(e.g. "verify row counts match between prod and the PR schema") with a real
LLM config — the exit-code contract is identical.
