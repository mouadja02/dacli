# Extending dacli

An extension is one Python module exporting `register(api)`. No manifest, no enum, no settings
section — the module *is* the registration. Connectors, slash commands, providers, themes are all
extensions. The agent writes these the same way you would by hand; this is the contract it follows.

## Where extensions live

```
<project>/.dacli/extensions/<id>/__init__.py   # project-only (highest precedence)
~/.dacli/extensions/<id>/__init__.py           # global, all workspaces
packages/dacli/.../seeds/extensions/<id>/      # bundled seeds (lowest; the worked examples)
```

Discovery is project → global → seed; first match wins. The seeds —
[`snowflake`](../packages/dacli/src/dacli/seeds/extensions/snowflake/__init__.py),
[`github`](../packages/dacli/src/dacli/seeds/extensions/github/__init__.py),
[`shell`](../packages/dacli/src/dacli/seeds/extensions/shell/__init__.py) — are reference
extensions: read them.

## The `register(api)` contract

```python
from dacli.core.verify import result_succeeded

def register(api):
    # Config the user supplies later via /connect. Secrets are entered hidden and stored
    # encrypted; NEVER write a secret value as a literal — validation rejects it.
    api.config_field("bucket", required=True, description="Target S3 bucket")
    api.config_field("access_key", secret=True)
    api.config_field("secret_key", secret=True)

    @api.tool(
        name="s3_list",                          # LLM-facing tool name
        description="List objects under a prefix",
        parameters={"prefix": {"type": "string"}},  # JSON-schema property map
        risk="safe",                             # safe | write | risky | irreversible
        postconditions=[result_succeeded()],     # at least one, always
    )
    async def s3_list(args, ctx):
        cfg = api.config()              # {"bucket": ..., decrypted secrets ...}
        ...
        return ctx.ok(rows)            # success; or ctx.fail("reason")
```

The module runs nothing at import time beyond `register(api)`. Registrations commit only if
`register` returns cleanly — a module that registers two tools then raises commits neither.

## The ExtensionAPI surface

| Method | Purpose |
|---|---|
| `@api.tool(name, description, parameters, risk, postconditions, ...)` | register an LLM-callable tool. `risk` and `postconditions` are mandatory. |
| `api.command(name, handler, *, description="")` | register a slash command. |
| `api.shortcut(key, handler, *, description="")` | register a keybinding. |
| `api.provider(name, config)` | register an LLM provider. |
| `api.config_field(name, *, secret=False, required=False, default=None, description="")` | declare a credential/setting (drives `/connect`, secret-aware). |
| `api.config()` | this extension's config, secrets decrypted at call time. |
| `api.on(event, handler)` | subscribe to a lifecycle event. |
| `api.append_entry(entry)` / `api.entries()` | append/replay state that outlives a reload. |

### The tool handler

`async def handler(args, ctx)`. `args` is the parsed parameter dict; `ctx` builds the result the
dispatcher governs, verifies, and audits:

- `ctx.ok(data=None, **metadata)` — success.
- `ctx.fail(error, **metadata)` — failure.

A handler may be sync or async; a bare return value is taken as a success payload.

### Risk tiers

`safe` (read-only) · `write` · `risky` (mutates, recoverable) · `irreversible` (no native undo).
The tier drives governance: an `irreversible` tool is blocked unless a rollback path is *verified*
to exist, and crosses a confirm gate.

### Post-conditions

A tool with no post-condition is refused at registration — fluent output is never proof. Name one
of the built-in checks as a string, or pass a `PostCondition` object for a custom check.

Built-in names: `result_succeeded`, `data_is_list`, `shell_exit_zero`, `shell_writes_observed`,
`shell_deletes_observed`. Use `result_succeeded` if unsure. Anchor a post-condition to the
*platform* (a row count, `bq show`, `head-object`), not the model, wherever you can.

### Config and secrets

`api.config_field(..., secret=True)` is the single declaration point. The `/connect` dialog enters
secrets hidden and encrypts them on write; the handler reads them through `api.config()` at call
time. Generated code never holds a key literal — a module that inlines a value for a `secret=True`
field is rejected by validation.

### Lifecycle events

`api.on("session_start", handler)` and `api.on("session_shutdown", handler)` fire around startup
and each reload (the handler receives a `reason` string). Reconstruct state on `session_start` from
what you `append_entry`'d, so persisting state never inflates the model context.

## The self-build loop

How "add an S3 connector" runs end to end:

1. **Ask.** You: "add an S3 connector." The agent reads this contract and the seeds.
2. **Clarify.** At an ambiguous fork it asks instead of guessing
   (`select` / `confirm` / `input`), e.g. "AWS CLI runner or direct credentials?".
3. **Write.** It writes `~/.dacli/extensions/s3/__init__.py` (or the project `.dacli/`).
4. **Validate.** The host imports the module in a **child process**, runs `register(api)` against a
   probe API, and checks every tool declares parameters + risk + a post-condition. A bad generation
   fails its own process, never the session; the prior version stays loaded.
5. **Hot-reload.** On success the host re-discovers and re-registers — new tools are live, no
   restart.
6. **Secrets.** `/connect s3` enters credentials hidden, encrypted to `secrets.json`.
7. **Iterate.** Edit the module, `/reload`, keep working.

## Skills

A skill is a `SKILL.md` doc (front matter `name` + `description`, then the method), not code. The
prompt shows name + one-liner; the agent reads the file when a task calls for it. Drop one under
`~/.dacli/skills/<name>/SKILL.md`, or add a seed under
`packages/dacli/src/dacli/seeds/skills/`.

See also [GOVERNANCE.md](GOVERNANCE.md) for how risk and post-conditions are enforced.
