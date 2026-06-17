# Connector config: the manifest-config pattern (09/A-4)

`config/settings.py` is a **harness** file. It should describe the kernel, not
every connector. Historically each built-in connector had a dedicated typed
section there (`S3Settings`, `GCSSettings`, `PostgresSettings`, …), which meant
two problems:

1. Third-party / **generated** connectors could not add a settings section, so
   they already read config from their manifest (`config_fields`) while built-ins
   required a Pydantic section — two tiers for the same job.
2. Adding a connector meant editing a harness file that should stay
   connector-agnostic.

The pattern below removes that split. **Every built-in connector now follows it**
(s3 was the first); `Settings` carries no per-connector typed sections — only
cross-cutting harness config (`llm`, `agent`, `context`, …) plus the
`connector_config` bag and the opt-in `mcp` bridge.

---

## The two layers of config

| Layer | Lives in | Read via | Typical contents |
|---|---|---|---|
| **Harness config** | typed sections on `Settings` (`llm`, `agent`, `context`, `governance`, `sandbox`, `terminal`, `orchestration`, `ui`) | `settings.<section>` | Anything tightly coupled to the kernel. **Stays typed.** |
| **Connector config (non-secret)** | `manifest.yaml` → `config_fields`; values under `connector_config.<id>` in `config.yaml` | `ConnectorConfig(settings, "<id>")` | bucket, region, binary paths, timeouts. |
| **Connector secrets** | encrypted `.dacli/dacli.json` (`secrets` block), written by `/connect` | `core.connector_config.load_connector_config("<id>", settings=settings)` | api keys, tokens, passwords. |

`ConnectorConfig` (the non-secret reader) and `load_connector_config` (the
secrets reader) are complementary — one is the `config.yaml` side, the other the
encrypted-store side. Both are **fail-soft**: they never raise on a missing
connector or field when a default is supplied, so a connector under construction
degrades to "unconfigured" rather than crashing the load.

### What the user writes in `config.yaml`

```yaml
connector_config:
  s3:
    bucket: my-bucket
    region: us-east-1
    profile: prod        # optional
    # aws_binary, prefix, timeout fall back to the manifest defaults
```

`connector_config` is an untyped `dict[str, dict[str, Any]]` by design, so an
unknown connector id never fails a config load. `${ENV_VAR}` substitution applies
to these values like any other config (it runs recursively in `load_config`).

---

## Migrating the next connector (the S3 recipe)

To move connector `<id>` off its typed `Settings` section:

1. **Manifest** — add a `config_fields` list to `connectors/<id>/manifest.yaml`,
   one entry per field (`name`, `type`, `required`, `default`, `description`;
   mark credentials with `secret: true`). Preserve the exact field names and
   defaults from the old `Xxx Settings` class so configured installs keep working.
   `get_config_fields("<id>")` automatically sources from here once the typed
   section is gone (`registry.py` checks the typed section first, then falls
   through to the manifest).
2. **Connector** — read config through the accessor:
   ```python
   from dacli.config.settings import ConnectorConfig
   ...
   cfg = ConnectorConfig(settings, "<id>")
   value = cfg.get("field", default)   # fail-soft; cfg.field also works but
                                       # raises AttributeError on a missing key
   ```
   Replace every `getattr(settings, "<id>", None)` / `getattr(cfg, field, dflt)`
   with `ConnectorConfig` + `.get(...)`.
3. **Settings** — delete the `class XxxSettings(BaseModel)` and its
   `<id>: XxxSettings = Field(...)` line from `config/settings.py`. Grep for
   `settings.<id>` / `XxxSettings` across `src/`, `tests/`, `eval/` first — there
   must be zero references left.
4. **Tests / sims** — anywhere a stub fed `settings.<id>` (e.g.
   `eval/sim/platforms.py::sim_settings`, connector unit tests), route the same
   values through `connector_config={"<id>": {...}}` instead.
5. **Wizard** — no change needed: `setup_wizard._validate_connector` already
   validates a section-less connector against `connector_config` using the
   manifest's required fields.

S3 did exactly this — see `connectors/s3/manifest.yaml`,
`connectors/s3/connector.py`, and `tests/test_connector_config_migration.py`.

---

## Backwards compatibility (the S3 breaking change)

Because `Settings` uses `extra="ignore"`, a legacy `config.yaml` with a
top-level `s3:` block is **silently dropped** once `S3Settings` is removed. Users
must move it:

```yaml
# before (no longer read)        # after
s3:                              connector_config:
  bucket: my-bucket                s3:
  region: us-east-1                  bucket: my-bucket
                                     region: us-east-1
```

`ConnectorConfig` reads only `connector_config.<id>` — the one-release
typed-section fallback shim was removed once every built-in finished migrating
(P10), so the move above is required, not optional, for all of them. A secret a
connector collects via `/connect` (stored under `secrets.<id>`) reaches it the
same way: `_overlay_secrets` routes any non-typed-section secret into
`connector_config.<id>`.
