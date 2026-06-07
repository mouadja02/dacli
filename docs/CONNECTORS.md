# Connectors

A **connector** is a self-describing capability provider for one platform. It is a plugin: a folder under
`connectors/` discovered at startup from its `manifest.yaml`. Adding one never touches the kernel, the
reasoning layer, or governance.

## The catalog

dacli ships 14 platform connectors plus the `system` built-in and the `diagram_mermaid` skill.

| Connector | Module | Default scope | Mutating ops | Native rollback primitive |
|---|---|---|---|---|
| `snowflake` | `connectors/snowflake` | read-only | SQL | Time Travel / `UNDROP`, zero-copy clone |
| `bigquery` | `connectors/bigquery` | read-only | SQL | table snapshot / time travel (+ `dry_run` gate) |
| `databricks` | `connectors/databricks` | read-only | SQL | Delta time travel / shallow clone |
| `dbt` | `connectors/dbt` | read-only | `run`/`build` | git-versioned transform + target snapshot |
| `s3` | `connectors/s3` | read-only | put / delete | versioned copy-aside |
| `gcs` | `connectors/gcs` | read-only | put / delete | object versioning |
| `postgres` | `connectors/postgres` | read-only | SQL | transaction / `pg_dump` |
| `mysql` | `connectors/mysql` | read-only | SQL | transaction / `mysqldump` |
| `mongodb` | `connectors/mongodb` | read-only | writes | `mongodump` copy-aside |
| `dynamodb` | `connectors/dynamodb` | read-only | item/table ops | point-in-time recovery |
| `airflow` | `connectors/airflow` | read-only | trigger / pause | unpause / gated |
| `dagster` | `connectors/dagster` | read-only | launch run | gated (terminate) |
| `github` | `connectors/github` | read-only | push / delete | revert commit / restore blob |
| `pinecone` | `connectors/pinecone` | read-only | — | n/a |

> **Least privilege by default.** Every connector starts at `read_only`. Write/risky/admin scope is opt-in
> per connection profile — even if the model asks a connector to write, the request is denied unless the scope
> was explicitly granted. See [GOVERNANCE.md](GOVERNANCE.md#permissions).

CLI-first connectors shell out to the platform's first-class CLI through an **injectable runner**, so the same
code that runs live can be driven by canned process output in tests (and in the [eval simulator](EVALUATION.md)).

---

## Anatomy of a connector

```text
connectors/<platform>/
├── connector.py     # the Connector subclass
├── manifest.yaml    # discovery metadata + golden task
└── SKILL.md         # progressive-disclosure documentation
```

### `connector.py`

```python
from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.core.verify import result_succeeded, PostCondition, VerificationContext

class MyPlatformConnector(Connector):
    name = "myplatform"

    def operations(self) -> list[OperationSpec]:
        return [
            OperationSpec(
                name="execute_myplatform_query",
                description="Run a query.",
                parameters={"type": "object",
                            "properties": {"query": {"type": "string"}},
                            "required": ["query"]},
                capability="myplatform.query",
                risk=Risk.RISKY,
                category="query",
                postconditions=[result_succeeded(), my_anchored_check()],
            ),
            # ...plus a read-only introspection op.
        ]

    async def invoke(self, op: str, args: dict) -> ToolResult: ...
    async def health(self) -> ToolResult: ...

    # Required for any IRREVERSIBLE op: prove the undo path exists live.
    async def verify_rollback(self, plan, args) -> tuple[bool, str]: ...
```

CLI-first connectors subclass `CliConnector` instead, which centralizes the injectable runner, `health`,
timing, and uniform `ToolResult` construction.

### `manifest.yaml`

```yaml
id: myplatform
name: My Platform
description: What it does
icon: "🔌"
class: dacli.connectors.myplatform.connector.MyPlatformConnector
required_config: [api_key]
enabled: false                # the wizard turns it on
default_scope: read_only      # least privilege
golden_task:
  name: Create an object and confirm it exists
  op: execute_myplatform_query
  description: Run a CREATE, then confirm via introspection that the object exists.
  expectation: the anchored post-condition passes.
```

---

## The Definition of Done (CI-enforced)

The thesis is that pouring connectors onto the spine is only safe because capability (𝒮) and its governance
counterpart (𝒢) ship **together, always**. `connectors/dod.py` turns that promise into a *mechanical* check
that `tests/test_connector_dod.py` runs over every discovered connector — and CI fails the build if any rule
is unmet. This is the structural cure for **governance debt**.

A connector ships only when **all** of the following hold:

1. **Manifest** — `id`, `name`, `description`, `class`, `enabled`, `required_config`, and a declared
   `default_scope` (read-only by default).
2. **Operations** — at least one, each with a JSON-schema `parameters` object.
3. **Post-conditions** — every op declares ≥1; every *mutating* op (write/risky/irreversible) declares ≥1
   **anchored** check that is more than bare `result_succeeded` — it asks the *environment*, not the model.
4. **Introspection** — a read-only op that can re-verify live state (and feed the catalog).
5. **Rollback** — any connector with a mutating op has a registered native rollback planner; any connector
   exposing an *irreversible* op also implements `verify_rollback` (so the path can be *proven*, not assumed).
6. **SKILL.md** — a progressive-disclosure doc next to the connector.
7. **Golden task** — a verifiable outcome declared in the manifest, referencing a real operation.

Run it locally:

```bash
python -m unittest tests.test_connector_dod -v
```

---

## Risk tiers and the environment-as-oracle

Each operation declares a `Risk` hint, but the *real* tier is derived at runtime from grounded signals (the
SQL verb, the target environment) — see [GOVERNANCE.md](GOVERNANCE.md). The post-conditions are where the
"environment is the oracle" idea becomes concrete:

| Platform | The oracle for a mutation |
|---|---|
| BigQuery | `bq show` confirms a `CREATE`; `dry_run` previews bytes/validity |
| Databricks | the statement **state** (`SUCCEEDED`/`FAILED`) |
| S3 / GCS | a live `head-object` / `ls` after the put/delete |
| dbt | `run_results.json` (every node materialized, every test passed) |
| Snowflake | information-schema / catalog re-introspection |

A result is accepted as "done" **only** when the platform confirms the intended state change — fluent success
from an API call is never enough.

---

## Adding a connector — checklist

1. `connectors/<platform>/connector.py` — implement `operations()`, `invoke()`, `health()` (+ `verify_rollback`
   for any irreversible op).
2. `connectors/<platform>/manifest.yaml` — id, class, `required_config`, `default_scope`, `golden_task`.
3. `connectors/<platform>/SKILL.md` — what it does and when to use it.
4. Register a native rollback planner in `governance/rollback.py` if it has mutating ops.
5. Add config to `config/settings.py` if it needs new credentials.
6. Add a golden test (offline, with an injected fake runner) under `tests/`.
7. Run the DoD gate and the full suite.

The registry discovers it on startup, the wizard offers it, and its operations become tools the agent can call.
