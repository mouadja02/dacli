# deferred post-conditions

During (the Great Decoupling), the generic `Dispatcher` replaced the
hand-written `if/elif` ladder in `DACLI._execute_tool`. That ladder carried
**regex-driven memory mutations** as a side effect of dispatch. They were
brittle (they silently corrupted state on any non-standard SQL — e.g. a
`CREATE OR REPLACE SCHEMA`, a quoted identifier, or a multi-line statement) and
mixed catalog-tracking concerns into the dispatch path.

They have been **removed** in and must be **reimplemented as proper
post-conditions / catalog updates in ** (where they belong, driven by
structured results rather than string matching on the raw query).

## Removed behaviors (from `core/agent.py` `_execute_tool`, on a successful
## `execute_snowflake_query`)

1. **Created-schema tracking**
   - Old trigger: `"CREATE SCHEMA" in query.upper()`
   - Old action: `memory.add_created_schema(query.split(".")[-1].replace(";", "").strip())`
   -: detect schema creation from the executed statement / Snowflake
     result metadata and record the schema name reliably.

2. **Created file-format tracking**
   - Old trigger: `"CREATE" in query.upper() and "FILE FORMAT" in query.upper()`
   - Old action: `memory.add_created_file_format(query.split("FILE FORMAT")[1].split("(")[0].strip())`
   -: record created file formats from structured execution results.

3. **Created-table tracking**
   - Old trigger: `"CREATE" in query.upper() and "TABLE" in query.upper()`
   - Old action: `memory.add_created_table(query.split("TABLE")[1].split("(")[0].strip())`
   -: record created tables from structured execution results.

## Memory API still available for the reimplementation

- `AgentMemory.add_created_schema(...)`
- `AgentMemory.add_created_file_format(...)`
- `AgentMemory.add_created_table(...)`

These methods are intentionally left in place; only the *automatic, regex-based
invocation* from the dispatch path was removed.

## Resolution (— done)

All three behaviors were reimplemented as **structured catalog updates**, no
regex on the dispatch path:

- **Detection moved into the connector** (its domain). `parse_catalog_effects()`
  in `connectors/snowflake/connector.py` derives a list of structured
  `{"action", "object_type", "scope"}` effects from the *executed* statement.
  It handles `CREATE OR REPLACE`, `IF NOT EXISTS`, leading modifiers
  (`TRANSIENT`/`TEMP`/…), fully-qualified and quoted identifiers, and multi-line
  SQL — the exact cases that silently corrupted the old regex. The effects ride
  back on `ToolResult.metadata["catalog_effects"]`.
- **Application moved into the dispatcher** (`Dispatcher._apply_catalog_effects`),
  driven by structured results and **gated on `OperationSpec.risk`**: a
  `create`/refresh effect applies on any successful op; an `invalidate` effect
  applies only when the op's declared risk is `write`/`risky`/`irreversible`
  (this is where 's risk metadata first earns its keep — write-invalidation).
- **Storage moved into the catalog cache** (`memory/catalog.py`): created objects
  become `CatalogEntry` records with `last_verified` + TTL; DROP/DML invalidate
  the matching scope (hierarchically — a schema invalidation cascades to its
  tables). The legacy `add_created_*` / `add_loaded_table` methods now write
  catalog entries instead of mutating pipeline-specific lists.

See `tests/test_memory_phase2.py` (`CatalogEffectParseTest`,
`DispatcherPostconditionTest`) for coverage of the non-standard-SQL cases and
the risk-gated invalidation invariant.
