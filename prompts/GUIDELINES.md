# Agent Guidelines & Rules

1. **Snowflake Object Creation**: Use `CREATE OR REPLACE` or `CREATE IF NOT EXISTS` for idempotency.
2. **File Formats**:
   - `SKIP_HEADER` and `PARSE_HEADER` are mutually exclusive.
   - `PARSE_HEADER = TRUE` is ONLY for `INFER_SCHEMA` or `MATCH_BY_COLUMN_NAME`. 
   - **CRITICAL**: Do NOT use any file format with `PARSE_HEADER = TRUE` (like `INFER_CSV_FORMAT`) in `SELECT` statements. This causes error `002005 (42601)`. Use a standard `CSV_FORMAT` (with `PARSE_HEADER = FALSE`) for querying/previewing data.
3. **Querying Staged Files**:
   - NEVER use `SELECT *` on CSVs. Use `SELECT $1, $2...`.
   - Use Named File Formats instead of inline definitions to avoid constant argument errors.
4. **GitHub Actions Workflows**:
   - **CRITICAL**: Workflows must have ONLY ONE trigger: `workflow_dispatch`.
   - **NEVER** use `on: push` or `on: schedule`. The agent triggers workflows manually as needed.
   - Example correct trigger:
     ```yaml
     on:
       workflow_dispatch:
     ```
5. **dbt Sources**:
   - **CRITICAL**: Each source must have a UNIQUE name across ALL schema files.
   - Do NOT define the same source in multiple files (e.g., `models/schema.yml` AND `models/sources.yml`).
   - Define sources ONCE in a single file (recommended: `models/sources.yml`).
   - Schema files (`schema.yml`) should only contain model configurations, tests, and documentation.
6. **Python Package Installation**:
   - Do NOT pin package versions in `pip install` commands (e.g., avoid `pip install dbt-snowflake==1.8.4`).
   - Use unpinned versions to let pip resolve dependencies automatically: `pip install dbt-snowflake`.
   - This prevents version conflicts and ensures compatibility.
