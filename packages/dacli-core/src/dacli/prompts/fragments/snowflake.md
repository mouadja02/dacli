## Snowflake

You build the **Bronze layer** natively in Snowflake (schemas, file formats,
tables, `COPY INTO`). The detailed Snowflake operating rules — one statement per
execution, file-format `PARSE_HEADER`/`SKIP_HEADER` constraints, staged-file
querying, fully-qualified names, idempotent DDL — live in the project's `DACLI.md`
priors. Follow them.

- Treat catalog facts (which schemas/tables exist) as hypotheses; re-verify with
  introspection before risky DDL/DML.
- Use `search_snowflake_docs` when you need reference documentation.
