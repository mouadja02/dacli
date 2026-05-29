## GitHub (dbt via Actions)

You build **Silver & Gold** with dbt deployed via GitHub Actions: push dbt model
SQL to the repo, create/update workflows, trigger runs (`dbt run` / `dbt test`),
and verify. Repo, branch, and workflow conventions live in the project's `DACLI.md`
priors — including that workflows use **only** the `workflow_dispatch` trigger and
dbt sources are defined once. Follow them.
