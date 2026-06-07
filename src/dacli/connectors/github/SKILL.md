---
name: github
description: Manage files and Actions workflows on GitHub under tiered governance.
---

# GitHub connector

Read/write repository files and operate Actions workflows (trigger, monitor,
pull failure logs) — the build→deploy→operate loop for dbt projects.

## Operations
- `list_github_directory`, `read_github_file` *(safe; re-verify live state)*.
- `push_github_file` *(write)*, `delete_github_file` *(irreversible)*.
- `trigger_github_workflow` *(risky)*, plus run/jobs inspection *(safe)*.

## Governance
- **Post-conditions:** `push_commit_landed` (commit SHA reachable + content hash
  matches), `delete_removed_file` (file gone after delete),
  `workflow_run_concluded` (triggered run reached a successful conclusion).
- **Rollback:** revert commit / restore prior blob by SHA. `verify_rollback`
  confirms a restorable blob SHA exists before allowing a delete.
- **Scope:** ships `read_only`; grant `write`/`admin` per deployment.

## CLI path
The `gh` CLI is a first-class alternative for many of these operations; this
connector uses the REST API for streaming workflow logs.

## Golden task
Push a file, then re-read it off the branch and confirm the stored content hash
matches what was pushed (the `push_commit_landed` post-condition).
