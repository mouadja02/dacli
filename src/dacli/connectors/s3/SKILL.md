---
name: s3
description: List, read, upload, and delete S3 objects with versioned rollback.
---

# S3 connector

Object storage for lake ingestion and the staging source for warehouse loads.
CLI-first via `aws`.

## Operations
- `list_s3_objects` *(safe; introspection)*.
- `read_s3_object` *(safe)*.
- `put_s3_object` *(write)*.
- `delete_s3_object` *(irreversible)*.

## Governance
- **Post-conditions:** `s3_object_present` (live head-object confirms a put
  landed), `s3_object_absent` (live head-object confirms a delete),
  `lists_objects`.
- **Rollback:** bucket **versioning** / copy-aside (`versioned_copy_aside`).
  `verify_rollback` confirms bucket versioning is enabled before a delete, so a
  prior version can be restored.
- **Scope:** ships `read_only`; grant wider per deployment.

## CLI path
`aws s3 cp s3://<bucket>/<key> -`, `aws s3api list-objects-v2|head-object|
delete-object|get-bucket-versioning`.

## Golden task
Upload content to a key, then confirm via head-object that the key exists.
