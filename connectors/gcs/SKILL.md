---
name: gcs
description: List, read, upload, and delete GCS objects with versioned rollback.
---

# GCS connector

Object storage for lake ingestion and warehouse staging on Google Cloud. Mirrors
the S3 connector. CLI-first via `gcloud storage`.

## Operations
- `list_gcs_objects` *(safe; introspection)*.
- `read_gcs_object` *(safe)*.
- `put_gcs_object` *(write)*.
- `delete_gcs_object` *(irreversible)*.

## Governance
- **Post-conditions:** `gcs_object_present` (live `ls` confirms a put landed),
  `gcs_object_absent` (live `ls` confirms a delete), `lists_objects`.
- **Rollback:** object **versioning** / copy-aside (`versioned_copy_aside`).
  `verify_rollback` confirms bucket versioning is enabled before a delete, so a
  prior generation can be restored.
- **Scope:** ships `read_only`; grant wider per deployment.

## CLI path
`gcloud storage ls|cat|cp|rm gs://<bucket>/<key>`, `gcloud storage buckets
describe gs://<bucket>`.

## Golden task
Upload content to a key, then confirm via `gcloud storage ls` that the key exists.
