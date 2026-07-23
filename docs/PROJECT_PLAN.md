# Project plan

The milestone definitions and acceptance outcomes originate in
`docs/ARCHITECTURE.md`. This file records execution state and verification so a
new Codex context can continue without relying on chat history.

| Milestone | Status | Outcome |
| --- | --- | --- |
| M0 | Complete (`8594b63`) | Loki push ingest, durable journal acknowledgement, MemTable, and the initial LogQL/query APIs |
| M1 | Complete locally (`2b51b29`) | Parquet parts, bloom/stream indexes, restart recovery, unified query, and merge |
| M2 | Complete | Object-store publication, conditional manifest updates, local cache eviction, and query restoration |
| M3 | Pending | JSON/logfmt parsing, metric queries, and field-filter push-down sufficient for real dashboards |
| M4 | Pending | OTLP trace ingest, trace-ID lookup, and Tempo-compatible APIs |
| M5 | Pending | Compaction tuning, retention, resource limits, and load validation against explicit targets |
| M6 | Pending | Read replica, manifest following, fenced promotion, and a machine-replacement rehearsal |

## Repository state note

Local `master` and `origin/master` contain different M1 commits based on the
same M0 commit. Continue from local `2b51b29`; do not reconcile or rewrite that
divergence without an explicit user decision.

## M2 acceptance checklist

- [x] A flushed part is uploaded before it becomes visible in the manifest.
- [x] Manifest replacement uses conditional object-store writes and rejects a
  competing replacement.
- [x] Startup restores the manifest catalog and safely reconciles interrupted
  uploads and local merge tombstones.
- [x] Independent local-only merge trees migrate into an initially empty
  manifest without losing or duplicating a tree.
- [x] Eviction retains metadata and indexes while removing least-recently-used
  Parquet bodies to the configured bound.
- [x] A query restores only matching evicted parts and succeeds afterward.
- [x] Object-store durability permits WAL prefix compaction without losing
  acknowledged suffix records.
- [x] A fresh-context review reports no blocking findings.
- [x] The complete required validation set passes after the final review fix.

Actual S3 credentials or an S3-compatible test endpoint are not stored in this
repository. In-memory and local-file backends exercise the object-store
contract in automated tests; live S3 validation remains an environment-level
deployment check.

M2 final verification: `cargo fmt --all -- --check`, Clippy with warnings
denied, all 134 tests, and `git diff --check` passed. A process-level local-file
object-store run also restored an evicted Parquet body and returned the expected
query results. The final fresh-context review reported no blocking findings.

## Completion protocol

For each pending milestone, replace broad outcome text with a concrete
acceptance checklist before implementation. Record only durable decisions and
verification results here; use Git history for the detailed patch record.
