# Project plan

The milestone definitions and acceptance outcomes originate in
`docs/ARCHITECTURE.md`. This file records execution state and verification so a
new Codex context can continue without relying on chat history.

| Milestone | Status | Outcome |
| --- | --- | --- |
| M0 | Complete (`8594b63`) | Loki push ingest, durable journal acknowledgement, MemTable, and the initial LogQL/query APIs |
| M1 | Complete locally (`2b51b29`) | Parquet parts, bloom/stream indexes, restart recovery, unified query, and merge |
| M2 | Complete | Object-store publication, conditional manifest updates, local cache eviction, and query restoration |
| M3 | Complete (review remediated) | JSON/logfmt parsing, metric queries, field-filter push-down, and bounded query execution sufficient for real dashboards |
| M4 | Complete | OTLP trace ingest, trace-ID lookup, and Tempo-compatible APIs |
| M5 | In progress | Compaction tuning, retention, resource limits, and load validation against explicit targets |
| M6 | Pending | Graceful shutdown for machine replacement (SIGTERM handling, forced flush-to-S3, drain-status readiness) and a machine-replacement rehearsal |

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

## M3 acceptance checklist

- [x] LogQL has an ordered AST for line filters, `json`, `logfmt`, and field
  comparisons using `=`, `!=`, `=~`, `!~`, `<`, `<=`, `>`, and `>=`.
- [x] JSON and logfmt extract scalar fields deterministically; structured
  metadata participates in field filtering, and malformed parser input has a
  consistent error-field behavior instead of aborting an entire scan.
- [x] String, regex, exact-decimal numeric, and duration filters return
  identical results for MemTable and flushed-part data. Numeric field values
  are rejected when they are not finite decimal values; the 2^53 boundary is
  covered by regression tests.
- [x] `rate`, `count_over_time`, and `bytes_over_time` implement `(t-range, t]`
  windows without inheriting the log query limit.
- [x] `sum`, `avg`, `min`, and `max`, with optional `by (...)`, plus `topk`,
  operate independently at every evaluation timestamp.
- [x] Range metrics return Loki `matrix` results and instant metrics return
  `vector` results; log queries remain `streams`.
- [x] Metric lookback scans and restores data before the HTTP start time, and
  invalid steps/ranges or unsupported expressions return clear client errors.
- [x] Positive exact field equality can conservatively prune new parts or row
  groups, including canonical numeric/duration values before remote-body
  restoration, while old M1/M2 parts load and fall back to scanning without
  false negatives.
- [x] A fresh-context review reports no blocking findings.
- [x] The complete required validation set passes after the final review fix.

M3 explicitly defers `line_format`, `label_format`, `unwrap`,
`quantile_over_time`, binary/vector operators, `without`, offsets, subqueries,
deduplication for crash-replay duplicates, and Parquet range reads. Metric
queries have bounded evaluation points, scanned rows, output samples, and
series; concurrent scans/evaluations are scheduled and timed out, but the
bounded row set is still evaluated in memory rather than using streaming or
pre-aggregation. Structured metadata is promoted to metric labels only when a
`by (...)` clause names the field, with a series cardinality limit. JSON
extraction currently supports scalar/object fields; top-level arrays and null
values are ignored or produce parser-error behavior rather than Loki's full
JSON semantics. Empty-string equality, stream-label fields, and synthesized
`_extracted` collision names remain conservative and do not drive exact-field
pruning.

M3 final verification: `cargo fmt --all -- --check`, Clippy with warnings
denied, `cargo test --all-targets` (171 tests), and `git diff --check` passed.
Review remediation now scans every overlapping part before applying the global
limit, includes the first metric lookback window when `start` is omitted,
counts and bounds physical row scans, streams metric evaluation output under
the sample cap, cooperatively cancels timed-out blocking work before releasing
permits and part guards, and allows parser fields named after storage columns
in metric grouping. Query limits include a 5,000,000-row scan budget, bounded
metric output, eight concurrent log scans, four concurrent metric evaluations,
and a 30-second execution/restore timeout. Object-store and journal failures
are reflected in readiness and retried without requiring new ingest.

## M4 acceptance checklist

The detailed English implementation plan and design decisions are recorded in
[`docs/M4_IMPLEMENTATION_PLAN.md`](M4_IMPLEMENTATION_PLAN.md).

- [x] OTLP gRPC accepts valid ResourceSpans/ScopeSpans and preserves span data.
- [x] Invalid IDs, timestamps, and oversized requests are rejected without partial ingestion.
- [x] OTLP acknowledgement follows durable WAL append.
- [x] Legacy Loki WAL records replay after the WAL format extension.
- [x] Trace data survives flush and process restart.
- [x] Trace-ID Bloom pruning never produces false negatives.
- [x] Lookup combines memtable and immutable parts across partitions.
- [x] Evicted trace bodies restore from the object store on demand.
- [x] Tempo trace-by-ID and bounded search APIs match the implemented compatibility shape.
- [x] Existing Loki ingest, LogQL, object-store, and readiness tests remain green.
- [x] A fresh-context review reports no blocking findings.
- [x] The complete validation set passes after the final review fix.

## M5 acceptance checklist

The implementation plan and provisional validation targets are recorded in
[`docs/M5_IMPLEMENTATION_PLAN.md`](M5_IMPLEMENTATION_PLAN.md).

- [x] M4 review and complete validation are green.
- [x] Merge parameters and query/retention limits are configurable and validated.
- [x] Compaction preserves restart/object-store correctness and stays within the agreed memory and query-disruption targets.
- [x] Expired log and trace parts are removed without deleting active or boundary data.
- [x] Loki and Tempo resource limits cover range, scan, memory, output, concurrency, and timeout.
- [ ] Reproducible load validation meets the agreed throughput, latency, memory, retention, and error-rate targets.

The sustained S3 load validation was not executed in this workspace because
no S3 credentials or S3-compatible deployment/test endpoint is available.
The mixed-workload tool is implemented, but remote restore, retention GC, and
long-running object-store behavior require an environment-level run.
- [ ] Load results and bottlenecks are documented.

## Completion protocol

For each pending milestone, replace broad outcome text with a concrete
acceptance checklist before implementation. Record only durable decisions and
verification results here; use Git history for the detailed patch record.
