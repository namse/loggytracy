# loggytracy architecture

A single-machine log and trace engine written in Rust. It combines VictoriaLogs' logical design with
the Parquet physical format and S3 tiering.

This document records what the engine *is*. [`VISION.md`](VISION.md) records what it is *for* — the three
invariants that are load-bearing, what is deliberately not built, and what would falsify the claim. Where
the two disagree, `VISION.md` is the intent and this one is the implementation.

[`COMPARISON.md`](COMPARISON.md) records how it currently measures against Loki on the same machine, at the
same container memory limit, over the same corpus. **The claim in `VISION.md` does not survive that run**:
loggytracy loses `| json | field="x"`, its own headline query, by 1.85x. It no longer loses on memory —
both systems survived a 2 GiB container, where the first run had to be published at 8 GiB because
loggytracy was OOM-killed at 2.
The bed is [`compare/`](../compare/), the raw artifacts are in [`artifacts/m9/`](artifacts/m9/), and the
document is regenerated from them rather than written. Read it before trusting any performance statement
elsewhere in these docs.

## Decided choices

| Item | Decision |
|---|---|
| Deployment | Single machine, single writer |
| Source of truth | S3-compatible object storage |
| Local disk | Cache (LRU eviction) |
| Memory | **One declared budget** (`LOGGYTRACY_MEMORY_BUDGET`) divided into ingest/flush/merge/query/sidecar arenas, each with its own accounting and its own refusal. Not a set of independent limits whose product is discovered afterwards — see [`VISION.md`](VISION.md) invariant I |
| Format versioning | **None.** Nothing on disk or on the wire is versioned and no build reads another's data — see [`VISION.md`](VISION.md), "What is deliberately not built". Changing a format means changing it; a stale data directory is deleted |
| Execution engine | **Hand-written.** DataFusion is rejected: its memory is not accountable at arena granularity, and the LogQL surface is small enough that a planner for it is smaller than the integration |
| Durability | Journal (append-only) + group commit + ack after fsync. Alloy WAL is assumed as a safety net |
| Replication | No replicas. For unexpected server or disk loss, the accepted loss window (RPO) is determined by the flush interval (`flush_max_bytes`/`flush_max_interval`, whichever comes first: 1 MiB/5 s by default), and this is intentionally accepted |

## Durability and recovery semantics

- **WAL + checkpoint invariant**: An acked record is always in the WAL and is also inserted into the memtable (the writer task inserts it after a successful write and immediately before ack). Therefore, the `(offset, memtable snapshot)` captured by `checkpoint()` is atomically consistent.
- **Recovery**: On startup, replay only `[checkpoint..replay_end]` from the WAL into the memtable and truncate a corrupt or partial tail at `replay_end`. Do not advance checkpoint during recovery — in-flight data exists only in the memtable, so checkpoint stays unchanged until the next flush's `checkpoint()` records the correct offset. Thus, in-flight data survives repeated "restart -> restart" cycles.
- **At-least-once (flush boundary)**: If the process crashes after flush completes part disk writes but before `set_checkpoint`, the part and the next replay can both contain the same data. This is an intentional durability-over-correctness trade-off; duplicates can appear in query results and deduplication is deferred to a later milestone.
- **Flush visibility boundary**: Queries hold the part/memtable operation read lock for the full scan, while flush performs part registration and flushing-buffer commit under the same operation write lock. A metric or log query overlapping a normal flush therefore does not count the same row in both memtable and part. At-least-once recovery duplicates remain possible when flush stops after part commit but before checkpoint, and durable deduplication is deferred to a later milestone.
- **Single writer, enforced rather than assumed**: the manifest carries a `writer_epoch`. A starting writer takes the next one, and every subsequent manifest CAS re-checks the epoch it loaded (`object_storage/object_io.rs`, `check_epoch`), so a previous instance that is still running — the split-brain a "single writer" deployment note cannot prevent on its own — fails its next write instead of interleaving with the new one, and stops rather than retrying forever.
- **Two lifecycle locks, and their order is load-bearing**: the *operation* lock is visibility — a query holds its read half for a whole scan, and flush, merge commit and part retirement take its write half. The *deletion* lock guards part files against removal, and nothing else: a merge rewrite holds its read half for as long as its group takes, because the rewrite needs its inputs to exist but does not care what is visible. **Deleters take both, deletion first.** Retention and cache eviction took the operation lock first until 2026-08-11, which meant a deleter's wait for a merge rewrite was served holding the lock every query needs: all 39 freezes across four one-hour soaks began on a retention tick, the longest 52 s, one of them to delete a single part. Deletion-then-operation is also merge's own order, so no cycle remains (`retention.rs`, `part_registry.rs`'s `deletion_lock` doc, and `a_retention_pass_waiting_for_a_merge_does_not_stop_queries`).
- **Merge replacement invariant**: A merge tombstone is recorded before the new part directory is renamed from `.tmp` to its final location. Restart recovery deletes old parts only after successfully opening the new part; if validation fails, old parts are retained.
- **Merge tombstone chain recovery**: On restart, collect all tombstone relationships first, follow them transitively through earlier generations, and then clean up old parts. Thus, even when merges across generations overlap after a failed deletion, deleting an intermediate tombstone cannot resurrect an earlier-generation part.
- **WAL compaction**: With object storage enabled, the writer task removes the WAL range before checkpoint after part upload and manifest CAS succeed. Checkpoint is reset to 0 before replacement, so a crash during compaction replays either the entire old WAL or the new suffix; duplicates are possible but data is not lost. Local-only mode preserves the existing offset checkpoint.
- **Unexpected disk loss**: If the local disk is lost entirely, WAL/MemTable data not flushed after the last successful flush (S3 upload + manifest update) cannot be recovered from the server side. This loss window is determined by `flush_max_bytes`/`flush_max_interval` (1 MiB/5 s by default, whichever comes first), and this level of loss is intentionally accepted without replicas. Planned hardware replacement follows the graceful-shutdown procedure below so this window does not apply.

| Physical format | Parquet (dictionary + zstd) + sidecar index files |
| Indexes | Stream index + per-block trigram bloom filter (no inverted index) |
| Query language | LogQL — high-usage subset only, with clear errors for unsupported syntax |
| API | Loki HTTP API compatible (direct Grafana Loki data source), with the Tempo API for traces |
| Ingest protocols | **OTLP only**, over gRPC (`:4317`) or HTTP (`POST /v1/logs`, `/v1/traces`), traces and logs. Loki push is removed — see [`VISION.md`](VISION.md), "Ingest is OTLP" |
| Query protocol | **Loki HTTP API, unchanged.** The ingest decision above does not touch it: Grafana's Loki data source is how anyone reads this engine, and a query protocol and an ingest protocol are separate choices |
| Transport security | **TLS is unsupported.** Only plain HTTP/gRPC is provided; a reverse proxy or service mesh handles end-to-end encryption |
| Multi-tenancy | Multi-tenant. `X-Scope-OrgID` identifies tenants, and tenants are the unit of throttling and quota |
| Validation environment | **Do not test against S3** (neither real cloud nor local MinIO). Trust the `object_store` crate and test our code closely up to the crate boundary |

## Transport security — TLS unsupported

TLS termination is not this process's responsibility. Certificate issuance, renewal, SNI, and mTLS
policies belong to layers that already handle them well (reverse proxy, ingress, or service mesh),
while the engine provides only plain HTTP and plain gRPC. S3 access in the storage layer uses HTTPS
through `object_store`, so it is unaffected by this decision.

Therefore, deployments must satisfy the following requirements.

- Keep the listening address inside the trust boundary. Direct exposure to a public network is unsupported.
- Perform authentication and authorization outside this process (at a proxy or gateway), and pass the
  proxy-verified tenant as `X-Scope-OrgID`. The engine trusts this header — **tenant isolation fails if
  the engine is directly reachable from a network location where the header can be forged.**
- The listeners bind loopback by default. A `0.0.0.0` bind is a deliberate configuration and belongs only behind something that terminates TLS and authenticates.

## Validation environment — do not test against S3

Do not connect to real cloud storage because of cost, or to local MinIO because **we trust the
`object_store` crate**. Correct S3 protocol implementation is the crate's responsibility and it has
its own test suite. Load validation uses only in-process latency and fault injection.

The validation principle is **test only our code**. Whether the S3 wire protocol, conditional PUT
semantics, and multipart behavior are correctly implemented belongs to the `object_store` crate, so we
do not revalidate them here. Our responsibility is one layer below — **whether the store created by this
binary with this configuration actually performs CAS** — and that is checked by a startup preflight,
not a load test (see below).

Therefore, load runs observe our loop behavior (flush progress, bounded backlog, backpressure, stable
RSS, and startup time), and none of these items depends on the object-store backend. The following risks
remain nonetheless.

- **Latency tail** — Loopback is below 1 ms while real S3 p99 is hundreds of milliseconds to seconds.
  The latency-injection wrapper is backend-agnostic, so running it over MinIO can provide both real wire
  behavior and a tail, and the load script does this by default. However, injected values are assumptions,
  not measurements, and because injection occurs above the `object_store` client, its retry layer is not
  validated. **This is the largest remaining risk.**
- **Throttling** — S3 limits request rates per prefix while MinIO does not. With the default configuration,
  the manifest is about 0.2 PUT/s and the total is about 10 PUT/s, four orders of magnitude below the limit,
  so the actual risk is small.
- **Cost** — This design has been dominated by R2 Class A costs, but MinIO provides no cost signal.
  Amounts cannot be measured locally, but operation counts can.
- **Provider-specific conditional PUT semantics** — CAS working on MinIO does not imply that it works on R2.

**CAS preflight.** At startup, `ObjectStorage::verify_conditional_put` checks with a probe object that
*a write that should be rejected is rejected*, and refuses startup otherwise. The positive path proves
nothing — the first manifest write in an empty prefix succeeds whether conditions are honored or not.
Because this check runs against the deployment target itself, it answers the locally unanswerable question
of whether CAS works with that provider. `file://` is a development backend that intentionally gives up
CAS, so it is skipped.

The procedure and acceptance criteria are in [`LOAD_VALIDATION.md`](LOAD_VALIDATION.md).

## Multi-tenancy

Multi-tenancy is not an optional feature but **the basic unit of resource management**. Because throttles
and quotas are operated per tenant, the tenant must be a first-class identifier across ingest, storage,
and query paths.

- **Identification**: `X-Scope-OrgID` header (the Loki/Tempo convention). OTLP uses the same key in gRPC
  metadata. Validate the value against `[a-zA-Z0-9_-]{1,64}` **before** journal append because it is used
  directly in object-store keys and local file paths.
  - `LOGGYTRACY_DEFAULT_TENANT` (default `default`): Tenant applied to requests without a header.
  - `LOGGYTRACY_MISSING_TENANT_POLICY` (default `default`): `default` accepts them as the tenant above;
    `reject` returns 400. A **blank** header is rejected by both policies so client bugs are not silently
    routed to another tenant.
- **Isolation point**: The tenant is **not** a storage-path partitioning axis, but a sort and index key
  inside each part. One part object contains all tenants, rows are sorted by `(tenant, timestamp_ns)`,
  and row groups never cross tenant boundaries. The tenant index in `meta.json` contains each tenant's row
  group ranges and min/max timestamps, and every read path **cannot address** a row group outside that range.
  The previous design that used tenants as a path axis was discarded because of R2 Class A costs — see
  [`docs/MULTI_TENANCY_DESIGN.md`](MULTI_TENANCY_DESIGN.md) for the cost model and rationale.
- **Retention**: Retention is a plan property and varies by tenant. The control plane pushes one tenant at
  a time, and loggytracy returns success only after storing it in object storage. It applies at **deletion**
  time, not write time, so plan upgrades and downgrades affect already-recorded data. A tenant never pushed
  is **policy-unknown, and unknown means retain**. Tenant deletion is retention `0`. See
  [`docs/RETENTION_DESIGN.md`](RETENTION_DESIGN.md) for details.
- **Throttle and quota targets**: ingest rate (bytes/s, events/s), active stream count (cardinality),
  storage capacity, concurrent query count, and query scan budget. When exceeded, ingest returns `429`
  (Alloy backs off and relies on its own WAL), while queries return `429` or `422`. Over gRPC the same
  refusal is `RESOURCE_EXHAUSTED` **carrying `RetryInfo`**: the OTLP specification makes a bare
  `RESOURCE_EXHAUSTED` non-retryable and tells the client to drop the telemetry, so the attachment is
  what makes "the client holds its data because the server declined it" true on that transport rather
  than only on HTTP. A *limit* violation is the opposite instruction — permanent for that batch — and
  answers `INVALID_ARGUMENT` with no `RetryInfo`.
- **Observability**: Expose every quota and rejection counter on `/metrics` with a tenant label. Operating
  quotas requires visibility into "who was blocked, where, and by how much."
- **Current state**: Identification, validation, and isolation are implemented. `X-Scope-OrgID` is extracted
  from OTLP HTTP and gRPC and recorded in WAL records (the owner survives restart), while MemTable,
  part, trace part, query, and catalog reads all require a tenant argument. Only `/metrics` retains a
  process-wide operator aggregation. **Per-tenant quotas, throttles, durable usage accounting, and tier
  partitioning** remain; these are steps 5 and 6 in `docs/MULTI_TENANCY_DESIGN.md`
  (see P0-3 in `docs/PRODUCTION_READINESS_REVIEW.md`).

## Data model

- Logs and spans are unified as wide events consisting of "timestamp + field set" (following the OTel data model).
- Fields have two layers:
  - **stream fields**: Low cardinality (`app`, `host`, and so on). They identify streams, are indexed in the stream index, and correspond to LogQL labels.
  - **General fields**: High cardinality is allowed (`user_id`, `trace_id`, and so on). Store them in columns and prune with bloom filters; they correspond to LogQL pipeline filters such as `| user_id="123"`.
- Detect value types at ingest (numbers, timestamps, IPs, and so on) and store them in typed columns rather than as strings.

## Write path

```
Alloy ──▶ Ingest API
            │
            ▼
         Journal append (sequential write, group commit: N MB or T ms, whichever comes first)
            │ Batch ack after fsync
            ▼
         MemTable (Arrow RecordBatch, immediately queryable)
            │ Size/time-based flush (deferred independently of ack)
            ▼
         Part creation (immutable): Parquet + sidecars
            │
            ▼
         S3 upload → manifest update → journal truncate
```

- Crash recovery = journal replay. Part size is independent of ingest speed.
- Background merge: small parts → large parts (LSM-style). Daily time partitions.
- Sort out-of-order timestamps during merge. The allowed window for late data outside partition boundaries is configurable.

## Part structure

A part is one immutable directory:

- `data.parquet` — Schema built from fields actually present in the part (dynamic per-part schema): a
  `_stream` u32 ordinal naming the row's label set (the sets themselves live once in `meta.json`'s
  stream table, in ordinal order), one `_sm:<key>` column per structured-metadata key up to a cap of 128
  chosen by row count, and a residual JSON column for the rare keys past the cap — null for every row
  whose keys all made columns, which is every row the intended consumer sends.
- Trigram bloom-filter sidecar — Per row group. Three-grams from text columns such as `_msg`, used to prune substring searches (`|=`). Word queries are also covered by trigrams.
- Stream-index sidecar — Stream fields → row-group postings (roaring bitmap).
- Metadata file — Time range, row count, field list, min/max, and CRC32 for each part file. Metadata itself is also checked with CRC32; inconsistent parts are not loaded.

Bloom filters are only for pruning; the final decision always comes from scanning blocks, so the scan guarantees correctness.

### What a part keeps in memory, and what it re-reads

Every structure above is durable in the part directory, which is what lets the
resident half of it be a *cache* rather than a commitment. Three mechanisms
younger than the rest of this document:

- **Sidecars are evictable, in halves.** The bloom half — the megabytes — lives
  under one process-wide LRU byte budget (`sidecar_cache_max_bytes`, 10% of the
  declared budget) and is re-read from `index.bin` on the next pruning query. The
  stream index — the kilobytes — stays resident, so the metadata paths that
  cannot fail stay infallible, and a matchers-only query never touches a bloom at
  all. Answer equality under eviction is pinned by a test that forces every open
  to evict every other part. Before this the sidecars grew with the part count
  and nothing evicted them, which is what ended the first day-long soak at
  630 MiB of sidecar in a 2 GiB container.
- **Decoded row groups are cached, and so are narrow-pass outcomes.**
  `part/group_cache.rs` keeps decoded groups for reuse across scans under
  `row_group_cache_max_bytes` (12.5% of the budget), keyed so that a selection is
  served by slicing what is already decoded. Beside it, the narrow pass — the
  per-row evaluation for rows a group-level filter could not exclude — memoizes
  its outcome per (immutable part, group, predicate), so a repeated rare query
  pays it mostly on groups holding nothing. Both are bounded and both are
  droppable: the cost of a miss is a re-read, never a wrong answer.
- **Row groups are decode units, and that sets their size.** A group must be
  decoded to read any of it, so larger groups mean more wasted decode per match:
  measured at 1.5 M rows and a 100-row answer, 8× larger groups cost **3.75×**
  the time (`benches/scan_scaling.rs`). The 8192-row default is on the right side
  of that curve, and the ceiling is 65 536 rows regardless — a group's bloom
  windows are 1024 rows each and the selection mask is a `u64`.

## Read path

LogQL parsing (chumsky) → plan → pruning in this order:

1. Time range → partition/part selection (manifest + part metadata)
2. Label matchers → row-group pruning with the stream index
3. Line filters (`|=`, `|~`) and structured-field filters → row-group pruning with trigram blooms
4. Scan only remaining row groups (MemTable + local parts + S3 range reads)

- This engine's differentiator is accelerating Loki's slow `| json | field="x"` pattern by push-down
  bloom pruning when the field was columnized at ingest. Push-down is part of the planner design from the start.
  **Measured against Loki and VictoriaLogs since**, on an equal container limit and the same corpus:
  [`COMPARISON.md`](COMPARISON.md) at 150 k rows and
  [`COMPARISON_LARGE_CORPUS.md`](COMPARISON_LARGE_CORPUS.md) at 1.5 M, both generated by `compare/run.sh`
  with a row-equality gate that withholds any timing whose answers disagreed. The claim and what would
  falsify it are stated in [`VISION.md`](VISION.md), which also scopes what the ten-times run changed.
- DataFusion was considered as the execution engine and is **rejected** — see "Decided choices" above.
  Execution is a merge of per-part sorted iterators feeding a bounded top-K heap, so that a query's memory
  is `limit` rows plus the heap rather than everything the window matched. **It is that now**: the log path
  passes the request's own `limit` into the scan alongside a scan-row budget, a scanned-byte ceiling and a
  memory reservation (`query/execution.rs`, `LogScan::new(..., limit, ...)`) — the `usize::MAX` this
  document used to name as invariant III's worst violation is gone from it. One `usize::MAX` remains and is
  a different thing: a metric query has no `limit` to stop at, since every matching row in the window
  contributes to a sample, so that path is bounded by `max_query_scan_rows`, `max_query_scan_bytes` and
  `max_metric_rows` instead of by a row count the client chose.

## S3 and manifest

- Update the manifest after a part upload completes. The manifest is versioned and uses S3 conditional writes (If-None-Match) for compare-and-swap.
- The local disk is a part cache. Keep small metadata/bloom/stream-index catalog files and LRU-evict only `data.parquet` bodies. Queries and merges download only bodies selected by time and label pruning into a verified temporary directory and pin them against eviction while reading. Parquet range-read optimization is deferred.
- Hardware replacement (graceful shutdown): 1) on SIGTERM, immediately block the ingest endpoint (reject new requests); 2) drain until accepted in-flight requests finish WAL append/ack; 3) force-flush the MemTable accumulated by then and verify S3 upload and manifest update completion; 4) terminate the process, discard the disk, and switch hardware. Data Alloy tried to send after blocking is retried from Alloy's own buffer because it receives no ack, then reaches the replacement when it resumes the same endpoint. A narrow disconnect just before ack during drain can cause duplicates (same nature as at-least-once above), but not loss.

### Object-store settings

 - `LOGGYTRACY_OBJECT_STORE_URL`: Format `s3://bucket/prefix`. Development and tests may use single-process `file:///absolute/path`. `file://` manifest updates use in-process serialization and atomic rename and do not provide multi-writer CAS. Unset means local-only mode.
 - S3 credentials, region, endpoint, and path-style options use the AWS/OBJECT_STORE environment variables read by `object_store`.
 - `LOGGYTRACY_CACHE_MAX_BYTES`: Local Parquet-body cache limit (10 GiB by default; small catalog files excluded). Evict the least recently accessed bodies first and download them again from the manifest for later queries.
 - On startup, recover the manifest catalog first. If the manifest is empty on first object-store activation, publish all final active parts calculated by local tombstone recovery with one CAS. If an existing manifest is present, resume interrupted merge tombstones from the oldest generation, then upload ordinary local parts. Leave a durable marker before upload so work interrupted before manifest publication can be validated and resumed on the next startup. A part with only a complete remote object set and no marker is considered an inactive generation and is not resurrected. Local directories outside the registry are not deleted automatically and remain for later retention.

### Ingest input limits

All limits apply **before** journal append, so rejected requests leave no trace in the WAL.
Protecting the engine takes priority over preserving every log line — Alloy retries or drops rejected batches from its own WAL.

- `LOGGYTRACY_MAX_PUSH_BYTES` (16 MiB by default): The largest single request the tenant ingest quota must always be able to admit — the token bucket's burst capacity is floored at this. The OTLP body limit itself is a 16 MiB constant, matched across both transports.
- `LOGGYTRACY_MAX_LINE_BYTES` (256 KiB by default)
- `LOGGYTRACY_MAX_LABEL_NAMES_PER_STREAM` (30 by default), `LOGGYTRACY_MAX_LABEL_NAME_BYTES` (1 KiB by default), and `LOGGYTRACY_MAX_LABEL_VALUE_BYTES` (2 KiB by default): Defend against stream-cardinality explosions. The stream index is a persistent catalog excluded from the cache limit, so a cardinality explosion becomes non-evictable disk usage.
- `LOGGYTRACY_MAX_TIMESTAMP_AGE` (7d by default) and `LOGGYTRACY_MAX_TIMESTAMP_SKEW` (1h by default): Acceptance window relative to the server clock. Disable with `off` when bulk-loading historical data. Because partitions are UTC-day based, clock errors or unit mistakes (sending seconds/milliseconds as nanoseconds) multiply partitions; in particular, **a future-date part never reaches the retention cutoff.**

- `LOGGYTRACY_DEFAULT_TENANT`, `LOGGYTRACY_MISSING_TENANT_POLICY`: Tenant identification (see "Multi-tenancy" above). Allowlist validation also applies before journal append, like the other limits.

### Retention settings

There are two modes and they **cannot be mixed**. Setting both causes a validation error at startup —
silently ignoring a retention setting would be the worst outcome.

 - `LOGGYTRACY_RETENTION_PERIOD` (unset by default): Global period applied to all tenants.
 - `LOGGYTRACY_TENANT_POLICY_TOKEN` (unset by default): Per-tenant retention. When set, the
   `PUT/GET/DELETE /loggytracy/api/v1/admin/tenants/{tenant}/retention` routes open and pushed policies
   become the sole authority. When unset, the routes do not exist. Use with
   `LOGGYTRACY_RETENTION_REWRITE_THRESHOLD` (0.5 by default — expired-row fraction at which a part is rewritten).
   There is **no** setting that caps pushed values. An instance-side cap would not reach unknown tenants,
   causing a tenant explicitly marked "retain forever" to retain less data than a tenant with no policy
   (`RETENTION_DESIGN.md`, "Rejected: an instance-side maximum").

Policies are stored as one object per tenant (`tenant_policies/<tenant>.json`) and return `200` only
after storage completes. Failure returns `503` and the control plane retries. All policies are loaded at
startup, and failure is fatal — the same severity as being unable to read the manifest.

Policies are **not** on the ingest or query hot path. Writes do not look them up at all, and reads leave
the range of a tenant without a policy unchanged (fail open).

These limits are still global. Per-tenant throttling and quotas are future work (see "Multi-tenancy" above).

## LogQL support (subset)

Supported (highest usage):

- Label matchers: `{app="x", env=~"prod|stage"}`
- Line filters: `|=`, `!=`, `|~`, `!~`
- Parsers: `| json`, `| logfmt`
- Label filters: comparison operators such as `| field="x"` and `| duration > 100ms`
- `| line_format`, `| label_format` (later)
- Metric queries: `rate`, `count_over_time`, `bytes_over_time`, `sum/avg/max/min by (...)`, `topk`
- `unwrap` + `quantile_over_time` (later)

Unsupported syntax is rejected during parsing with a clear error message.

## Milestones

| Stage | Scope | Completion criteria |
|---|---|---|
| M0 | axum + Loki push ingest + journal (group commit, ack) + MemTable + LogQL matchers/line filters | Query logs through the Grafana Loki data source |
| M1 | Part flush (Parquet + trigram bloom + stream index) + journal recovery + unified MemTable/part queries + merge | Data survives restart and pruning is verified |
| M2 | object_store S3 upload + manifest (conditional write) + disk-cache eviction | Queries succeed from S3 after cache deletion |
| M3 | LogQL expansion: json/logfmt parsers, metric queries, field-filter push-down | Real dashboards run |
| M4 | Traces: OTLP ingest + trace_id lookup (bloom) + Tempo API | Query traces through the Grafana Tempo data source |
| M5 | Merge/compaction tuning, retention, resource limits (query memory/range limits), load tests | Target throughput is achieved |
| M6 | Graceful-shutdown hardware replacement (SIGTERM handler + force-flush + drain-status readiness) | Hardware replacement rehearsal succeeds with traffic moved to new hardware without loss |
| M7 | Local S3 load validation (Tier B: in-process latency/fault-injection store / Tier C: local MinIO real S3 protocol) + stronger load-analysis gauge observability | Throughput, latency, memory, retention, and error rate are validated against targets; bottlenecks are documented; manifest CAS, remote restore, and retention GC are verified on MinIO |
| M8 | **The ruler.** Retire the untrustworthy numbers, add criterion microbenchmarks for the hot paths, rewrite the load harness (N connections, intended-send-time latency, realistic corpus, concurrent reads), add CI | A performance regression is detected by a test rather than by reading prose |
| M9 | **The comparison bed.** Loki beside loggytracy at an equal container memory limit, same corpus, four query shapes plus ingest, disk-per-GB and object-store operation counts | The claim in [`VISION.md`](VISION.md) is either supported by a published table or abandoned |
| M10 | **Declared memory budget** ([`VISION.md`](VISION.md) I). One budget knob, arena accounting, honest memtable metering, sidecars inside the budget, admission by budget rather than by slot | The engine runs a sustained mixed load under a declared budget and a test asserts peak RSS stays under it |
| M11 | **Bounded copies and deep pruning** ([`VISION.md`](VISION.md) II, III). `Arc<Labels>` end to end, the two free memcpys removed, single sort and single parse; streaming top-K execution, projection pushdown, cached Parquet footers, regex literal extraction | The M8 benchmarks move, and M9's table is regenerated against the same Loki build |

## References

- VictoriaLogs `lib/logstorage` (Go, Apache 2.0 — design reference): part/block structure, type-detecting encoding, bloom tokenizer, indexdb
- VictoriaTraces: precedent for traces on the same storage
- Quickwit: search architecture on object storage
- ClickHouse `ngrambf_v1`, Google Code Search: trigram indexing techniques

## Main crates

`tokio`, `axum`, `tonic`/`prost` (OTLP), `arrow`/`parquet`, `datafusion` (under consideration), `object_store`, `chumsky` (LogQL), `roaring`, `opentelemetry-proto`
