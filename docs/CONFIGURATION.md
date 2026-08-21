# Configuration reference

All settings are environment variables. `Config::from_env` reads them and `Config::validate` checks
them; if validation fails, startup is refused because not starting is safer than running with a bad configuration.

Tests enforce that this document does not omit any knob from `src/config.rs`
(`every_configuration_knob_is_documented`). Adding a knob to the code without documenting it here
breaks the tests rather than the build.

Duration values use formats such as `500ms`, `30s`, `5m`, `2h`, and `7d`. `off`/`none`/an empty
string mean "disabled" and are valid only for knobs that can be disabled.

---

## Required settings

| Variable | Default | Description |
|---|---|---|
| `LOGGYTRACY_DATA_DIR` | `./data` | WAL, checkpoint, and local part cache. **This directory is the only copy of unflushed data.** Do not discard this disk during hardware replacement |
| `LOGGYTRACY_OBJECT_STORE_URL` | unset (local-only) | `s3://bucket/prefix` or `file:///path`. **When unset, only the local disk is used without S3 tiering** — unsuitable for production because the disk becomes the source of truth |
| `LOGGYTRACY_LISTEN_ADDR` | `127.0.0.1:3100` | Loki-compatible HTTP. **Loopback by default**: there is no TLS and no authentication here, so reaching this listener from off the machine has to be a decision rather than the result of not making one. A container that receives traffic sets this to `0.0.0.0:3100` |
| `LOGGYTRACY_OTLP_GRPC_ADDR` | `127.0.0.1:4317` | OTLP gRPC. The trace and **log** services share this listener. Loopback by default for the same reason |

`file://` is for **single-process development and does not provide CAS**. Using it on shared or network
storage causes manifest lost updates, which means data loss. `from_url` logs a warning at startup.

Credentials and endpoints passed to `object_store` are supplied through `AWS_*` or `OBJECT_STORE_*`
environment variables (`OBJECT_STORE_*` takes precedence). For S3-compatible stores,
**`OBJECT_STORE_CONDITIONAL_PUT=etag` is effectively required**; without it, the startup preflight refuses to run.

## Multi-tenancy

| Variable | Default | Description |
|---|---|---|
| `LOGGYTRACY_DEFAULT_TENANT` | `default` | Tenant assigned to requests without `X-Scope-OrgID` |
| `LOGGYTRACY_MISSING_TENANT_POLICY` | `default` | `default` or `reject`; whether requests without a header are assigned to the default tenant or rejected |
| `LOGGYTRACY_ALLOWED_TENANTS` | unset (allow all) | Comma-separated list. Tenants outside the list receive 403. **Because the header value supplied by the upstream is trusted without proof, anyone who can reach the listener can create any tenant when this list is unset** |
| `LOGGYTRACY_TENANT_POLICY_TOKEN` | unset (disabled) | Enables the per-tenant policy admin API and disables global retention |
| `LOGGYTRACY_DEFAULT_TENANT_INGEST_BYTES_PER_SECOND` | unset (unlimited) | Default for tenants whose rate has not been pushed by the control plane |
| `LOGGYTRACY_DEFAULT_TENANT_QUERY_SCAN_BYTES_PER_SECOND` | none (unlimited) | Read-side default for tenants the control plane has pushed no `query_rate` for |
| `LOGGYTRACY_DEFAULT_TENANT_MAX_STREAMS` | none (unbounded) | Distinct streams a tenant may hold, for tenants with no pushed `max_streams`. **Stream cardinality is the one cost neither retention nor merge reclaims** — `stream.idx` is an eviction-exempt catalog |
| `LOGGYTRACY_DEFAULT_TENANT_MAX_STORED_BYTES` | none (unbounded) | Bytes a tenant may keep stored, for tenants with no pushed `max_stored_bytes`. A plain byte count, or `off`. **Set this before opening a free tier**: a tenant nothing was pushed for is one nobody sold anything to, and unbounded means the first of them decides how much disk the rest get |
| `LOGGYTRACY_MAX_CONCURRENT_QUERIES_PER_TENANT` | 4 | Queries one tenant may run at once. The scan rate bounds work over time; this bounds how much happens simultaneously, so one tenant cannot take every permit of the shared query semaphore |
| `LOGGYTRACY_TENANT_INGEST_BURST` | `10s` | Time during which an unused tenant rate can accumulate for one burst. Capacity never falls below `MAX_PUSH_BYTES` |

**Constraint:** startup is refused when `MISSING_TENANT_POLICY=default` but the default tenant is absent
from `ALLOWED_TENANTS`, because every request without a header would create a tenant outside the list.

**Constraint:** startup is refused when any tenant policy is stored but `TENANT_POLICY_TOKEN` is absent.
Without the token, query clamping disappears and deleted data can reappear.

### Per-tenant ingest rates are not configured here

Because rates vary by plan and can change after launch, **the control plane pushes them per tenant.**
They are the `ingest_rate` field in the policy body, alongside `retention`. The value is bytes per
second such as `4MiB/s`, `0` (writes disabled), or `unlimited`.

```
PUT /loggytracy/api/v1/admin/tenants/{tenant}/retention
{"retention": "7d", "ingest_rate": "4MiB/s", "query_rate": "64MiB/s", "max_streams": 100,
 "max_stored_bytes": "10GiB"}
```

`query_rate` is the read side, in bytes of *scanned* data per second. It is
charged after a query completes with what the scan actually read, because the
cost of a query is not knowable before running it — so a tenant that overruns is
refused on its next query rather than mid-scan, which bounds the overrun at one
query instead of preventing it. `0` means the tenant may not query at all, which
is a real state: a suspended account still owns its data.

`max_streams` bounds distinct streams rather than a rate, because stream
cardinality is the one cost neither retention nor merge takes back. Only a
stream that is new to both the parts and the buffers can be refused — a tenant
at its limit keeps writing to the streams it has rather than going dark, so the
failure is a client that mints label values and not a client that grew.

`max_stored_bytes` is the other half of a plan that sells a period and a size:
retention decides when bytes leave, and this decides how many may pile up before
they do. It is a size, not a rate, so it is spelled `10GiB` rather than
`10GiB/s`, and pushing a rate spelling is refused rather than guessed at.

It is enforced by **refusing writes, not by deleting**. A tenant over its limit
keeps every byte it has and gets `429` with `Retry-After: 60`; the space comes
back on its own when retention retires the oldest parts, and writes resume
without anyone doing anything. Deleting to make room would mean this engine
choosing which of a customer's logs to destroy, which is not a decision it has
the standing to make.

The comparison is against the tenant's own byte extents in the shared objects —
logs and traces both — read from `meta.json` rather than from the local files,
so it does not change as the cache evicts and restores bodies. The Parquet
footer and the sidecars belong to no single tenant and are not charged to one.
`GET …/tenants/{tenant}/usage` reports the same number as `stored_bytes`
alongside `max_stored_bytes`, which is what a control plane shows a customer.

The body is the complete policy, not a patch. If it is pushed without `ingest_rate`, the existing value
is **cleared** and reverts to the default above.

This rate applies to one instance; it is not the monthly usage sold by a plan. Monthly usage spans
multiple instances and outlives any instance, so only the control plane can own that state.

## Ingest input limits

All limits are checked **before** writing to the journal, so rejected requests leave no WAL record.

| Variable | Default | Description |
|---|---|---|
| `LOGGYTRACY_MAX_PUSH_BYTES` | 16 MiB | Largest single request the tenant ingest quota must always admit — the token bucket's burst floor. The OTLP body limit itself is a 16 MiB constant on both transports |
| `LOGGYTRACY_MAX_LINE_BYTES` | 256 KiB | Maximum size of one log line |
| `LOGGYTRACY_MAX_LABEL_NAMES_PER_STREAM` | 30 | Maximum labels per stream |
| `LOGGYTRACY_MAX_LABEL_NAME_BYTES` | 1024 | |
| `LOGGYTRACY_MAX_LABEL_VALUE_BYTES` | 2048 | |
| `LOGGYTRACY_MAX_TIMESTAMP_AGE` | `7d` (`off` allowed) | Reject timestamps older than this |
| `LOGGYTRACY_MAX_TIMESTAMP_SKEW` | `1h` (`off` allowed) | Reject timestamps farther in the future than this. **A future part never reaches the retention cutoff** — confusing seconds or milliseconds with nanoseconds is common |

Set both timestamp knobs to `off` only when bulk-loading historical data.

## Memory budget

| Variable | Default | Description |
|---|---|---|
| `LOGGYTRACY_MEMORY_BUDGET` | 60% of the detected limit (`off` disables) | Bytes the engine budgets for itself. Unset, the process reads cgroup v2 `memory.max` (falling back to `/proc/meminfo` `MemTotal`) and takes 60% — VictoriaLogs' own contract, and the one measured surviving the sustained 2 GiB workload that OOM-killed both this engine and Loki (`todo.md`, soak section, 2026-08-08). The budget re-seeds the **defaults** of the ceilings below; every explicit knob still overrides its derived value |

When the budget is active the following defaults are computed from it, with the
shares taken from the re-measured attribution (`docs/MEMORY_ATTRIBUTION.md`,
build `b9165b0`) rather than guessed. The startup log prints every derived
value beside the budget and its source.

| Derived knob | Share of budget | Floor |
|---|---|---|
| `MERGE_MAX_MEMORY_BYTES` | 25% | 64 MiB |
| `MERGE_MAX_INPUT_BYTES` | half the merge budget | 32 MiB |
| `QUERY_MEMORY_BUDGET_BYTES` and `MAX_QUERY_MEMORY_BYTES` | 25% | 8 MiB |
| `ROW_GROUP_CACHE_MAX_BYTES` | 12.5% | 16 MiB |
| `MAX_MEMTABLE_BYTES` | 10% (accounted bytes; resident cost measured ~1.73×) | 32 MiB |
| `SIDECAR_CACHE_MAX_BYTES` | 10% | 32 MiB |

The nominal shares sum to 72.5%: the remainder covers flush (which rides
ingest), the sidecar stream indexes, and the metering gap above.

**Measured capacity at this budget** (24-hour soak, 2 GiB container, retention
30 m, 2026-08-12): the engine sustains **the full offered 20 k eps — 19,999.8,
nothing throttled** — for 24 hours, 1.73 billion events, with anon flat between
1.47 and 1.57 GiB (peak 1.84), query response p95 428 ms / p99 640 ms, and zero
5xx in 432,001 queries. The residents that a day is run to watch are flat:
sidecar 120 MiB, row-group cache 153 MiB, WAL file 34 MiB, WAL backlog 3 MiB
against its 1 GiB bound.

**The ceiling** (rate ladder at the same configuration, 45 minutes a rung, with
enough ingest connections that the client is never the constraint — 32 at 30 k,
96 at 45 k, sized from the measured service p95; 2026-08-14):

| offered | achieved | refused `429` | `memtable_buffered` peak |
|---|---|---|---|
| 30 k | **29,996.5 eps** | **0%** | 78.0 MiB |
| 45 k | **34,666 eps** | 22.9% | 126.3 MiB |

Two numbers, and an operator wants the first: **30 k eps sustained with nothing
refused**, and **34,666 eps absorbed** while refusing the excess. Everything
refused is a `429`; no rung produced a 5xx or an OOM. What sets the ceiling is
**flush, not the WAL** — the saturated rung pins `memtable_buffered` at
126.3 MiB against the 122.9 MiB `max_memtable_bytes` ("flush is not keeping up"),
while the WAL backlog peaks at 27.7 MiB against its 1 GiB bound and the journal
writer is *less* busy at the higher rate because group commit amortizes
(`records/batch` 1.59 → 4.07). Raising `LOGGYTRACY_MAX_MEMTABLE_BYTES` moves the
refusal, not the flush rate.

No rung between 30 k and 45 k has been run, so the knee is bracketed to 15 k and
not narrower. The pair this replaces — **22 k sustained / 24,274 absorbed**, the
same rig two commits earlier — moved because the flush pass spent 63% of itself
building blooms through a `BTreeSet` over a domain a bitmap covers in 2 MiB
(`816b260`): `write_index` is now 10.9 µs an event instead of 25.2. A change to
the flush path invalidates this table, which is why it carries its date.

The predecessor of this measurement read ~18.6 k eps with 6.9% answered `429`
(2026-08-10) — that run carried a retention/merge lock-order stall that froze the
flush thread for up to 52 s at a time, which is where the throttling came from;
fixed in `ca32ee5`. `todo.md`'s soak section holds both verdicts and the
run-by-run history.

## Backpressure

| Variable | Default | Description |
|---|---|---|
| `LOGGYTRACY_MAX_MEMTABLE_BYTES` | 256 MiB (`off` allowed; budget-derived) | Return 429 when the two memtables exceed this combined size |
| `LOGGYTRACY_MAX_WAL_BACKLOG_BYTES` | 1 GiB (`off` allowed) | Return 429 when unflushed WAL exceeds this size |
| `LOGGYTRACY_MAX_INFLIGHT_PUSH_BYTES` | 128 MiB (`off` allowed; budget-derived at 5%) | Return 429 when request bodies admitted and not yet answered exceed this size. The other two bound buffers this server owns; this one bounds what its callers hand it, which was otherwise `concurrency × 16 MiB` with nothing limiting concurrency. Safe at any value: an idle server always admits one body, so a ceiling below one legal push cannot refuse it forever. Measured in flight on the comparison bed: 0.3 MiB — this closes a hole rather than recovering memory. Read it live as `loggytracy_inflight_push_bytes`. **HTTP only**: gRPC has no `Content-Length` to charge and tonic decodes before the service is reached, so that transport stays bounded by `max_decoding_message_size` × its concurrency |
| `LOGGYTRACY_MIN_FREE_DISK_BYTES` | 2 GiB | Free space on the data directory's filesystem below which ingest is refused with 429, or `off`. **The last guard, not the first** — eviction bounds the cache and the backlog limit bounds the WAL; this covers what neither does. Past it a flush cannot write, which is the worst state this engine has. Must not be below `FLUSH_MAX_BYTES` |
| `LOGGYTRACY_DISK_SAMPLE_INTERVAL` | `10s` | How often free space is re-read. Bounds how stale the number the ingest gate reads can be |
| `LOGGYTRACY_BACKPRESSURE_RETRY_AFTER` | `1s` | How long a throttled client is told to wait, on **both** transports: HTTP renders it as `Retry-After`, gRPC as a `google.rpc.RetryInfo` attached to `RESOURCE_EXHAUSTED`. The OTLP specification makes that attachment the difference between a retryable refusal and one a collector is told to drop the batch on, so the value is not decoration on either side. `Retry-After` has whole-second granularity and both sides use it, rounded up and never zero, so the two transports name the same number |

**Constraint:** `MAX_MEMTABLE_BYTES` cannot be smaller than `FLUSH_MAX_BYTES`, or writes would be
rejected for data that has not even reached the threshold at which flushing is requested.

Disabling these limits restores the old behavior of growing without bound until OOM. The architecture
assumes clients back off on 429 and rely on their own WAL, so disabling them breaks that assumption.

## Journal and flush

| Variable | Default | Description |
|---|---|---|
| `LOGGYTRACY_MAX_BATCH_BYTES` | 1 MiB | Maximum bytes grouped into one write+fsync |
| `LOGGYTRACY_MAX_BATCH_MS` | `0` (no wait) | **0 is the default and recommended.** Group commit forms behind writes: data arriving during write/fsync goes into the next batch. A nonzero value caps per-connection throughput at `1000/this value` pushes/s. Increase it only on disks where fsync costs more than waiting |
| `LOGGYTRACY_FLUSH_MAX_BYTES` | 1 MiB | Flush when the memtable reaches this size |
| `LOGGYTRACY_FLUSH_MAX_INTERVAL` | `5s` | Flush at this interval even when the size threshold is not reached. **This value is the RPO for unexpected disk loss, and it is also the object-store bill** — see below |
| `LOGGYTRACY_FLUSH_CHECK_INTERVAL` | `500ms` | Interval at which the flush loop checks conditions |
| `LOGGYTRACY_FLUSH_CHUNK_BYTES` | 32 MiB (minimum 1 MiB) | Most a flush materializes at once. The snapshot is written in chunks of this many bytes, so the flush transient is bounded by the chunk rather than by how large the memtable grew while the previous flush ran |
| `LOGGYTRACY_WAL_COMPACT_MIN_BYTES` | 64 MiB, `off` disables | Local-only mode truncates the WAL's dead prefix (the bytes before the checkpoint, which no recovery path reads) once it exceeds both this floor and the live suffix. `off` restores the old behaviour where `journal.wal` keeps everything ever ingested. With an object store configured the WAL always compacts and this knob is ignored |
| `LOGGYTRACY_ROW_GROUP_SIZE` | 8192 (maximum 65536) | Parquet row group row count. Groups also stop at tenant boundaries, so **the number of tenants in a part is a lower bound for the actual row group count** |

## Merge

| Variable | Default | Description |
|---|---|---|
| `LOGGYTRACY_MERGE_INTERVAL` | `30s` | |
| `LOGGYTRACY_MERGE_MIN_PART_COUNT` | 4 (minimum 2) | Do not perform a normal merge below this count |
| `LOGGYTRACY_MERGE_TARGET_PART_ROWS` | 1,000,000 | Target output row count (soft) |
| `LOGGYTRACY_MERGE_MAX_PART_ROWS` | 4,000,000 | Maximum output row count (hard) |
| `LOGGYTRACY_MERGE_MAX_INPUT_BYTES` | 512 MiB (budget-derived) | Input limit for one group. **Uncompressed (materialized) bytes** |
| `LOGGYTRACY_MERGE_MAX_MEMORY_BYTES` | 1 GiB (budget-derived) | Hard limit that one read can materialize |
| `LOGGYTRACY_MERGE_MAX_GROUPS_PER_TICK` | 16 | |

**Constraint:** `MERGE_MAX_INPUT_BYTES <= MERGE_MAX_MEMORY_BYTES`. Both values are compared with
`materialized_bytes` recorded in part metadata (the memory actually occupied when read), so their units
match. If a limit is exceeded, groups are split in half and a single part is rewritten in row-group
windows, so the operation cannot fail permanently.

## Retention

| Variable | Default | Description |
|---|---|---|
| `LOGGYTRACY_RETENTION_PERIOD` | unset (**retain forever**) | Global retention period. If unset, S3 and the disk grow forever |
| `LOGGYTRACY_RETENTION_INTERVAL` | `5m` | |
| `LOGGYTRACY_RETENTION_BATCH_SIZE` | 100 | Number of parts processed per tick |
| `LOGGYTRACY_RETENTION_GRACE_PERIOD` | `1h` | Grace period before deleting orphan objects |
| `LOGGYTRACY_MAX_RETENTION_RUNTIME` | `2m` | Object-store operation timeout for retention/GC |
| `LOGGYTRACY_RETENTION_REWRITE_THRESHOLD` | 0.5 | Rewrite when the expired-row fraction of a part exceeds this value. Tenant deletion (`retention: "0"`) ignores this value |

**Constraint:** `RETENTION_PERIOD` and `TENANT_POLICY_TOKEN` cannot be set together. Per-tenant
retention replaces the global period, and startup failure is safer than silently ignoring one setting.

## Cache

| Variable | Default | Description |
|---|---|---|
| `LOGGYTRACY_CACHE_MAX_BYTES` | 10 GiB | Local part cache limit. Exceeding it triggers LRU eviction |
| `LOGGYTRACY_CACHE_EVICTION_INTERVAL` | `30s` | |

Small catalog files such as the stream index are not evicted. Therefore, **a label-cardinality explosion
becomes disk usage that cannot be evicted**.

## Query resource limits

| Variable | Default | Description |
|---|---|---|
| `LOGGYTRACY_MAX_QUERY_RANGE` | unset | Maximum requested time range |
| `LOGGYTRACY_MAX_QUERY_SCAN_ROWS` | 5,000,000 | |
| `LOGGYTRACY_MAX_QUERY_SCAN_BYTES` | 2 GiB | |
| `LOGGYTRACY_MAX_QUERY_MEMORY_BYTES` | 512 MiB (budget-derived) | One query's own materialization cap |
| `LOGGYTRACY_QUERY_MEMORY_BUDGET_BYTES` | 512 MiB (minimum 8 MiB; budget-derived) | The shared pool **all** queries together materialize from, reserved incrementally as rows survive the pipeline. A query refused here gets an error naming the pool; before this the aggregate was `MAX_CONCURRENT_QUERY_SCANS × MAX_QUERY_MEMORY_BYTES` and nothing enforced it |
| `LOGGYTRACY_ROW_GROUP_CACHE_MAX_BYTES` | 256 MiB, `off` disables (budget-derived) | Decoded row groups kept across scans. A part is immutable, so a group decoded whole by one scan serves every later scan without paying the reader build again; the budget bounds what stays resident (`loggytracy_row_group_cache_bytes` reports it). Entries die with their part on merge or retirement |
| `LOGGYTRACY_SIDECAR_CACHE_MAX_BYTES` | unbounded (`off`; budget-derived) | Byte cap on the resident bloom half of part sidecars, evicted LRU across parts. The blooms are durable in `index.bin`, so an evicted part's next pruning query pays one re-read; the stream-index half stays resident (tens of KB per part). Unbounded, residency is ~2 MiB per live part and grows with ingest rate × retention window — the term that killed the first 24-hour soak (`todo.md`) |
| `LOGGYTRACY_MAX_LOG_LIMIT` | 100,000 | Maximum `limit` parameter |
| `LOGGYTRACY_MAX_QUERY_RUNTIME` | `30s` | Also the timeout for metadata endpoints |
| `LOGGYTRACY_MAX_CONCURRENT_QUERY_SCANS` | 8 | Shared with metadata endpoints |
| `LOGGYTRACY_MAX_SERIES_MATCHERS` | 32 | Number of `match[]` entries for `series`. Each matcher is a full pass |
| `LOGGYTRACY_MAX_CONCURRENT_TAILS` | 8 | Live tail (`/loki/api/v1/tail`) connections held at once. Over the limit the upgrade is refused with 429 rather than accepted and dropped |
| `LOGGYTRACY_TAIL_POLL_INTERVAL` | `1s` | How often a live tail asks for new lines. This is both its latency floor and its cost per connection |
| `LOGGYTRACY_MAX_RESTORE_RUNTIME` | `25s` | Cache-miss restore timeout |

### Metric queries

| Variable | Default |
|---|---|
| `LOGGYTRACY_MAX_METRIC_EVALUATION_POINTS` | 10,000 |
| `LOGGYTRACY_MAX_METRIC_SERIES` | 100,000 |
| `LOGGYTRACY_MAX_METRIC_SAMPLES` | (see `config.rs`) |
| `LOGGYTRACY_MAX_CONCURRENT_METRIC_EVALUATIONS` | 4 |

`LOGGYTRACY_MAX_METRIC_ROWS` **was removed and is now ignored.** It capped an
intermediate rather than an answer: the scan handed the evaluator every matching
line and the evaluator reduced them to `(timestamp, value)` per series one step
later, discarding the text. So a `rate()` over a busy stream was refused for
materializing something the client would never receive. The rows are now folded
into per-series samples as they are scanned, and what a metric query costs is
bounded by `LOGGYTRACY_MAX_QUERY_SCAN_BYTES` for the read, `MAX_QUERY_RUNTIME`
for the wall clock, and `MAX_METRIC_SERIES` / `MAX_METRIC_SAMPLES` for the
answer — all of which bound something real. An instance that still sets the
variable starts normally and ignores it.

### Trace queries

| Variable | Default |
|---|---|
| `LOGGYTRACY_MAX_TRACE_SPANS` | 100,000 |
| `LOGGYTRACY_MAX_TRACE_SEARCH_LIMIT` | 1,000 |
| `LOGGYTRACY_MAX_CONCURRENT_TRACE_SCANS` | 8 |
| `LOGGYTRACY_MAX_TRACE_QUERY_RUNTIME` | `30s` |
| `LOGGYTRACY_MAX_TRACE_RESTORE_RUNTIME` | `25s` |

## Startup and shutdown

| Variable | Default | Description |
|---|---|---|
| `LOGGYTRACY_STARTUP_RETRY_BUDGET` | `5m` | Retry object-store startup steps for this duration. Absorb transient failures, then exit and let the orchestrator apply restart backoff |
| `LOGGYTRACY_SHUTDOWN_FLUSH_WARN_AFTER` | `30s` | Warn the operator on stdout when force-flush has failed for this long |

## Load harness only (do not use in production)

These settings inject in-process latency and errors for `scripts/run_load_local.sh`. Setting any of them
activates the wrapper, so **never set them in production.**

| Variable | Description |
|---|---|
| `LOGGYTRACY_OBJECT_STORE_LATENCY_MS` | Base write latency |
| `LOGGYTRACY_OBJECT_STORE_READ_LATENCY_MS` | Base read latency (write value when unset) |
| `LOGGYTRACY_OBJECT_STORE_LATENCY_JITTER_MS` | Added `uniform(0, jitter)` |
| `LOGGYTRACY_OBJECT_STORE_ERROR_RATE` | 0.0–1.0. Injected **only into writes** |
| `LOGGYTRACY_OBJECT_STORE_FAULT_SEED` | Reproduction seed |

## Clocks

There is nothing to configure in production. It is still useful to understand how time-dependent behavior is tested.

- **Monotonic clock** (flush interval, force-flush backoff, startup retry budget) uses `tokio::time::Instant`.
  `tokio::time::pause()` virtualizes it, so a five-minute budget can be tested in ten milliseconds.
- **Wall clock** (timestamp acceptance window, default query range, retention cutoff) is read through `Clock`.
  Tests can freeze and advance the clock to target boundaries precisely instead of changing data into the past.

## Logging

The process follows `RUST_LOG` directly. When unset, it uses `loggytracy=info,warn`.

## Allocator

| Variable | Default | Description |
|---|---|---|
The production binary's global allocator is **jemalloc** (`src/main.rs`), adopted after the soak
measured glibc's retained-free creep killing 2 GiB in hours with every gauged resident flat and
every glibc knob below already applied. The three `MALLOC_*` knobs and the trim loop therefore act
only in `--features memprof` builds, whose instrumented allocator still goes through glibc.
jemalloc runs at its defaults — a five-way A/B on the soak rig found no setting that beat them —
and an operator override goes through `_RJEM_MALLOC_CONF` (this build is symbol-prefixed, so the
plain `MALLOC_CONF` name is not consulted).

| `LOGGYTRACY_MALLOC_TUNING` | on | On glibc the process caps malloc arenas and fixes a 128 KiB trim threshold before any thread exists — `docs/MEMORY_ATTRIBUTION.md` measured 44–69% of the cgroup's anonymous memory as freed-but-retained heap without it. `off` restores glibc's defaults for an A/B or if a throughput regression is suspected |
| `LOGGYTRACY_MALLOC_ARENA_MAX` | 4 | The arena cap the tuning applies. 1 was measured first and rejected: anon fell 3.6x but the allocation-heavy flush path halved its cadence contending for the single arena. 0 leaves glibc's own arena scaling in place (trim threshold still applied) |
| `LOGGYTRACY_MALLOC_TRIM_INTERVAL` | `60s` (`off` disables) | How often a background loop calls glibc's `malloc_trim(0)`, returning free pages from the middle of every arena to the kernel. The fixed trim threshold only releases heap tops; without this the second 24-hour soak measured an ~130 MiB/hour anonymous creep with every gauged resident flat, reaching a 2 GiB kill at t≈8653 s. No-op on non-glibc builds |

---

## Tuning starting points

- **Want a smaller RPO** → Lower `FLUSH_MAX_INTERVAL`. Object-store writes increase accordingly, and on a
  per-request backend that is money — see the table below before choosing
- **High ack latency** → First check whether `MAX_BATCH_MS` is 0. If not, that value is the latency floor
- **WAL backlog is growing** → Flush cannot keep up with ingest. Check for 429 responses
  (`loggytracy_ingest_throttled_total`); if none appear, the limit is too high
- **`/ready` stays at 503** → Check which `/metrics` `*_errors_total` is increasing.
  Flush, merge, retention, object store, and local cache each lower readiness independently
- **The disk is filling** → Reduce `CACHE_MAX_BYTES` or set `RETENTION_PERIOD`. Nothing is deleted when the latter is unset
- **Want p95/p99** → Apply `histogram_quantile` to `loggytracy_query_latency_ms_bucket`,
  which carries an `endpoint` label (`query_range`, `query`, `tail`, `patterns`,
  `detected_fields`, `volume`). Per endpoint is the useful cut — a dashboard slow
  because `volume` is slow does not look like a slow `query_range` — and the whole
  read path is `sum by (le)` across them. There is no unlabeled series to read
  instead: one would double-count under `sum`.
  `*_latency_ns_total` provides only an average
- **Queries are slow but nothing is failing** → Check `loggytracy_query_scans_queued_total`. Nonzero
  means scans waited for a slot, and `loggytracy_query_scan_queue_wait_ns_total` divided by it is how
  long they waited. The scheduler admits by arrival order and knows nothing about cost, so a cheap
  dashboard query queues behind expensive ones: measured at 24 concurrent readers, a 60-second query
  reached p95 6.46 s while the wide scans ahead of it measured 392 ms
  ([`LOAD_RESULTS.md`](LOAD_RESULTS.md) §11). `loggytracy_query_scans_in_flight_peak` is the high-water
  mark since start — a sampled gauge cannot see a burst that fills and drains between two scrapes


## The flush interval is the object-store bill

A flush costs **four PUTs and one GET**: three PUTs for the part's immutable
files (`data.parquet`, `index.bin`, `meta.json`), one for the manifest, and the
GET is the manifest it replaced. **Pinned by a test rather than by a load run**
— `object_storage::tests::publishing_a_part_costs_a_fixed_number_of_requests` —
which also holds the two properties that matter: publishing the tenth part into
a nine-part manifest costs what publishing the first into an empty one cost, and
the file count per part is asserted rather than incidental. The load run that
first counted these requests is retired ([`LOAD_RESULTS.md`](LOAD_RESULTS.md) §9)
and the count outlived it, because a request count is a property of this code
rather than of a machine or a corpus.

A flush skips an empty memtable, so an idle instance costs nothing. An instance
with continuous traffic flushes on every tick, which makes PUT volume a function
of this one setting. The table below is arithmetic on the pinned count and
nothing else:

| `FLUSH_MAX_INTERVAL` | Class A / day | / month | R2 cost |
|---|---|---|---|
| 2 s | 172,800 | 5.18 M | $18.83 |
| 5 s (**default**) | 69,120 | 2.07 M | $4.83 |
| 15 s | 23,040 | 0.69 M | free tier |
| 30 s | 11,520 | 0.35 M | free tier |
| 60 s | 5,760 | 0.17 M | free tier |

**The default does not fit the budget this engine was designed around.** The
shared-part layout exists because per-tenant objects broke a $1/month plan, and
one busy instance at the default spends almost five times that before any
tenant multiplier. Consolidating the two index sidecars into one file took a
fifth off this table; the remaining term is the flush rate itself.

The default is 5 s anyway, because this setting is also the RPO — how much
acknowledged data a disk loss can cost — and that is a deployment decision
between money and durability, not one this repository should make on someone's
behalf. What it can do is refuse to let the choice be made without the price.

**What is not in the table, and is unverified:** merge and retention PUTs ride
on top of it, and their rate is a function of the workload. Every figure this
repository has for that rate came from the retired harness, so treat the table
as the floor for a busy instance rather than as its bill.

Watch `loggytracy_object_store_operations_total{kind="put"}` to see the real
number for a real workload; the rates above will change and the counts will not.


## Sizing an instance

The query term used to be `MAX_CONCURRENT_QUERY_SCANS × MAX_QUERY_MEMORY_BYTES`
— 8 × 512 MiB, four gigabytes no single knob mentioned and nothing enforced.
Queries now reserve from one shared pool, so the table is budgets rather than
products:

| term | default |
|---|---|
| `QUERY_MEMORY_BUDGET_BYTES` (every query together) | 512 MiB |
| `MERGE_MAX_MEMORY_BYTES` (one merge at a time) | 1 GiB |
| **Peak materialized** | **1.5 GiB** |

The process logs this number once at startup (`peak_materialized_bytes`), because
there is nowhere else to learn it. Trace scans still sit outside it
(`MAX_TRACE_SPANS` is a count, not bytes), and so do the memtable and the flush
chunk — the startup log names what is excluded.

An instance sized from its idle footprint is still sized **far** too small: peak
RSS is reached within about a minute of load starting and returns to idle when
load stops, so a quiet screenshot describes nothing.

**This bound is the only sizing figure here that is not retired**, because it is
arithmetic on the configured limits rather than a measurement. The multiple
between idle and peak used to be quoted in this paragraph; it came from the
retired harness ([`LOAD_RESULTS.md`](LOAD_RESULTS.md) §7) and is struck. Size
against the bound, and measure the peak on a real workload.

**How far a real workload gets into the query term is now measured: 9.2%.** With
all eight scan slots occupied and 2,403 scans queued behind them, peak RSS was
496 MB ([§11](LOAD_RESULTS.md)). The per-scan cap is nowhere near binding at that
shape of query, so the bound above stays an upper bound rather than becoming a
sizing target — 850 MB with merge running is still the larger figure, and the
read path is not where this engine's memory goes.

Not in the number, and why:

- **Trace scans.** `MAX_TRACE_SPANS` is a count, not a byte budget, so there is
  no honest term to add. `MAX_CONCURRENT_TRACE_SCANS` × the span size of the
  workload is the missing product.
- **The memtable.** Bounded by backpressure rather than by a constant: ingest is
  refused before it grows without limit. Note that `MAX_MEMTABLE_BYTES` is
  enforced against an accounting that undercounts what a line actually occupies,
  so the real ceiling is above the setting — see [`VISION.md`](VISION.md) I.
- **Resident part sidecars.** Linear in (tenant, part) pairs, and small relative
  to the query term at every part count this engine has been run at. The
  absolute figures are retired; the shape is not, and
  `loggytracy_part_sidecar_resident_bytes` reports it live.
- **Peak RSS above the sum of these.** It is live memory held while ingest,
  flush and merge overlap. Two explanations were proposed for it and both were
  refuted: it is not the merge memory budget, which barely moved it, and it is
  not allocator high-water retention, because RSS returns to its starting value
  after load stops ([`LOAD_RESULTS.md`](LOAD_RESULTS.md) §6 and §7 keep both
  refutations).
- **`CACHE_MAX_BYTES`.** Disk, not memory.

**To lower the peak, lower the concurrency first.** `MAX_CONCURRENT_QUERY_SCANS`
is the multiplier; halving it takes a gigabyte and a half off the bound, and it
degrades a burst of queries into a queue rather than degrading every query.
