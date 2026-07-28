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
{"retention": "7d", "ingest_rate": "4MiB/s", "query_rate": "64MiB/s", "max_streams": 100}
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

The body is the complete policy, not a patch. If it is pushed without `ingest_rate`, the existing value
is **cleared** and reverts to the default above.

This rate applies to one instance; it is not the monthly usage sold by a plan. Monthly usage spans
multiple instances and outlives any instance, so only the control plane can own that state.

## Ingest input limits

All limits are checked **before** writing to the journal, so rejected requests leave no WAL record.

| Variable | Default | Description |
|---|---|---|
| `LOGGYTRACY_MAX_PUSH_BYTES` | 16 MiB | Maximum compressed push body size |
| `LOGGYTRACY_MAX_DECOMPRESSED_PUSH_BYTES` | 64 MiB | Maximum length reported by the snappy header; prevents the header from determining the allocation size |
| `LOGGYTRACY_MAX_LINE_BYTES` | 256 KiB | Maximum size of one log line |
| `LOGGYTRACY_MAX_LABEL_NAMES_PER_STREAM` | 30 | Maximum labels per stream |
| `LOGGYTRACY_MAX_LABEL_NAME_BYTES` | 1024 | |
| `LOGGYTRACY_MAX_LABEL_VALUE_BYTES` | 2048 | |
| `LOGGYTRACY_MAX_TIMESTAMP_AGE` | `7d` (`off` allowed) | Reject timestamps older than this |
| `LOGGYTRACY_MAX_TIMESTAMP_SKEW` | `1h` (`off` allowed) | Reject timestamps farther in the future than this. **A future part never reaches the retention cutoff** — confusing seconds or milliseconds with nanoseconds is common |

Set both timestamp knobs to `off` only when bulk-loading historical data.

## Backpressure

| Variable | Default | Description |
|---|---|---|
| `LOGGYTRACY_MAX_MEMTABLE_BYTES` | 256 MiB (`off` allowed) | Return 429 when the two memtables exceed this combined size |
| `LOGGYTRACY_MAX_WAL_BACKLOG_BYTES` | 1 GiB (`off` allowed) | Return 429 when unflushed WAL exceeds this size |
| `LOGGYTRACY_BACKPRESSURE_RETRY_AFTER` | `1s` | `Retry-After` value included in 429 responses |

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
| `LOGGYTRACY_ROW_GROUP_SIZE` | 8192 (maximum 65536) | Parquet row group row count. Groups also stop at tenant boundaries, so **the number of tenants in a part is a lower bound for the actual row group count** |

## Merge

| Variable | Default | Description |
|---|---|---|
| `LOGGYTRACY_MERGE_INTERVAL` | `30s` | |
| `LOGGYTRACY_MERGE_MIN_PART_COUNT` | 4 (minimum 2) | Do not perform a normal merge below this count |
| `LOGGYTRACY_MERGE_TARGET_PART_ROWS` | 1,000,000 | Target output row count (soft) |
| `LOGGYTRACY_MERGE_MAX_PART_ROWS` | 4,000,000 | Maximum output row count (hard) |
| `LOGGYTRACY_MERGE_MAX_INPUT_BYTES` | 512 MiB | Input limit for one group. **Uncompressed (materialized) bytes** |
| `LOGGYTRACY_MERGE_MAX_MEMORY_BYTES` | 1 GiB | Hard limit that one read can materialize |
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
| `LOGGYTRACY_MAX_QUERY_MEMORY_BYTES` | 512 MiB | |
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
| `LOGGYTRACY_MAX_METRIC_ROWS` | 1,000,000 |
| `LOGGYTRACY_MAX_METRIC_SERIES` | 100,000 |
| `LOGGYTRACY_MAX_METRIC_SAMPLES` | (see `config.rs`) |
| `LOGGYTRACY_MAX_CONCURRENT_METRIC_EVALUATIONS` | 4 |

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
- **Want p95/p99** → Apply `histogram_quantile` to `loggytracy_query_latency_ms_bucket`.
  `*_latency_ns_total` provides only an average


## The flush interval is the object-store bill

A flush costs **four PUTs and one GET**: three PUTs for the part's immutable
files (`data.parquet`, `index.bin`, `meta.json`), one for the manifest, and the
GET is the manifest it replaced. Measured, not estimated
([`LOAD_RESULTS.md`](LOAD_RESULTS.md) §9), and pinned by a test that also holds
the two properties that matter — publishing the tenth part into a nine-part
manifest costs what publishing the first into an empty one cost, and the file
count per part is asserted rather than incidental.

A flush skips an empty memtable, so an idle instance costs nothing. An instance
with continuous traffic flushes on every tick, which makes PUT volume a function
of this one setting:

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

Watch `loggytracy_object_store_operations_total{kind="put"}` to see the real
number for a real workload; the rates above will change and the counts will not.


## Sizing an instance

Every limit in this document is enforced on its own, and none of them is
enforced against the machine. The largest term in the footprint is a product of
two knobs that never appear next to each other:

| term | default | worst case |
|---|---|---|
| `MAX_CONCURRENT_QUERY_SCANS` × `MAX_QUERY_MEMORY_BYTES` | 8 × 512 MiB | **4 GiB** |
| `MERGE_MAX_MEMORY_BYTES` (one merge at a time) | 1 GiB | **1 GiB** |
| **Peak materialized** | | **5 GiB** |

The process logs this number once at startup (`peak_materialized_bytes`), because
there is nowhere else to learn it.

It is an upper bound, not an estimate: reaching it needs every scan slot full
and each one at its cap. What matters is that nothing prevents it, and that an
instance sized from its idle footprint is sized about **fifty times too small** —
15 MB idle against 850 MB under load, measured in
[`LOAD_RESULTS.md`](LOAD_RESULTS.md) §7.

Not in the number, and why:

- **Trace scans.** `MAX_TRACE_SPANS` is a count, not a byte budget, so there is
  no honest term to add. `MAX_CONCURRENT_TRACE_SCANS` × the span size of the
  workload is the missing product.
- **The memtable.** Bounded by backpressure rather than by a constant: ingest is
  refused before it grows without limit. Measured at tens of megabytes under
  sustained load.
- **Resident part sidecars.** 18.7 MB at 10,099 parts ([§8](LOAD_RESULTS.md)) —
  real, and two orders of magnitude below the query term.
- **Allocator retention.** Peak RSS tracks whether merge runs at all rather than
  what it is allowed to materialize ([§6](LOAD_RESULTS.md)), so RSS lags the
  budget downward after load subsides.
- **`CACHE_MAX_BYTES`.** Disk, not memory.

**To lower the peak, lower the concurrency first.** `MAX_CONCURRENT_QUERY_SCANS`
is the multiplier; halving it takes a gigabyte and a half off the bound, and it
degrades a burst of queries into a queue rather than degrading every query.
