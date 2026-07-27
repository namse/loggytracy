# Load and measurement results (living)

This document records measurements. Numbers kept only in chat or a terminal disappear, and disappeared
numbers have to be measured again. Policies and procedures are in [`LOAD_VALIDATION.md`](LOAD_VALIDATION.md).

**Keep reproducible facts in tests, not in prose.** The "fixed location" below is the test name;
only numbers belong here. Items without tests require a load run.

Machine: `Darwin arm64` (Apple Silicon), 8 logical CPUs, 16 GiB. **Because this is not the target
specification (4 vCPU / 16 GiB), absolute values are records, not gates.**

---

## 1. Removing group-commit latency (P1-3)

Before and after fixing the batch loop so it no longer consumed `max_batch_ms` when the channel was empty.
Tier B, 45 seconds, proposed 3000 eps.

| | Before (`782e7ff`) | After (`e28f605`) |
|---|---|---|
| ack p50 | 208.8 ms | **5.9 ms** |
| ack p95 | 212.2 ms | **11.7 ms** |
| ack p99 | 214.4 ms | **37.1 ms** |

Evidence that ack latency was controlled by the timer rather than the storage backend: with `file://` or
MinIO, with or without injected latency, all previous values clustered around ~250 ms.

Fixed location: `journal::tests::sequential_appends_do_not_wait_out_a_batch_timer`
(20 sequential appends must finish within 1.5 seconds — the old default took 4 seconds).

## 2. Tenant-fragmentation cost (N3)

The same 5,000 rows and the same `row_group_size=8192`, with **only the tenant count changed**.

| | row group | `data.parquet` |
|---|---|---|
| 1 tenant | 1 | 28,029 B |
| 500 tenants | 500 | **691,119 B** |

**24.7x.** Row groups stop at tenant boundaries, so **tenant count is a lower bound for row-group count**;
Parquet carries column metadata per row group, and this engine carries bloom filters per row group.

### What this ratio depends on

24.7x is **not** a function of tenant count. The condition above has 10 rows per tenant, and the ratio is approximately

```
amplification ≈ 1 + (fixed cost per pair) / (compressed bytes per row × rows per tenant in a part)
```

The measurements below change only rows per tenant (same 500 tenants, based on total bytes):

| Rows per tenant | Bytes/row | Versus one tenant |
|---|---|---|
| 10 | 192.1 B | 31.5x |
| 200 | 16.2 B | 2.7x |
| 2,000 | 7.2 B | 1.18x |
| 8,192 | 6.4 B | 1.05x |

The synthetic line above compresses to 5.5 B/row with zstd. Repeating this with realistic logs containing
`trace_id` and latency gives **5.9x** at 10 rows per tenant and **1.07x** at 2,000 rows. The ratio is large
because the denominator is small; a small denominator also means a small absolute amount.

Because `merge_target_part_rows` is one million, 500 tenants in a merged part have 2,000 rows per tenant.
**24.7x is a transient value from immediately after flush until merge picks it up.** The only wall merge
cannot cross is daily partitioning, so the variable that determines steady state is *rows per tenant per day*.

### The real unit is therefore the (tenant, part) pair, not a row

Fixed cost emitted per pair (increment over one tenant divided by row-group count):

| | Per pair |
|---|---|
| Parquet row group | 1,326 B |
| Two blooms | 145 B |
| `meta.json` segment | 179 B |
| **Resident** portion (bloom + stream index) | 127 B |

Converted to R2 storage, this is negligible — even a worst-case scenario with 10,000 idle tenants is
around $0.05 per month. The affected resources are **RSS and startup**. `PartReader::open_internal`
reads and retains the complete bloom and stream index, while `CACHE_MAX_BYTES` eviction removes only
`data.parquet`, so sidecars remain as long as the part exists.

The pair count is determined not by a tenant's own ingest volume but by **how many parts contain that
tenant's writes**, which is determined by other tenants' ingest volume. `loggytracy_part_tenant_segments`
is that count.

Fixed locations: `part::tests::tenant_breadth_sets_the_row_group_floor_and_what_that_costs`
(the ratio depends on zstd, so the test fixes the relationship rather than a threshold and prints fixed
cost per pair), and `merge::tests::layout_gauges_count_tenant_part_pairs_not_rows`.

## 3. Backpressure holds at the limit

The memtable limit was **lowered to 8 MiB** and events were pushed without pacing. 250,000 events,
500 tenants, latency injection close to real S3 (20 + uniform(0,180) ms), and 0.2% error injection.

| | Value |
|---|---|
| Result | **PASS** |
| Events accepted | 250,000 |
| Rejected with 429 | **512,546** |
| Actual errors | **0** (server `ingest_errors_total` did not increase) |
| Ack p50 / p95 / p99 | 5.9 / 12.1 / 16.2 ms |
| Final memtable | 8.19 MB (just below the 8 MiB limit) |
| WAL backlog | 8.36 MB (bounded) |
| Peak RSS | 4.95 MB |
| Flush successes / injected-error recoveries | 64 / 2 |

**How to read this:** When ingest greatly exceeds flush capacity, the system does not buffer without
bound; it **reaches the limit and rejects requests.** More than 500,000 429 responses are not a failure:
they show that backpressure behaved correctly at that point. The fact that ack latency remained around
12 ms also matters — accepted requests are fast, and requests that cannot be accepted are rejected immediately.

Fixed locations (logic): `ingest::tests::push_is_refused_once_the_memtable_is_over_its_limit` and
`ingest::tests::push_is_refused_once_the_wal_backlog_is_over_its_limit` — both test **engaging and
clearing**. Latched backpressure would permanently remove an instance from service after one burst,
which is worse than unbounded growth.

### Harness defect revealed by this run

The first run numerically failed with `error_rate 0.995`. **The harness was counting 429 as errors.**
A run defended as designed looks catastrophic when reported as a 99.5% error rate — it was actually a
feature preventing catastrophe. 429 was separated into `push_throttled` and excluded from the error rate
(after `e925334`).

## 4. Relationship between flushes and part count

With merge actually disabled (`MERGE_INTERVAL=3600s`, `RETENTION_PERIOD=off`), **12 flushes produced
12 parts.** As expected.

This measurement was made because an earlier run observed "3 parts after 33 flushes," suggesting an
engine bug. The actual cause was that `scripts/run_load_local.sh` unconditionally overwrote the caller's
`MERGE_INTERVAL`, so it was really running at 8 seconds. Merge was doing its job. The script was fixed
(around `ea6b0b3`).

## 5. Eviction → restore

This is not observed with the default configuration. Merge consolidates recent parts and its result is
always local, while retention deletes old data and leaves the probe querying an empty range. Both are normal.

Observation configuration and results:

```
MERGE_INTERVAL=3600s  RETENTION_PERIOD=off  CACHE_MAX_BYTES=524288
LOAD_RESTORE_LOOKBACK_SECONDS=40
```

| | Value |
|---|---|
| `restore_observed` | **true** |
| Evictions | 111 |
| Part count | 66 |
| Restore errors | 0 |
| Restore latency p50 / p95 / p99 | 31 / 749 / 1,626 ms |

**Remaining limitation:** The probe cannot distinguish "restored and read" from "nothing matched" —
both return 200. The result above is valid because the server counter confirms it, but it would be better
for the probe to verify the number of rows read.

---

## Not yet measured

Recorded honestly.

- **Long-running leaks.** The runs above do not answer what responds to staying alive rather than throughput
  (file-descriptor leaks, memory fragmentation). Only this needs a long run, so it is an **occasional** run.
- **Startup and query-planning time with tens of thousands of parts (P1-11).** With normal merge, part
  count remains bounded, so measuring this requires a separate configuration with merge disabled. Run 4 is
  the first step in that direction.
- **Object-store operation counts.** They are a proxy for cost estimation, but there is no instrumentation.
