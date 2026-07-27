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
| Peak RSS | **not measured** — see the correction below |
| Flush successes / injected-error recoveries | 64 / 2 |

**How to read this:** When ingest greatly exceeds flush capacity, the system does not buffer without
bound; it **reaches the limit and rejects requests.** More than 500,000 429 responses are not a failure:
they show that backpressure behaved correctly at that point. The fact that ack latency remained around
12 ms also matters — accepted requests are fast, and requests that cannot be accepted are rejected immediately.

Fixed locations (logic): `ingest::tests::push_is_refused_once_the_memtable_is_over_its_limit` and
`ingest::tests::push_is_refused_once_the_wal_backlog_is_over_its_limit` — both test **engaging and
clearing**. Latched backpressure would permanently remove an instance from service after one burst,
which is worse than unbounded growth.

### Corrections to this run

**The peak RSS recorded here was the load generator's, not the server's.**
`current_rss_bytes` read `std::process::id()`, which inside the harness is the
harness. So the figure was a few megabytes of request buffers, it was compared
against a 4 GiB gate it could never exceed, and it was written down as an engine
result. The number that gives it away is in the table above: a 4.95 MB peak
next to an 8 MiB memtable in the same run. The harness now takes the server PID
and reports `null` when it was not given one — a gate that cannot measure must
not pass.

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

## 6. The layout axis, measured (N3)

500 tenants, 400,000 events, `file://` with Tier B fault injection, retention
off. Two runs differing only in whether merge runs.

| | parts | (tenant, part) pairs | pairs/part | parts/tenant | sidecar resident | `meta.json` | peak RSS |
|---|---|---|---|---|---|---|---|
| merge off (`3600s`) | 38 | 3,309 | 87.1 | 6.62 | 1.55 MB | 645 KB | 187 MB |
| merge on (`8s`) | 7 | 927 | 132.4 | 1.85 | 539 KB | 179 KB | 697 MB |

Per pair: **468 B resident** (bloom + stream index) and **195 B of
`meta.json`**. Both are within a few percent of what the unit test prints for a
synthetic part, so the model behind the earlier analysis holds on a real
workload.

**Merge is the lever on pair count, as predicted.** Turning it on cuts pairs by
3.6x and parts-per-tenant from 6.6 to 1.85. Pair count tracks parts, and merge
is what bounds parts.

**But it is not the lever on peak RSS — it is the opposite.** The merge-on run
peaks at 697 MB against 187 MB with merge off, because merge materializes the
parts it reads and `merge_max_memory_bytes` is 1 GiB. At this scale the sidecars
are 0.5–1.5 MB while merge transiently uses hundreds of megabytes, so **the
binding memory constraint is the merge budget, not fragmentation.** That
reverses the working assumption these gauges were added to test.

Extrapolating the resident cost to the 10,000-part configuration P1-11 cares
about, at the same tenant breadth: ~870,000 pairs, ~407 MB of resident
sidecars. Material on a 16 GiB target, and still smaller than one merge.

### Following the FAIL: two hypotheses, both wrong, one bug

The merge-on run ends `FAIL` while merge-off passes. Two explanations were
proposed and both were tested.

| run | verdict | peak RSS | flushes | `remote_healthy` at end |
|---|---|---|---|---|
| merge off, 3% write errors | PASS | 187 MB | 38 | true |
| merge on, 3% write errors | FAIL | 697 MB | 31 | **false** |
| merge on, **0%** errors | PASS | 758 MB | 72 | true |
| merge on, 3% errors, merge budget 1 GiB → 128 MiB | FAIL | 514 MB | 31 | **false** |

**"Peak RSS is the merge budget" — wrong.** Cutting `merge_max_memory_bytes`
eightfold moved peak RSS 697 → 514 MB and changed no verdict. The no-error run
peaks *higher* at 758 MB and passes. Peak RSS tracks whether merge runs at all,
not what it is allowed to materialize, which points at allocator high-water
retention rather than at a budget worth tuning.

**"The FAIL is memory pressure from merge" — wrong.** The no-error run has the
highest RSS of the four and passes.

**What it actually is — and the explanation that was wrong.** The first
diagnosis offered here was that health recovery degrades as reporters multiply:
`mark_remote_healthy_since(epoch)` restored health only by CAS from the epoch
the caller observed before its operation, so a failure by any other worker in
between defeated that worker's recovery. Sampling `remote_healthy` every 250 ms
through a run refutes it — the run with **fewer** reporters has the **worse**
duty cycle:

| run | healthy | transitions | longest unhealthy |
|---|---|---|---|
| merge on, 3% errors | 66.4% | 14 in 75 s | 11.5 s |
| merge off, 3% errors | 41.0% | 17 in 48 s | 4.2 s |

Both flap. The PASS/FAIL split between them was the terminal sample landing on
different sides of a signal that changes every few seconds — luck, not a
behavioural difference.

The actual defect is simpler: **a single failed request meant "the store is
down"**, and `/ready` reads that. At a 3% write-error rate — which the engine
survives with no ingest errors and no lost data — readiness flipped 14-17 times
a minute. An orchestrator watching it pulls the instance in and out of service
over an error rate that cost nothing.

Health is now hysteretic: three consecutive failures with no success between
them mark the store down, and one success clears it. Re-measured on the same
workload:

| run | healthy | transitions |
|---|---|---|
| merge on, 3% errors | **99.3%** | 2 |
| merge off, 3% errors | **100.0%** | 0 |

The harness gated on the terminal sample, which is what made its verdict a coin
flip. It now samples through the run and gates on the fraction.

**What survived the fix, and what it turned out to be.** With merge on the WAL
backlog ended at 47.6 MB against 9.6 MB with merge off. Sampling it every
500 ms over a ten-minute run answers whether that grows:

| t (s) | 0 | 81 | 162 | 243 | 324 | 406 | 487 | 568 |
|---|---|---|---|---|---|---|---|---|
| backlog (MB) | 10.8 | 71.0 | 6.3 | 35.5 | 36.9 | 7.1 | 8.4 | 8.4 |

It oscillates between 6 and 140 MB with a linear trend of **-0.04 MB/s**, and
the second-half mean (20.2 MB) is below the first-half mean (28.0 MB). Merge and
flush do contend, and flush catches up: **bounded, not growing.** N8 is a tuning
note rather than a defect.

The peak reached 140 MB while the terminal sample read 47.6 MB, so that
single-sample reading was luck too — the third time in this section that gating
on a terminal sample produced a number that meant nothing.

The harness now samples the backlog and asks the question that distinguishes the
two cases: **does flush ever catch up.** A backlog that rises and falls is flush
keeping up in bursts; one that only rises is flush losing. Comparing halves
would have worked here and failed a short run, which is all ramp and no plateau;
drainage holds for both. The ceiling it is judged against is the engine's own
`max_wal_backlog_bytes`, because reaching that engages backpressure, and
engaging is the design working — the old 16 MiB harness target failed runs whose
backlog was oscillating exactly as intended.

Fixed location: `merge::tests::layout_totals_count_tenant_part_pairs_and_survive_a_silent_merge`,
`merge::tests::merging_parts_removes_their_pairs_from_the_totals`.
