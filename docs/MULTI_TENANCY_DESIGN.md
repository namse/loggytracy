# Multi-tenancy design

Design record for making loggytracy multi-tenant. Written to be self-contained:
a fresh context should be able to start implementing from this document alone.

Status: **accepted; steps 1, 2 (partly), and 4 are implemented.** See
[Implementation status](#implementation-status) for what actually landed.
Supersedes the "테넌시" section of
`ARCHITECTURE.md` (lines 48-65), which specified tenant-as-path-axis. That
approach is rejected here on cost grounds — see [Why not tenant-per-path](#why-not-tenant-per-path).

## Why this exists

loggytracy is the log and trace backend for **fn0** (`~/fn0`), which sells a
**$1/month plan**. The engine must serve every project on that plan without
losing money, so the cost model is a hard design constraint rather than an
afterthought.

Requirements:

1. Per-tenant monthly quotas and usage limits.
2. A tenant must never see another tenant's data.
3. Total cost per project must fit the budget derived below.

## Budget

From `~/fn0/docs/cloud/dollar-plan.md`:

| Item | Value |
|---|---|
| Revenue after Paddle + domain | **$0.35–0.80 / subscriber / month** (infra budget) |
| Current expected total, all resources | $0.06–0.16 / user |
| Metrics line (500 active series, self-hosted VictoriaMetrics, 2-replica HA) | **$0.008 / project / month** — stated as "1.0–2.4% of the infra budget" |
| Grafana Cloud at $3.25/project | rejected, "~390x more" |

The metrics line is the governing precedent: **one observability axis is
expected to cost about $0.01/project/month.** Logs and traces are held to the
same bar.

**$0.01/project/month = 2,222 R2 Class A operations/month.**

### R2 unit costs

| Resource | Price |
|---|---|
| Storage | $0.015 / GB-month |
| Class A (PUT, LIST) | **$4.50 / million** |
| Class B (GET, HEAD) | $0.36 / million |
| Egress | free |
| DELETE | free |
| Free tier | 10 GB storage, 1M Class A, 10M Class B per month |

Verify against current Cloudflare pricing before committing numbers to
public copy; these match `dollar-plan.md` as of 2026-07.

## Cost model of the current engine

The dominant cost is **Class A operations**, and their count is set by the
flush cadence, not by data volume.

Per flush with an object store configured:

| Operation | Code | Class |
|---|---|---|
| Upload 4 part files (`data.parquet`, `bloom.tri`, `stream.idx`, `meta.json`) | `object_storage/cache.rs:2` `upload_part` over `PART_FILES` (`object_storage/mod.rs:18`) | **A** × 4 |
| Manifest read for CAS | `object_storage/object_io.rs:304` `load_manifest_versioned` | B × 1 |
| Manifest write | `object_storage/object_io.rs:298` `publish` | **A** × 1 |
| Trace manifest read (**happens even with zero traces**) | `flush.rs:353` → `object_storage/catalog.rs:265` | B × 1 |
| Delete superseded objects (merge, retention) | `delete_part_objects` | free on R2 |

The flush trigger (`flush.rs:87-96`):

```rust
if memtable.is_empty() && trace_memtable.is_empty() { continue; }
if (size as u64) < flush_max_bytes && elapsed < flush_max_interval { continue; }
```

Defaults are `flush_max_bytes = 1 MiB`, `flush_max_interval = 5s`
(`config.rs:85-86`). Any tenant with continuous trickle traffic therefore
flushes every 5 seconds — **518,400 flush cycles per month**, independent of
how little data it sends.

### Why not tenant-per-path

If the tenant becomes a storage path axis, every tenant writes its own objects.
Even after consolidating the four sidecars into one object, and counting zero
manifest and zero merge overhead:

| | Value |
|---|---|
| Class A budget | 2,222 / month |
| Objects per flush per tenant | 1 (best case) |
| **Maximum flush frequency** | **once per 19.4 minutes** |

With realistic manifest and merge overhead this lands at an RPO of 45-60
minutes. Worse, a small tenant buffering 20 minutes of logs produces
multi-kilobyte parts, which compress poorly and pile up merge debt.

At the current 5s default the same layout costs **$9.33/project/month** —
about 9x the plan's entire revenue.

**Conclusion: per-tenant objects do not fit this price point at any usable
RPO. Objects must be shared across tenants.**

Note that flush cadence does **not** affect query freshness: the memtable is
part of the unified query path, so ingested data is visible immediately. A
longer flush interval only widens the window for *simultaneous loss of server
and disk*, a risk `ARCHITECTURE.md:13` already accepts deliberately. The WAL
covers process crashes with zero loss, and M6 graceful shutdown covers planned
machine replacement.

## Accepted design: shared parts with tenant-aligned segments

One part object holds **all tenants**. The tenant is a sort key and index axis
*inside* the object, not a path component.

### Layout

- Rows sorted by **`(tenant_id, timestamp_ns)`**.
  Currently `part/format.rs:20` and `:68` sort by `timestamp_ns` alone.
- **Row groups aligned to tenant boundaries** — a row group never spans two
  tenants (split at the boundary).
- `meta.json` gains a per-tenant index:
  `tenant -> { row_group_range, min_ts_ns, max_ts_ns, row_count, byte_range }`.
- Bloom and stream index stay per-row-group. **No change** to
  `part/indexes.rs` or the bloom sidecar format.

### Why the sort key change is what makes this work

An earlier analysis rejected tenant-as-label because pruning fails. The reason
was specific and fixable: the stream index (`part/indexes.rs:20-41`) is a
row-group-granular posting list, and with rows sorted by timestamp alone, every
active tenant appears in every row group. The tenant → row-group bitmap becomes
all-ones and prunes nothing.

Sorting by `(tenant, ts)` removes that. Within a tenant's segment rows remain
timestamp-ordered, so the early-termination logic in `part_registry.rs:307-310`
and `memtable.rs:47-51` keeps working unchanged. Only the part-level
`min_ts_ns`/`max_ts_ns` pruning has to become per-tenant, which the new meta
index provides.

### What this buys

| Requirement | How it is met |
|---|---|
| Isolation | A query cannot **address** row groups outside its tenant's range. Same fail-closed property as separate files. |
| Time pruning | Timestamp order preserved inside each tenant segment. |
| Bloom / stream index | Row-group granular; unchanged. |
| Transfer on cache miss | Range GET of the tenant's `byte_range` only. |
| **Class A** | **One object per flush for all tenants combined.** |

Cost per project becomes **inversely proportional to project count** — the
right shape for a platform.

### Retention: partition by tier, not by tenant

> **Superseded.** This section records the reasoning as it stood when the
> document was written. Partitioning on `(tier, day)` was rejected because it
> fixes retention at write time, so a plan upgrade or downgrade never reaches
> data that is already on disk. Per-tenant retention is instead decided from
> the `meta.json` tenant index at **deletion** time; see
> [`RETENTION_DESIGN.md`](RETENTION_DESIGN.md). Partitions stay on `day`.

Shared objects cannot express per-tenant retention. fn0 does not need it:
retention is a **plan attribute** ($1 plan, free plan), not a per-project
setting.

Partition on `(tier, day)`. Whole-object deletion works as it does today
(`retention.rs:88` filters on `max_ts_ns < cutoff` and removes directories),
and R2 charges nothing for DELETE.

Individual account deletion, in order of preference:

1. **Wait for retention expiry.** Document as "logs purged within N days of
   account deletion." Zero engineering. Standard industry practice.
2. **Lazy purge at merge.** Merge already rewrites parts; drop the deleted
   tenant's rows at that point. No extra I/O, but latency until the next merge.

*Resolved:* deleting a tenant is pushing `retention: "0"`, which does both — the
data is invisible from the next query, parts holding only that tenant are
deleted whole, and shared parts drop its rows at the next merge, ignoring the
rewrite threshold so reclamation is bounded. There is no completion report; see
[`RETENTION_DESIGN.md`](RETENTION_DESIGN.md).

## Read path

Planning is unaffected and remains **zero R2 requests**.

`remote_lifecycle.rs:35-42` returns early before touching the network:

```rust
let required = required_parts();
if missing_parts(&required).is_empty() {
    return Ok(read_guard);   // cache hit: zero R2 requests
}
```

`candidate_part_ids_with_exact_fields` (`part_registry.rs:107`) reads only
in-memory `PartReader`s backed by local catalog files, which are excluded from
LRU eviction (`ARCHITECTURE.md:123`). Total catalog size is roughly unchanged
by sharing, because bloom and stream index are row-group granular and the total
row count is the same either way.

Two conditions must hold or reads get much worse:

### Condition 1: range reads become mandatory

`download_part` (`object_storage/cache.rs:246`) currently does
`store.get(&path)` → `result.bytes()`, fetching the whole object.
`ARCHITECTURE.md:123` defers range reads to future work.

Against a shared part this would download **every tenant's data on every
miss** — a 200 MB part to serve a tenant's 200 KB is 1000x amplification.
Range GET using the per-tenant `byte_range` is a **prerequisite**, not an
optimization. R2 charges a range GET as one Class B, same as a full GET, and
egress is free, so this reduces bytes at no request cost.

### Condition 2: local cache layout is decoupled from remote layout

| | Purpose | Unit |
|---|---|---|
| Remote (R2) | minimize Class A | **shared** object |
| Local (disk cache) | query locality, isolation | **per-tenant** slice |

Store each range-fetched slice under a `(part_id, tenant)` key. The local cache
then behaves exactly as it does today, and `evict_cache` LRU works per slice.

Caching whole shared objects locally is the failure mode to avoid — it wastes
cache space and couples tenants.

### Read-side improvements

- Consolidating sidecars takes a cache miss from **4 GETs to 1**. Today a miss
  re-downloads all of `PART_FILES` even though the three catalog files are
  already local (`cache.rs:246` uses `&PART_FILES` when `include_data=true`,
  and `remove_dir_all(&final_dir)` wipes the good local copies first).
- Merge concatenates each tenant's segments across parts, making a tenant's
  data **more contiguous** in merged parts. One range GET then retrieves more
  useful data. Merge helps more here than under per-tenant parts.

### Known read-side overhead

Tenant-aligned row groups mean a tenant with 50 rows still occupies its own row
group. With 1,000 active tenants a part may contain 1,000+ row groups, and
Parquet footer metadata (a few hundred bytes per column chunk) can reach 1-2 MB.

Assessment:

- **Compression loss is negligible in absolute terms.** It lands only where
  byte counts are tiny; large tenants still get full-size row groups.
- **Footer size needs measurement.** If it becomes significant, pool tenants
  below a minimum row count (~512) into a shared row group and index them by
  row range in `meta.json`. Pruning granularity coarsens and isolation weakens
  from "cannot address" to "row-range filter" — acceptable but strictly worse,
  so only do this if measurement demands it.

**Open item: measure Parquet footer size at realistic tenant counts before
finalizing the row-group policy.**

## Supporting changes

### Consolidate the four sidecars into one object

`PART_FILES` (`object_storage/mod.rs:18`) is four files, so each part costs four
PUTs. A single container — `[parquet][bloom][stream.idx][meta][offset footer]` —
makes it one, a **4x Class A reduction**, and fixes the redundant-GET problem
above.

Affected: `upload_part`, `download_part`, `restore_catalog`,
`delete_part_objects`, `garbage_collect_orphans`. All iterate the `PART_FILES` /
`CATALOG_FILES` constants, so the change is contained.

### Raise `flush_max_bytes` substantially

With all tenants in one memtable, the 1 MiB byte trigger (`config.rs:85`) fires
long before the interval, making the interval setting meaningless. Raise to
**64–256 MiB** so the interval is the only trigger.

The WAL protects against process crash, so a large memtable is an RPO concern
only, not a durability one. 256 MiB is comfortable on an OCI A1 (12 GB+).

### Tenant ID validation

The tenant ID arrives in the `X-Scope-OrgID` header and flows into R2 object
keys and local filesystem paths. `is_safe_path_component`
(`object_storage/recovery.rs:439`) only rejects traversal, not hostile names.

Validate against a strict allowlist (`[a-zA-Z0-9_-]{1,64}`) **before journal
append**, consistent with the existing rule that all input limits apply before
the WAL (`ARCHITECTURE.md:135`).

## Cost result

Shared parts, 60s flush, consolidated sidecar:

| Line | Class A / month, platform-wide |
|---|---|
| Flush part objects | 43,200 |
| Manifest CAS PUT | 43,200 |
| Merge (write amplification ~4x, on large parts) | ~29,000 |
| **Total** | **~115,000 = $0.52 / month** |

| Projects | Class A cost / project |
|---|---|
| 100 | $0.005 |
| 1,000 | **$0.0005** |
| 10,000 | $0.00005 |

### Flush cadence is a platform-wide fixed cost

| Flush interval (RPO for total disk loss) | Platform-wide / month | At 100 projects |
|---|---|---|
| 5s | $4.67 | $0.047 |
| 15s | $1.56 | $0.016 |
| 30s | $0.78 | $0.008 |
| **60s** | **$0.52** | **$0.005** |
| 5 min | $0.10 | $0.001 |

Start at 60s and tighten as project count grows. Cadence can also be set per
tier (free 5 min, $1 plan 60s) since objects are already partitioned by tier.

## Proposed quotas

Sized bottom-up from what a real app emits, then priced — the same method
`dollar-plan.md` used for metrics.

### What a request generates

Anchor: the CPU pool is 500 CPU-minutes ≈ **2M SSR requests**; p50 usage is
1-5% of caps.

| Source | Per request | At cap (2M req) | p50 (50k req) |
|---|---|---|---|
| Platform access log (method, path, status, duration, trace_id) | ~300 B | 0.6 GB | 15 MB |
| User logs (~1 line per handler) | ~200 B | 0.4 GB | 10 MB |
| Traces (root + ~5 spans × 400 B) | ~2 KB | 4 GB | 100 MB |
| **Total** | **~2.5 KB** | **~5 GB** | **~125 MB** |

Maxing the CPU pool produces about **5 GB/month** of logs and traces. The
ingest quota is sized so it does not bind before the compute quota does.

### What actually costs money

Under shared parts, **ingest volume barely affects Class A**: flush object
count is set by cadence, and merge output count scales with part count, not
bytes. A heavy tenant makes parts *larger*, not *more numerous*.

| Axis | Cost source | Dominant variable |
|---|---|---|
| Ingest volume | R2 storage $0.015/GB-mo | **retention period** |
| Query volume | scan CPU + cache-miss Class B | **scanned bytes** |
| Stream cardinality | `stream.idx` is an eviction-exempt persistent catalog | active stream count |

### Recommended values ($1 plan, 7-day retention)

| Quota | Value | Worst | Expected |
|---|---|---|---|
| Log + trace ingest | **10 GB / month** | $0.0035 | ~$0.0001 |
| Retention | **7 days** | — | — |
| Query scan | **50 GB / month** (+2 GB / hour burst) | $0.019 | ~$0.0004 |
| Active log streams | **100 / project** | ~$0 (disk only) | ~$0 |
| Flush + merge Class A (shared across tenants) | — | $0.0005 | $0.0005 |
| **Total** | | **~$0.023** | **~$0.0015** |

Expected cost is **below the $0.008 metrics line**. Worst case is bounded and
barely moves the plan's existing $2.2/user worst-case total.

Free tier follows the documented 1/10 rule: **1 GB ingest, 5 GB scan, 3-day
retention, 20 streams** (~$0.002 worst case).

### Rationale

**Ingest 10 GB** — 2x headroom over the 5 GB a maxed CPU pool produces; covers
400k requests even for an app logging 5 lines each. Matches the existing
"Object storage 10 GB" line for table consistency.
Storage math: 10 GB × (7/30) = 2.33 GB retained, ~10x compression from Parquet
dictionary + zstd → 233 MB → **$0.0035**.

**Retention 7 days** — the single largest storage lever. At 30 days storage
becomes $0.015, twice the metrics line. Seven days is sufficient for
side-project debugging; **30-day retention is the natural upsell for the ~$5
tier** `dollar-plan.md` already anticipates.

**Query scan 50 GB** — a cap-using project retains 233 MB compressed, so this
is **214 full rescans per month (~7/day)**.
CPU: 50 GB at ~150 MB/s/core effective = 341 s = 0.095 OCPU-hr × $0.0138 =
$0.0013. Class B worst case (all misses, ~1 MB slices) = 50,000 GETs = $0.018,
the same order as the already-accepted "Object GET 100k → $0.036" line.

Scanned bytes is the right unit: the engine already measures it
(`metrics.rs:19` `query_scanned_bytes`, `config.rs:48` `max_query_scan_bytes`),
it matches Loki / CloudWatch / BigQuery convention, and unlike a query *count*
it actually bounds cost — a single query can scan everything.

**Active streams 100** — structurally identical to the metrics active-series
cap. `stream.idx` is excluded from the cache ceiling (`ARCHITECTURE.md:147`),
so **cardinality explosion becomes un-evictable disk**. A well-structured app's
`{service, env, level}` combinations number 10-30; 3x gives 100. Enforce with
the same keep-existing / drop-new semantics as `fn0/src/metric_gate.rs`, and
report drops back as `fn0.logs.dropped`.

This is the gap `PRODUCTION_READINESS_REVIEW.md:276` records as
`max_streams_per_user` — "없음 — 테넌시 필요".

## Implementation status

| Layer | State |
|---|---|
| Tenant identity | **done** — `tenant.rs` `TenantId` allowlist, `X-Scope-OrgID` on Loki push and OTLP gRPC metadata, `LOGGYTRACY_DEFAULT_TENANT` / `LOGGYTRACY_MISSING_TENANT_POLICY` |
| Journal | **done** — `LGY3` framed record carries the tenant; replay restores each record under its own tenant, and a pre-tenancy record falls back to the default tenant |
| MemTable | **done** — `HashMap<TenantId, HashMap<Labels, …>>`; every read method requires a tenant |
| Log part format | **done** — `_tenant` leading column, `(tenant, timestamp_ns)` sort, tenant-aligned row groups, per-tenant index in `meta.json` |
| Trace part format | **done** — same shape (`tenant` column, tenant-aligned row groups, tenant index) |
| Read path isolation | **done** — `PartReader`/`PartRegistry`/`TraceRegistry`/MemTable queries, `label_names`, `label_values`, `series`, `stats` all take a required tenant; part-level time pruning is per tenant |
| Merge | **done** — reads rows through the tenant index and re-sorts by `(tenant, ts)`; no cross-tenant mixing beyond the shared object |
| Per-tenant retention | **deletion side done**, intake being replaced — the control plane pushes one tenant at a time and it is applied at deletion time; partitions stay on `day`. See [`RETENTION_DESIGN.md`](RETENTION_DESIGN.md) |
| Partition on `(tier, day)` | **rejected** — retention baked in at write time cannot honour a plan change; superseded by [`RETENTION_DESIGN.md`](RETENTION_DESIGN.md) |
| Sidecar consolidation | **not done** — still four `PART_FILES` |
| Range GET / per-`(part, tenant)` cache | **not done** |
| Quotas, per-tenant semaphores, tenant-labelled metrics | **not done** |
| Durable usage accounting | **not done** |

`/metrics` deliberately keeps process-wide gauges (`global_stats`): it is the
operator scrape, not a tenant-facing endpoint.

### Open questions

**Where does a tenant's tier come from?** *Answered* — the control plane pushes
one tenant's retention at a time, loggytracy persists it before acknowledging,
and it is applied at **deletion** time rather than at write time. `Partition on (tier, day)` was rejected along with every
other scheme that fixes retention when the bytes are written, because none of
them honours a plan upgrade or downgrade. Partitions stay on `day`. Full record:
[`RETENTION_DESIGN.md`](RETENTION_DESIGN.md).

**Parquet footer size at high tenant counts** remains unmeasured — the open item
recorded under [Known read-side overhead](#known-read-side-overhead).

## Implementation checklist

Ordered by dependency.

### 1. Tenant identity

- [x] Extract `X-Scope-OrgID` in the Loki push path and the OTLP gRPC metadata.
- [x] Strict allowlist validation before journal append.
- [x] Config for the missing-header policy (accept as default tenant vs reject).
- [x] Thread tenant through `Journal::append` / `append_trace` records.

### 2. Storage format

- [x] Add `tenant` to the row model; sort by `(tenant_id, timestamp_ns)` in
      `part/format.rs`.
- [x] Align row-group boundaries to tenant boundaries in `row_group_bounds`.
- [x] Add the per-tenant index to `meta.json` (`part/metadata.rs`).
- [x] ~~Partition on `(tier, day)` instead of `day`~~ — rejected; per-tenant
      retention is applied at deletion time instead ([`RETENTION_DESIGN.md`](RETENTION_DESIGN.md)).
- [ ] Consolidate `PART_FILES` into a single container object.

### 3. Read path

- [x] Per-tenant `min_ts`/`max_ts` pruning from the meta index.
- [ ] Range GET using `byte_range` in `download_part`.
- [ ] Key the local cache by `(part_id, tenant)`.
- [ ] Parallelize `restore_parts` and `restore_catalog` (both are serial
      `for … .await` loops today; 20 missed parts = 20+ sequential round trips,
      bounded only by `max_restore_runtime` at 25s, `config.rs:116`).

### 4. Isolation surface

Every method below currently returns data across all tenants. **Make the
tenant a required argument enforced by the type system** — this is the whole
isolation boundary once files are no longer separate.

- [x] `PartRegistry`: `label_names`, `label_values`, `series`, `stats`
- [x] `MemTable`: `label_names`, `label_values`, `series`, `stats`
- [x] Tempo: `trace_by_id`, `search`, `search_tags`, `search_tag_values`

### 5. Quotas and limits

- [ ] Per-tenant ingest rate and monthly volume, checked **before** journal
      append.
- [ ] Active stream count cap, keep-existing / drop-new.
- [ ] Query scanned-bytes accounting per tenant, monthly + hourly.
- [ ] Per-tenant semaphores replacing the global ones in `app_state.rs:16-18`.
- [ ] Per-tenant cache budget (see risks below).
- [ ] Tenant labels on all quota and rejection counters in `metrics.rs`.

### 6. Durable usage accounting

Monthly quotas need a counter that survives restart, and **it cannot be derived
from live parts** — retention deletes the evidence before the month ends.

- [ ] Store per-tenant monthly usage in the object store, CAS-updated like the
      manifest.
- [ ] Tie increments to `FlushTransaction` (`flush.rs:312-322`), which already
      provides a crash-consistent commit boundary keyed on the journal
      checkpoint. No new durability mechanism required.
- [ ] Restore in-memory counters at startup from the durable value plus
      unflushed memtable bytes.

## Risks and watch items

**Cache hit rate degrades as project count grows.** `cache_max_bytes` is a
single global setting (`config.rs:39`, default 10 GiB) and `evict_cache` is one
global LRU pass. At 1,000 projects × 233 MB retained = 233 GB against a 10 GiB
cache, the Class B "worst case" above becomes the expected case. Compare
$0.36/M Class B against OCI block volume at $0.025/GB-month: **growing the
cache is almost always cheaper than eating the misses.** Track this as an
operational scaling item.

**Cross-tenant cache interference.** One tenant's wide historical query evicts
everyone else's hot slices. Same class of problem as the global semaphores.
Needs a per-tenant cache budget.

**Cardinality explosion now pollutes shared parts.** Per-tenant ingest limits
move from "nice to have" to prerequisite.

**`is_empty()` skip stops helping.** With many tenants someone is always
sending, so flush runs every cycle. This is fine — cost is now proportional to
cadence, not tenant count, which makes it predictable.

**Trace sampling is the reserve lever.** The sizing above assumes 100% trace
collection, and traces are ~4x the volume of logs. If costs run over, head
sampling (always keep error traces, sample successful ones) adjusts spend
without touching user-facing quotas.

**Single writer is assumed throughout.** Confirmed out of scope — shared parts
would need rework for multiple writers. If bring-your-own-R2 ships later, route
those tenants through a separate per-tenant path instead.

## Related documents

- `ARCHITECTURE.md` — engine architecture; its "테넌시" section (48-65) is
  superseded by this document
- `PRODUCTION_READINESS_REVIEW.md` — P0-3 (119-145) is the production gate this
  design closes; line 276 records the missing `max_streams_per_user`
- `~/fn0/docs/cloud/dollar-plan.md` — plan economics, unit costs, quota method
- `~/fn0/docs/fn0/limits.md` — published quota table this must extend
