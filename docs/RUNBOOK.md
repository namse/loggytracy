# Operations runbook

Setting meanings are in [`CONFIGURATION.md`](CONFIGURATION.md), and design rationale is in
[`ARCHITECTURE.md`](ARCHITECTURE.md). This document covers only **what to look at and what to do when something goes wrong**.

---

## Deployment prerequisites — start here

This engine is **single-machine, single-writer**. Breaking that assumption corrupts data.

1. **The disk must follow the pod.** The WAL in `LOGGYTRACY_DATA_DIR` is the **only copy** of data acked
   since the last flush. Use a StatefulSet + fixed PV, with the volume following the pod when rescheduled
   to another node. Deployment + emptyDir loses data.
2. **Set `terminationGracePeriodSeconds` high.** Force-flush retries without a hard timeout during shutdown.
   If the orchestrator sends SIGKILL after 30 seconds, unflushed data remains only in the WAL, and the disk
   may be discarded when the pod is scheduled on another node. **At least 10 minutes, preferably more.**
3. **Use one replica.** Raising this to two or more lets the second instance claim the writer epoch and the
   first one is fenced and killed. This is intentional, but such a configuration must not be created.
4. **Keep the listening address inside the trust boundary.** TLS and authentication are outside this process,
   and `X-Scope-OrgID` is trusted without proof.

## What to alert on

| Signal | Condition | Meaning |
|---|---|---|
| `loggytracy_ingest_throttled_total` | Increasing | Returning 429; flush cannot keep up with ingest |
| `loggytracy_wal_backlog_bytes` | Upward trend | Same cause, earlier signal |
| `loggytracy_flush_errors_total` | Increasing while `flush_success_total` is flat | **Flush stopped.** Most dangerous state |
| `loggytracy_remote_healthy` | Stays 0 | Object store unreachable |
| `loggytracy_merge_debt_parts` | Upward trend | Merge cannot keep up; query-planning cost rises |
| `loggytracy_retention_rewrite_skipped_total` | Increasing | A part is too large to rewrite. **Tenant deletion is not complete** |
| `loggytracy_tenant_policy_unknown_tenants` | Greater than 0 | Unknown tenants are accumulating data from the control plane's perspective |
| `loggytracy_ingest_quota_rejected_total` | Increasing | A tenant exceeded its rate. **Different from `ingest_throttled_total`** — the server is healthy and the tenant is sending more than its plan allows; this is a plan issue, not a scaling issue |
| `loggytracy_pending_flush_bytes` | Does not reach 0 while draining | Shutdown has not reached durability |
| `loggytracy_part_sidecar_resident_bytes` | Rising relative to RSS budget | Sidecars are not evicted; resident memory is linear in part count |
| `loggytracy_part_tenant_segments` | Near `part_count × tenant count` | Each tenant is scattered across nearly every part, maximizing shared-part fixed cost |

`part_tenant_segments` is the number of (tenant, part) pairs. Divide by `part_count` for the average tenant
width of one part, or divide by tenant count for **the number of parts containing one tenant**. Each pair
adds one row group, two blooms, and one metadata segment, so the cost of a nearly idle tenant is determined
by this value rather than its own ingest volume.

`/ready` is lowered **independently** by flush, merge, retention, OTLP, object storage, and the local cache.
The 503 body identifies the problem.

---

## Response by symptom

### `/ready` stays at 503

```
curl -s localhost:3100/ready          # the body identifies the component
curl -s localhost:3100/metrics | grep _errors_total
```

Continue below based on what is increasing.

### Flush stopped (`flush_errors` increases, `flush_success` is flat)

WAL backlog and MemTable continue growing, and 429 starts once the limit is exceeded. Data is still safe — it is in the WAL.

1. If `remote_healthy` is 0, it is an object-store problem. Continue to the item below.
2. If the log says `fenced by a newer writer`, **another instance claimed the prefix.** This process will
   exit soon. Find why two instances were started first. **Do not discard this disk** because it contains unflushed data.
3. Check whether the disk is full. The WAL is truncated only after a successful flush.

### Object store is unreachable

The engine retries forever and keeps `/ready` at 503. Automatic recovery is expected, so **waiting is the default response**.
Ingest continues until the backlog limit, then returns 429 to clients.

During startup, it retries for `LOGGYTRACY_STARTUP_RETRY_BUDGET` (5 minutes by default) and then exits.
If it looks like a crash loop, fix the store rather than increasing that budget.

### Startup is rejected with a "conditional writes" error

The preflight detected **a store that does not enforce conditional writes**. Running it as-is causes manifest
lost updates, which means data loss.

For an S3-compatible store, set `OBJECT_STORE_CONDITIONAL_PUT=etag`. For a single-process development
store, use a `file://` URL — it intentionally gives up CAS.

### Disk is full

```
du -sh $LOGGYTRACY_DATA_DIR/*        # which of wal / parts / traces is large
```

- **parts/traces are large** → Reduce `CACHE_MAX_BYTES`. They are cache only and can be restored from S3.
  However, if `RETENTION_PERIOD` is unset, **nothing is deleted from S3** — decide that first.
- **WAL is large** → Flush is not progressing. Follow the item above.
- The stream index is not evicted. A label-cardinality explosion becomes **non-evictable disk usage**,
  so the only remedy is to fix labels at ingest.

### Tenant deletion does not finish

If `retention_rewrite_skipped_total` increases, a part cannot be rewritten within `MERGE_MAX_MEMORY_BYTES`.
It is already invisible to queries, but **the bytes remain.**

Increase `MERGE_MAX_MEMORY_BYTES` or reduce `ROW_GROUP_SIZE` (which makes windows smaller).

### Two instances started with the same prefix

The old instance logs `fenced by a newer writer` and exits with code 1. This means the defense worked, not
that an incident occurred. However:

- **Do not discard the old instance's disk.** Its WAL contains unflushed data.
- The new instance operates normally.
- To preserve old data, stop the new instance, restart the old instance on its disk, let flushing complete,
  and then replace it through the normal procedure.

---

## Planned hardware replacement

Following the order is lossless. **If the order is violated, fencing kills the old instance**, so the violation
will not go unnoticed.

1. Send `SIGTERM` to the old instance. Draining starts and ingest returns 503.
2. Wait until `curl /metrics | grep pending_flush_bytes` is **0** and `force_flush_complete` is 1.
   The log warns the operator if this takes a long time. **Waiting is correct.**
3. The process exits on its own. Exit code 0 means all acked data is durable. **Do not discard the disk if it is not 0.**
4. Start the new instance afterward.

The exit code in step 3 is the only basis for judgment. SIGKILL has no exit code, so if step 2 was skipped
and the process was forced down, restart it on that disk to recover.

## Recovery after forced termination

The WAL remains. Restarting on the **same disk** lets replay recover unflushed data.

Because delivery is at-least-once, **some logs may be duplicated** if termination happens at a flush boundary.
This is an intentional trade-off (`ARCHITECTURE.md`), and the lack of a way to observe duplicates is a known gap.

## Backups

S3 is the source of truth. The local disk is the cache plus the unflushed WAL.

- Configure object-store versioning/replication policy **in the store**. The engine does not manage it.
- One manifest object contains the complete part list. If it is lost, the catalog disappears even when part
  objects remain. **Versioning is strongly recommended.**
- Backing up the local disk is not meaningful — it contains either cache data or data that is not yet durable.
