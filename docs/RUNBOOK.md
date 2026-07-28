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
   The point of the long grace period is that the decision to give up should be an operator's
   (`kill -USR1`, which exits non-zero and says so) rather than a timer's (SIGKILL, which says nothing).
3. **Use one replica.** Raising this to two or more lets the second instance claim the writer epoch and the
   first one is fenced and killed. This is intentional, but such a configuration must not be created.
4. **Keep the listening address inside the trust boundary.** TLS and authentication are outside this process,
   and `X-Scope-OrgID` is trusted without proof.

## Sizing

**Size on peak, not on idle.** Measured at 8000 eps with 500 tenants: RSS idles
around 15 MB and peaks around 850 MB, reached within a minute of load starting,
and returns to idle when load stops. That is live memory held while ingest,
flush and merge overlap — not a leak, and not something a smaller
`merge_max_memory_bytes` reduces, because the groups being merged are usually
far below that budget already. An instance sized from a quiet screenshot is
sized roughly fifty times too small.

## What to alert on

| Signal | Condition | Meaning |
|---|---|---|
| `loggytracy_ingest_throttled_total` | Increasing | Returning 429; flush cannot keep up with ingest |
| `loggytracy_wal_backlog_bytes` | Upward trend | Same cause, earlier signal |
| `loggytracy_flush_errors_total` | Increasing while `flush_success_total` is flat | **Flush stopped.** Most dangerous state |
| `loggytracy_remote_healthy` | Stays 0 | Object store unreachable. Set by three consecutive failures with no success between them, so an isolated failed request does not trip it |
| `loggytracy_remote_consecutive_failures` | Rising but below 3 | The store is degrading without being declared down — the early signal the health flag deliberately hides |
| `loggytracy_merge_debt_parts` | Upward trend | Merge cannot keep up; query-planning cost rises |
| `loggytracy_retention_rewrite_skipped_total` | Increasing | A part is too large to rewrite. **Tenant deletion is not complete** |
| `loggytracy_tenant_policy_unknown_tenants` | Greater than 0 | Unknown tenants are accumulating data from the control plane's perspective |
| `loggytracy_wal_replayed_entries` | Non-zero after a restart | The previous run did not shut down cleanly. **This is the upper bound on log lines this restart may have duplicated** — delivery is at-least-once, so records the WAL still held may already have been durable |
| `loggytracy_stream_limit_rejected_total` | Increasing | A tenant is creating streams past its limit. **Usually a client putting a request id or timestamp in a label**, not a plan being outgrown — check the label names before raising anything |
| `loggytracy_query_quota_rejected_total` | Increasing | A tenant is over its read quota — scan rate or concurrency. Like the ingest one: a plan question, not a scaling one |
| `loggytracy_ingest_quota_rejected_total` | Increasing | A tenant exceeded its rate. **Different from `ingest_throttled_total`** — the server is healthy and the tenant is sending more than its plan allows; this is a plan issue, not a scaling issue |
| `loggytracy_delete_hidden_rows_total` | Rising steadily long after a request | The rows are hidden but not gone — no rewrite has reached the parts holding them. See below |
| `loggytracy_delete_requests_rejected_total` | Increasing | A tenant is at the per-request limit. Each outstanding request is a predicate every one of that tenant's scans evaluates per row |
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

### A deletion request stays at `received`

`GET /loki/api/v1/delete` reports `received` until no part could still hold a
row the request covers. The rows are already unreadable — the scan masks them
from the moment the request was accepted — so this is not an availability
problem. It means the bytes are still there.

The removal happens inside the merge rewrite. A part that could hold a covered
row is selected for rewrite on that basis alone, whatever its size, so the
request advances on the next merge tick that reaches it. If it does not:

- Check `loggytracy_merge_success_total` is increasing. Nothing is removed while
  merge is failing.
- Check `loggytracy_retention_rewrite_skipped_total`. A part too large to rewrite
  within `MERGE_MAX_MEMORY_BYTES` blocks deletion for the same reason it blocks
  tenant deletion, and the same fix applies.
- `MERGE_MAX_GROUPS_PER_TICK` bounds how many parts one tick rewrites, so a
  tenant spread across many parts advances a few per tick rather than all at
  once.

The status is deliberately conservative: part metadata records `streams` for the
whole part, so a part holding that stream for a *different* tenant keeps the
request at `received`. It never claims a removal that has not happened.

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

### Shutdown takes longer than expected

The drain waits for the merge group that was running when the signal arrived.
Merge stops taking *new* groups the moment it is told to drain, so the bound is
one group, not a whole tick — but one group can be a minute at scale, and
`MERGE_MAX_INPUT_BYTES` is what sets it.

If SIGTERM to exit is much longer than that, read the log timeline rather than
guessing: each stage announces itself (`flush task stopped`, `merge task
stopped`, `servers drained; force-flushing before exit`). Measured on a
two-hour run at 500 tenants, the force-flush itself was 47 ms.

### When the force-flush cannot finish

If the object store is down, step 2 never completes. That is the design: giving up would lose data, so the
retry has no timeout. Two ways out, and they are not equivalent.

- **`kill -USR1 <pid>`** — abandon the force-flush deliberately. The process exits **non-zero**, logs that
  it did so, and leaves the data in the WAL. Use this when the store will not recover soon and the pod has
  to go. Then **keep the disk** and restart on it.
- **Doing nothing until the grace period expires** — the orchestrator sends SIGKILL. The data is in the
  same place, but there is no exit code and no log line saying why, so nothing distinguishes it from a
  clean shutdown afterwards. This is the case `terminationGracePeriodSeconds` is set high to avoid.

Inside a container stdin is not a TTY, so the `exit`/`quit` command on stdin is only useful when a person
is attached to a terminal. `SIGUSR1` is the one that works in a deployment.

## Recovery after forced termination

The WAL remains. Restarting on the **same disk** lets replay recover unflushed data.

Because delivery is at-least-once, **some logs may be duplicated** if termination happens at a flush boundary.
This is an intentional trade-off (`ARCHITECTURE.md`).

How much was duplicated is now observable. Startup logs a WARN naming the record and entry counts it
replayed, and `loggytracy_wal_replayed_entries` holds the entry count for the life of the process. It is an
upper bound, not a measurement: those entries may have been durable already, or may not have been. Nothing
removes the duplicates yet — that is deduplication, still open in `todo.md`.

## Backups

S3 is the source of truth. The local disk is the cache plus the unflushed WAL.

- Configure object-store versioning/replication policy **in the store**. The engine does not manage it.
- One manifest object contains the complete part list. If it is lost, the catalog disappears even when part
  objects remain. **Versioning is strongly recommended.**
- Backing up the local disk is not meaningful — it contains either cache data or data that is not yet durable.
