# Production readiness review (2026-07-26, fresh context)

Reviewed revision: `32dd7b6` (multi-tenancy step 1 + retention push complete, working tree clean)
Review scope: entire `src/` (28,300 LOC), `docs/`, and deployment assets
Validation: all 274 `cargo test` tests passed, with zero `cargo clippy --all-targets` warnings

The previous review is [`PRODUCTION_READINESS_REVIEW.md`](PRODUCTION_READINESS_REVIEW.md) (revision `56afbbe`).
This document rereads the repository from scratch, including the multi-tenancy and retention code added since
then. It continues the previous review's item numbers (P0-1 and so on) and prefixes new findings with `N`.

> **Update (after fixes):** The diagnoses below reflect the review point (`32dd7b6`). P0-1, P0-2, and N1
> in Gate 1 were fixed immediately after this review. See each item heading and the "Change history" section.

## Verdict

**Not production-ready at the time of review.** P0-1 and P0-2, which the previous review designated as
Gate 1 (data safety), remained open, and the newly added multi-tenancy/retention code combined with P1-8
(merge-limit unit mismatch) to **break even the physical guarantee of tenant deletion.**

Changes since the previous review:

| Item | Previous | Current |
|---|---|---|
| P0-1 WAL compaction wedge | Open | **Open** (reproduced in this review) |
| P0-2 ingest backpressure | Open | **Open** (no 429 path in the code) |
| P0-3 multi-tenancy not implemented | Open | **Identification, isolation, and retention closed**, throttles/quotas open |
| P1-1 Tempo search restores everything | Open | **Fixed since** — the window reaches the pin set and the row-group selection (see N6) |
| P1-4 writer fencing | Open | **Fixed since** — manifest `writer_epoch` |
| P1-8 merge-limit unit mismatch | Open | **Fixed since** — `materialized_bytes`, plus the split fallback (see N1) |

## Reproduced in this review

P0-1 was confirmed by execution rather than code inspection. A consecutive-compaction test was temporarily
added to `src/journal/tests.rs`, observed, and reverted.

```
first_offset=57 wal_after_first=0 second_offset=49 wal_after_second=49
result=Err(InvalidInput, "WAL compaction checkpoint moved backwards")
```

---

## Gate 1 — deployment-blocking conditions (open)

### P0-1. WAL compaction wedge — later **fixed**

- Location: `src/journal/compaction.rs`, `src/journal/replay.rs:37`; wedge propagation in `src/flush.rs:51-84`

`compact_wal` ends by leaving a state file with `write_compaction_state(&state_path, &state, 2)` and
**does not delete it.** Compaction truncates the WAL and resets checkpoint to 0, so the next offset is in
a new coordinate system, but the next call compares it with a stale offset from the old system.

| Next offset | Result |
|---|---|
| `< state.offset` | `Err("WAL compaction checkpoint moved backwards")` → permanent wedge |
| `== state.offset` | Silent no-op; WAL is not truncated |
| `> state.offset` | Happens to work |

It cannot recover by restart because of `if state.phase != 1 { return Ok(()) }` at `replay.rs:37`.
The wedge propagation path was also confirmed: `pending_checkpoint` in `flush.rs` remains `Some` forever and
`continue` runs every tick, **blocking entry into a new flush** while ingest continues returning `204`.

### P0-2. No ingest backpressure — later **fixed**

- Location: `src/ingest.rs:114` (`push_inner`), `src/trace_ingest.rs`, `src/config.rs`

`rg "429|TOO_MANY_REQUESTS"` found no 429 path anywhere in the code. `push_inner` checks only draining,
body size, labels, lines, and timestamps; it does not check MemTable/WAL backlog and has no corresponding knobs.
Combined with P0-1, "failure → guaranteed OOM" follows directly.

### P1-4. No single-writer enforcement (open)

`src/object_storage/catalog.rs` has no lease/fencing token. Two processes with the same prefix both believe
they can operate normally. M6 hardware replacement reaches exactly this state if the new instance starts
before the old one is fully dead.

---

## N — new findings from this review

### N1. Tenant deletion (`retention: "0"`) does not guarantee physical deletion

Two paths cause this simultaneously.

**(a) Large parts are never rewritten.**
`src/merge/scheduler.rs:150-160` counts a read failure for a retention-only group as
`retention_rewrite_skipped` and continues. Comments justify this as "the input is fixed, so the next tick
will not fit either; it is only a missed optimization," but **in the zero-retention path it is not an
optimization; it is the deletion itself.**

Using the defaults:

| Value | Default |
|---|---|
| `merge_max_part_rows` | 4,000,000 |
| `merge_max_memory_bytes` | 1 GiB |
| Budget per row | About 268 B (including `size_of::<Row>()`) |

Unless lines are short, rewriting a full-size part is structurally impossible, and deleted-tenant rows
remain forever on disk and S3. The previous review classified P1-8 as a merge-performance problem; with
tenant deletion now present, it also means **deletion requests cannot be handled.**

**(b) Deletion depends on one runtime flag.**
If `LOGGYTRACY_TENANT_POLICY_TOKEN` is missing, `TenantPolicy::load` (`tenant_policy.rs:419`) returns
`disabled()`. Then every `query_floor_ns` becomes `None`, and **all deleted data that was hidden without
being rewritten becomes queryable again.** The admin route also disappears (`router.rs:35`), leaving no
surface for an operator to notice. A restart with one missing environment variable resurrects data.

**Remediation**
- Split and retry a group when rewriting fails; split a single part by row-group range and rewrite it into
  multiple output parts. Promote zero-retention failures from `retention_rewrite_skipped` to errors.
- Refuse startup when any policy is stored but the token is missing.

### N2. No tenant allowlist

`TenantId::parse` (`src/tenant.rs:18`) checks only `[a-zA-Z0-9_-]{1,64}`. Without allowlist validation,
anyone who can send the header can create unlimited tenants. `todo.md` marks this complete, but the code
does not implement it. N3 and N4 amplify the result.

### N3. Row groups are forcibly split at tenant boundaries

`row_group_bounds` in `src/part/format.rs:210` ends a row group whenever the tenant changes. This is
correct, but **tenant count becomes a lower bound for row-group count.** In the target workload with many
small tenants, one flush containing 500 tenants produces 500 five-row groups; Parquet column-chunk metadata
and per-group bloom filters scale with tenant count, and compression collapses. This must be considered
alongside the 1 MiB default `flush_max_bytes`.

### N4. `/metrics` traverses every part on every scrape

`tenant_policy_gauges` (`src/query/handlers.rs:587`) scans tenant segments in every part, while
`merge_debt_part_count` in the same handler calls `select_groups` → `estimated_part_bytes` → `fs::metadata`
for every part. This is O(parts × tenants) work on an unauthenticated endpoint and is even heavier than
the previous review's P2-7 finding.

### N5. The part on-disk format has no version field and its upgrade path is asymmetric

`MetaFile` (`src/part/metadata.rs:99`) gained `tenants: Vec<TenantSegment>` without `#[serde(default)]`
and has no `version` field. Parts from an existing deployment fail `meta.json` deserialization and panic at
`startup.rs:132`.

In contrast, the WAL explicitly handles pre-tenancy records as the default tenant at `replay.rs:145-153`.
**The journal is designed for lossless upgrades, but parts cannot be read at all.** This is not currently
visible because there is no data, but every future schema change hits the same wall without a format version field.

### N6. Tempo metadata paths improved, but have no time pruning

`pin_all_trace_parts` changed to `tenant_part_ids(tenant)`, narrowing it to the tenant range (partial P1-1
improvement). But `search` computes start/end and passes `scan_trace_spans(..., None, ...)`, then **filters
after scanning** (`tempo/handlers.rs:113-130`), while `search_tags`/`search_tag_values` have no time
parameters. One Grafana tag-dropdown opening still restores all traces for that tenant.

### N7. A single failed request marked the object store down — **fixed**

- Location: `RemoteCache` health state (`src/object_storage/catalog.rs`)
- Verification: **Reproduced and fixed**, `docs/LOAD_RESULTS.md` section 6

`/ready` reads `remote_healthy`, and one failed object-store request set it.
At a 3% injected write-error rate — which the engine survives with no ingest
errors and no lost data — the flag flipped 14-17 times a minute and read false
34-59% of the time. That is an instance an orchestrator pulls in and out of
service over an error rate that cost nothing.

The first explanation recorded here was wrong and is left in
[`LOAD_RESULTS.md`](LOAD_RESULTS.md) rather than quietly replaced: it blamed the
epoch guard in `mark_remote_healthy_since`, predicting that recovery degrades as
reporters multiply. Sampling the flag through a run refutes that — the
configuration with *fewer* reporters has the *worse* duty cycle. Both flapped,
and the PASS/FAIL difference between them was which side of a signal changing
every few seconds the terminal sample happened to land on.

**Fixed**: health is hysteretic. Three consecutive failures with no success
between them mark the store down; one success clears it. Callers report the
outcome of the operation they finished instead of capturing an epoch first,
which removed the guard along with its call-site ceremony. What is given up is
that a slow success predating an outage now resets the count, delaying detection
by one more round of failures — bounded, where the guard's failure mode was not.
`loggytracy_remote_consecutive_failures` exposes the pressure the flag now hides
below its threshold. Measured after: 99.3-100% healthy, 0-2 transitions.

### N8. Merge and flush contend — **measured, and it is bounded**

- Verification: **Measured**, `docs/LOAD_RESULTS.md` section 6

With merge enabled the WAL backlog runs several times higher than with it
disabled. Sampled every 500 ms over ten minutes it oscillates between 6 and
140 MB with a linear trend of -0.04 MB/s and a second-half mean below the first
half's. Merge and flush contend and flush catches up, so this is a tuning note
rather than a defect: the contention costs backlog depth, not divergence.

It also produced the third instance in this investigation of a terminal sample
meaning nothing — the peak was 140 MB while the run ended at 47.6 MB. The
harness now samples the backlog and gates on whether flush ever drains it,
against the engine's own `max_wal_backlog_bytes` rather than a number the
harness invented.

What is not settled is the depth itself. 140 MB of unflushed WAL is 140 MB that
a simultaneous loss of machine and disk would take, and that window widens with
merge running. Whether that is acceptable is a retention-of-risk question for
the deployment, not a defect in the engine.

### N9. Peak RSS is concurrent live memory — **explained, not a defect**

- Verification: **Measured**, `docs/LOAD_RESULTS.md` section 7

Peak RSS is 173-187 MB with merge off and 697-758 MB with merge on, and cutting
`merge_max_memory_bytes` eightfold barely moved it. Two hypotheses were recorded
and both are wrong: it is not the merge budget, and it is not allocator
high-water retention either.

Watching RSS through a run and for 90 seconds after settles it. The peak is
853.6 MB; the final reading is 14.8 MB, back to the starting value, while merge
kept running through the idle tail. The memory is live, held while ingest, flush
and merge overlap, and the allocator returns it. The budget knob did not move it
because the groups being merged were far smaller than the budget — with parts of
a few megabytes, `merge_max_input_bytes` was never binding.

**Consequence is a sizing rule.** Peak is roughly 50x idle and is reached within
a minute of load starting, so an instance sized from its idle footprint is sized
about fifty times too small. [`RUNBOOK.md`](RUNBOOK.md) says so.

---

## Still open from the previous review

Only status is confirmed here; see the previous document for details.

| Item | Status |
|---|---|
| P1-2 OTLP log ingest unimplemented | **Fixed** — `LogsService` on the gRPC listener, plus `POST /v1/logs` and `/v1/traces` on the HTTP one. Both transports share one admission and normalization path |
| P1-3 group commit consumes batch timer | **Fixed** — the batch loop no longer waits out `max_batch_ms` on an empty channel, and the default is now zero linger |
| P1-5 MemTable O(rows) size calculation + flush deep clone | **Fixed** — size in O(1) (with P0-2); the snapshot is now shared with the flush through an `Arc` instead of copied |
| P1-9 eviction holds write lock during synchronous directory traversal | **Fixed** — the work runs in `spawn_blocking` with the guard moved into it, and it is now driven by the registry's part directories rather than by two levels of `read_dir` over the whole tree |
| P1-10 object-store startup errors panic | **Fixed** — retried within `LOGGYTRACY_STARTUP_RETRY_BUDGET`. The remaining `panic!` sites are the deliberate give-up past that budget |
| P1-11 startup/flush cost linear in part count | **Partly fixed** — restore overlaps its downloads and reconcile no longer re-reads the manifest per merge group. The manifest is still rewritten in full per flush |
| P2-1 Loki/Tempo API gaps | **Mostly fixed** — `tail`, `index/volume(_range)`, `detected_labels`, `detected_fields`, `format_query`, JSON push, Tempo v2 tags and `/api/echo`. `patterns` and the `delete` API remain, both deliberately |
| P2-2 resource guards on metadata endpoints | **Fixed** — semaphore, timeout, `start`/`end`, and `match[]` count limits |
| P2-5 duplicates after a crash unobservable | **Fixed** — startup reports replayed records and entries, as a WARN and as `loggytracy_wal_replayed_entries`. Removing them is fixed too: every part is written through one sort, which drops entries identical in tenant, stream, timestamp, line and metadata. The replay counters remain the upper bound on what a restart introduced before the next merge collapses it |
| P2-7 `/metrics` has no histograms or labels | **Partly fixed** — latency histograms are there, so p95/p99 is derivable. Endpoint labels are still absent |
| P2-8 stdin abort ineffective in containers | **Fixed** — `SIGUSR1` abandons a stuck force-flush and exits non-zero. stdin is kept for interactive use |
| P3 deployment assets | **Fixed** — Dockerfile, [`CONFIGURATION.md`](CONFIGURATION.md), [`RUNBOOK.md`](RUNBOOK.md) |
| P2 real S3 validation | **Confirmed out of scope** — local MinIO is the limit ([`LOAD_VALIDATION.md`](LOAD_VALIDATION.md)) |

---

## Strong areas (in the newly added code)

- **Retention push design.** The code records the rationale for "ack after storage"
  (`tenant_policy.rs:497-513`), fatal startup-load failures, the distinction *unknown tenant* ≠ *infinite*,
  and preventing push-age reversal by recording `updated_at` under `write_lock`. Removing polling and the
  `reqwest` dependency also leaves object storage as the only outbound call.
- **Admin authentication.** Not mounting routes when the token is unset, constant-time `secret_matches`,
  a 4 KiB body limit, and separating `push_rejected` from `admin_unauthorized` correctly distinguish
  "is the control plane broken?" from "who is knocking?"
- **Tenant isolation is applied across read paths.** `query_floor_ns` covers every log/metric/labels/
  label_values/series/index_stats/Tempo handler, and splitting shared-part row groups at tenant boundaries
  guarantees isolation at the index level.
- **Retention reuses merge transactions, tombstones, and manifest CAS** instead of writing its own parts,
  a good choice that does not expand the crash-safety surface.
- Changing log deletion to **unregister immediately without waiting for trace deletion failure**
  (`retention.rs:255-262`) is correct.

---

## Change history

Immediately after this review, a batch of Gate 1 items was fixed. 285 tests passed, with zero clippy warnings.

| Item | Status | Summary |
|---|---|---|
| P0-1 WAL compaction wedge | Fixed | Remove the intent record durably immediately after success. Phase 2 is represented by record absence; surviving phase-2 records are treated as complete and removed, so already-wedged instances recover automatically after upgrade |
| P0-2 ingest backpressure | Fixed | Track MemTable/WAL backlog in O(1); above the limit, return `429` + `Retry-After` before journal append (`RESOURCE_EXHAUSTED` for OTLP) |
| N1(a) merge split fallback | Fixed | Split groups in half and rewrite single parts in row-group windows. Tenant deletion no longer permanently skips large parts |
| N1(b) startup without policy token | Fixed | Refuse startup when any policy is stored but the token is missing |
| N5 format version field | Fixed | Add `version` to part/trace-part `meta.json`. Check before checksum validation so format differences are not treated as corruption |
| P1-8 merge-limit units | Fixed | Add `materialized_bytes` to part metadata. Group selection and read budgets use the same unit, and `validate` enforces limit ordering |
| N4 `/metrics` O(parts) | Fixed | The merge worker publishes merge debt and the retention worker publishes unknown tenants. Scrapes only read |
| N2 tenant allowlist | Fixed | `LOGGYTRACY_ALLOWED_TENANTS`; tenants outside the list receive 403 |
| N6 Tempo time pruning | **fixed** | `search`, `search_tags` and `search_tag_values` all take `start`/`end`, and the window reaches the pin set and the row-group selection. `search` now matches a trace on span overlap rather than on its earliest span, which is both Tempo's rule and the one the row-group bounds can answer |
| P2-2 metadata guards | Fixed | Semaphore, timeout, `start`/`end`, and `match[]` count limits |
| P1-4 writer fencing | Fixed | `writer_epoch` in the manifest; claim at startup, verify on every CAS, self-fence and terminate on fencing |

### P0-1 — fix details

`compact_wal` removes the intent record at the end of the success path and fsyncs the parent directory.
The record no longer exists in the normal state, so the next compaction cannot compare offsets from different
coordinate systems. Only two kinds of records can remain, each with one meaning.

| Record | Meaning | Handling |
|---|---|---|
| Phase 1 + tmp exists | Crash before rename | Restore old checkpoint and retry (unchanged) |
| Phase 1 + tmp missing | Crash after rename | Treat as complete and remove the record |
| Phase 2 | Leftover from an old build | Treat as complete and remove the record |

`replay.rs` follows the same rules, so restart recovery also works. Tests cover consecutive compactions
(shrinking, equal, and growing cases), compaction and replay recovery for an old-build phase-2 leftover,
and caller retry after intent-removal failure. Crash injection is armed per WAL path — a process-global flag
could be consumed first by an unrelated parallel test.

### P0-2 — fix details

`MemTable`/`TraceMemTable` maintain total bytes with atomic counters (half of P1-5). Stream identifiers are
double-counted when buffers merge, so `merge_snapshot` returns the duplicate amount and subtracts it from
the counter; the counter then matches a full traversal **exactly** (fixed by tests). `Journal` holds
`wal_bytes`/`checkpoint_bytes`, making backlog O(1) — `/metrics` no longer runs `stat` + checkpoint-file
reads on every scrape.

`IngestGate` is the single decision point for both protocols. Blocking only one would move excess traffic
to the other. New knobs are `LOGGYTRACY_MAX_MEMTABLE_BYTES` (256 MiB by default),
`LOGGYTRACY_MAX_WAL_BACKLOG_BYTES` (1 GiB by default), and `LOGGYTRACY_BACKPRESSURE_RETRY_AFTER` (1 s by
default); the first two can be disabled with `off`. `config.validate` rejects a MemTable limit below
`flush_max_bytes`, which would reject data before asking flush to move it.

New metrics: `loggytracy_ingest_throttled_total`, `loggytracy_memtable_buffered_bytes`.

### N1(a) — fix details

`rewrite_group` alternates reading, filtering, and writing within one blocking task. Batching would consume
the memory the split was intended to save. Above the limit, a multi-part group is recursively split in half,
while a single part is traversed in row-group windows with `PartReader::read_rows_in_row_groups`. Window size
is calculated from the part's actual average row width. Regardless of output-part count, all outputs share
the same merge tombstone (`old_dirs`), so the commit remains one transaction. Outputs already written are
removed in place if an intermediate step fails.

`retention_rewrite_skipped` now counts only cases that cannot work even after splitting — effectively a
configuration where a single row group exceeds the budget. Retention-only groups also use `meta.json` to
filter already-reclaimed groups before reading them.

### N5/P1-8 — fix details

Check `version` in `meta.json` **before** checksum validation. Checksums are calculated over the structure,
so they matter only after both sides agree on the structure; reversing the order makes a format change look
like a checksum mismatch — a disk failure rather than a version difference. The manifest already had
`format_version`.

P1-8 is solved by recording `materialized_bytes` (memory actually occupied when read) in part metadata.
Centralizing calculation in `Row::materialized_bytes` prevents group selection and read budgets from diverging,
and removing `fs::metadata` from `estimated_part_bytes` also lightens N4.

### N2/P2-2 — fix details

Tenants outside the allowlist receive **403**. It is not 400 because the request itself is valid and the
client has nothing to fix. If the allowlist is enabled without the default tenant, every headerless request
would create an out-of-list tenant, so `validate` rejects the configuration.

The four metadata endpoints acquire `MetadataGuard`. The retention floor is folded into `start_ns`, expressing
"the range requested by the client" and "the range the tenant is still entitled to see" as one boundary
(`MetadataWindow`). Without `start`, the query goes back only `max_query_range`, not through all history —
the infinite default was what caused every part to be read in the first place. An empty range is an empty response,
not an error.

### P1-4 — fix details

`writer_epoch` is in the manifest rather than a separate object because it costs nothing to check. Every
write already reads the manifest it will replace. At startup, claim the same number for both log and trace
(claiming only one would leave trace writes unfenced), then verify the epoch read by every CAS.

`ObjectStorage` directly reports fencing to `ShutdownState`. Flush, merge, retention, and force-flush respond
identically without each worker needing to understand fencing.

**Self-fencing is a decision made in this work.** Force-flush stops retrying when fenced. Every other
force-flush failure is transient and merits infinite retry, but this one does not — continuing leaves the
process alive until the orchestrator kills it, after which the pod may be scheduled on another node and the
disk containing the only copy of unflushed data is discarded. The loss M6 intended to prevent would occur
through exactly that path. Exit code 1, data remains in the WAL, and the log explicitly states disk preservation.

**Operational implication:** A new instance claims the epoch immediately at startup. Thus M6's procedure of
"fully drain the old instance before starting the new one" is now **enforced**. Violating the order makes the
old instance stop and exit abnormally instead of failing silently.

The rest remains in `todo.md`.

---

## Production-readiness gates (updated)

### Gate 1 — data safety

- [x] Fix P0-1 WAL-compaction wedge + consecutive-compaction/crash-injection tests
- [x] P0-2 ingest backpressure (MemTable/WAL-backlog limit → 429)
- [x] N1(a) split fallback on merge memory overflow (guaranteed tenant deletion)
- [x] N1(b) fail startup when policies are stored but the token is missing
- [x] P1-4 writer fencing (manifest epoch + self-fence)
- [x] Unify units for P1-8 `merge_max_input_bytes` vs `merge_max_memory_bytes`

### Gate 2 — multi-tenancy completion

- [x] N2 tenant allowlist
- [ ] Per-tenant throttles/quotas/`max_streams_per_user`, tenant-labeled metrics
- [x] P2-2 metadata endpoint resource guards + apply `start`/`end`
- [x] N4 remove O(parts) work from `/metrics`
- [ ] Adjust the default bind to the trust boundary

### Gate 3 — operability

- [x] N5 part format version field
- [ ] P1-10 retry transient failures at startup (remove crash loop)
- [ ] P2-7 histograms + endpoint labels
- [ ] P2-8 non-stdin abort path
- [ ] P3 Dockerfile + configuration reference + runbook + alert rules

### Gate 4 — scale validation

- [ ] Measure row-group fragmentation with many tenants for N3
- [x] N6 Tempo time pruning (search and both tag endpoints)

### Gate 5 — feature completeness

- [x] P1-2 OTLP logs — implement/register `LogsService`
- [x] P1-2 OTLP/HTTP (`/v1/logs`, `/v1/traces`), protobuf and JSON
- [ ] P2-1 Loki API gaps
- [ ] P2-5 duplicate observability → deduplication
- [ ] LogQL improvements in P1 of `todo.md`
