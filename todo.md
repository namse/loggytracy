# TODO

This tracks work deferred beyond M3's current scope and work for later milestones.

The complete production-readiness gate list is in [`docs/PRODUCTION_READINESS_REVIEW_2026-07-26.md`](docs/PRODUCTION_READINESS_REVIEW_2026-07-26.md)
(previous review: [`docs/PRODUCTION_READINESS_REVIEW.md`](docs/PRODUCTION_READINESS_REVIEW.md)).

## P0 — production gates

- [x] **Fix WAL compaction wedge** (same item as the P5 BLOCKER below). Remove the intent record durably
      immediately after success, and treat remaining phase-2 records as complete and remove them — already
      wedged instances recover through an upgrade, so no manual deletion procedure is needed.
- [x] **Ingest backpressure**: return `429` before journal append when MemTable/WAL backlog limits are exceeded
      (+`Retry-After`, OTLP uses `RESOURCE_EXHAUSTED`). Track MemTable/WAL backlog size in O(1).
      Knobs: `LOGGYTRACY_MAX_MEMTABLE_BYTES`, `LOGGYTRACY_MAX_WAL_BACKLOG_BYTES`,
      `LOGGYTRACY_BACKPRESSURE_RETRY_AFTER`.
- [x] **Guarantee tenant deletion**: split groups in half when merge exceeds the memory limit, and rewrite a
      single part in row-group windows. Large parts cannot retain zero-retention rows forever.
- [x] **Block startup without the policy token**: fail startup when a tenant policy is stored but the token is
      missing, so hidden deleted data does not reappear.
- [x] **Writer fencing**: claim the manifest's `writer_epoch` at startup and verify it on every CAS.
      On fencing, return ingest 503, `/ready` 503, stop force-flush retries, and exit with code 1.
      **M6's "fully drain the old instance before starting the new one" procedure is now enforced.**
- [x] **Tenant allowlist**: `LOGGYTRACY_ALLOWED_TENANTS`. Tenants outside the list receive 403.
      Startup fails if the default tenant is not in the list.
- [x] **On-disk format version**: Check `version` in part/trace-part `meta.json` before checksum validation.
- [x] **Metadata endpoint guards**: Add semaphore, timeout, `start`/`end`, and `match[]` count limits to
      `labels`/`label_values`/`series`/`index_stats`.
- [x] **Remove O(parts) from `/metrics`**: Workers publish merge-debt and unknown-tenant gauges.
- [ ] **Multi-tenancy** (in progress). The design, cost model, and implementation checklist are in
      [`docs/MULTI_TENANCY_DESIGN.md`](docs/MULTI_TENANCY_DESIGN.md).
      **The previous design using tenants as a storage-path axis (`docs/ARCHITECTURE.md`, "Multi-tenancy")
      was discarded because of R2 Class A costs** — writing objects per tenant does not fit the $1 plan
      budget at any RPO.
  - [x] Extract and validate `X-Scope-OrgID` (Loki push + OTLP gRPC), and configure the missing-header policy
  - [x] Record the tenant in WAL records (owner survives restart; existing WAL recovers under the default tenant)
  - [x] Shared tenant parts: `(tenant, ts)` sort + row groups aligned to tenant boundaries + tenant index in
        `meta.json` (for both logs and traces)
  - [x] Isolation surface: require tenant arguments for MemTable, PartRegistry, TraceRegistry, queries, and catalog reads
  - [x] Per-tenant retention deletion path — determine expiration from the tenant index, whole-delete + merge rewrite,
        and clamp every read path. Design and rationale: [`docs/RETENTION_DESIGN.md`](docs/RETENTION_DESIGN.md)
  - [x] Replace policy polling with **push**. `PUT` one tenant at a time, store it in object storage, then ack;
        load all at startup (failure is fatal). Tenant deletion = retention `0` (ignores rewrite threshold).
        Removed polling and the direct `reqwest` dependency — object storage is the only outbound call.
        Details: [`docs/RETENTION_DESIGN.md`](docs/RETENTION_DESIGN.md)
  - [x] ~~`(tier, day)` partitioning~~ — rejected. Fixing retention at write time does not apply plan changes
        to existing data. Partitions remain by `day`.
  - [ ] Consolidate four part sidecars into one, add Parquet range reads (P2), and use `(part, tenant)` local cache keys
  - [x] **Per-tenant ingest rate** — `ingest_rate` rides the same push as retention. The control plane sets the
        number; this side only owns the field and enforcement points. Check before decompression so an over-limit
        tenant cannot consume CPU.
  - [x] Per-tenant query-scan quota and concurrency — `query_rate` rides the same pushed
        policy as `ingest_rate`, charged after a scan with what it actually read
  - [x] Per-tenant stream cardinality limit — `max_streams` on the pushed policy, enforced
        against the union of what the tenant holds in parts and in the buffers
  - [ ] Tenant-labeled metrics
  - [ ] Durable monthly usage accounting — **this belongs to the control plane, not the instance.** A month spans
        instances and outlives them. This side only exports per-tenant usage for the control plane to account for.
- [x] Document TLS unsupported as an architecture decision
- [x] Ingest input limits (body/decompressed length/line/label count and length/timestamp acceptance window)

## P1 — LogQL improvements

- [ ] Support `line_format`, `label_format`
- [ ] Support `unwrap` and `quantile_over_time`
- [ ] Support binary/vector operators
- [ ] Support `without`, offset, and subqueries
- [ ] Support Loki-compatible semantics for JSON top-level arrays and `null` values
- [ ] Improve exact-field pruning for empty-string equality, stream-label fields, and `_extracted` name collisions

## P2 — correctness and storage performance

- [ ] Deduplicate duplicate logs that can result from crash replay
- [ ] Add Parquet range reads (**multi-tenancy prerequisite** — shared parts must read only a tenant's byte range,
      so this is no longer an optional optimization)
- [ ] Improve metric evaluation from bounded in-memory computation to streaming/pre-aggregation
- [x] ~~Validate a deployment environment using real S3~~ — **confirmed out of scope.** This is an indie project,
      so local MinIO is the upper bound for load validation. What is validated, what risks remain, and what to
      check on the first real deployment are in [`docs/LOAD_VALIDATION.md`](docs/LOAD_VALIDATION.md)

## P3 — M5 operational validation

- [ ] Tune compaction
- [x] Fix the unit mismatch between `merge_max_input_bytes` and `merge_max_memory_bytes`. Record
      `materialized_bytes` in part metadata so group selection and read budgets use the same unit, and have
      `validate` enforce `merge_max_input_bytes <= merge_max_memory_bytes`.
- [x] Implement retention policies and expired-data deletion (including a separate retention timeout knob)
- [ ] Tune resource limits such as query memory, range, and concurrency to operational targets
- [ ] **Tier D duration/scale run** — over 2 hours, over 10,000 parts, over 500 tenants, and one restart during
      the run. Existing Tier B/C runs last tens of seconds and cannot exercise P1-11 (O(N) paths) or N3
      (row-group fragmentation). Acceptance criteria are in [`docs/LOAD_VALIDATION.md`](docs/LOAD_VALIDATION.md)
- [x] **CAS preflight** — verify at startup that conditional writes are enforced and refuse startup otherwise.
      Running against the deployment target itself resolves what local validation could not answer.
- [ ] **Measure object-store operation counts** — PUT/GET/LIST per flush/merge/retention cycle. Amounts cannot
      be measured locally, but counts are backend-independent and measurable; because this design has been
      dominated by R2 Class A costs, these counts are the cost estimate.
- [x] Document load-test results and bottlenecks — [`docs/LOAD_RESULTS.md`](docs/LOAD_RESULTS.md).
      Keep reproducible facts in tests and quote only numbers in the document.
- [ ] **Mitigate N3**: With 500 tenants, the same 5,000 rows become 24.7x (28 KB → 691 KB). Row groups stop
      at tenant boundaries, so tenant count is a lower bound for row-group count and Parquet column metadata
      and bloom filters scale with it. The target workload has many small tenants, so this directly affects design.
- [ ] Improve the load probe to verify rows read — it currently cannot distinguish "restored and read" from
      "nothing matched."

## P4 — M6 hardware replacement

Detailed plan: [`docs/M6_IMPLEMENTATION_PLAN.md`](docs/M6_IMPLEMENTATION_PLAN.md)

- [x] Implement graceful-shutdown handler (SIGTERM/SIGINT starts the drain sequence, which owns process termination)
- [x] Block ingest: Loki push 503 and OTLP UNAVAILABLE while draining (before journal append)
- [x] in-flight drain: axum `with_graceful_shutdown` + tonic `serve_with_shutdown`
- [x] Final force-flush after background workers (flush/merge/retention/eviction) shut down normally
- [x] Implement force-flush: ignore thresholds, drain MemTable/pending checkpoint, and wait for S3 upload/manifest update completion
- [x] Infinite retry + stdout warning on persistent object-store failure; exit only from operator stdin input (no hard timeout)
- [x] Automatic lossless recovery through journal replay after forced termination and restart
- [x] Drain-status readiness: `/ready` 503 while draining + pending bytes/flush completion exposed in `/metrics`
- [x] Hardware replacement rehearsal (new instance resumes traffic without loss)
- [x] Fresh-context review (remaining gates) — `docs/PRODUCTION_READINESS_REVIEW_2026-07-26.md`

## P5 — M7 local S3 load validation

Detailed plan: [`docs/M7_IMPLEMENTATION_PLAN.md`](docs/M7_IMPLEMENTATION_PLAN.md)

- [x] Strengthen observability gauges (add merge-debt gauge; active part count, WAL backlog, and MemTable bytes already exist in `/metrics`)
- [x] Tier B: wrap `LatencyFaultStore` + `from_url` opt-in (in-process latency/fault injection, reproducible seed)
- [x] ~~Tier C: MinIO~~ — **removed.** Do not test against S3 because the `object_store` crate is trusted
- [x] ~~Verify MinIO manifest CAS~~ — replaced by startup preflight, which checks the deployment target store itself
- [x] Improve load harness: target-rate pacing, separate warmup/steady state, forced eviction→restore, pass/fail against targets (`src/bin/load.rs`)
- [x] Document results, machine profile, and bottlenecks in `docs/M7_LOAD_RESULTS.md`
- [x] **BLOCKER — fix the infinite WAL compaction wedge (complete):** After the first compaction, the phase-2
  compaction-state file was not removed. After the coordinate system reset, the compaction offset was compared
  with a stale offset and the flush loop permanently wedged with `"WAL compaction checkpoint moved backwards"`.
  It reproduced without fault injection in both Tier B (`file://`) and Tier C (MinIO). It occurred only on the
  object-store backend path (local-only mode uses `set_checkpoint`). Before this fix, the M7 acceptance run
  "lossless recovery + bounded backlog" could not pass.
