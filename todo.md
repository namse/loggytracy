# TODO

This tracks work deferred beyond M3's current scope and work for later milestones.

The complete production-readiness gate list is in [`docs/PRODUCTION_READINESS_REVIEW_2026-07-26.md`](docs/PRODUCTION_READINESS_REVIEW_2026-07-26.md)
(previous review: [`docs/PRODUCTION_READINESS_REVIEW.md`](docs/PRODUCTION_READINESS_REVIEW.md)).
The three invariants the work below serves are in [`docs/VISION.md`](docs/VISION.md).

## M8 — the ruler (precondition for everything after it)

No optimization starts before this. Every performance number currently in the repository was produced by a
harness that cannot reach its own offered rate, on data with cardinality 1 and near-zero entropy, with reads
that never contend with writes. Optimizing against those numbers reproduces them.

- [ ] **Retire the numbers, keep the reasoning.** Strike the tables in [`docs/LOAD_RESULTS.md`](docs/LOAD_RESULTS.md)
      and [`docs/M7_LOAD_RESULTS.md`](docs/M7_LOAD_RESULTS.md); keep the refuted hypotheses, the terminal-sample
      lesson, and §8's finding that every "merge disabled" run in the document had one merge in it. Fix the
      dangling references while there: a cited test that does not exist (`LOAD_RESULTS.md:90`), a cited artifact
      that does not exist (`m7_tier_c_result.json`), and a surviving artifact that disagrees with the document
      citing it on both build revision and verdict
- [ ] **`benches/` with criterion** — WAL append, memtable insert, `rows_from_snapshot`, bloom build and lookup,
      Parquet row-group scan, LogQL evaluation. These are the regression gate; there is currently none of any kind
- [x] **Rewrite `src/bin/load.rs`** (now `src/bin/load/`): N keep-alive connections per workload over a
      hand-rolled HTTP/1.1 client on tokio (no new dependency — the server binary keeps object storage as its
      only outbound call); latency taken from the *intended* send, with service time and response time both
      reported and their gap called out; the corpus is `loggytracy::corpus`, promoted out of `benches/` so
      there is one generator; out-of-order and late arrival are workload knobs; queries are an independent
      workload with their own rate and connections; percentiles carry their sample count and refuse to be
      computed below it; RSS is `VmHWM` from `/proc/<server pid>/status` with a sampled `VmRSS` series, and a
      run that cannot read it fails; the WAL gate is a trend, not `trough <= peak * 0.5`
- [x] **Delete `src/bin/m5_load.rs`** — deleted. It measured `std::process::id()`, the harness's own RSS
- [x] **Fix `scripts/run_load_local.sh`** — the repository root is derived from the script's own location, the
      addresses and the result path are defaults rather than assignments, and the harness's non-zero verdict
      exit no longer truncates the server log
- [x] **CI.** [`.github/workflows/ci.yml`](.github/workflows/ci.yml), on every push and every pull request:
      `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` (the tree is at zero warnings and that is
      a maintained property), `cargo test`, `cargo bench --no-run` so `benches/` — a separate crate nothing else
      compiles — cannot rot, a criterion `--test` pass that executes every bench body once, and a twenty-second
      debug run of [`scripts/run_load_local.sh`](scripts/run_load_local.sh). Neither the bench pass nor the load
      run is a measurement: a shared runner's timings are noise, and this repository has already published
      numbers from a machine that was busy doing something else. The load run is gated on the *presence* of a
      verdict, not its value. The toolchain is pinned in [`rust-toolchain.toml`](rust-toolchain.toml) rather than
      in the workflow, so the version that gates a pull request is the one `cargo` picks locally
- [ ] **Bench regression *detection*, not just bench execution.** CI proves the bench bodies run; it compares
      nothing. Criterion baselines live in `target/criterion/` and do not survive a fresh runner, so an honest
      gate needs a stored baseline — publish the estimates as an artifact, restore the previous one, and compare
      against a threshold wide enough to survive shared hardware, or run the comparison on a machine that is not
      shared. Half of this is worse than none: "regression check: passed" computed over noise is a claim nobody
      will re-examine

## M9 — the comparison bed

- [ ] Loki single-binary beside loggytracy under docker-compose: **same corpus, same machine, same container
      memory limit**
- [ ] Four query shapes — label-only, `|=` substring, `| json | field="x"`, `rate()` aggregation — plus ingest
      throughput, bytes on disk per GB ingested, and object-store request counts
- [ ] Publish the table, including when it loses. The claim and its falsification condition are in
      [`docs/VISION.md`](docs/VISION.md)

## M10 — declared memory budget ([`docs/VISION.md`](docs/VISION.md) I)

- [ ] **Honest metering first.** `entries_bytes` (`memtable.rs:69-81`) counts line and label lengths only —
      not the 56-byte `LogEntry`, the 48-byte slot per metadata pair, malloc headers, or `Vec` slack. Measured
      shapes are 1.4–2.8x under, so `MAX_MEMTABLE_BYTES=256 MiB` is really 400–700 MiB. A budget on a dishonest
      meter is worse than no budget, because it will be believed
- [ ] `LOGGYTRACY_MEMORY_BUDGET` divided into ingest/flush/merge/query/sidecar arenas with per-arena accounting
      and per-arena refusal. Existing knobs become overrides; what is not overridable is that they sum
- [ ] **Query admission by budget, not by slot.** Replace `MAX_CONCURRENT_QUERY_SCANS × MAX_QUERY_MEMORY_BYTES`
      (8 × 512 MiB = 4 GiB, admitted in a comment at `config.rs:522`) with a shared arena. Same ceiling, and a
      burst of cheap queries no longer queues behind a slot count
- [ ] **Sidecars inside the budget.** They are outside it on purpose today (`part/reader.rs:77-81`), so resident
      memory grows with part count unbounded. Make them LRU-evictable — they are already durable in `index.bin`
- [ ] **Stop materializing `PartMeta::streams`** (`part/mod.rs:231`, `part/metadata.rs:172-176`) — every distinct
      label set in every open part, held as live `String`s. Stream identity needs a fingerprint, not the text
- [ ] **Bound in-flight push bodies.** The ingest gate is checked once at request entry and nothing limits
      concurrency, so (in-flight requests x 64 MiB) sits outside the accounting
- [ ] **A test that runs at a declared budget and asserts peak RSS stays under it.** Not a sizing paragraph in
      the runbook

## M11 — bounded copies and deep pruning ([`docs/VISION.md`](docs/VISION.md) II, III)

Write path:

- [ ] **`Arc<Labels>` end to end** — memtable, `Row`, part write, reader, query result. `Row::from_entry`
      (`part/mod.rs:302`) currently clones the whole `BTreeMap` per row, `encode_stream_index`
      (`part/indexes.rs:77-82`) clones every name and value again per row per label, and `write_meta`
      (`part/metadata.rs:25-28`) clones the set a third time. Largest single payoff in the repository
- [ ] Remove the two free memcpys: the line clone at `ingest.rs:247` (the source is separately owned and could
      be consumed), and the whole-payload copy in `frame_tenant_record` (`journal/mod.rs:90-101`) whose 7-byte
      prefix belongs in `writer_loop`'s batch buffer
- [ ] One sort — `sort_rows` runs globally (`part/format.rs:22`) and again per partition (`:91`)
- [ ] One parse — `encode_blooms` runs the JSON and logfmt parsers over every line twice, to size the filter
      and then to fill it (`part/format.rs:335-341`, `:360-366`)
- [ ] Move `rows_from_snapshot` and the global sort inside `spawn_blocking` (`flush.rs:233` is outside the one
      at `:253`), so an O(n log n) pass over a full snapshot stops blocking an async worker
- [ ] Cap the exact-field bloom. `exact_capacity` is the raw token count for the row group
      (`part/format.rs:329-347`), so a wide-JSON tenant can make `index.bin` larger than `data.parquet`
- [ ] Consider compressing the WAL payload. It stores the decompressed protobuf, discarding the client's
      snappy, which makes the WAL the dominant term in write amplification

Read path:

- [ ] **Kill `normal_scan_limit = usize::MAX`** (`query/execution.rs:102-106`). Any pipeline stage today means
      the whole window is materialized before the pipeline reduces it — the most common Grafana query shape.
      Replace with a merge of per-part sorted iterators feeding a bounded top-K heap
- [ ] That also deletes the triple materialize-and-sort with a per-row `Labels` clone at each hop
      (`reader.rs:1041`, `part_registry.rs:628`, `execution.rs:202`)
- [ ] **Projection pushdown.** `ProjectionMask` appears nowhere; `count_over_time({app="x"}[5m])` decodes every
      label column and the `structured_metadata` JSON blob
- [ ] **Cache Parquet footer metadata on the reader.** `open_part_data` re-opens the file and re-runs
      `ArrowReaderMetadata::load` per selected row group (`reader.rs:640`)
- [ ] Do not allocate the line before the filter that rejects it (`reader.rs:727` precedes `:728`)
- [ ] **Extract required literals from `|~` regexes** so trigram blooms apply. `bloom_prune` matches only
      `LineFilter::Contains` (`reader.rs:778-787`)
- [ ] Parallelize part scans within a query (`part_registry.rs:579` is sequential), and stop holding a scan
      permit across an object-store restore (`execution.rs:367` vs `:374`)
- [ ] Memtable query: binary-search the sorted stream instead of counting every entry against the scan budget,
      and stop sorting the whole stream on every query (`memtable.rs:145-156`)
- [ ] Verify the trigram bloom's `to_lowercase()` on both sides (`bloom.rs:139-148`, `:57`) cannot produce a
      false negative for non-ASCII substring filters — a dropped result is a correctness bug, not a pruning miss
- [ ] Do not write an `.access` marker file per candidate part per query on the scan thread
      (`part_registry.rs:596-607`)

## Test-suite repairs (fold into M8)

- [ ] **`src/tests/e2e.rs` is not e2e.** `ingest_once` (`:69`) hand-decodes the push and calls `journal.append`
      directly instead of going through `ingest::push`, and `:102-110` hand-executes what `flush.rs` does, in
      the test's own order. A bug in `flush_once`'s ordering is invisible to it
- [ ] Replace wall-clock assertions with virtual time. `tokio/test-util` was added for exactly this
      (`Cargo.toml:33-37`) and `start_paused` appears three times, all in `startup.rs`. The flush loop, merge
      scheduler, retention loop and eviction cadence are still untested under it
- [ ] Use the injectable `Clock` (`src/clock.rs`) in fixtures instead of `SystemTime::now()`, so window-boundary
      behaviour stops depending on when the suite runs
- [ ] Make the measurement tests pin their numbers. `part/tests.rs:907` computes the fragmentation cost,
      prints it, and asserts only that many > one — a 100x regression passes, and `LOAD_RESULTS.md` cites it as
      the fixed location for five specific figures
- [ ] Assert something in the parser list tests (`logql/tests.rs:240,285,537,727` check only `is_ok()`) and the
      readiness tests (`query/tests.rs:397,406,419`)
- [ ] Add a concurrent read/write test. There is none
- [ ] `#[cfg(test)]` the `pub fn for_test` constructors shipping in production code (`backpressure.rs:117`,
      `tenant_quota.rs:119`, `tenant_policy.rs:806,811`, `object_storage/fault_store.rs:90`), and test the
      wiring `startup.rs` actually uses rather than a parallel assembly

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
  - [ ] Add Parquet range reads (P2) and use `(part, tenant)` local cache keys. **Sidecar consolidation is
        done**: the trigram blooms and the stream index are one `index.bin`, so a part is three files rather
        than four — one fewer billed PUT per flush, one fewer round trip per catalog restore, and one fewer
        checksum pass per part at startup, which §8 measured as the actual startup cost
  - [x] **Per-tenant ingest rate** — `ingest_rate` rides the same push as retention. The control plane sets the
        number; this side only owns the field and enforcement points. Check before decompression so an over-limit
        tenant cannot consume CPU.
  - [x] Per-tenant query-scan quota and concurrency — `query_rate` rides the same pushed
        policy as `ingest_rate`, charged after a scan with what it actually read
  - [x] Per-tenant stream cardinality limit — `max_streams` on the pushed policy, enforced
        against the union of what the tenant holds in parts and in the buffers
  - [x] Per-tenant usage — `GET .../tenants/{tenant}/usage` on the admin API. **Not** labels on
        `/metrics`: that scrape is unauthenticated and process-wide by design, and a label per
        tenant is the cardinality problem this engine bounds everywhere else
  - [ ] Durable monthly usage accounting — **this belongs to the control plane, not the instance.** A month spans
        instances and outlives them. This side only exports per-tenant usage for the control plane to account for.
- [x] Document TLS unsupported as an architecture decision
- [x] Ingest input limits (body/decompressed length/line/label count and length/timestamp acceptance window)

## P1 — LogQL improvements

- [x] Support `line_format`, `label_format` — a deliberate subset of Go templates
      (literal text and `{{.field}}`), refusing what it cannot render rather than approximating
- [x] Support `unwrap` (bare field and `duration(field)`) plus `sum_over_time`,
      `avg_over_time`, `min_over_time`, `max_over_time` and `quantile_over_time`
- [x] Support binary operators with a scalar operand (`+ - * / %`, `== != > >= < <=`).
      Vector-to-vector is refused: both sides would need their own scan, which is a
      planner change rather than a parser one
- [x] Support `without`
- [x] Support `offset`
- [x] Support subqueries
- [x] Loki-compatible JSON semantics for arrays, top-level arrays and `null`
- [x] Exact-field pruning on stream-label fields, through the stream index
- [ ] Exact-field pruning for empty-string equality and `_extracted` collisions —
      both stay conservative on purpose: an empty equality also matches an absent
      field, and absence is not indexed anywhere

## P2 — correctness and storage performance

- [x] Deduplicate duplicate logs that can result from crash replay — every part is written through one
      sort, which now drops entries identical in tenant, stream, timestamp, line and metadata. A flush
      cannot see a twin that is already in an older part, so the removal lands the first time the two
      are merged; `loggytracy_wal_replayed_entries` still reports the upper bound a restart introduced
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
- [ ] Tune resource limits such as query memory, range, and concurrency to operational targets. **The
      arithmetic exists now**: `peak_materialized_bytes` is computed, logged at startup and documented in
      [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md) — 5 GiB at the defaults, of which 4 is
      `MAX_CONCURRENT_QUERY_SCANS × MAX_QUERY_MEMORY_BYTES`. What is still missing is a *measurement* at
      that concurrency: every load run so far has been ingest-dominated, so the largest term in the budget
      has never been exercised. Choosing different numbers before then would be guessing with more steps
- [x] **Tier D duration/scale run** — 2.01 hours, 500 tenants, a graceful shutdown, a restart and a fence,
      every behavioural gate met. It found one defect nothing shorter could: shutdown waited out merge
      groups it started *after* the signal, 117 of the 118 seconds it took. The 10,000-part axis is
      measured separately in §8, because reaching it means effectively disabling merge, which is a
      different question from steady-state stability. Results: [`docs/LOAD_RESULTS.md`](docs/LOAD_RESULTS.md) §10
- [x] **CAS preflight** — verify at startup that conditional writes are enforced and refuse startup otherwise.
      Running against the deployment target itself resolves what local validation could not answer.
- [x] **Measure object-store operation counts** — `loggytracy_object_store_operations_total` counts every
      request by kind, and a test pins publication at four PUTs per part plus one GET and one PUT for the
      manifest, independent of manifest size. Measured numbers and what they say about the bill (the flush
      interval sets the Class A cost, and the current default does not fit the $1 budget) are in
      [`docs/LOAD_RESULTS.md`](docs/LOAD_RESULTS.md) §9. Retirement costs four DELETEs per part (one per
      immutable file, unbatched) plus two LISTs per orphan sweep
- [x] Document load-test results and bottlenecks — [`docs/LOAD_RESULTS.md`](docs/LOAD_RESULTS.md).
      Keep reproducible facts in tests and quote only numbers in the document.
- [x] **Mitigate N3** — measured, and the mitigation already existed. The 24.7x is a function of
      rows-per-tenant-per-part, not of tenant count: merge cuts (tenant, part) pairs 3.6x and
      parts-per-tenant from 6.6 to 1.85, amortising the ratio to ~1.07x. At 10,099 parts the resident
      sidecar cost is 18.7 MB, not the 407 MB first extrapolated, because parts that fragmented are also
      small. The binding memory constraint at this scale is the merge budget, which is hundreds of
      megabytes against the sidecars' single digits — the opposite of the working assumption. Numbers in
      [`docs/LOAD_RESULTS.md`](docs/LOAD_RESULTS.md) §2, §6 and §8, with the two refuted hypotheses kept.
- [x] Improve the load probe to verify rows read — the probe counts the lines returned, so "restored and
      read" is distinguishable from "nothing matched"

## P2 — Loki API surface

- [x] `patterns` — a read-time miner over a bounded sample of the window, reporting the lines it
      looked at. No index is added to the write path
- [x] `delete` API — hides on acceptance at the single scan every read path funnels through, removes
      the bytes at the next rewrite. Design and the reasons for each refusal:
      [`docs/RETENTION_DESIGN.md`](docs/RETENTION_DESIGN.md)

## Deferred with a reason

- [ ] **P1-11: manifest as generational deltas plus periodic snapshots.** The manifest is still rewritten in
      full on every publish, so one object takes every commit. What that costs is now measured
      ([`docs/LOAD_RESULTS.md`](docs/LOAD_RESULTS.md) §9): one PUT and one GET per flush, on one key,
      whose rate is the flush interval. **Not the startup bottleneck** — §8 measured startup at 10,099
      parts and found the cost is local checksum validation, not manifest I/O, so the O(N) half of P1-11
      is already answered.
      What remains is the hot key, and the fix that actually removes it is not "deltas" but making each
      commit a `PutMode::Create` at its own generation key, so no key takes two writes. That replaces the
      commit protocol every durability guarantee here rests on — lost-update protection, merge input
      revalidation, writer fencing, the cross-domain flush transaction — and it cannot be validated by
      anything short of another soak. It is listed in
      [`docs/LOAD_VALIDATION.md`](docs/LOAD_VALIDATION.md) as the *response to* observed throttling on the
      first real deployment, not as a precondition for it, and that is the right order: the mitigation
      should be built when there is a measurement saying it is needed, against the backend that produced
      the measurement.

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
