# M7 Load Validation Results

This records the first M7 load-validation runs across both object-store tiers,
the machine profile, per-target pass/fail, and the bottleneck the runs
identified. The raw harness output is checked in as `docs/m7_tier_b_result.json`
and `docs/m7_tier_c_result.json`.

## Summary verdict

| Tier | Backend | Verdict | Reason |
| --- | --- | --- | --- |
| B | in-process latency/fault store over `file://` | **FAIL** | flush loop wedged by a WAL-compaction bug (see Bottleneck) |
| C | real S3 protocol over local MinIO | **FAIL** | same WAL-compaction bug, reproduced with **no** fault injection |

Both tiers pass every *numeric* target (ack p95/p99, query p95, RSS, error rate)
but fail the *behavioral* gate because the flush loop stops making progress: the
WAL backlog is unbounded and `part_count` freezes. The load harness surfaced a
real, pre-existing engine bug — exactly the M7 objective.

## Machine profile

- `Darwin arm64` (Apple Silicon), 8 logical CPUs, 16 GiB RAM — a developer
  laptop, **not** the 4-vCPU / 16-GiB reference machine from the M5 target table.
- Absolute throughput/latency numbers here are therefore not a verdict on the
  numeric targets; per the M5 plan, those are validated on target-class hardware.
  The local runs assert error-free operation, bounded RSS relative to the
  configured cap, correct backpressure, and successful eviction→restore and
  retention. The build revision for both runs is `782e7ff`; the deterministic
  fault seed is `20260724`, and the workload seed reported by the harness is
  `1592598566`.

## Tier B — deterministic in-process fault/latency injection

Config: `file://` inner store wrapped by `LatencyFaultStore`
(`LATENCY_MS=5`, `LATENCY_JITTER_MS=10`, `READ_LATENCY_MS=20`,
`ERROR_RATE=0.03`, `FAULT_SEED=20260724`); `CACHE_MAX_BYTES=8 MiB`,
`CACHE_EVICTION_INTERVAL=3s`, `FLUSH_MAX_INTERVAL=2s`, `MERGE_INTERVAL=8s`,
`RETENTION_PERIOD=20s`, `RETENTION_GRACE_PERIOD=5s`. Workload: 45 s, 10 s warmup,
offered 3000 events/s, 100 events/push, ~1 KiB events.

- Ack latency (steady): p50 208.8 ms, p95 212.2 ms, p99 214.4 ms — all under the
  250 ms / 1 s targets. (See caveat below on why ack latency is flat at ~208 ms.)
- Log query p95 10.9 ms; metric query p95 39.5 ms; tempo search p95 3.4 ms.
- RSS peak 4.19 MB — far under the 4 GiB cap.
- Eviction observed (15 evictions), retention observed (9 reclamations),
  restore probes ran with **zero** restore errors.
- Injected write errors were recovered (remote ended healthy, no acknowledged
  ingest loss).
- **Failure:** `flush_success_delta=2` against `flush_errors_delta=39`,
  `wal_backlog_bytes=19.9 MB` (over the 16 MiB liveness cap), `part_count=0`.

## Tier C — real S3 protocol over local MinIO

Config: `docker compose up minio`, bucket `loggytracy` created by the
`minio-init` one-shot, server pointed at `s3://loggytracy/loggytracy` with
`OBJECT_STORE_ENDPOINT=http://127.0.0.1:9000`, `OBJECT_STORE_ALLOW_HTTP=true`,
`OBJECT_STORE_VIRTUAL_HOSTED_STYLE_REQUEST=false`,
`OBJECT_STORE_CONDITIONAL_PUT=etag`, MinIO root credentials. No fault injection.
Workload: 25 s, 5 s warmup, offered 2000 events/s. Driven by
`scripts/run_load_s3.sh`.

- **Manifest CAS over the real S3 protocol works.** With
  `OBJECT_STORE_CONDITIONAL_PUT=etag`, `PutMode::Create` / `PutMode::Update`
  (If-None-Match / If-Match) succeeded: three log parts and one trace part were
  published to MinIO, the manifest `generation` advanced to 4, and there were
  **no** `Precondition`/`AlreadyExists` errors. This is the required Tier C
  confirmation that manifest CAS, remote publish, and retention run against
  `AmazonS3`, not just the in-memory/file backends.
- Ack latency (steady): p50 208.6 ms, p95 211.5 ms, p99 215.8 ms.
- Eviction observed (5), restore probes ran with zero restore errors, RSS peak
  4.11 MB.
- **Failure:** same wedge — `flush_success_delta=2`, `flush_errors_delta=20`,
  `part_count` frozen at 3. The 25 s run ended before the WAL backlog crossed the
  16 MiB cap (10.8 MB), so the wedge is caught by the success-vs-error liveness
  check rather than the backlog cap.

## Identified bottleneck (the blocker)

**WAL compaction wedges the flush loop after the first successful compaction,
losing flush liveness under any sustained object-store-backed run.**

Root cause: `compact_wal` (`src/journal/compaction.rs`) writes a durable
compaction-state file at phase 2 to make the rename/fsync boundary idempotent,
but that file is **never removed after a successful compaction** during live
operation (only `replay.rs` clears it, and only for phase 1, on startup). After
a compaction, the WAL is truncated and the checkpoint resets to 0, so subsequent
checkpoint offsets live in a **new coordinate space**. The next compaction's
offset is smaller than the stale phase-2 offset recorded in the *old* coordinate
space, so `compact_wal` hits the `offset < state.offset` guard and returns
`"WAL compaction checkpoint moved backwards"`. The flush loop then retries the
same doomed checkpoint every second forever: `part_count` freezes, the WAL grows
unbounded, and the memtable never drains.

Evidence:

- Reproduced with the Tier B fault store (39 occurrences) **and** with Tier C
  MinIO and **zero** fault injection (21 occurrences) — it is neither
  backend-specific nor fault-induced.
- A separate no-fault `file://` control run reproduced it 21 times after only 2
  successful flushes.
- It only affects object-store-backed runs, because `advance_checkpoint`
  compacts (`journal.compact_checkpoint`) only when `remote_cache.is_some()`;
  local-only runs use `set_checkpoint` and are unaffected — which is why the
  existing local test suite never caught it.

Per M7 scope discipline, this is an engine correctness fix scoped as its own
follow-up, not folded into M7 silently. Sketch of the fix: clear the
compaction-state file once a compaction is durably complete (and treat a
surviving phase-2 state on startup as already-done and remove it), so the next
compaction starts in a clean coordinate space instead of comparing against a
stale absolute offset. This is durability-critical crash-recovery code and needs
its own focused change plus crash-injection tests.

## Caveats / notes

- **Flat ~208 ms ack latency** is a client artifact, not a server signal: the
  harness uses a single synchronous connection, and the journal batches appends
  up to `max_batch_ms` (200 ms default), so each lone in-flight push waits out
  the batch timer. On target-class hardware with concurrent producers this
  disappears. It does not affect the bottleneck finding.
- **Achieved vs offered rate** (~476 vs 3000 eps) is bounded by the same single
  connection plus the 200 ms batch wait, not by server backpressure.
- Numeric targets are reported for completeness only; the accepted numeric
  verdict must come from a target-class 4-vCPU/16-GiB run after the compaction
  fix lands.

## Reproduction

- Tier B: `scripts/run_load_local.sh` (starts the release server with the fault
  knobs + `file://` remote, runs `target/release/load`, writes
  `docs/m7_tier_b_result.json`).
- Tier C: `scripts/run_load_s3.sh` (brings up MinIO, runs the release server
  against `s3://…`, runs the harness, writes `docs/m7_tier_c_result.json`).
