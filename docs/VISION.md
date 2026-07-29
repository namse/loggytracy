# loggytracy vision

**loggytracy answers, inside a memory budget you declared, on one machine, the
queries Loki has to brute-force.**

[`ARCHITECTURE.md`](ARCHITECTURE.md) records what this engine *is*. This document
records what it is *for*, which of its properties are load-bearing, and what
would prove the claim wrong. Where the two disagree, this one is the intent and
the other is the implementation.

The feature surface is done. Logs and traces ingest over Loki push and OTLP, the
Loki and Tempo HTTP APIs answer Grafana, LogQL covers the high-usage subset,
tenants are isolated and their retention is enforced, and the durability
protocol survives crashes, restarts and a split writer. What remains is not
features. It is that this engine does not yet keep the promise its shape implies.

---

## Three invariants

Everything below is a target. The current state violates all three, and each
section says where.

### I. Memory is a budget you declare, not a number that emerges

An operator gives one number:

```
LOGGYTRACY_MEMORY_BUDGET=1GiB
```

That number is the same number they put in the container's memory limit, and the
engine's job is to stay under it. The budget is divided into **arenas**, each
with its own accounted allocation and its own refusal when full. The shares
below are the ones [`MEMORY_ATTRIBUTION.md`](MEMORY_ATTRIBUTION.md) measured;
the earlier table here was a guess and every one of its numbers moved:

| Arena | Share | Measured high-water at 2 GiB | Holds | On overflow |
|---|---|---|---|---|
| ingest | 20% | 378 MiB | memtable, trace memtable, in-flight push bodies | `429` + `Retry-After` (already the mechanism) |
| flush | 25% | **721 MiB** | materialized rows, Parquet writer buffers | defer the flush; ingest backs up into its own arena and refuses there |
| merge | 25% | **771 MiB** | one merge group | split the group; skip the tick |
| query | 25% | 242 MiB + 203 MiB untagged | every concurrent scan, pipeline stage and metric evaluation | queue, then `429` |
| sidecar | 5% | 17 MiB | blooms, stream index, part metadata | evict least-recently-used sidecars; reload from `index.bin` |

Shares are defaults, individually overridable. What is not overridable is that
they sum to the budget.

**Two of the five do not fit their share as the engine is written**, and that is
the finding rather than a sizing problem: flush materializes a whole memtable
snapshot at 3.3× its accounted size, and one merge group reached 771 MiB against
a `merge_max_memory_bytes` default of 1 GiB — half the container. Their shares
are targets the code must be made to meet (invariant II's `Arc<Labels>` and a
chunked flush; a group split sized from the budget), not descriptions of it.

**Two consequences that are architectural, not tuning.**

*The query arena replaces the concurrency knob.* Today the query bound is
`MAX_CONCURRENT_QUERY_SCANS × MAX_QUERY_MEMORY_BYTES` — 8 × 512 MiB = 4 GiB
(`config.rs:522` says so in a comment). A product of two knobs that never appear
next to each other is not a budget. Admission becomes "is there room in the query
arena", not "is there a free slot", so a burst of cheap queries runs wide and a
burst of expensive ones queues. Same ceiling, better degradation, one number.

*Sidecars move inside the budget.* They are currently outside it on purpose
(`part/reader.rs:77-81`), which means resident memory grows with part count and
nothing stops it. `PartMeta::streams` is worse than the blooms: it materializes
every distinct label set in every open part (`part/mod.rs:231`,
`part/metadata.rs:172-176`), so its cost is stream cardinality × part count in
live `String`s. Sidecars are already durable on local disk, so making them
evictable costs a re-read of `index.bin`, and stream identity does not need the
label text — a fingerprint is enough. Measured: **~240 kB of sidecar and ~140 kB
of `PartMeta` per part**, which is 0.2–1.2% of the anonymous peak at the part
counts the comparison bed reaches. The concern is right and the 10% this
document used to give it was wrong by an order of magnitude.

**Accounting must be honest before any of this means anything.** `entries_bytes`
(`memtable.rs:69-81`) counts `line.len()` plus label name and value lengths and
nothing else — not the 56-byte `LogEntry`, not the 48-byte slot per metadata
pair, not malloc headers, not `Vec` slack. Measured shapes come out **1.4× to
2.8× under** in isolation and **1.70–1.79× under** in situ on the comparison
corpus, so `MAX_MEMTABLE_BYTES=256 MiB` is really ~440 MiB. A budget computed
from a dishonest meter is a worse guarantee than no budget, because it will be
believed.

**And a live-byte meter is not enough on its own.** At the moment the kernel
killed the process at 2 GiB, the engine's live heap was **669 MiB** — every
arena inside its share, the budget reporting a third full — while 44% of the
anonymous footprint was memory the process had already freed and glibc had not
returned. The engine asked for 52 GB across 444 million allocations in 33
seconds; at that rate the allocator's steady state is a heap the size of the
transient peak. So the budget has a precondition: the process must make its own
anonymous footprint track its live bytes (measured: `MALLOC_ARENA_MAX=1` with
trim thresholds takes `anon / live` from 2.5–4.1 to **1.34** and more than
doubles time-to-OOM), and the budget must be verified against the cgroup's
`anon` rather than against the sum of its own arenas.
[`MEMORY_ATTRIBUTION.md`](MEMORY_ATTRIBUTION.md) is the measurement.

**How this is verified:** a test that runs the engine at a declared budget under
sustained mixed load and asserts peak RSS stays under it. Not a sizing paragraph
in the runbook. Today the runbook says "size on peak, not idle — roughly fifty
times" ([`RUNBOOK.md`](RUNBOOK.md):27), which is the honest description of an
engine that does not have this invariant.

### II. A line's bytes are copied a bounded number of times, and label sets are never de-shared

Target: **three copies** from socket to disk — the request body, the WAL write
buffer, the Arrow column buffer. Today it is six before the `204` and twelve
before the bytes land in Parquet.

Two of the six are free:

- `push_inner` iterates `&push_req.streams` and clones each line
  (`ingest.rs:247`) while `push_req` is separately owned and could be consumed.
- `frame_tenant_record` copies the entire payload to prepend a 7-byte prefix
  (`journal/mod.rs:90-101`), which `writer_loop` then copies again into the batch
  buffer (`writer.rs:304-316`). The prefix belongs in the batch buffer.

The JSON and OTLP paths are worse: they re-encode a Loki `PushRequest` for the
WAL so replay has one decoder (`proto.rs:99-127`). The single-decoder property is
right and worth keeping; materializing a whole second message with per-line and
per-label clones to get it is not.

**Label sets are the bigger term.** The memtable holds one `Labels` per stream —
correct, and its byte accounting reflects it (`memtable.rs:57-67`). Then
`Row::from_entry` clones the whole `BTreeMap` **per row** (`part/mod.rs:302`),
`encode_stream_index` clones every name and value again per row per label
(`part/indexes.rs:77-82`), and `write_meta` clones the set a third time
(`part/metadata.rs:25-28`). A ten-label stream with ten thousand entries turns
200 bytes of labels into roughly 150 MB of `Vec<Row>`, and nothing gates it.

The read path is the same mistake mirrored: reader → registry → execution
materializes, clones per row, and sorts, **three times**
(`reader.rs:1041`, `part_registry.rs:628`, `execution.rs:202`) — and a **fourth**
on the metric path, which
[`MEMORY_ATTRIBUTION.md`](MEMORY_ATTRIBUTION.md) found by measuring: after the
scan returns, `evaluate_metric_query` walks every row on an async worker thread
and clones `stream.labels` per row again (`query/metrics.rs:134-155`), outside
the `spawn_blocking` at `:158` and outside every budget. It is 203 MiB at its
high-water, and it is the whole reason `rate()` is 7.1× slower than Loki.

Measured, so the payoff has a number: `rows_from_snapshot` allocates **1 503
bytes and 17 allocations per row** and holds **1 345 bytes per row live** at five
labels, identically at 1, 256 and 8192 streams — the clone is per row, not per
stream. In the running engine that is a `Vec<Row>` at **3.3× the accounted
memtable it was built from**, and the flush path's whole cost is **26 kB and 356
allocations per 368-byte line**.

`Arc<Labels>` end to end — memtable, `Row`, part write, reader, query result —
removes all of it. This is one type change with the largest single payoff in the
repository.

Also in this invariant: **one decode, one sort, one parse.** `sort_rows` runs
globally and then again per partition (`part/format.rs:22`, `:91`), and
`encode_blooms` runs the JSON and logfmt parsers over every line twice — once to
size the filter, once to fill it (`format.rs:335-341`, `:360-366`).

### III. Query cost is bounded before the scan starts, and pruning goes as deep as the format allows

**The worst violation is the most common query.** When a query has any stage
beyond a line filter — `| json`, `| logfmt`, a field filter — the scan limit is
set to `usize::MAX` (`query/execution.rs:102-106`), because the limit cannot be
applied before the pipeline runs. So `{app="x"} | json | status="500"` with
`limit=100` materializes every matching row in the window, up to the 512 MiB
ceiling, and then throws almost all of it away. That is the shape Grafana sends
most, and it is the shape this engine claims to be good at.

The fix is not a smaller ceiling. It is that execution **streams**: a merge of
per-part sorted iterators feeding a bounded top-K heap, materializing exactly
`limit` rows plus the heap. That also deletes the triple-materialize of
invariant II, because there is nothing left to materialize.

**Measured, and it is not the whole of the query cost.** Running the identical
workload with only `| json | field=` queries against only label-only queries,
`usize::MAX` costs **6.5× the live materialization** (111 MiB against 17 MiB) and
3× the allocations. But the label-only shape — the one where the limit *does*
apply — still allocates **270 MB and 1.19 million allocations per query to return
a hundred rows**, and the query path is still 57% of all allocation traffic in
that run. Bounding the scan removes the query arena's residency and about half
its churn; the rest is the per-row work that happens before any limit can apply,
which is the triple materialize and `reader.rs:727` allocating the line before
the filter that rejects it. See
[`MEMORY_ATTRIBUTION.md`](MEMORY_ATTRIBUTION.md).

**Pruning we index for and then do not use:**

- **No projection pushdown.** `ProjectionMask` appears nowhere in the tree. Every
  label column and the `structured_metadata` JSON blob is decoded even for
  `count_over_time({app="x"}[5m])`, which needs one column.
- **No predicate pushdown, no Parquet statistics, no Parquet blooms.** Filtering
  is a row loop (`reader.rs:697-752`) that allocates the line with `to_string()`
  *before* testing the filter that will reject it (`:727`).
- **Footer re-parsed per row group.** `open_part_data` re-opens the file, re-runs
  `ArrowReaderMetadata::load` and rebuilds the schema for every selected row
  group (`reader.rs:640`). Two hundred row groups is two hundred footer parses.
- **`|~` never prunes.** `bloom_prune` matches only `LineFilter::Contains`
  (`reader.rs:778-787`), so `|~ "error.*timeout"` gets no trigram pruning even
  though both literals are indexable. Extracting required literals from the
  regex closes this.
- **Sequential parts.** Within one query, parts are scanned one at a time
  (`part_registry.rs:579`), and an object-store restore holds a scan permit while
  doing pure network I/O (`execution.rs:367` vs `:374`).

What is already right and must not regress: row groups aligned to tenant
boundaries (`format.rs:221-238`), which is what makes every row-group-granular
structure selective in a shared part; the exact-field bloom with canonical
numeric forms, a `label_format` barrier, and BTF4's zero-length filter used as a
*positive* prune (`ast.rs:154-189`, `part/mod.rs:52-60`); and the single read
funnel that `volume`, `patterns`, `detected_fields` and `tail` are all expressed
in terms of rather than given private scans.

---

## The claim, and what would falsify it

The differentiator is invariant III applied to one query shape.

`| json | field="value"` is a full scan in Loki: Loki indexes labels, not
structured fields, so the parser stage runs over every line in the window.
loggytracy columnizes and blooms those fields at ingest, so the same query prunes
at row-group granularity. ~~**This is built and has never been measured against
anything.**~~ **It has now been measured, and it lost.**

The claim is therefore stated as a falsifiable one:

> At an equal container memory limit, on the same corpus and the same machine,
> loggytracy answers `{...} | json | field="value"` over a window Loki must scan
> in materially less time, without giving up ingest throughput or disk footprint.

It is abandoned if the comparison in [`COMPARISON.md`](COMPARISON.md) shows Loki
within noise on that shape, or shows loggytracy losing on ingest or
bytes-per-GB by enough that the query win does not pay for it. Publishing the
comparison means publishing it when it loses.

**The M9 result, and this section is subordinate to it — "if a measurement
contradicts this document, the measurement wins".** On the same corpus, the same
machine and the same container limit, loggytracy is **1.49x slower cold and
1.44x slower warm** on `| json | field=`; it is 1.69x slower on `|=` and 7.1x
slower on `sum(rate())`; it wins only the label-only shape, at 0.36x. It
achieves 16.8 k eps against Loki's 19.9 k and holds 323 MiB per GB of settled
data against 267. And the limits were not equal by choice: at 2 GiB loggytracy
was OOM-killed and Loki was not, so the published run is at 8 GiB.

The claim is not abandoned, because none of that is a property of the *format* —
it is invariant III unbuilt. `normal_scan_limit` is still `usize::MAX` for
exactly this shape, there is still no projection pushdown, and the bloom the
claim rests on is still behind a full materialize. What the measurement removes
is the option of asserting the claim before that work is done. The number to
beat is now written down.

---

## What is deliberately not built

- **No cluster, no replication, no query frontend, no separate index store.**
  Single writer is the design, and writer fencing enforces it. Every guarantee
  here — the manifest CAS commit, merge input revalidation, the flush
  transaction — rests on there being exactly one.
- **No DataFusion.** Recorded as "under consideration" in
  [`ARCHITECTURE.md`](ARCHITECTURE.md); closed here as rejected. It conflicts with
  invariant I — its memory is not accountable at arena granularity — and with
  invariant III, where the bounded top-K stream is a specific execution shape
  rather than a general one. The LogQL surface is small enough that a planner for
  it is smaller than the integration.
- **No TLS, no authentication.** Already decided; unchanged.
- **No per-tenant object paths.** Already decided on cost grounds; unchanged.

---

## The ruler comes before the work

Every performance number in this repository was produced by a harness that
cannot be trusted, and the documents holding those numbers say so themselves in
places. Optimizing against them would reproduce them.

What is wrong, concretely:

- The harness is **single-connection, one request in flight, `Connection: close`
  per request** (`bin/load.rs:715`, `:732`). Every latency includes a TCP
  handshake; keep-alive is never exercised.
- **Coordinated omission is uncorrected.** The pacer keeps a nominal schedule
  (`bin/load.rs:184-191`) but the stopwatch starts at the *actual* send (`:195`),
  so when the server slows the harness stops issuing and never records the delay
  it owes. `offered_eps: 3000` against `achieved_eps: 478` in the one checked-in
  artifact is the offered rate being unreachable by construction.
- **Cardinality 1** — one hardcoded label set (`bin/load.rs:693`) — and lines
  padded with `"x".repeat(...)` (`:680`), i.e. near-zero entropy. The documents
  record that realistic lines compress 5.9× where this data compressed 31.5×
  ([`LOAD_RESULTS.md`](LOAD_RESULTS.md) §2) and the harness was not changed.
- **Reads never contend with writes** — queries fire inline in the push loop on
  the same thread (`bin/load.rs:221`).
- Percentiles over samples of 167 and 8; a "p95" over eight samples is the
  maximum.
- Not reproducible: the only run script `cd`s to a path that does not exist on
  this machine, one cited artifact is missing, the surviving one disagrees with
  the document citing it on both build revision and verdict, and a cited test
  name does not exist in the tree.
- **No benchmarks at all.** No criterion, no `benches/`, no CI. There is no
  performance regression detection of any kind.

So the first work is the ruler, and it is a precondition for the rest:

1. **Retire the numbers, keep the reasoning.** `LOAD_RESULTS.md` and
   `M7_LOAD_RESULTS.md` contain genuinely valuable narrative — refuted
   hypotheses, the terminal-sample lesson, the discovery that "merge disabled"
   runs all contained one merge. That survives. The tables do not.
2. **Microbenchmarks for the hot paths**: WAL append, memtable insert, flush row
   materialization, bloom construction and lookup, Parquet scan, LogQL
   evaluation. These are the regression gate.
3. **Rewrite the harness**: N connections with keep-alive, latency measured from
   *intended* send time, a realistic corpus with a cardinality knob and
   out-of-order arrival, reads concurrent with writes, RSS from `VmHWM`.
4. **The comparison bed**: Loki single-binary beside loggytracy, same corpus,
   same machine, same container memory limit. Four query shapes — label-only,
   `|=` substring, `| json | field=`, `rate()` aggregation — plus ingest
   throughput, bytes on disk per GB ingested, and object-store request counts.
5. **CI**, because none of this holds without it.

Only then: the budget architecture (I), the copy elimination (II), the streaming
executor and pushdown (III).

---

## How to read a disagreement

If a measurement contradicts this document, the measurement wins and this
document is edited. If an implementation contradicts it, the implementation is
wrong. If a feature request contradicts "what is deliberately not built", the
answer is no, and the reason is that these three invariants are the product.
