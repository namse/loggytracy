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

**They are not independent, and the order was wrong when this document was
written.** It listed them as three parallel goals. [`MEMORY_ATTRIBUTION.md`](MEMORY_ATTRIBUTION.md)
established that **II is a precondition for I**: at the moment the kernel killed
the process at a 2 GiB limit, 44% of its anonymous footprint was memory it had
already freed, and its live heap was 669 MiB. An arena budget denominated in live
bytes would have reported a third full while the process was dying. You cannot
budget memory the allocator will not give back, and the way to make it give it
back is to stop asking for it — the engine requested 52 GB across 444 million
allocations in 33 seconds, 217× the rate data arrived at.

So invariant I's arena machinery is the *last* step, not the first. Ahead of it,
in order: ~~the verification test~~ — **built** ([`MEMORY_BUDGET_GATE.md`](MEMORY_BUDGET_GATE.md));
then II, because that is where the churn is; then whatever the allocator still
retains, measured and published as a multiplier; and only then the arenas, sized
from what remains.

**The order was right, and the first step of II is what proved it.** The gate was
red at 2 GiB and green at 5, with a 2.24× overshoot when given room. Sharing label
sets rather than copying them per row — nothing else on either list — made it
green at 2 GiB and took the overshoot to 0.93×, at a higher delivered rate than
before. That is one item of II's list moving invariant I's number by 2.4×, which
is the argument for doing II before I rather than a claim about it.

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
| flush | 25% | **721 MiB** (build `50190cf`; not re-measured since the label sets were shared) | materialized rows, Parquet writer buffers | defer the flush; ingest backs up into its own arena and refuses there |
| merge | 25% | **771 MiB** | one merge group | split the group; skip the tick |
| query | 25% | 242 MiB + 203 MiB untagged | every concurrent scan, pipeline stage and metric evaluation | queue, then `429` |
| sidecar | 5% | 17 MiB | blooms, stream index, part metadata | evict least-recently-used sidecars; reload from `index.bin` |

Shares are defaults, individually overridable. What is not overridable is that
they sum to the budget.

**Two of the five did not fit their share when this was measured**, and that is
the finding rather than a sizing problem: flush materialized a whole memtable
snapshot at 3.3× its accounted size, and one merge group reached 771 MiB against
a `merge_max_memory_bytes` default of 1 GiB — half the container. Their shares
are targets the code must be made to meet (invariant II's `Arc<Labels>` and a
chunked flush; a group split sized from the budget), not descriptions of it.

**`Arc<Labels>` has landed and the table has not been re-measured, so every
figure in it is build `50190cf`'s.** The flush arena's 721 MiB was a *copy* of the
memtable — that copy is now a refcount — and the bench that agreed with it to
1.4% has moved from 1 345 to 823 bytes per row and from 26–28 MB of peak live to
13.85 MB. The right response is to re-run
[`MEMORY_ATTRIBUTION.md`](MEMORY_ATTRIBUTION.md) rather than to scale these
numbers by the bench's factor, because the whole point of that document is that
the in-situ composition was not what anyone predicted.

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

**That test is built, it is the first of these steps rather than the last, and it
was red.** `src/bin/memory_gate.rs` runs the comparison bed's workload — ingest
with reads concurrent with writes — in a cgroup v2 scope at a declared budget and
compares the peak of the cgroup's `anon` against it, because the engine's own
accounting is the thing being audited and cannot be the auditor. It distinguishes
four outcomes by exit code, and *could not be measured* is one of them and is a
failure. Measured on build `50190cf`: at the 2 GiB the comparison bed used,
**OOM-killed at t≈49 s**; the smallest declared budget it survived its own load at
was **5 GiB**, and given 8 GiB of room while asked to stay inside 2 GiB its
anonymous peak was 4586 MiB — **2.24× the budget it was given.**

**Invariant II's first step closed most of that gap.** On build `9199e07`, with
label sets shared instead of copied per row and nothing else on either list built,
the gate is `UNDER_BUDGET` at **2 GiB** at 90–96% of it across three runs, red at
1792 MiB, and the same `--limit 8GiB` experiment reads **1913 MiB — 0.93×** rather
than 2.24×, at 19.7 k eps against 18.7 k. What is left is 6% of headroom at 2 GiB,
which is not a budget anyone should deploy on, and the rest of the invariant is
what buys it. [`MEMORY_BUDGET_GATE.md`](MEMORY_BUDGET_GATE.md) is both baselines
and their limitations.

### II. A line's bytes are copied a bounded number of times, and label sets are never de-shared

**This is the load-bearing one.** It was written as the middle of three and
[`MEMORY_ATTRIBUTION.md`](MEMORY_ATTRIBUTION.md) moved it to the front: the
churn it describes is what makes invariant I unachievable, because an allocator
asked for 52 GB in 33 seconds keeps most of it. 72% of that traffic is the read
path returning a hundred rows, and 20% is a flush path spending 26 kB and 356
allocations on a 368-byte line. The measurement also confirmed the label clone
in situ at 1,326 bytes per row against the microbenchmark's 1,345 — the two agree
to 1.4%, which is why this invariant can be worked on with a bench rather than a
container.

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

**Label sets were the bigger term, and this part is done.** The memtable holds one
`Labels` per stream — correct, and its byte accounting reflects it
(`memtable.rs:57-67`). Then `Row::from_entry` cloned the whole `BTreeMap` **per
row**, `encode_stream_index` cloned every name and value again per row per label
because `BTreeMap::entry` wants an owned key, and `write_meta` cloned the set a
third time. The read path mirrored it: reader → registry → execution materializes,
cloned per row, and sorts, **three times** — and a **fourth** on the metric path,
which [`MEMORY_ATTRIBUTION.md`](MEMORY_ATTRIBUTION.md) found by measuring, on an
async worker thread outside the `spawn_blocking` and outside every budget, 203 MiB
at its high-water. A **fifth** turned up while removing them: `sample_value` built
the whole field set per row to read the one field an `unwrap` names.

`Labels` is now reached through `SharedLabels = Arc<Labels>` from the memtable to
the query result, so a stream's label set is allocated once and every row points
at it. The two writer sites that needed owned keys are keyed by borrows instead,
and the reader interns one label set per distinct stream per scan. **The claim
that this was the largest single payoff in the repository held, and it is the one
number that says so:** the gate went from `OOM_KILLED` at 2 GiB to `UNDER_BUDGET`
at 2 GiB, and the measured overshoot from 2.24× to 0.93×
([`MEMORY_BUDGET_GATE.md`](MEMORY_BUDGET_GATE.md)).

On the benches, where the number that had to move was bytes per row:
`rows_from_snapshot` allocated **1 457 / 1 505 / 1 569 bytes and 11 / 17 / 27
allocations per row** at 2 / 5 / 10 labels, identically at 1, 256 and 8192 streams
— the clone was per row, not per stream — and now allocates **823.4 bytes and 6.00
allocations per row at every point of that sweep**, with peak live flat at
13.85 MB instead of 26.0–28.3 MB. The part scan went from **3796 / 3955 / 4078
bytes and 11.2 / 19.3 / 27.4 allocations per row** to **3167 / 3263 / 3337 and
6.19 / 6.32 / 6.45**, with peak live flat at 31.0 MB instead of 54.0–58.4 MB.
Nothing in either bench got slower, including the two-label case where an atomic
increment replaces a small bulk allocation and a regression was the expectation:
`rows/from_entry` is 1.82× faster at two labels and 4.40× at ten.

**Three things this did not remove.** `process_entry_with_labels_cancellable`
still clones the label set into a mutable field map **per row**, for every query
including one with no pipeline stages, and undoing that is a copy-on-write field
view across the whole pipeline rather than a type change. The three
materialize-and-sort hops on the read path are still three, because only the clone
at each hop went away. And `Row::materialized_bytes` and
`estimated_log_entry_memory_bytes` still charge every row for label bytes it now
shares, so merge sizing and the query memory ceiling are conservative by the
rows-per-stream factor — a meter to fix with the rest of the metering, not a limit
to loosen on the way past.

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

Those figures are build `50190cf`'s, and every one of them included a `Labels`
clone per row at each of the three hops and a fourth on the metric path, which no
longer happen. They are an upper bound on what this shape costs now; nothing here
re-measured them, and the ratio between the two shapes is the part of the finding
that does not depend on it.

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
