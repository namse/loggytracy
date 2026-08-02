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

**The second step bought headroom rather than another whole step down.** The
streaming top-K executor took the 2 GiB pass from 90–96% of the budget to
**78–83%** and the workload's own anonymous high-water from 1913 to 1659 MiB, at a
slightly higher delivered rate again — and 1792 MiB is still `OOM_KILLED`. Two
items of II's list, and the second one moved the margin and not the floor;
[`MEMORY_BUDGET_GATE.md`](MEMORY_BUDGET_GATE.md) says why the two need not move
together.

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

**Three things that did not remove, and one of them is now gone too.** The three
materialize-and-sort hops on the read path **are one materialization of `limit`
rows and one sort over them**, which is invariant III's streaming executor below
and the step that closed it. What is still there:
`process_entry_with_labels_cancellable` clones the label set into a mutable field
map **per row**, for every query including one with no pipeline stages, and
undoing that is a copy-on-write field view across the whole pipeline rather than a
type change — the bound reduced how many times it is called, not what a call
costs. And `Row::materialized_bytes` and `estimated_log_entry_memory_bytes` still
charge every row for label bytes it now shares, so merge sizing and the query
memory ceiling are conservative by the rows-per-stream factor — a meter to fix
with the rest of the metering, not a limit to loosen on the way past.

Also in this invariant: **one decode, one sort, one parse.** `sort_rows` runs
globally and then again per partition (`part/format.rs:22`, `:91`), and
`encode_blooms` runs the JSON and logfmt parsers over every line twice — once to
size the filter, once to fill it (`format.rs:335-341`, `:360-366`).

### III. Query cost is bounded before the scan starts, and pruning goes as deep as the format allows

**The worst violation was the most common query, and it is fixed.** When a query
had any stage beyond a line filter — `| json`, `| logfmt`, a field filter — the
scan limit was set to `usize::MAX`, because the limit could not be applied before
the pipeline ran. So `{app="x"} | json | status="500"` with `limit=100`
materialized every matching row in the window, up to the 512 MiB ceiling, and then
threw almost all of it away. That was the shape Grafana sends most, and the shape
this engine claims to be good at.

Execution now **streams**. `PartReader`, `PartRegistry` and `MemTable` offer rows
one at a time to a `RowSink` (`part/sink.rs`), and the executor's sink
(`log_scan.rs`) is a bounded top-K collector that runs the pipeline as each row
arrives. So a pipeline stage no longer defeats the limit: the scan reads until it
holds `limit` rows that *survived* the pipeline. From the sink's frontier — the
timestamp a row must beat once the sink is full — the scan skips a whole part on
its tenant segment's span without opening it, a whole row group on
`row_group_min_ts`/`max_ts` without touching the Parquet body, and the rest of a
part on the first row past the frontier, which is sound because a tenant's rows
are ordered inside a part. Ties go to whichever row arrived first, so the answer
is exactly what a stable sort plus `truncate(limit)` gave. That also deleted the
triple-materialize of invariant II: there is nothing left to materialize but the
`limit` rows, and one sort over those.

**Measured on `benches/query.rs`, limit 100 over 202 000 rows in the window.**
`| json | field=` went from processing every row in the window — 200 250 lines,
703 MB of allocation traffic, 26.8 MB of peak live, 308.78 ms — to **3 130 lines,
43.9 MB, 5.81 MB and 13.067 ms**, and label-only from 69 178 lines and 178 MB to
**187 lines and 0.37 MB**. On the gate the margin at 2 GiB widened from 6% to
17–22% and the workload's own anonymous high-water fell from 1913 to 1659 MiB, but
the smallest surviving budget is still 2 GiB
([`MEMORY_BUDGET_GATE.md`](MEMORY_BUDGET_GATE.md)). Two figures the old text
quoted from [`MEMORY_ATTRIBUTION.md`](MEMORY_ATTRIBUTION.md) — 111 MiB of live
materialization against 17 MiB for the two shapes, and 270 MB per label-only query
— were build `50190cf`'s and are not comparable to these; the attribution has not
been re-run and its tables still describe an engine that no longer exists.

**And a limit only helps where a limit binds.** On the comparison bed's own
dataset the `json_field` figure at `limit=20 000` is unchanged at 16 384 lines,
because 20 000 over ~1 250 matching rows never fills the sink. At Grafana's
default 100 it is 5 528–11 072, against Loki's 1 251 for the same answer. The
residue is not the limit: Loki's index takes it to one stream's chunks, while a
row group here interleaves every stream in it and each of their rows is decoded
and filtered. That is the pruning below, and it is now the largest remaining term
on the shape the claim rests on.

**Pruning we index for and then do not use:**

- **No projection pushdown.** `ProjectionMask` appears nowhere in the tree. Every
  label column and the `structured_metadata` JSON blob is decoded even for
  `count_over_time({app="x"}[5m])`, which needs one column.
- **No predicate pushdown, no Parquet statistics, no Parquet blooms.** Filtering
  is a row loop (`reader.rs`, `scan_batch`) that allocates the line with
  `to_string()` before testing the filter that will reject it.
- **`|~` never prunes.** `bloom_prune` matches only `LineFilter::Contains`, so
  `|~ "error.*timeout"` gets no trigram pruning even though both literals are
  indexable. Extracting required literals from the regex closes this.
- **Sequential parts.** Within one query, parts are scanned one at a time, and an
  object-store restore holds a scan permit while doing pure network I/O
  (`execution.rs`).
- ~~**Footer re-parsed per row group.**~~ Closed by the streaming rewrite: the
  footer is parsed once per part scan, and the reader clones an `Arc` of it per
  row group and per backward window. Caching it across *scans* is still open.

What is already right and must not regress: row groups aligned to tenant
boundaries (`format.rs:221-238`), which is what makes every row-group-granular
structure selective in a shared part; the exact-field bloom with canonical
numeric forms, a `label_format` barrier, and BTF4's zero-length filter used as a
*positive* prune (`ast.rs:154-189`, `part/mod.rs:52-60`); and the single read
funnel that `volume`, `patterns`, `detected_fields` and `tail` are all expressed
in terms of rather than given private scans.

---

## Ingest is OTLP

**Logs and traces arrive over OTLP and nothing else.** The Loki push endpoint is
removed. The Loki *query* API is not — it is how Grafana reads this engine, and
an ingest protocol and a query protocol are separate decisions that happened to
share a name.

**Why.** loggytracy has one intended consumer, and everything it would store
already arrives as OTLP there: guest traces, guest logs (stdout, converted to
OTLP log records by the host), and the worker's own telemetry. Two things in that
deployment are not OTLP and neither is loggytracy's business — node metrics go
by Prometheus remote_write, and this engine does not do metrics at all; and
systemd journal currently goes by Loki push, which the collector can convert,
because `otelcol.receiver.loki` exists precisely for that.

**What it buys is larger than one endpoint.** An OTLP record is re-encoded into
a Loki `PushRequest` before it reaches the WAL, so that replay has a single
decoder (`proto.rs`). That is a whole second message materialized with a clone
per line and per label, then serialized, then framed, then batched — five copies
for the WAL alone, on the exact path the consumer uses. The re-encode exists
only because two ingest protocols had to converge on one WAL record. With one
protocol it has nothing to do, and [invariant II](#ii-a-lines-bytes-are-copied-a-bounded-number-of-times-and-label-sets-are-never-de-shared)
gets the copies back.

**What stays.** The `| json` parser stays and is not deprecated. A guest's
`println!` becomes the *body* of an OTLP log record as a plain string, and
nothing in that chain parses it — so a guest that logs JSON still needs a parser
stage at query time. What changes is that this stops being the headline; see the
claim below.

**A consequence worth knowing before it surprises someone.** Which OTLP
attributes become stream labels is a schema decision, and loggytracy currently
uses *Loki's own default promotion list* (`otlp_log.rs`,
`PROMOTED_RESOURCE_ATTRIBUTES`), so a collector configured for Loki produces the
same streams here. The corollary is that a collector which moves a source to
OTLP without mapping its labels to OTel semantic conventions loses index pruning
in **both** systems identically: `{unit="fn0-worker"}` stops being a label
lookup and becomes a structured-metadata filter. The fix is a `transform`
mapping on the collector — `unit` to `service.name` and so on — and it is one
piece of work that serves both.

---

## The claim, and what would falsify it

The differentiator is invariant III applied to one query shape — and **which
shape is the right one changed when ingest became OTLP.**

The claim used to be about `| json | field="value"`: a log line written as JSON
text, parsed at query time. That is a real shape and it is still supported, but
it is not the one the consumer sends. Over OTLP the fields that matter arrive as
**attributes** — `project_id`, `unit`, `trace_id`, `span_id`, severity — and
loggytracy puts them in `structured_metadata`, which the exact-field bloom
already indexes. The query is `| trace_id="x"`, with **no parser stage at all**.

So the claim is now about that shape, and it is a better claim, because it is
where the three systems genuinely differ:

* **loggytracy** indexes structured metadata into a per-row-group bloom.
* **Loki** stores structured metadata and **does not index it** — the filter is a
  scan.
* **VictoriaLogs** turns OTLP attributes into columns.

`| json | field="value"` remains measured, as `json_field` and
`json_field_rare` in [`COMPARISON.md`](COMPARISON.md), because a guest that
prints JSON produces exactly it. It is no longer what the engine is *for*.

The claim is therefore stated as a falsifiable one:

> At an equal container memory limit, on the same corpus and the same machine,
> loggytracy answers `{...} | field="value"` over structured metadata — the shape
> an OTLP attribute produces — in materially less time than Loki, which does not
> index it, and not materially worse than VictoriaLogs, which columnizes it,
> without giving up ingest throughput or disk footprint.

**Both halves are measured now, on the wire the claim was always about.** The
bed ingests OTLP on all three systems since 2026-08-02 — the same protobuf
body at each engine's own `/v1/logs` spelling — and every ratio below printed
only after all three pairs agreed on all 168 queries of every shape. The Loki
half **holds** — `metadata_rare` at **0.03x**, 2.4 ms against 78.8 — and the
VictoriaLogs half **does not yet** — **1.49x cold / 1.54x warm**, 2.4 ms
against 1.6. It began at 12.6x when `structured_metadata` was a JSON blob
parsed per row; columnizing it, then the page-level time selection and the
1024-row pages, brought it to 1.5x, and what remains is millisecond-scale
constant work against a purpose-built column store. An earlier short run had
loggytracy *losing* the Loki half at 3.10x; that number was this engine's own
bounded-scan defect wearing a performance costume, and it reversed the day the
defect did. The honest wire also resolved the `json_field_rare` pair the
design intended: the same rare value reached *through a parser* is now
**0.04x** against VictoriaLogs — under OTLP it keeps the line unparsed like
everyone else, pays the unpack at query time, and the per-row-group bloom over
the parsed field is the difference.

It is abandoned if the comparison shows Loki within noise on that shape despite
not indexing it, or shows loggytracy losing on ingest or bytes-per-GB by enough
that the query win does not pay for it. Publishing the comparison means
publishing it when it loses — which it has already done once, on the shape this
claim replaced.

**What the previous claim's measurements said, kept because moving a target does
not retract them.** Two comparison runs, both in
[`COMPARISON.md`](COMPARISON.md) and its artifacts. The first published at 8 GiB
because loggytracy was OOM-killed at 2 and Loki was not: `| json | field=` 1.49x
slower cold, `|=` 1.69x, `sum(rate())` 7.1x, winning only label-only at 0.36x.
The second published at 2 GiB, which both systems survived — that being the
change — and read 1.85x on `| json | field=`, 2.81x on `|=`, 4.50x on
`sum(rate())`, 0.57x on label-only, with a per-limit table showing the bounded
scan working: 282 676 lines read at a limit of 100 against 1 331 072 at 20 000.

Since that second run the rows have been ordered by stream before time, which a
short local run measured at 0.08x on label-only, 0.15x on `|=` and **0.47x on
`json_field`** — a shape that was losing. **That has not been confirmed in the
bed**, and one shape got worse in the same change (`json_field` backward, see
[`todo.md`](../todo.md)), so it is a signal and not a result.

None of that is retracted by this section. What it establishes is that
invariant III was unbuilt and is now half built, and that the number to beat is
written down for the shape that is no longer the headline.
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
- **No format versioning.** Nothing on disk or on the wire carries a version, and
  nothing reads data written by an older build — not parts, trace parts, the
  manifest, the WAL, the compaction state, or the bloom container. There is no
  deployment, so there is never old data, and every compatibility path was code
  that could not run. It is not free to keep: the bloom container carried four
  formats, which made the reader hold three flags describing what each writer had
  been capable of and made `exact_field_bloom` an `Option` that was always
  `Some`. Changing a format is therefore just changing it; a stale local data
  directory is deleted rather than migrated.

  Two things that resemble versioning and are not: `object_store`'s
  `UpdateVersion`, which is the compare-and-swap ETag every durability guarantee
  here rests on, and `CARGO_PKG_VERSION` in `/metrics` and the Loki `buildinfo`
  endpoint, which is API surface.

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
