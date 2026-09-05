# What collecty's memory is supposed to be, and what it is

collecty's resident memory is supposed to be a function of the ceilings its
operator declares and of nothing else. [`CONFIGURATION.md`](CONFIGURATION.md)
says so in one sentence — "resident memory is roughly this ceiling plus one
batch buffer plus the runtime. It is not affected by how far behind signy is —
that backlog lives on disk" — and until 2026-09-04 nothing had measured it.

**It is not true.** Over thirteen hours at 20 k eps a single collecty process
went from 40 MiB to 192 MiB and was still climbing, its disk queue empty the
whole time. This document declares what the footprint should be, records what
it is, and is the place every experiment that follows writes its result.

The order of the two halves is deliberate. The budget below was written before
the instrument that could attribute it existed, so it is a prediction the
measurements can falsify rather than a description fitted to them.

---

## 1. The declared budget

At the shipped defaults — 64 MiB in flight, 8 MiB segments, a 1 GiB queue — and
under a workload whose exports are far smaller than the 16 MiB request ceiling:

| term | where it comes from | bytes |
|---|---|---|
| process floor | measured: a release build, idle, having taken nothing | **4.6 MiB** |
| three open segments' compressors | measured: `ZSTD_CCtx` at level 3, in use, 3,663,385 B, plus the writer's `ZSTD_CStreamOutSize` output buffer, 131,591 B, per signal | **10.9 MiB** |
| exports admitted and not yet appended | `COLLECTY_MAX_INFLIGHT_BYTES`, the declared ceiling | ≤ 64 MiB |
| the copy of each of those the framing makes | `wire::frame_record` allocates a second buffer per export to prepend four bytes | ≤ 64 MiB |
| the segment being shipped | `Queue::read_segment`'s buffer, ≤ `COLLECTY_QUEUE_SEGMENT_BYTES`, and the `Bytes` it becomes | ≤ 16 MiB |
| per-connection read buffers | hyper's, ≤ ~400 KiB each | connections × 0.4 MiB |
| the queue's own bookkeeping | one 24-byte `Held` per segment, at most `queue_max / segment` of them | ~3 KiB |

Two numbers come out of that, and they are what this document gates on.

* **Steady state ≤ 64 MiB.** A workload that keeps only a small fraction of the
  in-flight ceiling occupied should sit near floor + compressors + one segment
  in flight, which is 35–45 MiB. 64 MiB is that with headroom.
* **Peak ≤ 128 MiB**, which is what the declared ceilings permit once the
  in-flight gate is actually holding the line it names.

And three invariants, which matter more than either number, because each one is
a thing the trace below shows collecty currently is a function of:

1. **Not a function of elapsed time.** A process at hour ten holds what a
   process at hour one holds.
2. **Not a function of connection count.** `COLLECTY_MAX_INFLIGHT_BYTES` is the
   ceiling on bytes being admitted, whatever number of clients offer them.
3. **Not a function of how far behind signy has been.** A backlog is disk. A
   backlog that has drained leaves nothing behind in memory.

---

## 2. What this does not establish

Stated first, in the discipline signy's
[`MEMORY_ATTRIBUTION.md`](../../signy/docs/MEMORY_ATTRIBUTION.md) sets.

* **Nothing here attributes anything.** collecty has no arena instrument yet, so
  every number below is a total — the cgroup's, or the kernel's view of the
  process. Which of the six suspects the growth belongs to is not decided by
  this document; the instrument is being built to decide it.
* **It is one workload on one machine.** 20 k eps of the load harness's log
  corpus over 8 ingest connections, on Linux 6.12.57, 12 logical CPUs, 31 GiB
  RAM, glibc 2.41, release build `2aa5ed6`, no allocator chosen and no
  `MALLOC_*` tuning. Another export size distribution is another experiment.
* **The run was stopped at 13 h of 24.** It was a signy soak, and collecty was
  a passenger in it; it was stopped on purpose to free the machine for the
  experiments this document sets up. The trend at the moment it stopped had not
  flattened, and a run that flattens at hour eighteen would not contradict
  anything below — it would answer a question this run leaves open.
* **The two outage steps are one observation each.** They are consistent with
  each other and with a mechanism, which is why they are recorded, but two
  events are not a curve.
* **Sampling is one reading per five seconds**, from the cgroup's `memory.stat`.
  A spike shorter than that is missed.

---

## 3. What is actually observed

`signy/target/soak/day-1`, 2026-09-04, 20 k eps through collecty into signy,
collecty in a 256 MiB cgroup at the shipped defaults. Its artifacts are kept in
`signy/target/soak/day-1-13h-partial/`.

### It does not converge

The collector was restarted by the rig's own fault injection at t = 8643 s, so
the ten hours after that are one process:

| hour | mean anon (MiB) | peak (MiB) |
|---|---|---|
| 0 | 102.5 | 124.4 |
| 1 | 134.2 | 143.5 |
| 2 | 115.0 | 146.2 |
| 3 | 136.2 | 145.7 |
| 4 | 150.0 | 156.3 |
| 5 | 158.3 | 161.3 |
| 6 | 165.1 | 167.4 |
| 7 | 169.6 | 172.1 |
| 8 | 173.0 | 175.1 |
| 9 | 179.2 | 188.1 |
| 10 | 188.7 | 190.4 |
| 11 | 191.0 | 191.9 |
| 12 | 192.0 | 192.1 |

A fresh process is at **40.2 MiB** twenty seconds after it starts and at
**98 MiB** fifteen minutes later. Ten hours later it is at **192.1 MiB**, and
the last four hours still add about 1 MiB each. The disk queue held 2.2 MiB and
3.7 segments throughout: **the growth is not the backlog**, and on that narrow
point `CONFIGURATION.md` is right.

**This supersedes the conclusion recorded on 2026-09-04 in `signy/todo.md`** —
"collecty converges, it does not leak", from a half-hour rehearsal that saw
111 MiB and a growth rate falling to 1.4 MiB/min. The rate does fall. It does
not reach zero, and half an hour was not long enough to tell the two apart. The
earlier observation is not retracted: at t = 30 min this run was also near
111 MiB.

### A drained backlog leaves memory behind

The rig took signy away twice. Both times collecty's anon was **flat for the
whole outage** and rose **while the queue drained**, and both times it stayed
up:

| outage | backlog built | anon before | anon after | permanent step |
|---|---|---|---|---|
| 50 s (t = 21600) | 92 MiB on disk | 161.3 MiB | 163.4 MiB | **+2.1 MiB** |
| 180 s (t = 34560) | 377 MiB on disk | 175.3 MiB | 187.6 MiB | **+12.3 MiB** |

Roughly 2–3 % of what passed through the drain stayed resident after the queue
was empty again. This is invariant 3 failing, and it is the sharpest causal
evidence in the trace: nothing about the *ingest* side changed across those
windows — the offered rate was constant — so what the drain did differently is
the send path shipping segments back to back as fast as signy would take them.

That is a hypothesis, not a finding. `Queue::read_segment` allocates a fresh
multi-megabyte buffer per segment, and a drain runs that loop at whatever rate
the network allows instead of the ~1/s a steady state runs it at; whether the
bytes that stayed are that buffer, its `Bytes`, the compressor churn beside it,
or the allocator declining to return any of them is exactly what the arena
instrument is for.

---

## 4. The experiments

Each is a 300 s run at 204 exports/s of a 41,656 byte mean over 8 connections,
plus 5 trace exports a second and a scrape every ten seconds, in a 256 MiB
cage, followed by a 60 s settle. `scripts/run_mem_local.sh` is the one command;
every row is that command with one thing changed. Peak anon is the cgroup's,
which is what an OOM kill is decided on; the arena columns are the collector's
own view and are never the verdict.

The rig is calibrated against what the 24-hour soak's collector actually
received, read out of its own queue reports: 205 exports/s of a 41,785 byte
mean. The one thing that does not match is the compression ratio — 3.63x here
against the soak's 4.87x — so a segment here is larger than production's, which
errs towards more memory pressure rather than less.

### Two baselines, and which is which

Every number below belongs to one of two builds of the same code, and they are
named here because otherwise the series has two baselines and no way to tell
them apart:

| | build | what it is for | at the start | at the end |
|---|---|---|---|---|
| `baseline` | `--features memprof` | attribution: the arena, `mallinfo2` and thread columns only exist here | 69.5 MiB | — |
| `baseline-shipped` | default features | the number that ships; carries no instrument | 60.7 MiB | **18.5 MiB** (`final-shipped`) |

Same load, same cage, same 300 s. **The instrument costs about 9 MiB — 14 %** —
so an instrumented row is never comparable with a shipped one, and the series
below is instrumented throughout so that its rows are comparable with each
other.

Those are also the directory names each run leaves under `target/mem/`.

### The instrumented series

Each row is the previous row's code plus one change, all on glibc unless the
row says otherwise, and all on the `baseline` (instrumented) build.

| # | change | peak anon | peak tagged live | libc retained | untagged in use | threads | allocated |
|---|---|---|---|---|---|---|---|
| 2 | baseline | **69.5 MiB** | 3.7 | 53.7 | 4.0 | 26 | 7.5 GB |
| 3 | `MALLOC_ARENA_MAX=1` *(diagnosis only)* | **16.2** | 3.9 | 6.1 | 7.5 | 24 | 7.5 GB |
| 4 | one spool thread, not the blocking pool | **26.1** | 3.8 | 16.6 | 7.4 | 16 | 7.4 GB |
| 5 | mimalloc *(rejected)* | **54.1** | 3.8 | 1.9 | 7.9 | 16 | 7.4 GB |
| 5b | mimalloc, `MIMALLOC_PURGE_DELAY=0` *(rejected)* | **35.7** | — | 5.4 | — | 16 | — |
| 6 | the framing copy removed | **21.8** | 1.4 | 18.0 | 7.4 | 16 | 4.9 GB |
| 7 | the compressor reused across segments | **18.8** | 3.8 | 9.4 | **0.4** | 16 | 4.9 GB |
| 8 | the segment read sized from the file | **18.8** | 5.3 | 10.8 | 0.5 | 16 | 4.9 GB |

Every run accepted 33,424 eps with no failures, so none of these numbers was
bought by refusing load.

**The shipped build goes 60.7 → 18.5 MiB, a 70 % reduction**, at 33,424 eps
accepted with no failures either side. That the instrumented build could be run
at all for every row of this series — signy's could not, and the day it most
wanted attribution it had to give it up — is what making the tagging
independent of the allocator bought.

### What each row established

* **The footprint was never the collector's data.** At the baseline, peak live
  bytes are 3.7 MiB inside a 69.5 MiB anonymous footprint — **5 %**. signy's
  equivalent measurement found 39.7 %. Whatever collecty is holding, it is not
  telemetry in flight.
* **It was threads times arenas.** `MALLOC_ARENA_MAX=1` takes the same run to
  16.2 MiB with identical allocation volume, identical live bytes and identical
  throughput.

  **That row is the smallest number in this document and it is deliberately not
  what collecty ships. Three reasons, because someone will otherwise read the
  table and ask why 16.2 lost to 18.5.**

  1. **They are not the same experiment.** 16.2 MiB is the *old* dispatch with
     one arena; 18.5 MiB is the *new* dispatch with the allocator left alone.
     "Capping arenas beats fixing the dispatch" is not what those two rows say,
     and the run that would say it — `MALLOC_ARENA_MAX=1` on top of the spool
     thread — has not been made. It is one six-minute run and it is worth
     making.
  2. **It is an environment variable, so it is a property of a deployment
     rather than of the collector.** A footprint that depends on an operator
     knowing to set `MALLOC_ARENA_MAX` is one that is wrong everywhere it is
     forgotten, and it is wrong silently. The structural fix is true in every
     deployment including the ones that never read this document.
  3. **It has a measured cost elsewhere.** signy tried the same setting and it
     re-inflicted the backlog that had made it non-default there: the WAL
     backlog climbed 2.9 → 50.7 MiB while the default's stayed near 1.4. One
     arena serializes allocation across threads, which is the whole point of it.
     collecty now does nearly all of its allocating on one thread, so that cost
     may be small here — but "may be" is not a measurement, which is reason 1
     again.
* **The structural fix recovers most of it.** `Queue::append` takes the queue's
  lock as its first act, so dispatching each append to the blocking pool bought
  no parallelism and cost a thread — and therefore an arena — per concurrent
  export. One spool thread: 69.5 → 26.1 MiB, threads 26 → 16.
* **mimalloc is the wrong allocator here, which is the opposite of signy's
  answer and the result most likely to be "corrected" by someone harmonizing
  the two.** It doubled the footprint at its defaults and was still worse than
  glibc with an eager purge. signy chose it for a workload holding hundreds of
  live megabytes across hundreds of millions of allocations a run; collecty
  holds one or two megabytes across a million, and an allocator that keeps
  arenas warm for that has nothing to keep them warm for.
* **A third of everything allocated was one needless copy.** `frame_record`
  built a second buffer per export whose only difference was four bytes in
  front, and both were alive at once: 7.4 → 4.9 GB allocated, peak live
  3.8 → 1.4 MiB.
* **The compressor was the C-side term, and moving it moved the C-side
  column.** `libc_in_use` minus the tagged total is where an allocation that
  does not go through the Rust allocator shows up. Reusing one context per
  signal instead of building one per segment took that column from 7.4 MiB to
  0.4 MiB. This is the row that most needed a column of its own to be
  believable, and it is why the instrument reports one.
* **Sizing the segment read measured nothing at steady state**, and is kept for
  a reason the steady state cannot show. See the drain below.

### The drain, which is its own experiment

The sink is taken away for 60 s at t=120 and comes back; the backlog goes to
disk and then drains. Both runs are the shipped build, and both built the same
backlog, so this compares like with like:

| | before every fix | after them |
|---|---|---|
| backlog at its peak | 199 MB | 198 MB |
| anon before the outage | 59.4 MiB | 15.9 MiB |
| anon after the drain finished | 105.8 MiB | 23.7 MiB |
| **permanent step** | **+46.4 MiB** | **+7.8 MiB** |
| peak anon | 105.8 MiB | 23.7 MiB |

**The step is 83 % smaller and it is not gone.** A drained backlog still leaves
about 4 % of what passed through it resident, against 23 % before. The
24-hour soak measured 2–3 % in production, on a collector whose signy had gone
away completely rather than refusing quickly; the two are not directly
comparable and the like-for-like pair above is the one to read.

### The connection sweep, which is its own experiment

Fixed byte rate, only the connection count varies, 120 s runs on the shipped
build:

| connections | peak anon | in-flight bytes the gate saw | peak live (instrumented) |
|---|---|---|---|
| 8 | 17.9 MiB | 0.08 MiB | 1.4 MiB |
| 64 | 40.3 MiB | — | — |
| 256 | 72.4 MiB | 9.69 MiB | **34.2 MiB** |

**Invariant 2 fails, and the instrument says why.** At 256 connections the
collector holds 34.2 MiB of live bytes at peak while its in-flight gate reports
9.69 MiB. The gap is request bodies that have been read into memory but not yet
charged: `route` collects the whole body and only then does `accept` acquire
the semaphore, so `COLLECTY_MAX_INFLIGHT_BYTES` bounds what is past the gate
and not what is buffered before it. Peak memory is therefore a function of how
many clients there are, not of the ceiling the operator declared. With the
16 MiB request ceiling the defaults allow, 64 connections could buffer a
gigabyte against a 64 MiB declared ceiling.

One hypothesis about this was measured and **rejected**: hyper's 400 kB
per-connection read buffer is not the term. Capping it at 64 kB moved 256
connections from 72.4 to 75.1 MiB, which is nothing, and the change was
reverted rather than kept for looking plausible.

**The fix is not in this series, deliberately — but it is smaller than this
document first claimed.**

Charging the gate incrementally, a chunk at a time as bytes arrive, does
deadlock: a set of requests that each hold some permits and each need more can
all wait on each other. That much stands. What does *not* follow, and what an
earlier draft of this section wrongly concluded, is that avoiding the deadlock
requires a new `503`.

**Reserving the whole request up front is deadlock-free and refuses nothing.**
Acquire before reading a byte of the body: `Content-Length` when the request
declares a valid one, `COLLECTY_MAX_REQUEST_BYTES` when it does not; poll the
body only once the permit is held; release when the export is appended. Every
request acquires all-or-nothing, so no request can hold permits while waiting
for permits, and tokio's semaphore is FIFO-fair, so the waiter at the head is
always the next served. Under pressure a body simply waits to be read, which is
backpressure the way the gate already documents it — "a request waits for room
rather than being refused".

What it costs is utilization rather than correctness. A chunked sender that
declares no length reserves the 16 MiB ceiling whatever it actually sends, so
the 64 MiB default admits four of those at once; a sender that declares its
length — which every OTLP exporter this collector has met does — reserves
exactly what it sends, and 64 MiB holds about sixteen hundred exports of the
size measured here.

So the decision left is about latency: a client whose body sits unread until
room appears may hit its own timeout, and that is a product decision about what
collecty does to a slow-consumed client. It is not a decision about whether to
start refusing.

### Against the budget

| | declared | shipped build, measured |
|---|---|---|
| steady state | ≤ 64 MiB | **18.5 MiB** at 8 connections, 300 s |
| peak | ≤ 128 MiB | **18.5 MiB**, from 60.7 before the series |
| not a function of time | — | the 13-hour trace predates every fix; unretested |
| not a function of connections | — | **fails**: 17.9 → 72.4 MiB from 8 to 256 |
| not a function of past backlog | — | **improved, not met**: +7.8 MiB per 198 MB drained |

**What this series has not established, in the order it is being answered.**

1. **Whether the ratchet over hours is gone.** Every row above is five minutes
   long. The only evidence that collecty grows with time is thirteen hours of a
   build that no longer exists, and reducing a cause is not the same as
   removing an effect — this is the question the whole series is for, and it is
   the one none of its rows answers. A fault-free multi-hour run on the new
   build is running for exactly this, with faults deliberately left out: the
   drain step already has its own answer above, and mixing it in would
   contaminate the time slope, which is the only thing that run is asked to
   decide.
2. **Whether it holds on a corpus that compresses like production's.** This
   rig achieves 3.63x where the soak's collector achieved 4.87x, so its
   segments are larger than production's. That errs towards more memory
   pressure rather than less, but it has not been shown to err only that way.
3. **What to do about connection scaling**, which is a policy decision and is
   set out above.

And one cheap run that would close a question this document opens rather than
answers: `MALLOC_ARENA_MAX=1` **on top of** the spool thread, which is the
comparison the arena row cannot make.

### A trap this rig fell into first

The first drain run put its queue under `/tmp`, which is tmpfs. Those pages are
shmem, they are charged to the cage, and with swap off they cannot be
reclaimed, so a backlog the collector is designed to put on disk filled the
cage and the kernel killed the process at 190 MB of queue with anon flat at
65 MiB. Worse, the rig reported that run `MEASURED`: it read `memory.events`
once, at the end, by which time the scope was gone and the OOM counter with it.

Both are fixed — the runner refuses a queue directory on tmpfs, samples the OOM
counter throughout, and fails a run whose process stopped early — and both are
recorded because a gate that can report a kill as a pass is the same class of
failure signy's `NOT_MEASURED` rule exists for.

**On a real filesystem the page cache is reclaimable and the process survives**,
but `memory.current` still reached 255.9 MiB of the 256 MiB cage while anon was
105.8. A sidecar sized to its collector's heap will meet its limit through the
queue's page cache long before the queue's own 1 GiB budget is reached. That is
a deployment fact `CONFIGURATION.md` does not currently state.
