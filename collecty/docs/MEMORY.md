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

Each one is its own commit, and each records **which hypothesis it confirmed or
rejected**, not only how many megabytes moved. A step that measures worse is
kept here rather than reverted quietly.

| # | change | what it would prove |
|---|---|---|
| 1 | budget declared | above |
| 2 | baseline, glibc, instrumented | tagged live vs `mallinfo2` vs cgroup anon, and the thread count beside them |
| 3 | `MALLOC_ARENA_MAX=1` | diagnosis only: how much of the gap is per-thread arenas |
| 4 | one spool thread instead of `spawn_blocking` | structural, still on glibc, directly comparable to 2 |
| 5 | mimalloc | the allocator as a shipping choice, judged after the structure is fixed rather than before |
| 6 | `frame_record`'s copy removed | in-flight live bytes halved |
| 7 | zstd context reused across segments | whether `libc_in_use − tagged_live` falls, which is where a C-side allocation lives |
| 8 | segment read buffer reused | the drain step above |
| 9 | in-flight gate charged as the body arrives | separate scaling test: connections 8 → 64 → 256 at a fixed rate, peak must plateau |
| 10 | outage and drain, repeated | separate scaling test: the step must go to zero |

Nine and ten are deliberately not judged on the same run as the rest. Both fix
a *shape* rather than a quantity, and on a fixed-connection, no-fault workload
both would measure approximately zero and look worthless.
