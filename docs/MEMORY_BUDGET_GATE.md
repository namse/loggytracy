# The budget gate

[`VISION.md`](VISION.md) invariant I says memory is a budget you declare. This is
the test that says whether it is: it runs the engine at a declared budget under
the comparison bed's mixed load and asserts that the **cgroup's anonymous
footprint** stays under the number that was declared.

**The current, honest answer to "at what declared budget does this engine survive
its own load?" is 5 GiB** — two and a half times the 2 GiB
[`COMPARISON.md`](COMPARISON.md) asked for and Loki met. At the 2 GiB the
comparison bed used, the gate is **red**: the process is OOM-killed at t≈49 s.
Nothing here is fixed; this is the ruler for the fixes.

It exists before them on purpose.
[`MEMORY_ATTRIBUTION.md`](MEMORY_ATTRIBUTION.md) established that the engine's
own accounting reported 669 MiB live and 111 MB of memtable at the instant the
kernel killed it, so **every other M10 item is a fix and this is the only thing
that can say whether a fix worked.**

---

## Running it

```
cargo run --release --bin memory_gate -- --budget 2GiB
```

One command, one machine-readable verdict, and an exit code per outcome. It
builds the release binaries, puts the server in a `systemd-run --user --scope`
cgroup at `MemoryMax=<budget>` with `MemorySwapMax=0` — the same native cgroup v2
mechanism [`../scripts/run_memprof_local.sh`](../scripts/run_memprof_local.sh)
establishes, and the same zero swap the comparison bed's
`memswap_limit == mem_limit` means — drives it with the M8 harness, and samples
`memory.stat` at 4 Hz.

`--help` lists every knob; all of them default to the comparison bed's values
(1.2 M events, 20 k eps offered over 8 connections, 5 qps of queries over their
own 4 connections, seed 1592598566). `--limit` sets the cgroup's `memory.max`
separately from the declared budget, which is how an overshoot is measured
instead of merely being fatal. `--server-env K=V` reaches the server, which is
how `LOGGYTRACY_MEMORY_BUDGET` will be handed to this gate when it exists.

Output lands in `target/memory_gate/<name>/`: `gate.json` (the verdict and every
number behind it), `anon.csv` (the sampled series), `load.json` (the harness's own
report), `server.log`, `harness.log`.

**The workload is reads concurrent with writes, and `--query-eps 0` is refused.**
The attribution measured query at 57–77 % of all allocation traffic, and
allocation traffic is what the allocator retains, so an ingest-only run is a
different experiment. The shape is the one `compare/` ran, so the gate's number
and the published one are comparable.

---

## What it validates against, and what it refuses to

**Peak `anon` from the cgroup's `memory.stat`. Only that.**

The engine's own view is the thing being audited, so it cannot be the auditor: at
the 2 GiB kill it read a third full. `memory.peak` is not it either — it includes
the page cache this engine's own Parquet and WAL writes create, which is
reclaimable and is not what an OOM kill is decided on. Both are recorded beside
the verdict; neither is gated.

One row of one passing run, at a 5 GiB budget, is the whole argument:

| in the same run | MiB |
|---|---|
| peak cgroup `anon` — what the kernel decides on | **4675** |
| peak cgroup `memory.peak` — includes page cache | 5120 (the limit) |
| peak `loggytracy_memtable_bytes` — what M9 had to reason with | **193** |

---

## The four outcomes

Distinguishable by exit code, and each one below was produced by a real run
rather than asserted:

| exit | verdict | means | seen in |
|---|---|---|---|
| 0 | `UNDER_BUDGET` | survived, delivered the offered workload, peak `anon` ≤ budget | the 5 and 6 GiB runs |
| 2 | `OVER_BUDGET` | survived, peak `anon` exceeded the declared budget | `--budget 2GiB --limit 8GiB` |
| 3 | `OOM_KILLED` | the kernel killed it inside its own declared budget | the 2 and 4 GiB runs |
| 4 | `NOT_MEASURED` | the measurement did not happen | `--seconds 8`, which delivers 13 % of the offered events |

**`NOT_MEASURED` is a failure, not a skip.** [`LOAD_RESULTS.md`](LOAD_RESULTS.md)
§3 is a peak RSS that had never been measured, written down as an engine result,
and the rule it arrived at is that a gate that cannot measure must not pass. So
each of these fails with a stated reason rather than reporting a budget: no
cgroup v2, no systemd user manager, a `memory.max` that is not the number the run
asked for, swap not disabled, the server never becoming ready, the server exiting
without a cgroup OOM event (a crash is not a budget result), zero samples of
`memory.stat`, no harness result, and — the one that matters for the fixes
ahead — **the engine accepting less than 90 % of the offered events**, because a
budget that is met by refusing the load was never exercised.

`OVER_BUDGET` is only reachable when `--limit` exceeds `--budget`. With the two
equal, which is the default and what an operator does, the kernel enforces the
ceiling before `anon` can cross it, so the honest verdict at a budget this engine
cannot hold is `OOM_KILLED`.

An OOM kill is established by two independent sources, recorded separately:
`oom_kill` in the cgroup's `memory.events`, and systemd's own `Result=oom-kill`
on the scope. In the 2 GiB run both fired, and the process also exited on
`SIGKILL`.

---

## The baseline

Build `50190cf`, default features (**not** `memprof` — its 16-byte tag per live
allocation was 66–268 MiB in the attribution's runs). Default glibc, no
`MALLOC_*` tuning. Machine: Linux 6.12.57, 12 logical CPUs, 31.3 GiB RAM. Every
run ingests 1.2 M events at an offered 20 k eps over 8 connections with 5 qps of
queries on 4 more.

| declared budget | cgroup limit | verdict | `anon` peak (MiB) | share of budget | at | achieved eps | queries |
|---|---|---|---|---|---|---|---|
| 2 GiB | 2 GiB | **`OOM_KILLED`** | 1845 | 90 % | 49.3 s | — | — |
| 4 GiB | 4 GiB | **`OOM_KILLED`** | 4082 | 99.7 % | 60.8 s | — | — |
| **5 GiB** | 5 GiB | `UNDER_BUDGET` | **4675** | **91 %** | 65.3 s | 18 875 | 273 |
| 5 GiB (repeat) | 5 GiB | `UNDER_BUDGET` | 4535 | 89 % | 65.8 s | 18 761 | — |
| 5 GiB (repeat) | 5 GiB | `UNDER_BUDGET` | 4392 | 86 % | 65.3 s | 18 820 | — |
| 6 GiB | 6 GiB | `UNDER_BUDGET` | 4545 | 74 % | 65.3 s | 18 830 | 271 |
| 2 GiB | 8 GiB | **`OVER_BUDGET`** | 4586 | **224 %** | 64.5 s | 18 711 | 271 |

Three things this says beyond the headline.

**The overshoot at the bed's limit is 2.24×.** Given 8 GiB of room and asked to
stay inside 2 GiB, the engine's anonymous peak is 4586 MiB. That is the size of
the gap the M10 and M11 fixes have to close, and it is a measurement rather than
an extrapolation from a run that died.

**Above 5 GiB the peak stops tracking the limit.** 4392–4675 MiB at a 5 GiB
limit, 4545 at 6 GiB, 4586 at 8 GiB: this workload's own anonymous high-water is
about **4.5 GiB**, and it is flat in the headroom it is given. Below that, the
peak *is* the limit — 99.7 % of it at 4 GiB, and 90 % at 2 GiB with 191 MiB of
page cache making up the rest — which is what dying at the ceiling looks like. The
attribution's 8 GiB run reached 4937 MiB on a `memprof` build carrying its own
instrument, and 4586 MiB here is the same number without it.

**5 GiB is not a recommendation.** It is the smallest whole GiB that survives,
and it survives at 86–91 % of itself with a 283 MiB spread across three
identical runs. Nobody should deploy on 9 % headroom. It is where invariant I
stands today, and the number M10 and M11 have to move.

---

## What this does not establish

Stated in the discipline [`COMPARISON.md`](COMPARISON.md) sets.

* **It is not the Docker bed.** A native cgroup v2 scope with the same
  `memory.max`, the same zero swap and the same `anon` — a container of the same
  kind, not the same container. It also achieves 18.7–18.9 k eps where the
  published Docker run achieved 16.8 k, so the two beds are not
  interchangeable and this one is the easier of them.
* **Sampling is 4 Hz**, so a spike shorter than 250 ms between samples is not in
  the series. A pass by less than one sample's worth of slope is therefore not
  proven; a *failure* is not affected, because a kill is the kernel's verdict and
  not the sampler's. The killed runs' peaks are the last reading before death and
  are lower bounds.
* **On a killed run the harness's own numbers are lost.** It is killed three
  seconds after the server dies, before it writes its report, so the ingest and
  query columns are empty for those rows and `delivered_fraction` reads zero.
  The verdict does not depend on them — an OOM kill outranks everything
  downstream of a dead server — but the rows are less informative than the
  surviving ones.
* **One workload, one corpus, one machine, one duration.** The run ends at 1.2 M
  events, i.e. about 65 s and a few dozen parts. A longer run reaches more parts,
  more merges and more sidecars, and nothing here says the 4.5 GiB high-water is
  the ceiling of a run that lasts an hour. `LOAD_RESULTS.md` §10's soak is the
  shape that would answer that.
* **It measures a peak, not a distribution.** Three runs at 5 GiB is enough to
  see a 283 MiB spread and not enough to put a bound on it.
* **The 90 % delivery floor is a judgement.** A run that delivered 89 % of its
  offered events would be reported as unmeasured; the number is chosen to reject
  a budget met by refusal, not calibrated against anything.

---

## Where it does not run

**Not in CI, and not on every push.** It needs a cgroup v2 scope, a systemd user
manager and minutes per run, and a peak memory figure from a shared runner whose
neighbours are invisible is exactly the kind of number this repository has
already published once and retired. CI does compile it —
`clippy --all-targets -D warnings` and `cargo test` both build the target — so it
cannot rot the way a script and a document did; and a run in an environment that
cannot give it a cgroup exits 4 rather than passing.

It is a binary and not a `#[test]` for the same reason: `cargo test` stays a
second long, and the verdict stays typed, compiled and gated instead of living in
`awk` beside a table someone has to remember to update.

---

## When the budget knob lands

`LOGGYTRACY_MEMORY_BUDGET` does not exist yet — it is the last step of M10, not
this one — so the gate takes the budget as its own input. That is deliberately
the contract the knob will have to satisfy: *given one declared number, peak
`anon` stays under it while the offered workload is delivered.* Until then the
knob can be exercised through `--server-env LOGGYTRACY_MEMORY_BUDGET=...` without
changing this program; once it exists, the gate should read it from the server it
started rather than accept it twice.
