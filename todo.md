# TODO

This tracks work deferred beyond M3's current scope and work for later milestones.

The complete production-readiness gate list is in [`docs/PRODUCTION_READINESS_REVIEW_2026-07-26.md`](docs/PRODUCTION_READINESS_REVIEW_2026-07-26.md)
(previous review: [`docs/PRODUCTION_READINESS_REVIEW.md`](docs/PRODUCTION_READINESS_REVIEW.md)).
The three invariants the work below serves are in [`docs/VISION.md`](docs/VISION.md).

## Open correctness defects

**Read this section before any milestone below.** These are wrong answers, not slow ones, and a milestone is
never a reason to leave one open. Each was found by a measurement rather than by review, and each is recorded
here with the measurement that found it so that fixing it can be verified the same way.

Neither is fixed at the point of discovery, on purpose: both were found by [`compare/`](compare/) while it was
being built, and a ruler that edits the thing it is measuring in the same change measures nothing. That
reason expires now — the bed is built and its baseline is published.

- [x] **`query_range` treats `end` as inclusive; Loki treats it as exclusive.**
      *Found by:* M9's row-equality check — 2 of 96 otherwise identical answers differed, always by exactly
      the row whose timestamp equals the window's `end`.
      *Confirmed:* directly against both endpoints over the same window. Both include `start`.
      *Severity:* a Loki-compatibility defect rather than a preference, because the endpoint claims Loki's
      contract. Invisible unless a boundary lands exactly on a row, which is why nothing before the
      comparison's step-aligned windows surfaced it.
      *Fixed by:* `part::QueryTimeRange`, which now owns the question "does this timestamp fall in the
      window" for the whole read path. The owner was not `parse_time_ns` — the timestamps parsed correctly;
      the boundary was spelled out four separate times below the query layer (the memtable scan, the
      part-level prune, the row-group prune and the row-level reject) and each one closed `end`. Log queries
      construct `half_open`, so `[start, end)` is decided once, by the handler. The scans whose `end` is not
      a client-supplied exclusive bound say `closed` explicitly: a metric scan's `end` is its last evaluation
      point, tail's is "now minus delay", and a merge's is "every row".
      *Verify with:* `compare/run.sh`, matrix phase — the check that found it is the end-to-end regression
      test. At unit level, three tests put a row exactly on the boundary and each fails if a single site
      drifts back: `memtable::tests::the_range_decides_whether_a_row_on_end_is_returned`,
      `part::tests::a_row_on_end_belongs_to_a_closed_window_and_not_to_a_half_open_one` (which also asserts
      row-group pruning agrees with the row-level test, since a tighter prune drops rows silently) and
      `query::tests::query_range_includes_start_and_excludes_end_in_the_memtable_and_in_parts`.
      `memtable::tests::query_includes_the_end_timestamp` had encoded the defect as the contract and is the
      first of those three now.
      *Not changed:* the metric step grid, which
      [`docs/COMPARISON.md`](docs/COMPARISON.md) records as a separate still-open difference — Loki aligns
      samples to absolute multiples of `step` and loggytracy steps from `start`.

- [x] **`| json` does not promote extracted fields into a log response's stream labels; Loki's does.**
      *Found by:* the same run, but **not** by the equality check — its digest is over `(timestamp, line)`
      pairs, so a label-set difference is structurally invisible to it. `json_field` was reported as 24/24
      agreed. The two label sets appear in [`docs/COMPARISON.md`](docs/COMPARISON.md) only because they were
      captured alongside.
      *Measured:* loggytracy returned 6 labels where Loki returned 22, the difference being every field the
      parser extracted.
      *Severity:* the log-query response shape, which is what Grafana's Logs panel renders as a line's
      detected fields. Metric grouping is **not** affected — `sum(count_over_time({app="api"} | json [5s])) by
      (level)` is covered by `query::tests::metric_grouping_uses_extracted_fields_and_parser_errors` and works.
      *Blocked on:* nothing. The checker is fixed first (next item) and is now **red on this defect**:
      `json_field` reports 0 of 24 agreed, and the fix is what has to turn it green.
      *Also measured while extending the checker:* loggytracy does not drop the extracted fields, it returns
      them in the **third element of each `values` tuple** — the structured-metadata slot — where Loki returns
      them as stream labels. So the response carries the same names in the wrong place, plus one name Loki does
      not have at all (`trace_id_extracted`, loggytracy's collision rename for a field that is also pushed
      metadata). The fix is about placement, not about extraction.
      *This and the "accepted" pushed-metadata difference were one defect, and that was settled against Loki
      rather than by argument.* `grafana/loki:3.3.2`, one stream with `level` as a stream label, `trace_id` and
      `pod_ip` pushed as structured metadata, and a line whose JSON carries `level` and `trace_id`:
      `{app="probe"}` answers with `trace_id` and `pod_ip` **among the `stream` labels** and a **two-element**
      `values` tuple, and `{app="probe"} | json` adds the extracted fields to that same set. Loki's default JSON
      encoding never uses the third element; the three-element tuple is its *opt-in*
      `X-Loki-Response-Encoding-Flags: categorize-labels` shape, and there it is an object of categories
      (`{"structuredMetadata": {…}, "parsed": {…}}`) rather than the flat map loggytracy was returning. So the
      digest's declared exemption described the same slot as this defect, and both are fixed.
      *Loki's collision rule, measured the same way:* a `| json` field colliding with a **stream label** becomes
      `<name>_extracted` and both survive and both filter. A field colliding with a **pushed metadata key** is
      **discarded** — `| json | trace_id="<the JSON value>"` matches nothing, `| json | trace_id="<the metadata
      value>"` matches, `trace_id_extracted` does not exist, and `line_format "{{.trace_id}}"` renders the
      metadata value. That is why Loki's response had no `trace_id_extracted` and loggytracy's did.
      *Fixed by:* `query::build_stream_data`, which now merges each row's stream labels with its post-pipeline
      field set, emits one stream per distinct merged label set and a `[timestamp, line]` tuple. It takes the
      requested `direction`, because merging two input streams into one group interleaves their rows.
      `logql::merge_extracted` implements the collision rule above; `LogQuery::process_entry_with_labels_*` now
      also passes a stream label that a `label_format` **rewrote** through to the response (Loki answers
      `| label_format level="rewritten"` with the new value), while an unchanged one is still not duplicated
      into the query-local metadata. The internal representation is unchanged — extracted fields still live on
      the query-local entry's `structured_metadata`, which is what filtering and `sum(...) by (extracted_field)`
      read (`query/tests.rs`, `sum(count_over_time(… | json [5s])) by (level)`).
      *Deliberately different:* a second collision on the same name. loggytracy answers `foo_extracted_2` and
      keeps the `foo_extracted` stream label; Loki appends `_extracted` once and **overwrites** the
      `foo_extracted` stream label with the extracted value, losing it. loggytracy does not lose a value it was
      given. This is the `_extracted` pruning item under "P1 — LogQL improvements", now with the measurement.
      Also unchanged: an *unaggregated* metric query's series identity. Loki promotes a row's metadata and
      extracted fields into it and loggytracy groups by stream labels plus whatever `by`/`without` names, so
      `rate({app="api"})` answers one series per `trace_id` on Loki and one per stream here. That is an
      identity/cardinality difference rather than a misplaced field, it is why the matrix asks for
      `sum(rate(...))`, and it stays reported as a difference.
      *Verified with:* the digest, not by assertion — the same short comparison the item below documents.
      `json_field` **0 of 24 before, 24 of 24 after**, with `label_only`, `line_filter` and `rate` unchanged at
      24/24. Then the digest's placement exemption was removed, because after the fix it could only hide a
      regression back into the `values` triple, and the same comparison re-run against a fresh Loki volume:
      **96 of 96**, with `label_only` now reading `stream:trace_id`/`stream:pod_ip` on both sides where it used
      to read the placement-blind `metadata:` tag. At unit level, five tests at the response-builder level pin
      this without Loki running: `query::tests::a_log_response_promotes_pushed_metadata_into_the_stream_labels`,
      `a_log_response_promotes_json_extracted_fields_into_the_stream_labels`,
      `an_extraction_shadowed_by_pushed_metadata_never_reaches_the_response`,
      `promotion_regroups_rows_by_their_whole_label_set_and_keeps_the_direction` and
      `label_format_over_a_stream_label_reaches_the_response`, plus
      `logql::tests::json_scalar_extraction_is_ordered_and_metadata_wins_collisions` for the collision rule and
      `matrix::tests::structured_metadata_in_the_values_triple_is_a_disagreement` for the ruler.
      *Not regenerated:* [`docs/COMPARISON.md`](docs/COMPARISON.md), which is the M9 run's document. Its
      generator's prose is updated with the exemption it no longer has; the next `compare/run.sh` publishes it.

- [x] **Extend the row-equality digest to cover labels.** The finding above matters less than the blind spot
      that hid it: a checker that proves two engines agree while silently not looking at half the response is
      the kind of green light this repository has already been burned by once
      ([`docs/LOAD_RESULTS.md`](docs/LOAD_RESULTS.md), retired).
      *Fixed by:* `matrix::digest_response`, which now digests one record per returned entry holding the
      entry's timestamp, its line **and every label it carried together with where the response put it**
      (`stream:` / `entry:` / `metric:`), so the same lines under different labels are no longer the same
      answer. A metric result additionally digests one record per series identity, so an extra empty series
      cannot hide, and the result type and `status` are hashed in as an envelope. A malformed `values` tuple is
      now an error rather than a silently skipped row — the old digest read what it could and ignored the rest,
      so a response it could not parse compared equal to an empty one.
      *One accepted difference turned out to be the same defect and was withdrawn:* pushed structured-metadata
      keys were digested **without** their placement, on the grounds that Loki promotes them into result
      identity while loggytracy returned them in the `values` triple. That is the same slot and the same
      sentence as the `| json` defect above, so it was fixed instead, and the exemption is gone — nothing is
      exempt from placement now, because after the fix the exemption could only hide a regression. Their
      *values* were and are still compared per entry, so `label_only` checks per-row `trace_id`/`pod_ip` that
      nothing checked before. What remains exempt is by name, not by placement: Loki's derived `detected_level`
      and `service_name` are dropped, and every answer records which dropped names it carried so the document
      states the exemption with its counts instead of the digest being quietly narrower than it claims. The
      metric step grid stays handled by `align_to_step`, i.e. by the query rather than by the digest.
      *Reported as:* the disagreement table gains a labels-differ column, and the label difference itself is
      printed grouped by difference — "24 queries: only loggytracy `entry:level`…, only Loki `stream:level`…"
      — instead of reaching the document by accident.
      *Also now checked:* response order against the requested `direction` (kept out of the digest, which must
      stay order-independent, and reported as its own fact plus a distrust item). *Deliberately not digested:*
      `data.stats`, which reports how much each engine had to read (16,384 lines against Loki's 1,251 for the
      same answer) and is the thing they are supposed to differ on; a log response's grouping into streams,
      because both now return one stream per metadata combination but neither promises the same partition of
      the same rows; and `limit` truncation, which no window in the matrix reaches (`limit` 20,000 against
      ~6,250 rows).
      *Verified by:* seed + matrix phases only, against Loki 3.3.2 in `compare/docker-compose.yml` and a local
      loggytracy, 30,000 rows over 8 streams and 3 windows — the shortest configuration that surfaces
      `json_field`, about two seconds per phase instead of `compare/run.sh`'s 25 minutes.
      `label_only` 24/24, `line_filter` 24/24, `rate` 24/24, **`json_field` 0/24** at the point the checker was
      fixed; 96/96 once the defect above was. The unit tests that pin each rule are
      `matrix::tests::the_same_lines_under_different_labels_are_not_the_same_answer`,
      `structured_metadata_in_the_values_triple_is_a_disagreement`,
      `json_extracted_fields_in_the_wrong_place_are_a_disagreement` and
      `every_label_is_digested_where_the_response_put_it`.
      *Re-seeding Loki is not idempotent, and a comparison rerun has to start from a fresh volume.* Loki drops
      an exactly duplicate entry from a **log** answer but still counts it in `rate`, so a second `seed` against
      the same volume leaves `label_only`/`line_filter`/`json_field` agreeing and doubles every `rate` sample.
      That was hit while verifying the fix above and is a property of the bed, not of either engine.
      *Not regenerated:* [`docs/COMPARISON.md`](docs/COMPARISON.md), which is the M9 run's document and
      describes the digest that run used. The next `compare/run.sh` publishes the extended one; regenerating it
      from the old artifacts would print the new prose over old digests.

## M8 — the ruler (precondition for everything after it)

No optimization starts before this. Every performance number currently in the repository was produced by a
harness that cannot reach its own offered rate, on data with cardinality 1 and near-zero entropy, with reads
that never contend with writes. Optimizing against those numbers reproduces them.

- [x] **Retire the numbers, keep the reasoning.** Both documents carry a retirement header and no number in
      them may be cited until the rewritten harness regenerates it. What survives is what never depended on the
      magnitudes: three refuted hypotheses kept rather than replaced, the terminal-sample lesson, §8's finding
      that `tokio::time::interval` fires immediately — so every "merge disabled" run had one merge in it — now
      written as explicit Invalidated markers on the sections it killed, and the structural object-store counts,
      which are properties of the code rather than of the harness. Three citations were wrong and are now
      recorded as wrong: a named test that never existed, an artifact that was never checked in, and a surviving
      artifact that disagrees with the document citing it on both build and verdict. That artifact is kept, with
      a header, because it is the only checked-in evidence *for* the retirement
- [x] **`benches/` with criterion** — six targets over WAL append, memtable insert and query,
      `rows_from_snapshot`, bloom build and lookup, part write and scan, and LogQL parse and evaluation. The
      corpus is seeded and clock-free with a cardinality knob, and prints its measured compression ratio every
      run so a drift back toward the retired harness's 31.5x is visible immediately. The row and part benches
      carry a counting global allocator, because time is the wrong instrument for invariants I and II: it
      already reads two allocations per label per row at `part/mod.rs:302`, which is the number `Arc<Labels>`
      has to move — and did: 11/17/27 allocations per row at 2/5/10 labels became 6.00 at all three, so the
      table's job is now to keep it flat rather than to watch it grow
- [x] **Rewrite `src/bin/load.rs`** (now `src/bin/load/`): N keep-alive connections per workload over a
      hand-rolled HTTP/1.1 client on tokio (no new dependency — the server binary keeps object storage as its
      only outbound call); latency taken from the *intended* send, with service time and response time both
      reported and their gap called out; the corpus is `loggytracy::corpus`, promoted out of `benches/` so
      there is one generator; out-of-order and late arrival are workload knobs; queries are an independent
      workload with their own rate and connections; percentiles carry their sample count and refuse to be
      computed below it; RSS is `VmHWM` from `/proc/<server pid>/status` with a sampled `VmRSS` series, and a
      run that cannot read it fails; the WAL gate is a trend, not `trough <= peak * 0.5`
- [x] **Delete `src/bin/m5_load.rs`** — deleted. It measured `std::process::id()`, the harness's own RSS
- [x] **The Dockerfile builds on a different compiler than CI gates** — fixed in M9, because the comparison
      bed needs an image. `rust-toolchain.toml` is now in the dependency-cache `COPY`, so the toolchain
      download happens once in the cached layer and the shipped binary is built by the compiler CI gates.
      Building it for the first time found two further defects that meant **the image had not been buildable
      at all** since the benches landed: `Cargo.toml` declares six `[[bench]]` targets and cargo refuses to
      *parse* a manifest whose declared targets are missing, so `benches/` is now copied too (nothing builds
      them; they only have to exist); and the dependency-cache stub wrote only `src/main.rs` while
      `Cargo.toml` declares a `[lib]`, so the layer failed outright — and once it is stubbed, `touch
      src/main.rs` alone leaves the empty library's fingerprint valid, so both targets are touched
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

**Done, and the claim lost.** The bed is [`compare/`](compare/); the published table is
[`docs/COMPARISON.md`](docs/COMPARISON.md), regenerated from `target/compare/*.json` by
`compare/run.sh` rather than written. A full run is about twenty-five minutes.

- [x] **Loki single-binary beside loggytracy under docker-compose** — `compare/docker-compose.yml`, identical
      `mem_limit` and `memswap_limit`, both on local filesystem storage, one isolated volume each. loggytracy
      runs **local-only** so that, like Loki's filesystem backend, it keeps one durable copy of a part rather
      than a local one and a remote one. `compare/loki-config.yaml` documents every deviation from Loki's
      defaults beside the reason, and the run captures Loki's own `GET /config` diffed against
      `GET /config?mode=defaults` so the published deviations are Loki's report and not this repository's claim
- [x] **Harness target support** — `LOGGYTRACY_LOAD_TARGET=loggytracy|loki` and no new dependency. The same
      corpus, seed, offered rate, connections and wire bytes go to both, because the push endpoint is the same
      endpoint. What differs is the metric vocabulary, the behavioural gates (Loki is gated on
      `loki_discarded_samples_total` being zero — a discard is this bed misconfiguring Loki, not a Loki result)
      and the memory source, which is now cgroup v2 `memory.peak` when `LOGGYTRACY_LOAD_CGROUP` is set
- [x] **A dataset both systems provably hold.** A paced load run sends what it managed to send at wall-clock
      timestamps, so two runs of it are two different datasets and nothing row-level can be compared between
      them. `LOGGYTRACY_LOAD_PHASE=seed` pushes a fixed corpus at fixed log timestamps from an anchor both runs
      are given; `matrix` then times the four shapes over it cold and warm and digests every answer
- [x] **Four query shapes plus ingest throughput and bytes on disk per GB ingested** — the table is in
      [`docs/COMPARISON.md`](docs/COMPARISON.md). At 8 GiB per container, 1.2 M events at an offered 20 k eps
      and zero errors on both sides: loggytracy wins `label_only` (0.36x), loses `|=` (1.69x), loses
      `| json | field=` (1.49x cold, 1.44x warm) and loses `sum(rate())` (7.1x); ingest 16.8 k eps against
      19.9 k; settled data 323 MiB/GB against 267 MiB/GB
- [x] **Row equality: 94 of 96 queries returned identical rows.** The two that did not are the finding below
- [ ] **Object-store request counts — deferred, with the reason published.** On a filesystem backend the two
      counters do not count the same thing: loggytracy's `loggytracy_object_store_operations_total` counts
      calls into the `object_store` crate and reads zero with no store configured, and Loki's filesystem chunk
      client emits no request counter at all. Making the axis real means putting **both** on MinIO, which
      changes both storage paths end to end and needs its own settling and validation — a different experiment,
      not an extra column
- [x] **Publish the table, including when it loses** — it loses

### What the bed found

- **Two correctness defects**, both promoted to "Open correctness defects" at the top of this file so a
      completed milestone's log is not where they live: `query_range` treats `end` as inclusive where Loki
      treats it as exclusive, and `| json` does not promote extracted fields into a log response's stream
      labels. The second is the more useful finding, because the row-equality check **did not catch it** — the
      digest is over `(timestamp, line)` pairs, so `json_field` was reported as 24/24 agreed while the two
      responses carried 6 labels and 22
- [x] **loggytracy was OOM-killed at a 2 GiB container limit where Loki was not**, ingesting 1.2 M events at
      20 k eps with the harness's query workload on. `memory.peak` climbed monotonically from 2 MB to the limit
      in forty seconds while `loggytracy_memtable_bytes` reported 111 MB, so the accounted memtable is not
      where it went. **Answered by [`docs/MEMORY_ATTRIBUTION.md`](docs/MEMORY_ATTRIBUTION.md)**: 44% of the
      anonymous peak is allocator-retained free memory, and the largest live terms are one merge group
      (771 MiB) and the flush's whole-snapshot `Vec<Row>` (721 MiB). The query path is implicated through the
      allocation traffic it generates rather than through what it holds. The bed sweeps limits and reports the
      sweep, so the published run is at 8 GiB and says so
- [ ] **In local-only mode the WAL is never compacted.** `flush.rs:219` passes `remote_cache.is_some()` as the
      `compact` flag, so without an object store the checkpoint offset advances and `journal.wal` keeps every
      byte ever ingested, uncompressed. It was 541 MiB against 143 MiB of parts — 79% of the disk footprint.
      Either local-only should compact too, or the mode should be documented as not for retention
- [x] **cgroup `memory.peak` includes the cgroup's own page cache**, so it is not a footprint on its own; both
      systems write a WAL and then large data files. The harness now samples `anon` out of `memory.stat` and
      reports the anonymous high-water mark beside the cgroup peak, because the anonymous figure is what an OOM
      kill is decided on
- [x] **Loki promotes structured metadata into metric identity.** A bare `rate({app="x"}[1m])` returns one
      series per `trace_id` on Loki and one per stream on loggytracy, so the matrix uses `sum(rate(...))`:
      otherwise the two are neither doing the same work nor producing comparable answers
- [x] **Loki puts metric samples on a grid aligned to absolute multiples of `step`** and will emit a point past
      the requested `end` to stay on it; loggytracy steps from `start`. The matrix aligns its window boundaries
      so this is a no-op, because otherwise a row-equality failure would be about the bed's choice of window

## M10 — declared memory budget ([`docs/VISION.md`](docs/VISION.md) I)

M9 supplied the number this milestone was missing: **at a 2 GiB container limit, ingesting 1.2 M events at an
offered 20 k eps with a 5 qps read workload, loggytracy is OOM-killed and Loki is not**
([`docs/COMPARISON.md`](docs/COMPARISON.md)). The accounted memtable was 111 MB at the time. That is invariant
I failing against a competitor on the same machine, and it is the test this section's last item asks for,
already written and already red.

### Phase A — diagnosis (done)

[`docs/MEMORY_ATTRIBUTION.md`](docs/MEMORY_ATTRIBUTION.md) is the measurement, `src/memprof.rs` is the
instrument (arena-tagging global allocator behind the default-off `memprof` feature), and
`scripts/run_memprof_local.sh` is the one-command reproduction. What it changes about the plan below:

- [x] **The dominant term is not any arena.** At the moment of the kill the engine's live heap was 669 MiB of
      2 GiB and **44% of the anonymous footprint was memory the process had already freed** and glibc had not
      returned — 61–69% in some variants, 67% in the surviving 8 GiB run. A budget denominated in live bytes
      would have read a third full at the instant the kernel killed the process
- [x] **It is a function of allocation rate, not of anything held.** 52 GB allocated in 33 s across 444 M
      allocations, 217x the offered data rate; query is 57–77% of that traffic and flush 17–38%.
      `MALLOC_ARENA_MAX=1` with trim thresholds takes `anon / live` from 2.5–4.1 to **1.34** and more than
      doubles time-to-OOM without fixing anything
- [x] **The query workload is not required and neither is merge.** Ingest alone is killed at 2 GiB on this bed
      (which contradicts `docs/COMPARISON.md`'s ingest-only observation; both are recorded, the difference is
      not explained). Merge disabled is killed sooner than ingest-only
- [x] **Refuted: sidecars and `PartMeta`** — 0.2–1.2% of the anonymous peak, ~240 kB + ~140 kB per part, and
      `loggytracy_part_sidecar_resident_bytes` is accurate to 4%. **Refuted: in-flight push bodies** — the
      journal append awaits its own completion, so in-flight is bounded by HTTP concurrency and measures
      ~0.3 MiB. **Refuted as a memory term: `rows_from_snapshot` outside `spawn_blocking`** — same bytes either
      side of the hand-off; it remains a latency defect
- [x] **Found, and not on anyone's list: a fourth per-row `Labels` clone.** `query/metrics.rs:134-155` walks
      every scanned row on an async worker thread, before the `spawn_blocking` at `:158`, cloning
      `stream.labels` per row — outside every arena, scanning to `max_metric_rows + 1` = 1 000 001 rows rather
      than the API limit, and 203 MiB at its high-water. Folded into M11's read-path list

### Phase B — the budget

**The gate comes first and it is built.** [`docs/MEMORY_BUDGET_GATE.md`](docs/MEMORY_BUDGET_GATE.md)
is the baseline; `src/bin/memory_gate.rs` is the gate. Every item below is a fix, and this is the
only thing that can say whether a fix worked, so it landed before them and it landed red.

- [x] **A test that runs at a declared budget and asserts peak memory stays under it** — and it
      asserts against the cgroup's `anon`, not against the sum of the arenas, because the sum of the
      arenas was a third of `anon` at the moment of the kill. One command,
      `cargo run --release --bin memory_gate -- --budget 2GiB`, in a `systemd-run --user --scope`
      cgroup with swap off, driven by the M8 harness with reads concurrent with writes at the
      comparison bed's parameters and seed. Four outcomes by exit code — 0 under budget, 2 over
      budget, 3 OOM-killed, **4 could not be measured, which is a failure and not a skip**
      (`docs/LOAD_RESULTS.md` §3: a gate that cannot measure must not pass; a budget met by
      refusing 90% of the offered load counts as unmeasured). **Measured: OOM-killed at t≈49 s at
      2 GiB; survives at 5 GiB at 86–91% of it; 4586 MiB of `anon` — 2.24× — when given 8 GiB of
      room and asked to stay inside 2 GiB — all on build `50190cf`. On `9199e07`, with M11's shared
      label sets, the same commands read: **`UNDER_BUDGET` at 2 GiB at 90–96% across three runs, and
      0.93x rather than 2.24x when given 8 GiB of room.** Not in CI: it needs a cgroup scope and minutes per run,
      and a peak-memory number off a shared runner is the kind this repository has already retired.
      CI compiles it, so it cannot rot the way a script and a document did
- [ ] **The gate should read the budget from the server once the knob exists**, rather than being
      told the same number twice. Until then `--server-env LOGGYTRACY_MEMORY_BUDGET=...` reaches the
      server without touching the gate
- [ ] **A killed run loses the harness's own numbers.** The harness is killed three seconds after
      the server dies and writes its report only at the end, so the ingest and query columns of
      every OOM row in `docs/MEMORY_BUDGET_GATE.md` are empty. A periodic partial write, or a
      result written on signal, would make a failing run as informative as a passing one
- [ ] **Make the anonymous footprint track live bytes first.** Measured precondition, not a tuning note: with
      the default glibc configuration no live-byte budget can be honest. `mallopt` at startup, or an allocator
      whose heap decays, or the arena-tagging allocator promoted into production. Whichever is chosen, the
      `anon / live` ratio it achieves must be published beside the budget
- [ ] **Honest metering.** `entries_bytes` (`memtable.rs:69-81`) counts line and label lengths only — not the
      56-byte `LogEntry`, the 48-byte slot per metadata pair, malloc headers, or `Vec` slack. Measured
      **1.70–1.79x under** in situ on the comparison corpus, so `MAX_MEMTABLE_BYTES=256 MiB` is really ~440 MiB
- [ ] `LOGGYTRACY_MEMORY_BUDGET` divided into ingest 20% / flush 25% / merge 25% / query 25% / sidecar 5% —
      the measured shares, not the guessed ones. Existing knobs become overrides; what is not overridable is
      that they sum. **Flush and merge did not fit their shares** (721 MiB and 771 MiB measured against
      512 MiB each at a 2 GiB budget), which is the work, not a reason to raise the shares. Flush's share is
      no longer the same problem — M11's shared label sets took `rows_from_snapshot` from 1 345 to 823 bytes
      per row and its peak live from 26–28 MB to 13.85 MB on the bench — but the arena was never
      re-measured in situ, so **721 MiB is a figure for a build that no longer exists and the number for
      this one is not known.** Re-run the attribution before sizing anything from it
- [ ] **Flush cannot be sized independently of ingest.** `rows_from_snapshot` held a copy of the memtable at
      **3.3x its accounted size** and 1 326–1 345 bytes per row, and the two peaked together. The label sets
      are now shared with the memtable rather than copied out of it, so the copy is the lines and the
      metadata; the multiple is no longer 3.3 and has not been measured in situ. Either the flush share is
      expressed as a multiple of the ingest share, or the flush streams the snapshot in bounded chunks
- [ ] **Query admission by budget, not by slot.** Replace `MAX_CONCURRENT_QUERY_SCANS × MAX_QUERY_MEMORY_BYTES`
      (8 × 512 MiB = 4 GiB, admitted in a comment at `config.rs:522`) with a shared arena. Same ceiling, and a
      burst of cheap queries no longer queues behind a slot count. The arena must include the metric path's
      materialization, which is outside it today
- [ ] **`merge_max_memory_bytes` must come from the budget.** Its 1 GiB default is half a 2 GiB container and
      is derived from nothing the operator set; one group reached 771 MiB live
- [ ] **Sidecars inside the budget.** They are outside it on purpose today (`part/reader.rs:77-81`), so resident
      memory grows with part count unbounded. Make them LRU-evictable — they are already durable in `index.bin`.
      Sized from the measured ~240 kB per part, not from a share
- [ ] **Stop materializing `PartMeta::streams`** (`part/mod.rs:231`, `part/metadata.rs:172-176`) — every distinct
      label set in every open part, held as live `String`s. Measured ~140 kB per part
- [ ] **Bound in-flight push bodies.** The ingest gate is checked once at request entry and nothing limits
      concurrency, so (in-flight requests x 64 MiB) sits outside the accounting. Measured at 0.3 MiB on the bed,
      so this is closing a hole rather than recovering memory
- [x] ~~**A test that runs at a declared budget and asserts peak RSS stays under it.**~~ Moved to the top of
      this phase and built there, because it is the gate the rest of these items are measured by rather than
      one of them: [`docs/MEMORY_BUDGET_GATE.md`](docs/MEMORY_BUDGET_GATE.md)

## M11 — bounded copies and deep pruning ([`docs/VISION.md`](docs/VISION.md) II, III)

Write path:

- [x] **`Arc<Labels>` end to end** — memtable, `Row`, part write, reader, registry, executor, query
      result and the metric path, as `SharedLabels = Arc<Labels>`. **The "largest single payoff" claim
      held.** `cargo bench --bench rows`: `rows_from_snapshot` goes from **1 457/1 505/1 569 bytes per
      row** at 2/5/10 labels to **823.4 at all three**, from **11/17/27 allocations per row** to
      **6.00**, and peak live from 26.0/27.0/28.3 MB to **13.85 MB flat** — the label term is not
      smaller, it is gone, and the table is now flat in the label sweep. `--bench part`: the scan goes
      from 3796/3955/4078 bytes per row to **3167/3263/3337** and from 11.2/19.3/27.4 allocations per
      row to **6.19/6.32/6.45**, with peak live 54.0/56.5/58.4 MB to **31.0 MB flat**. Timings improved
      everywhere and nothing regressed: `rows/from_snapshot` 1.80–4.31x, `rows/from_entry` 1.82x at two
      labels and **4.40x at ten**, `part/scan_tenants/1` 1.36x, `part/scan_label_columns/10` 1.27x,
      `memtable/query_cardinality/8192` 1.83x. **The gate moved from 5 GiB to 2 GiB**
      ([`docs/MEMORY_BUDGET_GATE.md`](docs/MEMORY_BUDGET_GATE.md)): `--budget 2GiB` was `OOM_KILLED` at
      t≈49 s and is now `UNDER_BUDGET` at 90–96% across three runs, and the 2.24x overshoot measured by
      `--budget 2GiB --limit 8GiB` is **0.93x** — the workload's own anonymous high-water fell from
      ~4.5 GiB to ~1.9 GiB while achieved eps rose from 18.7 k to 19.7 k. `encode_stream_index` is now
      keyed by borrows of the rows and `write_meta` collects distinct borrows, so both clone per stream
      rather than per row; the reader interns one label set per distinct stream per scan, keyed on a
      hash of the row's label columns with every candidate verified against them
  - [x] **A fifth site, on the metric path and not on this list:** `sample_value`
        (`query/metrics.rs`) built a `BTreeMap` of every label and every metadata pair per row to read
        the one field an `unwrap` names. It resolves that field directly now
  - [ ] **A sixth, still there:** `process_entry_with_labels_cancellable` (`logql/ast.rs`) clones
        `labels` into a mutable `fields` map **per row**, for every query including one with no pipeline
        stages at all. Removing it needs a copy-on-write field view across the whole pipeline, not a
        type change, and it sits directly on the just-fixed extracted-field placement.
        **It did not fall out of the streaming rewrite and was not taken there.** The sink calls it on
        every row the storage scan produces, so what the bound removed was the number of calls and not
        their cost — `benches/query.rs`'s `json_field` case now makes 3 130 of them instead of 200 250,
        which is the same 4.4x-per-row it always was. The one thing that changed is the shape of the
        fix: the pipeline now runs inside the sink with the row's own labels in hand, so a
        copy-on-write field view has exactly one caller to satisfy
  - [ ] **Two meters now over-count, both conservatively.** `Row::materialized_bytes`
        (`part/mod.rs`) and `estimated_log_entry_memory_bytes` (`query/mod.rs`) charge every row for the
        label bytes it shares, so merge group sizing and `max_query_memory_bytes` are stricter than the
        memory that is really held. Neither was loosened here: that changes a limit and belongs with
        M10's honest metering
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

- [x] **`normal_scan_limit = usize::MAX` is gone**, and the read path is a bounded top-K sink the
      reader, the registry and the memtable stream into (`src/part/sink.rs`, `src/log_scan.rs`). The
      scan reads until it holds `limit` rows that **survived** the pipeline, and skips a whole part
      from its tenant segment's span, a whole row group from `row_group_min_ts`/`max_ts`, and the
      rest of a part on the first row past the sink's frontier — ties go to whichever row arrived
      first, so the answer is what a stable sort plus `truncate(limit)` gave. **`benches/query.rs`
      is the new bench and the evidence**, limit 100 over 202 000 rows in the window:

      | shape | dir | lines | bytes allocated | peak live | time |
      |---|---|---|---|---|---|
      | `label_only` | backward | 69 178 → **187** | 178 416 315 → **367 655** | 10.5 → **0.08** MB | 49.000 ms → **243.43 µs** |
      | `label_only` | forward | 6 750 → **1 003** | 165 841 034 → **8 031 426** | 10.2 → **2.89** MB | 31.377 ms → **2.0404 ms** |
      | `line_filter` | backward | 142 906 → **9 517** | 355 132 227 → **76 060 366** | 10.7 → **6.42** MB | 92.237 ms → **17.189 ms** |
      | `line_filter` | forward | 112 478 → **15 183** | 355 744 979 → **39 117 415** | 11.0 → **5.03** MB | 89.450 ms → **10.316 ms** |
      | `json_field` | backward | 200 250 → **3 130** | 703 107 130 → **43 949 524** | 26.8 → **5.81** MB | 308.78 ms → **13.067 ms** |
      | `json_field` | forward | 200 250 → **4 783** | 703 106 682 → **21 343 859** | 26.9 → **5.05** MB | 308.00 ms → **9.2568 ms** |

      `--bench part`: `scan_tenants/1` **−66.5 %**, `/16` −57.5 %, `/128` −12.5 %;
      `scan_filters/no_filter` **−75.3 %**, `line_filter_hit` −75.8 %, `exact_field_hit` −75.4 %. The
      unbounded full-part scan is flat — `scan_label_columns` −1.5 % / +0.4 % / +0.7 % and
      3163.5/3258.2/3330.4 bytes and 6.17/6.29/6.41 allocations per row against
      3166.7/3263.4/3337.4 and 6.19/6.32/6.45, peak live 30.96 MB against 30.97 — because a sink
      that has not filled holds a plain `Vec` and ranks nothing. `--bench memtable`:
      `query_cardinality/256` **−92.2 %**, `/8192` −82.2 %, `query_line_filter/contains` −93.3 %,
      `query_stream_depth/100` −61.0 %, `/2000` −41.0 %, `/50000` −2.6 % (the last is the
      whole-stream sort two items below, which this does not touch).
      **The gate margin widened and the surviving budget did not fall**
      ([`docs/MEMORY_BUDGET_GATE.md`](docs/MEMORY_BUDGET_GATE.md)): 2 GiB is `UNDER_BUDGET` at
      **78–83 %** across three runs against 90–96 %, the `--limit 8GiB` high-water is
      **1659 MiB / 0.81x** against 1913 / 0.93x, and 19 871–19 889 eps against 19 720–19 729 — but
      1792 MiB is still `OOM_KILLED`, at 94.4 %.
      **Regressed:** `part/write` +2.6 % with a byte-identical allocation table and +0.4 % on the
      same bench without this change, i.e. ~2 % of code layout in a crate that grew a module;
      `scan_filters/line_filter_pruned` +1.9 % at 583 ns, where every row group is pruned and
      constructing the sink is the only new work
  - [x] **The triple materialize-and-sort is gone with it.** `PartReader` and `PartRegistry` now push
        rows into the caller's sink instead of returning `Vec<StreamResult>`, so the only rows held
        anywhere are the `limit` in the executor's sink and the only sort is over those. The old
        `query_*` entry points remain as wrappers over a `TopKRows` sink, so tests, merge and the
        object-store restore probe kept their signatures. Merge reads `Row`s straight out of a
        `RowCollector` rather than through `StreamResult`s it immediately flattened
  - [ ] **The bed's own `data.stats` number moved less than the bench, and the reason is not the
        limit.** Reproducing the 30 000-row, 8-stream, 3-window seed dataset the 16 384-against-Loki's-1 251
        figure came from: at the matrix's own `limit` of **20 000** the figure is **unchanged** at
        16 384/16 384/13 616, because 20 000 over ~1 250 matching rows is a limit that never binds
        and therefore never bounds anything. At Grafana's default `limit=100` it is
        **16 384 → 11 072, 16 384 → 9 272, 13 616 → 5 528** for `json_field` and
        8 192 → 7 184, 8 192 → 5 376, 5 424 → 800 for `label_only`. Still 4.4–8.8x Loki's 1 251, and
        the residue is not the limit: Loki's index takes it to one stream's chunks, while a row group
        here interleaves all eight streams and every row of them is decoded and filtered. That is
        projection and predicate pushdown, the next two items, not this one
- [ ] **Projection pushdown.** `ProjectionMask` appears nowhere; `count_over_time({app="x"}[5m])` decodes every
      label column and the `structured_metadata` JSON blob. **This is now the largest remaining term on the
      shape the claim rests on**, measured: see the `data.stats` item above
- [x] **The Parquet footer is parsed once per part scan** instead of once per selected row group, which
      fell out of the streaming rewrite: the scan needs one handle it can clone per row group and per
      backward window, so hoisting `open_part_data` out of the loop was the way to get one.
      `PreadReader` is a `File` behind an `Arc` and `ArrowReaderMetadata` is an `Arc<ParquetMetaData>`,
      so the clones are refcounts. Caching it across *scans* on the reader is still open and is a
      different lifetime question
  - [x] **A backward scan no longer decodes a whole row group to reverse it.** Parquet reads forwards
        only, so a backward scan used to `collect()` every batch of the group and reverse the list —
        8192 rows of Arrow string arrays built to answer a `limit=100`, in the direction Grafana
        defaults to. It now reads the group in windows from its end with `with_offset`, doubling the
        window each time so a scan that does read the whole group pays O(group rows) in skipped
        records rather than one skip over the whole prefix per window
- [ ] Do not allocate the line before the filter that rejects it (`reader.rs:727` precedes `:728`)
- [ ] **Extract required literals from `|~` regexes** so trigram blooms apply. `bloom_prune` matches only
      `LineFilter::Contains` (`reader.rs:778-787`)
- [ ] Parallelize part scans within a query (`part_registry.rs:579` is sequential), and stop holding a scan
      permit across an object-store restore (`execution.rs:367` vs `:374`)
- [ ] Memtable query: binary-search the sorted stream instead of counting every entry against the scan budget,
      and stop sorting the whole stream on every query (`memtable.rs`, `scan_memtable_stream`). **The
      scan half is done** — the sink's frontier ends a stream at the first entry past it, which is
      `query_stream_depth/100` −61 % and `/2000` −41 % — and the sort is the whole of what is left,
      which is why `/50000` only moved 2.6 %
- [ ] Verify the trigram bloom's `to_lowercase()` on both sides (`bloom.rs:139-148`, `:57`) cannot produce a
      false negative for non-ASCII substring filters — a dropped result is a correctness bug, not a pruning miss
- [ ] Do not write an `.access` marker file per candidate part per query on the scan thread
      (`part_registry.rs`). Narrower than it was: a part the sink's frontier rejects is skipped before
      the marker is written, so a limited query now writes one per part it actually opens

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
      field, and absence is not indexed anywhere. The *naming* half is settled and
      no longer open: measured against `grafana/loki:3.3.2`, a `| json` field
      colliding with a stream label becomes `<name>_extracted` (both survive, both
      filter) and one colliding with a pushed structured-metadata key is discarded
      outright, which loggytracy now matches — see the `| json` entry under "Open
      correctness defects". What is left is one deliberate divergence: on a
      *second* collision Loki appends `_extracted` once more and overwrites the
      existing `foo_extracted` stream label, while loggytracy answers
      `foo_extracted_2` and keeps it, because it will not drop a value it was
      given. A name that could have been synthesized that way must therefore
      never drive a row-group prune, which is what
      `query::tests::synthesized_extracted_field_never_false_negative_prunes_parts`
      and `..._restores_an_evicted_part_conservatively` hold

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

- [ ] **`query_range`'s `end` is inclusive and Loki's is exclusive**, and **`| json` does not promote extracted
      fields into a log response's stream labels**. Both are tracked in "Open correctness defects" at the top of
      this file, which is the copy to keep current — these are wrong answers and do not belong on an API-surface
      wishlist

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
