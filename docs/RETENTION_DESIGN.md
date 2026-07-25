# Per-tenant retention design

Design record for per-tenant retention in loggytracy. Written to be
self-contained: a fresh context should be able to start implementing from this
document alone.

Status: **the deletion side is implemented; the policy intake is being
replaced.**

- Everything from [Two-layer deletion](#two-layer-deletion) down is live code.
- [Policy intake](#policy-intake) specifies a **push** endpoint that replaces
  the polling loop the code has today. [Migration checklist](#migration-checklist)
  is the exact delta.

Closes the open question left by `MULTI_TENANCY_DESIGN.md` ("Where does a
tenant's tier come from?", lines 416-420) and supersedes the
`Partition on (tier, day)` row of its status table. Partitions stay on `day`.

## Why this exists

Retention was one global `retention_period` applied to every tenant. A platform
needs per-tenant retention because retention is a plan attribute.

The requirement that drove the design: **a tenant that upgrades or downgrades
should get the retention they pay for now, not the one that was in force when
the bytes were written.**

Any scheme that bakes retention into the write path fails that requirement,
because the write path only knows the tier at write time. That rules out the
obvious designs, so they are recorded here to keep a fresh context from
re-deriving them.

### Rejected: partition by tier or by expiry, tier supplied at ingest

Partition on `(tier, day)` — or, better, on the computed expiry day — and let
the client pass its retention in a request header. Deletion becomes a prefix
listing plus a free DELETE, and no rewrite is ever needed.

Rejected because retention is then **fixed at write time**. Upgrading from a
3-day plan to a 30-day plan leaves the last three days of data dying on the old
schedule. Making it retroactive requires either an event-driven backfill or a
rewrite at merge, which is exactly the cost this design was supposed to avoid.
It also requires a header (so a Loki-compatible deployment needs a fallback
ladder), a new `LGY4` journal framing to carry retention through crash replay
(the header is gone at replay time, same reason the tenant is framed today),
and a rewrite of the `part/metadata.rs:205` invariant that ties a partition
name to its data's day.

### Rejected: a separate GC process

Move retention out of loggytracy into a janitor process that reads the control
plane and rewrites objects.

Rejected on two grounds. loggytracy holds **one machine, one process** as a
design constraint. And the system rests on a single-writer assumption:
`operation_lock` (`part_registry.rs:25`) is an in-process `RwLock` and means
nothing across a process boundary, so a second writer would force a re-review
of every crash-safety invariant built in M5-M7 — temp-dir rename, merge
tombstones, startup cache reconciliation, manifest CAS contention, and orphan
attribution. Too much risk for one feature.

### Rejected: polling the control plane for the whole map

The first implementation polled `GET <url>` every five minutes and kept the
last successful response in memory. This is the code as it stands; the rest of
this section is why it is being removed.

Rejected because **the map holds relative durations, and a relative duration
means something different every time it is re-applied.** A map that says
`acme: 3d` deletes more data every hour it stays in force. A stale map is not a
frozen decision, it is an actively destructive one.

Concretely: the control plane goes down, a customer upgrades from `3d` to
`30d`, loggytracy cannot see the upgrade, and every retention tick keeps
deleting another slice of data the new plan should have kept. The exposure is
the length of the outage — hours or days — and loggytracy cannot even detect
the situation, because "I could not fetch" does not distinguish *the policy is
unchanged* from *the policy changed and I cannot see it*.

Polling puts the knowledge on the wrong side. Only the control plane knows
whether a policy changed, and only it can know whether loggytracy has been
told. Push moves the retry duty to the side holding that knowledge and bounds
the exposure to a retry rather than to an outage. A second, smaller reason: the
polled map lived in memory only, so a restart dropped it entirely.

### Rejected: absolute-cutoff delete commands with job tracking

Considered: drop the retention concept from loggytracy altogether and expose
`delete tenant X's data older than <absolute timestamp>`, returning a job id,
with a second endpoint reporting whether that job has finished.

The core of the idea is right. An absolute cutoff is idempotent, order-
independent, and safe to store indefinitely — precisely what a relative
duration is not. Per-tenant watermarks under a `max` merge would make retries
and reordering free.

It was rejected on the completion side. Physical deletion here is deliberately
lazy: a part is rewritten only when enough of it has expired, and a part that
has grown past `merge_max_part_rows` is never merged again. So a large part
holding a handful of expired rows is reclaimed by no existing path, and a job
defined as *the bytes are gone* would never complete. Making it complete means
rewriting large parts to reclaim a few rows on demand — the exact cost the lazy
design exists to avoid — plus durable job records reconciled across restarts.

Best-effort retention needs none of that. Revisit only if a completion
guarantee becomes a product requirement.

## Decision

**The control plane pushes one tenant's retention at a time. loggytracy
persists it before acknowledging, and applies it at deletion time.**

Two independent halves:

- **Intake is push, per tenant, durable.** loggytracy never calls out. The
  control plane learns immediately whether its change took effect, and owns the
  retry when it did not.
- **Application is at deletion time, not write time.** Upgrades and downgrades
  are honoured automatically, with no header, no journal change, and no
  partition-scheme change.

Three properties make the second half cheap:

1. **The tenant index already exists.** `meta.json` carries a
   `Vec<TenantSegment>` with each tenant's row-group range, `min`/`max`
   timestamps and row count (`part/metadata.rs:66`,
   `validate_tenant_segments` at `:247`). Deciding *who has expired in this
   part* reads local metadata only — **no object is downloaded to make the
   decision.**
2. **Logical and physical deletion are separated.** Users see retention
   enforced immediately; bytes are reclaimed lazily, when it is cheapest.
3. **Rewrites reuse merge.** Retention never writes a part itself.

## Policy intake

### Contract

```
PUT /loggytracy/api/v1/admin/tenants/{tenant}/retention
Authorization: Bearer <LOGGYTRACY_TENANT_POLICY_TOKEN>
Content-Type: application/json

{"retention": "30d"}

200 OK    the policy is durable and in force
400       malformed tenant id or retention value; nothing stored
401       missing or wrong token
503       could not be persisted; the control plane must retry
```

```
GET    /loggytracy/api/v1/admin/tenants/{tenant}/retention
       200 {"tenant":"acme","retention":"30d","updated_at":"<rfc3339>"}
       404 no policy for this tenant

DELETE /loggytracy/api/v1/admin/tenants/{tenant}/retention
       200 the tenant returns to *unknown*, which keeps its data forever
```

Values are Prometheus-style durations (`7d`, `24h`, `90m`), the literal
`"infinite"`, or `"0"` (see [Tenant deletion](#tenant-deletion)).

**One tenant per request. There is no bulk endpoint.** A bulk write would be a
read-modify-write over shared state, and a partially applied or reordered bulk
push is exactly the failure the per-tenant shape makes impossible.

**A tenant that was never pushed has an unknown policy, and unknown means
keep.** Nothing is deleted for a tenant the control plane has not mentioned.
This is the central safety rule of the design: loggytracy never invents a
deletion.

`"infinite"` and *never pushed* produce the same retention behaviour, but they
are tracked separately in metrics, so a control plane that silently stops
managing a tenant is visible as a rising `tenant_policy_unknown_tenants` gauge
rather than as invisible unbounded storage.

### Durability

A push is acknowledged only once the policy is durable. This is the whole point
of the push shape: a `200` is a promise that the policy survives a restart, so
the control plane's retry loop terminates on a real guarantee.

- **One object per tenant**, at `<prefix>/tenant_policies/<tenant>.json`,
  holding `{"retention":"30d","updated_at":"<rfc3339>"}`.
- One object per tenant makes a push a single blind write: no read-modify-write,
  no CAS, no contention between two tenants updated concurrently. It is the
  storage-level reason the contract is per tenant.
- With no object store configured (`LOGGYTRACY_OBJECT_STORE_URL` unset) the
  same files live under `<data_dir>/tenant_policies/`, written temp-file-then-
  rename like the rest of the local state.
- **Startup loads every object under the prefix** into the in-memory map, and a
  failed load is **fatal at boot** — the same class of failure as a manifest
  that cannot be read. Booting with a silently empty map would unclamp every
  query and hand back data a downgrade had already hidden.
- The in-memory map serves every read; the objects are the truth restored at
  boot. A push updates the map only after the write succeeds.

### Validation

The request body is untrusted input.

- The tenant id is re-validated through the existing `TenantId` allowlist
  (`tenant.rs`). A malformed id is a `400`, never propagated into a path.
- A malformed retention value is a `400`. Nothing partial can be stored, because
  one request carries one tenant.
- Values are clamped to `LOGGYTRACY_MAX_TENANT_RETENTION` when set.
- The bearer token is compared in constant time. With no token configured the
  routes are not mounted at all.

### Relationship to the existing global `retention_period`

Two modes, never mixed:

| `LOGGYTRACY_TENANT_POLICY_TOKEN` | Behaviour |
|---|---|
| unset | Exactly the old behaviour: global `retention_period`, or no retention when it too is unset. The admin routes do not exist. |
| set | The pushed policies are the sole authority. |

Setting **both** the token and `retention_period` is a config validation error
(`config.rs` `validate`). A silently ignored retention setting is the worst
possible outcome, so it fails at startup instead.

## Two-layer deletion

### Logical: enforced immediately at query time

Every read path clamps the requested range to the tenant's retention:

```
effective_start = max(requested_start, now - retention(tenant))
```

- Applied in `query/handlers.rs` at the entry point, so logs, metric queries,
  `label_names`, `label_values`, `series` and `stats` all inherit it.
- Trace lookup by `trace_id` has no range, so spans older than the cutoff are
  filtered by timestamp instead.
- **Unknown tenant → no clamp** (fail-open). Combined with the ingest path
  never consulting a policy, retention is entirely off the hot path.

This is what makes lazy physical deletion acceptable — the data is already
invisible to the tenant before the bytes are gone.

### Physical: lazy, and always through merge

Each retention tick walks the `TenantSegment`s of every part and compares each
segment's `max_ts_ns` against that tenant's cutoff. Timestamps stay event-time,
matching the original `max_ts_ns < cutoff` semantics (`retention.rs:88`).

| Part state | Action | Cost |
|---|---|---|
| every segment expired | delete the part whole — the existing path, unchanged | free |
| some segments expired, part is a merge candidate anyway | merge drops those rows while rewriting | no extra I/O |
| some segments expired, merge would not pick it up | it becomes a group of one, so merge rewrites it | one rewrite |

**Retention never writes a part.** For the third case `merge_once`
(`merge/scheduler.rs:57-62`) admits the part as a valid group of one. Merge
already reads rows and re-sorts them, so dropping expired rows is a filter
applied to rows it has already loaded.

This keeps **one commit path** for part replacement. Merge's transaction
(`merge/transaction.rs`), its tombstone, and its manifest CAS are reused
unchanged, and no second crash-safety story has to be written or reviewed.

Cache invalidation also disappears as a problem: a rewritten part gets a new id
and the old one is unregistered, which is exactly the lifecycle merge already
drives.

To avoid rewriting a large part to reclaim a handful of rows, the third case
triggers only once the expired fraction *reaches*
`LOGGYTRACY_RETENTION_REWRITE_THRESHOLD` (expired rows ÷ part rows, from the
tenant index — again, no download; the comparison is `>=`). Below the threshold
the rows stay on disk, invisible to queries, until the part is merged or
expires whole. A part that has grown past `merge_max_part_rows` is never merged
for size reasons, so rows below the threshold in a large part can survive
indefinitely. That is the accepted price of laziness, and the reason
job-tracked deletion was rejected.

A group of one can also turn out to be unreadable — a part above
`merge_max_memory_bytes` fails `read_all_rows_with_limit` on every tick, and no
retry changes that because the inputs never change. Such a group is counted in
`retention_rewrite_skipped` and skipped, **not** reported as a merge error:
holding `merge_healthy` low would take `/ready` to 503 over reclamation that
correctness never depended on, while the rows in question are already invisible
to queries. A group that reaches `merge_min_part_count` is work that has to
happen either way, so a read failure there still fails the tick.

### Cost

With three distinct retention values in play, a day partition is rewritten
about twice before being dropped whole, so total bytes written roughly triples.
Against the `MULTI_TENANCY_DESIGN.md` budget — storage around $0.0035 per
project per month, Class A well under that — this is immaterial at the scale
the plan targets. It should be re-checked if retention tiers proliferate.

## Tenant deletion

**Deleting a tenant is `retention: "0"`.** There is no separate purge endpoint,
and no job to track.

Zero retention puts the cutoff at *now*, so:

- every query for that tenant returns empty from the next request onward;
- parts holding only that tenant are deleted whole on the next retention tick;
- shared parts drop the tenant's rows the next time merge rewrites them.

Because reclamation is best-effort, the last point would otherwise carry no
bound at all — rows can survive in a large part indefinitely. So that the word
*deletion* means something, **a tenant at zero retention ignores
`retention_rewrite_threshold`**: any part still holding rows for it is eligible
for rewrite regardless of the expired fraction. That turns "never" into "the
next few merge ticks" without introducing job tracking.

It is still not a compliance-grade guarantee. loggytracy does not report when
the last byte is gone; see the rejected design above for what that would cost.

`DELETE` on the policy endpoint is **not** tenant deletion. It returns the
tenant to *unknown*, which keeps the data forever.

## Traces

Trace parts carry the same tenant segments (`MULTI_TENANCY_DESIGN.md` status
table: "Trace part format — done"). Everything above applies symmetrically via
`TraceRegistry`; the retention loop already handles both registries
(`retention.rs:99-112`).

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `LOGGYTRACY_TENANT_POLICY_TOKEN` | unset | Bearer token for the admin routes. Unset disables per-tenant retention entirely and the routes are not mounted. |
| `LOGGYTRACY_MAX_TENANT_RETENTION` | unset | Clamp on any pushed value. |
| `LOGGYTRACY_RETENTION_REWRITE_THRESHOLD` | 0.5 | Expired-row fraction that forces a rewrite. Ignored for tenants at zero retention. |

Checked in `validate` alongside the other retention settings
(`config.rs:388-394`).

Removed with the polling loop: `LOGGYTRACY_TENANT_POLICY_URL`,
`..._INTERVAL`, `..._TIMEOUT`, `..._AUTH_HEADER`, `..._MAX_BYTES`. The
`reqwest` dependency goes with them — nothing outbound remains.

## Metrics

Following `metrics.rs` conventions (monotonic counters plus the gauges added in
M7):

- `tenant_policy_push_accepted`, `tenant_policy_push_rejected`,
  `tenant_policy_push_persist_errors`
- `tenant_policy_known_tenants` (gauge), `tenant_policy_infinite_tenants`
  (gauge), `tenant_policy_unknown_tenants` (gauge — tenants with data but no
  policy). "With data" includes both memtables, not only the parts: a tenant
  that has just started pushing owns no `meta.json` segment until its first
  flush, which is exactly when a control-plane omission is newest.
- `tenant_policy_last_push_age_seconds` (gauge — age of the newest policy on
  this pod). It gates nothing; it tells an operator whether the control plane
  is still talking to this instance.
- `retention_expired_rows_dropped`, `retention_parts_rewritten`,
  `retention_rewrite_skipped` (retention-only merge groups that could not be
  read; a number that keeps rising means a part is permanently too large for
  `merge_max_memory_bytes`)

`/metrics` stays operator-facing and process-wide; no tenant labels here (see
`MULTI_TENANCY_DESIGN.md`, "Implementation status").

## Failure modes

| Situation | Result |
|---|---|
| Per-tenant retention disabled | The old global behaviour, unchanged. |
| No tenant has ever been pushed | Nothing is deleted; queries unclamped. |
| Store write fails during a push | `503`, nothing applied, nothing changed. The control plane retries. |
| Policy load fails at boot | Fatal; the process does not start. Same as an unreadable manifest. |
| Tenant never pushed | Kept forever. Visible via `tenant_policy_unknown_tenants`. |
| Control plane stops pushing | The last pushed policies stay in force forever. See [Accepted risks](#accepted-risks). |
| Retention shortened (downgrade) | Query clamp effective on the next request; bytes reclaimed at the next merge. |
| Retention lengthened (upgrade) | Effective immediately for everything still on disk. |
| Data already deleted before an upgrade | Gone. Retention lengthening cannot resurrect bytes — documented product behaviour. |
| Upgrade push delayed or lost | Data keeps being deleted at the old, shorter retention meanwhile. See [Accepted risks](#accepted-risks). |

## Accepted risks

**A delayed upgrade can cost data.** loggytracy keeps applying the last policy
it was told. If a tenant is upgraded from `3d` to `30d` and the push does not
land — loggytracy unreachable, the store failing the write, the control plane
backing off — then for as long as that lasts, retention and merge keep deleting
at `3d`. Data crossing the old cutoff during the window is gone, and the
upgrade cannot bring it back.

**This is accepted, not mitigated.** The exposure is bounded by the control
plane's retry rather than by an outage, because the control plane receives an
explicit failure and owns the retry: seconds to minutes in practice, and the
loss is the slice of data that crosses the old cutoff during that window. This
is the specific improvement over polling, where the same window was the entire
length of the outage and loggytracy could not even detect it.

The opposite direction is safe. A delayed *downgrade* costs storage only, and a
policy that never arrives is never invented — an unknown tenant keeps
everything.

If this ever needs closing: have the control plane re-push unchanged policies on
a heartbeat, then stop physical deletion when the newest policy is older than
some multiple of that heartbeat. It is deliberately not built now, because
without a heartbeat "no push" cannot be distinguished from "nothing changed",
so such a gate would have nothing to measure.

## Non-goals

- **Compliance-grade proof of deletion.** `retention: "0"` deletes a tenant and
  bounds reclamation, but loggytracy never reports that the last byte is gone.
  A guarantee would need job tracking — rejected above, with the reasons.
- **Per-tenant quotas, throttling, tenant-labelled metrics** — step 5 of
  `MULTI_TENANCY_DESIGN.md`.
- **Durable usage accounting** — step 6.
- **Tier-based partitioning** — rejected above; the status table row should be
  updated to point here.

## Migration checklist

### Already implemented, keep unchanged

- [x] `src/tenant_policy.rs` — `TenantRetention` (`Finite(Duration)` /
      `Infinite`), `Cutoffs`, duration parsing, `TenantId` re-validation,
      clamping.
- [x] `retention.rs` — per-tenant cutoff from segments; whole-part deletion
      keeps the existing path.
- [x] `merge/scheduler.rs`, `merge/selection.rs` — `select_groups` admits parts
      with expired rows as groups of one; expired rows are dropped after
      `read_all_rows_with_limit` and before the flush.
- [x] `query/handlers.rs` — range clamp; metric scan-start floor;
      `tempo/handlers.rs` trace lookup timestamp filter.
- [x] `AppState` — holds `Arc<TenantPolicy>` for the query handlers.
- [x] `/metrics` rendering of the retention counters and tenant gauges.
- [x] The per-tenant deletion tests in `retention.rs`, `merge/tests.rs` and
      `query/tests.rs`, and the trace-side clamp tests in `tempo/tests.rs`
      (lookup, search, both tag endpoints, and an unknown tenant on all four).

### To change

- [ ] Remove the poller from `retention::retention_loop`, plus
      `TenantPolicy::refresh`, `fetch_snapshot`, `parse_auth_header`, and the
      `reqwest` dependency.
- [ ] Replace `PolicySnapshot` (one whole-map snapshot with a single
      `fetched_at`) with a per-tenant map where each entry carries its own
      `updated_at`. Keep the `Arc<…>` read path — a push copies the map,
      inserts, and swaps, which is cheap because pushes are rare and keeps
      query reads lock-light.
- [ ] Add the three admin routes and bearer auth in `router.rs`, mounted only
      when the token is configured.
- [ ] Persist one object per tenant at `tenant_policies/<tenant>.json`, through
      `ObjectStorage` when configured and under `<data_dir>` otherwise. Update
      the in-memory map only after the write succeeds; return `503` when it
      fails.
- [ ] Load all policies at startup, before the workers spawn. Failure is fatal.
- [ ] `config.rs` — drop the five polling variables, add
      `LOGGYTRACY_TENANT_POLICY_TOKEN`, and re-key the mutual-exclusion check
      against `retention_period` on the token.
- [ ] `merge/selection.rs` — a tenant at zero retention ignores
      `retention_rewrite_threshold`.

### Tests to add

- [ ] a push is acknowledged only after the write lands; a failing store
      returns `503` and changes neither the map nor query behaviour
- [ ] policies survive a restart, and a tenant downgraded before the restart is
      still clamped after it
- [ ] a boot with an unreadable policy object fails instead of starting with an
      empty map
- [ ] `DELETE` returns a tenant to unknown: data kept, queries unclamped
- [ ] `retention: "0"` empties queries immediately and makes every part holding
      that tenant merge-eligible regardless of the threshold
- [ ] an unauthenticated or wrong-token push changes nothing
- [ ] a malformed tenant id or retention value is a `400` and stores nothing
- [ ] `LOGGYTRACY_MAX_TENANT_RETENTION` still clamps a pushed value
- [ ] setting both `RETENTION_PERIOD` and the token fails validation

### Docs

- [ ] `ARCHITECTURE.md` retention section, `MULTI_TENANCY_DESIGN.md` status
      table, `todo.md`.

## What the implementation settled that the design left open

Four points where the deletion side had to make a call the design did not spell
out. All four survive the move to push intake.

**Retention does not mark; merge decides.** The design has retention flag a
part as merge-eligible. The code has `merge_once` derive that itself from the
same policy state (`merge/selection.rs` `select_groups`). Same outcome —
retention still never writes a part, and the rewrite still goes through merge's
one commit path — with no shared mutable set for two workers to race over.
`select_groups` also admits parts above `merge_max_part_rows`, which
`group_for_merge` never considers at all and which would otherwise never
reclaim anything. That opens one path the old candidate filter made
impossible — such a part can be too large to materialize — so a group that
exists only for retention carries a flag and a read failure on it is counted
and skipped rather than failing the tick. See
[Physical: lazy, and always through merge](#physical-lazy-and-always-through-merge).

**The rewrite trigger is coarser than the drop.** Eligibility is computed from
whole tenant segments (`expired_log_row_fraction`), so it stays a pure
`meta.json` read with no download. The drop applied during the rewrite is
per row against that row's tenant cutoff, which reclaims strictly more. The
threshold is a trigger heuristic; the filter is exact.

**Trace parts are never rewritten.** There is no trace merge, so the third row
of the physical-deletion table has no vehicle on the trace side: a trace part
is deleted whole when every tenant in it has expired, and otherwise waits.
Partially expired trace spans are still invisible — every Tempo handler applies
the floor, in one of two shapes. `trace_by_id` and the two tag endpoints have
no range, so they drop spans one by one; `search` clamps its window, and since
a trace is placed by its own earliest span, a trace that *began* below the
floor leaves the results whole rather than appearing shortened. That is the
range semantics Tempo already has, and it errs toward hiding. All four are
covered in `tempo/tests.rs`. `TraceTenantSegment` also carries no timestamps,
so a segment's bound is taken from the `row_group_max_ts` entries it owns.

**Label endpoints clamp at part granularity.** `labels`, `label_values`,
`series` and `index_stats` have no range to clamp, so they take the floor
directly. MemTable entries are filtered per entry; parts are pruned per part,
because the stream index has no per-stream timestamps. A part that straddles
the floor therefore still contributes all of its label values. Finer pruning
would need time-partitioned stream postings, which is not worth it for these
endpoints.

**A fully clamped-away range returns empty, not an error.** When the whole
requested window is older than the tenant's retention, the clamped start passes
the end. The range is validated against the request the client actually made,
then clamped, and an empty `streams`/`matrix`/`vector` result is returned — a
downgrade must not turn previously valid queries into `400`s.
