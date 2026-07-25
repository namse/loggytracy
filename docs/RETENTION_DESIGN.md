# Per-tenant retention design

Design record for per-tenant retention in loggytracy. Written to be
self-contained: a fresh context should be able to start implementing from this
document alone.

Status: **implemented.**

Closes the open question left by `MULTI_TENANCY_DESIGN.md` ("Where does a
tenant's tier come from?", lines 416-420) and supersedes the
`Partition on (tier, day)` row of its status table. Partitions stay on `day`.

## Why this exists

Retention today is one global `retention_period` applied to every tenant
(`retention.rs:73-78`). A platform needs per-tenant retention because retention
is a plan attribute.

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

## Decision

**Pull the tenant→retention map from a configured endpoint, and apply it at
deletion time.**

Because the decision happens when data is deleted rather than when it is
written, upgrades and downgrades are honoured automatically, with no header, no
journal change, and no partition-scheme change.

Three properties make this cheap:

1. **The tenant index already exists.** `meta.json` carries a
   `Vec<TenantSegment>` with each tenant's row-group range, `min`/`max`
   timestamps and row count (`part/metadata.rs:66`,
   `validate_tenant_segments` at `:247`). Deciding *who has expired in this
   part* reads local metadata only — **no object is downloaded to make the
   decision.**
2. **Logical and physical deletion are separated.** Users see retention
   enforced immediately; bytes are reclaimed lazily, when it is cheapest.
3. **Rewrites reuse merge.** Retention never writes a part itself.

## Policy source

### Contract

```
GET <LOGGYTRACY_TENANT_POLICY_URL>

200 OK
{
  "tenants": {
    "acme":    "30d",
    "hobby-1": "7d",
    "intern":  "infinite"
  }
}
```

Values are Prometheus-style durations (`7d`, `24h`, `90m`), or the literal
`"infinite"`.

**A tenant absent from the response has an unknown policy, and unknown means
keep.** Nothing is deleted for a tenant the control plane did not mention. This
is the central safety rule of the design: loggytracy never invents a deletion.

`"infinite"` and *absent* produce the same retention behaviour, but they are
tracked separately in metrics, so a control plane that silently drops a tenant
from its output is visible as a rising `tenant_policy_unknown_tenants` gauge
rather than as invisible unbounded storage.

`"0"` is a valid value and expires everything for that tenant. It is honoured
because it falls out of the arithmetic, not because deletion is a feature here
— see [Non-goals](#non-goals).

### Fetch behaviour

- Polled every `LOGGYTRACY_TENANT_POLICY_INTERVAL` from inside
  `retention_loop`. No new process, no new writer.
- On failure: keep the previous snapshot, skip physical deletion for that tick,
  increment an error counter. Storage cost is the only penalty, so the failure
  direction is safe.
- **Before the first successful fetch there is no snapshot, and no physical
  deletion happens at all.** A control plane that is down at boot cannot cause
  deletion.
- Staleness is exported as a gauge. A stale snapshot keeps being applied — it
  is the last known policy, and applying it is more correct than applying
  nothing. Staleness only delays how fast a downgrade takes effect.
- The health flag (`retention_healthy`) is **not** cleared by fetch failures.
  An unreachable control plane must not make the pod look unhealthy and get
  restarted; it only stops reclamation.

### Validation

The response is untrusted input.

- Every tenant key is re-validated through the existing `TenantId` allowlist
  (`tenant.rs`). A malformed key is dropped with a warning, not propagated into
  a path.
- Retention values are clamped to `LOGGYTRACY_MAX_TENANT_RETENTION` when set.
- Response body size is capped by `LOGGYTRACY_TENANT_POLICY_MAX_BYTES`; the
  request has a timeout.
- An optional auth header is sent, configured as a single `Name: value` string.
- A parse failure leaves the previous snapshot intact. A partially valid
  response is **rejected whole** rather than applied partially, so a truncated
  or half-written control-plane response cannot mark tenants as unknown and
  thereby change deletion behaviour. (Unknown means keep, so a partial apply is
  not dangerous — but it is confusing, and whole-or-nothing is easier to reason
  about.)

### Relationship to the existing global `retention_period`

Two modes, never mixed:

| `LOGGYTRACY_TENANT_POLICY_URL` | Behaviour |
|---|---|
| unset | Exactly today's behaviour: global `retention_period`, or no retention when it too is unset. |
| set | The snapshot is the sole authority. |

Setting **both** is a config validation error (`config.rs` `validate`). A
silently ignored retention setting is the worst possible outcome, so it fails
at startup instead.

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
- **Unknown tenant or no snapshot → no clamp** (fail-open). The control plane
  being down never breaks queries, only reclamation. Combined with the
  ingest path never consulting the endpoint, the control plane is entirely
  off the hot path: it can be down and both writes and reads are unaffected.

This is what makes lazy physical deletion acceptable — the data is already
invisible to the tenant before the bytes are gone.

### Physical: lazy, and always through merge

Each retention tick walks the `TenantSegment`s of every part and compares each
segment's `max_ts_ns` against that tenant's cutoff. Timestamps stay event-time,
matching today's `max_ts_ns < cutoff` semantics (`retention.rs:88`).

| Part state | Action | Cost |
|---|---|---|
| every segment expired | delete the part whole — the existing path, unchanged | free |
| some segments expired, part is a merge candidate anyway | merge drops those rows while rewriting | no extra I/O |
| some segments expired, merge would not pick it up | mark it merge-eligible so merge rewrites it | one rewrite |

**Retention never writes a part.** For the third case it makes the part
eligible for merge and lets `merge_once` do the write. Concretely, `merge_once`
(`merge/scheduler.rs:57-62`) skips groups smaller than
`merge_min_part_count`; a part carrying expired rows becomes a valid group of
one. Merge already reads rows and re-sorts them, so dropping expired rows is a
filter applied to the rows it has already loaded.

This keeps **one commit path** for part replacement. Merge's transaction
(`merge/transaction.rs`), its tombstone, and its manifest CAS are reused
unchanged, and no second crash-safety story has to be written or reviewed.

Cache invalidation also disappears as a problem: a rewritten part gets a new id
and the old one is unregistered, which is exactly the lifecycle merge already
drives.

To avoid rewriting a large part to reclaim a handful of rows, the third case
triggers only when the expired fraction exceeds
`LOGGYTRACY_RETENTION_REWRITE_THRESHOLD` (expired rows ÷ part rows, from the
tenant index — again, no download). Below the threshold the rows stay on disk,
invisible to queries, until the part is merged or expires whole.

### Cost

With three distinct retention values in play, a day partition is rewritten
about twice before being dropped whole, so total bytes written roughly triples.
Against the `MULTI_TENANCY_DESIGN.md` budget — storage around $0.0035 per
project per month, Class A well under that — this is immaterial at the scale
the plan targets. It should be re-checked if retention tiers proliferate.

## Traces

Trace parts carry the same tenant segments (`MULTI_TENANCY_DESIGN.md` status
table: "Trace part format — done"). Everything above applies symmetrically via
`TraceRegistry`; the retention loop already handles both registries
(`retention.rs:99-112`).

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `LOGGYTRACY_TENANT_POLICY_URL` | unset | Policy endpoint. Unset disables per-tenant retention entirely. |
| `LOGGYTRACY_TENANT_POLICY_INTERVAL` | 300s | Poll period. |
| `LOGGYTRACY_TENANT_POLICY_TIMEOUT` | 10s | Per-request timeout. |
| `LOGGYTRACY_TENANT_POLICY_AUTH_HEADER` | unset | `Name: value` sent with each poll. |
| `LOGGYTRACY_TENANT_POLICY_MAX_BYTES` | 8 MiB | Response body cap. |
| `LOGGYTRACY_MAX_TENANT_RETENTION` | unset | Clamp on any value from the endpoint. |
| `LOGGYTRACY_RETENTION_REWRITE_THRESHOLD` | 0.5 | Expired-row fraction that forces a rewrite. |

Follows the existing `env_duration` / `env_required_duration` helpers in
`config.rs:248-263` and is checked in `validate` alongside the other retention
settings (`config.rs:388-394`).

**HTTP client:** add `reqwest` with `default-features = false` and only the
features needed (`json`, and the TLS backend already pulled in by
`object_store`'s `aws` feature — check which one before choosing, to avoid
linking two TLS stacks).

## Metrics

Following `metrics.rs` conventions (monotonic counters plus the gauges added in
M7):

- `tenant_policy_fetch_success`, `tenant_policy_fetch_errors`,
  `tenant_policy_fetch_latency_ns`
- `tenant_policy_known_tenants` (gauge), `tenant_policy_unknown_tenants`
  (gauge — tenants with data but no policy)
- `tenant_policy_snapshot_age_seconds` (gauge)
- `retention_expired_rows_dropped`, `retention_parts_rewritten`

`/metrics` stays operator-facing and process-wide; no tenant labels here (see
`MULTI_TENANCY_DESIGN.md`, "Implementation status").

## Failure modes

| Situation | Result |
|---|---|
| Endpoint unset | Today's behaviour, unchanged. |
| Endpoint down at boot | No physical deletion; queries unclamped. Nothing is lost. |
| Endpoint down later | Last snapshot keeps applying to queries; reclamation pauses. |
| Tenant missing from response | Kept forever. Visible via `tenant_policy_unknown_tenants`. |
| Malformed response | Previous snapshot retained whole; error counter increments. |
| Tenant id fails allowlist | That entry dropped, warning logged; rest of the snapshot applies. |
| Retention shortened (downgrade) | Query clamp effective within one poll; bytes reclaimed at next merge. |
| Retention lengthened (upgrade) | Effective immediately for everything still on disk. |
| Data already deleted before upgrade | Gone. Retention lengthening cannot resurrect bytes — documented product behaviour. |

## Non-goals

- **Account deletion / immediate purge.** Deleting a specific tenant's data on
  demand is an operational responsibility outside loggytracy. A future feature
  may assist it; this design does not provide it. Sending `"0"` happens to
  expire a tenant's data through the normal path, but it is not an interface
  built for that purpose and carries no promptness guarantee.
- **Per-tenant quotas, throttling, tenant-labelled metrics** — step 5 of
  `MULTI_TENANCY_DESIGN.md`.
- **Durable usage accounting** — step 6.
- **Tier-based partitioning** — rejected above; the status table row should be
  updated to point here.

## Implementation checklist

- [x] `src/tenant_policy.rs` — `TenantRetention` (`Finite(Duration)` /
      `Infinite`), `PolicySnapshot` (map + fetched-at), parsing, `TenantId`
      re-validation, clamping.
- [x] Poller inside `retention::retention_loop`, on its own interval in the
      same `select!`; snapshot behind `RwLock<Option<Arc<PolicySnapshot>>>`.
- [x] `AppState` — holds `Arc<TenantPolicy>` so query handlers can read it.
- [x] `config.rs` — the seven variables above, plus the mutual-exclusion check
      against `retention_period`.
- [x] `Cargo.toml` — `reqwest` with `rustls-tls-native-roots` + `http2`, the
      exact feature set `object_store`'s `aws` feature already links.
- [x] `retention.rs` — per-tenant cutoff from segments; whole-part deletion
      keeps the existing path.
- [x] `merge/scheduler.rs`, `merge/selection.rs` — `select_groups` admits parts
      with expired rows as groups of one; expired rows are dropped after
      `read_all_rows_with_limit` and before the flush.
- [x] `query/handlers.rs` — range clamp; metric scan-start floor;
      `tempo/handlers.rs` trace lookup timestamp filter.
- [x] `metrics.rs` + `/metrics` rendering.
- [x] Tests:
      - unknown tenant is never deleted, in both memtable and flushed states
      - endpoint down at boot deletes nothing
      - downgrade hides data at query time before the bytes are gone
      - upgrade before expiry keeps data alive past the old cutoff
      - part with all tenants expired takes the free whole-delete path
      - part with one expired tenant is rewritten and the other tenant's rows
        survive with correct row groups and tenant index
      - malformed / oversized / unauthorised responses leave the snapshot intact
      - setting both `RETENTION_PERIOD` and `TENANT_POLICY_URL` fails validation
      - traces follow the same rules
- [x] Docs: `MULTI_TENANCY_DESIGN.md` status table and open questions,
      `ARCHITECTURE.md`, `todo.md`.

## What the implementation settled that the design left open

Four points where the code had to make a call the design did not spell out.

**Retention does not mark; merge decides.** The design has retention flag a
part as merge-eligible. The code has `merge_once` derive that itself from the
same policy snapshot (`merge/selection.rs` `select_groups`). Same outcome —
retention still never writes a part, and the rewrite still goes through merge's
one commit path — with no shared mutable set for two workers to race over.
`select_groups` also admits parts above `merge_max_part_rows`, which
`group_for_merge` never considers at all and which would otherwise never
reclaim anything.

**The rewrite trigger is coarser than the drop.** Eligibility is computed from
whole tenant segments (`expired_log_row_fraction`), so it stays a pure
`meta.json` read with no download. The drop applied during the rewrite is
per row against that row's tenant cutoff, which reclaims strictly more. The
threshold is a trigger heuristic; the filter is exact.

**Trace parts are never rewritten.** There is no trace merge, so the third row
of the physical-deletion table has no vehicle on the trace side: a trace part
is deleted whole when every tenant in it has expired, and otherwise waits.
Partially expired trace spans are still invisible — every Tempo handler applies
the floor. `TraceTenantSegment` also carries no timestamps, so a segment's
bound is taken from the `row_group_max_ts` entries it owns.

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
