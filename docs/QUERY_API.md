# The query API

This is the authoritative reference for loggytracy's first-party read
surface. It is written for both consumers at once: the fn0 console, and any
agent driving `curl`. The API is small on purpose — GET requests, URL query
parameters, NDJSON out — and every refusal names the offending input, what the
endpoint accepts, and a correct form, so a caller that only sees error text
can correct itself. A test (`every_query_api_route_and_param_is_documented`)
fails if a route or parameter exists in the code and not on this page.

The engine assumes a secured channel ([`DEPLOYMENT.md`](DEPLOYMENT.md) §5):
there is no TLS and no authentication in this process, and the tenant is
whatever the `X-Scope-OrgID` header says. The gateway in front — fn0's
control plane in the intended deployment — authenticates the caller and
**overwrites** that header.

Every example below works as written:

```sh
curl -sH 'X-Scope-OrgID: acme' \
  'http://127.0.0.1:3100/loggytracy/api/v1/logs?start=-1h&attr=level=error&contains=timeout&limit=50'
```

## Routes

| Method | Path | Answers |
|---|---|---|
| GET | `/loggytracy/api/v1/logs` | log rows matching the filters, newest first by default |
| GET | `/loggytracy/api/v1/logs/histogram` | per-bucket row counts under the same filters |
| GET | `/loggytracy/api/v1/logs/attributes` | attribute key names in the window (autocomplete) |
| GET | `/loggytracy/api/v1/logs/attributes/{key}/values` | a key's values in the window (autocomplete) |
| GET | `/loggytracy/api/v1/logs/tail` | a live chunked-NDJSON stream of new rows |
| POST/GET/DELETE | `/loggytracy/api/v1/logs/delete` | submit / list / cancel deletion requests |

Unchanged and outside this document's scope: `/metrics` (Prometheus text),
`/ready`, the admin routes under `/loggytracy/api/v1/admin`, and OTLP ingest.
A request to any other path answers 404 with this route list.

## The parameter grammar

One grammar serves every endpoint; each endpoint accepts the subset listed in
its section, and an unknown parameter is a 400 that names the accepted set.

| Parameter | Repeatable | Meaning |
|---|---|---|
| `start`, `end` | no | Absolute time — unix seconds (`1756100000`), milliseconds (13 digits), microseconds (16), nanoseconds (19), decimal seconds (`1756100000.123`), or RFC3339 (`2026-08-25T14:00:00+09:00`) — or relative: `-1h`, `-30m`, `-90s` is that long before now. The unit suffix is what makes it relative; a bare negative integer stays a negative epoch. Defaults: `end` = now, `start` = `end` − `LOGGYTRACY_MAX_QUERY_RANGE`. |
| `attr` | yes | An attribute filter with the operator embedded in the value: `attr=level=error`, `attr=level!=debug`, `attr=path=~/api/.*`, `attr=host!~db-.*`. The key ends at the first `!=`, `!~`, `=~`, or `=`. Regexes are anchored: the value must match whole. Repeated filters AND. |
| `contains` | yes | The line must contain this substring. |
| `not_contains` | yes | The line must not contain this substring. |
| `regex` | yes | The line must match this regex (unanchored, like `grep`). |
| `not_regex` | yes | The line must not match this regex. |
| `parse` | `json` and/or `logfmt`, once each | Run that parser over the stored line first; `attr` filters then see pushed attributes *and* extracted fields. One non-obvious rule, inherited from the pipeline: a pushed attribute **shadows** an extracted field of the same name. |
| `limit` | no | Rows to return (`/logs`) or per poll (`/logs/tail`). Default 100, capped by `LOGGYTRACY_MAX_LOG_LIMIT`. |
| `direction` | no | `forward` or `backward` (default) — ascending or descending time. |
| `bucket` | no | Histogram bucket width, duration syntax: `30s`, `5m`, `1h`. |
| `delay` | no | Tail only: whole seconds (≤ 5) to hold rows back, for writers whose clocks trail the server's. |
| `request_id` | no | Cancelling a delete request: the id from the GET listing. |

All filters AND. There is deliberately no OR, no attribute-exists, and no
numeric comparison — the flat model stays flat until a real consumer hits the
wall. Percent-encode values (`curl --get --data-urlencode` does it for you);
a practical URL stays far under the ~8 KB proxies allow.

## `GET /logs` — search

Accepts `start`, `end`, `attr`, `contains`, `not_contains`, `regex`,
`not_regex`, `parse`, `limit`, `direction`.

One row per line, `application/x-ndjson`, sorted in query direction:

```json
{"timestamp":"1756100000123456789","line":"request timed out","attributes":{"level":"error","service_name":"api"}}
```

`timestamp` is nanoseconds **as a string**: the values exceed 2^53, past
which JSON numbers silently lose precision in every JavaScript consumer.
Scan cost rides in response headers: `X-Loggytracy-Scanned-Rows`,
`X-Loggytracy-Scanned-Bytes`. The response is fully decided before the first
byte, so an error is always an HTTP status with a JSON body — never a broken
stream.

```sh
# Today's errors mentioning a user, oldest first
curl -sH 'X-Scope-OrgID: acme' --get \
  --data-urlencode 'start=2026-08-25T00:00:00+09:00' \
  --data-urlencode 'attr=level=error' \
  --data-urlencode 'contains=user_4711' \
  --data-urlencode 'direction=forward' \
  'http://127.0.0.1:3100/loggytracy/api/v1/logs'

# Structured search over JSON lines: parsed fields become filterable
curl -sH 'X-Scope-OrgID: acme' \
  'http://127.0.0.1:3100/loggytracy/api/v1/logs?start=-15m&parse=json&attr=status=500'
```

## `GET /logs/histogram` — the chart

Accepts `start`, `end`, `attr`, `contains`, `not_contains`, `regex`,
`not_regex`, `parse`, `bucket`.

Counts rows per bucket under the same filters `/logs` takes. Buckets are
half-open `[bucket_start, bucket_end)`, epoch-aligned to the width, and
clipped to the query range — the partial first and last buckets count only
in-range rows. Empty buckets are emitted, so the series is dense. Without
`bucket`, the smallest of 1s/10s/1m/10m/1h/1d keeping the count ≤ 100 is
chosen; the hard cap is `LOGGYTRACY_MAX_METRIC_EVALUATION_POINTS`.

```json
{"bucket_start":"1756100000000000000","bucket_end":"1756100030000000000","count":42}
```

`sum(count)` over the buckets equals the row count `/logs` would return for
the same filters and range (unlimited), and a test holds the two to it.

## `GET /logs/attributes` and `/logs/attributes/{key}/values` — autocomplete

Keys accept `start`, `end`. Values accept `start`, `end`, `attr`.

```json
{"key":"service_name"}
{"value":"api"}
```

Keys come from the memtable and the part metadata census. Values come from a
bounded sample — the newest 1000 rows in the window — not from a catalog:
rare or old values may be missing over a long range. The optional `attr`
filters on the values endpoint narrow the sample to matching rows, so a
filter chip dropdown offers values that actually co-occur with the chips
already placed. Line filters are not accepted here: values are sampled
without evaluating line content, and answering as though they were filtered
by it would be a silent approximation.

## `GET /logs/tail` — follow

Accepts `start`, `attr`, `contains`, `not_contains`, `regex`, `not_regex`,
`parse`, `limit`, `delay`.

A chunked NDJSON stream: rows in the same shape as `/logs`, ascending in
time, plus a keep-alive line every ~15 idle seconds:

```json
{"heartbeat":true}
```

A heartbeat is not data — skip any line carrying a `heartbeat` key. `start`
is the resume cursor: reconnect with the last timestamp you saw and nothing
is missed (a row you already have may repeat; rows are deduplicated per
connection, not across them). Nothing is ever dropped: a burst larger than
`limit` is delivered across later polls, so a slow reader falls behind
visibly rather than losing lines. On shutdown the stream ends cleanly —
reconnect. Concurrent tails are capped by
`LOGGYTRACY_MAX_CONCURRENT_TAILS`; past the cap the request is refused with
429 rather than queued.

```sh
curl -sNH 'X-Scope-OrgID: acme' \
  'http://127.0.0.1:3100/loggytracy/api/v1/logs/tail?attr=service_name=api&contains=ERROR'
```

The proxy in front must pass streaming responses through unbuffered and not
apply an idle timeout shorter than the heartbeat interval.

## `/logs/delete` — deletion requests

Deletion semantics — hide immediately, remove at the next rewrite — are in
[`RETENTION_DESIGN.md`](RETENTION_DESIGN.md). The surface:

- `POST /logs/delete?attr=app=api&contains=secret&start=-24h` — at least one
  `attr` filter and an explicit `start` are required; `end` defaults to the
  moment of submission. `parse` is not in this endpoint's grammar: deleting
  by a parsed field would change meaning whenever the parser does. Answers
  204.
- `GET /logs/delete` — the tenant's requests, one NDJSON line each. The
  `query` field is the persisted canonical form (percent-encoded flat
  filters) — resubmittable as-is.
- `DELETE /logs/delete?request_id=<id>` — withdraw a request whose rows are
  still only hidden. Answers 204, or 404 for an unknown id.

A tenant may hold a bounded number of requests at once; past it the POST
answers 429 with the bound. Requests persist across restarts in the
canonical form above. A stored request from before this format fails startup
loudly; the fix is to delete the stored object and re-submit.

## Errors

Refusals are `application/json`:

```json
{"error":"unknown parameter 'atr': this endpoint accepts start, end, attr, … — see docs/QUERY_API.md"}
```

| Status | Meaning | Retry? |
|---|---|---|
| 400 | The request is malformed or over-broad; the message names the input and the governing limit | No — fix the request |
| 401/403 | Tenant refusals from `X-Scope-OrgID` handling ([`MULTI_TENANCY_DESIGN.md`](MULTI_TENANCY_DESIGN.md)) | No |
| 404 | Unknown route (the body lists the real ones) or unknown delete request | No |
| 429 | Tenant query quota, tail cap, delete cap, or this instance's query memory pool is momentarily full | Yes — these clear on their own |
| 503 | Draining for shutdown, or a deletion could not be made durable | Yes, against the replacement instance |
| 504 | The query ran past `LOGGYTRACY_MAX_QUERY_RUNTIME` | Narrow the range or filters |

## Limits

Every knob that bounds a query — range, runtime, scanned rows and bytes,
memory, limits, tail count — is documented in
[`CONFIGURATION.md`](CONFIGURATION.md) under its `LOGGYTRACY_*` name; the
messages above cite the same names.
