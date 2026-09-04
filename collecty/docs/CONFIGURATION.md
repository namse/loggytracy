# collecty configuration

Every knob is an environment variable. There is no config file and no command
line. Defaults are in `src/config.rs`, and the process refuses to start on a
combination that cannot work rather than starting and failing later.

Sizes accept a plain byte count or a binary suffix (`512`, `64KiB`, `8MiB`,
`1GiB`). Durations require a unit (`500ms`, `30s`, `5m`, `2h`); a bare number is
rejected, because a number with no unit is a guess about which unit was meant.

## Where it listens and where it puts things

| Variable | Default | What it does |
|---|---|---|
| `COLLECTY_LISTEN_ADDR` | `127.0.0.1:4318` | Where applications export to, over OTLP/HTTP. The default answers only the machine it runs on; a sidecar or host daemon other containers export to needs `0.0.0.0:4318`, which the image sets |
| `COLLECTY_DATA_DIR` | `/var/lib/collecty` | Holds the queue, under `queue/`. **This directory is the only copy of an acknowledged export until signy takes it** — it must outlive the container |
| `COLLECTY_SIGNY_URL` | `http://127.0.0.1:3100` | Where batches go. Plain HTTP only |

There is no authentication and no TLS, so **the bind address is the whole of
the access control**. Binding a routable address publishes an endpoint that
takes anything anyone sends it; it belongs behind the same trust boundary as
the hop to signy.

Three paths are served, and nothing else: `POST /v1/logs`, `POST /v1/traces`
and `POST /v1/metrics`. The body must be an uncompressed OTLP protobuf export
request (`Content-Type: application/x-protobuf`). The JSON encoding and a
`Content-Encoding` are both refused with `415` — see
[`ARCHITECTURE.md`](ARCHITECTURE.md) for why neither can be stored.

**This is the stack's only ingest wire, not merely collecty's.** signy takes a
collecty batch and nothing else — its OTLP push routes and its OTLP gRPC
listener were removed — so OTLP/HTTP 1.1, protobuf, uncompressed is what an
exporter must be configured for. There is nowhere to send OTLP JSON, and
nowhere to send OTLP over gRPC.

**The export has to name its tenant**, in the `tenant.id` resource attribute
signy reads it from. collecty does not check this and cannot: checking would
mean decoding, and not decoding is the point. An export that names no tenant
signy serves is accepted here, queued, shipped, and dropped on arrival —
counted in signy's `signy_ingest_dropped_resources_total` and nowhere on this
side. Configure the exporting SDK, not collecty.

## What bounds memory

| Variable | Default | What it does |
|---|---|---|
| `COLLECTY_MAX_REQUEST_BYTES` | `16MiB` | Largest single export accepted. Matches signy's own ceiling so an export that collecty takes is one signy can take. Refused with `413` before the body is buffered when the request declares its length, and while reading when it does not |
| `COLLECTY_MAX_INFLIGHT_BYTES` | `64MiB` | Total bytes of exports being compressed and written at once. A request waits for room rather than being refused. Must be at least `COLLECTY_MAX_REQUEST_BYTES`, or a large export could never be admitted |

Resident memory is **not** roughly this ceiling. Measured, at 204 exports/s of
41 kB over 8 connections, the collector's anonymous footprint is about 18 MiB
while the declared ceiling is 64 MiB and the bytes actually in flight are under
a megabyte. [`MEMORY.md`](MEMORY.md) is the measurement and the three things it
found that this sentence used to get wrong:

- **It is a function of how many clients there are.** 18 MiB over 8
  connections, 72 MiB over 256, at the same byte rate. A body is read into
  memory before the in-flight gate charges it, so `COLLECTY_MAX_INFLIGHT_BYTES`
  bounds what is past the gate and not what is buffered before it.
- **A backlog that has drained leaves some of itself behind**, about 4 % of
  what passed through, so it is affected by how far behind signy has been even
  though the backlog itself is on disk.
- **The disk backlog is charged to a container's memory limit as page cache.**
  A collector in a 256 MiB container reached 256 MiB of `memory.current` with a
  190 MB queue and 106 MiB of heap. On a real filesystem those pages are
  reclaimable and the process survives; on tmpfs they are not and it is killed.
  **Size a container against `COLLECTY_QUEUE_MAX_BYTES`, not against the
  heap**, or set the queue budget to something the container can hold. The
  default queue budget is 1 GiB.

## What bounds disk

| Variable | Default | What it does |
|---|---|---|
| `COLLECTY_QUEUE_MAX_BYTES` | `1GiB` | The whole budget, shared by the three signals' queues rather than split between them. **This number is how long signy may be down before data is lost.** At 1 MB/s of compressed logs it is about 17 minutes |
| `COLLECTY_QUEUE_SEGMENT_BYTES` | `8MiB` | Segment close size, in compressed bytes, and therefore the unit of everything else: one request carries one segment, one `fsync` covers one, dropping under a full queue takes one at a time, and a cut delivery re-sends one. Smaller segments lose less per drop and cost more requests |

When `COLLECTY_QUEUE_MAX_BYTES` is reached the oldest segment is unlinked —
whichever signal it belongs to — and the application keeps running. Watch
`collecty_queue_dropped_bytes_total`: any movement means data was thrown away.

## What controls sending

| Variable | Default | What it does |
|---|---|---|
| `COLLECTY_SEGMENT_MAX_AGE` | `1s` | How long an open segment may keep collecting before it closes and becomes sendable. Nothing leaves the machine and nothing is on the device until a segment closes, so on a quiet host this is both the delivery latency and **the loss window for a power cut**. Each signal keeps its own age, so a quiet host closes up to three segments per interval rather than one |
| `COLLECTY_RETRY_INITIAL` | `100ms` | First backoff after signy declines |
| `COLLECTY_RETRY_MAX` | `30s` | Backoff ceiling. Doubling, with up to 25% jitter |
| `COLLECTY_SEND_TIMEOUT` | `30s` | How long one batch may wait for an answer before it counts as a retryable failure |

There is no linger and no minimum batch size: if the queue holds anything it is
sent immediately. Batches grow on their own when signy is slow, which is the only
time a larger batch helps.

## What it says about itself

| Variable | Default | What it does |
|---|---|---|
| `COLLECTY_TENANT` | unset | Which tenant collecty's **own** `collecty_*` metrics are filed under. The only tenant collecty has an opinion about: everything it forwards carries its own inside the payload, which collecty never decodes. **Unset, the metrics are not exported at all** — signy drops an export naming no tenant and says nothing back, so producing them would queue and ship bytes to be thrown away. The stderr summary runs either way. Validated at startup against `[a-zA-Z0-9_-]{1,64}`, the grammar signy parses one with, so a typo fails the process instead of turning every self-export into a silent drop |
| `COLLECTY_REPORT_INTERVAL` | `60s` | How often `collecty_*` metrics are produced and the stderr summary is written |
| `COLLECTY_ZSTD_LEVEL` | `3` | 1 to 22. See the measurement below before raising it |
| `COLLECTY_LOG_FORMAT` | `text` | `json` for a log a collector will read |
| `COLLECTY_LOG` | `info` | `tracing` filter, e.g. `collecty::send=debug` |

Metrics go through collecty's own queue and land in signy as ordinary OTLP. There
is no `/metrics` port. While signy is unreachable the metrics describing that are
queued behind it, which is why the stderr summary exists.

### The metrics

One sender and one set of counters across the three queues, so every family is a
single series with no attributes.

| Family | Kind | What it answers |
|---|---|---|
| `collecty_queue_bytes` | gauge | What the segments occupy on disk, which is also how far behind signy is — an answered segment is unlinked. **The one to alert on** |
| `collecty_queue_segments` | gauge | Segment count across the three signals, never below three: each holds an open segment of its own |
| `collecty_records_appended_total` | counter | Exports accepted from applications |
| `collecty_bytes_appended_total` | counter | Plain bytes accepted, before the segment compresses them. Against `collecty_bytes_sent_total` this is the ratio this host is achieving |
| `collecty_segments_sent_total` | counter | Segments signy accepted |
| `collecty_bytes_sent_total` | counter | Compressed bytes shipped |
| `collecty_segments_refused_total` | counter | Segments **dropped** because signy would not take them. Any movement is data loss. Records signy itself drops are counted on its side, in `signy_collect_dropped_records_total`, and resources it drops for their tenant in `signy_ingest_dropped_resources_total` — neither of which moves anything here, because signy answers `200` for the segment either way |
| `collecty_bytes_refused_total` | counter | Compressed bytes dropped the same way |
| `collecty_send_retries_total` | counter | Deliveries signy declined and that were retried |
| `collecty_queue_dropped_bytes_total` | counter | Bytes **dropped** because the queue was full. Any movement is data loss |
| `collecty_queue_dropped_segments_total` | counter | Segments unlinked while full |

These metrics are themselves an OTLP export, so they carry a tenant like any
other — `COLLECTY_TENANT`, above. Without it they are not produced at all,
because signy would drop them without saying so.

Records are counted where they arrive and not where they leave: what a segment
holds is inside its compression, and counting it on the way out would mean
decompressing every segment to learn a number nothing acts on.

## Refusals at startup

The process exits with status 2 and one line on stderr when:

- `COLLECTY_MAX_INFLIGHT_BYTES` is below `COLLECTY_MAX_REQUEST_BYTES`
- `COLLECTY_QUEUE_MAX_BYTES` is below `COLLECTY_QUEUE_SEGMENT_BYTES`
- `COLLECTY_QUEUE_MAX_BYTES` cannot hold a single `COLLECTY_MAX_REQUEST_BYTES` export
- `COLLECTY_ZSTD_LEVEL` is outside 1 to 22
- `COLLECTY_TENANT` is set to something signy would not parse as a tenant id
- a size or duration cannot be parsed

Each of these would otherwise start a process that refuses or destroys every
export it is given, which is worse than not starting.
