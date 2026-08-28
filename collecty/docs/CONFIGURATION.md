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
| `COLLECTY_SOCKET_PATH` | `/run/collecty/otlp.sock` | The Unix socket applications export to. Parent directories are created. A stale socket file left by a killed process is replaced; a socket something is still listening on refuses the start |
| `COLLECTY_SOCKET_MODE` | `0666` | Socket permissions, octal. The default lets an application running as another user write, which is the ordinary case for a host daemon. Tighten it and control access with the directory's permissions instead |
| `COLLECTY_DATA_DIR` | `/var/lib/collecty` | Holds the queue, under `queue/`. **This directory is the only copy of an acknowledged export until signy takes it** — it must outlive the container |
| `COLLECTY_SIGNY_URL` | `http://127.0.0.1:3100` | Where batches go. Plain HTTP only |

The socket path has a hard limit the operating system sets, not collecty:
`sun_path` is 104 bytes on macOS and 108 on Linux, including the trailing
`NUL`. A path over it fails at bind, and the error names the path.

## What bounds memory

| Variable | Default | What it does |
|---|---|---|
| `COLLECTY_MAX_REQUEST_BYTES` | `16MiB` | Largest single export accepted. Matches signy's own ceiling so an export that collecty takes is one signy can take. Refused with `OUT_OF_RANGE` before the body is buffered |
| `COLLECTY_MAX_INFLIGHT_BYTES` | `64MiB` | Total bytes of exports being compressed and written at once. A request waits for room rather than being refused. Must be at least `COLLECTY_MAX_REQUEST_BYTES`, or a large export could never be admitted |

Resident memory is roughly this ceiling plus one batch buffer plus the
runtime. It is not affected by how far behind signy is — that backlog lives on
disk.

## What bounds disk

| Variable | Default | What it does |
|---|---|---|
| `COLLECTY_QUEUE_MAX_BYTES` | `1GiB` | One queue holds every signal, so this is the whole budget. **This number is how long signy may be down before data is lost.** At 1 MB/s of compressed logs it is about 17 minutes |
| `COLLECTY_QUEUE_SEGMENT_BYTES` | `8MiB` | Segment close size, in compressed bytes, and therefore the unit of everything else: one request carries one segment, one `fsync` covers one, dropping under a full queue takes one at a time, and a cut delivery re-sends one. Smaller segments lose less per drop and cost more requests |

When `COLLECTY_QUEUE_MAX_BYTES` is reached the oldest segment is unlinked and the
application keeps running. Watch `collecty_queue_dropped_bytes_total`: any
movement means data was thrown away.

## What controls sending

| Variable | Default | What it does |
|---|---|---|
| `COLLECTY_SEGMENT_MAX_AGE` | `1s` | How long an open segment may keep collecting before it closes and becomes sendable. Nothing leaves the machine and nothing is on the device until a segment closes, so on a quiet host this is both the delivery latency and **the loss window for a power cut** |
| `COLLECTY_RETRY_INITIAL` | `100ms` | First backoff after signy declines |
| `COLLECTY_RETRY_MAX` | `30s` | Backoff ceiling. Doubling, with up to 25% jitter |
| `COLLECTY_SEND_TIMEOUT` | `30s` | How long one batch may wait for an answer before it counts as a retryable failure |

There is no linger and no minimum batch size: if the queue holds anything it is
sent immediately. Batches grow on their own when signy is slow, which is the only
time a larger batch helps.

## What it says about itself

| Variable | Default | What it does |
|---|---|---|
| `COLLECTY_REPORT_INTERVAL` | `60s` | How often `collecty_*` metrics are produced and the stderr summary is written |
| `COLLECTY_ZSTD_LEVEL` | `3` | 1 to 22. See the measurement below before raising it |
| `COLLECTY_LOG_FORMAT` | `text` | `json` for a log a collector will read |
| `COLLECTY_LOG` | `info` | `tracing` filter, e.g. `collecty::send=debug` |

Metrics go through collecty's own queue and land in signy as ordinary OTLP. There
is no `/metrics` port. While signy is unreachable the metrics describing that are
queued behind it, which is why the stderr summary exists.

### The metrics

One queue and one sender, so every family is a single series with no
attributes.

| Family | Kind | What it answers |
|---|---|---|
| `collecty_queue_bytes` | gauge | What the segments occupy on disk, which is also how far behind signy is — an answered segment is unlinked. **The one to alert on** |
| `collecty_queue_segments` | gauge | Segment count |
| `collecty_records_appended_total` | counter | Exports accepted from applications |
| `collecty_bytes_appended_total` | counter | Plain bytes accepted, before the segment compresses them. Against `collecty_bytes_sent_total` this is the ratio this host is achieving |
| `collecty_segments_sent_total` | counter | Segments signy accepted |
| `collecty_bytes_sent_total` | counter | Compressed bytes shipped |
| `collecty_segments_refused_total` | counter | Segments **dropped** because signy would not take them. Any movement is data loss. Records signy itself drops are counted on its side, in `signy_collect_dropped_records_total` |
| `collecty_bytes_refused_total` | counter | Compressed bytes dropped the same way |
| `collecty_send_retries_total` | counter | Deliveries signy declined and that were retried |
| `collecty_queue_dropped_bytes_total` | counter | Bytes **dropped** because the queue was full. Any movement is data loss |
| `collecty_queue_dropped_segments_total` | counter | Segments unlinked while full |

Records are counted where they arrive and not where they leave: what a segment
holds is inside its compression, and counting it on the way out would mean
decompressing every segment to learn a number nothing acts on.

## Refusals at startup

The process exits with status 2 and one line on stderr when:

- `COLLECTY_MAX_INFLIGHT_BYTES` is below `COLLECTY_MAX_REQUEST_BYTES`
- `COLLECTY_QUEUE_MAX_BYTES` is below `COLLECTY_QUEUE_SEGMENT_BYTES`
- `COLLECTY_QUEUE_MAX_BYTES` cannot hold a single `COLLECTY_MAX_REQUEST_BYTES` export
- `COLLECTY_ZSTD_LEVEL` is outside 1 to 22
- a size or duration cannot be parsed

Each of these would otherwise start a process that refuses or destroys every
export it is given, which is worse than not starting.
