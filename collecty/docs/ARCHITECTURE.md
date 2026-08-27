# collecty architecture

A per-machine OTLP collector written in Rust. It takes exports over a Unix
domain socket, writes them zstd-compressed to an append-only disk queue, and
ships them to signy in batches.

This document records what the collector *is* and why each decision was made.
There are no comments in the source, so every "why" that would have been one
lives here. [`CONFIGURATION.md`](CONFIGURATION.md) is the knob-by-knob reference.

## Decided choices

| Item | Decision |
|---|---|
| Deployment | One process per machine or one sidecar per pod; both are supported and neither is assumed |
| Ingest protocol | **OTLP over gRPC on a Unix domain socket**, all three signals |
| Payload handling | **Never decoded.** The bytes that arrive are the bytes that are stored and the bytes that are sent |
| Acknowledgement | After `write(2)`, before `fsync`. See "What an acknowledgement means" |
| Durability | Append-only segments, crc32 per record, periodic `fsync`, cursor on disk |
| When the queue is full | **Drop the oldest segment.** The application is never refused for lack of disk |
| Delivery | One batch in flight at a time, in queue order. A resend after a crash is **suppressed by signy**, not duplicated — see "Sending twice" |
| Transport to signy | `POST /signy/api/v1/collect` with `Content-Encoding: zstd`, one route for all three signals |
| Sender identity | Random 16 bytes made with the queue directory, kept in the cursor file. Not configurable, and not the hostname — it names the queue, not the machine |
| Tenancy | **None.** The tenant travels inside the payload as a resource attribute (obsy issue #9), so a collector that does not decode has nothing to do |
| Self-observation | `collecty_*` metrics encoded as OTLP and pushed through collecty's own queue, plus a periodic summary on stderr |
| Format versioning | **None.** Nothing on disk is versioned. A queue written by another build is deleted, not migrated |
| Transport security | **None, by design.** A Unix socket has no network to secure, and the hop to signy is expected to stay inside a trust boundary |

## The property the whole design rests on

Two independent formats both survive plain concatenation, and collecty is built
out of that coincidence.

**OTLP export requests concatenate.** `ExportLogsServiceRequest` carries exactly
one field, `repeated ResourceLogs resource_logs = 1`; the trace and metric
requests have the same shape. Protobuf's merge rule says parsing two serialized
messages back to back yields one message whose repeated fields are the
concatenation. So joining the serialized bytes of N exports **is** the merged
export — no decode, no re-encode, no field walking.

**zstd frames concatenate.** A zstd stream may hold any number of frames, and
decompressing the stream yields the concatenation of what each frame held. So
joining N independently compressed frames **is** one stream that decompresses to
the joined plaintext.

Only the first half is signal-specific: logs merge with logs, not with spans.
One queue holds all three, so each export is compressed **behind a five byte
header** naming its signal and its length, and the batch signy receives is a
sequence of those records rather than one anonymous run of bytes. Splitting them
apart again is a walk over the plaintext with no decoding in it; joining each
signal's payloads is then the same free merge as before.

```
export₁ (logs)   ──encode──▶ [L]bytes₁ ──zstd──▶ frame₁ ─┐
export₂ (traces) ──encode──▶ [T]bytes₂ ──zstd──▶ frame₂ ─┼─▶ frame₁‖frame₂‖frame₃
export₃ (logs)   ──encode──▶ [L]bytes₃ ──zstd──▶ frame₃ ─┘      (the body sent)
                                                          │
              signy: one decompress ──────────────────────┴─▶ [L]bytes₁[T]bytes₂[L]bytes₃
                     one walk, one decode per signal ───────▶ logs:   bytes₁‖bytes₃
                                                              traces: bytes₂
```

collecty compresses each export once, on arrival, and never touches it again.
It does not decode on the way in, does not merge structurally, and does not
recompress on the way out. The header is written before compression, so even it
costs nothing on the send path.

Both halves are pinned by tests in `src/wire.rs`
(`concatenated_frames_decompress_to_the_concatenated_records`,
`concatenated_export_requests_decode_as_one_merged_request`,
`a_batch_survives_both_layers_at_once`), and the whole path is pinned end to end
by signy's `one_batch_of_mixed_signals_lands_in_every_store_it_names`.

## Receiving

A gRPC server bound to a Unix socket, serving the three OTLP `Export` methods.
It uses a **passthrough codec** (`src/receive/codec.rs`) rather than the
generated prost services: the decoder hands back the message bytes as `Bytes`
and the encoder writes nothing. An empty body is a valid
`ExportLogsServiceResponse` with no `partial_success`, which is what a successful
export answers, so a response costs zero bytes and zero encoding work.

Because of this, `prost` and `opentelemetry-proto` are **not** on the receive
path at all. They appear at runtime only to *encode* collecty's own metrics
(see "Self-observation"), and in tests to prove the concatenation property.

Two ceilings guard memory:

- **Per request.** tonic's `max_decoding_message_size` refuses an oversized
  export with `OUT_OF_RANGE` *before* buffering the body. `Intake::accept` keeps
  the same check for callers that do not arrive over gRPC.
- **In flight.** A `Semaphore` whose permits are bytes. Each request acquires its
  own length and holds it until the record is on disk, so the memory a burst can
  reach is a declared number rather than a product of concurrency and size.

Compression and the queue append both run on a blocking thread. Compression is
the only meaningful CPU this process spends.

## What an acknowledgement means

**A `200` means the bytes reached the kernel, not the device.** `Queue::append`
does `write(2)` and returns; `fsync` happens on a timer (`COLLECTY_FSYNC_INTERVAL`,
1 s) and when a segment rolls.

This was chosen over acknowledging after `fsync`, and the reason is that it is
the option that leaves the **least** memory held anywhere:

- Acknowledging after `fsync` delays every response by a device sync. The
  application's exporter queue grows by exactly that delay, and collecty holds
  the in-flight request for the same span. Both sides pay.
- Acknowledging from an in-memory channel is no faster in practice and moves the
  bytes from the application's heap to collecty's — the same bytes, a different
  owner, and a channel-sized ceiling on top.
- Acknowledging after `write` is the only one where the bytes leave user memory
  entirely. Page cache is reclaimable, is not charged to either process's RSS,
  and a cgroup counts it as reclaimable rather than as pressure.

What that costs: a machine that loses power loses at most one `fsync` interval.
A collecty that crashes or is redeployed loses nothing, because the kernel still
holds what was written. The lost window belongs to a failure that takes the
application on that machine with it, and signy already accepts a loss window of
its own flush interval for the same class of event.

## The disk queue

One queue for every signal, under `{data_dir}/queue/`.

```
00000000000000000000.seg   append-only segment, rolls at COLLECTY_QUEUE_SEGMENT_BYTES
00000000000000000001.seg
cursor                     44 bytes: segment u64, offset u64, sequence u64, sender id, crc32
```

A record is a 12-byte header and a zstd frame:

| bytes | field | why |
|---|---|---|
| 0..4 | compressed length | frames the record |
| 4..8 | uncompressed length | a batch's ceiling is *uncompressed* bytes, because that is what signy admits against; knowing it without decompressing is what makes batch assembly free |
| 8..12 | crc32 of the frame | tells a torn tail from bit rot |

**The signal is inside the frame, not in this header.** Compressed, the frame
holds five more bytes in front of the payload — one signal tag and the payload's
length — and those are the bytes signy reads to split a batch apart. Putting the
tag here instead would have been cheaper to read locally and useless remotely:
the queue header never leaves this machine, and the sender would then have to
rebuild an index for signy out of bytes it is otherwise free to `memcpy`. So the
queue stays signal-agnostic and only the two ends of the wire know the tag.

**One frame per record rather than one stream per segment.** A streaming
compressor over a whole segment would compress better, but its output cannot be
cut at record boundaries, and cutting at record boundaries is exactly what
lets a batch be a slice and a poisoned record be isolated. The ratio is paid to
keep the byte range addressable.

**Recovery.** On open, the last segment is walked from its start and truncated at
the first record whose header does not fit, whose length runs past the file, or
whose crc does not match. Earlier segments are not walked: a corrupt record found
later, while reading, ends that segment and the reader moves to the next one.
Trusting a bad length field to find the next boundary would be worse than losing
the tail of one segment.

**The cursor is advisory.** It is overwritten in place with no rename, because
losing it costs a resend and a resend is cheap. A torn write fails its crc, and
a queue whose cursor will not load replays from the oldest record it still
holds.

**The sequence and the sender id share the cursor's fate.** The sequence counts
records handed out for sending, and signy remembers the highest it has stored
under the sender id. If the id survived a cursor that did not, the numbering
would restart under a name signy still held a high-water mark for, and every
record under that mark would be skipped as one it had already seen. So a cursor
that will not load takes the id with it: the queue comes back as a sender signy
has never heard of, and nothing is skipped.

**Numbers are handed out at read time, not at append time.** A segment dropped
under a full queue takes its records with it before they were ever numbered, so
there is no gap to explain. All the number has to be is monotonic, and reading
only ever moves forward.

**Drop-oldest works in whole segments.** Rewriting a file to remove its head is
expensive; unlinking is one syscall. If the cursor was inside the segment being
dropped it jumps to the start of the next one.

**Dropped records are counted in bytes, not in records.** Counting records would
mean walking every segment at startup to know how many each holds, which is a
full read of the backlog on every restart. Bytes are free and answer the question
an operator actually asks.

**Backlog and disk usage are separate numbers.** `collecty_queue_bytes` is what
the segments occupy; `collecty_queue_backlog_bytes` is what has not reached signy
yet. They differ because a delivered segment is not unlinked until a later one
exists, and only the second one means "signy is behind".

## Sending

One sender task, one batch in flight, in queue order. There is no
linger: if the queue has anything, it goes now. Batching is therefore automatic —
when signy is slow or down the backlog grows and the next batch is larger, which
is the only time a large batch is useful.

A batch is capped at `COLLECTY_BATCH_MAX_BYTES` **uncompressed** (64 MiB by
default) and `COLLECTY_BATCH_MAX_RECORDS` records. A single record that alone
exceeds the ceiling is still sent rather than blocking the queue behind it.

**The ceiling is this process's memory and nothing else.** It used to be half of
what signy would admit, because signy collected the whole body before it looked
at it. signy now decompresses and ingests a record at a time and never holds
more than one, so the only thing the number bounds is the frames kept here while
an attempt is out. A large batch only happens when the backlog is large, which is
exactly when saving round trips is worth the memory.

The request carries `Content-Encoding: zstd`, `x-collecty-sender` and
`x-collecty-start-sequence`. There is no declared-size header any more: signy
has nothing to admit a whole batch against.

### Sending twice

The batch body carries no numbers. It does not have to: the header names the
first record's number and signy counts as it reads, so record *i* of the body is
`start + i`.

signy remembers the highest number it has stored per sender, and skips anything
at or below it. The answer says how far it has got, and the cursor moves to
that.

That closes the window this design used to leave open. The cursor was written
after the answer arrived, so a collecty that died in between — or an answer lost
on the way back — resent a batch signy already had, and every record in it was
stored a second time. Logs collapsed on merge; spans and metric samples did not.
Now the resend arrives, is counted in `signy_collect_skipped_records_total`, and
stores nothing.

The same holds for a batch cut off halfway. Whatever reached signy is durable
with its number beside it, and the resend picks up from there rather than
starting again.

### How an answer is read

| Answer | Meaning | Action |
|---|---|---|
| 2xx | accepted and durable in signy; the body is `{"stored":n}` | advance the cursor to `n` |
| 400, 413, 415, 422 | this *batch* is not acceptable | isolate and drop |
| everything else, including 401/403/429/5xx and any connection failure | signy cannot take it *right now* | retry with backoff |

The default is **retry, not drop**. A `403` from a misconfigured tenant policy
must not destroy data, so only the four statuses that say "this body is wrong"
are treated as permanent. Backoff is 100 ms doubling to 30 s with jitter.

`n` is normally the last record's number and the whole attempt is committed. It
can be *higher* — signy got further on an earlier attempt than collecty heard
about — and then the next batch starts under it and is skipped there. If it is
lower, the records past it stay in the queue rather than being taken on trust.

A single bad *record* no longer reaches this table. signy splits a batch by
signal itself, and a signal it will never accept — an undecodable body, a tenant
at its storage limit — is dropped there, logged, counted in
`signy_collect_dropped_records_total`, and answered `200`. Sending it back would
only have collecty halve the batch to rediscover what signy already knew, one
round trip at a time, and drop it anyway.

**signy drops on every client error, including `403`.** That is wider than this
table, and the queue being shared is the reason. One application exporting under
a tenant signy does not serve would otherwise stop every other application's
logs, spans and metrics behind it on that machine — a mistake in one process
becomes an outage for the host. The same goes for a tenant at its storage limit,
whose `429` clears only when retention retires parts. Both are real data loss and
both are visible: a warning per drop and
`signy_collect_dropped_records_total`, which the runbook alerts on.

What still reaches the table as a refusal is a batch that is wrong as a *whole*:
not zstd, framing that does not add up, or more uncompressed bytes than signy
will hold — and that last one halving genuinely fixes. The statuses this table
calls retryable stay retryable, because they are answered before a batch is ever
split: a fenced or draining instance, and a gate that is behind.

### Poison isolation

Not decoding has one cost: a corrupt payload is only discovered when signy
refuses the batch that contains it, and a batch that always fails would block
the queue head forever.

So a permanent refusal halves the batch and retries the left half, narrowing
until one record is left; that record is dropped, counted in
`collecty_records_refused_total`, logged at error, and the cursor moves past it.
With signy dropping what it cannot decode, this path is now reached only by a
batch refused for its shape rather than its content, but it stays: it is the only
thing standing between a queue head that always fails and a queue that never
drains.
For eight records with the sixth poisoned, the attempts are 8, 4, 4, 2, 1, 3, 1,
2 — seven records delivered, one dropped, and this exact sequence is asserted in
`send::tests::a_refused_batch_is_halved_until_the_one_bad_record_is_dropped`.

**Almost nothing reaches this path any more.** signy reads a batch a record at
a time, so a record over any of its ceilings — bytes or `MAX_OTLP_LOG_RECORDS`
and its two siblings — is dropped there and answered `200` like any other
undecodable one. What is left for halving is a batch wrong in its framing, which
halving cannot fix either. It stays because it costs nothing and it is the only
thing standing between a queue head that always fails and a queue that never
drains.

## Self-observation

collecty encodes its own counters as an OTLP metrics export and pushes them
through its own queue, so they take the same path as everything else and need no
listening port. `opentelemetry-proto` is used rather than hand-written prost
structs on purpose: a hand-transcribed field number fails silently, because the
test that round-trips it decodes with the same wrong definition.

The cost is that while signy is unreachable, the metrics describing that outage
are stuck in the queue behind it. The periodic summary written to stderr exists
for exactly that window — it is the only channel that still works when nothing
else does.

## What is deliberately not built

- **A metrics endpoint.** Adding a port would mean a second surface to secure and
  operate, for numbers that already have a path.
- **Multiple in-flight batches.** Ordered delivery with one cursor is a single
  number to reason about and to recover. Throughput here is bounded by signy, not
  by the link.
- **A read or query API.** The queue is a pipe, not a store.
- **Payload transformation.** No attribute injection, no filtering, no sampling.
  All of it requires decoding, and not decoding is the point.
- **Tenancy.** See the decisions table.
- **TLS.** See the decisions table.

## Where this depends on signy

- `/signy/api/v1/collect` must exist, accept `Content-Encoding: zstd`, and read
  the five byte record header this document describes.
- signy must drop a record it will never accept rather than refusing the batch
  that carries it. collecty cannot tell one record from another without decoding,
  so a refusal it cannot act on becomes a halving search it should not have to
  run — and with one queue for the machine, a batch that can never be accepted
  blocks every application on it.
- signy must remember the number in `x-collecty-start-sequence` per sender and
  answer with how far it has got. Without that a resend is stored twice, which
  logs collapse on merge and spans and metric samples do not.
- signy must tolerate duplicates it does not catch. Its own recovery is still
  at-least-once across a flush boundary, so a restart there can replay records
  already in parts.
