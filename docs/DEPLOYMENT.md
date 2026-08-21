# Deploying loggytracy

A single machine, a single writer, Cloudflare R2 behind it, and a gateway in
front. Follow it top to bottom for a first deployment.

Settings are explained in [`CONFIGURATION.md`](CONFIGURATION.md); what to do
when something breaks is in [`RUNBOOK.md`](RUNBOOK.md). This document is the
part in between: how to get from nothing to a process that is running.

---

## 1. Object storage

The object store is where the data actually lives. The local disk is a cache
plus a WAL that has not been flushed yet.

**Create the bucket and turn on object versioning.** One manifest object holds
the complete part list; every part can survive and the catalog will still be
gone if that object is lost. Versioning is what makes that recoverable, and it
is not something the engine can do for you.

**Create an API token** scoped to that one bucket, with read and write. R2
tokens come with an access key id and secret in the S3 shape, which is what
`object_store` wants.

```
LOGGYTRACY_OBJECT_STORE_URL=s3://your-bucket/loggytracy
OBJECT_STORE_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
OBJECT_STORE_REGION=auto
OBJECT_STORE_CONDITIONAL_PUT=etag
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

`OBJECT_STORE_CONDITIONAL_PUT=etag` is not optional. Every commit is a
compare-and-swap on the manifest, and without conditional writes two commits
silently overwrite each other, which is data loss rather than an error. Startup
runs a preflight that writes a probe object and checks **that a write which
should be rejected is rejected**, and refuses to start otherwise — so if the
process comes up at all, conditional writes work on your provider. That check
is the only place this question can be answered, because it is the deployment
target itself answering it.

## 2. The disk

The data directory holds the WAL and the local cache. **Until a flush lands,
the WAL is the only copy of acknowledged data.**

Give it its own partition or volume rather than sharing the root filesystem.
That does not stop it filling — the engine refuses writes at
`LOGGYTRACY_MIN_FREE_DISK_BYTES` before it gets there — but it keeps a full data
directory from taking the operating system, the container runtime and the system
logs down with it. An engine that has stopped can still tell you it has stopped.

Rough size: `LOGGYTRACY_CACHE_MAX_BYTES` (10 GiB by default) plus
`LOGGYTRACY_MAX_WAL_BACKLOG_BYTES` (1 GiB) plus the free-space floor (2 GiB)
plus room to grow. 32 GiB is a comfortable starting point at a small tenant
count.

```
sudo useradd --system --home-dir /var/lib/loggytracy --create-home loggytracy
sudo install -d -o loggytracy -g loggytracy -m 0750 /var/lib/loggytracy
sudo install -d -o root -g loggytracy -m 0750 /etc/loggytracy
```

## 3. Memory

**Size on peak, not on idle.** Measured at 8000 events/s across 500 tenants,
resident memory idles near 15 MB and peaks near 850 MB within a minute of load
starting. That is live memory held while ingest, flush and merge overlap, and it
comes back when load stops. An instance sized from a quiet screenshot is sized
about fifty times too small. 4 GB of RAM is a sane floor.

## 4. The service

### systemd

`/etc/systemd/system/loggytracy.service`:

```ini
[Unit]
Description=loggytracy
After=network-online.target
Wants=network-online.target
# Restarting is always safe here — the disk stays put and the WAL replays. What
# is not safe is restarting forever: five failures in ten minutes means
# something a restart does not fix, so the unit stops and the alerting notices.
StartLimitIntervalSec=600
StartLimitBurst=5

[Service]
ExecStart=/usr/local/bin/loggytracy
User=loggytracy
Group=loggytracy

Environment=LOGGYTRACY_DATA_DIR=/var/lib/loggytracy
Environment=LOGGYTRACY_LOG_FORMAT=json
EnvironmentFile=/etc/loggytracy/loggytracy.env

# Shutdown force-flushes without a deadline, because the alternative to waiting
# is discarding acknowledged data. systemd's default is 90 seconds, which is a
# SIGKILL in the middle of that.
TimeoutStopSec=infinity
# Startup reads every part's metadata before serving, which is linear in part
# count.
TimeoutStartSec=600

Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Three settings carry the weight here.

`TimeoutStopSec=infinity` is the important one. On SIGTERM the process stops
accepting writes and force-flushes everything it has acknowledged; that is the
only thing standing between a planned restart and losing the last few seconds of
every customer's logs. The default 90 seconds cuts it off with a SIGKILL. If
that ever needs to be given up on, it is a person's call, not a timer's:
`kill -USR1` abandons the force-flush and exits non-zero, saying so in the log.

`Restart=always` is right for this shape of deployment. The process exits 1 in
two cases — an operator abandoned a force-flush, or another writer claimed the
object-store prefix — and both log "the WAL still holds unflushed data, do not
discard this disk". On a single machine the disk does not go anywhere, so
restarting on it replays the WAL and recovers. That warning is aimed at
orchestrators that move a workload to another node and leave the volume behind.

`StartLimitBurst=5` is what keeps that from becoming a loop. A wrong R2
credential fails the same way every time; after five tries in ten minutes the
unit gives up and stays failed, which is a state to alert on.

### Docker

```
docker run -d --name loggytracy \
  --restart unless-stopped \
  --stop-timeout 3600 \
  -v /var/lib/loggytracy:/var/lib/loggytracy \
  --env-file /etc/loggytracy/loggytracy.env \
  -p 127.0.0.1:3100:3100 -p 127.0.0.1:4317:4317 \
  ghcr.io/namse/loggytracy:latest
```

`--stop-timeout` matters even more here than under systemd: Docker's default is
**10 seconds**. With Compose the equivalent is `stop_grace_period: 1h`.

The image already sets `LOGGYTRACY_LOG_FORMAT=json` and binds `0.0.0.0`, which
is why the ports above are published to loopback only.

## 5. The gateway contract

The engine has no TLS and no authentication. It reads `X-Scope-OrgID` and
believes it. Everything that makes that safe lives in front of it.

- **Terminate TLS and authenticate at the gateway.** Only the gateway may reach
  the listener; bind it to loopback, or to a private interface with a firewall.
- **Overwrite the tenant header, never append it.** A customer that sends its
  own `X-Scope-OrgID` must not have it survive. Appending produces two header
  values and the engine reads the first one, which is the client's. In nginx:
  `proxy_set_header X-Scope-OrgID $verified_tenant;` replaces whatever arrived.
- **Set `LOGGYTRACY_ALLOWED_TENANTS`.** It is off by default, which means any
  string that reaches the listener becomes a tenant. With the list set, anything
  outside it gets 403 — a second line behind the gateway, for the day something
  reaches the port that should not have.

Verify it, rather than assuming it, once the gateway is up:

```
curl -H 'X-Scope-OrgID: someone-elses-tenant' https://your-gateway/loki/api/v1/push -d '{}'
```

The engine should see your own tenant, not that one.

## 6. Tenant policy and free-tier defaults

Set `LOGGYTRACY_TENANT_POLICY_TOKEN` to a long random string. It does two
things: it enables per-tenant retention, and it mounts the admin API. Without
it the admin routes do not exist at all rather than existing unauthenticated.

**Set defaults before opening a free tier.** A tenant the control plane has
pushed nothing for is a tenant nobody sold anything to, and every limit is
unbounded by default — the first such tenant decides how much disk and how much
write throughput everyone else gets.

`/etc/loggytracy/loggytracy.env`, in full:

```
LOGGYTRACY_OBJECT_STORE_URL=s3://your-bucket/loggytracy
OBJECT_STORE_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
OBJECT_STORE_REGION=auto
OBJECT_STORE_CONDITIONAL_PUT=etag
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...

LOGGYTRACY_LISTEN_ADDR=127.0.0.1:3100
LOGGYTRACY_OTLP_GRPC_ADDR=127.0.0.1:4317

LOGGYTRACY_TENANT_POLICY_TOKEN=<a long random string>
LOGGYTRACY_ALLOWED_TENANTS=acme,globex,initech
LOGGYTRACY_MISSING_TENANT_POLICY=reject

# Free tier: what a tenant gets before a plan is pushed for it.
LOGGYTRACY_DEFAULT_TENANT_INGEST_BYTES_PER_SECOND=262144
LOGGYTRACY_DEFAULT_TENANT_QUERY_SCAN_BYTES_PER_SECOND=16777216
LOGGYTRACY_DEFAULT_TENANT_MAX_STREAMS=1000
LOGGYTRACY_DEFAULT_TENANT_MAX_STORED_BYTES=1073741824
```

`MISSING_TENANT_POLICY=reject` is worth the line. With the default, a request
that arrives without a header is quietly filed under the default tenant — and if
the gateway ever stops setting the header, every customer's logs land in the
same place instead of failing loudly.

Then push a plan per tenant. Retention and every limit take effect immediately
and survive restarts; there is nothing to reload:

```
curl -X PUT https://your-gateway/loggytracy/api/v1/admin/tenants/acme/retention \
  -H "Authorization: Bearer $LOGGYTRACY_TENANT_POLICY_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"retention": "30d", "ingest_rate": "4MiB/s", "query_rate": "64MiB/s",
       "max_streams": 10000, "max_stored_bytes": "50GiB"}'
```

The body is the whole policy, not a patch: a field left out is cleared.

Read a tenant's usage — this is what a customer-facing dashboard shows:

```
curl https://your-gateway/loggytracy/api/v1/admin/tenants/acme/usage \
  -H "Authorization: Bearer $LOGGYTRACY_TENANT_POLICY_TOKEN"
```

`stored_bytes` against `max_stored_bytes` is the storage figure. Over the limit,
writes get 429 and nothing is deleted; the space returns on its own as retention
retires the oldest parts, and writes resume without anyone doing anything.

## 7. Monitoring

Scrape `/metrics` with vmagent or VictoriaMetrics itself:

```yaml
scrape_configs:
  - job_name: loggytracy
    static_configs:
      - targets: ['127.0.0.1:3100']
```

Add `node_exporter` too. The engine reports free space on its own data
directory (`loggytracy_data_dir_free_bytes`), but nothing else about the
machine.

Point Grafana at VictoriaMetrics as a Prometheus data source and build the
alerts there — Grafana Alerting evaluates and delivers in one piece, so there is
no vmalert and no Alertmanager to run. The full list of what is worth alerting
on is in [`RUNBOOK.md`](RUNBOOK.md); these four are the ones that get someone
out of bed:

| Alert | Expression | Why |
|---|---|---|
| Flush stopped | `increase(loggytracy_flush_errors_total[10m]) > 0 and increase(loggytracy_flush_success_total[10m]) == 0` | The worst state this engine has. The WAL grows until ingest is refused |
| Object store unreachable | `loggytracy_remote_healthy == 0` for 5m | Nothing becomes durable |
| Disk filling | `loggytracy_data_dir_free_bytes < 4e9` | Twice the refusal floor, so it is a warning and not a report |
| Instance down | absent target for 2m | Covers a crash loop that exhausted `StartLimitBurst` |

**One of these cannot be alerted from this machine.** If the VM dies, Grafana
dies with it and no alert is sent. Add an external dead man's switch — a
`healthchecks.io` ping from a cron on the box, or any uptime check pointed at
the gateway. It is the cheapest part of this document and the only thing
covering the failure that takes everything else down.

## 8. First boot

Watch the log. In order, it will tell you:

- the configured memory budget — check it against the machine's RAM
- `restored object-store manifest` with a generation and part count
- a warning if a listener bound loopback, which is expected here
- `loggytracy listening`

Then check that it is serving:

```
curl -sf http://127.0.0.1:3100/ready && echo ready
curl -s http://127.0.0.1:3100/metrics | grep -E 'remote_healthy|data_dir_free'
```

`/ready` returning 503 names the reason in its body. `remote_healthy 1` means
R2 answered, and the conditional-write preflight passing is implied by the
process having started at all.

Send a line through the gateway and read it back before calling it done.

## 9. Upgrading

The image is built and pushed by CI on every push to master, tagged by commit.
Pin to the commit tag; `latest` is for typing, not for deployments.

```
docker pull ghcr.io/namse/loggytracy:<sha>
sudo systemctl restart loggytracy     # or: docker stop --time 3600 && docker run ...
```

There is one instance, so a restart is a gap in ingest — clients hold their own
WAL across it and resend. What matters is that the old process is allowed to
finish its force-flush, which is what `TimeoutStopSec` above is for. Confirm it
did:

```
journalctl -u loggytracy | grep 'graceful shutdown complete'
```

**Never run two instances against the same object-store prefix.** The second one
claims the writer epoch and the first fences itself and exits 1, with unflushed
data still on its disk. That defence works, but recovering from it means
restarting the old instance on its own disk to drain it — see
[`RUNBOOK.md`](RUNBOOK.md).
