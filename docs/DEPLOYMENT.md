# Deploying loggytracy

A single machine, a single writer, Cloudflare R2 behind it, and a gateway in
front. Follow it top to bottom for a first deployment.

The machine needs Docker and nothing else — no toolchain, no account for the
service, no packages. Everything below is either a directory to create or a flag
to pass.

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
sudo install -d -o 10001 -g 10001 -m 0750 /var/lib/loggytracy
sudo install -d -o root -g root -m 0700 /etc/loggytracy
```

No account is created on the host, and 10001 is not a typo for a name. The image
runs as uid 10001, and a bind mount carries the host's numbers through
untranslated, so the directory has to be owned by that number or the first WAL
write fails with a permission error. Create it before the first `docker run`:
a bind-mount source that does not exist yet is created by the daemon as
root-owned, which fails the same way and looks like the image's fault.

`/etc/loggytracy` holds the environment file, and `--env-file` is read by the
docker CLI on the host rather than by the process in the container, so root-only
is enough and 10001 has no business reading it.

## 3. Memory

**Size on peak, not on idle.** Measured at 8000 events/s across 500 tenants,
resident memory idles near 15 MB and peaks near 850 MB within a minute of load
starting. That is live memory held while ingest, flush and merge overlap, and it
comes back when load stops. An instance sized from a quiet screenshot is sized
about fifty times too small. 4 GB of RAM is a sane floor.

## 4. The service

One process, one container, one command. Pull the tag you mean to run — `latest`
is for typing, not for deployments — and start it:

```
docker pull ghcr.io/namse/loggytracy:<sha>

docker run -d --name loggytracy \
  --restart unless-stopped \
  --stop-timeout=-1 \
  --log-driver json-file --log-opt max-size=100m --log-opt max-file=5 \
  --env-file /etc/loggytracy/loggytracy.env \
  -v /var/lib/loggytracy:/var/lib/loggytracy \
  -p 127.0.0.1:3100:3100 \
  -p 127.0.0.1:4317:4317 \
  ghcr.io/namse/loggytracy:<sha>
```

The image already sets `LOGGYTRACY_DATA_DIR=/var/lib/loggytracy` and
`LOGGYTRACY_LOG_FORMAT=json`, binds both listeners to `0.0.0.0`, and runs as uid
10001. What is left on the command line is the part the image cannot know: this
machine's disk, this deployment's credentials, and who is allowed to reach the
ports.

Four flags carry the weight here.

**`--stop-timeout=-1`.** On SIGTERM the process stops accepting writes and
force-flushes everything it has acknowledged; that is the only thing standing
between a planned restart and losing the last few seconds of every customer's
logs. Docker's default cuts it off with a SIGKILL after **10 seconds**, which is
the shortest deadline any of the usual supervisors sets. `-1` means no deadline —
the daemon waits for the container to exit — and it is set at creation, so a
plain `docker stop loggytracy` inherits it and nobody has to remember a flag at
the moment it matters. `docker stop --timeout=60` overrides it for one call,
which is a decision to lose data and should have to be typed like one. The equals
sign is there so the `-1` reads as a value rather than another flag; on a daemon
too old to take the indefinite form, a number of seconds longer than any
force-flush could plausibly need — `86400` — is the same bet with a worse
ending.

If a force-flush ever has to be abandoned, that is a person's call rather than a
timer's: `docker kill --signal=USR1 loggytracy` abandons it, exits non-zero and
says so in the log, where a SIGKILL says nothing at all.

**`--restart unless-stopped`.** The process exits 1 in two cases — an operator
abandoned a force-flush, or another writer claimed the object-store prefix — and
both log "the WAL still holds unflushed data, do not discard this disk". On a
single machine the disk does not go anywhere, so restarting on it replays the WAL
and recovers. `unless-stopped` also brings the engine back after a host reboot,
and leaves a container that was stopped on purpose stopped — including across a
reboot, which is what the fencing recovery in [`RUNBOOK.md`](RUNBOOK.md) depends
on. The other side of that: after a deliberate `docker stop`, `docker start
loggytracy` is a step someone has to take.

What Docker cannot say is "stop trying". A service manager can give up after,
say, five failures in ten minutes, on the grounds that whatever is wrong is not
something a restart fixes, and stay failed where the alerting can see it.
`on-failure:5` looks like that setting and is not one: Docker documents that the
policy does not restart the container when the daemon restarts, so a host reboot
would leave the engine down and nothing would bring it back. A restart loop is
the lesser failure, so it is allowed to loop, and **"Instance down" in §7 is what
notices it** rather than the restart policy. A wrong R2 credential fails the same
way every time and nothing here will stop trying.

**`--log-opt max-size`.** The default json-file driver does not rotate anything.
The engine's own logs would then grow without bound on the root filesystem — the
one place §2 worked to keep the data directory off — and a log engine that fills
a disk with its own logs is a poor advertisement. Put the same two options in
`/etc/docker/daemon.json` if this machine runs other containers, so it is the
daemon's default rather than something each `docker run` has to remember.

**`-p 127.0.0.1:...`.** Publishing a port inserts an iptables rule that is
evaluated before the host firewall's own chain, so `-p 3100:3100` is reachable
from the internet on a machine whose firewall is configured to deny it. Behind
that port there is no TLS and no authentication (§5). The address prefix is what
keeps a published port on loopback, and on this listener it is doing real work
rather than being tidy.

There is deliberately no `HEALTHCHECK`. Docker does not restart an unhealthy
container — only an orchestrator does — so the only thing it would change is a
word in `docker ps`, and `/ready` returns 503 in exactly the states where
restarting is the wrong answer: an object store that is down recovers on its own
and a restart only loses the WAL's head start. `/metrics` is where that judgment
belongs, and §7 is where it is made.

### Why not Compose

Because of the first flag. `stop_grace_period` takes a duration and has no
infinite form, so the setting that decides whether a planned restart is lossless
would become a number somebody guessed; `24h` is as close as it gets. Compose
earns its place by ordering containers that depend on each other, and here there
is one container that depends on nothing else on the machine. If a gateway or a
collector later moves onto the same box, that is when a compose file starts
paying for itself — and the grace period is the line to check first when it does.

### A host reboot is not a `docker stop`

The daemon stops running containers on its way down, but systemd bounds how long
the daemon itself may take, and past that bound the container is killed whatever
`--stop-timeout` says. So a planned reboot is the procedure in §9 first — stop
the engine, check the exit code — and `reboot` afterwards, followed by `docker
start loggytracy`, since a container stopped on purpose stays that way.

## 5. The gateway contract

The engine has no TLS and no authentication. It reads `X-Scope-OrgID` and
believes it. Everything that makes that safe lives in front of it.

- **Terminate TLS and authenticate at the gateway.** Only the gateway may reach
  the listener; bind it to loopback, or to a private interface with a firewall.
- **Overwrite the tenant header, never append it.** A customer that sends its
  own `X-Scope-OrgID` must not have it survive. Appending produces two header
  values and the engine reads the first one, which is the client's. In nginx:
  `proxy_set_header X-Scope-OrgID $verified_tenant;` replaces whatever arrived.
- **Set `LOGGYTRACY_TENANT_POLICY_TOKEN`** (§6). Without it any string that
  reaches the listener becomes a tenant. With it, the pushed policies are the
  tenant registry: anything the control plane has not onboarded gets 403 — a
  second line behind the gateway, for the day something reaches the port that
  should not have, and it never needs a restart to change.

Verify it, rather than assuming it, once the gateway is up:

```
curl -H 'X-Scope-OrgID: someone-elses-tenant' https://your-gateway/loki/api/v1/push -d '{}'
```

The engine should see your own tenant, not that one.

## 6. Tenant policy and free-tier defaults

Set `LOGGYTRACY_TENANT_POLICY_TOKEN` to a long random string. It does three
things: it enables per-tenant retention, it mounts the admin API, and it makes
the pushed policies the tenant registry — only tenants the control plane has
pushed a policy for are served. Without it the admin routes do not exist at
all rather than existing unauthenticated.

Onboarding a tenant *is* the policy push below: the moment the `PUT` answers
200, that tenant's requests are served, with no restart and nothing else to
call. Offboarding is the same API backwards — push `retention: "0"` to expire
the data, then `DELETE …/retention` to return the tenant to unknown, which
refuses its requests from then on. A deployment that files headerless requests
under the default tenant (`MISSING_TENANT_POLICY=default`) must push a policy
for that default tenant too; it is onboarded like any other.

**Set defaults before opening a free tier.** A push may carry only `retention`
and leave the limits out, and every omitted limit is unbounded by default — the
first such tenant decides how much disk and how much write throughput everyone
else gets. The `DEFAULT_TENANT_*` values below are what an onboarded tenant
gets for the fields its policy never named.

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
LOGGYTRACY_MISSING_TENANT_POLICY=reject

# Free tier: what an onboarded tenant gets for fields its plan never named.
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

List every tenant this instance serves, with the policy each one was pushed —
the reconciliation read for a control plane checking what it believes it
onboarded:

```
curl https://your-gateway/loggytracy/api/v1/admin/tenants \
  -H "Authorization: Bearer $LOGGYTRACY_TENANT_POLICY_TOKEN"
```

## 7. Monitoring

Scrape `/metrics` with vmagent or VictoriaMetrics itself:

```yaml
scrape_configs:
  - job_name: loggytracy
    static_configs:
      - targets: ['127.0.0.1:3100']
```

That target is the published port on the host. A scraper that is itself a
container does not see it — `127.0.0.1` there is the scraper's own loopback — so
put both on a user-defined network and scrape `loggytracy:3100`, which also means
the metrics port never has to be published at all.

Add `node_exporter` too. The engine reports free space on its own data
directory (`loggytracy_data_dir_free_bytes`), but nothing else about the
machine — including the root filesystem the container logs are written to.

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
| Instance down | absent target for 2m | Nothing stops a restart loop here, so this is the only thing that reports one |

**One of these cannot be alerted from this machine.** If the VM dies, Grafana
dies with it and no alert is sent. Add an external dead man's switch — a
`healthchecks.io` ping from a cron on the box, or any uptime check pointed at
the gateway. It is the cheapest part of this document and the only thing
covering the failure that takes everything else down.

## 8. First boot

Watch the log — `docker logs -f loggytracy`, which is the whole of it, since a
container that has never been removed keeps every line it has written. In order,
it will tell you:

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
docker stop loggytracy
docker inspect -f '{{.State.ExitCode}}' loggytracy
docker logs --tail=20 loggytracy | grep 'graceful shutdown complete'
docker rm loggytracy
docker run -d --name loggytracy ... ghcr.io/namse/loggytracy:<sha>
```

There is one instance, so a restart is a gap in ingest — clients hold their own
WAL across it and resend. What matters is that the old process was allowed to
finish its force-flush, which is what `--stop-timeout=-1` is for, and the two
lines in the middle are how you know it did. **Read them before `docker rm`.**
Removing the container deletes its logs and its exit code together, and they are
the only evidence that exists — a service manager writes to a journal that
outlives the process, and here nothing outlives the container. An exit code of 0
means every acknowledged line is durable. Anything else means it is not — **keep
the disk** and go to [`RUNBOOK.md`](RUNBOOK.md) rather than starting the new tag
on it.

The old container has to stop before the new one starts, in that order and not
the other way around, for the reason below.

**Never run two instances against the same object-store prefix.** The second one
claims the writer epoch and the first fences itself and exits 1, with unflushed
data still on its disk. That defence works, but recovering from it means
restarting the old instance on its own disk to drain it — see
[`RUNBOOK.md`](RUNBOOK.md).
