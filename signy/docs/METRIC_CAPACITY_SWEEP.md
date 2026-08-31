# Metric active-series capacity sweep

compare/run_metric_capacity.sh measures the largest one-shot metric
population accepted by each target under the same Docker memory and memswap
limit. It runs Signy, VictoriaMetrics, and Grafana Mimir separately. Each
candidate destroys the selected target's named volume before starting it, so a
previous candidate cannot make the next candidate pass or fail.

The default experiment uses the old comparison budget and searches from 10,000
to 2,000,000 requested burst series with a 10,000-series final uncertainty:

    cd signy
    COMPARE_MEMORY=2g compare/run_metric_capacity.sh

This is intentionally a long experiment. A short smoke run that validates the
argument logic without Docker is:

    CAPACITY_SWEEP_LOWER=100 \
    CAPACITY_SWEEP_UPPER=400 \
    CAPACITY_SWEEP_TOLERANCE=100 \
    CAPACITY_SWEEP_HOLD_SECONDS=1 \
    compare/run_metric_capacity.sh --dry-run

The same options are available as command-line arguments. For example:

    compare/run_metric_capacity.sh \
      --targets 'signy victoriametrics mimir' \
      --memory 2g --lower 10000 --upper 2000000 --tolerance 10000

If another comparison stack is using the default ports, isolate the run with a
Compose project name and alternate host ports:

    COMPOSE_PROJECT_NAME=signy-metric-capacity \
    SIGNY_PORT=3111 VICTORIAMETRICS_PORT=3141 MIMIR_PORT=3151 \
    compare/run_metric_capacity.sh --targets 'signy victoriametrics mimir'

The search exponentially ramps candidates (lower, lower*ramp_factor, ...)
until the first failing point, then binary-searches the bracket. A candidate
passes only if the harness observed 100% series, request, and datapoint acceptance,
the target stayed alive, it was not OOM-killed, and sampled anonymous memory
was strictly below that target's cgroup memory.max. A live 429 is recorded
as safe_saturation=true, but it is not a capacity pass.

Artifacts are written incrementally and are safe to resume:

- trials.jsonl: one complete record per target/candidate, including offered,
  accepted, and refused series/datapoints, HTTP status counts, anonymous
  peak, cgroup memory.peak, cgroup limit, alive/OOM state, elapsed time, and
  latency.
- trials.csv: the same flattened fields for spreadsheets.
- manifest.json: immutable experiment settings.
- state-<target>.json: the current exponential/binary bracket.
- <target>_<series>.json and logs: the raw load-harness result and stderr.

Existing target/candidate records are reused. If a run is interrupted, invoke
the same command and it will continue at the first missing candidate. Use a
new CAPACITY_SWEEP_OUT directory when changing memory, image versions,
workload knobs, or fairness assumptions.

The harness sends the exact same OTLP protobuf metric bodies to all targets.
Signy uses its collect framing; VictoriaMetrics uses
/opentelemetry/v1/metrics; Mimir uses the official /otlp/v1/metrics route
and a pinned grafana/mimir:3.1.4 monolith on local filesystem storage.
Signy's old SIGNY_MAX_ACTIVE_SERIES=500000 compose override is empty by
default, so this experiment exercises byte admission.

For fairness, the Mimir config explicitly disables non-memory cardinality and
rate guards: `max_global_series_per_user` and `max_global_series_per_metric`
are 0, while `ingestion_rate` and `ingestion_burst_size` are set to 1e9.
Mimir's documented defaults are conceptually 150000, 0, 10000/s, and 200000,
respectively. The single-process ingester also leaves `max_series` and
`max_ingestion_rate` at 0. These are benchmark settings, not recommended
production limits; see the [Mimir configuration parameters](https://grafana.com/docs/mimir/latest/configure/configuration-parameters/).
Block files and ingester TSDB files use separate directories in the fresh
`mimir-data` volume to avoid storage-role overlap.

This is an ingest capacity comparison, not a total product benchmark.
VictoriaMetrics and Mimir have different WAL/TSDB and background-flush
policies, Mimir's monolith is not a production cluster, and the local
filesystem, Docker image versions, CPU scheduling, compiler revision, request
batch size, scrape interval, and single-tenant setup all affect the number.
A one-shot burst also measures resident ingest capacity, not the number of
historical series retained on disk. Mimir is included for ingest only; its
query API is deliberately outside run_metrics.sh and this sweep. A result
within the binary-search tolerance is an interval, not an exact threshold.
