#!/usr/bin/env bash
#
# The metrics comparison bed (M14, issue #8): loggytracy beside
# VictoriaMetrics, same machine, same container memory limit, identical OTLP
# metric bodies at identical timestamps, the six fn0 shapes cold and warm.
#
#     compare/run_metrics.sh
#
# The same rules as run.sh, which this mirrors deliberately: every knob is a
# default rather than an assignment, there is no per-target code path to fork
# (only the METRICS_TARGETS list), and each phase writes its JSON whether or
# not a later phase likes it.
#
# **Phase 2 state (the ruler's iron half).** The default target list is
# VictoriaMetrics alone: the loggytracy metric read surface does not exist
# yet, and a bed that pretended otherwise would time 404s. The loggytracy
# column is absent by declaration, not by omission — Phase 8 flips the
# default to "loggytracy victoriametrics", adds the ingest-churn phases and
# the document generation (`docs/COMPARISON_METRICS.md` via compare_report),
# and the memory-limit sweep rides with them. Empirical pins this bed rests
# on, measured against victoria-metrics v1.150.0 on 2026-08-26: OTLP protobuf
# accepted at /opentelemetry/v1/metrics; datapoint attributes become labels
# verbatim; metric names pass through unchanged; OTLP explicit-bounds
# histograms are stored as `_bucket{le=}`/`_sum`/`_count` series with plain
# decimal `le` renderings ("0.005", "+Inf").
#
# The shape of the run:
#
#   1. Fresh volumes, the metric targets up at the same memory limit.
#   2. **Metric seed**, one target at a time: the fixed scrape grid, cumulative
#      counters, the churn block's replaced generations. See
#      src/bin/load/metric_workload.rs.
#   3. **Settle**: VictoriaMetrics is told to flush its in-memory parts (rows
#      are searchable before the flush, but the disk number is not a disk
#      number until it lands), then everything is left alone. Disk and the
#      ingest memory peak are read here.
#   4. **Restart all**, so the cold pass is a process that just started.
#   5. **Metric matrix**, one target at a time, six shapes, cold then warm,
#      full per-answer records for the two agreement classes.
#   6. Read the query-phase peaks and disk again, write bed_metrics.json.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT/compare"

OUT="${COMPARE_METRICS_OUT:-$ROOT/target/compare-metrics}"
mkdir -p "$OUT"

export COMPARE_MEMORY="${COMPARE_MEMORY:-2g}"
export LOGGYTRACY_PORT="${LOGGYTRACY_PORT:-3110}"
export VICTORIAMETRICS_PORT="${VICTORIAMETRICS_PORT:-3140}"
export LOGGYTRACY_BUILD_REVISION="${LOGGYTRACY_BUILD_REVISION:-$(git -C "$ROOT" rev-parse --short HEAD)}"
export LOGGYTRACY_BUILD_BRANCH="${LOGGYTRACY_BUILD_BRANCH:-$(git -C "$ROOT" rev-parse --abbrev-ref HEAD)}"

# Phase 8 flips this default to "loggytracy victoriametrics"; see the header.
METRICS_TARGETS="${COMPARE_METRICS_TARGETS:-victoriametrics}"
VICTORIAMETRICS_IMAGE="victoriametrics/victoria-metrics:v1.150.0"

port_of() {
  case "$1" in
    loggytracy) echo "$LOGGYTRACY_PORT" ;;
    victoriametrics) echo "$VICTORIAMETRICS_PORT" ;;
  esac
}
container_of() { echo "loggytracy-compare-$1-1"; }
ready_path_of() {
  case "$1" in
    victoriametrics) echo /health ;;
    *) echo /ready ;;
  esac
}
volume_of() { echo "loggytracy-compare_$1-data"; }

# The metric dataset. Defaults land at ~232k decomposed samples over an hour
# of scrape time — small on purpose while the bed is one-sided; the published
# run sizes these up alongside the Phase 8 churn phases.
METRIC_SCRAPES="${COMPARE_METRIC_SCRAPES:-360}"
METRIC_SCRAPE_SECONDS="${COMPARE_METRIC_SCRAPE_SECONDS:-10}"
METRIC_SERVICES="${COMPARE_METRIC_SERVICES:-8}"
METRIC_INSTANCES="${COMPARE_METRIC_INSTANCES:-4}"
METRIC_REPEATS="${COMPARE_METRIC_REPEATS:-5}"
METRIC_WINDOWS="${COMPARE_METRIC_WINDOWS:-3}"
METRIC_STEP_SECONDS="${COMPARE_METRIC_STEP_SECONDS:-30}"
SETTLE_SECONDS="${COMPARE_METRICS_SETTLE_SECONDS:-60}"
SEED="${COMPARE_SEED:-1592598566}"

# The scrape timestamps. Computed once and given to every run, for the same
# reason the log bed's anchor is; backdated well past VictoriaMetrics'
# -search.latencyOffset default (30s) so no query's window is trimmed by it.
METRIC_SPAN_S=$(( METRIC_SCRAPES * METRIC_SCRAPE_SECONDS ))
ANCHOR_S=$(( ( $(date +%s) - METRIC_SPAN_S - 300 ) / METRIC_STEP_SECONDS * METRIC_STEP_SECONDS ))
ANCHOR_NS=$(( ANCHOR_S * 1000000000 ))

say() { printf '\n=== %s ===\n' "$*" >&2; }

cgroup_of() {
  local pid
  pid=$(docker inspect -f '{{.State.Pid}}' "$1")
  echo "/sys/fs/cgroup$(cut -d: -f3 "/proc/$pid/cgroup")"
}

wait_ready() {
  local target=$1 tries=0
  local port path
  port=$(port_of "$target")
  path=$(ready_path_of "$target")
  until [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port$path")" = "200" ]; do
    tries=$((tries + 1))
    if [ "$tries" -gt 120 ]; then
      echo "$target never became ready" >&2
      docker compose --profile metrics logs --tail 40 "$target" >&2
      exit 1
    fi
    sleep 2
  done
}

declare -A CGROUP

read_cgroups() {
  local target
  for target in $METRICS_TARGETS; do
    CGROUP[$target]=$(cgroup_of "$(container_of "$target")")
  done
}

bring_up() {
  say "starting the metrics bed at ${COMPARE_MEMORY} per container"
  docker compose --profile metrics down -v --remove-orphans >/dev/null 2>&1 || true
  # Only the metric targets: the log bed's other containers have no business
  # sharing the cores this bed is measuring.
  docker compose --profile metrics up -d $METRICS_TARGETS
  local target
  for target in $METRICS_TARGETS; do
    wait_ready "$target"
  done
  read_cgroups
}

needs_build=false
for TARGET in $METRICS_TARGETS; do
  [ "$TARGET" = "loggytracy" ] && needs_build=true
done
if [ "$needs_build" = "true" ]; then
  say "building the loggytracy image"
  docker compose build loggytracy
fi
docker compose --profile metrics pull --ignore-buildable -q || true
bring_up

# Each system's own report of what it runs with, from the process itself.
for TARGET in $METRICS_TARGETS; do
  case "$TARGET" in
    victoriametrics)
      curl -s "http://127.0.0.1:$(port_of victoriametrics)/flags" -o "$OUT/victoriametrics_flags.txt"
      ;;
    loggytracy)
      docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \
        "$(container_of loggytracy)" >"$OUT/loggytracy_env.txt"
      docker compose logs --no-color --no-log-prefix loggytracy >"$OUT/loggytracy_startup.log" 2>&1 || true
      ;;
  esac
done

say "building the harness"
cargo build --manifest-path "$ROOT/Cargo.toml" --release --bin load
LOAD_BIN="$ROOT/target/release/load"

run_metric_phase() {
  local phase=$1 target=$2
  say "$phase: $target"
  LOGGYTRACY_LOAD_TARGET="$target" \
  LOGGYTRACY_LOAD_PHASE="$phase" \
  LOGGYTRACY_LOAD_ADDR="127.0.0.1:$(port_of "$target")" \
  LOGGYTRACY_LOAD_CGROUP="${CGROUP[$target]}" \
  LOGGYTRACY_LOAD_SEED="$SEED" \
  LOGGYTRACY_LOAD_METRIC_ANCHOR_NS="$ANCHOR_NS" \
  LOGGYTRACY_LOAD_METRIC_SCRAPES="$METRIC_SCRAPES" \
  LOGGYTRACY_LOAD_METRIC_SCRAPE_SECONDS="$METRIC_SCRAPE_SECONDS" \
  LOGGYTRACY_LOAD_METRIC_SERVICES="$METRIC_SERVICES" \
  LOGGYTRACY_LOAD_METRIC_INSTANCES="$METRIC_INSTANCES" \
  LOGGYTRACY_LOAD_METRIC_REPEATS="$METRIC_REPEATS" \
  LOGGYTRACY_LOAD_METRIC_WINDOWS="$METRIC_WINDOWS" \
  LOGGYTRACY_LOAD_METRIC_STEP_SECONDS="$METRIC_STEP_SECONDS" \
  LOGGYTRACY_LOAD_RESULT_PATH="$OUT/${phase}_$target.json" \
  LOGGYTRACY_BUILD_REVISION="$LOGGYTRACY_BUILD_REVISION" \
    "$LOAD_BIN" >/dev/null || echo "  ($phase verdict was not PASS for $target; the result file says why)" >&2
}

for TARGET in $METRICS_TARGETS; do
  run_metric_phase metric-seed "$TARGET"
done

say "settling for ${SETTLE_SECONDS}s"
# VictoriaMetrics buffers recent rows in in-memory parts; the flush makes the
# disk number a disk number. loggytracy flushes on its own cadence.
if [[ " $METRICS_TARGETS " == *" victoriametrics "* ]]; then
  curl -s "http://127.0.0.1:$(port_of victoriametrics)/internal/force_flush" >/dev/null || true
fi
sleep "$SETTLE_SECONDS"

disk_of() {
  docker run --rm -v "$(volume_of "$1"):/data:ro" busybox du -sb /data | cut -f1
}

declare -A DISK PEAK_INGEST PEAK_QUERY DISK_END OOM
for TARGET in $METRICS_TARGETS; do
  DISK[$TARGET]=$(disk_of "$TARGET")
  PEAK_INGEST[$TARGET]=$(cat "${CGROUP[$TARGET]}/memory.peak")
  PEAK_QUERY[$TARGET]=0
done

say "restarting everything so the cold query pass is cold"
docker compose --profile metrics restart $METRICS_TARGETS
for TARGET in $METRICS_TARGETS; do
  wait_ready "$TARGET"
done
read_cgroups

for TARGET in $METRICS_TARGETS; do
  run_metric_phase metric-matrix "$TARGET"
  THIS=$(cat "${CGROUP[$TARGET]}/memory.peak")
  [ "$THIS" -gt "${PEAK_QUERY[$TARGET]}" ] && PEAK_QUERY[$TARGET]=$THIS
done

for TARGET in $METRICS_TARGETS; do
  DISK_END[$TARGET]=$(disk_of "$TARGET")
  OOM[$TARGET]=$(docker inspect -f '{{.State.OOMKilled}}' "$(container_of "$TARGET")")
done

PEAK_JSON=""
DISK_JSON=""
TARGETS_JSON=""
for TARGET in $METRICS_TARGETS; do
  TARGETS_JSON="$TARGETS_JSON${TARGETS_JSON:+,}\"$TARGET\""
  PEAK_JSON="$PEAK_JSON${PEAK_JSON:+,}
    \"$TARGET\": { \"ingest\": ${PEAK_INGEST[$TARGET]}, \"query\": ${PEAK_QUERY[$TARGET]}, \"oom_killed\": ${OOM[$TARGET]} }"
  DISK_JSON="$DISK_JSON${DISK_JSON:+,}
    \"$TARGET\": { \"settled\": ${DISK[$TARGET]}, \"after_queries\": ${DISK_END[$TARGET]} }"
done

cat >"$OUT/bed_metrics.json" <<JSON
{
  "generated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "revision": "$LOGGYTRACY_BUILD_REVISION",
  "branch": "$LOGGYTRACY_BUILD_BRANCH",
  "machine": "$(uname -sr); $(nproc) logical CPUs; $(awk '/MemTotal/ {printf "%.1f GiB RAM", $2/1048576}' /proc/meminfo)",
  "docker": "$(docker version --format '{{.Server.Version}}')",
  "compose": "$(docker compose version --short)",
  "victoriametrics_image": "$VICTORIAMETRICS_IMAGE",
  "targets": [$TARGETS_JSON],
  "memory_limit": "$COMPARE_MEMORY",
  "settle_seconds": $SETTLE_SECONDS,
  "metric_anchor_ns": $ANCHOR_NS,
  "metric_scrapes": $METRIC_SCRAPES,
  "metric_scrape_seconds": $METRIC_SCRAPE_SECONDS,
  "object_store": "none: every system is on its local filesystem",
  "peak_bytes": {$PEAK_JSON
  },
  "disk_bytes": {$DISK_JSON
  }
}
JSON

echo "results: $OUT" >&2

# Every seed must have landed in full: a matrix over a partial dataset is not
# a measurement. The matrix verdict is not gated here while the bed is
# one-sided; Phase 8's document generation reads the shape errors instead.
for TARGET in $METRICS_TARGETS; do
  VERDICT=$(grep -o '"verdict": *"[A-Z_]*"' "$OUT/metric-seed_$TARGET.json" 2>/dev/null \
    | head -1 | grep -o '[A-Z_]*' | tail -1)
  if [ "$VERDICT" != "PASS" ]; then
    echo "metric-seed verdict for $TARGET is '${VERDICT:-missing}', not PASS; failing the bed" >&2
    exit 1
  fi
  say "seed gate: $TARGET PASS"
done
