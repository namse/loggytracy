#!/usr/bin/env bash
#
# The one-command reproduction of docs/COMPARISON.md.
#
#     compare/run.sh
#
# Everything it does is in this file, in order, and every knob is a default
# rather than an assignment so a caller can vary one without editing anything
# (the rule scripts/run_load_local.sh was fixed to follow in M8).
#
# The shape of the run, and why it is this shape:
#
#   1. Fresh volumes, both containers up at the same memory limit.
#   2. **Ingest**, one target at a time. Sequential rather than concurrent, so
#      neither system's throughput is a function of what the other was doing on
#      the same twelve cores. Both are stopped at the same *event target*, so
#      both end up holding the same number of entries — a run capped on time
#      would let the faster system hold more data and then be measured querying
#      it.
#   3. **Seed**, the fixed verification dataset, identical bytes at identical
#      timestamps on both sides. This is what the query numbers and the
#      row-equality check are taken over; see src/bin/load/matrix.rs.
#   4. **Settle**, equally: Loki is told to flush (its chunks otherwise sit in
#      the ingester for up to `max_chunk_age`, two hours, and a disk number
#      taken before that is not a disk number), then both are left alone for
#      SETTLE_SECONDS so loggytracy's flush and merge and Loki's compactor can
#      run. Disk and the ingest memory peak are read here.
#   5. **Restart both**, so "cold" in the query matrix means a process that has
#      just started rather than one whose caches the seeding filled.
#   6. **Matrix**, one target at a time, four shapes, cold then warm.
#   7. Read the query-phase memory peak and disk again, then generate the
#      document from the JSON.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT/compare"

OUT="${COMPARE_OUT:-$ROOT/target/compare}"
DOC="${COMPARE_DOC:-$ROOT/docs/COMPARISON.md}"
mkdir -p "$OUT"

export COMPARE_MEMORY="${COMPARE_MEMORY:-2g}"
export LOGGYTRACY_PORT="${LOGGYTRACY_PORT:-3110}"
export LOKI_PORT="${LOKI_PORT:-3120}"
export LOGGYTRACY_BUILD_REVISION="${LOGGYTRACY_BUILD_REVISION:-$(git -C "$ROOT" rev-parse --short HEAD)}"
export LOGGYTRACY_BUILD_BRANCH="${LOGGYTRACY_BUILD_BRANCH:-$(git -C "$ROOT" rev-parse --abbrev-ref HEAD)}"

# Ingest phase. The event target is the stop condition; the duration is only a
# cap, and a run that hits the cap instead is reported as such.
LOAD_SECONDS="${COMPARE_LOAD_SECONDS:-240}"
LOAD_EVENTS="${COMPARE_LOAD_EVENTS:-1200000}"
LOAD_EPS="${COMPARE_LOAD_EPS:-20000}"
LOAD_CONNECTIONS="${COMPARE_LOAD_CONNECTIONS:-8}"
LOAD_QUERY_EPS="${COMPARE_LOAD_QUERY_EPS:-5}"
# Verification dataset.
VERIFY_ROWS="${COMPARE_VERIFY_ROWS:-150000}"
VERIFY_STREAMS="${COMPARE_VERIFY_STREAMS:-32}"
MATRIX_REPEATS="${COMPARE_MATRIX_REPEATS:-5}"
MATRIX_WINDOWS="${COMPARE_MATRIX_WINDOWS:-3}"
SETTLE_SECONDS="${COMPARE_SETTLE_SECONDS:-150}"
SEED="${COMPARE_SEED:-1592598566}"

# The verification dataset's log timestamps. Computed once and given to both
# runs, because two runs deriving it from their own clocks would seed two
# different datasets and the row-equality check would fail for a reason that
# has nothing to do with either engine.
VERIFY_STEP_NS=1000000
MATRIX_STEP_SECONDS="${COMPARE_MATRIX_STEP_SECONDS:-10}"
VERIFY_SPAN_S=$(( VERIFY_ROWS * VERIFY_STEP_NS / 1000000000 ))
# Rounded down to a whole query step, so that the matrix's window boundaries
# land on the grid Loki aligns metric samples to and the last window is not
# truncated by the alignment. See `align_to_step` in src/bin/load/matrix.rs.
ANCHOR_S=$(( ( $(date +%s) - VERIFY_SPAN_S - 300 ) / MATRIX_STEP_SECONDS * MATRIX_STEP_SECONDS ))
ANCHOR_NS=$(( ANCHOR_S * 1000000000 ))

say() { printf '\n=== %s ===\n' "$*" >&2; }

cgroup_of() {
  local pid
  pid=$(docker inspect -f '{{.State.Pid}}' "$1")
  echo "/sys/fs/cgroup$(cut -d: -f3 "/proc/$pid/cgroup")"
}

wait_ready() {
  local port=$1 name=$2 tries=0
  until [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/ready")" = "200" ]; do
    tries=$((tries + 1))
    if [ "$tries" -gt 120 ]; then
      echo "$name never became ready" >&2
      docker compose logs --tail 40 "$name" >&2
      exit 1
    fi
    sleep 2
  done
}

bring_up() {
  say "starting the bed at ${COMPARE_MEMORY} per container"
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  docker compose up -d
  wait_ready "$LOGGYTRACY_PORT" loggytracy
  wait_ready "$LOKI_PORT" loki
  LT_CGROUP=$(cgroup_of loggytracy-compare-loggytracy-1)
  LK_CGROUP=$(cgroup_of loggytracy-compare-loki-1)
}

alive() {
  [ "$(docker inspect -f '{{.State.Running}}' "$1")" = "true" ]
}

say "building the images"
docker compose build
bring_up

# Loki's own report of what it is running with, captured from the process
# rather than from the file this repository wrote. `?mode=diff` is broken in
# 3.3.2 ("unsupported type <nil>"), so the full config and Loki's own defaults
# are both captured and the document diffs them.
curl -s "http://127.0.0.1:$LOKI_PORT/config" -o "$OUT/loki_config.yaml"
curl -s "http://127.0.0.1:$LOKI_PORT/config?mode=defaults" -o "$OUT/loki_config_defaults.yaml"
diff -U0 \
  <(sed 's/[[:space:]]*$//' "$OUT/loki_config_defaults.yaml") \
  <(sed 's/[[:space:]]*$//' "$OUT/loki_config.yaml") \
  >"$OUT/loki_config.diff" || true
curl -s "http://127.0.0.1:$LOKI_PORT/loki/api/v1/status/buildinfo" -o "$OUT/loki_buildinfo.json"
curl -s "http://127.0.0.1:$LOGGYTRACY_PORT/loki/api/v1/status/buildinfo" -o "$OUT/loggytracy_buildinfo.json"
# loggytracy's side of the same disclosure: the container's environment is its
# whole configuration surface, and it logs the derived numbers at startup.
docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \
  loggytracy-compare-loggytracy-1 >"$OUT/loggytracy_env.txt"
docker compose logs --no-color --no-log-prefix loggytracy >"$OUT/loggytracy_startup.log" 2>&1 || true

run_load() {
  local target=$1 port=$2 cgroup=$3
  say "ingest: $target"
  LOGGYTRACY_LOAD_TARGET="$target" \
  LOGGYTRACY_LOAD_PHASE=load \
  LOGGYTRACY_LOAD_ADDR="127.0.0.1:$port" \
  LOGGYTRACY_LOAD_CGROUP="$cgroup" \
  LOGGYTRACY_LOAD_TIER=compare \
  LOGGYTRACY_LOAD_SEED="$SEED" \
  LOGGYTRACY_LOAD_SECONDS="$LOAD_SECONDS" \
  LOGGYTRACY_LOAD_EVENTS="$LOAD_EVENTS" \
  LOGGYTRACY_LOAD_TARGET_EPS="$LOAD_EPS" \
  LOGGYTRACY_LOAD_CONNECTIONS="$LOAD_CONNECTIONS" \
  LOGGYTRACY_LOAD_QUERY_EPS="$LOAD_QUERY_EPS" \
  LOGGYTRACY_LOAD_OTLP_EPS=0 \
  LOGGYTRACY_LOAD_RESULT_PATH="$OUT/load_$target.json" \
  LOGGYTRACY_BUILD_REVISION="$LOGGYTRACY_BUILD_REVISION" \
    "$LOAD_BIN" >/dev/null || echo "  (load verdict was not PASS for $target; the result file says why)" >&2
}

run_verify() {
  local phase=$1 target=$2 port=$3 cgroup=$4
  say "$phase: $target"
  LOGGYTRACY_LOAD_TARGET="$target" \
  LOGGYTRACY_LOAD_PHASE="$phase" \
  LOGGYTRACY_LOAD_ADDR="127.0.0.1:$port" \
  LOGGYTRACY_LOAD_CGROUP="$cgroup" \
  LOGGYTRACY_LOAD_SEED="$SEED" \
  LOGGYTRACY_LOAD_VERIFY_ANCHOR_NS="$ANCHOR_NS" \
  LOGGYTRACY_LOAD_VERIFY_ROWS="$VERIFY_ROWS" \
  LOGGYTRACY_LOAD_VERIFY_STREAMS="$VERIFY_STREAMS" \
  LOGGYTRACY_LOAD_VERIFY_STEP_NS="$VERIFY_STEP_NS" \
  LOGGYTRACY_LOAD_MATRIX_REPEATS="$MATRIX_REPEATS" \
  LOGGYTRACY_LOAD_MATRIX_WINDOWS="$MATRIX_WINDOWS" \
  LOGGYTRACY_LOAD_MATRIX_STEP_SECONDS="$MATRIX_STEP_SECONDS" \
  LOGGYTRACY_LOAD_RESULT_PATH="$OUT/${phase}_$target.json" \
  LOGGYTRACY_BUILD_REVISION="$LOGGYTRACY_BUILD_REVISION" \
    "$LOAD_BIN" >/dev/null || echo "  ($phase verdict was not PASS for $target; the result file says why)" >&2
}

say "building the harness"
cargo build --manifest-path "$ROOT/Cargo.toml" --release --bin load
LOAD_BIN="$ROOT/target/release/load"

# The ingest phase is run at each limit in COMPARE_MEMORY_LIMITS, in order, and
# the first limit at which **both** systems survive carries the rest of the
# pipeline. A limit at which one of them is OOM-killed is not an error to
# retry past silently: it is an ingest result at that limit, it is recorded in
# `attempts` below, and the published document reports it. The list is a
# default like everything else here, and a single-entry list turns the sweep
# off.
ATTEMPTS=""
SURVIVED=""
for LIMIT in ${COMPARE_MEMORY_LIMITS:-2g 8g}; do
  export COMPARE_MEMORY="$LIMIT"
  bring_up
  run_load loggytracy "$LOGGYTRACY_PORT" "$LT_CGROUP"
  LT_ALIVE=$(alive loggytracy-compare-loggytracy-1 && echo true || echo false)
  LT_OOM_AT=$(docker inspect -f '{{.State.OOMKilled}}' loggytracy-compare-loggytracy-1)
  if [ "$LT_ALIVE" = "true" ]; then
    run_load loki "$LOKI_PORT" "$LK_CGROUP"
  fi
  LK_ALIVE=$(alive loggytracy-compare-loki-1 && echo true || echo false)
  LK_OOM_AT=$(docker inspect -f '{{.State.OOMKilled}}' loggytracy-compare-loki-1)

  ATTEMPTS="$ATTEMPTS${ATTEMPTS:+,}{\"limit\":\"$LIMIT\",\"loggytracy_survived\":$LT_ALIVE,\
\"loggytracy_oom_killed\":$LT_OOM_AT,\"loki_survived\":$LK_ALIVE,\"loki_oom_killed\":$LK_OOM_AT}"

  if [ "$LT_ALIVE" = "true" ] && [ "$LK_ALIVE" = "true" ]; then
    SURVIVED="$LIMIT"
    cp "$OUT/load_loggytracy.json" "$OUT/load_loggytracy_$LIMIT.json"
    cp "$OUT/load_loki.json" "$OUT/load_loki_$LIMIT.json"
    break
  fi
  say "a container did not survive ${LIMIT}; keeping the result and trying the next limit"
  cp "$OUT/load_loggytracy.json" "$OUT/load_loggytracy_$LIMIT.json" 2>/dev/null || true
  cp "$OUT/load_loki.json" "$OUT/load_loki_$LIMIT.json" 2>/dev/null || true
done

if [ -z "$SURVIVED" ]; then
  echo "no limit in '${COMPARE_MEMORY_LIMITS:-2g 8g}' let both systems finish the ingest phase;" >&2
  echo "the per-limit results are in $OUT and no query comparison was run." >&2
  exit 1
fi

run_verify seed loggytracy "$LOGGYTRACY_PORT" "$LT_CGROUP"
run_verify seed loki "$LOKI_PORT" "$LK_CGROUP"

say "settling for ${SETTLE_SECONDS}s"
# Loki holds a chunk in the ingester until it is idle for `chunk_idle_period`
# (30m) or reaches `max_chunk_age` (2h). Neither happens inside a run that
# takes minutes, so without this its chunks would never reach the filesystem
# and its bytes-on-disk would be a number about the run length. loggytracy
# flushes on its own every five seconds at its default, so this is what makes
# the two comparable rather than what gives Loki an advantage.
curl -s -X POST "http://127.0.0.1:$LOKI_PORT/flush" >/dev/null || true
sleep "$SETTLE_SECONDS"

disk_of() {
  docker exec "$1" du -sb "$2" | cut -f1
}
disk_breakdown() {
  docker exec "$1" sh -c "du -sb $2/* 2>/dev/null" | awk '{printf "%s%s:%s", (NR>1?",":""), $2, $1}'
}

LT_DISK=$(disk_of loggytracy-compare-loggytracy-1 /var/lib/loggytracy)
LK_DISK=$(disk_of loggytracy-compare-loki-1 /loki)
LT_DISK_PARTS=$(disk_breakdown loggytracy-compare-loggytracy-1 /var/lib/loggytracy)
LK_DISK_PARTS=$(disk_breakdown loggytracy-compare-loki-1 /loki)
LT_PEAK_INGEST=$(cat "$LT_CGROUP/memory.peak")
LK_PEAK_INGEST=$(cat "$LK_CGROUP/memory.peak")

say "restarting both so the cold query pass is cold"
docker compose restart
wait_ready "$LOGGYTRACY_PORT" loggytracy
wait_ready "$LOKI_PORT" loki
LT_CGROUP=$(cgroup_of loggytracy-compare-loggytracy-1)
LK_CGROUP=$(cgroup_of loggytracy-compare-loki-1)

run_verify matrix loggytracy "$LOGGYTRACY_PORT" "$LT_CGROUP"
run_verify matrix loki "$LOKI_PORT" "$LK_CGROUP"

LT_PEAK_QUERY=$(cat "$LT_CGROUP/memory.peak")
LK_PEAK_QUERY=$(cat "$LK_CGROUP/memory.peak")
LT_DISK_END=$(disk_of loggytracy-compare-loggytracy-1 /var/lib/loggytracy)
LK_DISK_END=$(disk_of loggytracy-compare-loki-1 /loki)

# Whether either container was killed for exceeding the limit. An OOM kill is
# a result, not an error, and a comparison that quietly restarted through one
# would be reporting the wrong thing.
LT_OOM=$(docker inspect -f '{{.State.OOMKilled}}' loggytracy-compare-loggytracy-1)
LK_OOM=$(docker inspect -f '{{.State.OOMKilled}}' loggytracy-compare-loki-1)

cat >"$OUT/bed.json" <<JSON
{
  "generated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "revision": "$LOGGYTRACY_BUILD_REVISION",
  "branch": "$LOGGYTRACY_BUILD_BRANCH",
  "machine": "$(uname -sr); $(nproc) logical CPUs; $(awk '/MemTotal/ {printf "%.1f GiB RAM", $2/1048576}' /proc/meminfo)",
  "docker": "$(docker version --format '{{.Server.Version}}')",
  "compose": "$(docker compose version --short)",
  "loki_image": "grafana/loki:3.3.2",
  "memory_limit": "$COMPARE_MEMORY",
  "memory_limit_attempts": [$ATTEMPTS],
  "memory_limit_bytes": $(docker exec loggytracy-compare-loggytracy-1 cat /sys/fs/cgroup/memory.max),
  "settle_seconds": $SETTLE_SECONDS,
  "verify_anchor_ns": $ANCHOR_NS,
  "object_store": "none: both systems are on their local filesystem",
  "peak_bytes": {
    "loggytracy": { "ingest": $LT_PEAK_INGEST, "query": $LT_PEAK_QUERY, "oom_killed": $LT_OOM },
    "loki": { "ingest": $LK_PEAK_INGEST, "query": $LK_PEAK_QUERY, "oom_killed": $LK_OOM }
  },
  "disk_bytes": {
    "loggytracy": { "settled": $LT_DISK, "after_queries": $LT_DISK_END, "breakdown": "$LT_DISK_PARTS" },
    "loki": { "settled": $LK_DISK, "after_queries": $LK_DISK_END, "breakdown": "$LK_DISK_PARTS" }
  }
}
JSON

say "generating $DOC"
cargo build --manifest-path "$ROOT/Cargo.toml" --release --bin compare_report
"$ROOT/target/release/compare_report" "$OUT" "$DOC"

# The artifacts are checked in beside the document, and copied by the same run
# that wrote the document, so the two cannot drift. The retired numbers this
# repository is recovering from had one cited artifact that did not exist and
# another that disagreed with the document citing it.
ARTIFACTS="${COMPARE_ARTIFACTS:-$ROOT/docs/artifacts/m9}"
mkdir -p "$ARTIFACTS"
for name in bed.json load_loggytracy.json load_loki.json seed_loggytracy.json \
  seed_loki.json matrix_loggytracy.json matrix_loki.json loki_config.diff \
  loggytracy_env.txt; do
  cp "$OUT/$name" "$ARTIFACTS/$name" 2>/dev/null || true
done
cp "$OUT"/load_*_*g.json "$ARTIFACTS/" 2>/dev/null || true
echo "results: $OUT" >&2
echo "document: $DOC" >&2
