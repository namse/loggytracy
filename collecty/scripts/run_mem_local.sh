#!/usr/bin/env bash
# One memory experiment, cages included.
#
# docs/MEMORY.md lists the experiments this runs. Each one is this script with
# exactly one thing changed, so that two runs are comparable: the load, the
# sink, the corpus, the rate and the connection count are the same every time
# unless the experiment is about one of them.
#
#   scripts/run_mem_local.sh baseline
#   MEM_ENV="MALLOC_ARENA_MAX=1" scripts/run_mem_local.sh arena1
#   MEM_FEATURES="memprof,mimalloc" scripts/run_mem_local.sh mimalloc
#   MEM_OUTAGE_AT=120 scripts/run_mem_local.sh drain
#   MEM_CONNECTIONS=256 scripts/run_mem_local.sh conn-256
#
# The verdict is peak anon out of the cgroup's own memory.stat, because that is
# what an OOM kill is decided on. The collector's own view is beside it and is
# never the verdict -- it is the thing being audited.
set -uo pipefail

NAME="${1:?usage: run_mem_local.sh <name>}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/target/mem/$NAME"

SECONDS_CAP="${MEM_SECONDS:-300}"
SETTLE="${MEM_SETTLE:-60}"
# Calibrated against what the 24-hour soak's collector actually received,
# read out of its own queue reports: 205 exports/s of a 41,785 byte mean, so
# 8.57 MB/s. These give 204 exports/s of 41,656 B. The one thing that does not
# match is the compression ratio -- 3.63x here against the soak's 4.87x -- so
# a segment here is larger than production's, which errs towards more memory
# pressure rather than less.
EPS="${MEM_EPS:-33000}"
CONNECTIONS="${MEM_CONNECTIONS:-8}"
RECORDS="${MEM_RECORDS:-161}"
TRACE_EPS="${MEM_TRACE_EPS:-5}"
METRIC_EVERY="${MEM_METRIC_EVERY:-10}"
LIMIT="${MEM_LIMIT:-256M}"
# `-` and not `:-`: MEM_FEATURES="" means the shipped build, which is a
# different experiment from not saying which build to use.
FEATURES="${MEM_FEATURES-memprof}"
COLLECTY_PORT="${MEM_COLLECTY_PORT:-4318}"
SINK_PORT="${MEM_SINK_PORT:-4319}"
OUTAGE_AT="${MEM_OUTAGE_AT:-}"
OUTAGE_FOR="${MEM_OUTAGE_FOR:-180}"
# The shipped ceilings. Left alone unless the experiment is about one of them,
# so a run measures the configuration a deployment gets.
INFLIGHT="${MEM_INFLIGHT:-64MiB}"
QUEUE_MAX="${MEM_QUEUE_MAX:-1GiB}"
SEGMENT="${MEM_SEGMENT:-8MiB}"
UNIT="collecty-mem-$NAME"

rm -rf "$OUT"; mkdir -p "$OUT"
QUEUE_DIR="$OUT/queue"

cleanup() {
  rm -f "$OUT/RUNNING"
  [ -n "${SAMPLER_PID:-}" ] && kill "$SAMPLER_PID" 2>/dev/null
  systemctl --user stop "$UNIT.scope" 2>/dev/null
  [ -n "${COLLECTY_PID:-}" ] && kill "$COLLECTY_PID" 2>/dev/null
  sleep 0.5
  [ -n "${COLLECTY_PID:-}" ] && kill -KILL "$COLLECTY_PID" 2>/dev/null
  [ "${MEM_KEEP_QUEUE:-0}" = "1" ] || rm -rf "$QUEUE_DIR"
  return 0
}
trap cleanup EXIT

require() { command -v "$1" >/dev/null || { echo "$1 is needed and is not here"; exit 4; }; }
require systemd-run
require systemctl
[ "$(stat -fc %T /sys/fs/cgroup 2>/dev/null)" = "cgroup2fs" ] || {
  echo "NOT_MEASURED: cgroup v2 is not mounted, so nothing would be limited"; exit 4; }

echo "building (features: ${FEATURES:-none})"
if [ -n "$FEATURES" ]; then
  cargo build --manifest-path "$ROOT/Cargo.toml" --release --bin collecty --features "$FEATURES" || exit 4
else
  cargo build --manifest-path "$ROOT/Cargo.toml" --release --bin collecty || exit 4
fi
cargo build --manifest-path "$ROOT/Cargo.toml" --release --example memrig || exit 4

# The collector in its own scope at the declared limit, swap off, exactly as
# signy's gate cages its engine.
env COLLECTY_DATA_DIR="$OUT" \
    COLLECTY_LISTEN_ADDR="127.0.0.1:$COLLECTY_PORT" \
    COLLECTY_SIGNY_URL="http://127.0.0.1:$SINK_PORT" \
    COLLECTY_MAX_INFLIGHT_BYTES="$INFLIGHT" \
    COLLECTY_QUEUE_MAX_BYTES="$QUEUE_MAX" \
    COLLECTY_QUEUE_SEGMENT_BYTES="$SEGMENT" \
    COLLECTY_REPORT_INTERVAL="10s" \
    COLLECTY_MEMPROF_CSV="$OUT/memprof.csv" \
    COLLECTY_MEMPROF_INTERVAL_MS=250 \
    ${MEM_ENV:-} \
    systemd-run --user --scope --quiet --unit="$UNIT" \
      -p MemoryMax="$LIMIT" -p MemorySwapMax=0 \
      "$ROOT/target/release/collecty" >"$OUT/collecty.log" 2>&1 &
COLLECTY_PID=$!

CGROUP=""
for _ in $(seq 1 50); do
  CANDIDATE="/sys/fs/cgroup/user.slice/user-$(id -u).slice/user@$(id -u).service/$UNIT.scope"
  [ -f "$CANDIDATE/memory.max" ] && { CGROUP="$CANDIDATE"; break; }
  CANDIDATE="/sys/fs/cgroup/user.slice/user-$(id -u).slice/user@$(id -u).service/app.slice/$UNIT.scope"
  [ -f "$CANDIDATE/memory.max" ] && { CGROUP="$CANDIDATE"; break; }
  sleep 0.2
done
[ -n "$CGROUP" ] || { echo "NOT_MEASURED: the scope's cgroup never appeared"; exit 4; }

# The limit has to be readable and the one that was asked for, or the number
# this run reports means nothing.
WANT=$(numfmt --from=iec "${LIMIT%B}" 2>/dev/null || echo 0)
GOT=$(cat "$CGROUP/memory.max")
[ "$GOT" = "$WANT" ] || { echo "NOT_MEASURED: memory.max is $GOT, asked for $WANT"; exit 4; }
[ "$(cat "$CGROUP/memory.swap.max")" = "0" ] || { echo "NOT_MEASURED: swap is not off"; exit 4; }

for _ in $(seq 1 50); do
  grep -q "accepting OTLP" "$OUT/collecty.log" 2>/dev/null && break
  sleep 0.2
done
grep -q "accepting OTLP" "$OUT/collecty.log" || { echo "NOT_MEASURED: collecty never listened"; tail -5 "$OUT/collecty.log"; exit 4; }

touch "$OUT/RUNNING"
(
  echo "t,anon,current,peak,queue_bytes,segments"
  T0=$(date +%s.%N)
  while [ -f "$OUT/RUNNING" ]; do
    now=$(date +%s.%N)
    anon=$(awk '$1=="anon"{print $2}' "$CGROUP/memory.stat" 2>/dev/null)
    printf '%s,%s,%s,%s,%s,%s\n' \
      "$(awk -v a="$now" -v b="$T0" 'BEGIN{printf "%.2f", a-b}')" \
      "${anon:-0}" "$(cat "$CGROUP/memory.current" 2>/dev/null)" \
      "$(cat "$CGROUP/memory.peak" 2>/dev/null)" \
      "$(du -sb "$QUEUE_DIR" 2>/dev/null | cut -f1)" \
      "$(find "$QUEUE_DIR" -name '*.seg' 2>/dev/null | wc -l)"
    sleep 0.25
  done
) >"$OUT/cgroup.csv" 2>/dev/null &
SAMPLER_PID=$!

OUTAGE_FLAGS=()
[ -n "$OUTAGE_AT" ] && OUTAGE_FLAGS=(--outage-at "$OUTAGE_AT" --outage-for "$OUTAGE_FOR")

echo "load: ${EPS} eps over ${CONNECTIONS} connections for ${SECONDS_CAP}s, cage $LIMIT"
"$ROOT/target/release/examples/memrig" \
  --collecty "127.0.0.1:$COLLECTY_PORT" --sink "127.0.0.1:$SINK_PORT" \
  --eps "$EPS" --connections "$CONNECTIONS" --seconds "$SECONDS_CAP" \
  --records-per-export "$RECORDS" --report "$OUT/rig.json" \
  --trace-eps "$TRACE_EPS" --metric-every "$METRIC_EVERY" \
  "${OUTAGE_FLAGS[@]}" >"$OUT/rig.log" 2>&1 &
RIG_PID=$!
wait $RIG_PID
RIG_STATUS=$?

# The settle is where signy's gate found its worst peak: the workload stopping
# is not the process being done.
echo "settling for ${SETTLE}s"
sleep "$SETTLE"

rm -f "$OUT/RUNNING"
sleep 0.5

KILLED=$(awk '$1=="oom_kill"{print $2}' "$CGROUP/memory.events" 2>/dev/null)
kill -TERM "$COLLECTY_PID" 2>/dev/null
sleep 2

python3 - "$OUT" "$LIMIT" "${KILLED:-0}" "$RIG_STATUS" <<'PY'
import csv, json, sys, os

out, limit, killed, rig_status = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
MiB = 1048576.0

def rows(name):
    path = os.path.join(out, name)
    if not os.path.exists(path):
        return []
    with open(path) as handle:
        return [r for r in csv.DictReader(handle) if r.get("t")]

cg = rows("cgroup.csv")
mp = rows("memprof.csv")
anon = [float(r["anon"]) for r in cg if r["anon"].isdigit()]
result = {"name": os.path.basename(out), "limit": limit, "oom_kills": killed}

if not anon:
    result["verdict"] = "NOT_MEASURED"
    result["reason"] = "no cgroup samples"
else:
    result["peak_anon_mib"] = round(max(anon) / MiB, 1)
    result["final_anon_mib"] = round(anon[-1] / MiB, 1)
    result["samples"] = len(anon)

try:
    with open(os.path.join(out, "rig.json")) as handle:
        result["load"] = json.load(handle)
except Exception:
    result["load"] = None

if mp:
    last = mp[-1]
    peak_live = max(int(r["tagged_live"]) for r in mp)
    result["engine"] = {
        "peak_tagged_live_mib": round(peak_live / MiB, 1),
        "final_tagged_live_mib": round(int(last["tagged_live"]) / MiB, 1),
        "final_by_arena_mib": {
            a: round(int(last[f"tagged_live_{a}"]) / MiB, 2)
            for a in ("other", "intake", "queue", "send")
        },
        "final_rss_mib": round(int(last["rss"]) / MiB, 1),
        "peak_threads": max(int(r["threads"]) for r in mp),
        "final_libc_in_use_mib": round(int(last["libc_in_use"]) / MiB, 1),
        "final_libc_free_mib": round(int(last["libc_free"]) / MiB, 1),
        "final_libc_mmap_mib": round(int(last["libc_mmap"]) / MiB, 1),
        "peak_inflight_mib": round(max(int(r["inflight_bytes"]) for r in mp) / MiB, 2),
        "header_bytes_mib": round(int(last["header_bytes"]) / MiB, 1),
        "total_allocated_gb": round(int(last["total_bytes"]) / 1e9, 1),
        "total_allocations_m": round(int(last["total_allocs"]) / 1e6, 1),
    }
    # Under glibc the tagged total and the C side share one heap, so this is
    # C-side allocation plus whatever Rust allocation no guard covered -- an
    # approximation, and the column that moves when zstd stops churning.
    result["engine"]["untagged_in_use_mib"] = round(
        (int(last["libc_in_use"]) - int(last["tagged_live"])) / MiB, 1
    )

if "verdict" not in result:
    load = result.get("load") or {}
    offered = load.get("offered_exports", 0)
    accepted = load.get("accepted_exports", 0)
    if killed:
        result["verdict"] = "OOM_KILLED"
    elif rig_status != 0 or offered == 0:
        result["verdict"] = "NOT_MEASURED"
        result["reason"] = "the load did not run"
    elif accepted < offered * 0.9:
        result["verdict"] = "NOT_MEASURED"
        result["reason"] = f"only {accepted} of {offered} exports were accepted"
    else:
        result["verdict"] = "MEASURED"

with open(os.path.join(out, "result.json"), "w") as handle:
    json.dump(result, handle, indent=2)
print(json.dumps(result, indent=2))
PY
