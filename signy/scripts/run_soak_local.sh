#!/usr/bin/env bash
#
# The soak leg: hours-to-a-day of ingest + query + merge + retention on the
# 2 GiB rig (todo.md, "Next: production polish", item 1).
#
#   scripts/run_soak_local.sh <run-name>
#
# run_memprof_local.sh's rig — a native cgroup v2 scope, the bed's corpus and
# offered rate — pointed at what only hours can show: part-count growth against
# the unevictable sidecars, WAL size under the local-compaction policy, merge
# pacing, row-group-cache gauge drift over thousands of evictions, and
# allocator retention (the 1.34-1.69 anon/live ratio was measured at 60 s).
#
# What is deliberately different from the memprof script:
#
#   * The data directory lives on disk, not tmpfs. tmpfs pages are charged to
#     the writing cgroup as shmem and never reclaimed with swap off, so hours
#     of parts would eat the 2 GiB limit and manufacture an OOM the disk
#     engine does not have. The 240 s memprof runs fit under that; a soak
#     cannot.
#   * Retention is on (30 m period, 60 s interval, 5 m grace by default) so
#     parts are deleted, sidecars retract, and the disk reaches a steady state
#     the run can be judged on.
#   * All three signals, not one. The log leg is the rate; the metric leg
#     scrapes a live population onto the *same tenant* and reads it back
#     through the first-party metric routes. Metrics are the signal no long run
#     had ever touched, and the one whose storage changed most recently.
#   * An object store is configured by default (`file://` under the run's own
#     output directory). Without one the engine never offloads, never retires a
#     part to the store, and never compacts the WAL -- three production paths a
#     local-only soak silently skips. What `file://` cannot exercise is the
#     compare-and-swap: the code declares it a single-process development
#     backend and takes the overwrite path, so CAS stays a question only a real
#     provider's startup preflight answers.
#   * A disk guard fails the run before the root filesystem does.
#   * The verdict is a set of trends — quarter-by-quarter means — rather than
#     one peak: a soak passes when the residents stop growing, not when a
#     threshold survives a minute.
#
# Every knob is a default so a caller can vary exactly one:
#
#   SOAK_SECONDS=900                 ./scripts/run_soak_local.sh smoke
#   SOAK_LIMIT=8G                    ./scripts/run_soak_local.sh headroom
#   SOAK_RETENTION=off               ./scripts/run_soak_local.sh no-retention
#   SOAK_SERVER_ENV="MALLOC_ARENA_MAX=1" ./scripts/run_soak_local.sh trimmed
#   SOAK_MEMORY_HIGH=1800M           ./scripts/run_soak_local.sh throttled
#   SOAK_OBJECT_STORE=off            ./scripts/run_soak_local.sh local-only
#   SOAK_METRIC_SCRAPE_SECONDS=0     ./scripts/run_soak_local.sh logs-only
#   SOAK_FEATURES=                   ./scripts/run_soak_local.sh production-allocator
set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
NAME="${1:?usage: run_soak_local.sh <run-name>}"
OUT="${SOAK_OUT:-$ROOT/target/soak}/$NAME"
rm -rf "$OUT"; mkdir -p "$OUT"

LIMIT="${SOAK_LIMIT:-2G}"
# Unset by default, so the rig keeps one wall and hits it. Set it and reclaim
# starts throttled and gradual below the hard limit instead of as a cliff at
# it — the question of whether the freeze is the cliff or the reclaim itself.
MEMORY_HIGH="${SOAK_MEMORY_HIGH:-}"
SECONDS_CAP="${SOAK_SECONDS:-86400}"
EPS="${SOAK_EPS:-20000}"
CONNS="${SOAK_CONNECTIONS:-8}"
QUERY_EPS="${SOAK_QUERY_EPS:-5}"
RETENTION="${SOAK_RETENTION:-30m}"
# Retention is a tenant policy the harness pushes, not a server-wide setting:
# SIGNY_RETENTION_PERIOD stopped being read when it moved there, so the leg
# this knob names had been running against nothing. `off` is this script's
# documented spelling; the policy's is `infinite`.
case "$RETENTION" in
  off) TENANT_RETENTION="infinite" ;;
  *)   TENANT_RETENTION="$RETENTION" ;;
esac
RETENTION_INTERVAL="${SOAK_RETENTION_INTERVAL:-60s}"
RETENTION_GRACE="${SOAK_RETENTION_GRACE:-5m}"
# The metric leg. The scrape interval and the query rate are what turn it on;
# `0` on either turns that half off. The population is 8 services x this many
# instances x (4 gauges + 4 counters + 1 histogram), so the default offers
# about 1,150 series -- a real instrument set beside the log rate rather than a
# cardinality test, which the bed measures separately.
METRIC_SCRAPE="${SOAK_METRIC_SCRAPE_SECONDS:-10}"
METRIC_QUERY_EPS="${SOAK_METRIC_QUERY_EPS:-1}"
METRIC_INSTANCES="${SOAK_METRIC_INSTANCES:-16}"
METRIC_CHURN="${SOAK_METRIC_CHURN_PER_SCRAPE:-0}"
# Shorter than the retention period by construction: a window past what the
# tenant keeps reads empty, and the leg's empty-answer gate would then be
# firing on this script's configuration rather than on the engine.
METRIC_WINDOW="${SOAK_METRIC_QUERY_WINDOW_SECONDS:-900}"
# The object store. `off` reverts to the local-only engine the published soaks
# ran on, which is the comparison, not the production shape.
STORE="${SOAK_OBJECT_STORE:-file://$OUT/store}"
# The comparison bed's seed, so this run's corpus is the bed's corpus.
SEED="${SOAK_SEED:-1592598566}"
BIN="${SOAK_BIN:-$ROOT/target/release/signy}"
PORT="${SOAK_PORT:-3153}"
# memprof is on by default: allocator retention over hours is half of what
# this run exists to observe, and anon/live needs the arena live bytes.
#
# `SOAK_FEATURES=` (explicitly empty) builds the *production* binary instead,
# which is a different allocator and not a detail: the memprof build's
# instrumented wrapper allocates through glibc, and the shipped binary uses
# mimalloc (`src/main.rs`). An absolute footprint and an arena split cannot be
# read off the same run, so a question about whether production survives has to
# be asked of this build. The `-` rather than `:-` is what makes the empty
# value mean "no features" instead of falling back to the default.
FEATURES="${SOAK_FEATURES-memprof}"
# Fail the run while the machine still has room, well before the filesystem
# is full: retention churn needs headroom to delete into.
DISK_MIN_AVAIL_KB="${SOAK_DISK_MIN_AVAIL_KB:-$((4 * 1024 * 1024))}"
# Seconds to keep sampling after the load stops, with the server still up.
#
# This answers the one question every run so far has left open: when nothing is
# being asked of it any more, does the process give its memory back? If
# resident falls toward live, what it was holding was freed memory the decay
# policy had not returned, which is a setting. If it does not fall, the memory
# is either still live or pinned by fragmentation, which is a defect in what
# the engine allocates rather than in what the allocator does with it. The runs
# before this killed the server the moment the harness stopped, so the question
# had never been put.
SETTLE="${SOAK_SETTLE_SECONDS:-120}"
UNIT="soak-$NAME"

if [ "${SOAK_SKIP_BUILD:-0}" != "1" ]; then
  cargo build --manifest-path "$ROOT/Cargo.toml" --release --bin load
  [ -n "${SOAK_BIN:-}" ] || cargo build --manifest-path "$ROOT/Cargo.toml" --release \
    --bin signy ${FEATURES:+--features "$FEATURES"}
fi

DATA="${SOAK_DATA:-$OUT/data}"
mkdir -p "$DATA"
SERVER_LOG="$OUT/server.log"

# The store's own directory, so its size can be sampled beside the data dir:
# with an object store configured the data dir is a cache and a WAL, and the
# bytes that outlive a part are over here.
STORE_DIR=""
STORE_ENV=""
if [ "$STORE" != "off" ]; then
  case "$STORE" in
    file://*) STORE_DIR="${STORE#file://}"; mkdir -p "$STORE_DIR" ;;
  esac
  STORE_ENV="SIGNY_OBJECT_STORE_URL=$STORE"
fi

systemctl --user reset-failed "$UNIT.scope" 2>/dev/null

cleanup() {
  for p in ${SAMPLER_PID:-} ${PROF_PID:-} ${WATCH_PID:-} ${SERVER_PID:-}; do kill "$p" 2>/dev/null; done
  sleep 1
  [ -n "${SERVER_PID:-}" ] && kill -KILL "$SERVER_PID" 2>/dev/null
  [ "${SOAK_KEEP_DATA:-0}" = "1" ] || rm -rf "$DATA" ${STORE_DIR:+"$STORE_DIR"}
  systemctl --user reset-failed "$UNIT.scope" 2>/dev/null
}
trap cleanup EXIT

# shellcheck disable=SC2086
env SIGNY_LISTEN_ADDR="127.0.0.1:$PORT" \
    SIGNY_DATA_DIR="$DATA" \
    SIGNY_RETENTION_INTERVAL="$RETENTION_INTERVAL" \
    SIGNY_RETENTION_GRACE_PERIOD="$RETENTION_GRACE" \
    ${STORE_ENV:-} \
    ${SOAK_SERVER_ENV:-} \
    systemd-run --user --scope --quiet --unit="$UNIT" \
      -p MemoryMax="$LIMIT" -p MemorySwapMax=0 \
      ${MEMORY_HIGH:+-p MemoryHigh="$MEMORY_HIGH"} \
      -- "$BIN" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 180); do
  curl -fsS "http://127.0.0.1:$PORT/ready" >/dev/null 2>&1 && break
  kill -0 "$SERVER_PID" 2>/dev/null || { echo "server died before ready"; cat "$SERVER_LOG"; exit 1; }
  sleep 0.5
done
# Read from the server's own process rather than derived from this shell's:
# where systemd places a user scope depends on the manager, and a wrong guess
# here would silently sample an empty cgroup.
CG="/sys/fs/cgroup$(cut -d: -f3 "/proc/$SERVER_PID/cgroup")"
[ -d "$CG" ] || { echo "no cgroup at $CG"; exit 1; }
echo "cgroup=$CG memory.max=$(cat "$CG/memory.max") memory.high=$(cat "$CG/memory.high") swap.max=$(cat "$CG/memory.swap.max")"
echo "out=$OUT data=$DATA seconds=$SECONDS_CAP eps=$EPS retention=$RETENTION"
echo "store=$STORE metric_scrape=${METRIC_SCRAPE}s metric_query_eps=$METRIC_QUERY_EPS"

# The memprof sampler's columns plus the disk: data_dir and journal.wal sizes
# (du is paid once a minute, the value carried between), and the filesystem's
# available space, which trips the guard. `anon` is what an OOM kill is
# decided on; `file` is the page cache the cgroup peak also includes.
#
# The last eight columns are the stall evidence, all cumulative. Memory: PSI's
# `some` and `full` stall microseconds (`full` = every thread in the cgroup
# stalled at once, which is what a whole-server freeze looks like from the
# kernel's side), direct reclaim's scanned and stolen pages (as against
# kswapd's, which costs the workload nothing), and file refaults, which say a
# reclaimed page was wanted again. Then the same `some`/`full` for I/O and
# `some` for CPU, because the first instrumented hour ruled memory out — a
# freeze with zero direct reclaim in it is not a reclaim stall, and the next
# question is which resource the threads were actually waiting on.
(
  echo "t,current,peak,anon,file,slab,sock,kstack,pgtables,memtable,memtable_buffered,pending_flush,wal_backlog,parts,sidecar,part_meta,rg_cache,query_success,query_errors,threads,mp_other,mp_ingest,mp_wal,mp_flush,mp_merge,mp_query,mp_sidecar,mp_part_meta,mp_rg_cache,mp_header,mi_arena,mi_mmap,mi_inuse,mi_free,data_dir,wal_file,disk_avail_kb,psi_some_us,psi_full_us,pgscan_direct,pgsteal_direct,refault_file,io_some_us,io_full_us,cpu_some_us,active_series,series_memtable,metric_parts,metric_dp_rejected,metric_mem_rejected,metrics_dir,store_dir,proc_rss,alloc_committed,alloc_live,alloc_retained,alloc_active,alloc_metadata"
  T0=$(date +%s.%N)
  echo "$T0" >"$OUT/sampler_t0"
  i=0; du_bytes=0; wal_bytes=0; metrics_bytes=0; store_bytes=0
  while [ -d "$CG" ]; do
    now=$(date +%s.%N)
    cur=$(cat "$CG/memory.current" 2>/dev/null) || break
    peak=$(cat "$CG/memory.peak" 2>/dev/null)
    st=$(cat "$CG/memory.stat" 2>/dev/null)
    psi=$(cat "$CG/memory.pressure" 2>/dev/null)
    psi_io=$(cat "$CG/io.pressure" 2>/dev/null)
    psi_cpu=$(cat "$CG/cpu.pressure" 2>/dev/null)
    m=$(curl -fsS --max-time 2 "http://127.0.0.1:$PORT/metrics" 2>/dev/null)
    if [ $((i % 60)) -eq 0 ]; then
      du_bytes=$(du -sb "$DATA" 2>/dev/null | cut -f1)
      metrics_bytes=$(du -sb "$DATA/metrics" 2>/dev/null | cut -f1)
      store_bytes=0
      [ -n "$STORE_DIR" ] && store_bytes=$(du -sb "$STORE_DIR" 2>/dev/null | cut -f1)
      wal_bytes=$(find "$DATA" -name 'journal.wal' -printf '%s\n' 2>/dev/null | awk '{s+=$1} END{print s+0}')
      avail_kb=$(df -k --output=avail "$DATA" 2>/dev/null | tail -1 | tr -d ' ')
      if [ "${avail_kb:-0}" -lt "$DISK_MIN_AVAIL_KB" ]; then
        echo "disk guard: ${avail_kb} KB available < ${DISK_MIN_AVAIL_KB} KB" >"$OUT/DISK_GUARD"
        pkill -f 'target/release/load'
        break
      fi
    fi
    cg=$(echo "$st" | awk '
      $1=="anon"{a=$2} $1=="file"{f=$2} $1=="slab"{s=$2} $1=="sock"{k=$2}
      $1=="kernel_stack"{ks=$2} $1=="pgtables"{p=$2}
      END{printf "%d,%d,%d,%d,%d,%d", a, f, s, k, ks, p}')
    rcl=$(echo "$psi" | awk -F'total=' '
      /^some /{s=$2+0} /^full /{f=$2+0} END{printf "%d,%d", s, f}')
    rcl="$rcl,$(echo "$st" | awk '
      $1=="pgscan_direct"{sc=$2} $1=="pgsteal_direct"{stl=$2}
      $1=="workingset_refault_file"{rf=$2}
      END{printf "%d,%d,%d", sc, stl, rf}')"
    rcl="$rcl,$(echo "$psi_io" | awk -F'total=' '
      /^some /{s=$2+0} /^full /{f=$2+0} END{printf "%d,%d", s, f}')"
    rcl="$rcl,$(echo "$psi_cpu" | awk -F'total=' '/^some /{printf "%d", $2+0}')"
    mx=$(echo "$m" | awk '
      $1=="signy_active_series"{as=$2}
      $1=="signy_series_memtable_bytes"{sm=$2}
      $1=="signy_metric_part_count"{mp=$2}
      $1=="signy_metric_datapoints_rejected_total"{dr=$2}
      $1=="signy_metric_memory_rejected_total"{mr=$2}
      END{printf "%d,%d,%d,%d,%d", as, sm, mp, dr, mr}')
    # Resident against committed, which is the production build's only answer
    # to "is this process big because it is using memory or because the
    # allocator kept it". The memprof build publishes the resident half only
    # and leaves the other at zero.
    al=$(echo "$m" | awk '
      $1=="signy_process_rss_bytes"{rs=$2}
      $1=="signy_allocator_committed_bytes"{cm=$2}
      $1=="signy_allocator_live_bytes"{lv=$2}
      $1=="signy_allocator_retained_bytes"{rt=$2}
      $1=="signy_allocator_active_bytes"{ac=$2}
      $1=="signy_allocator_metadata_bytes"{md=$2}
      END{printf "%d,%d,%d,%d,%d,%d", rs, cm, lv, rt, ac, md}')
    gg=$(echo "$m" | awk '
      $1=="signy_memtable_bytes"{mt=$2}
      $1=="signy_memtable_buffered_bytes"{mb=$2}
      $1=="signy_pending_flush_bytes"{pf=$2}
      $1=="signy_wal_backlog_bytes"{wb=$2}
      $1=="signy_part_count"{pc=$2}
      $1=="signy_part_sidecar_resident_bytes"{sc=$2}
      $1=="signy_part_meta_bytes"{pm=$2}
      $1=="signy_row_group_cache_bytes"{rc=$2}
      $1=="signy_query_success_total"{qs=$2}
      $1=="signy_query_errors_total"{qe=$2}
      END{printf "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d", mt, mb, pf, wb, pc, sc, pm, rc, qs, qe}')
    threads=$(awk '/^Threads:/{print $2}' "/proc/$(head -1 "$CG/cgroup.procs" 2>/dev/null)/status" 2>/dev/null)
    mp=$(echo "$m" | awk -F'[{}" ]+' '
      /^signy_memprof_live_bytes\{/{v[$3]=$NF}
      /^signy_memprof_malloc_bytes\{/{i[$3]=$NF}
      /^signy_memprof_header_bytes /{h=$2}
      END{printf "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d",
          v["other"], v["ingest"], v["wal"], v["flush"], v["merge"],
          v["query"], v["sidecar"], v["part_meta"], v["row_group_cache"], h,
          i["arena"], i["mmapped"], i["in_use"], i["free"]}')
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$(awk -v a="$now" -v b="$T0" 'BEGIN{printf "%.2f", a-b}')" \
      "$cur" "$peak" "$cg" "$gg" "${threads:-0}" "$mp" \
      "$du_bytes" "$wal_bytes" "${avail_kb:-0}" "$rcl" \
      "$mx" "${metrics_bytes:-0}" "${store_bytes:-0}" "$al"
    i=$((i + 1))
    sleep 1
  done
) >"$OUT/mem.csv" 2>/dev/null &
SAMPLER_PID=$!

# The cumulative memprof counters, once a minute: mem.csv carries live bytes,
# and the allocation traffic that explains them is only in the totals.
(
  while true; do
    body=$(curl -fsS --max-time 2 "http://127.0.0.1:$PORT/metrics" 2>/dev/null | grep memprof)
    [ -n "$body" ] && { echo "=== $(date +%s.%N)"; echo "$body"; }
    sleep 60
  done
) >"$OUT/memprof.txt" 2>/dev/null &
PROF_PID=$!

# The harness has a 60 s request timeout, so without this a run whose server
# was killed mid-soak would sit for minutes producing nothing.
( while kill -0 "$SERVER_PID" 2>/dev/null; do sleep 1; done; sleep 3; pkill -f 'target/release/load' ) &
WATCH_PID=$!

SIGNY_LOAD_TARGET=signy \
SIGNY_LOAD_PHASE=load \
SIGNY_LOAD_ADDR="127.0.0.1:$PORT" \
SIGNY_LOAD_CGROUP="$CG" \
SIGNY_LOAD_TIER=soak \
SIGNY_LOAD_SEED="$SEED" \
SIGNY_LOAD_SECONDS="$SECONDS_CAP" \
SIGNY_LOAD_EVENTS=0 \
SIGNY_LOAD_TARGET_EPS="$EPS" \
SIGNY_LOAD_CONNECTIONS="$CONNS" \
SIGNY_LOAD_QUERY_EPS="$QUERY_EPS" \
SIGNY_LOAD_OTLP_EPS=0 \
SIGNY_LOAD_METRIC_LEG_SCRAPE_SECONDS="$METRIC_SCRAPE" \
SIGNY_LOAD_METRIC_LEG_CHURN_PER_SCRAPE="$METRIC_CHURN" \
SIGNY_LOAD_METRIC_QUERY_EPS="$METRIC_QUERY_EPS" \
SIGNY_LOAD_METRIC_QUERY_WINDOW_SECONDS="$METRIC_WINDOW" \
SIGNY_LOAD_METRIC_INSTANCES="$METRIC_INSTANCES" \
SIGNY_LOAD_TENANT_RETENTION="$TENANT_RETENTION" \
SIGNY_LOAD_RESULT_PATH="$OUT/load.json" \
  "$ROOT/target/release/load" >"$OUT/harness.log" 2>&1
HARNESS_STATUS=$?
LOAD_STOPPED_AT=$(date +%s.%N)

# The settle window: the load is gone, the server is not. Sampling continues,
# so the series carries a stretch with no work in it to compare against.
if [ "$SETTLE" -gt 0 ] 2>/dev/null && kill -0 "$SERVER_PID" 2>/dev/null; then
  echo "settling for ${SETTLE}s with the load stopped"
  sleep "$SETTLE"
fi

# Scraped while the server is still up, which is the only time it can be. The
# journal writer's phase histograms are cumulative over the run, so one scrape
# at the end is the whole distribution — and it is the only thing that says
# which phase a slow push was in. mem.csv cannot carry them: they are buckets,
# not a gauge.
curl -fsS --max-time 5 "http://127.0.0.1:$PORT/metrics" >"$OUT/metrics-final.txt" 2>/dev/null || true

kill "$WATCH_PID" 2>/dev/null; WATCH_PID=
kill "$PROF_PID" 2>/dev/null; PROF_PID=
sleep 2
kill "$SAMPLER_PID" 2>/dev/null; SAMPLER_PID=

ALIVE=false
kill -0 "$SERVER_PID" 2>/dev/null && ALIVE=true
GUARD=ok
[ -f "$OUT/DISK_GUARD" ] && GUARD="tripped: $(cat "$OUT/DISK_GUARD")"

# The verdict is trends. Samples are split into quarters by time (restricted
# to rows whose metrics scrape succeeded), and each resident's quarter means
# are printed side by side: a healthy soak's Q2..Q4 are flat. GROWING flags
# Q4 > 1.10 * Q2 — past the warmup, still climbing at the end.
{
  echo "run=$NAME limit=$LIMIT seconds=$SECONDS_CAP eps=$EPS retention=$RETENTION"
  # Which allocator the run measured. An absolute footprint from the memprof
  # build and one from the shipped build are different numbers about different
  # heaps, and a verdict that does not say which is a trap for whoever reads it
  # next.
  case "$FEATURES" in
    "")         SOAK_ALLOCATOR=mimalloc ;;
    *memprof*)  SOAK_ALLOCATOR=glibc+memprof ;;
    *jemalloc*) SOAK_ALLOCATOR=jemalloc ;;
    *)          SOAK_ALLOCATOR="$FEATURES" ;;
  esac
  echo "build=${FEATURES:-production} allocator=$SOAK_ALLOCATOR store=$STORE"
  echo "alive=$ALIVE harness_status=$HARNESS_STATUS disk_guard=$GUARD"
  # Exit 3 is the harness reporting that nothing it pushed reached storage. The
  # verdict below is trends, and a server that was never given anything to hold
  # is flat in every quarter, which is what a healthy soak looks like.
  if [ "$HARNESS_STATUS" = 3 ]; then
    echo "NO LOAD DELIVERED: harness exit 3, so the trends below are of an idle server"
  fi
  grep -o '"verdict":"[A-Z_]*"' "$OUT/load.json" 2>/dev/null | head -1
  awk -F, '
    NR>1 && ($10+0>0 || $14+0>0) { n++; t[n]=$1+0
      for (c=2; c<=NF; c++) v[c,n]=$c+0
      if ($4+0 > anon_peak) anon_peak = $4+0
    }
    END {
      if (n < 8) { print "too few samples for trends: " n; exit }
      m = 1048576.0
      # column, name, unit. `count` prints as it is; everything else is MiB.
      nr = split("4 anon mib;13 wal_backlog mib;14 parts count;15 sidecar mib;\
16 part_meta mib;17 rg_cache mib;35 data_dir mib;36 wal_file mib;\
51 metrics_dir mib;52 store_dir mib;46 active_series count;\
47 series_memtable mib;48 metric_parts count;53 proc_rss mib;\
54 alloc_committed mib;55 alloc_live mib;56 alloc_retained mib;\
57 alloc_active mib;58 alloc_metadata mib", rows, ";")
      printf "%-14s %12s %12s %12s %12s  %s\n", "mib/count", "Q1", "Q2", "Q3", "Q4", "trend"
      for (r = 1; r <= nr; r++) {
        split(rows[r], f, " "); c = f[1] + 0
        for (q = 0; q < 4; q++) {
          lo = int(n * q / 4) + 1; hi = int(n * (q + 1) / 4); s = 0
          for (k = lo; k <= hi; k++) s += v[c,k]
          mean[q] = s / (hi - lo + 1)
        }
        trend = (mean[3] > mean[1] * 1.10) ? "GROWING" : "flat"
        if (f[3] == "count")
          printf "%-14s %12.0f %12.0f %12.0f %12.0f  %s\n", f[2], mean[0], mean[1], mean[2], mean[3], trend
        else
          printf "%-14s %12.1f %12.1f %12.1f %12.1f  %s\n", f[2], mean[0]/m, mean[1]/m, mean[2]/m, mean[3]/m, trend
      }
      printf "anon_peak_mib=%.1f cgroup_peak_mib=%.1f duration_s=%s samples=%d\n", \
             anon_peak/m, v[3,n]/m, t[n], n
      # anon/live at the last scraped sample, when the build carries memprof.
      live = 0; for (c = 21; c <= 29; c++) live += v[c,n]
      if (live > 0)
        printf "final anon/live=%.2f (anon=%.1f live=%.1f free=%.1f)\n", \
               v[4,n]/live, v[4,n]/m, live/m, v[34,n]/m
      printf "final queries: success=%d errors=%d threads=%d\n", v[18,n], v[19,n], v[20,n]
    }' "$OUT/mem.csv"
  # The stall table: the run's freezes as the server itself saw them — a query
  # success counter that does not advance — each one carried alongside the
  # kernel's reclaim evidence for the same window. A freeze whose psi_full
  # covers most of its length is the cgroup at its limit, not a slow query.
  awk -F, '
    NR>1 && ($10+0>0 || $14+0>0) { n++
      t[n]=$1+0; qs[n]=$18+0; cur[n]=$2+0; file[n]=$5+0
      psf[n]=$39+0; stl[n]=$41+0; rf[n]=$42+0
      iof[n]=$44+0; cps[n]=$45+0
    }
    END {
      if (n < 8) { print "too few samples for stalls: " n; exit }
      m = 1048576.0; MIN = 3.0
      start = 0; ne = 0
      for (k = 2; k <= n; k++) {
        if (qs[k] == qs[k-1]) { if (!start) start = k - 1 }
        else if (start) { d = t[k] - t[start]
          if (d >= MIN) { ne++; es[ne] = start; ee[ne] = k; ed[ne] = d; tot += d }
          start = 0 }
      }
      if (start && t[n] - t[start] >= MIN) { ne++; es[ne]=start; ee[ne]=n; ed[ne]=t[n]-t[start]; tot += t[n]-t[start] }
      printf "stalls (query counter flat >= %.0f s): n=%d total=%.1fs", MIN, ne, tot
      if (ne) { mx = 0; for (e = 1; e <= ne; e++) if (ed[e] > mx) mx = ed[e]; printf " max=%.1fs", mx }
      printf "\n"
      # Longest first, at most five: a table is evidence, a list of forty is noise.
      for (e = 1; e <= ne; e++) ord[e] = e
      for (a = 1; a <= ne; a++) for (b = a+1; b <= ne; b++)
        if (ed[ord[b]] > ed[ord[a]]) { tmp = ord[a]; ord[a] = ord[b]; ord[b] = tmp }
      for (r = 1; r <= ne && r <= 5; r++) { e = ord[r]; s = es[e]; f = ee[e]
        printf "  t=%-7.1f %6.1fs  mem_full=+%.1fs io_full=+%.1fs cpu_some=+%.1fs pgsteal_direct=+%d refault=+%d cur=%.0f file=%.0f\n", \
               t[s], ed[e], (psf[f]-psf[s])/1e6, (iof[f]-iof[s])/1e6, (cps[f]-cps[s])/1e6, \
               stl[f]-stl[s], rf[f]-rf[s], cur[f]/m, file[f]/m
      }
      d = t[n] - t[1]
      printf "run totals: mem_full=%.1fs (%.2f%%) io_full=%.1fs (%.2f%%) cpu_some=%.1fs (%.2f%%) pgsteal_direct=%d refault=%d\n", \
             (psf[n]-psf[1])/1e6, 100.0*(psf[n]-psf[1])/1e6/d, \
             (iof[n]-iof[1])/1e6, 100.0*(iof[n]-iof[1])/1e6/d, \
             (cps[n]-cps[1])/1e6, 100.0*(cps[n]-cps[1])/1e6/d, stl[n]-stl[1], rf[n]-rf[1]
    }' "$OUT/mem.csv"
  # Where an accepted push's server-side time went. Every push in the process
  # is written by one task, so queue/write/fsync/insert are the whole of it.
  # The percentiles are bucket bounds, not interpolations: the histogram is
  # cumulative in `le`, so what this can honestly say is "p95 is at or below
  # this bound", and saying more would be inventing resolution the buckets do
  # not have.
  awk '
    /^signy_(journal|flush)_.*_bucket\{le="/ {
      split($1, a, "_bucket"); name = a[1]
      split($1, b, "\""); bound = b[2]
      if (bound == "+Inf") next
      if (!(name in seen)) { seen[name] = ++order; oname[order] = name }
      k = name SUBSEP (bound + 0); cum[k] = $2 + 0
      bounds[name SUBSEP (++nb[name])] = bound + 0
    }
    /^signy_(journal|flush)_.*_count / { split($1, a, "_count"); total[a[1]] = $2 + 0 }
    /^signy_(journal|flush)_.*_sum / { split($1, a, "_sum"); sum[a[1]] = $2 + 0 }
    /^signy_journal_batches_total /{ batches = $2 + 0 }
    /^signy_journal_batched_records_total /{ records = $2 + 0 }
    /^signy_flush_rows_total /{ frows = $2 + 0 }
    /^signy_flush_parts_total /{ fparts = $2 + 0 }
    END {
      if (!order) exit
      printf "push and flush phases (one writer task, one flush loop; ms, bucketed upper bounds):\n"
      printf "  %-26s %10s %8s %8s %8s\n", "phase", "n", "mean", "p50<=", "p95<="
      for (o = 1; o <= order; o++) {
        name = oname[o]; n = total[name]; if (!n) continue
        p50 = ""; p95 = ""
        for (i = 1; i <= nb[name]; i++) {
          bd = bounds[name SUBSEP i]; c = cum[name SUBSEP bd]
          if (p50 == "" && c >= 0.50 * n) p50 = bd
          if (p95 == "" && c >= 0.95 * n) p95 = bd
        }
        short = name; sub(/^signy_/, "", short); sub(/_ms$/, "", short)
        printf "  %-26s %10d %8.2f %8s %8s\n", short, n, sum[name] / n, \
               (p50 == "" ? ">30000" : p50), (p95 == "" ? ">30000" : p95)
      }
      if (batches) printf "  batches=%d records=%d records/batch=%.2f\n", \
                          batches, records, records / batches
      if (fparts) printf "  flush rows=%d parts=%d rows/part=%.0f\n", \
                          frows, fparts, frows / fparts
    }' "$OUT/metrics-final.txt" 2>/dev/null
  # Did stopping the work give the memory back? The window before the load
  # stopped against the tail of the settle window, on the numbers that decide
  # it. A resident that falls toward live was holding freed memory the decay
  # policy had not returned; one that does not fall is holding memory that is
  # either still live or pinned, and those are the engine's problem rather than
  # the allocator's.
  if [ -f "$OUT/sampler_t0" ] && [ "$SETTLE" -gt 0 ] 2>/dev/null; then
    awk -F, -v t0="$(cat "$OUT/sampler_t0")" -v stopped="$LOAD_STOPPED_AT" -v settle="$SETTLE" '
      BEGIN { at = stopped - t0; window = 30 }
      NR>1 {
        t = $1 + 0
        if (t > at - window && t <= at) { bn++; ba+=$4; br+=$53; bc+=$54; bl+=$55; bv+=$57 }
        if (t >= at + settle - window) { an++; aa+=$4; ar+=$53; ac+=$54; al+=$55; av+=$57 }
      }
      END {
        if (!bn || !an) { printf "settle: not enough samples (before=%d after=%d)\n", bn, an; exit }
        m = 1048576.0
        printf "settle (%.0fs with the load stopped), mean of 30 s before and 30 s after:\n", settle
        printf "  %-16s %10s %10s %10s\n", "mib", "loaded", "settled", "returned"
        printf "  %-16s %10.1f %10.1f %10.1f\n", "anon",      ba/bn/m, aa/an/m, (ba/bn-aa/an)/m
        printf "  %-16s %10.1f %10.1f %10.1f\n", "proc_rss",  br/bn/m, ar/an/m, (br/bn-ar/an)/m
        if (bc > 0) printf "  %-16s %10.1f %10.1f %10.1f\n", "alloc_committed", bc/bn/m, ac/an/m, (bc/bn-ac/an)/m
        if (bl > 0) printf "  %-16s %10.1f %10.1f %10.1f\n", "alloc_live", bl/bn/m, al/an/m, (bl/bn-al/an)/m
        if (bv > 0) {
          printf "  %-16s %10.1f %10.1f %10.1f\n", "alloc_active", bv/bn/m, av/an/m, (bv/bn-av/an)/m
          printf "  fragmentation (active-live) %.1f -> %.1f MiB; awaiting decay (committed-active) %.1f -> %.1f MiB\n", \
                 (bv/bn-bl/bn)/m, (av/an-al/an)/m, (bc/bn-bv/bn)/m, (ac/an-av/an)/m
        }
      }' "$OUT/mem.csv"
  fi
  # The metric leg, from the harness's own result rather than from the gauges:
  # what was offered, what was kept, and whether every read shape that must
  # return rows kept returning them. `jq` is already a dependency of
  # compare/run_metric_capacity.sh; without it the block is skipped rather than
  # the verdict failing.
  if command -v jq >/dev/null 2>&1 && [ -f "$OUT/load.json" ]; then
    jq -r '
      .behavioral.metric_leg as $leg |
      if ($leg == null) or ($leg.enabled != true) then "metric leg: off"
      else
        "metric leg: pass=\($leg.pass) scrapes=\($leg.ingest.scrapes) late=\($leg.ingest.scrapes_late) " +
        "datapoints offered=\($leg.ingest.datapoints_offered) accepted=\($leg.ingest.datapoints_accepted) " +
        "acceptance=\($leg.ingest.acceptance) refused_requests=\($leg.ingest.requests_refused) errors=\($leg.ingest.errors)",
        "  server: active_series=\($leg.server.active_series_end) metric_parts=\($leg.server.metric_part_count_end) " +
        "created=\($leg.server.series_created) retired_flushed=\($leg.server.series_retired_flushed) " +
        "evicted_idle=\($leg.server.series_evicted_idle) cardinality_rejected=\($leg.server.cardinality_rejected) " +
        "memory_rejected=\($leg.server.memory_rejected)",
        "  reads: answered=\($leg.queries.answered) errors=\($leg.queries.errors) throttled=\($leg.queries.throttled)",
        ($leg.queries.per_shape | to_entries[] |
          "    \(.key | .[0:22]): issued=\(.value.issued) judged=\(.value.judged) series=\(.value.series_returned) " +
          "empty=\(.value.empty_answers) p95=\(.value.latency_ms.response.p95_ms // "-")"),
        (if ($leg.shapes_that_answered_nothing | length) > 0
         then "  SHAPES THAT ANSWERED NOTHING: \($leg.shapes_that_answered_nothing | join(", "))"
         else empty end),
        (if $leg.ingest.first_error then "  first ingest error: \($leg.ingest.first_error)" else empty end),
        (if $leg.queries.first_error then "  first read error: \($leg.queries.first_error)" else empty end)
      end' "$OUT/load.json" 2>/dev/null

    # What a retained datapoint costs on disk. An estimate, and labelled one:
    # the metrics directory holds about `retention` seconds of accepted
    # datapoints, so this divides the steady directory size by that many. The
    # mix is printed with it because a gauge point and a histogram point are
    # not the same point, and this run's ratio is neither one alone.
    RETENTION_SECONDS=$(awk -v r="$RETENTION" 'BEGIN{
      if (r ~ /^[0-9]+s$/) { printf "%d", r + 0 }
      else if (r ~ /^[0-9]+m$/) { printf "%d", (r + 0) * 60 }
      else if (r ~ /^[0-9]+h$/) { printf "%d", (r + 0) * 3600 }
      else { print 0 } }')
    ACCEPTED=$(jq -r '.behavioral.metric_leg.ingest.datapoints_accepted // 0' "$OUT/load.json")
    ELAPSED=$(jq -r '.run.elapsed_seconds // 0' "$OUT/load.json")
    awk -F, -v accepted="$ACCEPTED" -v elapsed="$ELAPSED" -v retention="$RETENTION_SECONDS" '
      NR>1 && ($10+0>0 || $14+0>0) { n++; last = $51 + 0 }
      END {
        if (!n || retention <= 0 || elapsed <= 0 || accepted <= 0) exit
        held = accepted / elapsed * retention
        if (held <= 0) exit
        printf "metrics on disk: %.1f MiB holding ~%.0f datapoints, %.0f B/datapoint (mixed instruments)\n", \
               last / 1048576.0, held, last / held
      }' "$OUT/mem.csv"
  fi
  echo "slow batches (>=250 ms), longest first:"
  sed 's/\x1b\[[0-9;]*m//g' "$SERVER_LOG" | grep "journal batch slow" \
    | sed 's/.*records=/records=/' | sort -t= -k4 -rn | head -5
  echo "server_log_tail:"; sed 's/\x1b\[[0-9;]*m//g' "$SERVER_LOG" | tail -3
} | tee "$OUT/verdict.txt"
