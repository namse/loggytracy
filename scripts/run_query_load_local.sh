#!/usr/bin/env bash
set -euo pipefail

# Path-independent for the reason `run_load_local.sh` states: a script that
# `cd`s to one developer's home directory is not a reproduction script.
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO_ROOT"

# Query-heavy profile.
#
# `run_load_local.sh` drives ingest and issues one query per N pushes from the
# same loop, so query concurrency there is one by construction. That is why the
# largest term in the configured memory budget —
# `max_concurrent_query_scans x max_query_memory_bytes`, four of five GiB at the
# defaults — has never been exercised by a load run. This script drives the
# other axis: modest ingest, many independent readers, and a retention period
# long enough that a wide scan has something to read.

DATA_DIR=$(mktemp -d)
REMOTE_DIR=$(mktemp -d)
SERVER_LOG=$(mktemp)
RESULT="${LOGGYTRACY_LOAD_RESULT_PATH:-docs/query_heavy_result.json}"

echo "data=$DATA_DIR remote=$REMOTE_DIR log=$SERVER_LOG"

cleanup() {
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

export LOGGYTRACY_DATA_DIR="$DATA_DIR"
export LOGGYTRACY_OBJECT_STORE_URL="file://$REMOTE_DIR"
export LOGGYTRACY_LISTEN_ADDR="127.0.0.1:3100"
export LOGGYTRACY_OTLP_GRPC_ADDR="127.0.0.1:4317"
# Tier B fault injection (seeded, reproducible). Read latency is what a restore
# pays, so it stays on: the point of the small cache below is that the readers
# miss.
export LOGGYTRACY_OBJECT_STORE_LATENCY_MS="${LOGGYTRACY_OBJECT_STORE_LATENCY_MS:-5}"
export LOGGYTRACY_OBJECT_STORE_LATENCY_JITTER_MS="${LOGGYTRACY_OBJECT_STORE_LATENCY_JITTER_MS:-10}"
export LOGGYTRACY_OBJECT_STORE_READ_LATENCY_MS="${LOGGYTRACY_OBJECT_STORE_READ_LATENCY_MS:-20}"
export LOGGYTRACY_OBJECT_STORE_ERROR_RATE="${LOGGYTRACY_OBJECT_STORE_ERROR_RATE:-0.03}"
export LOGGYTRACY_OBJECT_STORE_FAULT_SEED="${LOGGYTRACY_OBJECT_STORE_FAULT_SEED:-20260728}"

# Far below the 8 MiB the ingest profile uses. The payload is padded with a
# repeated byte, so parquet compresses a 300k-event run down to single-digit
# megabytes and an 8 MiB cache holds all of it — the first attempt at this
# profile evicted thirteen times and restored zero, because nothing a reader
# wanted had ever left the disk.
export LOGGYTRACY_CACHE_MAX_BYTES="${LOGGYTRACY_CACHE_MAX_BYTES:-262144}"
export LOGGYTRACY_CACHE_EVICTION_INTERVAL="${LOGGYTRACY_CACHE_EVICTION_INTERVAL:-3s}"
export LOGGYTRACY_FLUSH_MAX_INTERVAL="${LOGGYTRACY_FLUSH_MAX_INTERVAL:-2s}"
export LOGGYTRACY_MERGE_INTERVAL="${LOGGYTRACY_MERGE_INTERVAL:-8s}"
# Retention has to outlast the query range or every wide scan reads an empty
# window and the run measures the scheduler admitting scans that do nothing.
export LOGGYTRACY_RETENTION_PERIOD="${LOGGYTRACY_RETENTION_PERIOD:-600s}"
export LOGGYTRACY_RETENTION_GRACE_PERIOD="${LOGGYTRACY_RETENTION_GRACE_PERIOD:-5s}"
export LOGGYTRACY_RETENTION_INTERVAL="${LOGGYTRACY_RETENTION_INTERVAL:-30s}"
# The term this profile exists to exercise. Named here rather than passed to the
# harness: it is the server that admits scans.
export LOGGYTRACY_MAX_CONCURRENT_QUERY_SCANS="${LOGGYTRACY_MAX_CONCURRENT_QUERY_SCANS:-8}"

./target/release/loggytracy >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:3100/ready" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then echo "server died:"; cat "$SERVER_LOG"; exit 1; fi
  sleep 0.5
done

if [ -z "${LOGGYTRACY_MACHINE_PROFILE:-}" ]; then
  if [ "$(uname -s)" = "Darwin" ]; then
    CPUS=$(sysctl -n hw.ncpu 2>/dev/null || echo '?')
    RAM_GIB=$(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1073741824 ))
  else
    CPUS=$(nproc 2>/dev/null || echo '?')
    RAM_GIB=$(awk '/MemTotal/ {printf "%d", $2/1048576}' /proc/meminfo 2>/dev/null || echo 0)
  fi
  LOGGYTRACY_MACHINE_PROFILE="$(uname -sm); ${CPUS} logical CPUs; ${RAM_GIB} GiB RAM"
fi

LOGGYTRACY_LOAD_SERVER_PID="$SERVER_PID" \
LOGGYTRACY_LOAD_TIER=B \
LOGGYTRACY_LOAD_RESULT_PATH="$RESULT" \
LOGGYTRACY_BUILD_REVISION="$(git rev-parse --short HEAD)" \
LOGGYTRACY_MACHINE_PROFILE="$LOGGYTRACY_MACHINE_PROFILE" \
LOGGYTRACY_LOAD_SECONDS="${LOGGYTRACY_LOAD_SECONDS:-150}" \
LOGGYTRACY_LOAD_WARMUP_SECONDS="${LOGGYTRACY_LOAD_WARMUP_SECONDS:-45}" \
LOGGYTRACY_LOAD_TARGET_EPS="${LOGGYTRACY_LOAD_TARGET_EPS:-2000}" \
LOGGYTRACY_LOAD_ENTRIES_PER_PUSH="${LOGGYTRACY_LOAD_ENTRIES_PER_PUSH:-100}" \
LOGGYTRACY_LOAD_TENANTS="${LOGGYTRACY_LOAD_TENANTS:-24}" \
LOGGYTRACY_LOAD_QUERY_CONNECTIONS="${LOGGYTRACY_LOAD_QUERY_CONNECTIONS:-24}" \
LOGGYTRACY_LOAD_QUERY_WINDOW_SECONDS="${LOGGYTRACY_LOAD_QUERY_WINDOW_SECONDS:-120}" \
LOGGYTRACY_LOAD_QUERY_LIMIT="${LOGGYTRACY_LOAD_QUERY_LIMIT:-5000}" \
  ./target/release/load

echo "=== server log tail ==="
tail -20 "$SERVER_LOG"
