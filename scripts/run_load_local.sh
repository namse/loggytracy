#!/usr/bin/env bash
set -euo pipefail
cd /Users/namse/loggytracy

DATA_DIR=$(mktemp -d)
REMOTE_DIR=$(mktemp -d)
SERVER_LOG=$(mktemp)
RESULT="${LOGGYTRACY_LOAD_RESULT_PATH:-docs/m7_tier_b_result.json}"

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
# Tier B fault injection (seeded, reproducible).
export LOGGYTRACY_OBJECT_STORE_LATENCY_MS=5
export LOGGYTRACY_OBJECT_STORE_LATENCY_JITTER_MS=10
export LOGGYTRACY_OBJECT_STORE_READ_LATENCY_MS=20
export LOGGYTRACY_OBJECT_STORE_ERROR_RATE=0.03
export LOGGYTRACY_OBJECT_STORE_FAULT_SEED=20260724
# Force eviction->restore and exercise merge/retention on the measured path.
export LOGGYTRACY_CACHE_MAX_BYTES=8388608
export LOGGYTRACY_CACHE_EVICTION_INTERVAL=3s
export LOGGYTRACY_FLUSH_MAX_INTERVAL=2s
export LOGGYTRACY_MERGE_INTERVAL=8s
export LOGGYTRACY_RETENTION_PERIOD=20s
export LOGGYTRACY_RETENTION_GRACE_PERIOD=5s
export LOGGYTRACY_RETENTION_INTERVAL=5s

./target/release/loggytracy >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:3100/ready" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then echo "server died:"; cat "$SERVER_LOG"; exit 1; fi
  sleep 0.5
done

LOGGYTRACY_LOAD_TIER=B \
LOGGYTRACY_LOAD_RESULT_PATH="$RESULT" \
LOGGYTRACY_BUILD_REVISION="$(git rev-parse --short HEAD)" \
LOGGYTRACY_MACHINE_PROFILE="$(uname -sm); $(sysctl -n hw.ncpu 2>/dev/null || echo '?') logical CPUs; $(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1073741824 )) GiB RAM (developer laptop, not 4-vCPU/16-GiB reference)" \
LOGGYTRACY_LOAD_SECONDS="${LOGGYTRACY_LOAD_SECONDS:-45}" \
LOGGYTRACY_LOAD_WARMUP_SECONDS="${LOGGYTRACY_LOAD_WARMUP_SECONDS:-10}" \
LOGGYTRACY_LOAD_TARGET_EPS="${LOGGYTRACY_LOAD_TARGET_EPS:-3000}" \
LOGGYTRACY_LOAD_ENTRIES_PER_PUSH="${LOGGYTRACY_LOAD_ENTRIES_PER_PUSH:-100}" \
LOGGYTRACY_LOAD_EVENT_BYTES="${LOGGYTRACY_LOAD_EVENT_BYTES:-1024}" \
  ./target/release/load

echo "=== server log tail ==="
tail -20 "$SERVER_LOG"
