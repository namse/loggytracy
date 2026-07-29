#!/usr/bin/env bash
#
# Runs a local server and the load harness against it.
#
# Two rules this script exists to keep:
#
#   1. It is path-independent. The previous version `cd`ed to
#      /Users/namse/loggytracy, so the only reproduction script in the
#      repository could not run on any other machine — including the one the
#      results were later re-checked on. The repository root is derived from
#      the script's own location.
#   2. Every knob a run might want to vary is a default, not an assignment.
#      Unconditional `export`s silently discarded what the caller passed: a run
#      invoked with a longer merge interval got the script's value instead and
#      produced numbers that looked like engine behaviour
#      (`docs/LOAD_RESULTS.md` §4). If you add a knob here, add it as
#      `${NAME:-default}`.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO_ROOT"

# `release` by default because a debug harness measures the harness. `debug` is
# for checking that this script and the harness run at all.
PROFILE="${LOGGYTRACY_LOAD_PROFILE:-release}"
case "$PROFILE" in
  release) BIN_DIR="target/release"; BUILD_FLAGS=(--release) ;;
  debug)   BIN_DIR="target/debug";   BUILD_FLAGS=() ;;
  *) echo "LOGGYTRACY_LOAD_PROFILE must be release or debug, got $PROFILE" >&2; exit 1 ;;
esac

if [ "${LOGGYTRACY_LOAD_SKIP_BUILD:-0}" != "1" ]; then
  cargo build "${BUILD_FLAGS[@]}" --bin loggytracy --bin load
fi

DATA_DIR=$(mktemp -d)
REMOTE_DIR=$(mktemp -d)
SERVER_LOG=$(mktemp)
# Not a checked-in path by default: a run that overwrites a published artifact
# by accident is how a document ends up disagreeing with the artifact it cites.
RESULT="${LOGGYTRACY_LOAD_RESULT_PATH:-$REPO_ROOT/target/load_result.json}"

echo "repo=$REPO_ROOT profile=$PROFILE data=$DATA_DIR remote=$REMOTE_DIR log=$SERVER_LOG result=$RESULT"

cleanup() {
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$DATA_DIR" "$REMOTE_DIR"
}
trap cleanup EXIT

LISTEN_ADDR="${LOGGYTRACY_LISTEN_ADDR:-127.0.0.1:3100}"
OTLP_ADDR="${LOGGYTRACY_OTLP_GRPC_ADDR:-127.0.0.1:4317}"

export LOGGYTRACY_DATA_DIR="${LOGGYTRACY_DATA_DIR:-$DATA_DIR}"
export LOGGYTRACY_OBJECT_STORE_URL="${LOGGYTRACY_OBJECT_STORE_URL:-file://$REMOTE_DIR}"
export LOGGYTRACY_LISTEN_ADDR="$LISTEN_ADDR"
export LOGGYTRACY_OTLP_GRPC_ADDR="$OTLP_ADDR"
# Tier B fault injection (seeded, reproducible).
export LOGGYTRACY_OBJECT_STORE_LATENCY_MS="${LOGGYTRACY_OBJECT_STORE_LATENCY_MS:-5}"
export LOGGYTRACY_OBJECT_STORE_LATENCY_JITTER_MS="${LOGGYTRACY_OBJECT_STORE_LATENCY_JITTER_MS:-10}"
export LOGGYTRACY_OBJECT_STORE_READ_LATENCY_MS="${LOGGYTRACY_OBJECT_STORE_READ_LATENCY_MS:-20}"
export LOGGYTRACY_OBJECT_STORE_ERROR_RATE="${LOGGYTRACY_OBJECT_STORE_ERROR_RATE:-0.03}"
export LOGGYTRACY_OBJECT_STORE_FAULT_SEED="${LOGGYTRACY_OBJECT_STORE_FAULT_SEED:-20260724}"
# Force eviction->restore and exercise merge/retention on the measured path.
export LOGGYTRACY_CACHE_MAX_BYTES="${LOGGYTRACY_CACHE_MAX_BYTES:-8388608}"
export LOGGYTRACY_CACHE_EVICTION_INTERVAL="${LOGGYTRACY_CACHE_EVICTION_INTERVAL:-3s}"
export LOGGYTRACY_FLUSH_MAX_INTERVAL="${LOGGYTRACY_FLUSH_MAX_INTERVAL:-2s}"
export LOGGYTRACY_MERGE_INTERVAL="${LOGGYTRACY_MERGE_INTERVAL:-8s}"
export LOGGYTRACY_RETENTION_PERIOD="${LOGGYTRACY_RETENTION_PERIOD:-20s}"
export LOGGYTRACY_RETENTION_GRACE_PERIOD="${LOGGYTRACY_RETENTION_GRACE_PERIOD:-5s}"
export LOGGYTRACY_RETENTION_INTERVAL="${LOGGYTRACY_RETENTION_INTERVAL:-5s}"

"./$BIN_DIR/loggytracy" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 60); do
  if curl -fsS "http://$LISTEN_ADDR/ready" >/dev/null 2>&1; then break; fi
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

# The harness exits non-zero when its verdict is not PASS, which is the point of
# having a verdict. `set -e` must not cut off the server log that explains why.
STATUS=0
LOGGYTRACY_LOAD_SERVER_PID="$SERVER_PID" \
LOGGYTRACY_LOAD_ADDR="${LOGGYTRACY_LOAD_ADDR:-$LISTEN_ADDR}" \
LOGGYTRACY_LOAD_OTLP_ADDR="${LOGGYTRACY_LOAD_OTLP_ADDR:-$OTLP_ADDR}" \
LOGGYTRACY_LOAD_TIER="${LOGGYTRACY_LOAD_TIER:-B}" \
LOGGYTRACY_LOAD_RESULT_PATH="$RESULT" \
LOGGYTRACY_BUILD_REVISION="${LOGGYTRACY_BUILD_REVISION:-$(git rev-parse --short HEAD)}" \
LOGGYTRACY_MACHINE_PROFILE="$LOGGYTRACY_MACHINE_PROFILE" \
  "./$BIN_DIR/load" || STATUS=$?

echo "=== server log tail ==="
tail -20 "$SERVER_LOG"
echo "=== harness exit status: $STATUS (0 = PASS) ==="
exit "$STATUS"
