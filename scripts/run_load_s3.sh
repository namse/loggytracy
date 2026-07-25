#!/usr/bin/env bash
# Tier C: run the M7 load harness against the real S3 protocol, backed by a
# local MinIO. This is what actually exercises the AmazonS3 code path -- manifest
# CAS (conditional put), multipart upload, path-style addressing, and real socket
# latency for eviction->restore round trips -- that the in-memory and file
# backends never touch.
#
# It brings MinIO up via docker compose, creates the bucket, launches the release
# server pointed at s3://loggytracy/loggytracy, runs the closed-loop load tool,
# scrapes /metrics, and tears everything down.
set -euo pipefail

cd "$(dirname "$0")/.."

BUCKET="${LOGGYTRACY_S3_BUCKET:-loggytracy}"
PREFIX="${LOGGYTRACY_S3_PREFIX:-loggytracy}"
ENDPOINT="${LOGGYTRACY_S3_ENDPOINT:-http://127.0.0.1:9000}"
HTTP_ADDR="${LOGGYTRACY_HTTP_ADDR:-127.0.0.1:3100}"
OTLP_ADDR="${LOGGYTRACY_OTLP_ADDR:-127.0.0.1:4317}"
DATA_DIR="${LOGGYTRACY_DATA_DIR:-$(mktemp -d)}"
RESULT_PATH="${LOGGYTRACY_LOAD_RESULT_PATH:-docs/m7_tier_c_result.json}"
SERVER_LOG="${LOGGYTRACY_SERVER_LOG:-$(mktemp)}"

echo "==> data dir:    $DATA_DIR"
echo "==> server log:  $SERVER_LOG"
echo "==> result path: $RESULT_PATH"

SERVER_PID=""
cleanup() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "==> stopping server ($SERVER_PID)"
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  echo "==> docker compose down"
  docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> docker compose up -d minio"
docker compose up -d minio
echo "==> waiting for MinIO bucket provisioning"
docker compose up minio-init

echo "==> building release binaries"
cargo build --release --bin loggytracy --bin load

# object_store consumes these from the process environment. path-style is
# required because MinIO does not serve virtual-hosted-style buckets by default.
# OBJECT_STORE_CONDITIONAL_PUT=etag makes PutMode::Create (If-None-Match) manifest
# CAS work against MinIO; see docs/M7_LOAD_RESULTS.md for the verification of this.
export LOGGYTRACY_OBJECT_STORE_URL="s3://${BUCKET}/${PREFIX}"
export OBJECT_STORE_ENDPOINT="$ENDPOINT"
export OBJECT_STORE_ALLOW_HTTP="true"
export OBJECT_STORE_VIRTUAL_HOSTED_STYLE_REQUEST="false"
export OBJECT_STORE_CONDITIONAL_PUT="${OBJECT_STORE_CONDITIONAL_PUT:-etag}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export LOGGYTRACY_DATA_DIR="$DATA_DIR"
export LOGGYTRACY_LISTEN_ADDR="$HTTP_ADDR"
export LOGGYTRACY_OTLP_GRPC_ADDR="$OTLP_ADDR"
# A small cache forces eviction, so the restore probes actually round-trip to
# MinIO. A short retention exercises the retention GC path on real objects.
export LOGGYTRACY_CACHE_MAX_BYTES="${LOGGYTRACY_CACHE_MAX_BYTES:-33554432}"
export LOGGYTRACY_CACHE_EVICTION_INTERVAL="${LOGGYTRACY_CACHE_EVICTION_INTERVAL:-5s}"
export LOGGYTRACY_FLUSH_MAX_INTERVAL="${LOGGYTRACY_FLUSH_MAX_INTERVAL:-2s}"
export LOGGYTRACY_MERGE_INTERVAL="${LOGGYTRACY_MERGE_INTERVAL:-10s}"

echo "==> starting server"
./target/release/loggytracy >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

echo "==> waiting for readiness"
for _ in $(seq 1 60); do
  if curl -fsS "http://${HTTP_ADDR}/ready" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "server exited early; log follows:" >&2
    cat "$SERVER_LOG" >&2
    exit 1
  fi
  sleep 1
done

echo "==> running load harness (Tier C)"
LOGGYTRACY_LOAD_TIER="C" \
LOGGYTRACY_LOAD_ADDR="$HTTP_ADDR" \
LOGGYTRACY_LOAD_OTLP_ADDR="$OTLP_ADDR" \
LOGGYTRACY_LOAD_RESULT_PATH="$RESULT_PATH" \
LOGGYTRACY_BUILD_REVISION="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" \
LOGGYTRACY_LOAD_SECONDS="${LOGGYTRACY_LOAD_SECONDS:-60}" \
LOGGYTRACY_LOAD_WARMUP_SECONDS="${LOGGYTRACY_LOAD_WARMUP_SECONDS:-10}" \
LOGGYTRACY_LOAD_TARGET_EPS="${LOGGYTRACY_LOAD_TARGET_EPS:-2000}" \
  ./target/release/load

echo "==> final /metrics"
curl -fsS "http://${HTTP_ADDR}/metrics" || true

echo "==> Tier C run complete; result written to $RESULT_PATH"
