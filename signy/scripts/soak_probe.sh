#!/usr/bin/env bash
#
# The feature probe: every read route, on a schedule, for as long as a soak
# runs.
#
#   scripts/soak_probe.sh <out-dir>
#
# A soak already says whether the engine stays inside its memory, whether parts
# and the WAL reach a steady state, and whether flush, merge and retention keep
# turning. It says nothing about whether the product still works. The load
# harness drives six log query shapes and five metric ones; the other eleven
# routes -- the autocompletes, the tail, the deletion surface, the trace
# timeline, the admin lifecycle -- had never been asked anything by a long run,
# so a route that started answering 500 on hour nine would have finished the
# soak with a clean verdict.
#
# So this walks the whole surface in docs/QUERY_API.md plus the admin routes,
# once every PROBE_INTERVAL, and writes one CSV row per probe per round. The
# artifact is a table of per-route success rates over 24 hours, which is what
# "everything still works" has to mean if it is to mean anything.
#
# What it does not do is judge. Rows go in the file; the verdict is computed at
# the end, over the whole run, where a single 429 in hour three can be seen for
# what it is instead of failing a round.
#
#   PROBE_INTERVAL=60 scripts/soak_probe.sh target/soak/run
#   PROBE_ROUNDS=1    scripts/soak_probe.sh target/soak/run   # one pass, for a smoke
set -uo pipefail

OUT="${1:?usage: soak_probe.sh <out-dir>}"
mkdir -p "$OUT"

ADDR="${PROBE_ADDR:-127.0.0.1:3190}"
BASE="http://$ADDR"
API="$BASE/signy/api/v1"
# The tenant the load harness writes logs, traces and metrics under: its corpus
# names them `<prefix>-<index>` and every leg uses the first.
TENANT="${PROBE_TENANT:-load-tenant-000}"
# A tenant of this probe's own, for the parts of the admin surface that create
# and destroy. Never one the load is writing to: a retention policy deleted out
# from under the harness would un-serve its tenant and drop every push after it.
ADMIN_TENANT="${PROBE_ADMIN_TENANT:-probe-tenant}"
INTERVAL="${PROBE_INTERVAL:-300}"
ROUNDS="${PROBE_ROUNDS:-0}"
# Instruments the metric leg publishes (src/bin/load/metric_workload.rs). A
# name this instance never had would make every metric probe read as a failure
# of the engine rather than of this script's guess.
METRIC="${PROBE_METRIC:-http_requests_total}"
HISTOGRAM="${PROBE_HISTOGRAM:-http_request_duration_seconds}"
# How long the tail is held open. Past the ~15 s keep-alive on purpose: on a
# tenant with nothing arriving, the heartbeat is the only thing that proves the
# stream is alive, and a window shorter than it would fail every round after the
# load stops.
TAIL_SECONDS="${PROBE_TAIL_SECONDS:-18}"
# Rounds between deletion-surface probes. Not every round: a request the engine
# promotes to `processed` before the probe withdraws it can no longer be
# withdrawn and stays in the tenant's listing for good, and a tenant may hold
# only MAX_DELETE_REQUESTS_PER_TENANT of them. Every sixth round is 48 probes
# across a day, which is far under that and still says the surface works.
DELETE_EVERY="${PROBE_DELETE_EVERY:-6}"

CSV="$OUT/probe.csv"
BODY=$(mktemp); HEAD=$(mktemp)
trap 'rm -f "$BODY" "$HEAD"' EXIT
[ -f "$CSV" ] || echo "round,t,probe,status,rows,ok,detail" >"$CSV"

# A tenant is served when a retention policy has been pushed for it, and the
# load harness pushes those for its own tenants as it starts. This probe is
# started first, so without this wait its first round asks every route about a
# tenant that does not exist yet and records the whole surface as broken.
wait_for_tenant() {
  local waited=0
  while [ "$waited" -lt "${PROBE_TENANT_WAIT:-600}" ]; do
    if [ "$(req GET "$API/admin/tenants/$TENANT/retention")" = "200" ]; then
      return 0
    fi
    sleep 5
    waited=$((waited + 5))
  done
  echo "tenant $TENANT was never onboarded; probing anyway"
}

T0=$(date +%s)
ROUND=0
PASS=0
FAIL=0
SKIP=0

# One request. Leaves the body in $BODY and prints the status code, or `000`
# when nothing answered -- which is a failure the same as a 500 and has to be
# distinguishable from one in the file.
req() {
  local method="$1" url="$2" tenant="${3:-$TENANT}" code
  # Emptied first: a curl that cannot connect leaves the previous probe's body
  # in place, and the failure would then be recorded with another route's
  # answer as its evidence.
  : >"$BODY"
  # No `|| echo`: curl prints `000` for a connection that never happened and
  # exits non-zero as well, so a fallback echo would put two codes in the
  # substitution and a newline in the middle of a CSV row.
  code=$(curl -sS -o "$BODY" -D "$HEAD" -w '%{http_code}' \
    --max-time 30 -X "$method" -H "X-Tenant-Id: $tenant" "$url" 2>/dev/null)
  echo "${code:-000}"
}

# Non-empty lines in the last body. awk rather than `grep -c`, which exits 1 on
# a count of zero and would put a second line in the substitution — a newline in
# the middle of a CSV row, which reads back as a row of its own.
rows() {
  local n
  n=$(awk 'NF { n++ } END { print n + 0 }' "$BODY" 2>/dev/null)
  echo "${n:-0}"
}

# Record one probe. `ok` is the caller's verdict; `detail` is whatever makes a
# failure diagnosable a day later without the server still being up.
# `ok` is 1, 0, or `skip`. Skipped is its own state and not a quiet failure:
# a probe this round deliberately did not run says nothing about the route, and
# folding it into either column would make the end-of-run table a lie.
record() {
  local name="$1" status="$2" row_count="$3" ok="$4" detail="${5:-}"
  printf '%d,%d,%s,%s,%s,%s,"%s"\n' \
    "$ROUND" "$(( $(date +%s) - T0 ))" "$name" "$status" "$row_count" "$ok" \
    "${detail//\"/\'}" >>"$CSV"
  case "$ok" in
    1) PASS=$((PASS + 1)) ;;
    skip) SKIP=$((SKIP + 1)) ;;
    *) FAIL=$((FAIL + 1)) ;;
  esac
}

# The common case: a 200 whose body has at least $3 lines.
expect() {
  local name="$1" status="$2" want="${3:-1}"
  local got; got=$(rows)
  if [ "$status" = "200" ] && [ "$got" -ge "$want" ]; then
    record "$name" "$status" "$got" 1
  else
    record "$name" "$status" "$got" 0 "$(head -c 200 "$BODY" | tr -d '\n')"
  fi
}

# The first value of a JSON string field, off the first NDJSON line. The bodies
# here are one flat object per line, so this is enough and pulling in jq for it
# would put a dependency in the soak rig.
field() {
  head -1 "$BODY" | grep -o "\"$1\":\"[^\"]*\"" | head -1 | cut -d'"' -f4
}

round() {
  ROUND=$((ROUND + 1))
  PASS=0; FAIL=0; SKIP=0

  # --- liveness -----------------------------------------------------------
  status=$(req GET "$BASE/ready")
  if [ "$status" != "200" ]; then
    # The engine is not answering. Every route would fail, and recording
    # twenty-eight failures would say the surface broke when what happened is
    # that the process was down -- which `ready` above already records, and
    # which the run's fault log explains. So the rest of the round is skipped.
    record ready "$status" 0 0 "the engine did not answer /ready"
    for route in metrics logs logs_filtered logs_histogram logs_attributes \
                 logs_attribute_values logs_tail delete_submit delete_list \
                 delete_cancel traces_search traces_by_id traces_attributes \
                 traces_attribute_values metrics_names metrics_labels \
                 metrics_label_values metrics_series metrics_query \
                 metrics_instant metrics_quantile admin_list \
                 admin_retention_get admin_usage admin_retention_put \
                 admin_retention_delete unknown_route; do
      record "$route" skipped 0 skip "the engine was not ready"
    done
    echo "round $ROUND at +$(( $(date +%s) - T0 ))s: engine not ready, round skipped"
    return
  fi
  record ready "$status" 0 1
  status=$(req GET "$BASE/metrics")
  if [ "$status" = "200" ] && grep -q '^signy_ingest_requests_total' "$BODY"; then
    record metrics "$status" "$(rows)" 1
  else
    record metrics "$status" "$(rows)" 0 "no signy_ingest_requests_total"
  fi

  # --- logs ---------------------------------------------------------------
  status=$(req GET "$API/logs?start=-5m&limit=5")
  expect logs "$status" 1

  # The filtering grammar rather than the route: a line filter, an attribute
  # matcher, a parser and a direction in one request. An empty answer is
  # legitimate here -- the filters may match nothing in the window -- so this
  # one is gated on the status alone.
  status=$(req GET "$API/logs?start=-5m&limit=5&parse=logfmt&direction=forward&contains=e")
  [ "$status" = "200" ] && record logs_filtered "$status" "$(rows)" 1 \
    || record logs_filtered "$status" "$(rows)" 0 "$(head -c 200 "$BODY" | tr -d '\n')"

  status=$(req GET "$API/logs/histogram?start=-15m&bucket=1m")
  expect logs_histogram "$status" 1

  status=$(req GET "$API/logs/attributes?start=-5m")
  expect logs_attributes "$status" 1
  key=$(field key)
  if [ -n "$key" ]; then
    status=$(req GET "$API/logs/attributes/$key/values?start=-5m")
    expect logs_attribute_values "$status" 1
  else
    record logs_attribute_values skipped 0 skip "the keys probe offered none"
  fi

  # The tail is a stream, so it is timed out on purpose and whatever arrived is
  # the answer. A heartbeat counts: it is the route proving it is streaming on a
  # tenant that happens to be quiet.
  curl -sN --max-time "$TAIL_SECONDS" -H "X-Tenant-Id: $TENANT" \
    "$API/logs/tail?limit=10" >"$BODY" 2>/dev/null
  tailed=$(rows)
  [ "$tailed" -ge 1 ] && record logs_tail stream "$tailed" 1 \
    || record logs_tail stream "$tailed" 0 "nothing in ${TAIL_SECONDS}s"

  # --- deletion surface ---------------------------------------------------
  # Deliberately a selector nothing matches. Submitting, listing and cancelling
  # is the whole of what this can check without hiding rows the rest of the
  # soak is about to query.
  if [ $(( ROUND % DELETE_EVERY )) -ne 1 ]; then
    record delete_submit skipped 0 skip "runs every ${DELETE_EVERY} rounds"
    record delete_list skipped 0 skip "runs every ${DELETE_EVERY} rounds"
    record delete_cancel skipped 0 skip "runs every ${DELETE_EVERY} rounds"
    status=skipped
  else
  status=$(req POST "$API/logs/delete?attr=service_name=__probe_no_such_service__&start=-2m")
  if [ "$status" = "204" ]; then
    record delete_submit "$status" 0 1
    status=$(req GET "$API/logs/delete")
    expect delete_list "$status" 1
    id=$(field request_id)
    [ -n "$id" ] || id=$(head -1 "$BODY" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
    if [ -n "$id" ]; then
      status=$(req DELETE "$API/logs/delete?request_id=$id")
      if [ "$status" = "204" ]; then
        record delete_cancel "$status" 0 1
      elif grep -q "already been applied" "$BODY"; then
        # The request finished its life before the probe could withdraw it.
        # That is the deletion path working, not the cancel route failing.
        record delete_cancel "$status" 0 1 "applied before it could be withdrawn"
      else
        record delete_cancel "$status" 0 0 "$(head -c 200 "$BODY" | tr -d '\n')"
      fi
    else
      record delete_cancel skipped 0 0 "the listing named no request id"
    fi
  else
    record delete_submit "$status" 0 0 "$(head -c 200 "$BODY" | tr -d '\n')"
    record delete_list skipped 0 skip "no request was accepted"
    record delete_cancel skipped 0 skip "no request was accepted"
  fi
  fi

  # --- traces -------------------------------------------------------------
  status=$(req GET "$API/traces?start=-5m&limit=5")
  expect traces_search "$status" 1
  trace=$(field trace_id)
  if [ -n "$trace" ]; then
    status=$(req GET "$API/traces/$trace")
    expect traces_by_id "$status" 1
  else
    record traces_by_id skipped 0 skip "the search returned no trace to fetch"
  fi
  status=$(req GET "$API/traces/attributes?start=-5m")
  expect traces_attributes "$status" 1
  key=$(field key)
  if [ -n "$key" ]; then
    status=$(req GET "$API/traces/attributes/$key/values?start=-5m")
    expect traces_attribute_values "$status" 1
  else
    record traces_attribute_values skipped 0 skip "the keys probe offered none"
  fi

  # --- metrics ------------------------------------------------------------
  status=$(req GET "$API/metrics/names?start=-5m")
  expect metrics_names "$status" 1
  status=$(req GET "$API/metrics/labels?start=-5m&metric=$METRIC")
  expect metrics_labels "$status" 1
  key=$(field key)
  if [ -n "$key" ]; then
    status=$(req GET "$API/metrics/labels/$key/values?start=-5m&metric=$METRIC")
    expect metrics_label_values "$status" 1
  else
    record metrics_label_values skipped 0 skip "the keys probe offered none"
  fi
  status=$(req GET "$API/metrics/series?metric=$METRIC&start=-5m&limit=5")
  expect metrics_series "$status" 1
  status=$(req GET "$API/metrics/query?metric=$METRIC&start=-5m&step=30s&func=rate&range=60s&agg=sum&by=service")
  expect metrics_query "$status" 1
  status=$(req GET "$API/metrics/instant?metric=$METRIC&func=rate&range=60s&agg=max")
  expect metrics_instant "$status" 1
  status=$(req GET "$API/metrics/quantile?metric=$HISTOGRAM&q=0.99&start=-5m&step=30s&range=60s")
  expect metrics_quantile "$status" 1

  # --- admin --------------------------------------------------------------
  status=$(req GET "$API/admin/tenants")
  if [ "$status" = "200" ] && grep -q "$TENANT" "$BODY"; then
    record admin_list "$status" "$(rows)" 1
  else
    record admin_list "$status" "$(rows)" 0 "the listing does not name $TENANT"
  fi
  status=$(req GET "$API/admin/tenants/$TENANT/retention")
  expect admin_retention_get "$status" 1
  status=$(req GET "$API/admin/tenants/$TENANT/usage")
  expect admin_usage "$status" 1

  # The half of the lifecycle that writes, on a tenant of this probe's own.
  : >"$BODY"
  status=$(curl -sS -o "$BODY" -w '%{http_code}' --max-time 30 \
    -X PUT -H 'Content-Type: application/json' \
    -d '{"retention":"1h"}' "$API/admin/tenants/$ADMIN_TENANT/retention" 2>/dev/null)
  status="${status:-000}"
  if [ "$status" = "200" ] || [ "$status" = "204" ]; then
    record admin_retention_put "$status" 0 1
    status=$(req DELETE "$API/admin/tenants/$ADMIN_TENANT/retention" "$ADMIN_TENANT")
    { [ "$status" = "200" ] || [ "$status" = "204" ]; } \
      && record admin_retention_delete "$status" 0 1 \
      || record admin_retention_delete "$status" 0 0 "$(head -c 200 "$BODY" | tr -d '\n')"
  else
    record admin_retention_put "$status" 0 0 "$(head -c 200 "$BODY" | tr -d '\n')"
    record admin_retention_delete skipped 0 skip "no policy was written to withdraw"
  fi

  # --- the fallback -------------------------------------------------------
  # A 404 that lists the real routes is a feature of the API, and a router that
  # started answering something else would be a regression nothing else here
  # would catch.
  status=$(req GET "$API/no-such-route")
  [ "$status" = "404" ] && record unknown_route "$status" 0 1 \
    || record unknown_route "$status" 0 0 "expected 404"

  echo "round $ROUND at +$(( $(date +%s) - T0 ))s: $PASS ok, $FAIL failed, $SKIP skipped"
}

wait_for_tenant

while true; do
  round
  [ "$ROUNDS" -gt 0 ] && [ "$ROUND" -ge "$ROUNDS" ] && break
  sleep "$INTERVAL"
done
