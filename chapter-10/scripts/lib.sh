# shellcheck shell=bash
# Sourced by every script in scripts/. Not a reader step.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

HINT="Is the stack up? From chapter-10/ run: docker compose up -d --build, then ./scripts/wait-until-ready.sh"
STATE_DIR="${TMPDIR:-/tmp}/tracing-in-action-ch10"
DEFAULT_REQUESTS=20000
RATE=200

die() { echo "$*" >&2; exit 1; }

q() { python3 scripts/query.py "$@"; }

# clickhouse-client reads stdin even for a --query, and `docker compose exec -T`
# hands it the caller's. From a terminal that never sends EOF, so a client
# without `< /dev/null` hangs with no output. ch_file needs its stdin for the
# .sql file, which is why it is a separate helper.
ch() { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }

# sampling_percentage of one processor in collector/sampler-config.yaml, so
# a table label always names the percentage the sampler is actually running.
sampler_pct() {
  awk -v s="$1:" '$1 == s { on = 1; next } on && /sampling_percentage:/ { print $2; exit }' collector/sampler-config.yaml
}

# Whether the sampler tier attaches exemplars: prints true or false.
sampler_exemplars_enabled() {
  awk '$1 == "spanmetrics/uniform:" { on = 1; next }
       on && /^  [^ ]/ { on = 0 }
       on && $1 == "exemplars:" { ex = 1; next }
       on && ex && $1 == "enabled:" { print $2; exit }' collector/sampler-config.yaml
}

# Everything on a Kafka topic, or nothing after 8 s of silence.
kafka_read() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9093 --topic "$1" --from-beginning --timeout-ms 8000 \
    < /dev/null 2> /dev/null || true
}

ge() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a + 0 >= b + 0) }'; }
eq() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a + 0 == b + 0) }'; }

answers() {
  case "$1" in
    checkout-service) docker compose exec -T checkout-service curl -sf -m 5 -o /dev/null http://localhost:8080/health < /dev/null 2> /dev/null ;;
    edge-collector) curl -sf -m 5 -o /dev/null http://localhost:8889/metrics ;;
    sampler-collector) curl -sf -m 5 -o /dev/null http://localhost:8890/metrics ;;
    prometheus) curl -sf -m 5 -o /dev/null http://localhost:9090/-/ready ;;
    clickhouse) curl -sf -m 5 -o /dev/null http://localhost:8123/ping ;;
    flink) curl -sf -m 5 -o /dev/null http://localhost:18081/overview ;;
    *) die "unknown service $1" ;;
  esac
}

# A service that is up but not yet answering (just restarted, still starting)
# is not the same as a stack that is down. Container states that mean "on its
# way" get a wait; anything else is the stack being down.
service_is_coming_up() {
  case "$(docker compose ps -a --format '{{.State}}' "$1" 2> /dev/null | head -n 1)" in
    running | restarting | created) return 0 ;;
    *) return 1 ;;
  esac
}

require() {
  local s
  for s in "$@"; do
    answers "$s" && continue
    service_is_coming_up "$s" || die "$s is not running. $HINT"
    echo "$s is starting up (it was just restarted?); waiting up to 120 s for it to answer"
    poll "waiting for $s" 120 answers "$s"
  done
}

# config_is_running SERVICE FILE...: a Collector reads its config once, at
# start. A file edited after the container started has not been applied, and
# the table labels (which read the file) would name a value the Collector is
# not running. Refuse, and name the command that applies it.
config_is_running() {
  local svc=$1 id started stale
  shift
  id=$(docker compose -f docker-compose.yml -f docker-compose.cardinality.yml ps -q "$svc" 2> /dev/null | head -n 1)
  [ -n "$id" ] || return 0
  started=$(docker inspect -f '{{.State.StartedAt}}' "$id" < /dev/null)
  stale=$(q newer-than "$started" "$@")
  [ -z "$stale" ] || die "$stale changed after $svc started, so $svc is still running the old values. Run: ${CONFIG_FIX:-docker compose restart $svc}"
}

# A signature of every config a workload's counts depend on.
config_signature() {
  cat collector/sampler-config.yaml collector/edge-config.yaml collector/edge-base.yaml | cksum | awk '{ print $1 }'
}

# poll LABEL SECONDS COMMAND...: rerun COMMAND once a second until it succeeds.
poll() {
  local label=$1 budget=$2 deadline
  shift 2
  deadline=$(( $(date +%s) + budget ))
  printf '%s... ' "$label"
  until "$@"; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo
      die "timed out after ${budget}s ${label}. Check docker compose ps and docker compose logs"
    fi
    sleep 1
  done
  echo ok
}

save_state() {
  mkdir -p "$STATE_DIR"
  chmod 700 "$STATE_DIR"
  cat > "$STATE_DIR/$1"
}

# state FILE KEY HOW_TO: one value a previous script saved, or exit with HOW_TO.
state() {
  local value
  [ -f "$STATE_DIR/$1" ] || die "$3"
  value=$(sed -n "s/^$2=//p" "$STATE_DIR/$1")
  [ -n "$value" ] || die "$3"
  echo "$value"
}

# state_or FILE KEY DEFAULT
state_or() {
  local value=""
  [ ! -f "$STATE_DIR/$1" ] || value=$(sed -n "s/^$2=//p" "$STATE_DIR/$1")
  echo "${value:-$3}"
}

# A request count, defaulting to what the README uses.
requests_arg() {
  case "${1:-$DEFAULT_REQUESTS}" in
    '' | *[!0-9]* | 0) die "usage: $0 [requests, default $DEFAULT_REQUESTS]" ;;
    *) echo "${1:-$DEFAULT_REQUESTS}" ;;
  esac
}

# ---------------------------------------------------------------------------
# Measured workload. Every comparison needs ground truth, so a script that
# compares placements sends its own fixed workload and reads what each
# placement counted for it. Counters only ever grow, so the answer is always
# "after minus before", and the "before" has to be taken once the previous
# workload has been counted everywhere. measure does both.

# Traces the sampler tier has decided, off its own telemetry. Two tail samplers
# decide every trace, so one request adds two. Background traffic keeps a few
# traces undecided at any moment, so "received == decided" never holds.
sampler_decided() {
  curl -sf -m 5 http://localhost:8892/metrics | awk '
    index($0, "otelcol_processor_tail_sampling_global_count_traces_sampled_total{") == 1 { s += $NF }
    END { printf "%d\n", s }'
}

endpoint_matches_prometheus() {
  [ "$(q snapshot endpoint)" = "$(q snapshot)" ]
}

# measure N: leave ground truth and per-placement deltas in the `measure` state
# file. Sends nothing when the same N was sent and counted since the last
# workload.
measure() {
  local n=$1 inst finished settled_at wait_for
  require checkout-service edge-collector sampler-collector prometheus clickhouse
  config_is_running sampler-collector collector/sampler-config.yaml
  config_is_running edge-collector collector/edge-config.yaml collector/edge-base.yaml
  inst=$(q instance)
  finished=$(state_or workload FINISHED 0)
  MEASURE_NOTE="This script sends its own workload and waits for every placement to count it: expect 4 to 7 minutes the first time, and about a minute when the workload is already counted."

  if [ "$(state_or measure INSTANCE x)" = "$inst" ] \
     && [ "$(state_or measure REQUESTS 0)" = "$n" ] \
     && [ "$(state_or measure SETTLED 0)" = "1" ] \
     && [ "$(state_or measure CONFIG x)" = "$(config_signature)" ] \
     && [ "$(state_or measure FINISHED 1)" = "$finished" ]; then
    echo "using the $n-request workload sent $(( $(date +%s) - finished )) s ago and already counted"
    return 0
  fi

  if [ "$(state_or measure CONFIG x)" != "x" ] && [ "$(state_or measure CONFIG x)" != "$(config_signature)" ]; then
    echo "the Collector config changed since the last workload was counted, so this sends a new one"
  fi
  settled_at=$(state_or settled AT 0)
  if [ "$(state_or workload INSTANCE x)" = "$inst" ] && [ "$finished" -gt "$settled_at" ]; then
    wait_for=$(( finished + 200 - $(date +%s) ))
    if [ "$wait_for" -gt 0 ]; then
      echo "an earlier workload may still be counting; waiting $wait_for s so it cannot mix into this one (this is the slow part; nothing is wrong)"
      sleep "$wait_for"
    fi
  fi
  echo "$MEASURE_NOTE"
  poll "waiting for Prometheus to catch up with the Collectors" 90 endpoint_matches_prometheus

  {
    q snapshot
    echo "decided=$(sampler_decided)"
  } | save_state before

  ./scripts/run-workload.sh "$n" "$RATE"
  settle_measure "$n"
}

edge_counted_everything() {
  local now before
  now=$(q exposition http://localhost:8889/metrics traces_span_metrics_calls_total \
    'service_name="checkout-service"' 'span_name="GET /checkout"' 'span_kind="SPAN_KIND_SERVER"')
  before=$(state before edge_calls "no baseline: run this script again")
  eq "$(q delta "$now" "$before")" "$N_WAIT"
}
sampler_decided_everything() {
  ge "$(q delta "$(sampler_decided)" "$DECIDED0")" "$(( 2 * N_WAIT ))"
}
clickhouse_has_every_kept_span() {
  local rows
  rows=$(ch --query "SELECT count() FROM tracing.otel_traces WHERE service_name = 'checkout-service' AND span_name = 'GET /checkout' AND timestamp >= toDateTime64($START_WAIT, 3)" 2> /dev/null) || return 1
  eq "$rows" "$UNIFORM_WAIT"
}
# The sampler tier's counts are final once a whole flush (60 s, plus margin)
# passes with none of them moving and ClickHouse holds exactly the spans the
# uniform sampler counted. Read live every time: the first flush after a
# restart comes about two minutes late, and one workload can land across two
# flushes, so a number read once after a fixed wait can be 0 or a few short.
sampler_flushed_everything() {
  local now
  now=$(q snapshot endpoint)
  if [ "$now" != "$LAST_WAIT" ]; then
    LAST_WAIT=$now
    CHANGED_WAIT=$(date +%s)
    return 1
  fi
  [ $(( $(date +%s) - CHANGED_WAIT )) -ge 67 ] || return 1
  UNIFORM_WAIT=$(q delta "$(echo "$now" | sed -n 's/^uniform_calls=//p')" \
    "$(state before uniform_calls "no baseline: run this script again")")
  clickhouse_has_every_kept_span
}

settle_measure() {
  local n=$1 line key after ch_rows ch_errors
  N_WAIT=$n
  START_WAIT=$(state workload START "no workload recorded: run ./scripts/run-workload.sh first")
  DECIDED0=$(state before decided "no baseline: run this script again")

  poll "waiting for the edge connector to count all $n requests (it flushes every 60 s)" 180 edge_counted_everything
  poll "waiting for the sampler to decide every trace" 180 sampler_decided_everything
  LAST_WAIT=$(q snapshot endpoint)
  CHANGED_WAIT=$(date +%s)
  poll "waiting for the sampler tier to flush its span metrics and for the kept spans to reach ClickHouse (a minute or more)" 300 sampler_flushed_everything
  poll "waiting for Prometheus to scrape them" 90 endpoint_matches_prometheus

  ch_errors=$(ch --query "SELECT count() FROM tracing.otel_traces WHERE service_name = 'checkout-service' AND span_name = 'GET /checkout' AND status_code = 'STATUS_CODE_ERROR' AND timestamp >= toDateTime64($START_WAIT, 3)")
  ch_rows=$UNIFORM_WAIT
  {
    echo "INSTANCE=$(q instance)"
    echo "REQUESTS=$n"
    echo "ERRORS=$(state workload ERRORS "no workload recorded")"
    echo "START=$START_WAIT"
    echo "FINISHED=$(state workload FINISHED "no workload recorded")"
    echo "SETTLED=1"
    echo "CONFIG=$(config_signature)"
    echo "stored_rows=$ch_rows"
    echo "stored_errors=$ch_errors"
    for line in $(q snapshot); do
      key=${line%%=*}
      after=${line#*=}
      echo "$key=$(q delta "$after" "$(state before "$key" "no baseline: run this script again")")"
    done
  } | save_state measure
  echo "AT=$(date +%s)" | save_state settled
}

# m KEY: one number out of the last measurement.
m() { state measure "$1" "nothing measured yet: run ./scripts/compare-placements.sh first"; }

# measure_settled: use whatever workload this stack has already counted, at any
# request count and under any sampler setting, or send the default one. For
# steps that read stored rows and need no particular sampler.
measure_settled() {
  require checkout-service edge-collector sampler-collector prometheus clickhouse
  if [ "$(state_or measure INSTANCE x)" = "$(q instance)" ] && [ "$(state_or measure SETTLED 0)" = "1" ]; then
    echo "using the $(m REQUESTS)-request workload sent $(( $(date +%s) - $(m FINISHED) )) s ago and already counted"
  else
    measure "$DEFAULT_REQUESTS"
  fi
}

# A file a "Try this" script edits is copied aside first, so reset puts back
# exactly the bytes that shipped, even after an interrupted run.
PRISTINE_DIR="$STATE_DIR/pristine"
keep_pristine() {
  mkdir -p "$PRISTINE_DIR"
  [ -f "$PRISTINE_DIR/$(basename "$1")" ] || cp -p "$1" "$PRISTINE_DIR/$(basename "$1")"
}
has_pristine() { [ -f "$PRISTINE_DIR/$(basename "$1")" ]; }
restore_pristine() {
  cat "$PRISTINE_DIR/$(basename "$1")" > "$1"
  rm -f "$PRISTINE_DIR/$(basename "$1")"
}
# rewrite FILE AWK_PROGRAM [awk args]: edits in place, same inode, so a file the
# Collector has bind-mounted stays the file it sees.
rewrite() {
  local file=$1 tmp
  shift
  tmp=$(mktemp)
  awk "$@" "$file" > "$tmp"
  cat "$tmp" > "$file"
  rm -f "$tmp"
}
