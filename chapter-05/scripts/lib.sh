# shellcheck shell=bash
# Sourced by every script in scripts/. Not a reader step.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

APP=http://localhost:8080
# Flink's UI and REST port. 8081 unless something else on this machine has it.
export FLINK_PORT="${FLINK_PORT:-8081}"
STATE_DIR="${TMPDIR:-/tmp}/tracing-in-action-ch05"
HINT="Is the stack up? From chapter-05/ run: docker compose up -d --build"
SEND="nothing sent yet: run ./scripts/send-traffic.sh first"
ARRIVING="still arriving: run ./scripts/wait-until-ready.sh first"
# GET /checkout plus ten spans under it: five in checkout-service, and five in
# the four services it calls. app/checkout.py has the shape.
SPANS_PER_CHECKOUT=11

die() { echo "$*" >&2; exit 1; }

q() { python3 scripts/query.py "$@"; }

# clickhouse-client reads stdin even for a --query, and `docker compose exec -T`
# hands it the caller's. From a terminal that never sends EOF, so a client
# without `< /dev/null` hangs with no output.
ch() { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
# ch_file FILE [ARGS...]: run a .sql file, which is why this one keeps stdin.
ch_file() {
  local file=$1
  shift
  docker compose exec -T clickhouse clickhouse-client "$@" --multiquery < "$file"
}

# Committed records on a topic. The Flink sinks write inside Kafka
# transactions, and every commit marker takes an offset of its own, so summing
# end offsets overcounts. Reading with read_committed and counting one line per
# record does not.
topic_records() {
  docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka-1:9093 --topic "$1" --from-beginning \
    --isolation-level read_committed --timeout-ms 5000 \
    --property print.value=false --property print.offset=true \
    < /dev/null 2>/dev/null | grep -c '^Offset:' || true
}

answers() {
  case "$1" in
    checkout-service) curl -sf -m 5 -o /dev/null "$APP/health" ;;
    clickhouse) curl -sf -m 5 -o /dev/null http://localhost:8123/ping ;;
    prometheus) curl -sf -m 5 -o /dev/null http://localhost:9090/-/ready ;;
    jaeger) curl -sf -m 5 -o /dev/null http://localhost:16686/ ;;
    flink) [ "$(q flink-state 2>/dev/null)" = RUNNING ] ;;
    *) die "unknown service $1" ;;
  esac
}

require() {
  local s
  for s in "$@"; do
    case "$s" in
      flink) answers flink || die "the Flink assembly job is not RUNNING. $HINT, then check docker compose logs flink-job-submit" ;;
      *) answers "$s" || die "$s is not answering. $HINT" ;;
    esac
  done
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

# The ClickHouse container is recreated by `docker compose down`, so its ID is
# what ties a state file to the stack it describes.
stack_id() { docker compose ps -q clickhouse 2>/dev/null; }

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

require_traffic() {
  local id
  id=$(state traffic STACK_ID "$SEND")
  [ "$id" = "$(stack_id)" ] || die "$SEND"
}

# recent RESULT PATTERN: the views read only the last hour, so a reader who
# comes back later gets an empty result. Say so instead of printing nothing.
recent() {
  printf '%s\n' "$1" | grep -q "$2" \
    || die "no checkouts in the last hour: run ./scripts/send-traffic.sh and ./scripts/wait-until-ready.sh"
}

require_ready() {
  require_traffic
  state traffic READY "$ARRIVING" > /dev/null
}

# await_both_paths STARTED COUNT: wait until COUNT checkouts sent after STARTED
# (nanoseconds on ClickHouse's clock) have all their spans in ClickHouse, then
# until Jaeger holds the stream-time copy of the newest of them. Flink emits in
# event-time order, so the newest one is the last to come out. Sets LAST.
await_both_paths() {
  CHECKOUTS_SINCE="SELECT trace_id FROM tracing.otel_traces
    WHERE span_name = 'GET /checkout'
      AND timestamp >= fromUnixTimestamp64Nano(toInt64($1))"
  WANT=$2
  poll "query-time path: waiting for ClickHouse to hold all $SPANS_PER_CHECKOUT spans of all $WANT checkouts" 180 clickhouse_has_every_span
  LAST=$(ch --query "$CHECKOUTS_SINCE ORDER BY timestamp DESC LIMIT 1")
  poll "stream-time path: waiting for Flink to assemble the newest one and Jaeger to store it" 180 jaeger_has_assembled_last
}
clickhouse_has_every_span() {
  local whole
  whole=$(ch --query "
    SELECT count() FROM (
      SELECT trace_id FROM tracing.otel_traces
      WHERE trace_id IN ($CHECKOUTS_SINCE)
      GROUP BY trace_id HAVING uniqExact(span_id) = $SPANS_PER_CHECKOUT)" 2>/dev/null) || return 1
  [ "${whole:-0}" -ge "$WANT" ]
}
jaeger_has_assembled_last() {
  q jaeger-sources "$LAST" 2>/dev/null | grep -qx "stream-time $SPANS_PER_CHECKOUT"
}
