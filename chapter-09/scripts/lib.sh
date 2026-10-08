# shellcheck shell=bash
# Sourced by every script in scripts/. Not a reader step.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

APP=http://localhost:8080
COLLECTOR=http://localhost:8888/metrics
STATE_DIR="${TMPDIR:-/tmp}/tracing-in-action-ch09"
HINT="Is the stack up? From chapter-09/ run: docker compose up -d --build"

die() { echo "$*" >&2; exit 1; }

q() { python3 scripts/query.py "$@"; }

# clickhouse-client reads stdin even for a --query, and `docker compose exec -T`
# hands it the caller's. From a terminal that never sends EOF, so a client
# without `< /dev/null` hangs with no output. ch_file needs its stdin for the
# .sql file, which is why it is a separate helper.
ch() { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }

# Sum of one metric on a text exposition endpoint, 0 when the metric is absent.
collector_metric() { q exposition "$COLLECTOR" "$@"; }

ge() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a + 0 >= b + 0) }'; }

answers() {
  case "$1" in
    checkout-service) curl -sf -m 5 -o /dev/null "$APP/health" ;;
    otel-collector) curl -sf -m 5 -o /dev/null "$COLLECTOR" ;;
    prometheus) curl -sf -m 5 -o /dev/null http://localhost:9090/-/ready ;;
    clickhouse) curl -sf -m 5 -o /dev/null http://localhost:8123/ping ;;
    loki) curl -sf -m 5 -o /dev/null http://localhost:3100/ready ;;
    *) die "unknown service $1" ;;
  esac
}

require() {
  local s
  for s in "$@"; do
    answers "$s" || die "$s is not answering. $HINT"
  done
}

# For a stack that was just started or a service that was just restarted.
await_services() {
  local budget=$1 deadline s
  shift
  deadline=$(( $(date +%s) + budget ))
  for s in "$@"; do
    until answers "$s"; do
      [ "$(date +%s)" -lt "$deadline" ] \
        || die "$s did not answer within ${budget}s. Check docker compose ps, then docker compose logs $s"
      sleep 1
    done
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

save_state() {
  mkdir -p "$STATE_DIR"
  chmod 700 "$STATE_DIR"
  cat > "$STATE_DIR/$1"
}

# state FILE KEY: one value a previous script saved, or exit with HOW_TO.
state() {
  local value
  [ -f "$STATE_DIR/$1" ] || die "$3"
  value=$(sed -n "s/^$2=//p" "$STATE_DIR/$1")
  [ -n "$value" ] || die "$3"
  echo "$value"
}

# The span metrics connectors write their counts out once per
# metrics_flush_interval, so anything the sampler has already decided is in the
# counts one interval (plus a second for the batch processor) later. A
# Prometheus scrape that starts after that has picked it up. Both times are read
# off Prometheus's own clock. Call it once the sampler has decided.
await_span_metrics() {
  local flush
  flush=$(sed -n 's/^ *metrics_flush_interval: *\([0-9][0-9]*\)s.*/\1/p' collector/gateway-config.yaml | sort -n | tail -1)
  SCRAPE_AFTER=$(awk -v t="$(q time)" -v f="${flush:-15}" 'BEGIN { printf "%.3f", t + f + 2 }')
  poll "$1" 120 prometheus_scraped_since_flush
}
prometheus_scraped_since_flush() {
  local scraped
  scraped=$(q scrape-time otel-spanmetrics 2>/dev/null) || return 1
  ge "$scraped" "$SCRAPE_AFTER"
}
