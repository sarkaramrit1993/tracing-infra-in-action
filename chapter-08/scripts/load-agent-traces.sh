#!/usr/bin/env bash
# Loads the agent run chapter 2 recorded (one trace, OTLP JSON) into the same
# tracing.otel_traces table the generated data sits in. No Collector and no live
# agent: the file is read once and turned into rows.
#
# Usage: ./scripts/load-agent-traces.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_clickhouse

RECORDED=../chapter-02/agent/results/agent-traces-otlp.json
[ -f "$RECORDED" ] || die "the recorded agent run is not at chapter-02/agent/results/agent-traces-otlp.json. Run from a full checkout of the companion repository."

already=$(ch --query "SELECT count() FROM tracing.otel_traces WHERE service_name = 'support-agent'") \
  || die "could not count the agent spans. $HINT"
case "$already" in
  0) ;;
  ''|*[!0-9]*) die "ClickHouse did not answer with a span count. $HINT" ;;
  *) die "$already agent spans are already loaded. To load them again, run ./scripts/remove-agent-traces.sh first." ;;
esac

rows=$(python3 generate/otlp_to_rows.py "$RECORDED") || die "could not read $RECORDED"
[ -n "$rows" ] || die "$RECORDED holds no spans"
{
  printf 'INSERT INTO tracing.otel_traces\n'
  printf '  (timestamp, trace_id, span_id, parent_span_id, service_name, span_name,\n'
  printf '   status_code, duration_ns, attributes) FORMAT JSONEachRow\n'
  printf '%s\n' "$rows"
} | docker compose exec -T clickhouse clickhouse-client --multiquery \
  || die "ClickHouse rejected the agent spans"

loaded=$(ch --query "SELECT count() FROM tracing.otel_traces WHERE service_name = 'support-agent'") \
  || die "could not count the agent spans. $HINT"
echo "loaded $loaded spans from chapter-02/agent/results/agent-traces-otlp.json into tracing.otel_traces"
