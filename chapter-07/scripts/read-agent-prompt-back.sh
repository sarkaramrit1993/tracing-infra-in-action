#!/usr/bin/env bash
# Reads the last agent run back from both stores: ClickHouse keeps every prompt
# whole, Tempo returns each one cut at 2048 bytes. Also prints what the sender
# and Tempo reported about the push, which is nothing wrong.
#
# Usage: ./scripts/read-agent-prompt-back.sh
# shellcheck source=scripts/agent-lib.sh
source "$(dirname "$0")/agent-lib.sh"

require_stack
require_clickhouse
TID=$(agent_trace)
KEY=gen_ai.input.messages

# Both stores take their own time, so wait until they hold the same spans.
both_stores_have_it() {
  local in_clickhouse in_tempo
  in_clickhouse=$(clickhouse_span_count "$TID")
  in_tempo=$(tempo_span_count "$TID")
  [ "$in_clickhouse" -gt 0 ] && [ "$in_clickhouse" = "$in_tempo" ]
}
poll "waiting for the trace in ClickHouse and Tempo" "${READ_BUDGET:-90}" both_stores_have_it

echo "trace_id = $TID"
echo
echo "The push, as the sender and Tempo counted it:"
echo "  Collector: refused $(collector_refused) spans, failed to send $(collector_failed_to_tempo) spans to Tempo"
echo "  Tempo: refused $(tempo_refused) spans, discarded $(tempo_discarded) spans"
echo "  ClickHouse and Tempo both hold $(tempo_span_count "$TID") spans of the trace"
echo
echo "What Tempo says about the cut:"
echo "  attribute values cut: $(tempo_cut)"
{ docker compose logs --no-log-prefix tempo 2> /dev/null || true; } \
  | { grep -o 'msg="attributes truncated".*' || true; } | tail -n 1 | sed 's/^/  log: /'
echo
echo "$KEY, one row per model call:"
{
  ch --format TSV --query "
    SELECT 'C', span_id, toUnixTimestamp64Nano(timestamp), length(attributes['$KEY'])
    FROM tracing.otel_traces
    WHERE trace_id = '$TID' AND mapContains(attributes, '$KEY')" 2> /dev/null || true
  tempo_string_attrs "$TID" "$KEY" | awk -F'\t' '{ print "T\t" $1 "\t" $2 "\t" $3 }'
} | awk -F'\t' '
  $1 == "C" { held[$2] = $4; start[$2] = $3 }
  $1 == "T" { got[$2] = $4; start[$2] = $3 }
  END {
    print "call\tClickHouse\tTempo"
    n = 0
    for (id in start) ids[++n] = id
    for (i = 1; i <= n; i++)
      for (j = i + 1; j <= n; j++)
        if (start[ids[j]] + 0 < start[ids[i]] + 0) { t = ids[i]; ids[i] = ids[j]; ids[j] = t }
    for (i = 1; i <= n; i++) print i "\t" held[ids[i]] "\t" got[ids[i]]
  }' | align
