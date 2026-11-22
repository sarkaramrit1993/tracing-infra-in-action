#!/usr/bin/env bash
# Finds the recorded agent run three ways, by tool, by model and by agent name,
# each through the span name, through the attributes map and through a
# materialized column. Reads rows and bytes from ClickHouse's own query log.
# The columns it adds are dropped again before it ends.
#
# Usage: ./scripts/find-runs-by-tool.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_clickhouse

loaded=$(ch --query "SELECT count() FROM tracing.otel_traces WHERE service_name = 'support-agent'") \
  || die "could not count the agent spans. $HINT"
[ "${loaded:-0}" != 0 ] || die "no agent spans yet. Run ./scripts/load-agent-traces.sh"
total=$(ch --query "SELECT count() FROM tracing.otel_traces") || die "could not count spans. $HINT"
echo "tracing.otel_traces holds $total spans, $loaded of them the agent run"

# route PREDICATE: run one filter, print "predicate, spans, traces, rows read, bytes read".
route() {
  local label="agent-filter-$$-$RANDOM" found stats
  found=$(ch --log_comment="$label" --use_query_condition_cache=0 --format TSV --query "
SELECT count(), uniqExact(trace_id) FROM tracing.otel_traces WHERE $1") \
    || die "the filter failed: $1"
  ch --query "SYSTEM FLUSH LOGS" || die "could not flush the query log"
  stats=$(ch --format TSV --query "
SELECT read_rows, formatReadableSize(read_bytes) FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment = '$label'
ORDER BY event_time_microseconds DESC LIMIT 1") || die "could not read the query log"
  [ -n "$stats" ] || die "the query log has no entry for: $1"
  printf '%s\t%s\t%s\n' "$1" "$found" "$stats"
}

table() {
  { printf 'filter\tspans\ttraces\trows read\tbytes read\n'; printf '%s\n' "$@"; } | align
}

tool_name=$(route "span_name = 'execute_tool issue_refund'") || exit 1
tool_map=$(route "attributes['gen_ai.tool.name'] = 'issue_refund'") || exit 1
model_name=$(route "span_name = 'chat stand-in-1'") || exit 1
model_map=$(route "attributes['gen_ai.request.model'] = 'stand-in-1'") || exit 1
agent_name=$(route "span_name = 'invoke_agent support-agent'") || exit 1
agent_map=$(route "attributes['gen_ai.agent.name'] = 'support-agent'") || exit 1

ch --query "
ALTER TABLE tracing.otel_traces
  ADD COLUMN IF NOT EXISTS tool_name LowCardinality(String)
    MATERIALIZED attributes['gen_ai.tool.name'],
  ADD COLUMN IF NOT EXISTS request_model LowCardinality(String)
    MATERIALIZED attributes['gen_ai.request.model'],
  ADD COLUMN IF NOT EXISTS agent_name LowCardinality(String)
    MATERIALIZED attributes['gen_ai.agent.name']" || die "could not add the columns"
for column in tool_name request_model agent_name; do
  ch --query "ALTER TABLE tracing.otel_traces MATERIALIZE COLUMN $column SETTINGS mutations_sync = 2" \
    || die "could not fill the $column column"
done

tool_col=$(route "tool_name = 'issue_refund'") || exit 1
model_col=$(route "request_model = 'stand-in-1'") || exit 1
agent_col=$(route "agent_name = 'support-agent'") || exit 1

ch --query "
ALTER TABLE tracing.otel_traces
  DROP COLUMN IF EXISTS tool_name,
  DROP COLUMN IF EXISTS request_model,
  DROP COLUMN IF EXISTS agent_name" || die "could not drop the columns. Run ./scripts/remove-agent-traces.sh"

echo
echo "by tool"
table "$tool_name" "$tool_map" "$tool_col"
echo
echo "by model"
table "$model_name" "$model_map" "$model_col"
echo
echo "by agent name"
table "$agent_name" "$agent_map" "$agent_col"
echo
echo "the columns are dropped again; ./scripts/remove-agent-traces.sh deletes the agent spans"
