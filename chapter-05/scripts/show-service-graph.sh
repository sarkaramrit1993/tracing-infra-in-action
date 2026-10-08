#!/usr/bin/env bash
# Runs listing 5.6, then the same self-join with the callee taken from each
# client span's peer.service attribute. Every span in this stack is emitted by
# one process, checkout-service, so the listing as printed finds every
# parent-child pair inside one service and filters all of them out. The
# downstream services exist here only as peer.service on the client spans.
#
# Usage: ./scripts/show-service-graph.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require clickhouse
require_ready

echo "listing 5.6, parent and child by service.name:"
listing=$(ch_file clickhouse/service_graph.sql --format TSV)
if [ -n "$listing" ]; then
  printf '%s\n' "$listing" | q table
else
  echo "  no edges: every span here comes from checkout-service, so each pair is internal work"
fi
echo
echo "the same self-join, with each callee named by peer.service:"
ch --format TSVWithNames --query "
SELECT parent_service, child_service,
       count() AS call_count,
       round(quantileTDigest(0.99)(duration) / 1e6, 1) AS p99_ms,
       countIf(status_code = 'STATUS_CODE_ERROR') AS error_count
FROM (
    SELECT
        if(p.span_attributes['peer.service'] != '',
           p.span_attributes['peer.service'], p.service_name) AS parent_service,
        s.span_attributes['peer.service'] AS child_service,
        s.duration,
        s.status_code
    FROM tracing.otel_traces AS s
    INNER JOIN tracing.otel_traces AS p
        ON s.trace_id = p.trace_id
       AND s.parent_span_id = p.span_id
    WHERE s.timestamp >= now() - INTERVAL 1 HOUR
      AND p.timestamp >= now() - INTERVAL 2 HOUR
      AND s.span_attributes['peer.service'] != ''
)
WHERE parent_service != child_service
GROUP BY parent_service, child_service
ORDER BY call_count DESC, parent_service, child_service" | q table
