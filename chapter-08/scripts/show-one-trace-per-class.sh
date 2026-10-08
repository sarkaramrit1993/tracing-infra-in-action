#!/usr/bin/env bash
# Prints one whole trace from each sampling class, root span first, with the
# weight each span carries.
#
# Usage: ./scripts/show-one-trace-per-class.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data

ch_table "
SELECT
  if(parent_span_id = '', '>', '') AS root,
  substring(trace_id, 1, 8) AS trace,
  span_name AS span,
  concat(toString(round(duration_ns / 1e6, 1)), 'ms') AS took,
  adjusted_count AS weight,
  status_code
FROM tracing.otel_traces
WHERE trace_id IN (
  SELECT min(trace_id) FROM tracing.otel_traces
  WHERE parent_span_id = '' GROUP BY adjusted_count)
ORDER BY
  weight DESC,
  trace_id,
  indexOf(['GET /checkout', 'validate_cart', 'inventory.reserve',
           'payment.charge', 'fraud.score', 'order.create',
           'notification.send'], span_name)"
