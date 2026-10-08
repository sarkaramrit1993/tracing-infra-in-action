#!/usr/bin/env bash
# Counts the kept root spans per sampling class and multiplies each class back
# up by its weight.
#
# Usage: ./scripts/show-sampling-policy.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data

ch_table "
SELECT p.class                   AS class,
       t.adjusted_count          AS weight,
       t.kept                    AS kept,
       t.kept * t.adjusted_count AS represents
FROM (
  SELECT adjusted_count, count() AS kept
  FROM tracing.otel_traces
  WHERE parent_span_id = '' GROUP BY adjusted_count) AS t
INNER JOIN tracing.sampling_policy AS p ON p.adjusted_count = t.adjusted_count
ORDER BY weight DESC"
