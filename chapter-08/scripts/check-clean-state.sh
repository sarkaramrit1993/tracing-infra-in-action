#!/usr/bin/env bash
# Shows that the database is the way the stack starts: three tables, no views,
# no skip index, and the generated population on disk.
#
# Usage: ./scripts/check-clean-state.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_clickhouse

ch_table "
SELECT name, engine FROM system.tables
WHERE database = 'tracing' AND name NOT LIKE '.inner%'
ORDER BY name"
echo
ch_table "
SELECT
  (SELECT count() FROM system.data_skipping_indices
    WHERE database = 'tracing' AND table = 'otel_traces') AS skip_indexes,
  (SELECT count() FROM tracing.otel_traces
    WHERE parent_span_id = '') AS root_spans,
  (SELECT sum(adjusted_count) FROM tracing.otel_traces
    WHERE parent_span_id = '') AS weighted"
