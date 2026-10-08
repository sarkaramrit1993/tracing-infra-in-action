#!/usr/bin/env bash
# Asks listing 8.1's two counts with the root-span filter left out.
#
# Usage: ./scripts/count-without-root-filter.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data

ch_table "
SELECT count() AS spans, sum(adjusted_count) AS weighted
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR)"
