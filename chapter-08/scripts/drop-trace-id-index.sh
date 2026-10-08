#!/usr/bin/env bash
# Takes listing 8.2's index back off, so the table is the way the stack starts.
#
# Usage: ./scripts/drop-trace-id-index.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_clickhouse

ch --query "ALTER TABLE tracing.otel_traces DROP INDEX IF EXISTS idx_trace_id"
echo "dropped idx_trace_id; tracing.otel_traces has no skip index"
