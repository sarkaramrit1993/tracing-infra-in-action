#!/usr/bin/env bash
# Prints the five most recent spans that came back in error.
#
# Usage: ./scripts/show-error-spans.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_ready
ch_table "
SELECT trace_id, span_name, status_code FROM tracing.otel_traces
WHERE status_code = 'STATUS_CODE_ERROR' ORDER BY timestamp DESC LIMIT 5"
