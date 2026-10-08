#!/usr/bin/env bash
# Deletes every tiering-demo row, re-applies listing 7.2 so its own two-day
# boundary is back, and shows what is left.
#
# Usage: ./scripts/clean-up-tiering.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
ch --query "
ALTER TABLE tracing.otel_traces DELETE WHERE service_name = 'tiering-demo'
SETTINGS mutations_sync = 2"
ch_file clickhouse/tiering.sql
echo "tiering-demo rows left: $(ch --query "SELECT count() FROM tracing.otel_traces WHERE service_name = 'tiering-demo'")"
show_parts
