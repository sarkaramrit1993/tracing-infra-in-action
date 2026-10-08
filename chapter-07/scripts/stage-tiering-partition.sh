#!/usr/bin/env bash
# Clears any tiering-demo rows an earlier run left, writes 50,000 spans dated
# yesterday at midday, and shows where every partition sits.
#
# Usage: ./scripts/stage-tiering-partition.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
ch --query "
ALTER TABLE tracing.otel_traces DELETE WHERE service_name = 'tiering-demo'
SETTINGS mutations_sync = 2"
stage_tiering_rows 1
show_parts
