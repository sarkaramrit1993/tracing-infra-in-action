#!/usr/bin/env bash
# Drops every view the rollup exercise built, then deletes the demo rows it
# wrote. Views first: a delete is not an insert, so a view still watching would
# keep the deleted rows in its rollup forever.
#
# Usage: ./scripts/clean-up-rollup.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_clickhouse

./scripts/drop-rollup-views.sh
ch --query "
ALTER TABLE tracing.otel_traces DELETE WHERE service_name LIKE 'rollup-demo%'
SETTINGS mutations_sync = 2"
echo "deleted every rollup-demo row from tracing.otel_traces"
