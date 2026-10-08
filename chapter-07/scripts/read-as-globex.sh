#!/usr/bin/env bash
# Reads the table as globex_reader, grouped by tenant.
#
# Usage: ./scripts/read-as-globex.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_tenancy
ch_table "SELECT tenant_id, count() AS rows FROM tracing.otel_traces GROUP BY tenant_id" --user globex_reader
