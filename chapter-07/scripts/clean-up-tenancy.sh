#!/usr/bin/env bash
# Undoes everything the tenancy exercise created: both policies, the demo rows,
# the three logins, the tenant map and the tenant_id column. Then shows the
# table is back to listing 7.1.
#
# Usage: ./scripts/clean-up-tenancy.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
reset_tenancy
ch --query "DROP TABLE IF EXISTS tracing.tenant_users"
ch --query "ALTER TABLE tracing.otel_traces DROP COLUMN IF EXISTS tenant_id"

echo "row policies left: $(ch --query "SELECT count() FROM system.row_policies")"
echo "rows in otel_traces: $(ch --query "SELECT count() FROM tracing.otel_traces")"
echo "columns: $(ch --query "
  SELECT arrayStringConcat(groupArray(name), ', ') FROM (
    SELECT name FROM system.columns
    WHERE database = 'tracing' AND table = 'otel_traces' ORDER BY position)")"
