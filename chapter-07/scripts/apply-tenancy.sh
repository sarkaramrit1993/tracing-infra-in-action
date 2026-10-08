#!/usr/bin/env bash
# Clears whatever an earlier tenancy run left, then applies listing 7.4 from
# clickhouse/tenancy.sql and prints the policy and the tenant map it created.
#
# Usage: ./scripts/apply-tenancy.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
reset_tenancy
ch_file clickhouse/tenancy.sql
ch --format Vertical --query "
SELECT short_name, database, table, select_filter, apply_to_all, apply_to_except
FROM system.row_policies"
echo
ch_table "SELECT * FROM tracing.tenant_users ORDER BY user_name"
