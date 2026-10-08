#!/usr/bin/env bash
# Creates a login called newhire with SELECT rights and no row in the tenant
# map, and counts what it reads under listing 7.4's policy.
#
# Usage: ./scripts/add-unmapped-login.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_tenancy
ch --query "CREATE USER IF NOT EXISTS newhire IDENTIFIED WITH no_password"
ch --query "GRANT SELECT ON tracing.otel_traces TO newhire"
ch --query "GRANT SELECT ON tracing.tenant_users TO newhire"
ch_table "SELECT count() AS newhire_reads FROM tracing.otel_traces" --user newhire
