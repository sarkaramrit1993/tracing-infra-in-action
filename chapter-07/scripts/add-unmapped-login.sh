#!/usr/bin/env bash
# Creates a login called newhire with SELECT rights and no row in the tenant
# map, and counts what it reads under listing 7.4's policy. On a re-run it puts
# listing 7.4's policy back and takes newhire out of the map first.
#
# Usage: ./scripts/add-unmapped-login.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_tenancy
ch --query "
CREATE ROW POLICY OR REPLACE tenant_filter ON tracing.otel_traces
USING tenant_id IN (SELECT tenant_id FROM tracing.tenant_users
                    WHERE user_name = currentUser())
TO ALL EXCEPT default"
ch --query "ALTER TABLE tracing.tenant_users DELETE WHERE user_name = 'newhire' SETTINGS mutations_sync = 2"
ch --query "CREATE USER IF NOT EXISTS newhire IDENTIFIED WITH no_password"
ch --query "GRANT SELECT ON tracing.otel_traces TO newhire"
ch --query "GRANT SELECT ON tracing.tenant_users TO newhire"
ch_table "SELECT count() AS newhire_reads FROM tracing.otel_traces" --user newhire
