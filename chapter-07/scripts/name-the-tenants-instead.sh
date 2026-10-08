#!/usr/bin/env bash
# Rewrites the policy to name the two tenant logins instead of exempting the
# operator, then counts what newhire and acme_reader read.
#
# Usage: ./scripts/name-the-tenants-instead.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_newhire
ch --query "
CREATE ROW POLICY OR REPLACE tenant_filter ON tracing.otel_traces
USING tenant_id IN (SELECT tenant_id FROM tracing.tenant_users
                    WHERE user_name = currentUser())
TO acme_reader, globex_reader"
ch_table "SELECT count() AS newhire_reads FROM tracing.otel_traces" --user newhire
echo
ch_table "SELECT count() AS acme_reads_of_tenant_b FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'" --user acme_reader
