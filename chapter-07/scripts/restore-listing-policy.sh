#!/usr/bin/env bash
# Puts listing 7.4's policy back, exempting only the operator, and counts what
# newhire reads again.
#
# Usage: ./scripts/restore-listing-policy.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_newhire
ch --query "
CREATE ROW POLICY OR REPLACE tenant_filter ON tracing.otel_traces
USING tenant_id IN (SELECT tenant_id FROM tracing.tenant_users
                    WHERE user_name = currentUser())
TO ALL EXCEPT default"
ch_table "SELECT count() AS newhire_reads FROM tracing.otel_traces" --user newhire
