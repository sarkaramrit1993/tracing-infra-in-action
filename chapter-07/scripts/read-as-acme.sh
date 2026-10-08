#!/usr/bin/env bash
# Reads the table as acme_reader: first everything it can see, grouped by
# tenant, then a direct request for the other tenant's rows.
#
# Usage: ./scripts/read-as-acme.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_tenancy
ch_table "SELECT tenant_id, count() AS rows FROM tracing.otel_traces GROUP BY tenant_id" --user acme_reader
echo
ch_table "SELECT count() AS tenant_b_rows FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'" --user acme_reader
