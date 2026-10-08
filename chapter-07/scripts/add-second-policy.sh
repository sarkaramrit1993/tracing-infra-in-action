#!/usr/bin/env bash
# Adds a second policy that names tenant_b for acme_reader, counts what
# acme_reader reads of tenant_b, then drops it and counts again.
#
# Usage: ./scripts/add-second-policy.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_tenancy
ch --query "DROP ROW POLICY IF EXISTS audit_read ON tracing.otel_traces"
ch --query "
CREATE ROW POLICY audit_read ON tracing.otel_traces
USING tenant_id = 'tenant_b' TO acme_reader"
ch_table "SELECT count() AS with_audit_read FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'" --user acme_reader
ch --query "DROP ROW POLICY audit_read ON tracing.otel_traces"
echo
ch_table "SELECT count() AS without_it FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'" --user acme_reader
