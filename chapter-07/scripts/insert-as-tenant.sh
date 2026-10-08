#!/usr/bin/env bash
# Gives acme_reader INSERT, writes one row tagged tenant_b as acme_reader, and
# counts that row as each tenant.
#
# Usage: ./scripts/insert-as-tenant.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_tenancy
ch --query "GRANT INSERT ON tracing.* TO acme_reader"
ch --user acme_reader --query "
INSERT INTO tracing.otel_traces
  (timestamp, trace_id, tenant_id, span_id, service_name, span_name,
   status_code, duration_ns, attributes)
VALUES (now64(9), 'cafe0000cafe0000cafe0000cafe0000', 'tenant_b', 'cafe1111',
        'checkout-service', 'validate_cart', 'STATUS_CODE_OK', 1000000,
        {'from':'acme_reader'})"
ch_table "SELECT count() AS globex_sees FROM tracing.otel_traces WHERE trace_id = 'cafe0000cafe0000cafe0000cafe0000'" --user globex_reader
echo
ch_table "SELECT count() AS acme_sees FROM tracing.otel_traces WHERE trace_id = 'cafe0000cafe0000cafe0000cafe0000'" --user acme_reader
