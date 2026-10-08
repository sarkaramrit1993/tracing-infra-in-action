#!/usr/bin/env bash
# Writes one row tagged tenant_b as the default login, which holds INSERT,
# then reads it back as tenant_b's login.
#
# Usage: ./scripts/insert-mislabeled-row.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_tenancy
delete_trace deadbeefdeadbeefdeadbeefdeadbeef
ch --query "
INSERT INTO tracing.otel_traces
  (timestamp, trace_id, tenant_id, span_id, service_name, span_name,
   status_code, duration_ns, attributes)
VALUES (now64(9), 'deadbeefdeadbeefdeadbeefdeadbeef', 'tenant_b', 'deadbeef',
        'checkout-service', 'validate_cart', 'STATUS_CODE_OK', 1000000,
        {'injected':'true'})"
ch_table "
SELECT tenant_id, span_id, attributes['injected'] AS injected
FROM tracing.otel_traces WHERE trace_id = 'deadbeefdeadbeefdeadbeefdeadbeef'" --user globex_reader
