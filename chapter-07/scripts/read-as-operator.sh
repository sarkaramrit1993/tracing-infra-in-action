#!/usr/bin/env bash
# Reads the table as the default login, which listing 7.4's policy exempts.
#
# Usage: ./scripts/read-as-operator.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_tenancy
ch_table "SELECT tenant_id, count() AS rows FROM tracing.otel_traces GROUP BY tenant_id ORDER BY tenant_id"
