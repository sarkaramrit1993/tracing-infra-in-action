#!/usr/bin/env bash
# Adds one row to the tenant map, putting newhire in tenant_a, and reads the
# table as newhire. The policy does not change.
#
# Usage: ./scripts/map-newhire-to-acme.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_newhire
ch --query "ALTER TABLE tracing.tenant_users DELETE WHERE user_name = 'newhire' SETTINGS mutations_sync = 2"
ch --query "INSERT INTO tracing.tenant_users (user_name, tenant_id) VALUES ('newhire', 'tenant_a')"
ch_table "SELECT tenant_id, count() AS rows FROM tracing.otel_traces GROUP BY tenant_id" --user newhire
