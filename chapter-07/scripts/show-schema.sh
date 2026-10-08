#!/usr/bin/env bash
# Prints the table the stack created from listing 7.1, as ClickHouse holds it.
#
# Usage: ./scripts/show-schema.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
ch --query "SHOW CREATE TABLE tracing.otel_traces FORMAT TSVRaw"
