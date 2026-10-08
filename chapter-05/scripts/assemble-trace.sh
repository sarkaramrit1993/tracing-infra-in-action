#!/usr/bin/env bash
# Assembles the newest checkout from ClickHouse at read time: fetch every span
# with the trace ID (listing 5.2), then rebuild the parent-child tree in memory.
# Runs app/scatter_gather_query.py inside the app container, which already has
# the ClickHouse driver installed.
#
# Usage: ./scripts/assemble-trace.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require clickhouse
require_ready
TRACE_ID=$(state traffic LAST_TRACE "$ARRIVING")

docker compose exec -T consumer-clickhouse python scatter_gather_query.py "$TRACE_ID" < /dev/null
