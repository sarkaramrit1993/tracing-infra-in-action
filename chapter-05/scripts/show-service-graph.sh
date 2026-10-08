#!/usr/bin/env bash
# Runs listing 5.6 as printed: joins every span to its parent and keeps the
# pairs whose two ends carry different service.name values. Each pair is one
# call from one service to another; pairs inside one service are internal work.
#
# Usage: ./scripts/show-service-graph.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require clickhouse
require_ready

edges=$(ch_file clickhouse/service_graph.sql --format TSVWithNames)
recent "$edges" 'checkout-service'
printf '%s\n' "$edges" | q table
