#!/usr/bin/env bash
# Removes the user.id Collector the cardinality step started and waits until
# Prometheus has stopped scraping it. Nothing else changes.
#
# Usage: ./scripts/cardinality-down.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require prometheus

COMPOSE="docker compose -f docker-compose.yml -f docker-compose.cardinality.yml"

target_gone() {
  ! q targets | grep -q edge-userid-spanmetrics
}

echo '[]' > targets/edge-userid.json
$COMPOSE rm -sf edge-collector-userid > /dev/null 2>&1
poll "waiting for Prometheus to drop the target" 120 target_gone
q targets | q table
