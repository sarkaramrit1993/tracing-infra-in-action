#!/usr/bin/env bash
# Edits collector/edge-config.yaml for a "Try this" and runs the step it belongs
# to. `reset` puts the shipped file back byte for byte, removes the user.id
# Collector if it is running, and restarts the edge Collector. The shipped file
# is copied aside before the first edit, so an interrupted run is recovered by
# `reset`.
#
#   bucket   add a 300ms histogram boundary, restart the edge, count series
#   limit    lower aggregation_cardinality_limit to 100, run the user.id step
#   reset    undo either
#
# Usage: ./scripts/try-edge.sh bucket | limit | reset
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

FILE=collector/edge-config.yaml
shipped() { has_pristine "$FILE" && cat "$PRISTINE_DIR/$(basename "$FILE")" > "$FILE"; return 0; }
restart_edge() {
  docker compose restart edge-collector > /dev/null 2>&1
  require edge-collector
}

case "${1:-}" in
  bucket)
    require edge-collector
    keep_pristine "$FILE"
    shipped
    rewrite "$FILE" '{ sub(/200ms, 500ms/, "200ms, 300ms, 500ms") } 1'
    echo "added a 300ms boundary to the buckets in $FILE; restarting the edge Collector"
    restart_edge
    ./scripts/count-series.sh
    ;;
  limit)
    require edge-collector
    keep_pristine "$FILE"
    shipped
    ./scripts/cardinality-down.sh > /dev/null
    rewrite "$FILE" '{ sub(/aggregation_cardinality_limit: 5000/, "aggregation_cardinality_limit: 100") } 1'
    echo "lowered aggregation_cardinality_limit to 100 in $FILE"
    ./scripts/cardinality-blast.sh
    ;;
  reset)
    if [ -n "$(docker compose -f docker-compose.yml -f docker-compose.cardinality.yml ps -q edge-collector-userid 2> /dev/null)" ]; then
      ./scripts/cardinality-down.sh > /dev/null
      echo "removed the user.id Collector"
    fi
    if has_pristine "$FILE"; then
      restore_pristine "$FILE"
      echo "restored $FILE"
    else
      echo "$FILE is the shipped file; restarting the edge Collector anyway"
    fi
    if answers edge-collector 2> /dev/null || service_is_coming_up edge-collector; then
      restart_edge
    fi
    ;;
  *)
    die "usage: ./scripts/try-edge.sh bucket | limit | reset"
    ;;
esac
