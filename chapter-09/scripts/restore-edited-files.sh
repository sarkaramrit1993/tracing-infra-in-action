#!/usr/bin/env bash
# Puts back any file an exercise edited and left behind as a .bak, and restarts
# the service that reads it so nothing keeps running the edited copy.
#
# Usage: ./scripts/restore-edited-files.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

restored=0
restore() {
  local file=$1
  shift
  rm -f "$file.tmp"
  [ -f "$file.bak" ] || return 0
  mv "$file.bak" "$file"
  echo "restored $file"
  restored=$((restored + 1))
  if [ "$#" -gt 0 ]; then
    docker compose "$@"
  fi
}

restore collector/gateway-config.yaml restart otel-collector
restore docker-compose.yml up -d prometheus
restore loki/loki.yaml restart loki
restore clickhouse/error_index.sql

[ "$restored" -gt 0 ] || echo "nothing to restore: every file is the one that shipped"
