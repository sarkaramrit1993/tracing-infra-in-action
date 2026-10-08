#!/usr/bin/env bash
# Drops every scratch table the compression exercise created, then lists what
# is left in the tracing database.
#
# Usage: ./scripts/drop-compression-tables.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
for t in $COMPRESS_TABLES; do
  ch --query "DROP TABLE IF EXISTS tracing.$t"
done
ch --query "SHOW TABLES FROM tracing"
