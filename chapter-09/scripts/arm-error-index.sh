#!/usr/bin/env bash
# Recreates the listing 9.2 error-issue index, empty, from clickhouse/error_index.sql.
# A materialized view only sees inserts made after it exists, so arm it before
# sending the traffic it should index.
#
# Usage: ./scripts/arm-error-index.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require clickhouse

ch --query "DROP VIEW IF EXISTS tracing.exc_mv"
ch --query "DROP TABLE IF EXISTS tracing.exceptions"
ch_file clickhouse/error_index.sql
echo "armed: tracing.exc_mv now feeds an empty tracing.exceptions"
