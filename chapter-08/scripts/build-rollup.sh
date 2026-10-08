#!/usr/bin/env bash
# Runs listing 8.3 from clickhouse/rollup.sql: builds the per-minute rollup
# from the rows already on disk, then reads the dashboard's request count.
#
# Usage: ./scripts/build-rollup.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data

out=$(ch_file clickhouse/rollup.sql)
[ -n "$out" ] || die "the rollup returned no rows for the last hour. $GENERATE"
{ printf 'service_name\trequests\n'; printf '%s\n' "$out"; } | align
