#!/usr/bin/env bash
# Drops every materialized view the exercises build. A view left behind keeps
# folding new inserts into its rollup, including the generator's next run.
#
# Usage: ./scripts/drop-rollup-views.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_clickhouse

ch --query "DROP VIEW IF EXISTS tracing.red_by_service"
ch --query "DROP VIEW IF EXISTS tracing.red_no_populate"
ch --query "DROP VIEW IF EXISTS tracing.red_no_root"
ch --query "DROP VIEW IF EXISTS tracing.red_unmerged"
ch --query "DROP VIEW IF EXISTS tracing.red_p99_summed"
ch --query "DROP VIEW IF EXISTS tracing.red_p99_merged"
echo "no materialized views left in tracing"
