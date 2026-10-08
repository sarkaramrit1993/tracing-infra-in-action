#!/usr/bin/env bash
# Shows what a bare toUInt64() cast does to a weight that is not a whole number.
# Reads no table.
#
# Usage: ./scripts/show-weight-rounding.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_clickhouse

ch_table "
SELECT round(500./37, 4) AS exact_weight,
       toUInt64(500./37) AS bare_cast,
       toUInt64(round(500./37)) AS rounded,
       round(100 * (500./37 - toUInt64(500./37))
             / (500./37), 1) AS pct_light"
