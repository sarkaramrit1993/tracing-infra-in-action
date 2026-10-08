#!/usr/bin/env bash
# Samples the same ten-million-request population with a real coin per trace,
# ten times with ten seeds, and shows how far each weighted total lands from
# the truth. Reads no table.
#
# Usage: ./scripts/flip-real-coins.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_clickhouse

ch_table "
SELECT seed, count() AS kept, sum(w) AS weighted,
       round(100 * (sum(w) - 10000000) / 1e7, 2) AS pct_off
FROM (
  SELECT number AS n, arrayJoin(range(1, 11)) AS seed,
         multiIf(n < 9920000, 100, n < 9970000, 2, 1) AS w
  FROM numbers(10000000))
WHERE cityHash64(n, seed) % w = 0
GROUP BY seed ORDER BY seed"
