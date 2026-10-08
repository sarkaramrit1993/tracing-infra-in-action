#!/usr/bin/env bash
# Builds two populations that sample to the same survivors and have very
# different numbers of users. Reads no table, so it needs no generated data.
#
# Usage: ./scripts/show-two-worlds.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_clickhouse

ch_table "
SELECT
  count()                           AS true_users_world_a,
  uniqExact(if(kept, n, 999999999)) AS true_users_world_b,
  uniqExactIf(n, kept)              AS sampled_users_both,
  sum(if(kept, w, 0))               AS weighted_requests
FROM (
  SELECT
    number AS n,
    multiIf(n < 9920000, 100, n < 9970000, 2, 1) AS w,
    multiIf(n < 9920000, n % 100 = 0,
            n < 9970000, (n - 9920000) % 2 = 0,
            1) AS kept
  FROM numbers(10000000))"
