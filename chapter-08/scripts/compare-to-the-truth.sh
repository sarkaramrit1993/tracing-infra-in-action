#!/usr/bin/env bash
# Runs listing 8.1 from clickhouse/unbiased.sql and lines its four answers up
# against what the generator recorded before it sampled anything.
#
# Usage: ./scripts/compare-to-the-truth.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data

out=$(ch_file clickhouse/unbiased.sql)
[ "$(printf '%s\n' "$out" | wc -l | tr -d ' ')" = 5 ] \
  || die "listing 8.1 returned no rows for the last hour. $GENERATE"

printf '%s\n' "$out" | awk -F'\t' '
  NR == 1 { biased = $2 } NR == 2 { weighted = $2 }
  NR == 3 { u99 = $2 } NR == 4 { w99 = $2 } NR == 5 { truth = $1; t99 = $2 }
  END {
    printf "%s\t%s\t%s\n", "", "requests", "p99_ms"
    printf "%s\t%s\t%s\n", "ignoring the weight", biased, u99
    printf "%s\t%s\t%s\n", "using the weight", weighted, w99
    printf "%s\t%s\t%s\n", "what really happened", truth, t99 }' | align
