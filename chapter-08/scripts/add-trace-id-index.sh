#!/usr/bin/env bash
# Runs listing 8.2 from clickhouse/skipindex.sql and prints the index part of
# each of its three EXPLAIN plans: before the index, after it, and with an
# hour bound on the same lookup.
#
# Usage: ./scripts/add-trace-id-index.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data

out=$(ch_file clickhouse/skipindex.sql)
[ "$(printf '%s\n' "$out" | grep -c '^Expression ((Project')" = 3 ] \
  || die "listing 8.2 did not print three EXPLAIN plans. Run: docker compose exec -T clickhouse clickhouse-client --multiquery < clickhouse/skipindex.sql"

printf '%s\n' "$out" | awk '
  /^Expression \(\(Project/ { n++; on = 0
    if (n == 1) print "1. before the index"
    if (n == 2) print "\n2. after ADD INDEX and MATERIALIZE INDEX"
    if (n == 3) print "\n3. the same lookup, bounded to the last hour"
    next }
  /^    Indexes:/ { on = 1; next }
  on { sub(/^    /, ""); print }'

survivors=$(printf '%s\n' "$out" | awk '
  /^Expression \(\(Project/ { n++ } n == 2 && /Granules:/ { last = $2 } END { print last }')
echo
echo "the bloom filter took the ${survivors#*/} granules the primary key left down to ${survivors%/*}"
