# shellcheck shell=bash
# Sourced by every script in scripts/. Not a reader step.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

HINT="Is the stack up? From chapter-08/ run: docker compose up -d --wait"
GENERATE="Run python3 generate/generate.py"

die() { echo "$*" >&2; exit 1; }

# clickhouse-client reads stdin even for a --query, and `docker compose exec -T`
# hands it the caller's. From a terminal that never sends EOF, so a client
# without `< /dev/null` hangs with no output. ch_file needs its stdin for the
# .sql file, which is why it is a separate helper. NOTES.md has the long version.
ch() { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }

# ch_table QUERY: run QUERY and print the result as aligned columns under their
# names. Numeric columns are right-aligned.
ch_table() { ch --format TSVWithNames --query "$1" | align; }

align() {
  awk -F'\t' '
    { n = NR; cols[NR] = NF; for (i = 1; i <= NF; i++) { cell[NR, i] = $i
        if (length($i) > w[i]) w[i] = length($i)
        if (NR > 1 && $i !~ /^-?[0-9][0-9.]*%?$/) text[i] = 1 } }
    END { for (r = 1; r <= n; r++) { line = ""
        for (i = 1; i <= cols[r]; i++) {
          fmt = text[i] ? "%-" w[i] "s" : "%" w[i] "s"
          line = line (i > 1 ? "   " : "") sprintf(fmt, cell[r, i]) }
        sub(/ +$/, "", line); print line } }'
}

require_clickhouse() {
  ch --query "SELECT 1" > /dev/null 2>&1 || die "ClickHouse is not answering. $HINT"
}

# Every chapter query reads the last hour, and the generator writes the twenty
# minutes before it ran. Past about forty minutes the oldest rows slide out of
# the window and every total reads short, which looks like broken weights.
require_data() {
  require_clickhouse
  local truth spans fresh
  truth=$(ch --query "SELECT count() FROM tracing.ground_truth")
  spans=$(ch --query "SELECT count() FROM tracing.otel_traces")
  [ "$truth" != 0 ] && [ "$spans" != 0 ] || die "no data yet. $GENERATE"
  fresh=$(ch --query "SELECT min(timestamp) >= toStartOfMinute(now() - INTERVAL 1 HOUR)
                      FROM tracing.otel_traces WHERE service_name = 'checkout-service'")
  [ "$fresh" = 1 ] \
    || die "the oldest rows have slid out of the one-hour window every query reads, so totals would read short. $GENERATE"
}

# insert_batch SERVICE MINUTE: 500 root spans in one minute, each with a weight
# of 100 and a duration of 180 ms, so one batch stands for 50,000 requests.
insert_batch() {
  ch --param_service="$1" --param_minute="$2" --query "
INSERT INTO tracing.otel_traces
  (timestamp, trace_id, span_id, parent_span_id, service_name, span_name,
   status_code, duration_ns, adjusted_count, attributes)
SELECT toDateTime64({minute:DateTime}, 9),
       lower(hex(MD5(concat({service:String}, toString(now64(9)), toString(number))))),
       lower(hex(reinterpretAsFixedString(toUInt64(number)))), '',
       {service:String}, 'GET /checkout', 'STATUS_CODE_UNSET',
       180000000, 100, map('http.method', 'POST')
FROM numbers(500)"
}
