# shellcheck shell=bash
# The variables below are read by the scripts that source this file.
# shellcheck disable=SC2034
# Sourced by every script in scripts/. Not a reader step.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

APP=http://localhost:8080
TEMPO=http://localhost:3200
STATE_DIR="${TMPDIR:-/tmp}/tracing-in-action-ch07"
HINT="Is the stack up? From chapter-07/ run: docker compose up -d --build --wait"

die() { echo "$*" >&2; exit 1; }

# clickhouse-client reads stdin even with --query, and `docker compose exec -T`
# hands it the caller's. From a terminal that never sends EOF, so a client
# without `< /dev/null` hangs with no output. ch_file needs stdin for the .sql
# file, which is why it is a separate helper. NOTES.md has the long version.
ch() { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }

# ch_table QUERY [client args]: run QUERY and print the result as aligned
# columns under their names. Numeric columns are right-aligned.
ch_table() {
  local sql=$1
  shift
  ch "$@" --format TSVWithNames --query "$sql" | align
}

align() {
  awk -F'\t' '
    { n = NR; cols[NR] = NF
      for (i = 1; i <= NF; i++) {
        cell[NR, i] = $i
        if (length($i) > w[i]) w[i] = length($i)
        if (NR > 1 && $i !~ /^-?[0-9][0-9.]*%?$/) text[i] = 1
      } }
    END {
      for (r = 1; r <= n; r++) {
        line = ""
        for (i = 1; i <= cols[r]; i++) {
          fmt = text[i] ? "%-" w[i] "s" : "%" w[i] "s"
          line = line (i > 1 ? "  " : "") sprintf(fmt, cell[r, i])
        }
        sub(/ +$/, "", line); print line
      } }'
}

require_clickhouse() {
  ch --query "SELECT 1" > /dev/null 2>&1 || die "ClickHouse is not answering. $HINT"
}

# Every state file is tied to the ClickHouse and Tempo containers that were
# running when it was written. `docker compose down -v` makes new ones, so a
# state file left over from before it no longer counts.
stack_id() {
  docker compose ps -q clickhouse tempo 2> /dev/null | tr -d '\n'
}

save_state() {
  mkdir -p "$STATE_DIR"
  chmod 700 "$STATE_DIR"
  cat > "$STATE_DIR/$1"
}

# state FILE KEY HOW_TO: one value a previous script saved, or exit with HOW_TO.
state() {
  local value
  [ -f "$STATE_DIR/$1" ] || die "$3"
  value=$(sed -n "s/^$2=//p" "$STATE_DIR/$1")
  [ -n "$value" ] || die "$3"
  echo "$value"
}

NO_TRAFFIC="nothing sent yet: run ./scripts/send-traffic.sh first"
NOT_READY="still arriving: run ./scripts/wait-until-ready.sh first"

require_traffic() {
  require_clickhouse
  [ "$(state traffic STACK_ID "$NO_TRAFFIC")" = "$(stack_id)" ] || die "$NO_TRAFFIC"
}

# For a step that reads what send-traffic.sh sent. Before wait-until-ready.sh
# has seen all of it land, a read looks fine and is short.
require_ready() {
  require_traffic
  state traffic READY "$NOT_READY" > /dev/null
}

# poll LABEL SECONDS COMMAND...: rerun COMMAND once a second until it succeeds.
poll() {
  local label=$1 budget=$2 deadline
  shift 2
  deadline=$(( $(date +%s) + budget ))
  printf '%s... ' "$label"
  until "$@"; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo
      die "timed out after ${budget}s $label. Check docker compose ps and docker compose logs"
    fi
    sleep 1
  done
  echo ok
}

# The trace a step looks up: the last checkout send-traffic.sh made, which
# wait-until-ready.sh confirmed is in both stores.
last_trace() {
  state traffic LAST_TRACE "$NOT_READY"
}

# Every scratch table the compression exercise can create.
COMPRESS_TABLES="compress_listing compress_plain compress_many_status compress_clock_first compress_clock_first_no_delta compress_no_delta compress_attr_zstd1 compress_attr_zstd9"

require_compression_loaded() {
  require_clickhouse
  local rows
  rows=$(ch --query "
    SELECT count() FROM system.tables
    WHERE database = 'tracing' AND name IN ('compress_listing', 'compress_plain')")
  [ "$rows" = 2 ] || die "no compression tables yet: run ./scripts/build-compression-tables.sh first"
  rows=$(ch --query "
    SELECT (SELECT count() FROM tracing.compress_listing) = 200000
       AND (SELECT count() FROM tracing.compress_plain) = 200000")
  [ "$rows" = 1 ] || die "compression tables are empty: run ./scripts/load-compression-tables.sh first"
}

# The newest partition holding the tiering exercise's own rows, or exit.
tiering_partition() {
  local part
  part=$(ch --query "
    SELECT DISTINCT toYYYYMMDD(timestamp) FROM tracing.otel_traces
    WHERE service_name = 'tiering-demo' ORDER BY 1 DESC LIMIT 1")
  [ -n "$part" ] || die "no tiering-demo rows: run ./scripts/stage-tiering-partition.sh first"
  echo "$part"
}

# The disk a partition of otel_traces sits on: default or s3_cold.
partition_disk() {
  ch --query "
    SELECT any(disk_name) FROM system.parts
    WHERE database = 'tracing' AND table = 'otel_traces' AND active AND partition = '$1'"
}

# stage_tiering_rows DAYS_AGO: 50,000 tiering-demo spans dated midday DAYS_AGO days back.
stage_tiering_rows() {
  ch --query "
    INSERT INTO tracing.otel_traces
      (timestamp, trace_id, span_id, service_name, span_name,
       status_code, duration_ns, attributes)
    SELECT
      toDateTime64(toStartOfDay(now()), 9) - toIntervalDay($1) + toIntervalHour(12)
        + toIntervalMillisecond(number),
      lower(hex(MD5(toString(intDiv(number, 6))))),
      lower(hex(reinterpretAsFixedString(toUInt64(number)))),
      'tiering-demo',
      ['validate_cart', 'payment.charge', 'order.create'][(number % 3) + 1],
      'STATUS_CODE_OK',
      toUInt64(1000000 + (number * 2654435761) % 200000000),
      map('tier', 'demo')
    FROM numbers(50000)"
}

show_parts() {
  ch_table "
    SELECT partition, disk_name, sum(rows) AS rows, count() AS parts,
           formatReadableSize(sum(bytes_on_disk)) AS size
    FROM system.parts
    WHERE database = 'tracing' AND table = 'otel_traces' AND active
    GROUP BY partition, disk_name ORDER BY partition"
}

# The aggregate the tiering exercise times, wherever the partition sits.
# --time prints the client's own elapsed time on stderr, which leaves out the
# second or so docker exec spends starting the client.
time_demo_query() {
  local timing
  timing=$(mktemp)
  if ! ch --time --format TSVWithNames --query "
    SELECT count() AS spans, uniqExact(trace_id) AS traces,
           round(avg(duration_ns) / 1000000.0, 2) AS avg_ms
    FROM tracing.otel_traces WHERE service_name = 'tiering-demo'" 2> "$timing" | align; then
    cat "$timing" >&2
    rm -f "$timing"
    die "the query failed. $HINT"
  fi
  echo "took $(tr -d '[:space:]' < "$timing")s"
  rm -f "$timing"
}

# Drops the policies, demo rows and logins the tenancy exercise creates. Leaves
# the tenant_id column and the tenant map, which tenancy.sql converges on.
reset_tenancy() {
  ch --query "DROP ROW POLICY IF EXISTS tenant_filter ON tracing.otel_traces"
  ch --query "DROP ROW POLICY IF EXISTS audit_read ON tracing.otel_traces"
  if [ "$(ch --query "
    SELECT count() FROM system.columns
    WHERE database = 'tracing' AND table = 'otel_traces' AND name = 'tenant_id'")" = 1 ]; then
    ch --query "
      ALTER TABLE tracing.otel_traces
      DELETE WHERE trace_id IN ('aaaa0000aaaa0000aaaa0000aaaa0000',
                                'bbbb0000bbbb0000bbbb0000bbbb0000',
                                'deadbeefdeadbeefdeadbeefdeadbeef',
                                'cafe0000cafe0000cafe0000cafe0000')
      SETTINGS mutations_sync = 2"
  fi
  ch --query "DROP USER IF EXISTS acme_reader, globex_reader, newhire"
}

require_tenancy() {
  require_clickhouse
  [ "$(ch --query "SELECT count() FROM system.users WHERE name IN ('acme_reader', 'globex_reader')")" = 2 ] \
    && [ "$(ch --query "SELECT count() FROM system.row_policies WHERE short_name = 'tenant_filter'")" = 1 ] \
    || die "listing 7.4 is not applied: run ./scripts/apply-tenancy.sh first"
}

require_newhire() {
  require_tenancy
  [ "$(ch --query "SELECT count() FROM system.users WHERE name = 'newhire'")" = 1 ] \
    || die "there is no newhire login yet: run ./scripts/add-unmapped-login.sh first"
}
