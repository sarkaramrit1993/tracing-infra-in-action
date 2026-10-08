#!/usr/bin/env bash
# Reads the exemplar trace ids off one latency histogram, pre or post sampler,
# and checks whether ClickHouse holds a trace for each.
#
# Usage: ./scripts/follow-exemplars.sh post
#        ./scripts/follow-exemplars.sh pre
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
case "${1:-}" in
  pre | post) SIDE=$1 ;;
  *) die "usage: ./scripts/follow-exemplars.sh post|pre" ;;
esac
require prometheus clickhouse

METRIC="${SIDE}_duration_milliseconds_bucket"
tids=$(q exemplars "$METRIC" "$(( $(date +%s) - 900 ))" | grep -E '^[0-9a-f]{32}$' || true)
[ -n "$tids" ] \
  || die "no exemplars on $METRIC in the last 15 minutes. Run ./scripts/send-traffic.sh, then ./scripts/wait-until-ready.sh"

list=$(printf '%s\n' "$tids" | sed "s/.*/'&'/" | paste -sd, -)
COUNTS=$(ch --query "SELECT trace_id, count() FROM tracing.otel_traces WHERE trace_id IN ($list) GROUP BY trace_id")
export COUNTS
printf '%s\n' "$tids" | awk '
  BEGIN { n = split(ENVIRON["COUNTS"], rows, "\n")
          for (i = 1; i <= n; i++) { split(rows[i], f, "\t"); spans[f[1]] = f[2] } }
  { k = ($0 in spans) ? spans[$0] : 0; print $0 " -> " k " spans"; total++; if (k > 0) found++ }
  END { printf "%d of %d exemplars point at a stored trace\n", found, total }'
