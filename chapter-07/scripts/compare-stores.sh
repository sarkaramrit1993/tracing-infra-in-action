#!/usr/bin/env bash
# Asks both stores for the same trace: ClickHouse by scanning rows for the
# trace id, Tempo by fetching the block that holds it.
#
# Usage: ./scripts/compare-stores.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_ready
TID=$(last_trace)
echo "trace_id = $TID"
echo
echo "ClickHouse (rows):"
ch_table "
SELECT span_name, round(duration_ns / 1000000.0, 1) AS took_ms
FROM tracing.otel_traces WHERE trace_id = '$TID' ORDER BY timestamp"
echo
echo "Tempo (block):"
curl -sf -m 10 "$TEMPO/api/traces/$TID" \
  | python3 -c '
import json, sys
spans = [s for b in json.load(sys.stdin)["batches"]
         for ss in b["scopeSpans"] for s in ss["spans"]]
spans.sort(key=lambda s: int(s["startTimeUnixNano"]))
rows = [("span_name", "took_ms")] + [
    (s["name"], "%g" % round((int(s["endTimeUnixNano"]) - int(s["startTimeUnixNano"])) / 1e6, 1))
    for s in spans]
w = max(len(r[0]) for r in rows)
for name, took in rows:
    print(name.ljust(w), took.rjust(7))
' || die "Tempo did not return trace $TID. $HINT"
