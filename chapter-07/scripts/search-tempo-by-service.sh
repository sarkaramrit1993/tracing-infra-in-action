#!/usr/bin/env bash
# Asks Tempo for recent checkout traces by service and span name, not by id,
# and prints how much it had to open to answer. Then asks ClickHouse.
#
# Usage: ./scripts/search-tempo-by-service.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_ready
echo "Tempo:"
curl -sf -m 30 -G "$TEMPO/api/search" \
  --data-urlencode 'q={ resource.service.name = "checkout-service" && name = "GET /checkout" }' \
  --data-urlencode 'limit=5' \
  | python3 -c '
import json, sys
doc = json.load(sys.stdin)
for t in doc.get("traces", []):
    print(t["traceID"], t.get("rootTraceName", ""))
m = doc.get("metrics", {})
print("Tempo read %s bytes of blocks to answer" % m.get("inspectedBytes", 0))
' || die "Tempo did not answer the search. $HINT"
echo
echo "ClickHouse:"
ch_table "
SELECT trace_id, span_name FROM tracing.otel_traces
WHERE service_name = 'checkout-service' AND span_name = 'GET /checkout'
ORDER BY timestamp DESC LIMIT 5"
