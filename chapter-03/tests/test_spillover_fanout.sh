#!/usr/bin/env bash
# Asserts what Listing 3.3 actually does.
#
# The chapter says: "Both exporters receive every span; this is a fan-out, not a
# failover pair. The Collector copies every span to both, always." Four reviewers
# raised this listing, so the claim is worth demonstrating rather than asserting.
#
# Two exporters in one pipeline means the Collector fans out. This sends a known
# number of spans through spillover-config.yaml and checks that each exporter
# sent that same number, not half each.
#
# Deterministic by design: no hashing, no load balancing across the assertion, so
# the only timing concern is the batch flush, which the script waits out by
# polling until both counters stop moving.
set -euo pipefail
cd "$(dirname "$0")/.."

SPANS=${SPANS:-600}
IMAGE=otel/opentelemetry-collector-contrib:0.154.0
NET=spillover-test-net
COL=spillover-test-collector
SINK1=spillover-sink-1
SINK2=spillover-sink-2

cleanup() {
  docker rm -f "$COL" "$SINK1" "$SINK2" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  rm -f /tmp/spillover-sink.yaml
}
trap cleanup EXIT

echo "== spillover fan-out: Listing 3.3 =="

# Sinks stand in for the two gateways. The listing points its load balancing
# exporter at otel-gateway-1 and otel-gateway-2 and its fallback at gateway-1,
# so gateway-1 receives from both exporters and gateway-2 from one.
cat > /tmp/spillover-sink.yaml <<'YAML'
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
exporters:
  debug: {}
service:
  telemetry:
    metrics:
      readers: [{pull: {exporter: {prometheus: {host: 0.0.0.0, port: 8888}}}}]
  pipelines:
    traces: {receivers: [otlp], exporters: [debug]}
YAML

docker network create "$NET" >/dev/null 2>&1 || true
for s in "$SINK1:otel-gateway-1" "$SINK2:otel-gateway-2"; do
  name=${s%%:*}; alias=${s##*:}
  docker run -d --name "$name" --network "$NET" --network-alias "$alias" \
    -v /tmp/spillover-sink.yaml:/cfg.yaml "$IMAGE" --config=/cfg.yaml >/dev/null
done

docker run -d --name "$COL" --network "$NET" -p 14318:4318 -p 18888:8888 \
  -v "$PWD/collector/spillover-config.yaml:/cfg.yaml" "$IMAGE" --config=/cfg.yaml >/dev/null

echo "  waiting for the collector to accept traffic..."
until curl -sf localhost:18888/metrics >/dev/null 2>&1; do sleep 2; done
sleep 3

echo "  sending $SPANS spans..."
python3 - "$SPANS" <<'PY'
import json, sys, urllib.request
n = int(sys.argv[1]); per = 50
for b in range(n // per):
    spans = [{"traceId": "%032x" % (b * per + i + 1), "spanId": "%016x" % (i + 1),
              "name": "s", "kind": 1,
              "startTimeUnixNano": "1544712660000000000",
              "endTimeUnixNano": "1544712661000000000"} for i in range(per)]
    body = {"resourceSpans": [{"resource": {"attributes": [
        {"key": "service.name", "value": {"stringValue": "spillover-test"}}]},
        "scopeSpans": [{"spans": spans}]}]}
    urllib.request.urlopen(urllib.request.Request(
        "http://localhost:14318/v1/traces", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"}), timeout=10)
PY

# Poll until both exporter counters stop moving, rather than sleeping a guess.
echo "  waiting for the batch processor to drain..."
prev=""; stable=0
for _ in $(seq 1 30); do
  # grep exits 1 until the first export lands; without the guard, pipefail plus
  # set -e kills the script here with no output.
  now=$(curl -s localhost:18888/metrics | grep '^otelcol_exporter_sent_spans' | sort | awk '{print $1"="$2}' | tr '\n' ' ' || true)
  if [ "$now" = "$prev" ] && [ -n "$now" ]; then
    stable=$((stable + 1)); [ "$stable" -ge 3 ] && break
  else
    stable=0
  fi
  prev="$now"; sleep 2
done

echo
curl -s localhost:18888/metrics | grep '^otelcol_exporter_sent_spans' | sed 's/^/    /'
echo

python3 - "$SPANS" <<'PY'
import re, sys, urllib.request
want = int(sys.argv[1])
text = urllib.request.urlopen("http://localhost:18888/metrics", timeout=5).read().decode()
counts = {}
for m in re.finditer(r'^otelcol_exporter_sent_spans\{([^}]*)\}\s+([\d.e+]+)', text, re.M):
    labels, val = m.group(1), int(float(m.group(2)))
    ex = re.search(r'exporter="([^"]+)"', labels)
    if ex:
        counts[ex.group(1)] = counts.get(ex.group(1), 0) + val

missing = [e for e in ("loadbalancing", "otlp/fallback") if e not in counts]
if missing:
    print(f"FAIL: no counter for {missing}; exporters seen: {sorted(counts)}")
    sys.exit(1)

bad = [f"{e}={c} (expected {want})" for e, c in counts.items() if c != want]
if bad:
    print("FAIL: fan-out did not deliver every span to every exporter")
    for b in bad:
        print(f"      {b}")
    print("      Listing 3.3's prose claims both exporters receive every span.")
    sys.exit(1)

print(f"PASS: both exporters each sent all {want} spans.")
print("      Two exporters in one pipeline fan out; they do not split the stream,")
print("      and they are not a failover pair. Listing 3.3's prose holds.")
PY
