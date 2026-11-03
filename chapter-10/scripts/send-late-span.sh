#!/usr/bin/env bash
# Finds a window Flink has already closed, sends one span whose event time falls
# inside it, and waits for Flink's late-record counter to move. The closed
# window's row does not change.
#
# Usage: ./scripts/send-late-span.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require flink

SPAN="GET /cart"
WINDOW=$(kafka_read red_metrics | q latest-window "$SPAN")
[ -n "$WINDOW" ] || die "Flink has not closed a window yet. Run ./scripts/wait-until-ready.sh first"

BEFORE_ROW=$(kafka_read red_metrics | q window-row "$SPAN" "$WINDOW")
BEFORE=$(q flink-late)
[ "$BEFORE" != "none" ] || BEFORE=0
echo "window already closed: $SPAN  $BEFORE_ROW"
echo "late spans Flink has dropped so far: $BEFORE"

q late-span "$SPAN" "$WINDOW" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9093 --topic spans.flat 2> /dev/null
echo "sent one $SPAN span with an event time 30 s into that window"

SENT=$(date +%s)
late_count_moved() {
  local now
  now=$(q flink-late)
  [ "$now" != "none" ] && ge "$now" "$(( BEFORE + 1 ))"
}
poll "waiting for Flink to report it (the REST value can lag by a few minutes)" 300 late_count_moved
echo "late spans Flink has dropped now: $(q flink-late)  (seen after $(( $(date +%s) - SENT )) s)"
echo "window after:  $SPAN  $(kafka_read red_metrics | q window-row "$SPAN" "$WINDOW")"
