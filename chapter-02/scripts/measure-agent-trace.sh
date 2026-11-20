#!/usr/bin/env bash
# Measures the agent trace at the 8 KB prompt for the current capture setting
# (three runs) and prints the table. Run it once with capture off and once
# with capture on, and the table shows both side by side.
#
# Usage: ./scripts/measure-agent-trace.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

CAPTURE=$(current_capture)
case "$CAPTURE" in
  no_content | span_only) ;;
  *) die "Capture is set to $CAPTURE. Run ./scripts/capture-on.sh or ./scripts/capture-off.sh first." ;;
esac

echo "Measuring three agent runs with capture $([ "$CAPTURE" = span_only ] && echo on || echo off). This takes about 15 seconds."
build_agent
mkdir -p "measure-out/$CAPTURE"
# Run as the reader's user so measure-out/ stays theirs to delete on Linux.
docker compose run --rm -T --user "$(id -u):$(id -g)" -v "$PWD/measure-out/$CAPTURE:/out" agent \
  python measure.py --sizes 8k --conditions "$CAPTURE" --runs 3 --out-dir /out > /dev/null 2> "$ERR_LOG" \
  || fail_with_log "The measurement failed. Its last output is above."

echo
docker compose run --rm -T --no-deps -v "$PWD/measure-out:/out:ro" agent python report.py /out 2> "$ERR_LOG" \
  || fail_with_log "Could not print the table. Its last output is above."
echo
echo "Next: ./scripts/capture-on.sh (or capture-off.sh), then ./scripts/measure-agent-trace.sh again."
echo "Files: chapter-02/measure-out/"
