#!/usr/bin/env bash
# Switches prompt and response capture on for every agent run that follows.
#
# Usage: ./scripts/capture-on.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
set_capture span_only
echo "Capture is on. The next run puts prompt, tool and retrieval text on the spans."
echo "Next: ./scripts/run-agent.sh"
