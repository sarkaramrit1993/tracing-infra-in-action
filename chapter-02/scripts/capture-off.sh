#!/usr/bin/env bash
# Switches prompt and response capture off, the default, for every agent run that follows.
#
# Usage: ./scripts/capture-off.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
set_capture no_content
echo "Capture is off. The next run carries model, token and tool metadata and no text."
echo "Next: ./scripts/run-agent.sh"
