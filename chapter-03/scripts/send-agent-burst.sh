#!/usr/bin/env bash
# Sends a short burst of AI agent runs through the agent tier to the gateway, with the
# gateway receiver at its 4 MiB default. The burst fits, so it all arrives.
#
# Usage: ./scripts/send-agent-burst.sh
# shellcheck source=scripts/agent-lib.sh
source "$(dirname "$0")/agent-lib.sh"

require_stack
echo "Switching the Collectors to the agent demo configs."
use_configs agent-genai-config.yaml gateway-default-limit-config.yaml
snapshot
echo "Running 5 AI agent runs with 4 model calls each. This takes about 40 seconds."
run_ai_agent 8k span_only 4 5
wait_for_export
report
echo "Next: ./scripts/send-oversized-agent-run.sh"
