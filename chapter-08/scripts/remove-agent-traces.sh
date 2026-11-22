#!/usr/bin/env bash
# Puts the table back the way the stack starts: deletes the agent spans and drops
# the columns find-runs-by-tool.sh adds, in case an interrupted run left them.
#
# Usage: ./scripts/remove-agent-traces.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_clickhouse

ch --query "
ALTER TABLE tracing.otel_traces
  DROP COLUMN IF EXISTS tool_name,
  DROP COLUMN IF EXISTS request_model,
  DROP COLUMN IF EXISTS agent_name"
ch --query "
ALTER TABLE tracing.otel_traces DELETE WHERE service_name = 'support-agent'
SETTINGS mutations_sync = 2"
echo "removed the agent spans; tracing.otel_traces has no agent columns"
