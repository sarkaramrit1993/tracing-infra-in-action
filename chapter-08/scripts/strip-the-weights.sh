#!/usr/bin/env bash
# Sets every adjusted_count on disk to 1, the way a processor that drops the
# sampling weight leaves a real store. python3 generate/generate.py undoes it.
#
# Usage: ./scripts/strip-the-weights.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data

ch --query "
ALTER TABLE tracing.otel_traces UPDATE adjusted_count = 1
WHERE 1 SETTINGS mutations_sync = 2"
echo "every adjusted_count is now 1. python3 generate/generate.py puts the weights back"
