#!/usr/bin/env bash
# Shows the stream-time path from Flink's side: the assembly job, the spans it
# has read, how far its watermark trails the clock, its checkpoints, and what
# it has written to the two output topics.
#
# Usage: ./scripts/show-flink-job.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require clickhouse flink
require_ready

q flink-summary
printf '%-30s %s\n' "checkouts in traces.assembled" "$(topic_records traces.assembled 'GET /checkout')"
printf '%-30s %s\n' "spans in spans.late" "$(topic_records spans.late)"
