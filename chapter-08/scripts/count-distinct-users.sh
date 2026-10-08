#!/usr/bin/env bash
# Counts distinct users in the store, scales that by the count rule's
# multiplier, and prints both next to the number of users that really shopped.
#
# Usage: ./scripts/count-distinct-users.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data

ch_table "
SELECT uniqExact(attributes['user.id']) AS users_in_store,
       round(uniqExact(attributes['user.id'])
             * sum(adjusted_count) / count()) AS scaled_by_the_rule,
       (SELECT users FROM tracing.ground_truth) AS users_that_shopped
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''
  AND attributes['user.id'] != ''"
