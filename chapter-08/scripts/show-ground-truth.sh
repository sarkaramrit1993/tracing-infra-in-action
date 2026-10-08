#!/usr/bin/env bash
# Prints the answer sheet: what the generator produced before it sampled.
#
# Usage: ./scripts/show-ground-truth.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data

ch_table "
SELECT requests AS true_requests, p99_ms AS true_p99_ms, errors AS true_errors
FROM tracing.ground_truth"
