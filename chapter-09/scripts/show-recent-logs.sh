#!/usr/bin/env bash
# Prints the 20 newest checkout-service log lines, whatever trace they carry.
#
# Usage: ./scripts/show-recent-logs.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require loki
q loki '{service_name="checkout-service"}' "$(( $(date +%s) - 900 ))" 20
