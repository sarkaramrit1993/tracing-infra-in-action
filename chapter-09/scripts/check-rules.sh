#!/usr/bin/env bash
# Lists every recording and alerting rule Prometheus loaded, with its health.
#
# Usage: ./scripts/check-rules.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require prometheus

rules=$(q rules)
[ -n "$rules" ] || die "Prometheus loaded no rules. Is rules/ mounted? Try: docker compose up -d prometheus"
printf '%s\n' "$rules"
