#!/usr/bin/env bash
# Counts every checkout-service span on both sides of the tail sampler.
#
# Usage: ./scripts/show-span-totals.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require prometheus

pre=$(q sum 'sum(pre_calls_total{service_name="checkout-service"})')
post=$(q sum 'sum(post_calls_total{service_name="checkout-service"})')
[ "$pre" != none ] \
  || die "no span metrics in Prometheus yet. Run ./scripts/send-traffic.sh, then ./scripts/wait-until-ready.sh"
[ "$post" != none ] || post=0
printf '%-20s %6d\n' "spans before sampler" "$pre" "spans after sampler" "$post"
