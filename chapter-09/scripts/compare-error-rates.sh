#!/usr/bin/env bash
# Counts fraud.score calls and errors on both sides of the tail sampler and
# prints the error rate each side reports for the same traffic.
#
# Usage: ./scripts/compare-error-rates.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require prometheus otel-collector
require_traffic
# The post side lands at least one sampler decision after the pre side, so a
# complete pre count says nothing about the post one. Only wait-until-ready.sh
# checks both.
state traffic READY "still arriving: run ./scripts/wait-until-ready.sh first" > /dev/null

SPAN='service_name="checkout-service",span_name="fraud.score"'
ERR='status_code="STATUS_CODE_ERROR"'
pre=$(q sum "sum(pre_calls_total{$SPAN})")
post=$(q sum "sum(post_calls_total{$SPAN})")
pre_err=$(q sum "sum(pre_calls_total{$SPAN,$ERR})")
post_err=$(q sum "sum(post_calls_total{$SPAN,$ERR})")

[ "$pre" != none ] && [ "$post" != none ] \
  || die "no fraud.score span metrics in Prometheus yet. Run ./scripts/send-traffic.sh, then ./scripts/wait-until-ready.sh"
[ "$pre_err" != none ] || pre_err=0
[ "$post_err" != none ] || post_err=0

row() {
  awk -v name="$1" -v calls="$2" -v errors="$3" 'BEGIN {
    rate = calls > 0 ? sprintf("%.1f%%", 100 * errors / calls) : "n/a"
    printf "%-14s %8d %7d %11s\n", name, calls, errors, rate }'
}
printf '%-14s %8s %7s %11s\n' "" calls errors "error rate"
row "before sampler" "$pre" "$pre_err"
row "after sampler" "$post" "$post_err"
