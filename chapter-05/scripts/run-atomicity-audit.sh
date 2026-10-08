#!/usr/bin/env bash
# Runs benchmarks/atomicity_audit.py once per failure mode and prints one row
# each. Needs no stack. The audit writes a result file next to itself, so this
# runs a copy in a scratch directory and leaves benchmarks/results/ as it was.
#
# Usage: ./scripts/run-atomicity-audit.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
cp benchmarks/atomicity_audit.py "$scratch/"

printf '%-18s %6s %7s %8s  %s\n' "failure mode" whole absent partial verdict
for mode in none drop-whole-trace producer-crash buffer-overflow; do
  out=$(FAILURE_MODE=$mode python3 "$scratch/atomicity_audit.py" || true)
  counts=$(printf '%s\n' "$out" | sed -n 's/.*whole=\([0-9,]*\) absent=\([0-9,]*\) partial=\([0-9,]*\).*/\1 \2 \3/p' | tr -d ,)
  [ -n "$counts" ] || die "the audit printed no counts for $mode: $out"
  verdict=$(printf '%s\n' "$out" | grep -o 'PASS\|FAIL' | tail -1)
  read -r whole absent partial <<< "$counts"
  printf '%-18s %6s %7s %8s  %s\n' "$mode" "$whole" "$absent" "$partial" "$verdict"
done
