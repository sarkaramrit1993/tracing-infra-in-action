#!/usr/bin/env bash
# Sends a fixed workload and reads the same calls behind three samplers: a plain
# tail sampler (each survivor counted once), a probabilistic sampler that stamps
# its threshold (each survivor counted at its weight), and a keep-every-error
# policy. One table shows the rate bias, the weighted fix and the error-rate skew.
#
# Usage: ./scripts/show-sampling-bias.sh [requests, default 20000]
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

N=$(requests_arg "${1:-}")
measure "$N"

PCT=$(sampler_pct tail_sampling/uniform)
PROB=$(sampler_pct probabilistic_sampler)
POLICY=$(sampler_pct tail_sampling/policy)
row() { printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$(q percent "$3" "$2")" "$(q ratio "$2" "$N")x"; }
{
  printf 'counted\tcalls\terrors\terror rate\tcalls vs truth\n'
  row "ground truth (load generator)" "$N" "$(m ERRORS)"
  row "edge, before the sampler" "$(m edge_calls)" "$(m edge_errors)"
  row "after a ${PCT}% tail sampler, each survivor once" "$(m uniform_calls)" "$(m uniform_errors)"
  row "after a ${PROB}% probabilistic sampler, at its weight" "$(m weighted_calls)" "$(m weighted_errors)"
  row "after keep-every-error plus ${POLICY}%, each survivor once" "$(m policy_calls)" "$(m policy_errors)"
} | q table
echo
echo "each span the probabilistic sampler kept was counted at a weight of $(q weight-per-span) (calls counted divided by spans kept)"
