#!/usr/bin/env bash
# Shows how many post-sampler latency bucket series Prometheus holds and which
# bucket boundaries (le values) they carry.
#
# Usage: ./scripts/show-histogram-buckets.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require prometheus
q buckets post_duration_milliseconds_bucket
