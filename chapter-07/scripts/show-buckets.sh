#!/usr/bin/env bash
# Lists the buckets in SeaweedFS, then the start of Tempo's block tree.
#
# Usage: ./scripts/show-buckets.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_ready
echo "s3.bucket.list" | docker compose exec -T seaweedfs weed shell 2> /dev/null | grep -v '^>'
echo
echo "fs.tree /buckets/tempo-blocks" | docker compose exec -T seaweedfs weed shell 2> /dev/null \
  | grep -v '^>' | head -12
