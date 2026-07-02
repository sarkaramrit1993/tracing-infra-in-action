#!/usr/bin/env bash
# Tears down all Chapter 3 local infrastructure: docker-compose stacks and the Kind cluster.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CLUSTER_NAME="tracing-ch3"

echo "=== Chapter 3 teardown ==="

# Bring down each compose stack. The benchmark file is an overlay, so it is
# torn down together with the base file. Guard so a stopped stack or missing
# file does not abort the rest.
teardown_stack() {
    local label="$1"; shift
    echo "Removing stack: $label"
    ( cd "$ROOT_DIR" && docker compose "$@" down -v --remove-orphans ) || \
        echo "  (skip: $label not running or compose unavailable)"
}

teardown_stack "multi-tier"  -f docker-compose.yml
teardown_stack "single-tier" -f docker-compose.single-tier.yml
teardown_stack "benchmark"   -f docker-compose.yml -f docker-compose.benchmark.yml

# Delete the Kind cluster if kind is installed.
if command -v kind >/dev/null 2>&1; then
    echo "Deleting Kind cluster: $CLUSTER_NAME"
    kind delete cluster --name "$CLUSTER_NAME" || \
        echo "  (skip: cluster '$CLUSTER_NAME' not found)"
else
    echo "  (skip: kind not installed)"
fi

echo "=== Teardown complete: compose stacks removed, Kind cluster '$CLUSTER_NAME' deleted ==="
