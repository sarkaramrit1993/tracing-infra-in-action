# shellcheck shell=bash
# Sourced by the agent scripts. Not a reader step.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

CAPTURE_VAR=OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT
JAEGER=http://localhost:16686

die() { echo "$*" >&2; exit 1; }

# Docker output stays off the screen unless a step fails, then its tail is shown.
ERR_LOG=$(mktemp)
trap 'rm -f "$ERR_LOG"' EXIT
fail_with_log() { tail -n 20 "$ERR_LOG" >&2; die "$1"; }

# compose reads .env from this folder on every call, so a value written here
# reaches every later `docker compose run` without the reader editing anything.
set_capture() {
  local tmp
  tmp=$(mktemp)
  { grep -v "^${CAPTURE_VAR}=" .env 2>/dev/null || true; echo "${CAPTURE_VAR}=$1"; } > "$tmp"
  mv "$tmp" .env
}

current_capture() {
  local value
  value=$({ grep "^${CAPTURE_VAR}=" .env 2>/dev/null || true; } | tail -n 1 | cut -d= -f2)
  echo "${value:-no_content}"
}

build_agent() {
  docker compose --profile agent build agent > "$ERR_LOG" 2>&1 \
    || fail_with_log "Could not build the agent image. The last lines of the build are above."
}
