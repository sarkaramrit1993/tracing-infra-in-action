#!/usr/bin/env bash
# Sends one AI agent run with the upload hook on and prompt capture off. The
# prompts go to files, and the spans carry only a reference to each file.
#
# Usage: ./scripts/capture-to-file.sh
# shellcheck source=scripts/agent-lib.sh
source "$(dirname "$0")/agent-lib.sh"

require_stack
require_clickhouse
echo "Running one AI agent run with the upload hook on and prompt capture off."
run_ai_agent 8k no_content 4 1 upload
echo "Waiting for the trace to settle in Tempo. This takes about 20 seconds."
SPANS=$(tempo_spans_settled "$TRACE_ID")
[ "$SPANS" -gt 0 ] || die "Tempo has none of the trace. $HINT"

REF=$(tempo_string_attrs "$TRACE_ID" gen_ai.input.messages_ref | cut -f4 | head -n 1)
[ -n "$REF" ] || die "The first model call has no gen_ai.input.messages_ref. See: docker compose --profile agent logs stand-in-model"
case "$REF" in
  file:///uploads/*_inputs.json) ;;
  *) die "Unexpected reference $REF" ;;
esac
FILE=${REF#file://}
[ -z "${FILE//[0-9a-zA-Z_.\/-]/}" ] || die "Unexpected reference $REF"
echo "gen_ai.input.messages_ref = $REF"
echo "Spans with gen_ai.input.messages in Tempo: $(tempo_string_attrs "$TRACE_ID" gen_ai.input.messages | wc -l | tr -d ' ')"
FILE_OUT=$({ docker compose run --rm -T --no-deps agent sh -c "wc -c < $FILE; tail -c 89 $FILE; echo" 2> /dev/null || true; })
[ -n "$FILE_OUT" ] || die "Could not read $FILE in the agent container. Run: docker compose --profile agent logs stand-in-model"
echo "The file, inside the agent container (size in bytes, then its last 89 bytes):"
echo "$FILE_OUT" | sed 's/^/  /'
