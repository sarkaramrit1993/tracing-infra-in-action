#!/usr/bin/env bash
# Edits generate/generate.py so slow and error requests are three percent of
# the population instead of 0.8, keeping the original as generate.py.bak.
# ./scripts/restore-edited-files.sh puts it back.
#
# Usage: ./scripts/widen-the-tail.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

FILE=generate/generate.py
SHIPPED="NORMAL, SLOW, ERROR = 9_920_000, 50_000, 30_000"
WIDE="NORMAL, SLOW, ERROR = 9_700_000, 180_000, 120_000"

if grep -qxF "$WIDE" "$FILE"; then
  echo "already widened: $FILE has $WIDE"
  exit 0
fi
grep -qxF "$SHIPPED" "$FILE" \
  || die "$FILE no longer has the line '$SHIPPED'. Run ./scripts/restore-edited-files.sh"
[ -f "$FILE.bak" ] || cp "$FILE" "$FILE.bak"
sed "s/^$SHIPPED\$/$WIDE/" "$FILE.bak" > "$FILE.tmp"
mv "$FILE.tmp" "$FILE"
echo "$FILE now has $WIDE"
