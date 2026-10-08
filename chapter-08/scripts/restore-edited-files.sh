#!/usr/bin/env bash
# Puts back generate/generate.py if ./scripts/widen-the-tail.sh edited it and
# left the original behind as a .bak.
#
# Usage: ./scripts/restore-edited-files.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

FILE=generate/generate.py
rm -f "$FILE.tmp"
if [ -f "$FILE.bak" ]; then
  mv "$FILE.bak" "$FILE"
  echo "restored $FILE"
else
  echo "nothing to restore: $FILE is the one that shipped"
fi
