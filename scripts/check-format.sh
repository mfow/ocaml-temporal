#!/bin/sh
set -eu

if git grep -nI -E '[[:blank:]]$' -- .; then
  echo "tracked files contain trailing whitespace" >&2
  exit 1
fi
