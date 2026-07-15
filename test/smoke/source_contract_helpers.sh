#!/bin/sh

# Requires one whitespace-insensitive token sequence inside a named top-level
# OCaml value binding. Scoping the search to the binding prevents a similar
# policy or workflow elsewhere in a large fixture from satisfying the check.
# The expected sequence must not depend on whitespace inside a string literal;
# this is a focused source-contract check, not an OCaml parser.
require_ocaml_binding_tokens() (
  path=$1
  binding=$2
  expected=$3
  contract=$4
  compact_expected=$(printf '%s' "$expected" | tr -d '[:space:]')

  if ! awk -v header="let $binding =" -v expected="$compact_expected" '
    {
      line = $0
      sub(/\r$/, "", line)
    }
    line == header {
      found = 1
      collecting = 1
    }
    collecting && line != header && (line ~ /^let / || line ~ /^\(\*\*/) {
      collecting = 0
    }
    collecting {
      body = body line
    }
    END {
      gsub(/[[:space:]]/, "", body)
      exit !(found && index(body, expected) != 0)
    }
  ' "$path"; then
    echo "$contract is missing from $binding: $expected ($path)" >&2
    exit 1
  fi
)
