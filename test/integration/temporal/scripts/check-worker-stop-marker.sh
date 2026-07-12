#!/bin/sh
set -eu

# Checks only the current run's marker. Compose's aggregated logs retain
# messages from earlier container instances, so a log line is not sufficient
# evidence that the worker stopped successfully for this invocation.
if [ "$#" -ne 1 ]; then
  echo "usage: check-worker-stop-marker.sh MARKER" >&2
  exit 2
fi

marker=$1
if [ ! -r "$marker" ]; then
  echo "worker did not publish shutdown marker: $marker" >&2
  exit 1
fi

actual=$(cat "$marker")
size=$(wc -c <"$marker")
# "worker-stopped" plus its terminating newline is exactly 15 bytes; checking
# the size as well as the read text rejects extra lines left by a stale or
# manually edited marker file.
if [ "$size" -ne 15 ] || [ "$actual" != "worker-stopped" ]; then
  echo "worker shutdown marker has unexpected contents: $marker" >&2
  exit 1
fi
