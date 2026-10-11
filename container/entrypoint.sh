#!/bin/bash
set -euo pipefail
display_file=$(mktemp)
x_pid=
bot_pid=
cleanup() {
  if [[ -n "$x_pid" ]]; then kill "$x_pid" 2>/dev/null || true; wait "$x_pid" 2>/dev/null || true; fi
  rm -f "$display_file"
}
forward() { if [[ -n "$bot_pid" ]]; then kill -TERM "$bot_pid" 2>/dev/null || true; fi; }
trap cleanup EXIT
trap forward TERM INT
Xvfb -displayfd 3 -screen 0 1280x800x24 -nolisten tcp 3>"$display_file" &
x_pid=$!
for ((i=0; i<100; i++)); do
  [[ -s "$display_file" ]] && break
  kill -0 "$x_pid" 2>/dev/null || { echo 'Xvfb exited during startup' >&2; exit 1; }
  sleep 0.1
done
[[ -s "$display_file" ]] || { echo 'Xvfb startup timed out' >&2; exit 1; }
export DISPLAY=":$(cat "$display_file")"
/usr/local/bin/xenomit-botd "$@" &
bot_pid=$!
# Signal traps interrupt wait. Leave Xvfb alive until botd finishes draining.
set +e
while true; do
  wait "$bot_pid"
  result=$?
  kill -0 "$bot_pid" 2>/dev/null || break
done
exit "$result"
