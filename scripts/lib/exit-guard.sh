#!/usr/bin/env bash
# Bash 3.2 can report success after a nounset error when an EXIT cleanup succeeds.
# Source this file, install `trap 'broca_exit_guard cleanup' EXIT`, and set the
# flag immediately before every intended successful exit, including fallthrough.
BROCA_EXIT_GUARD_FINISHED=0

broca_exit_guard() {
  local status=$?
  local cleanup_status=0
  "$@" || cleanup_status=$?
  if [ "$status" -eq 0 ] && [ "$cleanup_status" -ne 0 ]; then
    status=$cleanup_status
  fi
  if [ "$status" -eq 0 ] && [ "${BROCA_EXIT_GUARD_FINISHED:-0}" -ne 1 ]; then
    printf '%s: exited before successful completion\n' "$0" >&2
    status=1
  fi
  exit "$status"
}
