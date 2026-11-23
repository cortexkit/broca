#!/usr/bin/env bash
# Shared reader for the feature floor, used by stage-release (to print it on the release card)
# and place-release (to refuse a build that cannot read it).
#
# WHAT THE FLOOR IS. The floor file lists the storage formats this machine's sessions are already
# written in (crates/broca-records/src/floor.rs; today only the session store's
# `session-storage/v1`). A build whose known-feature table does not cover the floor could not
# serve sessions that exist here: placing it is a rollback onto unreadable data. place-release
# asks the candidate build for its table (`ck-broca --wal-known-features`) and refuses unless it
# covers the floor. The file and the flag keep the names they had when the floor listed the old
# log format's frame features.
#
# These scripts only READ the floor. The session store raises it before it writes its first
# session in a state root; it only ever grows.
#
# Format: one feature name per line, sorted. Blank lines and surrounding whitespace are ignored.
# Anything else on a line is a name, so a stray line makes placement refuse rather than pass.

# The floor lives in the state root, resolved by broca's own rule: $BROCA_STATE_ROOT when set and
# non-empty, else $XDG_DATA_HOME/cortexkit/broca, else $HOME/.local/share/cortexkit/broca. The
# same rungs as crates/broca-module-serve/src/state_root.rs and scripts/lib/state_root.py;
# scripts/test-state-root.py checks that the three agree.
broca_state_root() {
  printf '%s\n' "${BROCA_STATE_ROOT:-${XDG_DATA_HOME:-$HOME/.local/share}/cortexkit/broca}"
}

broca_wal_floor_path() {
  printf '%s/wal-feature-floor\n' "$(broca_state_root)"
}

# Prints the floor's names, one per line. A missing file is an empty floor (nothing gated has
# been written here yet). Returns 2 when the file exists but cannot be read: a floor nobody can
# read must not be treated as an empty one.
broca_wal_floor_names() {
  local file="$1" line
  [ -e "$file" ] || return 0
  [ -r "$file" ] || return 2
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%$'\r'}"
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    if [ -n "$line" ]; then
      printf '%s\n' "$line"
    fi
  done <"$file"
  return 0
}

# One line for a card: the names comma-separated, or `none`.
broca_wal_floor_summary() {
  local names
  names="$(broca_wal_floor_names "$1")" || { printf 'UNREADABLE\n'; return 0; }
  if [ -z "$names" ]; then
    printf 'none\n'
  else
    printf '%s\n' "$names" | paste -sd, - | sed 's/,/, /g'
  fi
}
