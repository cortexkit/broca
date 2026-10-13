#!/usr/bin/env bash
# Shared reader for the WAL feature floor, used by stage-release (to print it on the release
# card) and place-release (to refuse a build that cannot read it).
#
# WHAT THE FLOOR IS. A WAL frame can name features its reader must understand. A build that does
# not know one of those names refuses the whole WAL rather than misreading it. The floor file
# lists every feature name that frames on THIS machine have already been written with, so a
# build whose known-feature table does not cover the floor would refuse data that exists here:
# placing it is a rollback into unreadable WALs. place-release asks the candidate build for its
# table (`ck-broca --wal-known-features`) and refuses unless it covers the floor.
#
# These scripts only READ the floor. Writer releases raise it, before their first frame that
# needs a new name; it only ever grows.
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
