#!/usr/bin/env bash
# Mint a RECREATED-LINEAGE specimen: a session whose materialized store rows describe one WAL
# while the WAL at its key carries a different identity and different content.
#
# This is the precondition for the guard added in 0da2fd3. Reading such a session must DISCARD
# the materialized seed and replay the WAL from sequence zero, so the returned page shows the
# WAL's content — never a splice of store rows onto a foreign tail, and never the old content
# wearing the new id.
#
# TWO CONSTRAINTS MAKE THIS HARDER THAN IT LOOKS, and both produce a green that proves nothing:
#
#   1. A SPECIMEN MINTED BEFORE THE FIX IS DEPLOYED IS USELESS. The migration adds lineage_id as
#      NULL for pre-existing rows (deliberately — that is the absence-means-unknown ruling that
#      spares every legacy session a forced full-WAL reread). NULL never compares as a mismatch,
#      so such a specimen sails down the incremental path and LOOKS LIKE A PASS while measuring
#      the grandfather clause rather than the repair. This script refuses if the column is absent.
#
#   2. THE SWAP MUST NOT BE FOLLOWED BY A SEND. A send re-projects, which rewrites the stored id
#      to match the new WAL and heals the mismatch before anyone can read it. So the second WAL
#      is produced in a SEPARATE session and copied over, rather than by appending here.
#
# The specimen is SINGLE-USE: the first read that detects the mismatch repairs the store, and the
# second read is an ordinary incremental read. That is the fix working. Mint several if a repeat
# is needed.
#
# Usage:  scripts/mint-lineage-specimen.sh [--count N]
# Exit:   0 minted, 1 refused on a checked condition, 2 cannot compare / bad invocation.

set -euo pipefail

COUNT=1
VICTIM_ARG=""
DONOR_ARG=""
ROOT_ARG=""
HARNESS_ARG=""
while [ $# -gt 0 ]; do
  case "$1" in
    --count|--victim|--donor|--project-root|--harness)
      if [ "$#" -lt 2 ] || [ -z "${2:-}" ]; then
        echo "mint-lineage-specimen: usage error: $1 requires an argument" >&2
        exit 2
      fi
      ;;
  esac
  case "$1" in
    --count)  COUNT="${2:-1}"; shift 2 ;;
    # ADOPT MODE: perform the swap on sessions someone else already owns.
    #
    # A specimen is only useful to a consumer whose module RESOLVES the session. alfonso-core's
    # page path serves an unknown session as empty, so a specimen minted here is invisible to it
    # however correct the underlying state is. In that case the owner mints the pair and this
    # script only performs the swap.
    --victim) VICTIM_ARG="${2:-}"; shift 2 ;;
    --donor)  DONOR_ARG="${2:-}"; shift 2 ;;
    # Adopted sessions may live under a different project root than this checkout.
    --project-root) ROOT_ARG="${2:-}"; shift 2 ;;
    # The harness component of the bind triple. NOT a constant: this store holds BOTH eras --
    # 19,993 sessions keyed 'runner' (broca-session's own --harness default) and 2,726 keyed
    # 'broca' from callers that canonicalize. Hardcoding either one silently misses the other,
    # and the miss surfaces as the drift arm's "key derivation may have drifted" rather than as
    # a wrong-harness message. Default matches what this script's own mint path produces.
    --harness) HARNESS_ARG="${2:-}"; shift 2 ;;
    *) echo "mint-lineage-specimen: unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [ -n "$VICTIM_ARG" ] && [ -z "$DONOR_ARG" ]; then
  echo "mint-lineage-specimen: --victim requires --donor" >&2; exit 2
fi
if [ -z "$VICTIM_ARG" ] && [ -n "$DONOR_ARG" ]; then
  echo "mint-lineage-specimen: --donor requires --victim" >&2; exit 2
fi
if [ -n "$VICTIM_ARG" ] && [ "$VICTIM_ARG" = "$DONOR_ARG" ]; then
  echo "mint-lineage-specimen: victim and donor must differ, or there is no mismatch" >&2
  exit 2
fi

if [ -z "$VICTIM_ARG" ] && ! command -v timeout >/dev/null 2>&1; then
  echo "mint-lineage-specimen: timeout is required; install GNU coreutils and put timeout on PATH (macOS: brew install coreutils, then add its libexec/gnubin)" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Adopted sessions may have been minted under another root; the key derivation must use the one
# they were actually created with or every lookup silently misses.
PROJECT_ROOT="${ROOT_ARG:-$REPO_ROOT}"
# broca-session defaults --harness to "runner"; adopted sessions may carry "broca".
HARNESS="${HARNESS_ARG:-runner}"
STATE_ROOT="${BROCA_STATE_ROOT:-${XDG_DATA_HOME:-$HOME/.local/share}/cortexkit/broca}"
CONN="$HOME/.local/share/cortexkit/run/subc-connection.json"
CLIENT="$REPO_ROOT/target/release/broca-session"
DB="$STATE_ROOT/run-index.db"
MODEL="${SPECIMEN_MODEL:-google/antigravity-gemini-3.5-flash}"

[ -x "$CLIENT" ] || { echo "mint: build broca-session first: cargo build --release --bin broca-session" >&2; exit 2; }
[ -f "$CONN" ]   || { echo "mint: no subc connection at $CONN" >&2; exit 2; }
[ -f "$DB" ]     || { echo "mint: no run-index at $DB" >&2; exit 2; }

# Constraint 1, checked rather than assumed.
if [ "$(sqlite3 "$DB" "PRAGMA table_info(meta);" | grep -c "lineage_id")" -eq 0 ]; then
  echo "mint: REFUSING — the live run-index has no lineage_id column." >&2
  echo "  The running daemon predates 0da2fd3. A specimen minted now would carry a NULL" >&2
  echo "  stored id, never compare as a mismatch, and produce a false green." >&2
  exit 1
fi

key_hash() {  # FNV-1a 64 over project_root␟runner␟session, matching the WAL filename derivation
  python3 - "$1" "$2" "$HARNESS" <<'PY'
import sys
root, sess, harness = sys.argv[1], sys.argv[2], sys.argv[3]
h = 0xcbf29ce484222325
for b in f"{root}\x1f{harness}\x1f{sess}".encode():
    h ^= b
    h = (h * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
print(f"{h:016x}")
PY
}

run_turn() {  # session, prompt
  timeout 180 "$CLIENT" --subc "$CONN" --project-root "$PROJECT_ROOT" \
    --session "$1" --harness "$HARNESS" run --model "$MODEL" --prompt "$2" 2>&1
}

stored_lineage() {  # session
  sqlite3 "$DB" "SELECT COALESCE(lineage_id,'NULL') FROM meta WHERE session='$PROJECT_ROOT'||char(31)||'$HARNESS'||char(31)||'$1';"
}

for n in $(seq 1 "$COUNT"); do
  STAMP="$(date +%s)-$n-$RANDOM"

  if [ -n "$VICTIM_ARG" ]; then
    # ADOPT: the caller owns these sessions and has already run turns through them.
    [ "$COUNT" -eq 1 ] || { echo "mint: --count is meaningless with an adopted pair" >&2; exit 2; }
    VICTIM="$VICTIM_ARG"
    DONOR="$DONOR_ARG"
    STAMP="adopted-$STAMP"
  else
    VICTIM="lineage-specimen-$STAMP"      # the session handed out; its store rows are the stale side
    DONOR="lineage-donor-$STAMP"          # supplies a WAL with a different identity and content

    # Drain the client before searching: grep -q can close a pipe after the first
    # completion marker and SIGPIPE a still-writing successful client.
    TURN_OUTPUT="$(run_turn "$VICTIM" "Reply with exactly: VICTIM_CONTENT_$STAMP")" && grep -aq "reason=Completed" <<<"$TURN_OUTPUT" \
      || { echo "mint: victim turn did not complete" >&2; exit 1; }
    TURN_OUTPUT="$(run_turn "$DONOR" "Reply with exactly: DONOR_CONTENT_$STAMP")" && grep -aq "reason=Completed" <<<"$TURN_OUTPUT" \
      || { echo "mint: donor turn did not complete" >&2; exit 1; }
  fi

  VICTIM_WAL="$STATE_ROOT/wal/$(key_hash "$PROJECT_ROOT" "$VICTIM").wal"
  DONOR_WAL="$STATE_ROOT/wal/$(key_hash "$PROJECT_ROOT" "$DONOR").wal"
  [ -f "$VICTIM_WAL" ] && [ -f "$DONOR_WAL" ] \
    || { echo "mint: expected WAL files not found; key derivation may have drifted" >&2; exit 2; }

  BEFORE="$(stored_lineage "$VICTIM")"
  [ "$BEFORE" != "NULL" ] \
    || { echo "mint: REFUSING — victim materialized with a NULL id, so it cannot mismatch." >&2; exit 1; }

  # The swap. Copy rather than move so the donor stays intact as a reference transcript.
  cp "$VICTIM_WAL" "/tmp/specimen-original-$STAMP.wal"
  cp "$DONOR_WAL" "$VICTIM_WAL"

  AFTER="$(stored_lineage "$VICTIM")"
  [ "$AFTER" = "$BEFORE" ] \
    || { echo "mint: REFUSING — the stored id changed during the swap ($BEFORE -> $AFTER)." >&2
         echo "  Something re-projected; the mismatch has already healed." >&2; exit 1; }

  echo "specimen $n"
  echo "  session        $VICTIM"
  echo "  project-root   $PROJECT_ROOT"
  echo "  stored id      $BEFORE"
  if [ -n "$VICTIM_ARG" ]; then
    # In adopt mode the content came from the CALLER's turns, so naming a sentinel this script
    # did not write would be a guess presented as fact -- and a wrong expected string is worse
    # than none, because the drive then fails on the assertion rather than the specimen.
    echo "  describes      whatever $VICTIM held before the swap"
    echo "  wal now holds  $DONOR's content, under a different identity"
  else
    echo "  describes      VICTIM_CONTENT_$STAMP"
    echo "  wal now holds  DONOR_CONTENT_$STAMP, under a different identity"
  fi
  echo "  original wal   /tmp/specimen-original-$STAMP.wal"
done

cat <<'NOTE'

To drive it:
  broca-session --subc <conn> --project-root <root> --session <session> read --limit 50

  read 1   page must byte-equal a from-zero WAL replay: it shows DONOR content, never VICTIM
           content and never a splice of the two
  read 2   still correct, and no longer replays from zero

Timing is not the contract. A slow second read means the repair failed, which is logged and
non-fatal by design — the page is WAL-authoritative either way.
NOTE
