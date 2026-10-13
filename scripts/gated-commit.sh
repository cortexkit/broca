#!/usr/bin/env bash
# Run the lint-inclusive gate, and commit ONLY if it passed.
#
# WHY THIS EXISTS. The natural way to type this is:
#
#     ./scripts/quickcheck.sh <crate> && git add -A && git commit -m "..."
#
# and that is wrong in a way the syntax cannot show you. `&&` chains an AND, but what you mean
# is an implication whose antecedent is the GATE specifically. In a multi-command chain the
# antecedent is whichever command ran last, so inserting `git add` between the gate and the
# commit silently moves the verdict onto `git add` — which essentially always succeeds. The
# gate still runs, still prints its failure, and the commit still happens.
#
# That is not a vigilance problem. The notation has no way to mark which of several commands
# carries the verdict, so the meaning lives only in the author's head and evaporates under any
# edit that adds a step. It has bitten this repo (a red quickcheck pushed because `git add`
# succeeded) and a sibling repo (`grep -c` exiting 0 on a match, letting a failed clippy
# through) within hours of each other, with no surface similarity between the two instances.
#
# The fix is not remembering. It is making the guarded path SHORTER than the unguarded one, so
# there is no keystroke argument for typing the chain by hand.
#
# Usage:  scripts/gated-commit.sh <commit-message-file> [crate ...]   <- PREFER THIS
#         scripts/gated-commit.sh -m "message" [crate ...]
#
# PREFER THE FILE FORM FOR ANYTHING LONGER THAN A LINE. A commit message is prose about code, so
# it naturally contains backticks, $, and quotes -- and inside `-m "..."` the shell evaluates all
# three BEFORE git sees them. On 2026-09-04 a message reading "those lines now carry
# `session=<name> wal=<addr>`" committed as "those lines now carry .": command substitution ate
# the only sentence that said what the change does, and the commit landed and pushed with exit 0.
#
# The failure is silent in the worst direction -- the gate passes, the commit succeeds, and the
# damage is to the durable record rather than to the code. A file is passed as a path and
# evaluated by nothing.
set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root" || exit 2

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <message-file> [crate ...]" >&2
  echo "       $0 -m <message> [crate ...]" >&2
  exit 2
fi

if [ "$1" = "-m" ]; then
  [ "$#" -ge 2 ] || { echo "error: -m needs a message" >&2; exit 2; }
  message="$2"; shift 2
  message_file=""
else
  message_file="$1"; shift
  [ -f "$message_file" ] || { echo "error: no message file at $message_file" >&2; exit 2; }
  message=""
fi

# Refuse an empty commit before spending minutes on a gate for nothing. Uses `status --porcelain`
# rather than `git diff`, which does not report UNTRACKED files — with the diff form this script
# refused to commit itself, and would refuse any new file. Found by running it rather than by
# reading it.
if [ -z "$(git status --porcelain)" ]; then
  echo "gated-commit: nothing to commit, refusing before running the gate" >&2
  exit 2
fi

# Derive the crates to check from the change itself when the caller names none. quickcheck
# requires at least one crate and exits 64 on none, so passing an empty list through produced a
# usage error dressed as a gate failure. Deriving also removes a hand-typed parameter, which is
# the same reason this script exists: a verification step whose arguments are reconstructed from
# memory each time is a verification step that will eventually be wrong.
if [ "$#" -eq 0 ]; then
  # Plain word-splitting rather than mapfile: macOS ships bash 3.2, where mapfile does not
  # exist and its absence surfaces as an unbound-variable error two lines later.
  # Root manifests and the lockfile affect compilation even when no crate source
  # changed. Associate these root files with the broca-subc package below so
  # scripts/quickcheck.sh verifies their resolved dependencies.
  #
  # Without this mapping, a root-only change skips Cargo verification entirely.
  # Selecting broca-subc runs scripts/quickcheck.sh with --locked, so a manifest
  # change that disagrees with Cargo.lock refuses the commit before staging.
  root_readers=$(git status --porcelain | awk '{print $NF}' \
    | sed -n -e 's|^Cargo.lock$|broca-subc|p' \
        -e 's|^Cargo.toml$|broca-subc|p')
  derived=$( { git status --porcelain | awk '{print $NF}' \
    | sed -n 's|^crates/\([^/]*\)/.*|\1|p'; echo "$root_readers"; } \
    | sed '/^$/d' | sort -u | tr '\n' ' ')
  if [ -n "$derived" ]; then
    # shellcheck disable=SC2086 -- deliberate split: crate names cannot contain spaces.
    set -- $derived
    echo "gated-commit: derived crates from the diff: $*"
  else
    # Nothing under crates/ changed, so quickcheck has no subject. Say so rather than
    # inventing one: a gate run against an unrelated crate would pass and mean nothing.
    echo "gated-commit: no crate source changed — committing without a crate gate"
    git add -A || exit 1
    if [ -n "$message_file" ]; then
      git commit -q -F "$message_file" || exit 1
    else
      git commit -q -m "$message" || exit 1
    fi
    echo "gated-commit: committed $(git log --oneline -1)"
    exit 0
  fi
fi

echo "gated-commit: running quickcheck on: $*"
# Capture the status DIRECTLY rather than inside `if ! ...`, where $? would hold the negated
# test result and report a failing gate as "exit 0". Caught by reading this script's own first
# refusal, which is the only reason it is right: the wrong version still refuses correctly and
# only lies about why.
./scripts/quickcheck.sh "$@"
status=$?
if [ "$status" -ne 0 ]; then
  # BOTH lines name the outcome, because a caller piping this through `tail -1` sees only the
  # second one. "the tree is untouched" meant "nothing was committed, your edits are as you
  # left them" -- and read through a tail it looks like "you have no changes", which sends the
  # reader hunting a lost edit instead of reading the gate output directly above. I misread my
  # own message that way twice on 2026-09-06.
  echo "gated-commit: GATE FAILED (exit $status) — NOT committing" >&2
  echo "gated-commit: gate failed (see its output above); nothing was committed and your" >&2
  echo "  working tree still holds your edits. Fix what the gate reported, then re-run." >&2
  exit 1
fi

git add -A || exit 1
if [ -n "$message_file" ]; then
  git commit -q -F "$message_file" || exit 1
else
  git commit -q -m "$message" || exit 1
fi

echo "gated-commit: gate passed, committed $(git log --oneline -1)"

# NOTE ON 0111781: that commit's message describes only the acceptance-turn script, but it also
# carries this file's crate-derivation change. `git add -A` swept both, and the message was
# written for one of them. Recorded rather than rewritten because the commit is pushed, and an
# inaccurate message that is findable beats a force-push that makes it un-findable.
