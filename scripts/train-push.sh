#!/usr/bin/env bash
# Push a train to its own branch, let CI be the gate, and advance main only on
# green. Operator tooling runs through the real GitHub CLI (gh) via watch-ci.sh;
# the shim is only for AI agent commands.
#
# Lifting this into another repository: carry four files together —
# scripts/train-push.sh, scripts/watch-ci.sh, scripts/lib/operator-gh.sh,
# scripts/lib/workflow-gates.py — plus `python3`, `git`, and the real `gh` on
# PATH. Nothing else here assumes this repository's layout: the default branch
# is read from origin/HEAD, and repo-local preflights run only when their
# scripts exist (see "Repo-local preflights" below), with the header line
# naming which ones ran.
#
# Why this is the default push path instead of scripts/gated-push.sh:
# the local full Rust gate takes ~12 minutes on this box and only sees macOS.
# Measured over the last week, about half the red trains failed on Linux or
# Windows only, so on those the local gate was paid for and could not have
# caught the failure. It also runs in the one working tree, so trains serialize
# behind each other and compete with worker sessions for this box's CPU (hence
# the box gate, the nice steering, and peers asking for quiet windows). CI runs
# the three platforms in parallel on other hardware in ~11-16 minutes.
#
# The contract we keep is "main is never red". That is a property of what lands
# on main, not of where the tests ran: nothing lands here except a sha whose CI
# run concluded success, and it lands by fast-forward only - never a merge,
# never a force. gated-push.sh stays for the work whose failures only reproduce
# locally (watcher/fseventsd, macOS exec assessment).
#
# THIS FILE RUNS FROM A PRIVATE COPY OF ITSELF (see the exec at the top of the body), so
# editing it while a train is watching CI cannot kill that train. The hazard it closes: bash
# reads a script lazily, by byte offset, as it executes, so an in-place rewrite of the file a
# live run is reading makes that run resume at the wrong offset and die on a syntax error in
# code it never reached. Cost it twice here: an edit to a refusal message, made while a train
# watched CI, killed the train at "line 789: syntax error near unexpected token `('" in a
# file that passes `bash -n` before and after. Nothing was damaged either time -- master did
# not move, because the corrupted run died before the landing push -- but the failure LOOKS
# like a broken script, and the next person goes debugging a file that is fine.
#
# The mechanism is the inode: a running bash holds an open fd and reads on, so what hurts it
# is a write that KEEPS THE INODE (`pathlib.write_text`, `>` redirection, `sed -i` on some
# platforms, most editors' fast-save path). Measured both ways on this box:
#
#   in-place write   inode 1364352744 -> 1364352744   the LIVE run picked up the new bytes
#   temp + rename    inode 1364355209 -> 1364355226   the LIVE run finished on the old ones
#
# Rename-into-place (write a temp, then `mv`) is still the right way to edit a live script
# generally; the private copy is what makes THIS one safe without anyone remembering. Lifted
# from cortexkit/aft origin/main (8b8a7f0fa) into this diverged copy.
#
# I hit this a SECOND time within the hour, editing this very file while its own
# train watched CI -- so the note above, which I had just written, is evidently not enough
# on its own. That is the argument for stating the mechanism: "do not edit" is a rule you
# must recall while busy; "rename into place" is a habit that holds whether you recall it
# or not.
#
# A MERGE IS ITSELF A TRAIN PUSH. Under required status checks, a merge commit
# made locally has no check of its own - main's protection sees an unchecked
# sha and refuses the push, however green both sides were separately. So do the
# merge on the train branch, let CI run on the merge sha, and fast-forward main
# to that same sha. A merge commit whose first parent is origin/main is already
# a descendant of it, so it fast-forwards like any other train.
#
# Usage:
#   scripts/train-push.sh <train-name>
#   scripts/train-push.sh <train-name> -- <local smoke command...>
#
# The optional smoke is the targeted slice the diff touches (the one or two
# suites you would rerun by hand), NOT the full gate. It exists to catch an
# obvious break before spending a CI run.
# Set TRAIN_PUSH_LINUX_PREFLIGHT=1 to repeat the portable CI lane before pushing.
# It is opt-in because CI runs the same tests and deployment need not wait for either.
#
# A red train leaves origin/train/<train-name> in place: fix, commit, and run
# this script again with the same name to update the branch and re-run CI.
#
# If main moves while CI runs, the train is re-queued automatically: rebase onto
# the new origin/main, re-push the branch, watch a fresh run - up to 3 rounds.
# The re-queue is for a moved branch ONLY. A red run is never retried: the same
# tree on a new base is red for the same reason, and version/lock skew in
# particular ends in a lockfile bump commit rather than in any retry.
#
# THE INVARIANT THE RE-QUEUE KEEPS: only the sha a check actually ran against
# may fast-forward main. A green run for the pre-rebase sha proves something
# about a base that no longer exists, so it never authorizes the landing of the
# rebased commit; the rebased sha gets pushed and waits for its own run. That is
# also why a moved main cannot be resolved by pushing the old sha harder.
#
# RELEASES ARE TRAINS TOO. Under required checks a release tag must point at a
# sha that is already on the default branch by fast-forward and carries the
# green check, so the sequence is: train, fast-forward, tag the green sha. The
# local full gate drops out of the release scripts along with everything else -
# tagging a locally-gated sha that never went through a train produces a tag
# whose commit no check ever saw.
#
# WHERE BRANCH PROTECTION IS UNAVAILABLE (a private repo on a Free plan has no
# required status checks), this script's fast-forward-only-on-green IS the gate.
# Nothing on the server will stop a direct push then, so the discipline of
# pushing through here is the whole of the contract.
#
# Exit codes:
#   0  landed on the default branch
#   1  CI red (or a push that reported success without moving origin)
#   2  precondition refusal, bad usage, failed smoke, or no CI run resolved
#   3  the default branch kept moving through 3 re-queue rounds, or the rebase
#      conflicted
set -euo pipefail

# Run from a private copy of this file (the header explains the hazard). The instance
# remembers the real path for its own diagnostics and for locating sibling scripts.
if [ -z "${TRAIN_PUSH_EXEC_COPY:-}" ]; then
  train_push_copy="$(mktemp "${TMPDIR:-/tmp}/train-push.XXXXXX")"
  cp "${BASH_SOURCE[0]}" "$train_push_copy"
  TRAIN_PUSH_EXEC_COPY="$train_push_copy" TRAIN_PUSH_SOURCE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")" \
    exec bash "$train_push_copy" "$@"
fi
watch_log=""
push_log=""
train_lock_held=""
# Install cleanup before acquisition: even a failed first remote lookup must release
# our lock. An owner check keeps a refused contender from deleting another train's lock.
trap 'rm -f "$watch_log" "$push_log" "$TRAIN_PUSH_EXEC_COPY"; if [ -n "$train_lock_held" ] && [ "$(awk "NR==1{print \$1}" "$train_lock_held/owner" 2>/dev/null || true)" = "$$" ]; then rm -rf "$train_lock_held"; fi' EXIT

script_dir="$(dirname "${TRAIN_PUSH_SOURCE:-${BASH_SOURCE[0]}}")"
repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

remote="origin"
# Resolved from the remote below, never assumed: this repo's default branch is
# main today, but a script that hardcodes it silently lands trains on a branch
# that is not the one protection and CI are configured for.
default_branch=""
# The workflow whose run gates a landing, shared with watch-ci.sh so the probe
# and the watch cannot disagree about which run counts. WATCH_CI_WORKFLOW is
# the one override for both scripts (a lift with `ci.yml` sets it once); the
# step-0 scan below reads the same variable, so the file it checks and the run
# it waits for cannot name two different workflows.
# PROVENANCE, AND IT IS NOT CLEAN: these four files were re-lifted from aft 0db2524cb105,
# which is NOT PUBLISHED. It sits in the sibling's local working copy, six commits ahead of
# origin/main (92c2c921d616). I ran `git log --format=%H -1` in that checkout and called the
# result "AFT's current main" — the command was correct and answered a different question, since
# HEAD in a live working copy is the author's desk, not the published record.
#
# Kept rather than reverted: the unpublished delta includes the rev-parse resolver fix for a
# short-sha defect three seats reproduced, verified live here (a short sha resolved run
# 34030265074 where the published version polls ten minutes and reports "no run appeared").
# Reverting would reinstate a real defect to restore a provenance claim.
#
# THE RESIDUAL RISK: if AFT amends or drops any of those six commits, this repo carries code
# upstream never shipped. Re-lift from `git rev-parse origin/main` once they publish.
#
# WHEN LIFTING FROM A SIBLING CHECKOUT ON THIS BOX, THE SHA IS `origin/<branch>`, NEVER `HEAD`.

# LIFT MODIFICATION (broca) — this repo's workflow file is ci.yml, not tests.yml.
#
# THE PAIR IS THE UNIT: watch-ci.sh has the same default and both must move together. Changing
# only this one made the first train prove its trigger on ci.yml, push its branch, then die on
# `HTTP 404: workflow tests.yml not found` — a pushed branch, no landing, manual cleanup. The
# upstream comment says WATCH_CI_WORKFLOW is deliberately ONE variable read by both scripts so
# the probe and the watch cannot disagree; I read that and broke it in the same edit.
#
# PREFER THE EXPORT to editing either default. Changing one of a coordinated pair is worse than
# changing neither: neither gives you upstream's loud refusal on the first run.
tests_workflow_name="${WATCH_CI_WORKFLOW:-ci.yml}"
# The repository the runs live in: from origin unless REPO is given, the same
# derivation watch-ci.sh uses, so the probe and the watch query one repository.
# A fixed default here was kept by the first lift and watched this repository's
# runs for another repository's trains.
repo_from_origin() {
  local url
  url="$(git config --get "remote.$remote.url" 2>/dev/null)" || return 1
  case "$url" in
    git@github.com:*) url="${url#git@github.com:}" ;;
    https://github.com/*) url="${url#https://github.com/}" ;;
    ssh://git@github.com/*) url="${url#ssh://git@github.com/}" ;;
    *) return 1 ;;
  esac
  printf '%s\n' "${url%.git}"
}
repo_slug="${REPO:-}"
# How long the first-run probe waits for a run to start. Overridable for the
# same reason watch-ci.sh's resolver knobs are: tests cannot wait out the
# real budget.
probe_attempts="${TRAIN_PUSH_PROBE_ATTEMPTS:-12}"
probe_sleep="${TRAIN_PUSH_PROBE_SLEEP:-10}"
# Failing job names that mean the red is dependency skew rather than a broken
# change: in this repo those are the Cargo.lock and manifest checks. Extend it
# when a repo names such a job something else.
skew_pattern="${TRAIN_PUSH_SKEW_PATTERN:-}"
if [ -z "$skew_pattern" ]; then
  skew_pattern='lock|version|pin|sibling'
fi

say() { printf 'train-push: %s\n' "$1"; }
refuse() {
  printf 'train-push: refusing — %s\n' "$1" >&2
  exit 2
}

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------
if [ "$#" -eq 0 ]; then
  refuse "no train name given (usage: scripts/train-push.sh <train-name> [-- <smoke command>])"
fi

# `--status` ANSWERS THE LOCK QUESTION WITHOUT STARTING A TRAIN.
#
# Without it the only way to ask "is a train in flight" is to RUN one, and running
# one pushes a remote ref and starts a CI job. I did exactly that tonight while
# checking whether a lock left by a timed-out run was stale: burned a CI cycle on a
# sha already on master and had to delete the ref I had just created. That is the
# same shape as the accidental placement on the other side of this seam an hour
# earlier -- a tool whose side effect you know perfectly well, run while you are
# attending to something else.
#
# Inverting the default is wrong HERE, unlike for a placement tool: this is the
# workflow command and making every real train pass a flag adds a step to the safe
# path. The missing thing was a way to ask, so this adds one and nothing else.
# A PID IS A HANDLE, NOT AN IDENTITY. The kernel guarantees uniqueness only among LIVE
# processes; once a pid exits its number is returned to the pool and lands on whatever starts
# next. So `kill -0 <pid>` answers "some process has this number", which is not the question.
#
# Found by CALLO in the copy AFT lifted from this file: their lock named a finished train and
# `kill -0` kept answering yes because the kernel had handed the number to Chrome an hour and a
# half later. Permanent wedge, and it gets MORE likely the longer a repo sits quiet -- the
# opposite of a race, which is why six seats ran this for two weeks without meeting it.
#
# The pair (pid, start-time) CANNOT recur: a recycled pid necessarily has a later start. That
# turns the handle into an identity without a heartbeat, a background writer, or anything the
# process has to keep asserting -- which matters because the lock survives SIGKILL, and a
# process that has been killed cannot maintain a liveness token.
#
# Unavailable start-time is treated as LIVE (fail closed), for the same reason an unreadable
# owner is: dispossessing a running train gives two live trains, which is what this guard exists
# to prevent, and the cost of the other direction is one manual rm the message names.
proc_start_time() {
  ps -o lstart= -p "$1" 2>/dev/null | sed 's/^ *//;s/ *$//'
}

# Returns 0 when the recorded owner is THIS pid at THIS start time. A bare pid match is not
# enough and a bare liveness check is not enough; both halves or it is stale.
owner_is_live() {
  _pid="$1"; _start="$2"
  [ -n "$_pid" ] || return 1
  kill -0 "$_pid" 2>/dev/null || return 1
  # No recorded start time: an owner file from before this field existed. Treat as live, since
  # the alternative is dispossessing a train that may be running.
  [ -n "$_start" ] || return 0
  _now="$(proc_start_time "$_pid")"
  # The process is alive but we cannot read its start time -- fail closed, same rule.
  [ -n "$_now" ] || return 0
  [ "$_now" = "$_start" ]
}

if [ "$1" = "--status" ]; then
  # READ THE FILE THE LOCK ACTUALLY WRITES. My first version read `held/pid`, a
  # name nothing creates, so the "live lock" arm passed against a fixture I had
  # built rather than against the mechanism -- the verifier checking what the test
  # wrote. The real claim is `held/owner` carrying "<pid> <train-name>".
  lock_dir="$(git rev-parse --path-format=absolute --git-common-dir 2>/dev/null)/train-push-locks"
  held="$lock_dir/held"
  # `|| true` because this runs under `set -e` and awk exits non-zero on a missing
  # file -- which is the COMMON case here, no lock at all. Without it the clean arm
  # died silently at exit 2, so the one reading an operator most wants ("is anything
  # running?") was the one that did not work.
  owner_pid="$(awk 'NR==1{print $1}' "$held/owner" 2>/dev/null || true)"
  owner_name="$(awk 'NR==1{print $2}' "$held/owner" 2>/dev/null || true)"
  owner_start="$(awk 'NR==2{print}' "$held/owner" 2>/dev/null || true)"
  if owner_is_live "$owner_pid" "$owner_start"; then
    printf 'train-push: a train IS in flight (pid %s, %s)\n' "$owner_pid" "${owner_name:-unnamed}"
  elif [ -d "$held" ]; then
    printf 'train-push: a STALE lock is present; the next run claims it automatically\n'
  else
    printf 'train-push: no train in flight\n'
  fi
  refs=$(git ls-remote --heads origin 'train/*' 2>/dev/null | sed 's|.*refs/heads/||')
  if [ -n "$refs" ]; then
    printf 'train-push: leftover refs (spent ones are safe to delete):\n'
    printf '%s\n' "$refs" | sed 's/^/  /'
  fi
  exit 0
fi

train_name="$1"
shift
case "$train_name" in
  -*) refuse "train name must not look like a flag (got '$train_name')" ;;
esac

smoke_given=0
smoke_cmd=()
if [ "$#" -gt 0 ]; then
  if [ "$1" != "--" ]; then
    refuse "unexpected argument '$1' (the smoke command must follow a bare --)"
  fi
  shift
  if [ "$#" -eq 0 ]; then
    refuse "-- given without a smoke command"
  fi
  smoke_given=1
  smoke_cmd=("$@")
fi

train_ref="train/$train_name"
# Validate before the name reaches a refspec: a name with a space, a leading
# dot, or '..' in it produces a confusing git error deep in the push instead of
# a named refusal here.
if ! git check-ref-format "refs/heads/$train_ref"; then
  refuse "'$train_name' is not a usable branch name component"
fi

# REFUSE A CONCURRENT TRAIN. Two trains overlap the moment one is backgrounded and another
# started -- which is exactly what an operator wants to do, because a train takes minutes and
# waiting is dull. They are not concurrent-safe: each resolves HEAD and moves refs on one
# remote, so the second can land the first's commits, or find the branch already where it meant
# to put it and report a failure over a landing that succeeded. That false failure is the mild
# outcome; the dangerous one is a train landing a sha its CI never tested.
#
# THIS USED TO READ THE REMOTE REFS, WHICH IS A PROXY FOR A RUNNING TRAIN AND IS WRONG IN BOTH
# DIRECTIONS. A ref OUTLIVES its process: a red CI or a watch that gave up leaves the ref as the
# repush target, and refusing there blocks the ordinary retry (AFT re-runs one train name six
# rounds in a day). And a ref ARRIVES LATE: a train that has resolved HEAD but not yet pushed
# holds no ref at all, so a second train started in that window saw nothing and was permitted --
# the exact concurrency this guard exists to refuse.
#
# A PID IS THE DIRECT OBSERVABLE. The lock is a record, not a mutex: it means nothing unless its
# process is alive, so a train killed with SIGKILL (where no trap runs) leaves a file that the
# next scan clears. Liveness is the truth and the file is only how we find the pid.
#
# LIMIT, stated because it bounds the claim: the lock is LOCAL and the refs are REMOTE, so this
# sees trains on this machine only. Single operator, single box here; a second machine driving
# the same remote is invisible to it, which is why leftover refs are still LISTED below.
#
# The lock lives in the COMMON git directory, which every linked worktree of this clone shares.
# `--git-dir` names a per-worktree directory, so two worktrees each took their own lock and
# both trains were admitted.
train_lock_dir="$(git rev-parse --path-format=absolute --git-common-dir 2>/dev/null)/train-push-locks"
mkdir -p "$train_lock_dir" 2>/dev/null || true
train_lock_held="$train_lock_dir/held"

# ACQUIRE BEFORE ANY NETWORK WORK, AND ACQUIRE ATOMICALLY. The previous version SCANNED
# for live locks here and WROTE this train's lock thirty lines later, with a `git ls-remote`
# in between. I looked straight at that window when I wrote it and called it "microseconds".
# It is not: it is a NETWORK ROUND TRIP, and the operator behaviour that produces an overlap
# -- background one train, immediately start another -- lands inside it by construction. Two
# trains both scanned, both saw nothing, both proceeded. Found by an audit round pointed at
# exactly this class, in the guard I had just written to fix an instance of it.
#
# `mkdir` is the POSIX atomic primitive: it fails with EEXIST if the directory already
# exists, so of N racing processes exactly one succeeds. There is no check-then-act to lose.
#
# THE OWNER FILE IS WRITTEN AFTER THE DIRECTORY EXISTS, which leaves a two-syscall window
# where the lock is held by someone who has not yet said who they are. An unreadable owner
# is therefore treated as LIVE and refused, never recovered: dispossessing a process that is
# mid-acquisition gives two live trains, which is the thing this guard exists to prevent,
# while refusing costs one manual `rm -rf` that the message names. Fail closed.
# Read this process's start time ONCE, outside the loop and outside the critical section.
my_start="$(proc_start_time "$$")"
train_lock_attempts=0
while true; do
  if mkdir "$train_lock_held" 2>/dev/null; then
    # Line 1 is "<pid> <name>", unchanged, so an older reader still parses it. Line 2 is the
    # start time; an absent line 2 reads as a pre-field owner and is treated as live.
    #
    # `$my_start` IS COMPUTED BEFORE THE mkdir, NOT HERE. Calling `ps` inside the critical
    # section turns a two-syscall window into a fork+exec one, and the other racer meets an
    # owner file that does not exist yet -- it still refuses (fail closed, correct) but with
    # "unidentified owner" instead of naming the train, which is a worse message for the
    # commonest race. Caught by the concurrency arm, which asserts the message.
    printf '%s %s\n%s\n' "$$" "$train_name" "$my_start" > "$train_lock_held/owner"
    break
  fi

  owner_pid="$(awk 'NR==1{print $1}' "$train_lock_held/owner" 2>/dev/null || true)"
  owner_name="$(awk 'NR==1{print $2}' "$train_lock_held/owner" 2>/dev/null || true)"
  owner_start="$(awk 'NR==2{print}' "$train_lock_held/owner" 2>/dev/null || true)"

  if [ -z "$owner_pid" ]; then
    printf 'train-push: the train lock is held with no readable owner.\n' >&2
    printf '  a train was killed between taking the lock and recording its pid.\n' >&2
    printf '  if you are sure no train is running:  rm -rf %s\n' "$train_lock_held" >&2
    refuse "train lock held by an unidentified owner; refusing rather than dispossessing it"
  fi

  if owner_is_live "$owner_pid" "$owner_start"; then
    printf 'train-push: train %s is RUNNING as pid %s\n' "${owner_name:-?}" "$owner_pid" >&2
    printf '  wait for it, or kill it if you know it is wedged.\n' >&2
    refuse "a train is already running on this machine; wait for it"
  fi

  # STALE OWNER. Claim the removal with a rename, not an `rm`: two processes that both find
  # the same stale owner would otherwise both delete and both acquire, and the second delete
  # would take the FIRST one's fresh lock with it. `mv` of a directory to an unused name is
  # rename(2), which fails with ENOENT for whoever arrives second, so exactly one recovers.
  # The loser falls through to the next iteration and meets the winner's live lock.
  if mv "$train_lock_held" "$train_lock_held.stale.$$" 2>/dev/null; then
    rm -rf "$train_lock_held.stale.$$" 2>/dev/null || true
  fi

  train_lock_attempts=$((train_lock_attempts + 1))
  if [ "$train_lock_attempts" -ge 3 ]; then
    refuse "could not acquire the train lock after clearing a stale owner; try again"
  fi
done

# Leftover refs no longer REFUSE -- no process is driving them, so they cannot race this
# train. They are still reported, with the delete composed, because the operator reading this
# is usually mid-CI-failure and composing `--delete train/<name>` by hand next to several
# refs is where the wrong one gets deleted.
stale_refs=$(git ls-remote --heads "${remote:-origin}" 'train/*' 2>/dev/null | wc -l | tr -d ' ')
if [ "${stale_refs:-0}" -gt 0 ]; then
  printf 'train-push: %s train ref(s) on %s with no live train here:\n' \
    "$stale_refs" "${remote:-origin}" >&2
  git ls-remote --heads "${remote:-origin}" 'train/*' 2>/dev/null |
    sed 's|.*refs/heads/|    git push origin --delete |' >&2
  printf '  (proceeding: a ref without a process cannot race this train)\n' >&2
fi

# Operator tooling runs on the upstream gh; the shim is only for agent commands.
# Sourced up front because the trigger probe, the default-branch fallback, and
# watch-ci.sh all need it - failing here beats failing after a CI run.
# shellcheck source=lib/operator-gh.sh
source "$script_dir/lib/operator-gh.sh" || exit 2

if [ -z "$repo_slug" ]; then
  repo_slug="$(repo_from_origin)" ||
    refuse "cannot derive the repository from $remote's URL; set REPO=owner/name"
fi
# watch-ci.sh derives the same value the same way; exporting it pins the watch
# to the repository this script proved the trigger on.
export REPO="$repo_slug"

# The historical repository must never publish its commit graph to the new public
# repository. A missing root object is expected in fresh public clones; when the
# root is present, merge-base errors other than "not an ancestor" are refusals.
push_private_history_guard() {
  local commit="$1"
  local private_root="${TRAIN_PUSH_PRIVATE_ROOT:-47b17104d2688c9d5914bea4e7b52ead894d07b9}"
  local merge_status

  if ! git cat-file -e "${private_root}^{commit}" 2>/dev/null; then
    return 0
  fi

  if git merge-base --is-ancestor "$private_root" "$commit"; then
    if [ "$repo_slug" != "cortexkit/broca-private" ]; then
      refuse "this commit carries the private history and may only go to cortexkit/broca-private"
    fi
  else
    merge_status=$?
    if [ "$merge_status" -ne 1 ]; then
      refuse "could not determine whether this commit carries the private history (git merge-base exited $merge_status)"
    fi
  fi
}

head_sha="$(git rev-parse HEAD)"
push_private_history_guard "$head_sha"

# ---------------------------------------------------------------------------
# Preconditions
# ---------------------------------------------------------------------------
# Step 0 reads the workflow files, because everything after it assumes a train
# push produces a run whose checks mean something.
#
# It is checked first because getting the order wrong is unrecoverable from the
# operator's side: turn on required status checks while the workflow still
# triggers on the default branch only, and every train pushes a branch that
# produces no run, so no check ever arrives and the branch becomes unpushable.
#
# The reading is done by scripts/lib/workflow-gates.py rather than by matching
# lines here: `if:` conditions are routinely written as folded scalars, and a
# line-oriented search misses them on exactly the workflows whose behaviour
# depends on the ref. The path derives from the same variable the probe and
# the watch use: two spellings of the workflow name in one script disagreed on
# every lift whose gate is not called tests.yml.
tests_workflow=".github/workflows/$tests_workflow_name"
gate_scanner="$script_dir/lib/workflow-gates.py"
# Absolute, so the pre-push warning below names a path the reader can act on
# from anywhere rather than one relative to the repo root.
git_dir="$(git rev-parse --absolute-git-dir)"

command -v python3 >/dev/null 2>&1 ||
  refuse "python3 is required to read the workflow files"
if [ ! -f "$tests_workflow" ]; then
  # `ls` on an unmatched glob exits nonzero, which under `set -e` would end the
  # script inside this substitution before the refusal is printed.
  present="$({ find .github/workflows -maxdepth 1 \( -name '*.yml' -o -name '*.yaml' \) 2>/dev/null || true; } | sort | tr '\n' ' ')"
  refuse "no $tests_workflow — set WATCH_CI_WORKFLOW=<file> to the workflow that gates a landing (present: ${present:-none}), and add \`train/**\` to on.push.branches in that file"
fi

set +e
gate_report="$(python3 "$gate_scanner" --train-ref "$train_ref" \
  --tests-workflow "$tests_workflow" \
  .github/workflows/*.yml .github/workflows/*.yaml 2>&1)"
scanner_rc=$?
set -e
if [ "$scanner_rc" -ne 0 ]; then
  printf '%s\n' "$gate_report" >&2
  refuse "could not read the workflow files (workflow-gates.py exit $scanner_rc)"
fi

if [ "$(printf '%s\n' "$gate_report" | grep -cx 'trigger|ok')" -eq 0 ]; then
  refuse "$tests_workflow_name does not run on $train_ref — add \`train/**\` to on.push.branches in .github/workflows/tests.yml"
fi

# Conditions that decide on something the landing path never satisfies. A gate
# that never evaluates true is not a check that passed, it is a check that never
# ran - and in a run summary the two are indistinguishable.
#
#   ref conditions   - a job gated on the default branch's ref is skipped on the
#                      train push (the ref is refs/heads/train/...), and the
#                      train run is the one whose checks protection consults
#                      when the fast-forward asks to land that sha. The gate
#                      therefore never guards anything. This refuses.
#   event conditions - whether `github.event_name == 'pull_request'` ever fires
#                      depends on how this repo lands changes, which cannot be
#                      read off the YAML. This warns.
#   tag-only refs    - a condition that only names refs/tags/ belongs to the
#                      release event rather than the landing path, and widening
#                      it to trains would be wrong. Warned, never refused.
#   concurrency      - a group that is not per-ref means pushing a train cancels
#                      an in-flight run on another ref. Warned.
#
# The judgement is "this condition against this trigger list", so findings are
# printed under a header naming the file, the events that start it and its
# concurrency group.
event_gated="$(printf '%s\n' "$gate_report" | sed -n 's/^warn|//p')"
ref_gated="$(printf '%s\n' "$gate_report" | sed -n 's/^ref|//p')"

if [ -n "$event_gated" ]; then
  {
    printf 'train-push: WARNING — conditions that may never fire on the landing path:\n'
    printf '%s\n' "$event_gated" | sed 's/^/  /'
    printf '  judge this against how the repo lands; the scanner cannot know the landing path\n'
  } >&2
fi

if [ -n "$ref_gated" ]; then
  {
    printf 'train-push: refusing — ref-gated conditions that no train can satisfy:\n'
    printf '%s\n' "$ref_gated" | sed 's/^/  /'
    printf '  fix: gate on github.event_name or paths, or widen the ref condition to include refs/heads/train/\n'
  } >&2
  exit 2
fi

# Last part of step 0: a repo-local pre-push hook that runs the full gate turns
# the red-train loop into something you cannot use - every fix-and-repush pays
# the gate again, and even deleting the branch after a land runs the suite. We
# only report it: the hook may be doing something else entirely, and running it
# here to find out would be the very cost being warned about.
warn_repo_local_pre_push() {
  local configured
  local repo_local="$git_dir/hooks/pre-push"
  local hook
  local -a candidates

  candidates=("$repo_local")
  configured="$(git config --get core.hooksPath 2>/dev/null || true)"
  if [ -n "$configured" ]; then
    case "$configured" in
      # AFT's managed dispatcher is not a gate: it chains to the repo-local
      # hook, which is already a candidate above. Its presence alone says
      # nothing about whether a gate runs. Matched by path shape rather than an
      # absolute location because the data directory moves with XDG settings.
      */cortexkit/aft/git-hooks | */cortexkit/aft/git-hooks/*) : ;;
      *)
        if [ "$configured/pre-push" != "$repo_local" ]; then
          candidates+=("$configured/pre-push")
        fi
        ;;
    esac
  fi

  for hook in "${candidates[@]}"; do
    [ -f "$hook" ] && [ -x "$hook" ] || continue
    {
      printf 'train-push: WARNING — repo-local pre-push hook: %s\n' "$hook"
      printf '  verify it does not refuse pushes to train/** or ref deletions\n'
    } >&2
  done
}

warn_repo_local_pre_push

# Refuse a tree that is mid-merge/cherry-pick/rebase for the same reason
# gated-push.sh does: a conflicted tree can carry stale HEAD state, and here it
# would also push a sha that is not the change under test.
#
# A rebase in progress is the DIRECTORY (rebase-merge/ or rebase-apply/), which
# git creates on start and removes on finish or abort. REBASE_HEAD is a
# convenience ref that git does not always remove when a rebase completes, so
# on its own it is a fossil, not an operation: a checkout that finished a
# rebase weeks ago would otherwise refuse every train until someone deleted
# the file by hand. MERGE_HEAD and CHERRY_PICK_HEAD are cleaned up by git and
# stay authoritative.
for marker in CHERRY_PICK_HEAD MERGE_HEAD; do
  if [ -e "$git_dir/$marker" ]; then
    refuse "$marker present (unresolved git operation)"
  fi
done
for rebase_dir in rebase-merge rebase-apply; do
  if [ -d "$git_dir/$rebase_dir" ]; then
    refuse "$rebase_dir/ present (rebase in progress)"
  fi
done
if [ -e "$git_dir/REBASE_HEAD" ]; then
  say "note: REBASE_HEAD present with no rebase in progress (a finished rebase's leftover ref; ignored)"
fi

# CI tests the pushed commit, not the working tree. Uncommitted work would be
# invisible to the gate and then silently absent from what lands on main.
if [ -n "$(git status --porcelain)" ]; then
  refuse "working tree is not clean (commit or stash before pushing a train)"
fi

git fetch -q --prune "$remote" || refuse "git fetch $remote failed"

# Read the default branch off the remote. Every later message, the moved-branch
# check and the fast-forward target all come from this one answer, so a repo
# whose default is not called main is landed on correctly instead of silently
# having a 'main' created for it.
resolve_default_branch() {
  local ref
  local name

  ref="$(git symbolic-ref -q "refs/remotes/$remote/HEAD" 2>/dev/null || true)"
  name="${ref#refs/remotes/"$remote"/}"
  if [ -n "$ref" ] && [ -n "$name" ] && [ "$name" != "$ref" ]; then
    printf '%s\n' "$name"
    return 0
  fi

  # A remote wired up by hand has no origin/HEAD to read; ask the forge rather
  # than guessing a name.
  name="$("$OPERATOR_GH" repo view "$repo_slug" --json defaultBranchRef --jq '.defaultBranchRef.name' 2>/dev/null || true)"
  [ -n "$name" ] || return 1
  printf '%s\n' "$name"
}

default_branch="$(resolve_default_branch || true)"
if [ -z "$default_branch" ]; then
  refuse "could not determine $remote's default branch (run: git remote set-head $remote -a)"
fi

remote_default="refs/remotes/$remote/$default_branch"
if ! git rev-parse --verify -q "$remote_default" >/dev/null; then
  refuse "no $remote/$default_branch to land on"
fi

# A local main behind origin/main means the train was built on a stale base:
# CI would test it green and the land would still be refused in step 5.
if git rev-parse --verify -q refs/heads/"$default_branch" >/dev/null; then
  if ! git merge-base --is-ancestor "$remote_default" "refs/heads/$default_branch"; then
    refuse "local $default_branch is behind $remote/$default_branch (git merge --ff-only $remote/$default_branch first)"
  fi
fi

head_sha="$(git rev-parse HEAD)"
# HEAD is what lands, so HEAD is what has to fast-forward main. Checked here so
# a doomed train is refused before it costs a CI run, and again after CI.
if ! git merge-base --is-ancestor "$remote_default" "$head_sha"; then
  refuse "HEAD is not a descendant of $remote/$default_branch (rebase onto $remote/$default_branch first)"
fi

# Validate before the trigger probe too: it is the first push in a new clone.
# A moved-main requeue changes HEAD and must earn a new portable preflight.
linux_preflight_sha=""
run_linux_preflight() {
  local candidate dirty
  if [ "${TRAIN_PUSH_LINUX_PREFLIGHT:-0}" != 1 ]; then
    return 0
  fi
  candidate="$(git rev-parse HEAD)" || refuse "could not resolve the preflight SHA"
  dirty="$(git status --porcelain)" || refuse "could not read worktree status before Linux preflight"
  [[ -z "$dirty" ]] || refuse "dirty worktree before Linux preflight"
  [[ "$linux_preflight_sha" != "$candidate" ]] || return 0
  say "portable preflight on remote Linux for $candidate"
  bash scripts/train-preflight.sh || refuse "remote Linux preflight failed; nothing was pushed (see output above)"
  dirty="$(git status --porcelain)" || refuse "could not read worktree status after Linux preflight"
  [[ "$(git rev-parse HEAD)" == "$candidate" && -z "$dirty" ]] || refuse "HEAD/worktree changed during Linux preflight"
  linux_preflight_sha="$candidate"
}
if [ "${TRAIN_PUSH_LINUX_PREFLIGHT:-0}" != 1 ]; then
  say "Linux preflight skipped (CI repeats it); opt in with TRAIN_PUSH_LINUX_PREFLIGHT=1"
fi
run_linux_preflight

# ---------------------------------------------------------------------------
# First-run self-check: prove the trigger instead of believing the YAML
# ---------------------------------------------------------------------------
# The scan above reads intent; this reads the platform. A widened trigger that
# has never been exercised is a claim, so once per repository push a throwaway
# commit to a probe branch and wait for a run to START on it. If the two
# disagree, it is always the platform that is right.
probe_marker="$git_dir/train-push-proven"

run_trigger_probe() {
  local probe_ref="train/trigger-probe"
  local probe_sha
  local rid=""
  local _attempt

  say "first train in this repo — proving $tests_workflow_name starts on a train branch"
  # commit-tree writes the probe commit as a loose object: HEAD, the index and
  # the working tree are untouched by the probe.
  probe_sha="$(git commit-tree "$head_sha^{tree}" -p "$head_sha" -m "train-push trigger probe")" ||
    refuse "could not build the trigger probe commit"
  push_private_history_guard "$probe_sha"
  git push -q --force "$remote" "$probe_sha:refs/heads/$probe_ref" ||
    refuse "could not push the trigger probe to $remote/$probe_ref"

  # ~2 minutes. A run that is going to exist is queued within seconds; waiting
  # longer would only delay the first train in every clone.
  #
  # Resolved by SHA across every workflow rather than filtered by workflow
  # file: the forge lists workflow files from the default branch, so a file the
  # train itself adds or renames is not queryable under its new name until it
  # lands, and a file-filtered query found nothing for a probe whose run had
  # started (the first lift that renamed ci.yml hit exactly this). The run is
  # matched to the gating workflow by its display name, which the file carries
  # in `name:` and which survives a rename; a file without `name:` is shown
  # under its path, so that is the fallback expectation.
  local want_name
  want_name="$(sed -n 's/^name:[[:space:]]*//p' "$tests_workflow" | head -1 | sed 's/^["'"'"']//; s/["'"'"']$//')"
  [ -n "$want_name" ] || want_name="$tests_workflow"
  for _attempt in $(seq 1 "$probe_attempts"); do
    rid="$("$OPERATOR_GH" run list --repo "$repo_slug" \
      --branch "$probe_ref" --limit 20 --json databaseId,headSha,workflowName \
      --jq ".[] | select(.headSha==\"$probe_sha\" and .workflowName==\"$want_name\") | .databaseId" 2>/dev/null | head -1)"
    [ -n "$rid" ] && break
    sleep "$probe_sleep"
  done

  git push -q "$remote" --delete "$probe_ref" ||
    printf 'train-push: warning — could not delete %s/%s\n' "$remote" "$probe_ref" >&2

  if [ -z "$rid" ]; then
    refuse "no $tests_workflow_name run started for the probe on $probe_ref — the platform does not run it on train branches, whatever the workflow file says"
  fi

  printf 'run %s started for probe %s on %s at %s\n' \
    "$rid" "$probe_sha" "$probe_ref" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" > "$probe_marker"
  say "trigger proven by run $rid"
}

if [ ! -f "$probe_marker" ]; then
  run_trigger_probe
fi

# Repo-local preflights. The script is lifted into other repositories as a
# file, so nothing here may assume this repository's layout: each preflight
# runs only when its script exists, and the header line names which ones ran
# so a repository with none is distinguishable from one whose block was
# deleted. This repository's two are the governed-docs gates gated-push.sh
# runs; both are read-only here (scripts/align-governed-docs.sh is the
# writing half and stays the remedy, not something this script performs).
preflights_ran=()
if [ -f scripts/audit-v049-agent-surface.ts ]; then
  bun scripts/audit-v049-agent-surface.ts \
    || refuse "governed-surface audit failed — run scripts/align-governed-docs.sh"
  preflights_ran+=(governed-surface-audit)
fi
if [ -f scripts/release-gate-v049.mjs ]; then
  node scripts/release-gate-v049.mjs \
    || refuse "release gate failed — run scripts/align-governed-docs.sh"
  preflights_ran+=(release-gate)
fi
# A repository may add its own preflights beside this script without editing
# it; the hook is sourced so it can call `refuse` and `say`.
if [ -f scripts/train-push.local.sh ]; then
  # shellcheck disable=SC1091
  . scripts/train-push.local.sh
  preflights_ran+=(train-push.local.sh)
fi
if [ "${#preflights_ran[@]}" -eq 0 ]; then
  say "preflights: none (no repo-local preflight scripts present)"
else
  say "preflights: ${preflights_ran[*]}"
fi

# ---------------------------------------------------------------------------
# Optional local smoke
# ---------------------------------------------------------------------------
if [ "$smoke_given" -eq 1 ]; then
  say "local smoke: ${smoke_cmd[*]}"
  set +e
  if [ "${#smoke_cmd[@]}" -eq 1 ]; then
    # A single argument is a shell string, which may be a pipeline. Without
    # pipefail a failing first stage is hidden behind a successful last stage,
    # so the smoke would report green over a failed command.
    bash -c "set -o pipefail
${smoke_cmd[0]}"
  else
    # Argv form runs bare: this script adds no pipe of its own, so the
    # command's own status is the status we read.
    "${smoke_cmd[@]}"
  fi
  smoke_rc=$?
  set -e
  if [ "$smoke_rc" -ne 0 ]; then
    refuse "local smoke failed (rc=$smoke_rc): ${smoke_cmd[*]}"
  fi
  say "local smoke ok"
fi

# ---------------------------------------------------------------------------
# Push the train, let CI gate it, land on green - re-queue if main moved
# ---------------------------------------------------------------------------
# main can move during the ~11-16 minutes CI takes, and a sha tested on the old
# base says nothing about the rebased result. A moved main is therefore a new
# round rather than a hand-back: rebase onto the new main, re-push the branch,
# watch a fresh run. Bounded at 3 because retrying forever on a busy main is
# unbounded CI spend, and by the third loss the honest answer is that this train
# needs a quiet window.
max_rounds=3
round=1
# The sha a check actually ran green against. Only this sha may land.
verified_sha=""

watch_log="$(mktemp "${TMPDIR:-/tmp}/train-push-watch.XXXXXX")"
push_log="$(mktemp "${TMPDIR:-/tmp}/train-push-push.XXXXXX")"

# What we believe $remote/$train_ref points at. Tracked explicitly so every
# re-push leases against the sha WE pushed instead of trusting a remote-tracking
# ref to have been refreshed along the way.
train_remote_sha=""
if git rev-parse --verify -q "refs/remotes/$remote/$train_ref" >/dev/null; then
  train_remote_sha="$(git rev-parse "refs/remotes/$remote/$train_ref")"
fi

push_train() {
  run_linux_preflight
  push_private_history_guard "$head_sha"
  # Taken BEFORE the push, so the run it starts is created after this instant by
  # construction. The watch refuses any older run for the same sha -- see WATCH_CI_NOT_BEFORE
  # in watch-ci.sh for the re-push that adopted a stale red.
  pushed_at="$(date -u +%s)"
  # A PUSH OF THE SHA THE REMOTE ALREADY HOLDS STARTS NO RUN. Refuse now rather than let
  # the watch wait out its whole resolve budget for a run that will never exist -- and,
  # before the bound above existed, adopt the previous run's verdict instead, which is how
  # a stale red was reported as current.
  if [ -n "$train_remote_sha" ] && [ "$train_remote_sha" = "$head_sha" ]; then
    refuse "$remote/$train_ref already holds $head_sha, so pushing it starts no CI run. To re-run CI on this sha, delete the ref first: git push $remote --delete $train_ref"
  fi
  if [ -z "$train_remote_sha" ]; then
    # Create with a plain push: a mistyped train name must not be able to
    # clobber a branch that already exists.
    say "creating $remote/$train_ref -> $head_sha"
    git push "$remote" "$head_sha:refs/heads/$train_ref" ||
      refuse "could not create $remote/$train_ref"
  else
    say "updating $remote/$train_ref -> $head_sha"
    git push --force-with-lease="refs/heads/$train_ref:$train_remote_sha" \
      "$remote" "$head_sha:refs/heads/$train_ref" ||
      refuse "could not update $remote/$train_ref (it moved since we pushed it — check who else is running this train)"
  fi
  train_remote_sha="$head_sha"
}

# Rebase the checked-out train onto the new main. A conflict ends the run: the
# resolution is a human judgement about two changes, and leaving a half-rebased
# tree behind would hand back a repository that cannot be used until someone
# figures out what state it is in.
rebase_onto_main() {
  local before="$1"
  local conflicted
  local now

  if git rebase "$remote_default"; then
    return 0
  fi

  conflicted="$(git diff --name-only --diff-filter=U 2>/dev/null || true)"
  git rebase --abort >/dev/null 2>&1 || true
  now="$(git rev-parse HEAD)"
  {
    printf 'train-push: rebase onto %s/%s conflicted — stopping.\n' "$remote" "$default_branch"
    if [ -n "$conflicted" ]; then
      printf '  conflicting file(s):\n'
      printf '%s\n' "$conflicted" | sed 's/^/    /'
    fi
    if [ "$now" = "$before" ]; then
      printf '  The rebase was aborted; the tree is back at %s and %s/%s still holds it.\n' \
        "$before" "$remote" "$train_ref"
    else
      printf '  WARNING: the abort did not restore HEAD (now %s, was %s) — check the tree before continuing.\n' \
        "$now" "$before"
    fi
    printf '  Resolve by hand, then run: scripts/train-push.sh %s\n' "$train_name"
  } >&2
  return 1
}

while true; do
  push_train

  verified_sha=""
  say "watching CI for $head_sha on $train_ref in $repo_slug (round $round of $max_rounds)"
  set +e
  WATCH_CI_NOT_BEFORE="$pushed_at" "$script_dir/watch-ci.sh" "$head_sha" 2>&1 | tee "$watch_log"
  watch_rc="${PIPESTATUS[0]}"
  set -e

  run_url="$(grep -m1 '^CI_RUN_URL ' "$watch_log" 2>/dev/null | sed 's/^CI_RUN_URL //' || true)"
  [ -n "$run_url" ] || run_url="(run url not reported)"

  if [ "$watch_rc" -ne 0 ]; then
    if [ "$watch_rc" -eq 1 ]; then
      # Report the jobs by name: with three platforms in parallel, "CI failed"
      # is not enough to know whether this needs a local reproduction or a
      # platform-specific fix.
      failing="$(grep -o "job='[^']*'" "$watch_log" 2>/dev/null | sed "s/job='//; s/'$//" | sort -u || true)"
      if [ -n "$failing" ]; then
        printf 'train-push: CI red — failing job(s):\n' >&2
        # One name per line, not word-split: job names contain spaces.
        printf '%s\n' "$failing" | sed 's/^/  /' >&2
      else
        printf 'train-push: CI red — no job name reported (run conclusion was not success)\n' >&2
      fi
      # A red run never enters the re-queue loop below - that loop is for a
      # moved default branch and nothing else. Rebasing a red train carries the
      # same tree onto a new base, so the re-run is red for the same reason and
      # spends one of the bounded rounds proving it. Dependency skew gets named
      # because it looks like a flake from the outside: the commit did not
      # change, CI resolved sibling repositories to different versions, and no
      # amount of retrying moves a lockfile - the fix is a bump commit.
      if [ -n "$failing" ] && [ "$(printf '%s\n' "$failing" | grep -Eci "$skew_pattern")" -gt 0 ]; then
        printf 'red is a version/lock skew, not contention: this terminates in a lockfile bump commit, not a retry\n' >&2
      fi
      printf 'train-push: run %s\n' "$run_url" >&2
      printf 'train-push: %s/%s still holds %s — fix, commit, and re-run: scripts/train-push.sh %s\n' \
        "$remote" "$train_ref" "$head_sha" "$train_name" >&2
      exit 1
    fi
    printf 'train-push: could not watch CI for %s (watch-ci exit %s); %s/%s is pushed and unwatched\n' \
      "$head_sha" "$watch_rc" "$remote" "$train_ref" >&2
    exit 2
  fi

  verified_sha="$head_sha"
  say "CI green: $run_url"

  # Re-check right before the push, not just at the start of the script.
  git fetch -q "$remote" "$default_branch" || refuse "git fetch $remote $default_branch failed"
  if git merge-base --is-ancestor "$remote_default" "$head_sha"; then
    break
  fi

  moved_sha="$(git rev-parse "$remote_default")"
  printf 'train-push: %s/%s moved to %s while CI ran (round %s of %s) — %s was tested on the old base\n' \
    "$remote" "$default_branch" "$moved_sha" "$round" "$max_rounds" "$head_sha" >&2

  if [ "$round" -ge "$max_rounds" ]; then
    {
      printf 'train-push: gave up after %s rounds — not landing.\n' "$max_rounds"
      printf '  %s/%s holds %s, rebased onto every %s this run saw.\n' \
        "$remote" "$train_ref" "$head_sha" "$remote/$default_branch"
      printf '  Re-run when %s is quieter: scripts/train-push.sh %s\n' \
        "$remote/$default_branch" "$train_name"
    } >&2
    exit 3
  fi

  # A MERGE TRAIN MUST NOT BE REBASED AUTOMATICALLY.
  #
  # Plain `git rebase` LINEARIZES: it replays the non-merge commits and drops the merge
  # commit itself. Any change recorded ONLY in that merge -- a conflict resolution, an
  # integration adjustment -- disappears, the rebase exits 0, and the reduced tree then gets
  # pushed, re-tested and landed. The ancestry guard below does not catch it: it checks that
  # what lands is the sha CI ran against, which is true of the reduced tree too.
  #
  # Reproduced: a merge carrying integration.txt, rebased onto a moved main -> exit 0, both
  # parents' files present, upstream's file present, integration.txt GONE.
  #
  # `--rebase-merges` is NOT the fix. It recreates the topology by RE-PERFORMING the merges,
  # so a manual adjustment recorded in the merge commit is still lost unless rerere happens
  # to hold it. Refusing is the only honest option: the operator knows what that merge
  # resolved and the script does not.
  #
  # This header documents merge trains as supported (see "A MERGE IS ITSELF A TRAIN PUSH"),
  # so this is a permitted input shape, not a misuse.
  merge_list="$(git rev-list --merges "$remote_default..$head_sha" 2>/dev/null || true)"
  merge_commits="$(printf '%s' "$merge_list" | grep -c . || true)"
  if [ "${merge_commits:-0}" -gt 0 ]; then
    {
      printf 'train-push: refusing to rebase a train that contains %s merge commit(s).\n' \
        "$merge_commits"
      printf '  A rebase linearizes them, and any change recorded ONLY in a merge -- a\n'
      printf '  conflict resolution or integration fix -- would be dropped SILENTLY.\n'
      printf '%s\n' "$merge_list" | sed 's/^/    merge /'
      printf '  %s/%s still holds %s; nothing has been rewritten.\n' \
        "$remote" "$train_ref" "$head_sha"
      printf '  Integrate %s/%s yourself, then re-run: scripts/train-push.sh %s\n' \
        "$remote" "$default_branch" "$train_name"
    } >&2
    exit 3
  fi

  if ! rebase_onto_main "$head_sha"; then
    exit 3
  fi
  head_sha="$(git rev-parse HEAD)"
  round=$((round + 1))
  say "rebased onto $moved_sha — train head is now $head_sha"
done

# The invariant, enforced rather than assumed: what lands is the sha the check
# ran against. If any future edit moves HEAD between the watch and this push,
# this stops main from fast-forwarding to a commit nothing verified.
if [ "$verified_sha" != "$head_sha" ]; then
  printf 'train-push: refusing to land %s — the green check ran against %s\n' \
    "$head_sha" "${verified_sha:-nothing}" >&2
  exit 1
fi

say "landing $head_sha on $remote/$default_branch"
set +e
push_private_history_guard "$head_sha"
git push "$remote" "$head_sha:refs/heads/$default_branch" 2>&1 | tee "$push_log"
land_rc="${PIPESTATUS[0]}"
set -e
if [ "$land_rc" -ne 0 ]; then
  # Branch protection rejecting the sha for want of a check is the one push
  # failure with a specific remedy, so say what it is instead of leaving the
  # operator to decode GH006.
  if grep -qE 'GH006|equired status check|rotected branch update failed' "$push_log"; then
    printf 'refused: %s has no status check on origin. Merge onto the train branch and push there; CI runs on the merge sha, then main fast-forwards.\n' \
      "$head_sha" >&2
  fi
  printf 'train-push: push of %s to %s/%s failed; %s/%s still holds the tested sha\n' \
    "$head_sha" "$remote" "$default_branch" "$remote" "$train_ref" >&2
  exit 1
fi

# Outcome check, not just command check: a push can report success through a
# wrapper (or fail on auth) while origin never moved.
git fetch -q "$remote" "$default_branch"
if ! git merge-base --is-ancestor "$head_sha" "$remote_default"; then
  printf 'train-push: push reported success but %s is not on %s/%s — origin did not move\n' \
    "$head_sha" "$remote" "$default_branch" >&2
  exit 1
fi
say "landed $head_sha on $remote/$default_branch"

# The branch existed to carry the train through CI; once the sha is on main it
# is noise. A failed delete does not un-land the commit, so it is a warning.
if ! git push -q "$remote" --delete "$train_ref"; then
  printf 'train-push: warning — could not delete %s/%s (delete it by hand)\n' "$remote" "$train_ref" >&2
fi
say "done"
