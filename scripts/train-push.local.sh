#!/usr/bin/env bash
# Repo-local preflight, SOURCED by scripts/train-push.sh (so `say` and `refuse`
# are available, and a stray `exit` here would kill the train).
#
# WHY THIS EXISTS. A push gate answers "is this change good" and you read it
# because you just pushed. A SCHEDULED run answers "is what already landed
# still good against the world as it is now" -- and its whole value is firing
# when nobody is looking, which is exactly why its result arrives when nobody
# is looking. Measured on this repo (2026-09-19): five scheduled failures since
# the cron landed, FOUR of them the sibling-wave class, and not one was opened.
# Each cleared when the wave was absorbed for an unrelated reason.
#
# WHY THE TRAIN AND NOT A GIT HOOK. core.hooksPath points at the fleet
# dispatcher under ~/.local/share/cortexkit/aft/git-hooks, which this repo does
# not own; there is no repo-local pre-push to extend. Every landing here passes
# through this script, so it is the path that cannot be skipped. If the fleet
# dispatcher ever grows the same notice, DELETE THIS FILE rather than keeping
# both -- two copies print it twice and neither owner will notice.
#
# NOTICES, NEVER REFUSALS. A scheduled red is about what already landed; the
# push in front of you may be its fix, and blocking unrelated work would be
# wrong. It reprints on every train until the schedule is green, so unlike a
# notification it cannot be missed by being away.
#
# AND IT SAYS WHEN IT COULD NOT LOOK. The first version of this file collapsed
# "I could not check" into "I checked and found nothing": a failing `gh`
# printed "a cron is declared but NO scheduled run exists" (a false alarm
# pointing at a healthy cron) and a missing `python3` printed NOTHING AT ALL,
# including on a red. Both were reassuring-direction silences produced by a
# two-valued answer over a three-state domain -- present, absent, and
# could-not-tell. Every arm below keeps the third state visible.

if ! command -v gh >/dev/null 2>&1; then
  say "NOTICE: gh is not installed -- the scheduled-run check did NOT run"
else
  # `--event schedule` filters SERVER-SIDE: the limit applies to the filtered
  # set, so a push flood cannot empty this window the way a bare --limit can.
  #
  # The grading runs through gh's OWN embedded jq rather than an external
  # interpreter, so the arm that matters -- "a scheduled run failed" -- has one
  # dependency instead of two. `python3` is used only for the age arm below,
  # and its absence is reported rather than swallowed.
  #
  # It grades the newest NON-CANCELLED sample, not the newest run. A cancelled
  # run is not a red: nothing broke, the sample was simply never taken, because
  # this repo's concurrency group is `ci-${{ github.ref }}` with
  # cancel-in-progress and a master push during the 10:40Z cron window cancels
  # it. Reading only the newest run would then go SILENT on a failure sitting
  # behind a cancel -- 2 of the last 33 samples here were cancelled, so that is
  # a live shape rather than a hypothetical one. A value correctly classified
  # as "not a verdict" is not a stopping point.
  _sched_ok=1
  _sched_last=$(gh run list --event schedule --limit 10 \
    --json conclusion,createdAt,headSha \
    --jq 'map(select(.conclusion != null and .conclusion != "cancelled"))
          | if length == 0 then "NOGRADED" else
              "\(.[0].conclusion) \(.[0].createdAt[0:16]) \(.[0].headSha[0:8])"
            end' 2>/dev/null) || _sched_ok=0

  if [ "${_sched_ok}" -eq 0 ]; then
    # gh answered non-zero: rate limit, no auth, no network. This is NOT
    # "no scheduled runs" and must never be reported as one.
    say "NOTICE: could not read the scheduled-run history (gh failed) -- the scheduled-run check did NOT run"
  else
    _sched_total=$(gh run list --event schedule --limit 10 --json conclusion \
      --jq 'length' 2>/dev/null || echo "")

    if [ "${_sched_total:-0}" = "0" ]; then
      # A cron that never fires and a repo with no cron look identical from the
      # run list; only the workflow file tells them apart.
      if grep -l "schedule:" .github/workflows/*.yml >/dev/null 2>&1; then
        say "NOTICE: a cron is declared but NO scheduled run exists -- a schedule that never fires looks exactly like one that passes"
      fi
    else
      case "${_sched_last:-}" in
        success*) : ;;
        NOGRADED)
          say "NOTICE: all ${_sched_total} recent scheduled runs were CANCELLED by pushes -- not failures, but the samples were never taken"
          ;;
        "")
          say "NOTICE: the scheduled-run history did not grade -- the conclusion check did NOT run"
          ;;
        *)
          say "NOTICE: the last graded SCHEDULED run was not green: ${_sched_last}"
          say "  It answers 'is what already landed still good', it reports to nobody, and on an unchanged sha it CANNOT be your diff."
          say "  gh run list --event schedule --limit 10"
          ;;
      esac

      # AGE, of the newest sample of any conclusion. A cron that ran for
      # months, went green, then stopped -- workflow renamed, default branch
      # moved, GitHub disabling crons on a quiet repo -- reports EXACTLY what a
      # healthy one reports, and that happens precisely when nobody is looking.
      #
      # The bound is 48h and not 24h because this cron is `0 6 * * *` and lands
      # at ~10:40Z, four-plus hours behind, consistently. An alarm that fires on
      # a healthy queue delay gets muted, and a muted alarm is how the next real
      # one reaches nobody.
      if ! command -v python3 >/dev/null 2>&1; then
        say "NOTICE: python3 is not available -- the scheduled-run AGE check did not run (the conclusion check above did)"
      else
        _sched_newest=$(gh run list --event schedule --limit 1 --json createdAt \
          --jq '.[0].createdAt' 2>/dev/null || echo "")
        _sched_age_h=$(printf '%s' "${_sched_newest}" | python3 -c '
import sys, datetime
raw = sys.stdin.read().strip()
if raw:
    t = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    print(int((datetime.datetime.now(datetime.timezone.utc) - t).total_seconds() // 3600))
' 2>/dev/null || echo "")
        if [ -z "${_sched_age_h}" ]; then
          say "NOTICE: could not compute the scheduled-run age -- the AGE check did not run"
        elif [ "${_sched_age_h}" -gt 48 ] 2>/dev/null; then
          say "NOTICE: the newest SCHEDULED run is ${_sched_age_h}h old -- a cron that stopped firing leaves a green last run and looks healthy"
        fi
      fi
    fi
  fi
  unset _sched_ok _sched_last _sched_total _sched_newest _sched_age_h
fi
