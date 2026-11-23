#!/usr/bin/env bash
# Deploy committed local bytes first; publishing and CI follow in a detached worker.
# Usage: scripts/deploy.sh --marker STRING --control STRING [--live-approved] [--no-publish]
#                          [--features LIST]
# --no-publish deploys to this machine only: nothing is pushed to GitHub, so no CI run, master
# fast-forward or public tag follows. Use it for development deploys, whose release suites have
# not run; publish later with the printed command.
# --features builds ck-broca with those Cargo features, such as heap-profile. It needs
# --no-publish: a featured build is never the release, so it is never pushed or tagged as one.
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
MARKER="" CONTROL="" FEATURES=""
placement_args=()
publish=1
while [ "$#" -gt 0 ]; do
  case "$1" in
    --marker|--control)
      [ "$#" -ge 2 ] && [ -n "$2" ] || { echo "deploy: usage error: $1 requires an argument" >&2; exit 2; }
      case "$1" in --marker) MARKER="$2" ;; --control) CONTROL="$2" ;; esac
      shift 2 ;;
    --live-approved) placement_args+=(--live-approved); shift ;;
    --no-publish) publish=0; shift ;;
    --features)
      [ "$#" -ge 2 ] && [ -n "$2" ] || { echo "deploy: usage error: --features requires a list" >&2; exit 2; }
      FEATURES="$2"; shift 2 ;;
    *) echo "deploy: unknown argument: $1" >&2; exit 2 ;;
  esac
done
[ -n "$MARKER" ] && [ -n "$CONTROL" ] || {
  echo "usage: scripts/deploy.sh --marker STRING --control STRING [--live-approved] [--no-publish] [--features LIST]" >&2; exit 2;
}
if [ -n "$FEATURES" ] && [ "$publish" -eq 1 ]; then
  echo "deploy: REFUSING -- --features builds a development binary; add --no-publish" >&2
  exit 2
fi
dirty="$(git status --porcelain)" || { echo "deploy: cannot read worktree status" >&2; exit 2; }
if [ -n "$dirty" ]; then
  echo "deploy: REFUSING -- dirty working tree; commit the change before deploying." >&2
  printf '%s\n' "$dirty" >&2
  exit 2
fi
sha="$(git rev-parse --verify "HEAD^{commit}")"
version="$(awk -F'"' '/^version = "/ {print $2; exit}' Cargo.toml)"
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "deploy: invalid workspace version '$version'" >&2; exit 2; }
tag="v$version"
if git show-ref --verify --quiet "refs/tags/$tag"; then
  tagged="$(git rev-parse --verify "refs/tags/$tag^{commit}")"
  [ "$tagged" = "$sha" ] || { echo "deploy: REFUSING -- $tag already names $tagged, not $sha" >&2; exit 2; }
else
  # Staging requires a local release tag. Only the post-deploy CI worker publishes it.
  git tag -a "$tag" "$sha" -m "broca $version"
fi
logs="$HOME/.local/state/cortexkit/broca/deploys"
mkdir -p "$logs"
logdir="$(mktemp -d "$logs/${sha:0:8}.XXXXXX")"
echo "deploy: logs $logdir"
if ! bash scripts/stage-release.sh --marker "$MARKER" --control "$CONTROL" ${FEATURES:+--features "$FEATURES"} 2>&1 | tee "$logdir/stage.log"; then
  echo "deploy: STAGING FAILED; nothing placed or restarted. See $logdir/stage.log" >&2
  exit 1
fi
[ "$(git rev-parse HEAD)" = "$sha" ] && [ -z "$(git status --porcelain)" ] || {
  echo "deploy: HEAD/worktree changed during staging; refusing placement" >&2; exit 1;
}
staged="$HOME/.local/share/cortexkit/staging/ck-broca.$(git rev-parse --short=8 "$sha")"
dest="$HOME/.local/share/cortexkit/bin/ck-broca"
report="$logdir/placement.json"
rollback_command() {
  if [ -f "$report" ]; then
    backup="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["backup"])' "$report")"
    if [ -n "$backup" ]; then
      printf 'Rollback: '
      printf '%q ' "$ROOT/scripts/place-release.sh" --staged "$backup"
      for client in broca-session; do
        client_backup="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["client_backups"][sys.argv[2]])' "$report" "$client")"
        if [ -n "$client_backup" ]; then
          printf '%q ' --session-staged "$client_backup"
        else
          # Rolling back a first installation restores absence, not an incompatible new client.
          printf '%q ' --remove-session
        fi
      done
      printf '%q ' --dest "$dest" --restart --live-approved
      printf '\n'
      return
    fi
  fi
  echo "Rollback: no rollback snapshot was created; inspect $logdir/place.log before retrying."
}
if ! bash scripts/place-release.sh --staged "$staged" --restart --report "$report" \
    ${placement_args[@]+"${placement_args[@]}"} 2>&1 | tee "$logdir/place.log"; then
  echo "deploy: PLACEMENT/RESTART FAILED. See $logdir/place.log" >&2
  rollback_command >&2
  exit 1
fi
# Record a conservative restart boundary after placement returns. A run admitted
# before the restart (even if its result arrives later) cannot certify the new daemon.
python3 - "$report" <<'PY'
import json, pathlib, sys, time
path = pathlib.Path(sys.argv[1])
report = json.loads(path.read_text())
report["restart_observed_at_ms"] = time.time_ns() // 1_000_000
path.write_text(json.dumps(report))
PY
expected_sha="$(shasum -a 256 "$staged" | awk '{print $1}')"
if ! bash scripts/verify-deploy.sh --staged "$staged" --sha "$expected_sha" --marker "$MARKER" --control "$CONTROL" \
    2>&1 | tee "$logdir/verify.log"; then
  echo "deploy: LIVE VERIFICATION FAILED" >&2
  rollback_command >&2
  exit 1
fi
# The daemon answers `module_warming` until the restarted broca has finished starting, and a
# one-time store upgrade can make that take minutes (0.3.246's took 100 s). That refusal is the
# daemon saying "not yet", not a failed deploy, so retry it for up to five minutes; any other
# failure ends the deploy at once.
acceptance() {
  local attempt status
  for attempt in $(seq 1 30); do
    if bash scripts/acceptance-turn.sh "$version" >"$logdir/acceptance.log" 2>&1; then
      cat "$logdir/acceptance.log"
      return 0
    else
      status=$?
    fi
    if [ "$status" -eq 3 ]; then
      cat "$logdir/acceptance.log"
      direct_verdict="TOOL REFUSED (project_required)"
      echo "deploy: WARNING -- direct tool acceptance: TOOL REFUSED (project_required), exit 3; continuing worker and scoped checks" >&2
      return 0
    fi
    if ! grep -q 'module_warming' "$logdir/acceptance.log"; then
      cat "$logdir/acceptance.log"
      return 1
    fi
    echo "deploy: broca is still starting (module_warming), acceptance attempt $attempt of 30; retrying"
    # Tests shorten the wait; a real deploy always waits 10 s.
    sleep "${BROCA_DEPLOY_WARMING_RETRY_SECS:-10}"
  done
  cat "$logdir/acceptance.log"
  echo "deploy: broca was still starting after five minutes" >&2
  return 1
}
# stage-release builds both ck-broca and this client from the same committed HEAD.
direct_verdict="PASS"
if [ ! -x "${CARGO_TARGET_DIR:-./target}/release/broca-session" ] || ! acceptance; then
  echo "deploy: LIVE ACCEPTANCE FAILED (including unavailable providers/client); not publishing." >&2
  rollback_command >&2
  exit 1
fi
# Project-free builds check mason/sidekick admissions whose plan fetched the aft
# tool provider, then wait for any candidate to complete. Runs without an aft
# fetch are counted as ignored; other terminal reasons are counted, not failures.
# Older builds compose and send an acceptance turn with their project folder.
# Observation sends nothing and reports metadata only; it never creates a
# daemon-authenticated project scope. It waits at most ten minutes for completion.
if ! python3 scripts/acceptance-worker-turn.py --placement-report "$report" \
    >"$logdir/acceptance-worker.log" 2>&1; then
  cat "$logdir/acceptance-worker.log" >&2
  echo "deploy: WORKER ADMISSION ACCEPTANCE FAILED; not publishing." >&2
  rollback_command >&2
  exit 1
fi
cat "$logdir/acceptance-worker.log"
if ! python3 scripts/lib/acceptance_scoped.py --placement-report "$report" \
    >"$logdir/acceptance-scoped.log" 2>&1; then
  cat "$logdir/acceptance-scoped.log" >&2
  echo "deploy: SCOPED DISPATCH ACCEPTANCE FAILED; not publishing." >&2
  rollback_command >&2
  exit 1
fi
cat "$logdir/acceptance-scoped.log"
ci_log="$logdir/ci.log"
if [ -n "$FEATURES" ]; then
  echo "deploy: --features $FEATURES: a development build, deployed here only and never published."
  echo "deploy: redeploy the release build of $version to replace it."
elif [ "$publish" -eq 0 ]; then
  echo "deploy: --no-publish: deployed here only. Publish later with: scripts/tag-when-green.sh $sha $version"
elif ! TAG_WHEN_GREEN_LOG="$ci_log" bash scripts/tag-when-green.sh "$sha" "$version"; then
  echo "deploy: live acceptance checks finished (direct: $direct_verdict; worker: PASS; scoped: PASS), but could not start background publication. Retry scripts/tag-when-green.sh $sha $version" >&2
  rollback_command >&2
  exit 1
fi
echo "== deployed broca $version"
echo "   sha        $sha"
python3 - "$report" <<'PY'
import json, sys
report = json.load(open(sys.argv[1]))
print("   pid        " + report["pid"])
print("   runs live at restart: " + str(len(report["active_runs_at_restart"])))
for status, label in (("resumed", "resumed after the restart"),
                      ("sealed", "sealed interrupted"),
                      ("finished", "ended normally"),
                      ("paused", "paused, not running yet")):
    if report.get(f"sessions_{status}"):
        print(f"   {label}: " + ", ".join(report[f"sessions_{status}"]))
if report["sessions_unconfirmed"]:
    print("   WARNING seal not confirmed: " + ", ".join(report["sessions_unconfirmed"]))
if report.get("sessions_cancelled"):
    print("   FAILURE shutdown drain cancelled run: " + ", ".join(report["sessions_cancelled"]))
if report["seal_error"]:
    print("   WARNING seal readback: " + report["seal_error"])
if report.get("sessions_cancelled"):
    sys.exit(1)
PY
echo "   acceptance direct: $direct_verdict; worker: PASS; scoped: PASS"
rollback_command
if [ "$publish" -eq 1 ]; then
  echo "   CI log     $ci_log (detached; CI does not gate deployment)"
fi
BROCA_EXIT_GUARD_FINISHED=1
