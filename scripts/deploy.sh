#!/usr/bin/env bash
# Deploy committed local bytes first; publishing and CI follow in a detached worker.
# Usage: scripts/deploy.sh --marker STRING --control STRING [--live-approved]
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
MARKER="" CONTROL=""
placement_args=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --marker|--control)
      [ "$#" -ge 2 ] && [ -n "$2" ] || { echo "deploy: usage error: $1 requires an argument" >&2; exit 2; }
      case "$1" in --marker) MARKER="$2" ;; --control) CONTROL="$2" ;; esac
      shift 2 ;;
    --live-approved) placement_args+=(--live-approved); shift ;;
    *) echo "deploy: unknown argument: $1" >&2; exit 2 ;;
  esac
done
[ -n "$MARKER" ] && [ -n "$CONTROL" ] || {
  echo "usage: scripts/deploy.sh --marker STRING --control STRING [--live-approved]" >&2; exit 2;
}
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
# Overlap the read-only metadata gate with staging, but do not permit placement
# until its status has been collected. Captured output stays private until then.
metadata_pid="" metadata_listing="" metadata_listing_error="" metadata_output=""
cleanup_metadata() {
  local status=$?
  trap - EXIT
  if [ -n "$metadata_pid" ]; then
    # The gate owns a process group, including vault/checker descendants, so an
    # earlier deploy failure cannot leave the background scan running.
    kill -TERM -- "-$metadata_pid" 2>/dev/null || true
    wait "$metadata_pid" 2>/dev/null || true
    kill -KILL -- "-$metadata_pid" 2>/dev/null || true
  fi
  rm -f "$metadata_listing" "$metadata_listing_error" "$metadata_output"
  exit "$status"
}
trap cleanup_metadata EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
metadata_listing="$(mktemp "$logdir/credential-listing.XXXXXX")" || {
  echo "deploy: credential metadata listing unavailable" >&2
  exit 2
}
metadata_listing_error="$metadata_listing.stderr"
metadata_output="$(mktemp "$logdir/credential-output.XXXXXX")" || exit 2
# Job control gives this worker its own process group without a setsid dependency.
set -m
(
  set +m
  trap - EXIT INT TERM
  if ! ck auth list --json >"$metadata_listing" 2>"$metadata_listing_error"; then
    echo "deploy: credential metadata listing unavailable" >&2
    exit 2
  fi
  exec python3 scripts/check-credential-metadata.py --vault-listing "$metadata_listing"
) >"$metadata_output" 2>&1 &
metadata_pid=$!
set +m
if ! bash scripts/stage-release.sh --marker "$MARKER" --control "$CONTROL" 2>&1 | tee "$logdir/stage.log"; then
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
      for client in broca-session broca-run; do
        client_backup="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["client_backups"][sys.argv[2]])' "$report" "$client")"
        if [ -n "$client_backup" ]; then
          case "$client" in broca-session) option=--session-staged ;; broca-run) option=--run-staged ;; esac
          printf '%q ' "$option" "$client_backup"
        else
          # Rolling back a first installation restores absence, not an incompatible new client.
          case "$client" in broca-session) option=--remove-session ;; broca-run) option=--remove-run ;; esac
          printf '%q ' "$option"
        fi
      done
      printf '%q ' --dest "$dest" --restart --live-approved
      printf '\n'
      return
    fi
  fi
  echo "Rollback: no rollback snapshot was created; inspect $logdir/place.log before retrying."
}
# Placement can restart the daemon. Collect the real worker status before that
# first restart-capable command, including refusal and cannot-compare outcomes.
if wait "$metadata_pid"; then
  metadata_status=0
else
  metadata_status=$?
fi
metadata_pid=""
tee "$logdir/account-metadata.log" <"$metadata_output"
if [ "$metadata_status" -ne 0 ]; then
  echo "deploy: ACCOUNT METADATA CHECK FAILED; nothing placed or restarted. See $logdir/account-metadata.log" >&2
  exit "$metadata_status"
fi
if ! bash scripts/place-release.sh --staged "$staged" --restart --report "$report" \
    ${placement_args[@]+"${placement_args[@]}"} 2>&1 | tee "$logdir/place.log"; then
  echo "deploy: PLACEMENT/RESTART FAILED. See $logdir/place.log" >&2
  rollback_command >&2
  exit 1
fi
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
  local attempt
  for attempt in $(seq 1 30); do
    if bash scripts/acceptance-turn.sh "$version" >"$logdir/acceptance.log" 2>&1; then
      cat "$logdir/acceptance.log"
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
if [ ! -x "${CARGO_TARGET_DIR:-./target}/release/broca-session" ] || ! acceptance; then
  echo "deploy: LIVE ACCEPTANCE FAILED (including unavailable providers/client); not publishing." >&2
  rollback_command >&2
  exit 1
fi
if ! python3 scripts/acceptance-worker-turn.py >"$logdir/acceptance-worker.log" 2>&1; then
  cat "$logdir/acceptance-worker.log" >&2
  echo "deploy: WORKER ADMISSION ACCEPTANCE FAILED; not publishing." >&2
  rollback_command >&2
  exit 1
fi
cat "$logdir/acceptance-worker.log"
ci_log="$logdir/ci.log"
if ! TAG_WHEN_GREEN_LOG="$ci_log" bash scripts/tag-when-green.sh "$sha" "$version"; then
  echo "deploy: live acceptance passed, but could not start background publication. Retry scripts/tag-when-green.sh $sha $version" >&2
  rollback_command >&2
  exit 1
fi
echo "== deployed broca $version"
echo "   sha        $sha"
python3 - "$report" <<'PY'
import json, sys
report = json.load(open(sys.argv[1]))
print("   pid        " + report["pid"])
print("   sessions live at restart: " + (", ".join(report["sessions_at_restart"]) or "none"))
print("   sessions sealed at restart (WAL readback): " + (", ".join(report["sessions_sealed"]) or "none"))
if report["sessions_unconfirmed"]:
    print("   WARNING seal not confirmed: " + ", ".join(report["sessions_unconfirmed"]))
if report.get("sessions_cancelled"):
    print("   FAILURE shutdown drain cancelled run: " + ", ".join(report["sessions_cancelled"]))
if report["seal_error"]:
    print("   WARNING seal readback: " + report["seal_error"])
if report.get("sessions_cancelled"):
    sys.exit(1)
PY
echo "   acceptance PASS (live tool dispatch; classed fetch-plan turn and WAL feature gates)"
rollback_command
echo "   CI log     $ci_log (detached; CI does not gate deployment)"
