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
      printf '%q ' "$ROOT/scripts/place-release.sh" --staged "$backup" --dest "$dest" --restart --live-approved
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
expected_sha="$(shasum -a 256 "$staged" | awk '{print $1}')"
if ! bash scripts/verify-deploy.sh --sha "$expected_sha" --marker "$MARKER" --control "$CONTROL" \
    2>&1 | tee "$logdir/verify.log"; then
  echo "deploy: LIVE VERIFICATION FAILED" >&2
  rollback_command >&2
  exit 1
fi
# stage-release builds both ck-broca and this client from the same committed HEAD.
if [ ! -x ./target/release/broca-session ] || \
    ! bash scripts/acceptance-turn.sh "$version" 2>&1 | tee "$logdir/acceptance.log"; then
  echo "deploy: LIVE ACCEPTANCE FAILED (including unavailable providers/client); not publishing." >&2
  rollback_command >&2
  exit 1
fi
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
echo "   acceptance PASS (live tool dispatch, result, completed turn)"
rollback_command
echo "   CI log     $ci_log (detached; CI does not gate deployment)"
