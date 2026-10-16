#!/usr/bin/env bash
# Publish immutable deployed bytes without rebasing or modifying the checked-out branch.
# Usage: scripts/tag-when-green.sh <sha> <version> (detaches and prints its log path)
set -euo pipefail
umask 077
if [ "${1:-}" = --watch ]; then
  [ "$#" -eq 4 ] || { echo "tag-when-green: invalid worker invocation" >&2; exit 2; }
  ROOT="$2"; shift 2
  worker=1
else
  ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  worker=0
fi
cd "$ROOT"
[ "$#" -eq 2 ] || { echo "usage: scripts/tag-when-green.sh <sha> <version>" >&2; exit 2; }
sha="$(git rev-parse --verify "$1^{commit}")"
version="$2"
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "tag-when-green: invalid version '$version'" >&2; exit 2; }
committed_version="$(git show "$sha:Cargo.toml" | awk -F'"' '/^version = "/ {print $2; exit}')"
[ "$version" = "$committed_version" ] || { echo "tag-when-green: version does not match $sha" >&2; exit 2; }
if [ "$worker" -eq 0 ]; then
  logs="$HOME/.local/state/cortexkit/broca/deploys"
  mkdir -p "$logs"
  log="${TAG_WHEN_GREEN_LOG:-$(mktemp "$logs/ci-${sha:0:8}.XXXXXX")}"
  # A private script copy survives edits made while the worker is waiting for CI.
  copy="$(mktemp "$(dirname "$log")/tag-worker.XXXXXX")"
  cp "${BASH_SOURCE[0]}" "$copy"
  python3 - "$copy" "$ROOT" "$sha" "$version" "$log" <<'PY'
import subprocess, sys
script, root, sha, version, log = sys.argv[1:]
with open(log, "ab", buffering=0) as output:
    process = subprocess.Popen(["bash", script, "--watch", root, sha, version],
                               stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                               start_new_session=True, close_fds=True)
print(f"tag-when-green: detached pid {process.pid}; CI log {log}")
PY
  exit 0
fi
tag="v$version"
if git show-ref --verify --quiet "refs/tags/$tag"; then
  [ "$(git rev-parse "refs/tags/$tag^{commit}")" = "$sha" ] || {
    echo "tag-when-green: REFUSING -- $tag names another commit" >&2; exit 2;
  }
fi
# Refuse publishing descendants of the historical private repository root;
# a local deployment must not expose that commit graph on the public remote.
private_root=47b17104d2688c9d5914bea4e7b52ead894d07b9
if git cat-file -e "$private_root^{commit}" 2>/dev/null; then
  history_rc=0
  git merge-base --is-ancestor "$private_root" "$sha" || history_rc=$?
  if [ "$history_rc" -ne 1 ]; then
    echo "tag-when-green: REFUSING -- private history is present or its ancestry cannot be determined; use the private repository workflow" >&2
    exit 2
  fi
fi
ref="refs/heads/train/deploy-$sha"
echo "tag-when-green: pushing $sha to $ref"
git push origin "$sha:$ref"
# The ref is immutable: an existing CI run for this exact SHA is also valid on retry.
bash "$ROOT/scripts/watch-ci.sh" "$sha"
echo "tag-when-green: CI green for $sha; attempting fast-forward of origin/master"
if ! git push origin "$sha:refs/heads/master"; then
  echo "tag-when-green: origin/master was NOT advanced (non-fast-forward or push failed); leaving master alone, no rebase or force attempted." >&2
fi
if ! git show-ref --verify --quiet "refs/tags/$tag"; then
  git tag -a "$tag" "$sha" -m "broca $version"
fi
[ "$(git rev-parse "refs/tags/$tag^{commit}")" = "$sha" ] || {
  echo "tag-when-green: REFUSING -- $tag moved while CI ran" >&2; exit 2;
}
git push origin "refs/tags/$tag:refs/tags/$tag"
echo "tag-when-green: published $tag for $sha"
