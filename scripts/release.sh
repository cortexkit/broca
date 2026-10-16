#!/usr/bin/env bash
# Tag and stage only the exact, already-green commit on origin/master.
# Commit version/lock changes with the code before CI, never in this script.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
refuse() { echo "release: $*" >&2; exit 2; }
[[ "$#" -ge 1 ]] || refuse 'usage: scripts/release.sh VERSION [--marker TEXT] [--gone TEXT] [--control TEXT] [--keep-days N]'
version="$1"; shift
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][a-zA-Z0-9.+-]+)?$ ]] || refuse 'expected a workspace version without a v prefix'
tag="v$version"
git check-ref-format "refs/tags/$tag" || refuse 'invalid release tag'
stage_args=()
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --marker|--gone|--control|--keep-days)
      [[ "$#" -ge 2 && -n "$2" ]] || refuse "$1 requires a value"
      stage_args+=("$1" "$2"); shift 2 ;;
    *) refuse "unsupported stage option: $1" ;;
  esac
done

dirty="$(git status --porcelain)" || refuse 'cannot read worktree status'
[[ -z "$dirty" ]] || refuse 'workspace is dirty; commit the complete versioned batch before training'
sha="$(git rev-parse --verify 'HEAD^{commit}')"
check_master() {
  git fetch --quiet --no-tags origin refs/heads/master:refs/remotes/origin/master || refuse 'cannot refresh origin/master'
  master_sha="$(git rev-parse --verify 'refs/remotes/origin/master^{commit}')"
  [[ "$sha" == "$master_sha" ]] || refuse 'HEAD is not the exact commit on origin/master; train the versioned batch first'
}
check_master
python_bin=""
for candidate in python3 python3.14 python3.13 python3.12 python3.11 /opt/homebrew/bin/python3; do
  if command -v "$candidate" >/dev/null && "$candidate" -c 'import tomllib' 2>/dev/null; then python_bin="$candidate"; break; fi
done
[[ -n "$python_bin" ]] || refuse 'Python 3.11+ is required to read the workspace version'
workspace_version="$("$python_bin" -c 'import tomllib; print(tomllib.load(open("Cargo.toml", "rb"))["workspace"]["package"]["version"])')"
[[ "$workspace_version" == "$version" ]] || refuse "workspace version is $workspace_version, not $version; bump it before the train"
# Only CI gates a release. Mutation replay proves the tests catch planted breaks, says nothing
# new about whether this build is correct, and takes ~50 minutes; it runs on every master push
# and nightly, and a failing row is a work item, not a reason to hold a release.
bash scripts/ci-status.sh "$sha" --only CI || refuse 'CI is not green for this exact commit; no tag created'

# CI lookup may take time. Recheck the commit and upstream before publishing a tag.
check_master
dirty="$(git status --porcelain)" || refuse 'cannot reread worktree status'
[[ "$(git rev-parse HEAD)" == "$sha" && -z "$dirty" ]] || refuse 'HEAD/worktree changed during the CI lookup'
if git rev-parse --verify --quiet "refs/tags/$tag" >/dev/null; then
  [[ "$(git rev-parse "$tag^{commit}")" == "$sha" ]] || refuse "$tag already names a different commit; never moving it"
else
  git tag -a "$tag" "$sha" -m "broca $version"
fi
echo "release: staging $tag at already-green $sha (no gate, train, or CI rerun)"
# macOS ships bash 3.2, where "${arr[@]}" on an empty array trips `set -u`.
bash scripts/stage-release.sh ${stage_args[@]+"${stage_args[@]}"}
echo "release: staged; $tag is local. Placement/restart still requires separate approval and scripts/place-release.sh."
