#!/usr/bin/env bash
# Publish a chosen, already-tagged deploy; do not bump, land, tag or deploy.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib/exit-guard.sh

scratch=""
release_state=""
cleanup() {
    if [[ $BROCA_EXIT_GUARD_FINISHED != 1 && -n $release_state ]]; then
        printf 'release: %s; inspect the Actions run and any GitHub draft before retrying.\n' "$release_state" >&2
    fi
    [[ -z $scratch ]] || rm -rf "$scratch"
}
trap 'broca_exit_guard cleanup' EXIT
refuse() { printf 'release: %s\n' "$*" >&2; exit 2; }
trap 'refuse interrupted' INT TERM

version="${1:-}"
dry_run=0
if [[ $# == 2 && $2 == --dry-run ]]; then
    dry_run=1
elif [[ $# != 1 ]]; then
    refuse 'usage: scripts/release.sh X.Y.Z [--dry-run]'
fi
python=""
for candidate in python3 python3.14 python3.13 python3.12 python3.11 /opt/homebrew/bin/python3; do
    if "$candidate" -c 'import tomllib' >/dev/null 2>&1; then python="$candidate"; break; fi
done
[[ -n $python ]] || refuse 'Python 3.11+ is required'
helper=scripts/release/release.py
repo=cortexkit/broca
"$python" "$helper" version "$version"
tag="v$version"
notes=".cortexkit/release-notes/$tag.md"

check_checkout() {
    local branch dirty landed
    branch=$(git symbolic-ref --quiet --short HEAD) || refuse 'checkout must be on master'
    [[ $branch == master ]] || refuse 'checkout must be on master'
    dirty=$(git status --porcelain --untracked-files=all) || refuse 'cannot inspect worktree status'
    [[ -z $dirty ]] || refuse 'checkout is dirty; commit the release notes and manifest first'
    git fetch --quiet --no-tags origin '+refs/heads/master:refs/remotes/origin/master'
    landed=$(git rev-parse --verify 'refs/remotes/origin/master^{commit}')
    [[ $sha == "$landed" && $sha == "$(git rev-parse HEAD)" ]] || refuse 'master is not the same commit as freshly fetched origin/master'
    git fetch --quiet --no-tags origin "refs/tags/$tag" || refuse "$tag does not exist on origin"
    [[ $(git rev-parse --verify 'FETCH_HEAD^{commit}') == "$sha" ]] || refuse "origin's $tag does not name master commit $sha"
}
sha=$(git rev-parse --verify 'HEAD^{commit}')
check_checkout
floor=$("$python" "$helper" inputs "$version")
git ls-files --error-unmatch -- "$notes" >/dev/null || refuse "$notes must be committed"
source scripts/lib/operator-gh.sh
# ci-status.sh also invokes gh; keep it on the same upstream CLI as dispatch.
gh_dir=$(dirname "$OPERATOR_GH")
export PATH="$gh_dir:$PATH"
check_available() {
    "$OPERATOR_GH" api --paginate --slurp "repos/$repo/releases?per_page=100" \
        | "$python" "$helper" available "$version"
}
check_available
bash scripts/release-ci-gate.sh "$sha"

case "$(uname -s)" in
    Darwin) os=darwin ;;
    Linux) os=linux ;;
    *) refuse 'the core-floor smoke tests need a native macOS or Linux host' ;;
esac
case "$(uname -m)" in
    arm64|aarch64) arch=arm64 ;;
    x86_64|amd64) arch=x64 ;;
    *) refuse 'unsupported host architecture' ;;
esac
mkdir -p target/scratch
scratch=$(mktemp -d "$PWD/target/scratch/public-release.XXXXXX")
asset="ck-subc-$os-$arch.zip"
# The public release index follows new core versions. Fetch the declared minimum
# core from its immutable tag so the tests exercise that version, not the latest.
core_url="https://github.com/cortexkit/subconscious/releases/download/subc-core-v$floor/$asset"
curl --fail --location --silent --show-error --max-time 120 "$core_url" -o "$scratch/$asset"
curl --fail --location --silent --show-error --max-time 60 "$core_url.sha256" -o "$scratch/$asset.sha256"
core_bin=$("$python" "$helper" unpack-core "$scratch/$asset" "$scratch")

if [[ ${RUSTC_WRAPPER+x} && -z $RUSTC_WRAPPER ]]; then unset RUSTC_WRAPPER; fi
if [[ ${CARGO_BUILD_RUSTC_WRAPPER+x} && -z $CARGO_BUILD_RUSTC_WRAPPER ]]; then unset CARGO_BUILD_RUSTC_WRAPPER; fi
nice -n 10 env CARGO_INCREMENTAL=0 cargo check --locked -p broca-module-serve --bin ck-broca
nice -n 10 env CARGO_INCREMENTAL=0 cargo check --locked -p broca-module-serve --bin broca-session
echo "release: testing daemon launch, seal and respawn against public core $floor ($asset)"
nice -n 10 env CARGO_INCREMENTAL=0 SUBC_CORE_BIN="$core_bin" BROCA_REQUIRE_SUBC=1 \
    cargo nextest run --locked -p broca-module-serve --no-tests=fail \
    -E 'binary(it) & test(seal_line_is_filterable_e2e::)'

check_checkout
check_available
if [[ $dry_run == 1 ]]; then
    echo "release: dry run passed for $tag; no commits, tags, workflows or releases changed"
    BROCA_EXIT_GUARD_FINISHED=1
    exit 0
fi

# Use the ID from this dispatch, not a list that might contain an older run or
# a concurrent dispatch for the same tag.
release_state="workflow dispatch for $tag attempted"
run=$("$OPERATOR_GH" api --method POST "repos/$repo/actions/workflows/release.yml/dispatches" \
    -f "ref=$tag" -f "inputs[version]=$version" -F return_run_details=true --jq '.workflow_run_id')
[[ $run =~ ^[1-9][0-9]*$ ]] || refuse "GitHub did not return the release workflow run ID for $tag"
release_state="GitHub release workflow run $run"
bash scripts/release/wait-workflow.sh "$repo" "$run"

release_state="GitHub workflow run $run succeeded; release index not yet confirmed"
deadline=$((SECONDS + 900))
while ! { curl --fail --location --silent --show-error --max-time 20 --header 'Cache-Control: no-cache' \
    https://cortexkit.io/releases/v1/index.json -o "$scratch/index.json" &&
    "$python" "$helper" index "$scratch/index.json" "$version" "$floor"; }; do
    [[ $SECONDS -lt $deadline ]] || refuse "index did not confirm broca $version requires_core $floor"
    sleep 15
done
printf 'release: published %s (%s); release index confirms broca %s requires_core %s\n' "$tag" "$sha" "$version" "$floor"
BROCA_EXIT_GUARD_FINISHED=1
