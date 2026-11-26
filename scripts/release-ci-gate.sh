#!/usr/bin/env bash
# Only a landed commit with green push CI is eligible for public release.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib/exit-guard.sh
trap 'broca_exit_guard :' EXIT

sha=$(git rev-parse --verify "${1:?usage: release-ci-gate.sh COMMIT}^{commit}")
git fetch --quiet --no-tags origin '+refs/heads/master:refs/remotes/origin/master'
if ! git merge-base --is-ancestor "$sha" refs/remotes/origin/master; then
    echo "release: $sha is not on origin/master" >&2
    exit 2
fi
GH_REPO=cortexkit/broca bash scripts/ci-status.sh "$sha" --event push --only CI
BROCA_EXIT_GUARD_FINISHED=1
