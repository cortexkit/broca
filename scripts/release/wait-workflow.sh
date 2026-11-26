#!/usr/bin/env bash
# A lost CLI connection is not a workflow conclusion. Only GitHub's completed state counts.
set -euo pipefail
source "$(dirname "$0")/../lib/exit-guard.sh"
trap 'broca_exit_guard :' EXIT

repo=${1:?missing repository}
run=${2:?missing workflow run ID}
deadline=$((SECONDS + 5400))
while :; do
    status=$("${OPERATOR_GH:?missing upstream gh}" run view "$run" -R "$repo" --json status,conclusion \
        --jq '[.status,.conclusion] | join("/")' 2>/dev/null) || status=""
    case "$status" in
        completed/success) break ;;
        completed/*) echo "release: workflow run $run ended $status" >&2; exit 2 ;;
    esac
    if [[ $SECONDS -ge $deadline ]]; then
        echo "release: timed out before workflow run $run reported completion" >&2
        exit 2
    fi
    sleep 15
done
BROCA_EXIT_GUARD_FINISHED=1
