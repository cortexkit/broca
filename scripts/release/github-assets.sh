#!/usr/bin/env bash
# Draft, verify and publish are separate workflow jobs. Only publish changes visibility.
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/lib/exit-guard.sh
trap 'broca_exit_guard :' EXIT
helper=scripts/release/release.py
repo=${GITHUB_REPOSITORY:?missing Actions repository}
tag=${TAG:?missing release tag}

case "${1:-}" in
    draft)
        version=${tag#v}
        [[ $tag == "v$version" ]]
        python3 "$helper" inputs "$version"
        gh api --paginate --slurp "repos/$repo/releases?per_page=100" | python3 "$helper" available "$version"
        gh release create "$tag" --repo "$repo" --verify-tag --draft --title "broca $tag" \
            --notes-file ".cortexkit/release-notes/$tag.md"
        id=$(gh release view "$tag" --repo "$repo" --json databaseId,isDraft \
            --jq 'select(.isDraft) | .databaseId')
        [[ $id =~ ^[1-9][0-9]*$ ]]
        printf 'release_id=%s\n' "$id" >> "${GITHUB_OUTPUT:?missing Actions output file}"
        ;;
    verify|publish)
        id=${RELEASE_ID:?missing draft release id}
        [[ $id =~ ^[1-9][0-9]*$ ]]
        endpoint="repos/$repo/releases/$id"
        inventory() { gh api "$endpoint" | python3 "$helper" inventory "$tag"; }
        if [[ $1 == verify ]]; then
            cp release-manifest.json assets/
            python3 "$helper" check-assets assets
            gh release upload "$tag" --repo "$repo" assets/*
            # Read assets by ID from GitHub Releases, not from the build artifact store.
            mkdir verified-assets
            inventory > verified-assets/download-inventory.txt
            mkdir verified-assets/downloads
            while read -r asset_id name; do
                gh api -H 'Accept: application/octet-stream' "repos/$repo/releases/assets/$asset_id" \
                    > "verified-assets/downloads/$name"
                if ! cmp -s "assets/$name" "verified-assets/downloads/$name"; then
                    echo "release: downloaded $name differs from the build artifact" >&2
                    exit 2
                fi
            done < verified-assets/download-inventory.txt
            python3 "$helper" check-assets verified-assets/downloads
            mv verified-assets/download-inventory.txt verified-assets/inventory.txt
        else
            # Asset IDs change on reupload. Recheck that exactly the verified assets are
            # still attached to the same draft before making it public.
            inventory > current-inventory.txt
            if ! cmp -s verified-assets/inventory.txt current-inventory.txt; then
                echo 'release: release assets changed after verification; leaving draft unpublished' >&2
                exit 2
            fi
            git fetch --quiet --no-tags origin "refs/tags/$tag"
            tag_commit=$(git rev-parse --verify 'FETCH_HEAD^{commit}')
            build_commit=$(git rev-parse --verify 'HEAD^{commit}')
            if [[ $tag_commit != "$build_commit" ]]; then
                echo "release: $tag changed after verification; leaving draft unpublished" >&2
                exit 2
            fi
            gh api --method PATCH "$endpoint" -F draft=false --jq '.html_url'
        fi
        ;;
    *) echo 'usage: github-assets.sh draft|verify|publish' >&2; exit 2 ;;
esac
BROCA_EXIT_GUARD_FINISHED=1
