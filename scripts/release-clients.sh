#!/usr/bin/env bash
# The clients use the module's commit suffix and are staged beside it. Explicit paths let a
# rollback use independently named snapshots without guessing which release they belong to.
broca_release_client_path() {
    local module="$1" client="$2" base suffix
    base="$(basename "$module")"
    case "$base" in
        ck-broca*) suffix="${base#ck-broca}" ;;
        *) suffix=".$base" ;;
    esac
    printf '%s/%s%s\n' "$(dirname "$module")" "$client" "$suffix"
}

broca_verify_client() {
    local client="$1" expected_version="$2" staged="$3" version expected_sha actual_sha
    if [ ! -x "$client" ]; then
        echo "REFUSED: missing executable client at $client" >&2
        return 1
    fi
    if ! version=$("$client" --version) || [ "$version" != "$(basename "$client") $expected_version" ]; then
        echo "REFUSED: client --version mismatch at $client: '$version'; expected '$(basename "$client") $expected_version'" >&2
        return 1
    fi
    if [ ! -f "$staged.sha256.postsign" ]; then
        echo "REFUSED: missing staged client sidecar at $staged.sha256.postsign" >&2
        return 1
    fi
    expected_sha=$(awk 'NR == 1 {print $1}' "$staged.sha256.postsign")
    actual_sha=$(shasum -a 256 "$client" | awk '{print $1}')
    if ! [[ "$expected_sha" =~ ^[0-9a-f]{64}$ ]] || [ "$actual_sha" != "$expected_sha" ]; then
        echo "REFUSED: client sha256 mismatch at $client (staged sidecar $staged.sha256.postsign)" >&2
        return 1
    fi
    printf '  client   %s -- version %s, sha256 %s MATCH\n' "$client" "$expected_version" "$actual_sha"
}
