#!/usr/bin/env bash
# Keep the table counts together: a string hit and a symbol hit prove different things.
broca_artifact_hits() { # <artifact> <literal needle> [best-effort]
    local s n listing
    if [ "${3:-}" = best-effort ]; then
        # verify-deploy.sh preserves its zero-on-error counts in this mode;
        # that caller requires a known control string to have a positive count.
        s=$(strings "$1" 2>/dev/null | grep -F -c -- "$2" || true)
    else
        # Staging must not mistake a failed strings invocation for an absent marker.
        listing=$(strings "$1") || return $?
        s=$(grep -F -c -- "$2" <<<"$listing" || true)
    fi
    n=$(nm -a "$1" 2>/dev/null | grep -F -c -- "$2" || true)
    echo "$s $n"
}
