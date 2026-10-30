#!/usr/bin/env bash
# Staging and placement must enforce the same policy, including directly supplied artifacts.
broca_refuse_nonshipping_markers() { # <artifact> <diagnostic prefix>
    local artifact="$1" prefix="$2" marker label control listing control_s artifact_s artifact_n
    while read -r label marker; do
        control=$(mktemp) || return 2
        printf '%s\n' "$marker" > "$control"
        if ! listing=$(strings "$control"); then
            rm -f "$control"
            echo "$prefix: REFUSING -- $label marker control could not be read." >&2
            return 2
        fi
        rm -f "$control"
        control_s=$(grep -F -c -- "$marker" <<<"$listing" || true)
        if [[ $control_s -eq 0 ]]; then
            echo "$prefix: REFUSING -- strings cannot see the planted $label marker control." >&2
            return 2
        fi
        if ! listing=$(strings "$artifact"); then
            echo "$prefix: REFUSING -- could not read the artifact for the $label check." >&2
            return 2
        fi
        artifact_s=$(grep -F -c -- "$marker" <<<"$listing" || true)
        artifact_n=$(nm -a "$artifact" 2>/dev/null | grep -F -c -- "$marker" || true)
        if [[ $(( artifact_s + artifact_n )) -gt 0 ]]; then
            echo "$prefix: REFUSING -- artifact contains the $label override marker: $marker" >&2
            return 2
        fi
        echo "== $label absent (planted strings control reads $control_s)"
    done <<'MARKERS'
test-clock BROCA_TEST_CLOCK_RETENTION_CLOCK_OVERRIDE_V1
test-harness BROCA_TEST_HARNESS_VIRTUAL_RETRY_WAITS_V1
test-attachments broca-nonshipping-attachments-v1
fs-override BROCA_TEST_FS_OVERRIDE_V1
MARKERS
}
