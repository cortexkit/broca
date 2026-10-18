#!/usr/bin/env bash
# Shared UUID check for the Mach-O image and its detached debug-symbol bundle.

broca_dsym_uuid_records() {
    local raw records
    raw=$(dwarfdump --uuid "$1") || return 1
    records=$(awk '$1 == "UUID:" { print $2, $3; found++ } END { if (found == 0) exit 1 }' <<<"$raw") || return 1
    [[ -n $records ]] || return 1
    LC_ALL=C sort <<<"$records"
}

broca_dsym_matches_binary() {
    local binary_uuids dsym_uuids
    [[ -d $2 ]] || return 1
    binary_uuids=$(broca_dsym_uuid_records "$1") || return 1
    dsym_uuids=$(broca_dsym_uuid_records "$2") || return 1
    [[ -n $binary_uuids && $binary_uuids == "$dsym_uuids" ]]
}
