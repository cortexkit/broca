#!/usr/bin/env bash
# Sign the executable, verify its hardened runtime, ZIP it, require accepted
# notarization, and only then checksum the ZIP. Gatekeeper checks the ticket
# online: Apple cannot staple a ZIP or a standalone executable.
set -euo pipefail
source "$(dirname "$0")/../lib/exit-guard.sh"
umask 077
credentials_dir=""
keychain=""
cleanup() {
    local status=0
    if [[ -n $keychain && -f $keychain ]]; then security delete-keychain "$keychain" || status=$?; fi
    if [[ -n $credentials_dir ]]; then rm -rf "$credentials_dir" || status=$?; fi
    return "$status"
}
trap 'broca_exit_guard cleanup' EXIT
fail() { printf 'release: %s\n' "$*" >&2; exit 2; }

for name in DEVELOPER_ID_APPLICATION_CERTIFICATE_BASE64 DEVELOPER_ID_APPLICATION_CERTIFICATE_PASSWORD \
    APP_STORE_CONNECT_API_KEY APP_STORE_CONNECT_API_KEY_ID APP_STORE_CONNECT_API_ISSUER_ID; do
    [[ -n ${!name:-} ]] || fail "missing Actions secret: $name"
done
source_dir=${1:?usage: macos-sign-notarize.sh SOURCE_DIR DIST_DIR darwin-ARCH VERSION}
dist_dir=${2:?missing output directory}
platform=${3:?missing platform}
version=${4:?missing version}
[[ $platform == darwin-arm64 || $platform == darwin-x64 ]] || fail "invalid macOS release platform: $platform"
credentials_dir=$(mktemp -d "${RUNNER_TEMP:?missing Actions runner temp}/broca-sign.XXXXXX")
keychain="$credentials_dir/developer-id.keychain-db"
keychain_password=$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')
certificate="$credentials_dir/developer-id.p12"
key="$credentials_dir/notary.p8"
python3 - "$certificate" <<'PY'
import base64
import os
from pathlib import Path
import sys

Path(sys.argv[1]).write_bytes(base64.b64decode(os.environ["DEVELOPER_ID_APPLICATION_CERTIFICATE_BASE64"]))
PY
printf '%s' "$APP_STORE_CONNECT_API_KEY" > "$key"
security create-keychain -p "$keychain_password" "$keychain"
security set-keychain-settings -lut 7200 "$keychain"
security unlock-keychain -p "$keychain_password" "$keychain"
security import "$certificate" -k "$keychain" -P "$DEVELOPER_ID_APPLICATION_CERTIFICATE_PASSWORD" -T /usr/bin/codesign
security set-key-partition-list -S apple-tool:,apple:,codesign: -s -k "$keychain_password" "$keychain" >/dev/null
rm -f "$certificate"
unset DEVELOPER_ID_APPLICATION_CERTIFICATE_BASE64 DEVELOPER_ID_APPLICATION_CERTIFICATE_PASSWORD APP_STORE_CONNECT_API_KEY
identity_list=$(security find-identity -v -p codesigning "$keychain")
identity=$(awk '/"Developer ID Application:/ {print $2}' <<< "$identity_list")
[[ $identity =~ ^[[:xdigit:]]{40}$ ]] || fail 'the temporary keychain must contain exactly one Developer ID Application identity'

mkdir -p "$dist_dir"
dist_dir=$(cd "$dist_dir" && pwd)
for binary in ck-broca broca-session; do
    executable="$source_dir/$binary"
    archive="$binary-$platform.zip"
    [[ ! -e $dist_dir/$archive ]] || fail "archive already exists: $archive"
    codesign --force --options runtime --timestamp --keychain "$keychain" --sign "$identity" "$executable"
    codesign --verify --strict -R 'anchor apple generic and certificate leaf[field.1.2.840.113635.100.6.1.13] exists' "$executable"
    details=$(codesign --display --verbose=4 "$executable" 2>&1)
    [[ $details =~ flags=[^[:space:]]*\(.*runtime ]] || fail "$binary is not signed with hardened runtime"
    [[ $("$executable" --version) == "$binary $version" ]] || fail "signed $binary does not report version $version"
    zip -q -j "$dist_dir/$archive" "$executable"
    xcrun notarytool submit "$dist_dir/$archive" --key "$key" --key-id "$APP_STORE_CONNECT_API_KEY_ID" \
        --issuer "$APP_STORE_CONNECT_API_ISSUER_ID" --wait --output-format json > "$credentials_dir/result.json"
    python3 - "$credentials_dir/result.json" "$archive" <<'PY'
import json
import sys

with open(sys.argv[1]) as result:
    payload = json.load(result)
if payload.get("status") != "Accepted":
    raise SystemExit(f"notarization for {sys.argv[2]} was not accepted: {payload.get('status')!r} ({payload.get('id')})")
PY
    ( cd "$dist_dir" && shasum -a 256 "$archive" > "$archive.sha256" )
done
BROCA_EXIT_GUARD_FINISHED=1
