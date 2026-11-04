#!/usr/bin/env bash
# Linux mirrors CI's Node setup action; Windows uses verified image tools instead.
set -euo pipefail
lane="${1:?machine lane required}"
scratch="${2:?scratch directory required}"
[[ "$lane" == linux* ]] || { echo "prepush-tools: runtime downloads are Linux-only, not $lane" >&2; exit 2; }
source "$(dirname "$0")/prepush-assets.conf"
fetch_tool() {
  local url="$1" checksum="$2" output="$3"
  curl --fail --location --silent --show-error "$url" -o "$output"
  printf '%s  %s\n' "$checksum" "$output" | sha256sum --check -
}

if [[ "$(node --version 2>/dev/null || true)" != v20.* ]]; then
  fetch_tool "$NODE_LINUX_URL" "$NODE_LINUX_SHA256" "$scratch/node.tar.xz"
  tar -xJf "$scratch/node.tar.xz" -C "$scratch"
  export PATH="$scratch/node-v20.20.2-linux-x64/bin:$PATH"
fi
python3 --version
node --version
npm --version
printf '%s\n' "$PATH" > "$scratch/runtime.path"
