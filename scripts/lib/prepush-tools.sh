#!/usr/bin/env bash
# CI installs these runtimes through setup actions; write runtime.path for the caller to load.
set -euo pipefail
lane="${1:?machine lane required}"
scratch="${2:?scratch directory required}"
source "$(dirname "$0")/prepush-assets.conf"
fetch_tool() {
  local url="$1" checksum="$2" output="$3"
  if [[ "$lane" == windows ]]; then
    local input="${BROCA_PREPUSH_INPUTS:?Windows offline input directory required}/${url##*/}"
    [[ -s "$input" ]] || { echo "prepush: missing offline Windows input $input" >&2; return 1; }
    cp "$input" "$output"
  else
    curl --fail --location --silent --show-error "$url" -o "$output"
  fi
  printf '%s  %s\n' "$checksum" "$output" | sha256sum --check -
}

if [[ "$lane" == windows ]] && ! command -v python3 >/dev/null; then
  python_dir="$scratch/python"
  mkdir -p "$python_dir"
  fetch_tool "$PYTHON_WINDOWS_URL" "$PYTHON_WINDOWS_SHA256" "$scratch/python.zip"
  powershell.exe -NoProfile -NonInteractive -Command \
    "Expand-Archive -LiteralPath '$(cygpath -w "$scratch/python.zip")' -DestinationPath '$(cygpath -w "$python_dir")'"
  # Embeddable Python's isolation file would hide PYTHONPATH and sibling script modules.
  rm "$python_dir/python313._pth"
  cp "$python_dir/python.exe" "$python_dir/python3.exe"
  python_path="$(cygpath -u "$python_dir")"
  export PATH="$python_path:$PATH"
fi

if [[ "$(node --version 2>/dev/null || true)" != v20.* ]]; then
  if [[ "$lane" == windows ]]; then
    fetch_tool "$NODE_WINDOWS_URL" "$NODE_WINDOWS_SHA256" "$scratch/node.zip"
    powershell.exe -NoProfile -NonInteractive -Command \
      "Expand-Archive -LiteralPath '$(cygpath -w "$scratch/node.zip")' -DestinationPath '$(cygpath -w "$scratch")'"
    node_path="$(cygpath -u "$scratch/node-v20.20.2-win-x64")"
    export PATH="$node_path:$PATH"
  else
    fetch_tool "$NODE_LINUX_URL" "$NODE_LINUX_SHA256" "$scratch/node.tar.xz"
    tar -xJf "$scratch/node.tar.xz" -C "$scratch"
    export PATH="$scratch/node-v20.20.2-linux-x64/bin:$PATH"
  fi
fi
python3 --version
node --version
npm --version
printf '%s\n' "$PATH" > "$scratch/runtime.path"
