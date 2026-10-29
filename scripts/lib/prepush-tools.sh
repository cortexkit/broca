#!/usr/bin/env bash
# CI installs these runtimes through setup actions; write runtime.path for the caller to load.
set -euo pipefail
lane="${1:?machine lane required}"
scratch="${2:?scratch directory required}"
fetch_tool() {
  local url="$1" checksum="$2" output="$3"
  curl --fail --location --silent --show-error "$url" -o "$output"
  printf '%s  %s\n' "$checksum" "$output" | sha256sum --check -
}

if [[ "$lane" == windows ]] && ! command -v python3 >/dev/null; then
  python_dir="$scratch/python"
  mkdir -p "$python_dir"
  fetch_tool 'https://www.python.org/ftp/python/3.13.5/python-3.13.5-embed-amd64.zip' \
    7d2650fd9d1b9d002d4a315d5f354247fd6a44f30517c7ef577b08f57a0fb6d9 "$scratch/python.zip"
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
    fetch_tool 'https://nodejs.org/dist/v20.20.2/node-v20.20.2-win-x64.zip' \
      dc3700fdd57a63eedb8fd7e3c7baaa32e6a740a1b904167ff4204bc68ed8bf77 "$scratch/node.zip"
    powershell.exe -NoProfile -NonInteractive -Command \
      "Expand-Archive -LiteralPath '$(cygpath -w "$scratch/node.zip")' -DestinationPath '$(cygpath -w "$scratch")'"
    node_path="$(cygpath -u "$scratch/node-v20.20.2-win-x64")"
    export PATH="$node_path:$PATH"
  else
    fetch_tool 'https://nodejs.org/dist/v20.20.2/node-v20.20.2-linux-x64.tar.xz' \
      df770b2a6f130ed8627c9782c988fda9669fa23898329a61a871e32f965e007d "$scratch/node.tar.xz"
    tar -xJf "$scratch/node.tar.xz" -C "$scratch"
    export PATH="$scratch/node-v20.20.2-linux-x64/bin:$PATH"
  fi
fi
python3 --version
node --version
npm --version
printf '%s\n' "$PATH" > "$scratch/runtime.path"
