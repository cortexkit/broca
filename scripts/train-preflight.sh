#!/usr/bin/env bash
# Run cheap, fixture-independent checks on Linux via `ck motor run`.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
command -v ck >/dev/null || { echo 'train-preflight: ck motor run is required; no local fallback' >&2; exit 200; }
echo 'train-preflight: remote Linux fmt/clippy and fixture-independent tests; no local fallback'
ck motor run --vcpus 8 --timeout 3600 --cwd "$root" -- 'bash scripts/linux-preflight.sh'
