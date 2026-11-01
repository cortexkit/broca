#!/usr/bin/env bash
# Run a filtered set of tests on Linux, in the same fixed build configuration that
# scripts/linux-quickcheck-attest.sh uses. Iterating with this script keeps one warm build, so
# the final attestation before a commit recompiles nothing new. A different `cargo test`
# selection or feature set would build a second copy of the workspace on the build server.
#
# Usage, on the Linux build server (from an agent's shell tool, `runon: "linux,8c"` sends the
# command to a remote Linux job with 8 CPUs):
#   scripts/linux-test.sh 'package(=broca-module) & test(/restart_writes::scheduler/)'
#
# The argument is a nextest filterset. A filter that selects no test fails. Nothing is
# attested: commit through scripts/gated-commit.sh as usual.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

if [[ "$(uname -s)" != Linux ]]; then
  echo "linux-test: run this on the Linux build server, for example as a remote 8-CPU job" >&2
  exit 2
fi
if [[ $# -ne 1 || -z "${1// }" ]]; then
  echo "usage: $0 '<nextest filterset>'" >&2
  echo "  e.g. $0 'package(=broca-module) & test(/restart_writes::scheduler/)'" >&2
  exit 2
fi

exec python3 scripts/lib/linux-quickcheck.py --tests "$1"
