#!/usr/bin/env bash
# GNU tar interprets a Windows drive's colon as a remote-host archive. Convert
# native runner paths with Git Bash's cygpath before handing them to tar.
set -euo pipefail
[[ "$#" -eq 2 ]] || { echo 'usage: unpack-fixtures.sh ARCHIVE DESTINATION' >&2; exit 2; }
archive="$1"
destination="$2"
if [[ "${RUNNER_OS:-}" == Windows ]]; then
  archive="$(cygpath -u "$archive")"
  destination="$(cygpath -u "$destination")"
fi
echo "unpack-fixtures: $archive -> $destination"
tar -xzf "$archive" -C "$destination"
