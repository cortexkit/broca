#!/usr/bin/env bash
# Hand-written Rust source stays under 5,000 lines, so a one-line edit never rebuilds one huge
# unit (workspace rule "Keep builds fast"). A file already larger is listed in
# scripts/file-size-allowlist.txt as `<lines> <path> <reason>`: the line count it had when the
# rule began, which it may not grow past, and why it is still that large. Split it by area when
# you next work in it, then lower or drop its entry.
#
# Test code is exempt: files under a tests/ folder and files named tests.rs, *_tests.rs or
# test_*.rs. Inline `mod tests` blocks still count, because they compile into the library.
#
# A limit can only come down: an entry the committed allowlist (HEAD's) lacks, or a limit above
# its committed one, fails, since either would let a file grow past 5,000 lines. Where HEAD has no
# allowlist yet, every entry is accepted.
#
# Exit 0 when every file is within its limit, 1 naming each file that is not or each raised
# limit, 2 on a malformed allowlist line. Plain bash 3.2 and awk, so it runs as macOS ships them.
set -uo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root" || exit 2
allowlist=scripts/file-size-allowlist.txt
[ -f "$allowlist" ] || { echo "check-file-sizes: $allowlist is missing" >&2; exit 2; }

# Untracked files count too: the commit gate runs this before it adds them.
counts=$(git ls-files -z --cached --others --exclude-standard -- 'crates/*.rs' | sort -zu | while IFS= read -r -d '' path; do
  case "$path" in
    (*/tests/*|*/tests.rs|*_tests.rs|*/test_*.rs) continue ;;
  esac
  [ -f "$path" ] && printf '%s %s\n' "$(wc -l < "$path" | tr -d ' ')" "$path"
done)

source "$root/scripts/lib/exit-guard.sh"
committed=$(mktemp "${TMPDIR:-/tmp}/file-size-committed.XXXXXX") || exit 2
trap 'broca_exit_guard rm -f "$committed"' EXIT
git show "HEAD:$allowlist" > "$committed" 2>/dev/null || : > "$committed"

printf '%s\n' "$counts" | awk -v allowlist="$allowlist" -v committed="$committed" -v limit=5000 '
  BEGIN {
    while ((getline line < committed) > 0) {
      if (line ~ /^[[:space:]]*(#|$)/) continue
      split(line, field, " ")
      if (field[1] ~ /^[0-9]+$/) { before[field[2]] = field[1] + 0; had = 1 }
    }
    while ((getline line < allowlist) > 0) {
      if (line ~ /^[[:space:]]*(#|$)/) continue
      n = split(line, field, " ")
      if (n < 3 || field[1] !~ /^[0-9]+$/) {
        print "check-file-sizes: malformed line in " allowlist ": " line > "/dev/stderr"
        malformed = 1
        continue
      }
      allowed[field[2]] = field[1] + 0
      if (had && !(field[2] in before)) {
        print "check-file-sizes: " field[2] " is not in the committed allowlist; split the file instead of listing it" > "/dev/stderr"
        failed = 1
      } else if (had && field[1] + 0 > before[field[2]]) {
        print "check-file-sizes: " field[2] " limit raised from " before[field[2]] " to " field[1] "; a limit can only come down" > "/dev/stderr"
        failed = 1
      }
    }
  }
  NF == 2 {
    lines = $1 + 0; path = $2; seen[path] = 1
    max = (path in allowed) ? allowed[path] : limit
    if (lines > max) {
      if (path in allowed)
        print "check-file-sizes: " path " has " lines " lines and may not grow past " max "; move code out of it" > "/dev/stderr"
      else
        print "check-file-sizes: " path " has " lines " lines, over the " limit "-line limit; split it by area" > "/dev/stderr"
      failed = 1
    } else if ((path in allowed) && lines < max) {
      print "check-file-sizes: " path " shrank to " lines " lines; lower its entry in " allowlist
    }
  }
  END {
    for (path in allowed)
      if (!(path in seen))
        print "check-file-sizes: " path " is gone or exempt; drop its entry from " allowlist
    if (malformed) exit 2
    exit failed
  }'
status=$?
BROCA_EXIT_GUARD_FINISHED=1
exit "$status"
