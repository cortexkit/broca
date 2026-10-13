#!/usr/bin/env bash
# Shared process/image probe for place-release and verify-deploy.
BROCA_DEPLOY_HOST="$(uname -s)"

# Keep format, signing applicability and image identity on one host dispatch. Linux identity
# includes the device: inode numbers alone can coincide on different filesystems.
broca_file_inode() {
  case "$BROCA_DEPLOY_HOST" in
    Darwin) stat -Lf '%i' "$1" 2>/dev/null ;;
    Linux) stat -Lc '%d:%i' "$1" 2>/dev/null ;;
    *) return 1 ;;
  esac
}

broca_release_kind() {
  local kind
  kind="$(file -b "$1" 2>/dev/null)" || return 1
  kind="${kind%%$'\n'*}"
  if [ "$BROCA_DEPLOY_HOST" = Darwin ]; then
    printf '%s\n' "$kind"
    [[ "$kind" == Mach-O* ]]
    return $?
  fi
  case "$BROCA_DEPLOY_HOST:$kind" in
    # Build IDs and debug/strip status vary between ELF releases, unlike class and machine.
    Linux:ELF*executable,*) printf '%s\n' "$kind" | cut -d, -f1-2 ;;
    *) return 1 ;;
  esac
}

broca_running_sha() {
  [ "$BROCA_DEPLOY_HOST" = Linux ] || return 1
  [ -n "$1" ] || return 1
  shasum -a 256 "/proc/$1/exe" | awk '{print $1}'
}

# ps command field 2 is argv0; the final field may instead be a daemon argument.
#
# Both readers consume their input to EOF instead of `exit`ing on the first match. The callers
# run under `set -o pipefail`, and an awk that exits early leaves ps (or lsof) writing into a
# closed pipe: SIGPIPE, pipeline status 141, and the caller dies. That killed place-release
# between placing the binary and restarting the module on 2026-09-26, with a real process
# table long enough to still be writing when awk quit.
broca_pid_for_binary() {
  if [ "$BROCA_DEPLOY_HOST" = Linux ]; then
    # /proc cmdline preserves argv boundaries, including an executable path with spaces.
    # ps supplies the enumeration; an unreadable candidate is unknown, not a missing daemon.
    python3 - "$1" <<'PY'
import os, pathlib, subprocess, sys
try:
    rows = subprocess.run(["ps", "-Ao", "pid=,uid="], check=True, capture_output=True, text=True)
    matches = []
    target = os.fsencode(sys.argv[1])
    for row in rows.stdout.splitlines():
        pid, uid = row.split()
        try:
            argv0 = pathlib.Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0", 1)[0]
        except FileNotFoundError:
            if pathlib.Path(f"/proc/{pid}").exists() and int(uid) == os.geteuid():
                raise  # A still-present process with hidden command data is unknown.
            continue
        except PermissionError:
            if int(uid) == os.geteuid():
                raise
            continue
        if argv0 == target:
            matches.append(pid)
    if len(matches) > 1:
        raise ValueError("multiple processes run the requested binary")
    if matches:
        print(matches[0])
except (OSError, ValueError, subprocess.SubprocessError) as error:
    print(f"running-image: unknown process identity: {error}", file=sys.stderr)
    sys.exit(1)
PY
    return $?
  fi
  [ "$BROCA_DEPLOY_HOST" = Darwin ] || return 1
  ps -Ao pid,command | awk -v p="$1" '$2==p && !found {print $1; found=1}'
}

# lsof's NODE column is the inode of the open text image. stat on NAME would instead
# follow a replaced path and report the new file even while the old image is running.
broca_running_inode() {
  local pid="$1"
  [ -n "$pid" ] && ps -p "$pid" >/dev/null 2>&1 || return 1
  if [ "$BROCA_DEPLOY_HOST" = Linux ]; then
    broca_file_inode "/proc/$pid/exe"
    return $?
  fi
  [ "$BROCA_DEPLOY_HOST" = Darwin ] || return 1
  lsof -p "$pid" -a -d txt 2>/dev/null | awk '
    NR==1 {for (i=1; i<=NF; i++) if ($i=="NODE") node=i; next}
    node && !found && $4=="txt" && $NF ~ /ck-broca$/ {print $node; found=1}
  '
}
