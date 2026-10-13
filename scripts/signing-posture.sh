#!/usr/bin/env bash
# Shared readers for a binary's code-signing posture, used by place-release (to gate the copy it
# is about to rename into place) and verify-deploy (to check the image the daemon is running).
#
# WHY THE POSTURE MATTERS. The subconscious daemon gives each module it launches a secret launch
# nonce, which the module presents to prove it is the process the daemon started. The only thing
# stopping another process of the same user from attaching a debugger and reading that secret
# out of memory (and then impersonating the module) is the hardened runtime. So a placed ck-broca must carry the `runtime` code-signature
# flag, and must not carry the `com.apple.security.get-task-allow` entitlement, which exists
# precisely to let a debugger attach.
#
# BOTH READERS TAKE A PATH TO THE BYTES THAT MATTER. A flags check on the staged artifact is not
# a check on the placed binary: place-release signs its copy again, and `codesign --force`
# replaces the whole signature, so a staged `runtime` flag does not survive a re-sign that does
# not ask for it. Callers pass the file that will run, never a sibling of it.
#
# Neither function pipes codesign into an early-exiting reader: the output is captured whole and
# matched in the shell, so there is no pipe for a SIGPIPE to kill under `set -o pipefail`.

# Prints the flag names from the CodeDirectory line of `codesign -dv`, comma-separated as
# codesign writes them (for example `adhoc,runtime`). Returns 1, printing nothing, when the
# file is unsigned, missing, or codesign cannot read it: the caller decides whether an unreadable
# posture is a refusal, and it must not be mistaken for an empty flag set.
broca_codesign_flags() {
  local out re
  out="$(codesign -dv "$1" 2>&1)" || return 1
  re='flags=0x[0-9a-fA-F]+\(([^)]*)\)'
  if [[ $out =~ $re ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

# True when a comma-separated flag list (as printed above) contains `runtime`.
broca_flags_have_runtime() {
  case ",$1," in
    *,runtime,*) return 0 ;;
    *) return 1 ;;
  esac
}

# Prints the entitlements codesign reports for a file, as XML (empty when it has none). Returns
# 1 when codesign cannot read them, so an unreadable answer is never read as "no entitlements".
broca_codesign_entitlements() {
  codesign -d --entitlements - --xml "$1" 2>/dev/null
}

# True when an entitlements dump contains the debugger-attach entitlement. Presence alone
# refuses, whatever its value: nothing in broca needs it, so a build carrying it at all was
# signed by a step that should not exist.
broca_entitlements_allow_debugger() {
  case "$1" in
    *com.apple.security.get-task-allow*) return 0 ;;
    *) return 1 ;;
  esac
}
