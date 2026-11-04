#!/usr/bin/env bash
# Output collection must also work before Python has been provisioned on Windows.
first_errors() {
  local path="$1" line count=0 found=0 context=0
  local initial=()
  while IFS= read -r line || [[ -n "$line" ]]; do
    count=$((count + 1))
    [[ $count -gt 8 ]] || initial+=("$line")
    if [[ $found == 0 ]]; then
      case "$line" in
        *error:*|*error\[*|*panicked*|*FAIL*|*refus*|*unsupported*|*not\ found*|*Traceback*) found=1 ;;
      esac
    fi
    if [[ $found == 1 ]]; then
      printf '%s\n' "$line"
      context=$((context + 1))
      [[ $context -lt 12 ]] || return 0
    fi
  done < "$path"
  if [[ $found == 0 ]]; then
    if [[ $count == 0 ]]; then
      echo '(no output captured)'
    else
      printf '%s\n' "${initial[@]}"
    fi
  fi
}

dispatch_state() {
  case "$1" in
    0) echo PASS ;;
    200|204) echo NOT-RUN ;;
    201) echo UNKNOWN ;;
    202) echo TIMED-OUT ;;
    203) echo CANCELLED ;;
    *) echo FAILED ;;
  esac
}
