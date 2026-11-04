#!/usr/bin/env python3
"""Build declarations and exact tool-result checks shared by live acceptance turns."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

NO_PROJECT = "bind-no-project/v1"
UNSCOPED_SESSION_ROOT = "broca-unscoped-v1"
PROJECT_REQUIRED = ("tool/route rejected: project_required — This connection has no project folder, "
                    "so AFT cannot run project tools on it.")


def bind_no_project(description):
    capabilities = description.get("capabilities", [])
    if isinstance(capabilities, dict):
        capabilities = capabilities.get("provides", [])
    if NO_PROJECT in capabilities:
        return True
    # The capability is currently in the HELLO manifest, not role.describe.
    # Read the placed artifact that verify-deploy checks against the running inode;
    # a release-specific marker or version cannot establish its bind semantics.
    binary = os.environ.get("BROCA_BIN") or Path.home() / ".local/share/cortexkit/bin/ck-broca"
    helper = Path(__file__).with_name("artifact-probe.sh")
    try:
        result = subprocess.run(["bash", "-c", 'source "$1" && broca_artifact_hits "$2" "$3"',
                                 "acceptance-probe", str(helper), str(binary), NO_PROJECT],
                                capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ValueError("cannot probe placed binary for bind-no-project/v1") from error
    counts = result.stdout.split()
    if result.returncode or len(counts) != 2 or not all(count.isascii() and count.isdigit() for count in counts):
        raise ValueError("cannot probe placed binary for bind-no-project/v1")
    # A declaration is a string constant. A symbol with a similar name is not one.
    return int(counts[0]) > 0


def tool_results_pass(text, no_project):
    results = [line for line in text.splitlines() if line.startswith("TOOL_RESULT ")]
    if not results:
        return False
    for line in results:
        result = re.fullmatch(r"TOOL_RESULT seq=\d+ id=\S+ is_error=(true|false) text=(.*)", line)
        if result is None:
            return False
        if result[1] == "true" and not (no_project and result[2] == PROJECT_REQUIRED):
            return False
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    probe = sub.add_parser("probe")
    probe.add_argument("description", type=Path)
    tool = sub.add_parser("tool-result")
    tool.add_argument("transcript", type=Path)
    tool.add_argument("no_project", choices=("0", "1"))
    args = parser.parse_args()
    try:
        if args.command == "probe":
            description = json.loads(args.description.read_text())["result"]
            print(int(bind_no_project(description)))
            return 0
        return 0 if tool_results_pass(args.transcript.read_text(), args.no_project == "1") else 1
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"acceptance-turn: declaration/result check failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
