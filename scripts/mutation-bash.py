#!/usr/bin/env python3
"""Run catalogue shell commands with an explicit native Bash executable."""
import os
from pathlib import Path
import shutil
import subprocess
import sys


def bash_path():
    configured = os.environ.get("BROCA_MUTATION_BASH")
    if configured:
        path = Path(configured)
        if not path.is_absolute() or not path.is_file():
            raise SystemExit("BROCA_MUTATION_BASH must name an existing absolute Bash executable")
        return str(path)
    if sys.platform == "win32":
        # Rust and CreateProcess both search System32 before the parent's PATH.
        # Refuse rather than accidentally sending a native proof through WSL.
        raise SystemExit("Set BROCA_MUTATION_BASH to the absolute Git Bash executable on Windows")
    bash = shutil.which("bash")
    if bash is None:
        raise SystemExit("Bash is required for mutation command proofs")
    return bash


def main(args):
    bash = bash_path()
    if sys.platform == "win32":
        # Windows has no exec: os.execv spawns a child and exits this process at once, so the
        # caller loses the child's output and exit status. It also joins argv unquoted, so a
        # Bash path under "Program Files" splits at the space. Wait for the child instead.
        return subprocess.run([bash, *args]).returncode
    os.execv(bash, [bash, *args])


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
