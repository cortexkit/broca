#!/usr/bin/env python3
"""Run catalogue shell commands with an explicit native Bash executable."""
import os
from pathlib import Path
import shutil
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


if __name__ == "__main__":
    bash = bash_path()
    os.execv(bash, [bash, *sys.argv[1:]])
