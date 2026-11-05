#!/usr/bin/env python3
"""EXIT cleanup must not turn a Bash 3.2 nounset error into success."""

import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
HELPER = Path(os.environ.get("BROCA_TEST_EXIT_GUARD", ROOT / "scripts/lib/exit-guard.sh"))
BASH = "/bin/bash" if Path("/bin/bash").exists() else (shutil.which("bash") or "bash")


def exit_trap_inventory(source):
    # Read command arguments without treating comments or quoted multiline bodies as code.
    commands = re.finditer(
        r'''(?m)^[ \t]*(?P<command>set|trap|source|\.)[ \t]+(?P<args>(?:'[^']*'|"(?:\\.|[^"\\])*"|\\\n|[^'"#;\n])*)''',
        source,
    )
    nounset = False
    sourced = False
    bodies = []
    for match in commands:
        args = shlex.split(match["args"])
        command = match["command"]
        if command == "set":
            for index, arg in enumerate(args):
                if arg == "--":
                    break
                if (arg.startswith("-") and not arg.startswith("--") and "u" in arg[1:]) or (
                    arg == "-o" and args[index + 1:index + 2] == ["nounset"]
                ):
                    nounset = True
        elif command in ("source", "."):
            sourced |= bool(args and args[0].endswith("/exit-guard.sh"))
        elif command == "trap":
            if args[:1] == ["--"]:
                args = args[1:]
            if args and args[0] != "-" and ({"EXIT", "0"} & set(args[1:])):
                bodies.append(args[0])
    return nounset, sourced, bodies


def unguarded_exit_traps(source):
    nounset, sourced, bodies = exit_trap_inventory(source)
    if not nounset or not bodies:
        return []
    if not sourced:
        return ["EXIT trap without sourcing lib/exit-guard.sh"]
    return [f"unguarded EXIT trap: {body}" for body in bodies
            if shlex.split(body)[:1] != ["broca_exit_guard"]]


class ExitGuardStructureTests(unittest.TestCase):
    def test_nounset_exit_traps_are_guarded(self):
        scripts = sorted((ROOT / "scripts").rglob("*.sh"))
        self.assertGreater(len(scripts), 30, "shell inventory unexpectedly empty or truncated")
        guarded = []
        for script in scripts:
            source = script.read_text()
            nounset, _, bodies = exit_trap_inventory(source)
            if nounset and bodies:
                guarded.append(str(script.relative_to(ROOT)))
            with self.subTest(script=str(script.relative_to(ROOT))):
                self.assertEqual(unguarded_exit_traps(source), [])
        self.assertGreaterEqual(len(guarded), 16, "known EXIT traps disappeared from the scan")
        print("EXIT guard inventory: " + ", ".join(guarded))

    def test_scan_detects_unguarded_traps(self):
        for option in ("-u", "-eu", "-euo pipefail", "-o nounset", "-e -u", "-o errexit -o nounset"):
            with self.subTest(option=option):
                self.assertTrue(unguarded_exit_traps(f"set {option}\ntrap 'true' EXIT\n"))

    def test_scan_requires_source_and_guard_on_each_trap(self):
        self.assertTrue(unguarded_exit_traps("set -u\ntrap 'broca_exit_guard true' EXIT\n"))
        self.assertTrue(unguarded_exit_traps(
            "set -u\nsource lib/exit-guard.sh\ntrap 'broca_exit_guard true' EXIT\n"
            "(\n  trap 'true' EXIT\n)\n"
        ))

    def test_scan_ignores_comments_and_trap_removal(self):
        self.assertEqual(unguarded_exit_traps("set -u\n# trap 'true' EXIT\ntrap - EXIT\n"), [])

    def test_scan_detects_multiline_traps_and_numeric_exit(self):
        for trap in ("trap 'true\ntrue' EXIT", "trap -- 'true' 0", "trap true INT EXIT TERM"):
            with self.subTest(trap=trap):
                self.assertTrue(unguarded_exit_traps("set -u\n" + trap))

    def test_scan_rejects_helper_mentions_without_use(self):
        self.assertTrue(unguarded_exit_traps(
            "set -u\n# source lib/exit-guard.sh\ntrap 'broca_exit_guard true' EXIT\n"
        ))
        self.assertTrue(unguarded_exit_traps(
            "set -u\nsource lib/exit-guard.sh\ntrap 'echo broca_exit_guard' EXIT\n"
        ))


class WorkflowSourcedGateTests(unittest.TestCase):
    """A CI step that sources the gate inherits its guarded exit trap, so the step must mark
    itself done when it ends, or clear the trap; otherwise a green step exits 1."""

    def test_steps_sourcing_the_gate_mark_completion(self):
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        blocks = re.findall(r"run: \|\n((?:[ ]{10}.*\n|\n)+)", workflow)
        sourcing = [block for block in blocks if "source scripts/rust-test-gate.sh" in block]
        self.assertGreaterEqual(len(sourcing), 2, "steps sourcing the gate disappeared from the scan")
        for block in sourcing:
            with self.subTest(step=block.strip().splitlines()[0]):
                lines = [line.strip() for line in block.splitlines() if line.strip()]
                self.assertTrue(
                    lines[-1] == "BROCA_EXIT_GUARD_FINISHED=1" or "trap - EXIT" in lines,
                    "a step that sources scripts/rust-test-gate.sh must end with "
                    "BROCA_EXIT_GUARD_FINISHED=1 or clear the trap",
                )

    def test_a_sourced_gate_fails_an_unmarked_step_and_passes_a_marked_one(self):
        for marker, expected in (("", 1), ("BROCA_EXIT_GUARD_FINISHED=1", 0)):
            with self.subTest(marker=marker or "unmarked"):
                result = subprocess.run(
                    [BASH, "-c", f"source scripts/rust-test-gate.sh\nrun_phase probe true\n{marker}"],
                    cwd=ROOT, capture_output=True, text=True,
                )
                self.assertEqual(result.returncode, expected, result.stdout + result.stderr)


class ExitGuardBehaviorTests(unittest.TestCase):
    def run_script(self, body, cleanup="printf 'cleaned\\n'"):
        scratch_root = ROOT / "target/scratch"
        scratch_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="broca-exit-guard-", dir=scratch_root) as scratch:
            script = Path(scratch) / "guarded.sh"
            script.write_text(
                'set -euo pipefail\nsource "$1"\n'
                f"cleanup() {{ {cleanup}; }}\n"
                "trap 'broca_exit_guard cleanup' EXIT\n" + body + "\n"
            )
            result = subprocess.run(
                [BASH, str(script), str(HELPER)], capture_output=True, text=True
            )
        self.assertIn("cleaned\n", result.stdout)
        return result

    def test_unbound_variable_fails_after_cleanup(self):
        result = self.run_script('printf "%s\\n" "$broca_missing_variable"')
        self.assertNotEqual(result.returncode, 0, result.stderr)
        self.assertIn("broca_missing_variable", result.stderr)

    def test_finished_fallthrough_succeeds(self):
        result = self.run_script("BROCA_EXIT_GUARD_FINISHED=1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")

    def test_finished_explicit_exit_succeeds(self):
        result = self.run_script("BROCA_EXIT_GUARD_FINISHED=1\nexit 0")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_unfinished_success_names_script(self):
        result = self.run_script(":")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(len(result.stderr.splitlines()), 1)
        self.assertIn("guarded.sh: exited before successful completion", result.stderr)

    def test_failure_keeps_status(self):
        for status in (1, 23, 130, 143):
            with self.subTest(status=status):
                result = self.run_script(f"exit {status}")
                self.assertEqual(result.returncode, status, result.stderr)

    def test_cleanup_failure_keeps_failure(self):
        for body, expected in (("BROCA_EXIT_GUARD_FINISHED=1", 19), ("exit 23", 23)):
            with self.subTest(body=body):
                result = self.run_script(body, "printf 'cleaned\\n'; return 19")
                self.assertEqual(result.returncode, expected, result.stderr)


if __name__ == "__main__":
    unittest.main()
