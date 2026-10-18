#!/usr/bin/env python3
"""Executable arms for stage-release.sh's staging sequence.

WHY THESE EXIST. iceteaSA found (issue #20) that the script installed the artifact into the
SHARED SEAM and only then signed it. `codesign` is macOS-only, so on Linux it exits 127, and
`set -e` aborted AFTER the artifact was in the seam and BEFORE the sha, the sidecar, and the
readback that exists to catch exactly that. The residue resolves by the name a placer looks
up, unsigned, with nothing to check it against -- in a directory whose own comment says it is
never age-swept because a sweeper cannot tell residue from a live artifact.

The fix is ordering, not a condition: sign and hash in the private scratch, publish into the
seam by rename. That also covers the case a `command -v codesign` guard would not -- codesign
PRESENT AND FAILING, which aborts at the same line with the same residue and is the macOS
failure nobody would think to test.

THESE ARMS EXTRACT THE REAL BLOCK FROM THE REAL SCRIPT rather than restating it. A copy of the
sequence proves the copy. The block is located by its own markers, so an edit that reorders the
publish is an edit these arms run.

WHAT IS ASSERTED, and why: the observable is THE SEAM DIRECTORY'S CONTENTS after a failure --
not the exit code, which is nonzero in both the fixed and broken versions. That is the whole
point: the broken version also failed loudly, and still left the file behind.
"""

import hashlib
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "stage-release.sh"


def staging_block():
    """The real publish sequence, lifted from the real script.

    From the SEAM assignment through the readback. Raises rather than returning a partial
    block, because a silently-truncated extract would test a fragment and pass.
    """
    src = SCRIPT.read_text()
    start = src.index('SEAM="$HOME/.local/share/cortexkit/staging"')
    end = src.index("# THE CURRENT-ARTIFACT MANIFEST")
    block = src[start:end]
    # Guard against extracting something that no longer contains what these arms are about.
    # These are present in BOTH the fixed and the pre-fix versions on purpose. A guard naming
    # something only the FIX has (`STAGING_TMP`) would make the mutation below fail with
    # "extraction is wrong" instead of "the seam has residue" -- a red test that demonstrates
    # nothing, which is how a mutation stops being evidence.
    for required in ("codesign", "shasum -c", "$SEAM"):
        if required not in block:
            raise AssertionError(
                f"the extracted block has no {required!r}; these arms would pass vacuously"
            )
    return block


def strip_block():
    """The real stripped-binary check, lifted from the real script.

    From its heading comment up to the seam block that `staging_block` starts with, so running
    the two back to back is the script's own order: check, then sign and publish.
    """
    src = SCRIPT.read_text()
    start = src.index("# REFUSE AN UNSTRIPPED RELEASE BINARY")
    end = src.index("# NAMED BY COMMIT, IN THE SEAM THE PLACER READS.")
    # The check only means something AFTER the build that produced the binary and BEFORE the
    # seam is touched; the end marker pins the second, this pins the first.
    if not src.index("cargo build --release") < start < end:
        raise AssertionError("the stripped-binary check is no longer between the build and the seam")
    block = src[start:end]
    for required in ("nm -a", 'debug_map_entries "$BUILT"', 'debug_map_entries "$STRIP_CONTROL"'):
        if required not in block:
            raise AssertionError(
                f"the extracted block has no {required!r}; these arms would pass vacuously"
            )
    return block


def dsym_block():
    """The real Darwin dSYM extraction and strip sequence, without invoking Cargo."""
    src = SCRIPT.read_text()
    start = src.index("# Save line tables before stripping the Mach-O.")
    end = src.index("# REFUSE AN UNSTRIPPED RELEASE BINARY", start)
    block = src[start:end]
    for required in ("dsymutil", 'strip -S "$BUILT"', 'broca_dsym_matches_binary "$BUILT" "$BUILT_DSYM"'):
        if required not in block:
            raise AssertionError(f"the dSYM sequence has no {required!r}")
    return block


# Fake `nm` behaviours, keyed by the path nm is asked to read: the script's -g control program
# lives at `strip-control.<pid>` in the scratch area, everything else is the release binary.
# STAB lines have the shape real macOS `nm -a` prints for debug-map entries.
STAB = "0000000000000000 - 00 0000    SO /src/lib.rs\\n"
SYMBOLS = "0000000100000328 T _main\\n                 U _printf\\n"
FAKE_NM = {
    "stripped": (STAB * 2, SYMBOLS, 0),
    "unstripped": (STAB * 2, STAB * 3 + SYMBOLS, 0),
    "fails": (STAB * 2, "", 1),
    # Prints symbols but never a debug-map entry, for any file: the counter cannot see them.
    "blind": (SYMBOLS, SYMBOLS, 0),
}


def write_fake_nm(binroot, mode):
    control, binary, binary_exit = FAKE_NM[mode]
    failing = "echo 'nm: error: truncated or malformed object' >&2; " if binary_exit else ""
    (binroot / "nm").write_text(
        "#!/bin/sh\n"
        'printf "nm|%s\\n" "$2" >> "$TOOL_LOG"\n'
        'case "$2" in\n'
        f"  *strip-control*) printf '{control}' ;;\n"
        f"  *) {failing}printf '{binary}'; exit {binary_exit} ;;\n"
        "esac\n"
    )
    # A compiler that writes its -o target, so the control program exists for nm to read.
    (binroot / "cc").write_text(
        "#!/bin/sh\n"
        'out=""\n'
        'while [ $# -gt 0 ]; do [ "$1" = -o ] && out=$2; shift; done\n'
        'printf "OBJ\\n" > "$out"\n'
    )


def run_staging(
    tmp, *, uname, codesign, built_bytes=b"BINARY", nm=None,
    instrument_publish=False, corrupt_copy=False, debug_info=False, uuid_mode="matched",
):
    """Run the extracted block against a scratch seam with stubbed tools.

    `uname` and `codesign` are stub behaviours, not the host's. Every arm therefore runs
    identically on macOS and Linux -- which matters because the defect is a platform
    difference, and an arm that only reproduces on one platform is an arm that does not run
    in CI on the other.
    """
    seam = tmp / "home" / ".local" / "share" / "cortexkit" / "staging"
    scratch = tmp / "scratch"
    scratch.mkdir(parents=True, exist_ok=True)
    # AN EXCLUSIVE PATH, not a prepended one. With the host's PATH still on the end, "missing"
    # finds /usr/bin/codesign on any macOS box -- the arm would pass for the wrong reason here
    # and fail on a Linux runner, which is the inverse of what it is testing. So the stub dir
    # is the whole PATH, and the real tools the block needs are linked into it by name.
    binroot = tmp / "stub"
    binroot.mkdir(exist_ok=True)
    for tool in ("mkdir", "install", "shasum", "cut", "chmod", "cp", "wc", "cmp", "mv", "basename", "rm", "cat", "awk", "diff", "sort"):
        found = shutil.which(tool)
        if found:
            (binroot / tool).symlink_to(found)

    (binroot / "uname").write_text(f'#!/bin/sh\nprintf "%s\\n" "{uname}"\n')
    if instrument_publish:
        (binroot / "cp").unlink()
        (binroot / "mv").unlink()
        (binroot / "cp").write_text(
            "#!/bin/sh\n"
            'set -eu\n'
            '"$REAL_CP" "$@"\n'
            'if [ "$1" = -R ]; then dest=$3; else dest=$2; fi\n'
            'case "$dest" in *.incoming) ;; *) exit 0 ;; esac\n'
            'final=${dest%.incoming}\n'
            '[ ! -e "$final" ] || { echo "final exists before verified rename: $final" >&2; exit 91; }\n'
            'case "$dest" in *.sha256.postsign.incoming) label=sidecar ;; *.dSYM.incoming) label=dsym ;; *.incoming) label=artifact ;; *) exit 92 ;; esac\n'
            'printf "copy|%s\\n" "$label" >> "$PUBLISH_LOG"\n'
            'if [ "$CORRUPT_COPY" = 1 ] && [ ! -d "$dest" ]; then printf corrupt >> "$dest"; fi\n'
        )
        (binroot / "mv").write_text(
            "#!/bin/sh\n"
            'set -eu\n'
            'if [ "$#" = 3 ]; then [ "$1" = -f ] || exit 93; source=$2; target=$3; else source=$1; target=$2; fi\n'
            'case "$target" in\n'
            '  *.sha256.postsign) incoming="${target}.incoming"; final=$target; original="$SCRATCH/ck-broca.deadbeef.incoming.sha256.postsign"; label=sidecar ;;\n'
            '  *.dSYM) incoming="${target}.incoming"; final=$target; original="$SCRATCH/ck-broca.deadbeef.dSYM"; label=dsym ;;\n'
            '  *) incoming="${target}.incoming"; final=$target; original="$SCRATCH/ck-broca.deadbeef.incoming"; label=artifact ;;\n'
            'esac\n'
            '[ "$source" = "$incoming" ] || exit 95\n'
            '[ -e "$source" ] || exit 96\n'
            '[ ! -e "$final" ] || { echo "final exists before rename: $final" >&2; exit 97; }\n'
            'if [ -d "$source" ]; then diff -qr "$source" "$original" >/dev/null || exit 98; else cmp -s "$source" "$original" || { echo "incoming copy was not verified: $source" >&2; exit 98; }; fi\n'
            'printf "rename|%s\\n" "$label" >> "$PUBLISH_LOG"\n'
            'exec "$REAL_MV" "$source" "$target"\n'
        )
        (tmp / "publish.log").write_text("")
    if codesign == "missing":
        pass
    elif codesign == "fails":
        # PRESENT AND FAILING: a broken keychain. This is the case a `command -v` guard
        # admits and does not survive.
        (binroot / "codesign").write_text("#!/bin/sh\necho 'errSecInternalComponent' >&2\nexit 1\n")
    elif codesign == "works":
        # Records its arguments, so an arm can assert what the signing step asked for.
        (binroot / "codesign").write_text(f'#!/bin/sh\necho "$@" >> "{tmp / "codesign.log"}"\nexit 0\n')
    else:
        raise AssertionError(f"unknown codesign stub {codesign!r}")
    if uname == "Darwin":
        (binroot / "dsymutil").write_text(
            "#!/bin/sh\n"
            'printf "dsymutil|%s\\n" "$1" >> "$TOOL_LOG"\n'
            'mkdir -p "$3/Contents/Resources/DWARF"\n'
            'printf "debug symbols\\n" > "$3/Contents/Resources/DWARF/ck-broca"\n'
        )
        (binroot / "strip").write_text(
            "#!/bin/sh\n"
            'printf "strip|%s|%s\\n" "$1" "$2" >> "$TOOL_LOG"\n'
        )
        (binroot / "dwarfdump").write_text(
            "#!/bin/sh\n"
            'path=$2\n'
            'uuid=11111111-2222-3333-4444-555555555555\n'
            'case "$UUID_MODE:$path" in mismatch:*.dSYM*) uuid=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee ;; esac\n'
            'printf "dwarfdump|%s\\n" "$path" >> "$TOOL_LOG"\n'
            'printf "UUID: %s (arm64) %s\\n" "$uuid" "$path"\n'
        )

    # `nm` set: run the stripped-binary check ahead of the publish, as the script does. Unset:
    # the publish alone, which is what the signing arms are about.
    script = staging_block()
    if nm is not None:
        write_fake_nm(binroot, nm)
        script = strip_block() + script
    if debug_info:
        script = dsym_block() + script
    for f in binroot.iterdir():
        if not f.is_symlink():
            f.chmod(0o755)

    built = tmp / "ck-broca.built"
    built.write_bytes(built_bytes)
    if uname == "Darwin":
        dwarf = built.with_name(built.name + ".dSYM") / "Contents" / "Resources" / "DWARF"
        dwarf.mkdir(parents=True)
        (dwarf / "ck-broca").write_text("debug symbols\n")
    tool_log = tmp / "tools.log"
    tool_log.write_text("")

    prelude = f"""
set -euo pipefail
export HOME={tmp / "home"}
export PATH={binroot}
ROOT={ROOT}
source "$ROOT/scripts/dsym.sh"
BUILD_SCRATCH={scratch}
BUILT={built}
BUILT_DSYM={built}.dSYM
STAGING_DSYM={scratch}/ck-broca.deadbeef.dSYM
export SCRATCH={scratch}
export PUBLISH_LOG={tmp / "publish.log"}
export TOOL_LOG={tool_log}
export UUID_MODE={uuid_mode}
export REAL_CP={shutil.which("cp")}
export REAL_MV={shutil.which("mv")}
export CORRUPT_COPY={1 if corrupt_copy else 0}
git() {{ printf 'deadbeef\\n'; }}
"""
    proc = subprocess.run(
        ["bash", "-c", prelude + script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    present = sorted(p.name for p in seam.iterdir()) if seam.is_dir() else []
    return proc, present


class StagingPublishArms(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = pathlib.Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_a_missing_codesign_on_linux_stages_normally(self):
        """Arm 1: Linux has no codesign and does not need one, so staging must COMPLETE.

        This is the case in the issue. It must not merely avoid residue -- it must produce a
        usable artifact with a verifying sidecar, or the fix has traded one broken platform
        for another.
        """
        proc, present = run_staging(self.tmp, uname="Linux", codesign="missing")
        self.assertEqual(proc.returncode, 0, f"stderr: {proc.stderr}")
        self.assertIn("ck-broca.deadbeef", present)
        self.assertIn("ck-broca.deadbeef.sha256.postsign", present)

    def test_a_missing_codesign_on_darwin_refuses_and_leaves_the_seam_empty(self):
        """Arm 2: on Darwin a missing codesign is a broken toolchain, not a non-applicable step.

        The distinction a `command -v` guard cannot make: it would silently stage an unsigned
        artifact here, which is the quiet version of the outcome the issue is about.
        """
        proc, present = run_staging(self.tmp, uname="Darwin", codesign="missing")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(present, [], f"the seam must be untouched, found: {present}")

    def test_a_failing_codesign_leaves_the_seam_empty(self):
        """Arm 3: codesign PRESENT AND FAILING -- the case the suggested guard admits.

        A broken keychain on macOS aborts at the same line the issue names. Before the fix
        this left the artifact in the seam exactly as the Linux case did; the platform was
        never the cause, the ORDERING was.
        """
        proc, present = run_staging(self.tmp, uname="Darwin", codesign="fails")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(present, [], f"the seam must be untouched, found: {present}")

    def test_a_working_codesign_publishes_binary_sidecar_and_dsym(self):
        """Arm 4: a working signer publishes the executable and its verification companions."""
        proc, present = run_staging(self.tmp, uname="Darwin", codesign="works")
        self.assertEqual(proc.returncode, 0, f"stderr: {proc.stderr}")
        self.assertEqual(
            present,
            ["ck-broca.deadbeef", "ck-broca.deadbeef.dSYM", "ck-broca.deadbeef.sha256.postsign"],
        )

        seam = self.tmp / "home" / ".local" / "share" / "cortexkit" / "staging"
        self.assertTrue((seam / "ck-broca.deadbeef.dSYM/Contents/Resources/DWARF/ck-broca").is_file())
        check = subprocess.run(
            ["shasum", "-c", "ck-broca.deadbeef.sha256.postsign"],
            cwd=seam,
            capture_output=True,
            text=True,
        )
        self.assertEqual(check.returncode, 0, f"the sidecar must verify: {check.stdout}{check.stderr}")

    def test_dsym_is_extracted_uuid_checked_then_stripped_and_published(self):
        proc, present = run_staging(
            self.tmp, uname="Darwin", codesign="works", nm="stripped",
            debug_info=True, instrument_publish=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(
            present,
            ["ck-broca.deadbeef", "ck-broca.deadbeef.dSYM", "ck-broca.deadbeef.sha256.postsign"],
        )
        seam = self.tmp / "home" / ".local" / "share" / "cortexkit" / "staging"
        self.assertTrue((seam / "ck-broca.deadbeef.dSYM/Contents/Resources/DWARF/ck-broca").is_file())
        self.assertIn("0 debug-map entries", proc.stdout)

        tools = (self.tmp / "tools.log").read_text().splitlines()
        build = str(self.tmp / "ck-broca.built")
        dsym = next(i for i, line in enumerate(tools) if line == f"dsymutil|{build}")
        stripped = next(i for i, line in enumerate(tools) if line == f"strip|-S|{build}")
        nm_binary = next(i for i, line in enumerate(tools) if line == f"nm|{build}")
        self.assertLess(dsym, stripped, tools)
        self.assertLess(stripped, nm_binary, tools)
        self.assertIn(f"dwarfdump|{build}", tools)
        self.assertIn(f"dwarfdump|{build}.dSYM", tools)
        self.assertEqual(
            (self.tmp / "publish.log").read_text().splitlines(),
            ["copy|sidecar", "rename|sidecar", "copy|dsym", "rename|dsym", "copy|artifact", "rename|artifact"],
        )
        source = SCRIPT.read_text()
        self.assertIn("CARGO_PROFILE_RELEASE_DEBUG=line-tables-only", source)
        self.assertIn("CARGO_PROFILE_RELEASE_STRIP=none", source)

    def test_a_dsym_uuid_mismatch_refuses_before_signing_or_publication(self):
        proc, present = run_staging(
            self.tmp, uname="Darwin", codesign="works", nm="stripped",
            debug_info=True, uuid_mode="mismatch",
        )
        self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("dwarfdump --uuid does not report matching UUIDs", proc.stderr)
        self.assertEqual(present, [])
        self.assertFalse((self.tmp / "codesign.log").exists())

    def test_staging_prune_uses_only_the_planted_seam(self):
        """The stage integration must pass its seam and deployed paths, never production defaults."""
        src = SCRIPT.read_text()
        self.assertIn('scripts/prune-staging.sh" --staging-dir "$SEAM" --placed "$DEPLOYED"', src)
        seam = self.tmp / "home" / ".local/share/cortexkit/staging"
        seam.mkdir(parents=True)
        outside = self.tmp / "outside"
        outside.mkdir()
        stale = seam / "ck-broca.11111111"
        stale.write_bytes(b"old")
        (seam / (stale.name + ".sha256.postsign")).write_text(
            f"{hashlib.sha256(b'old').hexdigest()}  {stale.name}\\n")
        for i in range(3, 9):
            name = f"ck-broca.{i:08x}"
            data = f"release{i}".encode()
            (seam / name).write_bytes(data)
            (seam / (name + ".sha256.postsign")).write_text(
                f"{hashlib.sha256(data).hexdigest()}  {name}\\n")
        current = seam / "ck-broca.22222222"
        current.write_bytes(b"current")
        (seam / (current.name + ".sha256.postsign")).write_text(
            f"{hashlib.sha256(b'current').hexdigest()}  {current.name}\\n")
        (seam / "ck-broca.current").write_text(
            f"{hashlib.sha256(b'current').hexdigest()}  {current.name}  20260101T000000Z\\n")
        placed = self.tmp / "placed"
        placed.write_bytes(b"current")
        foreign = outside / "ck-aft.abcdef12"
        foreign.write_bytes(b"foreign")
        # The pruner keeps the newest releases by mtime. Files written back to back can share an
        # mtime on Linux (the kernel stamps from a coarse clock), so set the order explicitly:
        # the stale release oldest, the current one newest.
        ordered = [stale] + [seam / f"ck-broca.{i:08x}" for i in range(3, 9)] + [current]
        for rank, path in enumerate(ordered, start=1):
            os.utime(path, ns=(rank * 1_000_000_000, rank * 1_000_000_000))
        proc = subprocess.run(
            ["bash", str(ROOT / "scripts" / "prune-staging.sh"), "--staging-dir", str(seam),
             "--placed", str(placed)], capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertFalse(stale.exists())
        self.assertEqual(foreign.read_bytes(), b"foreign")

    def test_stage_prune_failure_warns_without_failing_release(self):
        src = SCRIPT.read_text()
        invocation = src[src.index("if ! prune_output="):src.index("if [[ ! -f $DEPLOYED ]]", src.index("if ! prune_output="))]
        proc = subprocess.run(["bash", "-c", "set -euo pipefail; DEPLOYED=/missing; SEAM=/missing; ROOT='" + str(ROOT) + "'; " + invocation + "echo release-ok"], capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("WARN prune skipped:", proc.stderr)
        self.assertIn("release-ok", proc.stdout)

    def test_final_names_appear_only_after_verified_incoming_renames(self):
        """Copies stay under incoming names until the stub verifies them at rename time."""
        proc, present = run_staging(
            self.tmp, uname="Linux", codesign="missing", instrument_publish=True
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(
            (self.tmp / "publish.log").read_text().splitlines(),
            ["copy|sidecar", "rename|sidecar", "copy|artifact", "rename|artifact"],
        )
        self.assertEqual(present, ["ck-broca.deadbeef", "ck-broca.deadbeef.sha256.postsign"])

    def test_a_corrupted_incoming_copy_is_refused_and_removed(self):
        """A copy that differs from private scratch is never renamed or left behind."""
        proc, present = run_staging(
            self.tmp, uname="Linux", codesign="missing", instrument_publish=True,
            corrupt_copy=True,
        )
        self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("incoming copy failed verification", proc.stderr)
        self.assertEqual((self.tmp / "publish.log").read_text().splitlines(), ["copy|sidecar"])
        self.assertEqual(present, [], f"the seam must have no final names, found: {present}")
        self.assertFalse(any(p.name.endswith(".incoming") for p in (self.tmp / "home").rglob("*")))

    def test_the_sidecar_is_published_before_the_artifact(self):
        """Arm 5: the runtime rename order is sidecar first, artifact second."""
        proc, present = run_staging(
            self.tmp, uname="Linux", codesign="missing", instrument_publish=True
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        events = (self.tmp / "publish.log").read_text().splitlines()
        self.assertEqual(
            events,
            ["copy|sidecar", "rename|sidecar", "copy|artifact", "rename|artifact"],
        )
        self.assertEqual(present, ["ck-broca.deadbeef", "ck-broca.deadbeef.sha256.postsign"])


    def test_staging_signs_with_the_hardened_runtime(self):
        """Arm 6: the staging signature asks for `-o runtime` and keeps its explicit identifier.

        The hardened runtime is what stops a same-user debugger from reading the secret launch
        nonce the daemon gives a module. place-release re-signs and checks the flag on what it places, so this
        arm is the staging half: an artifact handed over hardened, under the same identifier.
        """
        proc, _ = run_staging(self.tmp, uname="Darwin", codesign="works")
        self.assertEqual(proc.returncode, 0, f"stderr: {proc.stderr}")
        calls = (self.tmp / "codesign.log").read_text().splitlines()
        self.assertEqual(len(calls), 1, calls)
        self.assertIn("-o runtime", calls[0])
        self.assertIn("--identifier ck-broca", calls[0])


class StrippedBinaryArms(unittest.TestCase):
    """The release binary must be stripped of debug info before it is signed or published.

    v0.3.170 shipped unstripped because cargo's strip step (rust-objcopy) crashed and cargo only
    warned. These run the real check followed by the real publish with a fake `nm`, and assert
    the SEAM: a refusal that still left an artifact behind would be the failure all over again.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = pathlib.Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def refuse(self, nm):
        proc, present = run_staging(self.tmp, uname="Darwin", codesign="works", nm=nm)
        self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(present, [], f"nothing may be published, found: {present}")
        self.assertFalse((self.tmp / "codesign.log").exists(), "nothing may be signed")
        return proc.stderr

    def test_a_stripped_binary_is_staged(self):
        proc, present = run_staging(self.tmp, uname="Darwin", codesign="works", nm="stripped")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(
            present,
            ["ck-broca.deadbeef", "ck-broca.deadbeef.dSYM", "ck-broca.deadbeef.sha256.postsign"],
        )
        self.assertIn("0 debug-map entries", proc.stdout)
        self.assertIn("the -g control reads 2", proc.stdout)

    def test_an_unstripped_binary_refuses_with_its_count(self):
        stderr = self.refuse("unstripped")
        self.assertIn("is not stripped of debug info", stderr)
        self.assertIn("3 debug-map entries", stderr)
        self.assertIn("rust-objcopy warning", stderr)

    def test_a_failing_nm_refuses_as_an_nm_failure(self):
        """A failing nm prints nothing, which a pipe into `grep -c` would count as 0 -- stripped."""
        stderr = self.refuse("fails")
        self.assertIn("nm -a failed (exit 1) on", stderr)
        self.assertNotIn("debug-map entries", stderr)

    def test_an_nm_that_cannot_see_debug_entries_refuses_on_the_control(self):
        """Without the -g control, an nm that never lists debug-map entries passes every binary."""
        stderr = self.refuse("blind")
        self.assertIn("found 0 debug-map entries in a control program", stderr)


class ReleaseCardFloorArms(unittest.TestCase):
    """The release card prints the WAL feature floor, so the placing seat sees which feature
    names the build must know before place-release enforces it.

    Driven through --probe-only, which prints the card from two existing files without building,
    signing, or writing to the shared staging directory.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = pathlib.Path(self._tmp.name)
        self.staged = self.tmp / "staged"
        self.deployed = self.tmp / "deployed"
        self.staged.write_text("NEWMARKERSTRING\nCONTROLSTRING\n")
        self.deployed.write_text("CONTROLSTRING\n")
        self.state = self.tmp / "state"
        self.state.mkdir()

    def tearDown(self):
        self._tmp.cleanup()

    def card(self):
        proc = subprocess.run(
            ["bash", str(SCRIPT), "--probe-only", str(self.staged),
             "--marker", "NEWMARKERSTRING", "--control", "CONTROLSTRING"],
            env={**os.environ, "BROCA_COMPARE_AGAINST": str(self.deployed),
                 "BROCA_STATE_ROOT": str(self.state)},
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        lines = [l.strip() for l in proc.stdout.splitlines() if l.strip().startswith("floor ")]
        self.assertEqual(len(lines), 1, proc.stdout)
        return lines[0]

    def test_the_card_prints_the_floor_names(self):
        (self.state / "wal-feature-floor").write_text("alpha\nbeta\n")
        line = self.card()
        self.assertIn("alpha, beta", line)
        self.assertIn(str(self.state / "wal-feature-floor"), line)

    def test_the_card_says_none_without_a_floor_file(self):
        self.assertIn("floor    none", self.card())


if __name__ == "__main__":
    # A dropped or renamed arm leaves the run green with fewer tests in it, so the count is pinned.
    anchors = unittest.defaultTestLoader.loadTestsFromModule(__import__(__name__)).countTestCases()
    print(f"stage-release suite anchors: {anchors}", flush=True)
    if anchors != 18:
        raise SystemExit("stage-release suite anchor count changed")
    unittest.main(verbosity=2)
