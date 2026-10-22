#!/usr/bin/env bash
# Build, sign, and stage a release artifact, then report what actually differs from the
# deployed one.
#
# This exists because the ritual was interactive, and an interactive step is a step nobody
# cleaned up after: twelve releases left 267 MB of world-readable versioned binaries in /tmp,
# where another seat's probe found them and measured the wrong thing. Names correct, contents
# genuine, only the location wrong -- which no downstream instrument can detect.
#
# It also measures a discriminator BEFORE you decide to release rather than after. Run late
# it is a formality; run first it is an instrument, and it will tell you when a version bump
# is the only thing that reached the binary.
#
# WHAT IT DELIBERATELY DOES NOT DO: derive the discriminator for you. The first version of
# this script diffed the two binaries' string SETS and reported 1039 differences for a release
# whose daemon was provably unchanged -- independently compiled binaries relay out their
# string tables, so a set difference is dominated by layout noise, and even the human-looking
# entries were concatenated table fragments. One of them ("Column out of bounds") is present
# in BOTH binaries and still showed as staged-only, because it appears standalone in one and
# inside a longer table entry in the other. Naming the string a change introduces is a
# judgement about what changed; the script measures it, and cannot make it.
#
# Usage: scripts/stage-release.sh [--marker STRING] [--gone STRING] [--control STRING] [--keep-days N]
set -euo pipefail

KEEP_DAYS=2
MARKER=""
CONTROL=""
PROBE_ONLY=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --keep-days) KEEP_DAYS=${2:?--keep-days needs a number}; shift 2 ;;
        --marker)    MARKER=${2:?--marker needs a string}; shift 2 ;;
        --control)   CONTROL=${2:?--control needs a string}; shift 2 ;;
    --gone)      GONE=${2:?--gone needs a string}; shift 2 ;;
        --probe-only) PROBE_ONLY=${2:?--probe-only needs an artifact path}; shift 2 ;;
        *) echo "stage-release: unknown option $1" >&2; exit 2 ;;
    esac
done

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
source "$ROOT/scripts/dsym.sh"
# The WAL feature floor is printed on the card so the placing seat sees which feature names the
# build must know; place-release enforces it.
source "$ROOT/scripts/wal-floor.sh"
WAL_FLOOR_FILE=$(broca_wal_floor_path)

# THE BUILD SCRATCH AREA, NOT THE SEAM THE PLACER READS. Age-swept below, so it must never
# be pointed at a shared directory: the sweeper deletes by mtime and cannot tell my residue
# from another seat's rollback artifact.
BUILD_SCRATCH=/tmp/broca-release-staging
# Overridable so the marker logic itself can be exercised against an older artifact. The
# default is the live deployed binary, which is what a real staging run compares against.
DEPLOYED=${BROCA_COMPARE_AGAINST:-$HOME/.local/share/cortexkit/bin/ck-broca}
VERSION=$(awk -F'"' '/^version = "/ {print $2; exit}' Cargo.toml)
[[ -n $VERSION ]] || { echo "stage-release: cannot read version from Cargo.toml" >&2; exit 2; }

# A marker is looked for in BOTH the string table and the symbol table, because a release
# whose change carries no new message has no string to point at. Setting a file mode, adding
# a struct field, changing control flow -- all invisible to `strings`, all visible as a
# monomorphized symbol in `nm`. Searching only strings makes those releases look unchanged,
# which is the same wrong answer as an absent marker and arrives wearing a measurement.
probe_hits() { # <file> <needle>
    # -F IS LOAD-BEARING. The marker is documented as a literal string, and without it grep reads
    # metacharacters as a pattern: a marker like run_index.db also matches run_indexXdb, inflating
    # the staged count and making a marker look discriminating when a literal search would show
    # 1/1 -- no release change at all. That fails in the REASSURANCE direction, certifying an
    # artifact whose marker proves nothing, which is the direction nobody trips over later.
    #
    # verify-deploy.sh's identical probe was fixed the same night this one was not. Same function,
    # same body, one twin repaired -- so if a third copy of this appears, fix all of them.
    #
    # REPORTS THE TABLES SEPARATELY, AND THAT IS THE POINT RATHER THAN DETAIL. This used to echo
    # `s + n`, so a count of 1 could be a string constant or a symbol and the caller could not tell.
    # Measured 2026-09-10: I put `stop_and_seal 1/0` on a release card from this tool; the placing
    # seat read `strings` only, got 0/0, and correctly refused. Both numbers were honest --
    # stop_and_seal survives as a SYMBOL and never appears as a string.
    #
    # A COUNT WITHOUT ITS TABLE MEANS TWO THINGS DEPENDING ON THE READER'S TOOL. That one failed
    # loud (0/0 refuses). The inverse is the dangerous shape: a marker reading 1/0 here and 1/1 in
    # a strings-only check would pass both seats while proving nothing about the artifact.
    local s n listing
    listing=$(strings "$1") || return $?
    s=$(grep -F -c -- "$2" <<<"$listing" || true)
    n=$(nm -a "$1" 2>/dev/null | grep -F -c -- "$2" || true)
    echo "$s $n"
}

# A filesystem clock override can advance deletion deadlines. Refuse it before any
# signing or publication, even when the caller supplies no optional release marker.
refuse_test_clock() {
    local marker="BROCA_TEST_CLOCK_RETENTION_CLOCK_OVERRIDE_V1"
    local control counts control_s control_n artifact_s artifact_n
    control=$(mktemp)
    printf '%s\n' "$marker" > "$control"
    if ! counts=$(probe_hits "$control" "$marker"); then
        rm -f "$control"
        echo "stage-release: REFUSING -- test-clock marker control could not be read." >&2
        return 2
    fi
    rm -f "$control"
    read -r control_s control_n <<<"$counts"
    if [[ $control_s -eq 0 ]]; then
        echo "stage-release: REFUSING -- strings cannot see the planted test-clock marker control." >&2
        return 2
    fi
    if ! counts=$(probe_hits "$1" "$marker"); then
        echo "stage-release: REFUSING -- could not read the artifact for the test-clock check." >&2
        return 2
    fi
    read -r artifact_s artifact_n <<<"$counts"
    if [[ $(( artifact_s + artifact_n )) -gt 0 ]]; then
        echo "stage-release: REFUSING -- artifact contains the test-clock override marker: $marker" >&2
        return 2
    fi
    echo "== test-clock absent (planted strings control reads $control_s)"
}

# THE VERDICT AS ONE FUNCTION: removal assertion, marker/control readings, card lines,
# discriminate refusal. Called at the end of a real staging run and by --probe-only.
report_verdict() {
# THE REMOVAL ASSERTION RUNS BEFORE THE NO-MARKER EXIT, because --marker and --gone
# are independent options and the operator asked for this one explicitly.
#
# It used to sit after that exit, so `--gone X` ALONE built, signed, staged and exited
# 0 without ever testing the removal -- an assertion the caller requested, silently not
# run, while the artifact shipped. The output said no marker was supplied and said
# nothing about the skipped check, which is the worst version: it reads as a considered
# result. I added --gone two hours before this was found, and the parsing treats it
# independently; only the reachability did not.
# A REMOVAL IS ASSERTED BY ITS ABSENCE, AND ABSENCE NEEDS THE OTHER IMAGE AS ITS CONTROL.
#
# A marker asks "did the new thing arrive". Its inverse asks "did the old thing leave",
# and a bare `expect 0` cannot answer it: 0 is what you get when the field is gone, when
# the reader is broken, AND when the string is mistyped. The DEPLOYED count is what makes
# the staged 0 mean something -- same reader, same string, same invocation, one artifact
# answering each way. So this refuses a `--gone` string that is also absent from the
# deployed image, because that pair proves nothing.
if [[ -n "${GONE:-}" ]]; then
    read -r g_new_s g_new_n <<<"$(probe_hits "$STAGED" "$GONE")"
    read -r g_old_s g_old_n <<<"$(probe_hits "$DEPLOYED" "$GONE")"
    g_new=$(( g_new_s + g_new_n ))
    g_old=$(( g_old_s + g_old_n ))
    g_table=strings; [[ $g_old_s -eq 0 ]] && g_table=nm
    printf '   gone    %-40s staged %s / deployed %s  (%s)\n' "$GONE" "$g_new" "$g_old" "$g_table"
    if [[ $g_old -eq 0 ]]; then
        echo "
REMOVAL CANNOT BE PROVEN: '$GONE' is absent from the DEPLOYED image too, so a staged 0
is indistinguishable from a broken reader or a mistyped string. Pick a string the
deployed binary actually carries -- the deployed count IS the control." >&2
        exit 1
    fi
    if [[ $g_new -ne 0 ]]; then
        echo "
STILL PRESENT: '$GONE' reads $g_new in the staged binary. The removal did not take." >&2
        exit 1
    fi
fi

if [[ -z $MARKER ]]; then
    cat <<'EOF'

No --marker given, so nothing has been measured beyond the version.

Name a string this release introduces -- an error message or a format string, not a match-arm
literal (rustc compares those by length and bytes without emitting them) and not a substring
of a number ("0.3.35" appears inside catalog prices like 0.3135). Pair it with --control, a
string present in BOTH, so a marker reading 0/0 is distinguishable from a probe pointed at
the wrong artifact.

A MARKER IS AN IDENTITY CHECK, NOT A FEATURE CHECK, and this matters when picking one.
It proves the placed bytes are the bytes on the card. It does NOT prove the feature works:
a string in a binary proves the string was COMPILED IN, not that the branch printing it is
reachable. A feature literal and a version literal are equally good at the job a marker can
actually do, so when a release adds no new production literal -- a behavioural fix, a
deleted branch, a private function that inlines away -- the version string is not a weaker
marker, it is the HONEST one. The feature is proven by its behaviour or not at all.

If you cannot name a NEW literal, that is not a failure to find a marker; it is a fact
about the release worth stating on the card, so the placing seat knows the identity is
proven and the behaviour is not.
EOF
    exit 0
fi

read -r new_s new_n <<<"$(probe_hits "$STAGED" "$MARKER")"
read -r old_s old_n <<<"$(probe_hits "$DEPLOYED" "$MARKER")"
new_hits=$(( new_s + new_n ))
old_hits=$(( old_s + old_n ))
printf '   marker  %-40s staged %s / deployed %s\n' "$MARKER" "$new_hits" "$old_hits"
printf '     str:%-36s staged %s / deployed %s\n' "$MARKER" "$new_s" "$old_s"
printf '     nm: %-36s staged %s / deployed %s\n' "$MARKER" "$new_n" "$old_n"
if [[ $new_s -eq 0 && $new_n -gt 0 ]]; then
    echo "
   NOTE: this marker is SYMBOL-ONLY. A seat verifying with \`strings\` alone will read 0/0 and
   refuse. Put 'nm:$MARKER' on the card, or pick a string literal the change introduces."
fi

# REFUSE A NON-DISCRIMINATING MARKER BEFORE READING THE CONTROL. This used to sit after
# the control block, so a marker absent from BOTH images fell through to the control
# check, which then chose a table for a marker that had used none, read a string control
# in nm as 0/0, and printed "the probe may be pointed at the wrong file". The probe was
# fine; the marker was a #[cfg(test)] string that never reaches a binary. Two refusals
# sharing exit 1 are distinguished only by their message, and the first one printed is
# the one the operator acts on -- so the more specific diagnosis must run first.
if [[ $new_hits -eq $old_hits ]]; then
    echo "
MARKER DOES NOT DISCRIMINATE: it reads the same in both artifacts ($new_hits/$old_hits in
strings+nm), so it cannot fail and looks like verification. If both are 0 the string is not
in the binary at all (a test-only literal, a comment, an inlined private fn). Pick a
different string -- the version string is an honest identity marker -- or accept that
ck-broca is unchanged and say so to whoever places it." >&2
    exit 1
fi
if [[ -n $CONTROL ]]; then
    # READS THE PAIR, like the marker path above. When probe_hits was split to report the string
    # and symbol tables separately, this twin kept treating its output as ONE number and every
    # control check died with `[[: 1 0: syntax error`. The control is what proves the probe is
    # pointed at the right file, so breaking it removes the check that makes the marker
    # trustworthy -- and it broke LOUDLY only because bash refuses that comparison.
    read -r c_new_s c_new_n <<<"$(probe_hits "$STAGED" "$CONTROL")"
    read -r c_old_s c_old_n <<<"$(probe_hits "$DEPLOYED" "$CONTROL")"
    # CHECK THE CONTROL IN THE TABLE THE MARKER DISCRIMINATED IN. Summing both tables
    # lets a control present only in `strings` vouch for a marker measured by `nm`, which
    # proves the strings reader works and says nothing about the nm reader -- and the
    # control exists precisely to prove THAT probe was pointed at a real file. SUBC found
    # this shape in their gate first; it was here too.
    if [[ $(( new_s - old_s )) -ne 0 ]]; then
        c_new=$c_new_s
        c_old=$c_old_s
        c_table=strings
    else
        c_new=$c_new_n
        c_old=$c_old_n
        c_table=nm
    fi
    printf '   control %-40s staged %s / deployed %s  (%s, the table the marker used)\n' "$CONTROL" "$c_new" "$c_old" "$c_table"
    if [[ $c_new -eq 0 || $c_old -eq 0 ]]; then
        echo "
CONTROL FAILED: a string expected in both artifacts is missing from one, so the marker
reading above is not trustworthy -- the probe may be pointed at the wrong file." >&2
        exit 1
    fi
fi

# PRINT THE CARD LINES READY TO PASTE, WITH THE TABLE ON BOTH.
#
# A marker line naming its table beside a control line that does not is how a
# table-mismatched pair reaches a placing seat: the reader sees `nm:` on one line, no
# prefix on the other, and has no reason to check that the control was read by the same
# instrument. SUBC's gate refused exactly that card. The table belongs on BOTH lines
# because a control in the other table is decorative -- it proves an instrument nobody
# used. Emitting the pair here means the card carries it whether or not I remember.
if [[ -n "${CONTROL:-}" ]]; then
    # SELECT THE COUNTS WITH THE TABLE, NOT JUST THE LABEL.
    #
    # The card named one table and printed the CROSS-TABLE SUM. Measured on real
    # artifacts: marker `projection` reads strings 12/14 and nm 157/143, so the card
    # said "strings:projection staged 169 / deployed 157" -- a number from neither
    # table, and in that case REVERSING the direction of the change. A placing operator
    # following the named table cannot reproduce it, and the card contradicts the
    # per-table diagnostics printed a few lines above.
    #
    # This is the control defect from earlier today, left standing on the marker: I
    # fixed the control to be read in the table the marker discriminated in, and did
    # not notice the marker's own line was summing. A count and its table are one fact
    # and must be selected together.
    m_table=strings; m_new=$new_s; m_old=$old_s
    if [[ $(( new_s - old_s )) -eq 0 ]]; then
        m_table=nm; m_new=$new_n; m_old=$old_n
    fi
    echo "
   CARD LINES, copy both:
      marker   ${m_table}:${MARKER}   staged ${m_new} / deployed ${m_old}
      control  ${c_table}:${CONTROL}   staged ${c_new} / deployed ${c_old}${GONE:+
      gone     ${g_table}:${GONE}   staged ${g_new} / deployed ${g_old}   <- a removal; the deployed count is its control}
      floor    $(broca_wal_floor_summary "$WAL_FLOOR_FILE")   ($WAL_FLOOR_FILE)
   Both carry the table on purpose: a control read by the other instrument proves
   nothing about the one that measured the marker."
fi
}

# --probe-only <artifact>: RUN THE VERDICT AGAINST AN EXISTING ARTIFACT. No build, no sign, no
# seam write. Operationally: re-carding a staged artifact with a better marker should not
# rebuild it (identical bytes, so a rebuild proves nothing and costs 100 s). Structurally: the
# verdict had no test seam -- the dirty-tree and tag guards below are right for a run that
# BUILDS, and they made the verdict untestable without building. This path skips both
# because it has no side effect they protect: it reads two files and prints. Never let it
# grow one.
if [[ -n $PROBE_ONLY ]]; then
    [[ -f $PROBE_ONLY ]] || { echo "stage-release: --probe-only: no such artifact: $PROBE_ONLY" >&2; exit 2; }
    STAGED=$PROBE_ONLY
    refuse_test_clock "$STAGED"
    echo "== probe-only: $(basename "$STAGED") vs deployed $DEPLOYED"
    report_verdict
    exit $?
fi

# Sweep by age rather than by name. A sweeper keyed on the current version cannot remove an
# artifact whose version it does not know, which is how the previous residue survived twelve
# releases: every run only ever knew about itself.
mkdir -p "$BUILD_SCRATCH"
# Owner-only, and re-applied on every run rather than only at creation. `mkdir -p` obeys the
# umask (0022 here, so 0755) and does nothing at all to a directory that already exists, so a
# staging area created once stays world-readable forever. The chmod is unconditional because
# repairing the existing directory is the case that matters -- this script was written because
# another seat found world-readable versioned binaries in shared /tmp, dispatchable by anyone
# who puts /tmp on PATH, and staging them under a predictable path rebuilt that exposure.
chmod 700 "$BUILD_SCRATCH"
# Keep cleanup inside BUILD_SCRATCH. Using STAGE_DIR here could delete release artifacts from
# the shared staging directory, including a copy another user needs for rollback. Sweep each
# dSYM bundle as one directory so its contents are removed with the bundle.
swept=$(find "$BUILD_SCRATCH" -maxdepth 1 \( -type f -o \( -type d -name '*.dSYM' \) \) -mtime "+$KEEP_DAYS" -print -exec rm -rf {} \; 2>/dev/null | wc -l | tr -d ' ')

# Registry checksums and pinned git revisions identify linked dependency contents.
# Refuse any external path dependency before building the release artifact.
python3 scripts/check-path-deps.py || exit 2

echo "== building v$VERSION"
# --locked preserves the registry checksums and git revisions CI verified.
# Update dependency pins and the lock deliberately rather than during staging.
# BUILD PROVENANCE, stamped here and nowhere else.
#
# The module reads these through option_env! gated on CK_BUILD_STAMPED, so a build that does not
# come through this script declares NO provenance rather than something plausible. That gate is
# the design, not ceremony: option_env! sees whatever is in the environment at compile time, and
# an ambient CK_BUILD_REV from any cargo invocation would otherwise mint a claim the build has
# not earned. A wrong sha is worse than a gap, because a deploy gate treats it as authoritative.
#
# A DIRTY TREE IS REFUSED RATHER THAN MARKED, and this replaced a `-dirty` suffix that no longer
# means what it used to.
#
# The suffix said COMMIT-MATCH-ONLY: the sha names an ancestor commit, not these bytes. That was
# a real signal while any string was accepted. Under canonical provenance forms (subc-protocol
# 0.17.0) a 40-hex sha plus a suffix is a FORM ERROR, so the module drops the ENTIRE provenance
# block -- lock digest and wire version with it. The suffix would now destroy more than it
# records, and the artifact would be indistinguishable from an unstamped dev build.
#
# Marking is the wrong repair anyway. A release binary built from uncommitted edits cannot be
# rebuilt by anyone, so the honest fix is not to describe it more carefully but to not produce
# it. Registry checksums and git revisions identify dependency contents; refusing a dirty broca
# tree makes its own source identity just as reproducible.
if [ -n "$(git status --porcelain 2>/dev/null)" ]; then
    echo "stage-release: REFUSING -- the working tree is dirty." >&2
    echo "  A release binary must be rebuildable from a commit, and these bytes are not:" >&2
    git status --porcelain | head -10 | sed 's/^/    /' >&2
    echo "  Commit or stash, then re-run." >&2
    exit 2
fi
# HEAD MUST BE A RELEASE TAG, because this tool BUILDS AND SIGNS on every invocation.
#
# That is the same exposure that placed a binary by accident on the other side of this
# seam: a tool whose side effect you know perfectly well, run while you are attending to
# something else -- here, checking whether a marker or a --gone arm discriminates. The
# artifact it drops is signed, sidecar'd, and named like a release, which is exactly what
# the name-the-exact-path-on-the-card rule exists to protect against. I produced one such
# stray while testing tonight and deleted it by hand; by hand is not a guard.
#
# The fix is structural rather than another flag: a TEST invocation is by construction not
# at a release tag, and a real staging run always is -- the workflow is bump, CI, tag,
# checkout the tag, stage. So this refuses the dangerous case without adding a step to the
# safe one, and there is no default to remember.
#
# It also closes a second hole that cost a superseded release: stage-release stamps
# `git rev-parse HEAD` into the binary's provenance, so staging one commit PAST the tag
# embeds a rev the tag does not name, and the embedded-commit check then fails on an
# artifact that looks fine. v0.3.80 was superseded for exactly that.
HEAD_TAG="$(git describe --exact-match --tags HEAD 2>/dev/null || true)"
if [ -z "$HEAD_TAG" ]; then
    echo "stage-release: REFUSING -- HEAD is not at a release tag." >&2
    echo "  This builds and SIGNS, so a test invocation would drop a signed artifact that" >&2
    echo "  looks placeable into the shared seam. HEAD is $(git rev-parse --short HEAD)$(git log -1 --format=', %s' HEAD | cut -c1-48)" >&2
    echo "  Tag the release first, or 'git checkout v<version>' to stage an existing one." >&2
    exit 2
fi

CK_BUILD_REV="$(git rev-parse HEAD)"
CK_BUILD_LOCK_DIGEST="$(shasum -a 256 Cargo.lock | awk '{print $1}')"
export CK_BUILD_STAMPED=1
export CK_BUILD_REV
export CK_BUILD_LOCK_DIGEST
echo "== provenance   rev  ${CK_BUILD_REV}"
echo "                lock ${CK_BUILD_LOCK_DIGEST}"

# Keep only the line tables needed to map sampled addresses to source locations. Cargo normally
# strips release debug info, so retain it until dsymutil has copied it into a detached bundle.
if [[ $(uname -s) == Darwin ]]; then
    export CARGO_PROFILE_RELEASE_DEBUG=line-tables-only
    export CARGO_PROFILE_RELEASE_STRIP=none
fi

if ! python3 scripts/lib/cargo-watchdog.py --step "build signed release artifact" -- cargo build --release --locked -p broca-module-serve --quiet; then
    cat >&2 <<'DRIFT'
stage-release: build refused under --locked.

The build could not use the committed dependency set. Inspect the Cargo error above;
if the manifests and lock disagree, re-resolve deliberately:

    cargo update -w && git diff Cargo.lock     # see what moved, and why
    # commit, push, and let CI verify the combination BEFORE staging again
DRIFT
    exit 2
fi
BUILT=target/release/ck-broca
[[ -f $BUILT ]] || { echo "stage-release: $BUILT missing after build" >&2; exit 2; }
refuse_test_clock "$BUILT"

# Save line tables before stripping the Mach-O. The symbol table stays in the executable for
# function names; the detached bundle supplies the file and line information for sample/Instruments.
if [[ $(uname -s) == Darwin ]]; then
    BUILT_DSYM="$BUILT.dSYM"
    rm -rf "$BUILT_DSYM"
    if ! dsymutil "$BUILT" -o "$BUILT_DSYM"; then
        echo "stage-release: REFUSING -- dsymutil could not produce $BUILT_DSYM." >&2
        exit 2
    fi
    if ! strip -S "$BUILT"; then
        echo "stage-release: REFUSING -- strip -S could not remove debug entries from $BUILT." >&2
        exit 2
    fi
    if ! broca_dsym_matches_binary "$BUILT" "$BUILT_DSYM"; then
        echo "stage-release: REFUSING -- dwarfdump --uuid does not report matching UUIDs for" >&2
        echo "  the stripped binary $BUILT and its dSYM $BUILT_DSYM." >&2
        exit 2
    fi
    echo "== dSYM         $(basename "$BUILT_DSYM") (UUID matches stripped binary)"
fi
source "$ROOT/scripts/running-image.sh"
if [ "$BROCA_DEPLOY_HOST" = Linux ] && ! broca_release_kind "$BUILT" >/dev/null; then
    echo "stage-release: REFUSING -- the Linux build is not an ELF executable: $BUILT" >&2
    exit 2
fi

# REFUSE AN UNSTRIPPED RELEASE BINARY, before anything is signed or published.
#
# v0.3.170 shipped with its debug info still in it: 4,276 debug-map entries in `nm -a` against 0
# for v0.3.168, and 2.2 MB larger. A global rustc wrapper outside this repo had removed DYLD_*
# from the environment, so the rust-objcopy that cargo runs to strip release debug info crashed
# loading libLLVM.dylib. Cargo printed a warning, reported success, and nothing here looked.
#
# The signal is the debug map: on macOS a linked binary that still carries debug info lists
# stab entries (`nm -a` lines whose second field is `-`), and a stripped one lists none.
#
# A COUNT OF 0 IS ONLY EVIDENCE IF THE COUNTER CAN SEE ENTRIES AT ALL. So the same counter first
# reads a tiny program built with -g in the scratch area and must find entries there; an `nm`
# that prints nothing would otherwise certify every binary as stripped.
#
# NM'S OUTPUT AND EXIT STATUS ARE READ SEPARATELY. In `nm -a f | grep -c ...` the exit status is
# grep's (or, under pipefail, whichever failed), and an nm that could not read the file prints
# nothing -- which counts as 0 entries, the passing answer. A binary nm cannot read is refused as
# such, never treated as stripped.
#
# Darwin only, like the signing step below: stab entries are how Mach-O records debug info. An
# ELF binary keeps DWARF in .debug_* sections that `nm -a` does not list this way, so on Linux
# the control program would read 0 and refuse every staging run without measuring anything.

# debug_map_entries <file>: sets DEBUG_MAP_ENTRIES to the number of debug-map lines in
# `nm -a <file>`. Returns nm's own non-zero exit status, leaving the count unset, when nm fails.
debug_map_entries() {
    local listing rc=0
    DEBUG_MAP_ENTRIES=""
    listing=$(nm -a "$1") || rc=$?
    [[ $rc -eq 0 ]] || return "$rc"
    DEBUG_MAP_ENTRIES=$(awk '$2 == "-" { n++ } END { print n + 0 }' <<<"$listing")
}

if [[ $(uname -s) == Darwin ]]; then
    # Compiled and linked as two steps so the linker writes a debug map pointing at the object
    # file, and the compiler driver does not run dsymutil and leave a .dSYM directory behind.
    # Under the age-swept scratch area, so residue from an interrupted run is swept like the rest.
    STRIP_CONTROL="$BUILD_SCRATCH/strip-control.$$"
    printf 'int main(void) { return 0; }\n' > "$STRIP_CONTROL.c"
    if ! { cc -g -c "$STRIP_CONTROL.c" -o "$STRIP_CONTROL.o" && cc "$STRIP_CONTROL.o" -o "$STRIP_CONTROL"; }; then
        rm -f "$STRIP_CONTROL" "$STRIP_CONTROL.c" "$STRIP_CONTROL.o"
        echo "stage-release: REFUSING -- could not build the -g control program the stripped-binary" >&2
        echo "  check is calibrated on, so that check cannot run. Nothing was signed or staged." >&2
        exit 2
    fi
    control_rc=0
    debug_map_entries "$STRIP_CONTROL" || control_rc=$?
    rm -f "$STRIP_CONTROL" "$STRIP_CONTROL.c" "$STRIP_CONTROL.o"
    if [[ $control_rc -ne 0 ]]; then
        echo "stage-release: REFUSING -- nm -a failed (exit $control_rc) on the -g control program, so the" >&2
        echo "  stripped-binary check cannot run. Nothing was signed or staged." >&2
        exit 2
    fi
    if [[ $DEBUG_MAP_ENTRIES -eq 0 ]]; then
        echo "stage-release: REFUSING -- nm -a found 0 debug-map entries in a control program built with" >&2
        echo "  -g, so it cannot see them, and a 0 on the release binary would prove nothing." >&2
        echo "  Nothing was signed or staged." >&2
        exit 2
    fi
    control_entries=$DEBUG_MAP_ENTRIES

    built_rc=0
    debug_map_entries "$BUILT" || built_rc=$?
    if [[ $built_rc -ne 0 ]]; then
        echo "stage-release: REFUSING -- nm -a failed (exit $built_rc) on $BUILT, so whether it is" >&2
        echo "  stripped of debug info is unknown. A binary nm cannot read is not a stripped one." >&2
        echo "  Nothing was signed or staged." >&2
        exit 2
    fi
    if [[ $DEBUG_MAP_ENTRIES -gt 0 ]]; then
        echo "stage-release: REFUSING -- $BUILT is not stripped of debug info: nm -a lists" >&2
        echo "  $DEBUG_MAP_ENTRIES debug-map entries, and a stripped release binary lists 0." >&2
        echo "  Likely cause: cargo's debug-info strip failed; look for a rust-objcopy warning in the" >&2
        echo "  build log. Nothing was signed or staged." >&2
        exit 2
    fi
    echo "== stripped     0 debug-map entries in $BUILT (the -g control reads $control_entries)"
else
    echo "stage-release: not Darwin -- skipping the stripped-binary check (it counts Mach-O debug-map entries)." >&2
fi

# NAMED BY COMMIT, IN THE SEAM THE PLACER READS.
#
# Both halves were defects, found when a placement refused:
#
# 1. This wrote to a private /tmp area, which I introduced for hygiene after twelve releases
#    left 267 MB of world-readable binaries around. Correct for that problem, and it quietly
#    moved the artifact off the seam the placing seat reads -- invisible until a placement.
# 2. It named the file by VERSION. The seam accumulates releases, so a placer resolving
#    "the ck-broca artifact" by glob has 20 candidates and can select bytes that are not the
#    ones on the card. Commit is unambiguous where version is not: a superseded and re-cut
#    v0.3.68 shares a version with its predecessor and never shares a commit.
#
# NEVER AGE-SWEPT. The scratch area above is mine and is swept; this seam is shared, and a
# sweeper here cannot distinguish my residue from an artifact another seat resolves by name.
SEAM="$HOME/.local/share/cortexkit/staging"
COMMIT=$(git rev-parse --short=8 HEAD)
STAGED="$SEAM/ck-broca.$COMMIT"
STAGED_DSYM="$STAGED.dSYM"
STAGING_DSYM="$BUILD_SCRATCH/ck-broca.$COMMIT.dSYM"
mkdir -p "$SEAM"

if [[ $(uname -s) == Darwin ]]; then
    rm -rf "$STAGING_DSYM"
    if ! cp -R "$BUILT_DSYM" "$STAGING_DSYM"; then
        echo "stage-release: REFUSING -- could not copy the dSYM into private release scratch." >&2
        exit 2
    fi
fi

# SIGN AND HASH PRIVATELY; THE SEAM IS TOUCHED ONLY BY THE TWO RENAMES AT THE END.
#
# This used to install into the seam FIRST and sign in place. iceteaSA found the window
# (issue #20): `set -e` plus an unguarded `codesign` -- macOS-only, exit 127 on Linux --
# aborts AFTER the artifact is in the seam and BEFORE the sha, the sidecar, and the readback
# that exists to catch exactly this. The residue is a file that resolves by the name a placer
# looks up, unsigned, with no sidecar to check it against, in a directory whose own comment
# above says it is never age-swept because a sweeper cannot tell residue from a live artifact.
#
# The guard they suggested fixes the Linux case. It does not fix the class: `codesign` PRESENT
# AND FAILING -- a broken keychain, a revoked identity -- aborts at the same line with the same
# residue, and that is the macOS failure nobody would think to test. So the fix is ordering
# rather than a condition: every step that can fail happens in the private scratch, and the
# seam only ever receives bytes that are already signed, hashed and verified.
STAGING_TMP="$BUILD_SCRATCH/ck-broca.$COMMIT.incoming"
# install -m rather than cp: cp carries the source's mode, which is 0755 for a built binary.
install -m 700 "$BUILT" "$STAGING_TMP"

# SIGNING IS REQUIRED ON DARWIN AND MEANINGLESS ELSEWHERE, so the test is the OS rather than
# `command -v codesign`. Those differ in the case that matters: on macOS a missing codesign is
# a broken toolchain and must refuse, and `command -v` would silently produce an unsigned
# artifact instead -- the same quiet outcome this fix removes, arriving by a different route.
#
# stderr is NOT discarded. It was, and that hid a real signing failure behind the generic
# `set -e` abort; with the signing off the seam, a loud failure here costs nothing.
if [[ $(uname -s) == Darwin ]]; then
    if ! command -v codesign >/dev/null 2>&1; then
        echo "stage-release: codesign is missing on a Darwin host. Refusing to stage an" >&2
        echo "  unsigned artifact; the seam is untouched." >&2
        exit 2
    fi
    # The identifier must be explicit. Without it codesign derives one from the output filename,
    # so signing the same bytes under two names yields two hashes -- and on a rebuild the derived
    # identifier changes, silently revoking the TCC grants attached to the old one.
    #
    # Signed under the TEMP name, and the identifier is what makes that safe: an explicit
    # --identifier means the signature does not depend on the filename, so the later rename
    # cannot invalidate it. Deriving it would have made this reordering a silent corruption.
    #
    # `-o runtime` is the hardened runtime. Without it any process of the same user can attach a
    # debugger to the running module and read the secret launch nonce the daemon handed it.
    # place-release signs its copy again and must pass the same option, because a re-sign
    # replaces this signature whole; its gate checks the flag on the bytes it actually places.
    codesign --force --sign - -o runtime --identifier ck-broca "$STAGING_TMP"
else
    echo "stage-release: not Darwin -- staging unsigned (code signing is a macOS concept)." >&2
fi

if [[ $(uname -s) == Darwin ]] && ! broca_dsym_matches_binary "$STAGING_TMP" "$STAGING_DSYM"; then
    echo "stage-release: REFUSING -- dwarfdump --uuid does not report matching UUIDs for" >&2
    echo "  the signed scratch binary $STAGING_TMP and its dSYM $STAGING_DSYM." >&2
    exit 2
fi

# HASHED AFTER SIGNING, FROM THE BYTES THAT WILL BE MOVED. A sidecar computed from the build
# output proves the build output was fine and says nothing about what landed -- which is the
# whole failure a sidecar exists to catch. The readback below re-verifies at the destination,
# so the rename is covered too.
STAGED_SHA=$(shasum -a 256 "$STAGING_TMP" | cut -d' ' -f1)
# `<sha>  <basename>`, TWO SPACES -- the format `shasum -c` parses.
#
# This wrote a BARE DIGEST LINE until 2026-09-05, and the fleet rule is that a sidecar is
# verified by running `shasum -c` on it rather than by comparing the text. A bare line is refused
# with "no properly formatted SHA checksum lines", so the placing seat compared by hand on
# v0.3.72 -- silently converting a mechanical check into a manual one, in the exact direction
# that keeps passing: the digest WAS correct, so nothing looked wrong.
#
# Basename rather than the full path so the check runs from the seam directory the placer is
# already standing in. Written beside the temp artifact, under the FINAL basename it will name,
# so what gets verified in scratch is byte-identical to what lands.
printf '%s  %s\n' "$STAGED_SHA" "$(basename "$STAGED")" > "$STAGING_TMP.sha256.postsign"
chmod 600 "$STAGING_TMP.sha256.postsign"

# COPY INTO THE SEAM UNDER AN INCOMING NAME, VERIFY THERE, THEN RENAME IN THE SEAM.
#
# $BUILD_SCRATCH is /tmp and the seam is under $HOME, so they may be on different filesystems.
# Copying directly to a final name would expose partial bytes there. Instead, the copy is
# confined to a `.incoming` name in the seam; size and sha256 must match the private original
# before the same-directory rename atomically publishes that file. Failed copies or checks are
# removed while the final name remains untouched.
publish_verified() {
    local source=$1 final=$2 incoming="${2}.incoming"
    local source_size incoming_size source_sha incoming_sha

    if ! cp "$source" "$incoming"; then
        rm -f "$incoming"
        return 1
    fi
    if ! source_size=$(wc -c < "$source") \
        || ! incoming_size=$(wc -c < "$incoming") \
        || ! source_sha=$(shasum -a 256 "$source" | cut -d' ' -f1) \
        || ! incoming_sha=$(shasum -a 256 "$incoming" | cut -d' ' -f1) \
        || [[ "$source_size" != "$incoming_size" || "$source_sha" != "$incoming_sha" ]]; then
        rm -f "$incoming"
        echo "stage-release: incoming copy failed verification: $incoming" >&2
        return 1
    fi
    if ! mv -f "$incoming" "$final"; then
        rm -f "$incoming"
        return 1
    fi
}

# A directory cannot replace a non-empty dSYM bundle with one rename. Verify an incoming copy,
# move any prior bundle aside, then atomically rename the complete new directory into its name.
publish_verified_dsym() {
    local source=$1 final=$2 incoming="${2}.incoming" previous="${2}.previous.$$" attempt=0

    rm -rf "$incoming"
    if ! cp -R "$source" "$incoming" || ! diff -qr "$source" "$incoming" >/dev/null; then
        rm -rf "$incoming"
        echo "stage-release: incoming dSYM copy failed verification: $incoming" >&2
        return 1
    fi
    while [[ -e $previous ]]; do
        attempt=$((attempt + 1))
        previous="${2}.previous.$$.${attempt}"
    done
    if [[ -e $final ]] && ! mv "$final" "$previous"; then
        rm -rf "$incoming"
        return 1
    fi
    if ! mv "$incoming" "$final"; then
        [[ -e $previous ]] && mv "$previous" "$final"
        rm -rf "$incoming"
        return 1
    fi
    [[ -e $previous ]] && rm -rf "$previous" || true
}

# ORDER IS DELIBERATE and the asymmetry is the point. A crash between the two leaves a sidecar
# naming an artifact that is not there, which `shasum -c` refuses loudly and which no by-name
# artifact lookup can find. The other order leaves the artifact a placer WILL find with no
# sidecar to check it against -- which is issue #20's residue, reintroduced at a narrower width.
# Fail toward the loud absence, never toward the quiet presence.
publish_verified "$STAGING_TMP.sha256.postsign" "$STAGED.sha256.postsign"
if [[ $(uname -s) == Darwin ]]; then
    publish_verified_dsym "$STAGING_DSYM" "$STAGED_DSYM"
fi
publish_verified "$STAGING_TMP" "$STAGED"

# PROVE IT IS CONSUMABLE HERE, rather than discovering it at the placement seat. Same class as a
# marker that does not discriminate: an artifact that looks right and cannot be used by the one
# command it exists for. Cheap enough to run every time, and it fails loudly in the direction
# that matters -- refusing to hand over an artifact whose verification would fail downstream.
#
# Run AT THE DESTINATION, after the renames, because that is what makes it a readback: it
# verifies the bytes at rest under the name the placer resolves, so it also covers the moves.
if ! ( cd "$SEAM" && shasum -c "$(basename "$STAGED").sha256.postsign" >/dev/null 2>&1 ); then
    echo "stage-release: the sidecar is not readable by 'shasum -c'. Refusing to hand over an" >&2
    echo "  artifact whose verification command would fail at the placement seat." >&2
    exit 2
fi
if [[ $(uname -s) == Darwin ]] && ! broca_dsym_matches_binary "$STAGED" "$STAGED_DSYM"; then
    echo "stage-release: REFUSING -- dwarfdump --uuid does not report matching UUIDs for" >&2
    echo "  the staged binary $STAGED and its dSYM $STAGED_DSYM." >&2
    exit 2
fi

# THE CURRENT-ARTIFACT MANIFEST. "Newest by mtime" is a fact about the directory; "current" is a
# fact about the hand-off, and only the hand-off side knows it. The seam holds every previous
# release (26 broca artifacts with verifying sidecars on 2026-09-18, all equally placeable to a
# gate that only checks sidecars), so this file is the one statement of which one is armed.
# Overwritten on every stage via rename-into-place, so it can never name two artifacts and can
# never be older than the last hand-off. prune-staging.py reads it so pruning always keeps the
# armed artifact. place-release.sh does NOT read it: the placement gate checks the artifact it is
# handed (file kind, signature, dSYM, smoke run, WAL feature floor), not whether that artifact
# is the current one, so the card must name the exact path printed below. Format is one line,
# shasum-shaped plus a UTC stamp, so `shasum -c` on the first two fields still verifies the
# named artifact.
CURRENT="$SEAM/ck-broca.current"
printf '%s  %s  %s\n' "$STAGED_SHA" "$(basename "$STAGED")" "$(date -u +%Y%m%dT%H%M%SZ)" > "$CURRENT.tmp"
chmod 600 "$CURRENT.tmp"
mv -f "$CURRENT.tmp" "$CURRENT"

echo "== staged"
echo "   path    $STAGED"
echo "   current $CURRENT"
echo "   sidecar $STAGED.sha256.postsign"
[[ $(uname -s) != Darwin ]] || echo "   dSYM    $STAGED_DSYM"
echo "   sha256  $STAGED_SHA"
echo "   floor   $(broca_wal_floor_summary "$WAL_FLOOR_FILE")   ($WAL_FLOOR_FILE)"
echo "           place-release refuses a build whose --wal-known-features does not cover it."
echo "   NAME THIS EXACT PATH ON THE CARD. A hash without a path invites a glob over a"
echo "   shared seam, and the seam holds every previous release."
[[ $swept -gt 0 ]] && echo "   swept   $swept artifact(s) older than ${KEEP_DAYS}d"

# Prune old staging artifacts while retaining the current hand-off, deployed binary, and five
# recent releases; exact-name matching leaves artifacts from other projects untouched.
if ! prune_output=$("$ROOT/scripts/prune-staging.sh" --staging-dir "$SEAM" --placed "$DEPLOYED" 2>&1); then
    prune_reason=${prune_output%%$'\n'*}
    echo "WARN prune skipped: $prune_reason" >&2
fi

if [[ ! -f $DEPLOYED ]]; then
    echo "== no deployed binary to compare against; discriminators unavailable"
    exit 0
fi

echo "== versus the deployed binary"
echo "   deployed  $("$DEPLOYED" --version 2>/dev/null || echo unknown)"
echo "   staged    $("$STAGED" --version 2>/dev/null || echo unknown)"

# probe_hits IS DEFINED HERE, ABOVE EVERY CALLER. Bash executes definitions in
# SEQUENCE, so a function called before its definition simply does not exist -- and
# the failure is masked twice over: the empty command-substitution output is consumed
# by a successful `read`, so `set -e` never sees the error, and the empty variables
# then evaluate as 0 in arithmetic, which lands on the "absent from the deployed
# image" refusal. A valid removal reads as unprovable.
#
# That is exactly what moving the removal block ahead of the no-marker exit did an
# hour ago, and my three-arm test could not have caught it: I extracted the block into
# a harness that DEFINED probe_hits itself, so ordering was supplied by the fixture.
# Keep every definition above both validation branches.

report_verdict
