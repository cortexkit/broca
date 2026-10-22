#!/usr/bin/env python3
"""Report runs that are live right now, read from the WAL.

Answers the one question a deploy needs: will restarting broca cut work in progress? A run cannot
survive a restart -- it is durably sealed as interrupted and nothing re-drives it -- so this must be
checked before a supervisor restart rather than inferred.

WHY NOT AN EXISTING SIGNAL. `run_index` state rows are a derived projection that can lag, and
`health.check` reports `active_sessions`, which counts session SLOTS: a slot stays open while its
session sits idle, so it answers "how many sessions exist" rather than "is anything running". The
health check gains a direct `runs_in_flight` gauge in v0.3.27; until that is deployed, this reads
the same fact from the durable log.

WHY NOT A QUIET-PERIOD HEURISTIC. Two earlier versions of this check inferred idleness from a lack
of recent activity, and both were wrong in the same way: a run waiting on a provider emits nothing
for the whole call, so silence is exactly what a long run produces. One of them declared a healthy
consult abandoned twelve seconds before it completed normally. The rule here is causal instead of
temporal: a run whose last write predates the current process cannot still be running, because the
process that was running it is gone.

Exit 0 when nothing is live, 1 when something is, 2 when the question cannot be answered.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import subprocess
import sys
import time

from lib import state_root, wal_gate
from lib.archive_read import ArchiveRefusal, logical_wals

# Live-only enumeration is intentional: archiving requires 14 idle days and an
# exclusive writer lease, so an archived session cannot hold a live run. Revisit
# this scope if archiving ever permits sessions that still have work in progress.
# A record header appears as bare JSON in the frame body. An occurrence inside message CONTENT is
# escaped (\"type\":\"run_started\"), so the backslashes keep it from matching -- which matters
# because transcripts about this engine legitimately quote these record names.
# Transitions are matched as raw bytes without decoding record JSON. Input selection
# separately verifies framing to resolve the lineage and its tombstone before this pass.
# The tokens are ASCII and a binary frame header cannot contain this literal.
TRANSITION = re.compile(
    rb'"type":"(run_started|run_finished|run_paused|step_started)"'
)

# Enough overlap that a token split across two reads is still seen. The longest token is
# 26 bytes; 64 leaves room without re-scanning meaningful volume.
_OVERLAP = 64
_CHUNK = 4096

# A gated frame (framing version 3, see scripts/lib/wal_gate.py) wraps its record as
# {"requires":[...],"record":{...}}, so the record's own `"type":"..."` token is still bare JSON
# and TRANSITION reads it unchanged. What this adds is noticing when that record, or any later
# one, needs a feature this script does not know: then a newer broca build wrote the file and
# its last transition cannot be trusted to mean what this script thinks it means.
#
# Anchored on the header rather than on the envelope text alone: the version byte 3 sits exactly
# 48 bytes (seq, fence, digest) before the payload's first byte. JSON never contains a raw 0x03
# byte (it must be escaped inside a string), so this cannot match inside a record's content.
GATED = re.compile(rb'\x03.{48}\{"requires":\[([^\]]*)\],"record":', re.DOTALL)
# How far before a transition token its own frame's gated envelope can start: the 49 header
# bytes after the length plus the envelope text, whose `requires` list is a handful of names.
_GATED_REACH = 4096

OPEN = "open"
SEALED = "sealed"
NEWER_WRITER = "newer-writer"


def _unknown_gated_ends(data: bytes, base: int) -> list[int]:
    """Absolute end offsets of gated envelopes in `data` naming a feature this script lacks.

    The end is the byte after `"record":`, i.e. where that frame's record starts.
    """
    ends = []
    for match in GATED.finditer(data):
        names = re.findall(rb'"([^"]*)"', match.group(1))
        if any(name.decode("utf-8", "replace") not in wal_gate.KNOWN_FEATURES for name in names):
            ends.append(base + match.end())
    return ends


def unsealed_by_frame_order(raw: bytes) -> bool:
    """Whole-buffer reference for `tail_state`'s token verdict, kept as its oracle.

    The main scan no longer calls this. It is kept to check that reading backwards
    in chunks gives the same last-transition answer as searching the entire buffer.

    Whether this lineage has work open right now, from its LAST transition alone.

    Two properties matter for a restart decision.

    A pause is not a close. An auth-paused run resumes by continuing the SAME episode
    with no second run_started, so counting starts against finishes-plus-pauses reads a
    resumed run as sealed while it is executing, and a restart approved on that reading
    cuts it. Only the last transition decides: a pause closes, a later step reopens.

    And the answer has a shelf life. This gate is read immediately before a restart, so
    every second spent scanning is a second in which a run can start and be cut by a
    restart this scan just approved. Input selection must verify the whole snapshot
    because legacy WALs may acquire a lineage frame after existing records. This pass
    needs only the deciding transition, without decoding the transcript's JSON.
    """
    for match in TRANSITION.finditer(raw):
        pass
    else:
        match = None
    return _opens(match)


def _opens(match: "re.Match[bytes] | None") -> bool:
    if match is None:
        return False
    # A step belongs to a run, so it reopens one a pause had closed.
    return match.group(1) in (b"run_started", b"step_started")


class WalUnreadable(Exception):
    """A WAL the scan could not read, so its state cannot be decided either way."""

    def __init__(self, path: pathlib.Path, error: OSError) -> None:
        super().__init__(f"cannot read {path.name}: {error}")
        self.path = path


def tail_state(path: pathlib.Path) -> str:
    """Read backwards until the tailmost transition token, and judge on that alone.

    Reading forward means reading everything: the deciding token is at the end, so a
    forward scan pays for the entire history to learn about its last few hundred bytes.

    Returns OPEN or SEALED from that token, or NEWER_WRITER when the token's own frame or any
    frame after it is gated on a feature this script does not know. A newer-writer file is never
    reported as sealed: a newer build's record there may open or close a run in a way this
    script cannot read.
    """
    try:
        size = path.stat().st_size
        if size == 0:
            return SEALED
        with path.open("rb") as handle:
            pos = size
            tail = b""
            # Every block read so far, newest first, so gated frames AFTER the deciding token
            # (in blocks already passed over) are still checked once the token is found.
            blocks: list[bytes] = []
            while pos > 0:
                step = min(_CHUNK, pos)
                pos -= step
                handle.seek(pos)
                block = handle.read(step)
                blocks.append(block)
                # Carry the previous head forward so a token straddling the seam survives.
                tail = block + tail[:_OVERLAP]
                last = None
                for last in TRANSITION.finditer(tail):
                    pass
                if last is not None:
                    token_at = pos + last.start()
                    # Make sure the token's own frame header is in hand, in case the frame is
                    # gated and its envelope began in a block not yet read.
                    reach = max(0, token_at - _GATED_REACH)
                    if reach < pos:
                        handle.seek(reach)
                        blocks.append(handle.read(pos - reach))
                        pos = reach
                    data = b"".join(reversed(blocks))
                    # An envelope ending at the token's record belongs to the token's own frame;
                    # one ending later belongs to a later frame. Either makes the verdict unsafe.
                    if any(end >= token_at - 1 for end in _unknown_gated_ends(data, pos)):
                        return NEWER_WRITER
                    return OPEN if _opens(last) else SEALED
            # No transition at all: the whole file is in hand, and any unknown gated frame in it
            # may be the newer build's own way of starting a run.
            if _unknown_gated_ends(b"".join(reversed(blocks)), 0):
                return NEWER_WRITER
            return SEALED
    except OSError as error:
        # An unreadable WAL is UNKNOWN, never idle. Returning False here would let a file the
        # scan could not open count as a sealed one, so the single failure mode of a restart
        # gate -- approving a restart that cuts a live run -- would be reached by a permission
        # change or a transient IO error rather than by anything about the run itself.
        raise WalUnreadable(path, error) from error


# KNOWN LIMIT, measured rather than assumed: a frame torn mid-write can hide a run that is
# starting. Matching the tailmost COMPLETE token means a trailing `{"type":"run_st` is invisible,
# so a file whose previous state was sealed reads as sealed. Reproduced deliberately -- sealed
# run, then a truncated run_started -- and the gate exits 0.
#
# Deliberately not fixed, because it is not a separate defect: it is one instance of the
# check-then-act window this gate has by construction. A run that starts one microsecond AFTER
# the scan completes is equally invisible and strictly more likely, since the scan takes ~0.4s
# while a frame write takes microseconds. No amount of frame parsing closes that.
#
# The consequence also bounds it. An unfsynced run_started has not been admitted -- admission
# fires at that barrier -- so a restart landing in this window fails a send that had not yet
# started work, rather than cutting a generation already minutes deep. That is the cheapest
# instant at which a run can be lost.
#
# The contract this gate can actually keep is "nothing was live when the scan ran". Deploy on a
# fresh result, not a stale one, and re-run it if anything delays placement.


def _self_test() -> int:
    """Prove the scan refuses an unreadable WAL rather than calling it idle.

    The gate's only catastrophic answer is "no live runs" when there are some, and the
    cheapest route to it was never a wrong parse -- it was a file the scan could not open.
    Kept executable so the guarantee is checked rather than remembered.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        wal_dir = pathlib.Path(tmp) / "wal"
        wal_dir.mkdir()
        live = wal_dir / "aaaa.wal"
        # A run_started with no terminal after it: the shape of an in-flight run.
        live.write_bytes(b'{"type":"run_started","run_id":"r"}\n')

        opens = tail_state(live) == OPEN
        if not opens:
            print("self-test FAILED: an unsealed WAL must read as open", file=sys.stderr)
            return 1

        live.chmod(0o000)
        try:
            tail_state(live)
        except WalUnreadable:
            pass
        else:
            print(
                "self-test FAILED: an unreadable WAL must raise, not report a state",
                file=sys.stderr,
            )
            return 1
        finally:
            live.chmod(0o644)

    print("self-test passed: unsealed reads open, unreadable refuses")
    return 0


class AmbiguousProcess(Exception):
    """The daemon's process cannot be identified: several match, or one of this user's
    processes hides its command line."""


def process_table(proc_root: pathlib.Path | None = None) -> list[tuple[str, str]]:
    """(pid, argv[0]) for every process whose command line can be read.

    On Linux argv[0] comes from /proc/<pid>/cmdline, which keeps argument boundaries, so an
    executable path containing spaces is read exactly. Elsewhere it is the first word of
    `ps`'s command column, the same rule place-release.sh's running-image.sh uses on macOS; an
    executable path with a space is cut there and never matches, and the gate then refuses as
    if broca were not running. `proc_root` lets tests supply a fake /proc.
    """
    root = proc_root or pathlib.Path("/proc")
    if proc_root is not None or (sys.platform.startswith("linux") and root.is_dir()):
        rows = []
        for entry in root.iterdir():
            if not entry.name.isdigit():
                continue
            try:
                raw = (entry / "cmdline").read_bytes()
            except OSError as error:
                # A process that exited since the listing, or another user's, is not the daemon
                # this account supervises. One of this user's that is still running but cannot
                # be read might be, so it is unknown, as in running-image.sh.
                try:
                    owner = entry.stat().st_uid
                except OSError:
                    continue
                if owner == os.geteuid():
                    raise AmbiguousProcess(
                        f"cannot read the command line of pid {entry.name}: {error}"
                    ) from error
                continue
            if raw:
                rows.append((entry.name, os.fsdecode(raw.split(b"\0", 1)[0])))
        return sorted(rows, key=lambda row: int(row[0]))
    listing = subprocess.run(
        ["ps", "-A", "-ww", "-o", "pid=,command="], capture_output=True, text=True, check=False
    ).stdout
    rows = []
    for line in listing.splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[0].isdigit():
            rows.append((fields[0], fields[1]))
    return rows


def matching_pids(pattern: str, proc_root: pathlib.Path | None = None) -> list[str]:
    """PIDs of processes whose EXECUTABLE (argv[0]) matches `pattern`.

    Only argv[0] is tested. Matching anywhere in the command line, as `pgrep -f` does, also
    counted processes that merely name the daemon's path in an argument: the rollback
    `place-release.sh ... --dest <path>/bin/ck-broca --restart` that deploy.sh prints, a
    `tail` of a log beside the binary, or this script run with --process. That second match
    made the restart gate refuse, so the rollback refused itself.
    """
    own = str(os.getpid())
    return [
        pid
        for pid, argv0 in process_table(proc_root)
        if pid != own and re.search(pattern, argv0)
    ]


def start_epoch(pid: str) -> int | None:
    """Start time of one process in seconds since the epoch, or None if it cannot be read."""
    if sys.platform.startswith("linux"):
        # Field 22 is ticks since boot. The command in field 2 can contain spaces and ')',
        # so split after its final delimiter rather than tokenising the entire record.
        record = pathlib.Path(f"/proc/{pid}/stat").read_text()
        ticks = int(record.rsplit(")", 1)[1].split()[19])
        boot = next(
            int(line.split()[1])
            for line in pathlib.Path("/proc/stat").read_text().splitlines()
            if line.startswith("btime ")
        )
        return int(boot + ticks / os.sysconf("SC_CLK_TCK"))
    started = subprocess.run(
        ["ps", "-o", "lstart=", "-p", pid], capture_output=True, text=True, check=False
    ).stdout.strip()
    if not started:
        return None
    epoch = subprocess.run(
        ["date", "-j", "-f", "%a %b %d %T %Y", started, "+%s"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    return int(epoch) if epoch.isdigit() else None


def process_start_epoch(pattern: str) -> int | None:
    """Start time of the running broca process, or None if it is not running.

    Raises AmbiguousProcess when several processes match. The boot epoch decides which unsealed
    WALs count as live, and the epoch of any process that started after the daemon would call
    the daemon's live runs abandoned and clear a restart that cuts them. So two candidates are
    a refusal, never a pick.
    """
    try:
        pids = matching_pids(pattern)
        if not pids:
            return None
        if len(pids) > 1:
            raise AmbiguousProcess(
                f"{len(pids)} processes match {pattern!r} (pids {' '.join(pids)}); pass --process "
                "with a pattern only the running daemon matches"
            )
        return start_epoch(pids[0])
    except (OSError, ValueError, IndexError, StopIteration):
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", default=str(state_root.default_state_root()))
    parser.add_argument("--process", default="bin/ck-broca", help="pgrep pattern for the module")
    parser.add_argument(
        "--boot-epoch",
        type=int,
        help=(
            "Compare against this boot time instead of the running process, for attributing a "
            "restart. See --attribute-restart, which is the intended way to use it."
        ),
    )
    parser.add_argument(
        "--attribute-restart",
        action="store_true",
        help=(
            "Report the runs a restart just cut, as a set difference against a scan taken before "
            "it. RUN THIS WITHIN SECONDS OF THE RESTART: a cut run is only sealed when its "
            "session is next sent to, so it is visible here until its caller comes back, and on a "
            "busy machine that can be under a minute. Reading it late reports a clean restart for "
            "the same reason reading it early would -- the evidence has moved on rather than never "
            "existed."
        ),
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="check that an unreadable WAL refuses instead of reading as idle",
    )
    args = parser.parse_args()

    if args.self_test:
        return _self_test()

    wal_dir = pathlib.Path(args.state_root) / "wal"
    if not any((wal_dir.parent / name).is_dir() for name in ("wal", "wal-ephemeral", "wal-durable")):
        print(f"cannot compare: no wal directory at {wal_dir}", file=sys.stderr)
        return 2

    try:
        boot = args.boot_epoch or process_start_epoch(args.process)
    except AmbiguousProcess as ambiguous:
        print(f"cannot compare: {ambiguous}", file=sys.stderr)
        return 2
    if boot is None:
        print(
            "cannot compare: broca is not running, so there is no process boundary to compare "
            "against (and nothing to cut)",
            file=sys.stderr,
        )
        return 2

    scanned = 0
    unsealed: list[tuple[str, float]] = []
    newer_writer: list[tuple[str, float]] = []
    try:
        files = logical_wals(wal_dir, include_archives=False)
    except ArchiveRefusal as error:
        print(f"cannot compare: {error}", file=sys.stderr)
        return 2
    for wal in files:
        scanned += 1
        try:
            state = tail_state(wal)
        except WalUnreadable as unreadable:
            # Refuse rather than guess: this is the one answer a restart gate must never
            # invent, and "cannot compare" is the exit code that already means it.
            print(f"cannot compare: {unreadable}", file=sys.stderr)
            return 2
        if state == OPEN:
            unsealed.append((wal.name, wal.stat().st_mtime))
        elif state == NEWER_WRITER:
            # Not corruption and not a reason to stop: listed by name, decided below.
            newer_writer.append((wal.name, wal.stat().st_mtime))

    # A scan that examined nothing would report a clean idle box. Refuse instead.
    if scanned == 0:
        print(f"cannot compare: no wal files under {wal_dir}", file=sys.stderr)
        return 2

    live = [(n, m) for n, m in unsealed if m >= boot]
    abandoned = [(n, m) for n, m in unsealed if m < boot]
    # A newer-writer file written since this process started may hold a live run this script
    # cannot recognise. One last written before the process started cannot: a live run writes
    # to its WAL, so a file untouched since the start belongs to a process that is gone.
    undecided = [(n, m) for n, m in newer_writer if m >= boot]

    now = time.time()
    print(f"wal files scanned  {scanned}")
    print(f"process started    {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(boot))}")
    print(f"live runs          {len(live)}")
    print(f"abandoned          {len(abandoned)}  (unsealed, last write predates this process)")
    print(
        f"newer-writer       {len(newer_writer)}  (a newer broca wrote these; "
        f"{len(undecided)} since this process started)"
    )
    for name, mtime in sorted(live, key=lambda pair: pair[1], reverse=True):
        print(f"  live  {name}  last write {now - mtime:.0f}s ago")
    for name, mtime in sorted(newer_writer, key=lambda pair: pair[1], reverse=True):
        print(f"  newer-writer  {name}  last write {now - mtime:.0f}s ago")

    # Abandoned names are the term that makes restart attribution exact: pre-existing unsealed
    # lineages would otherwise be blamed on the deploy.
    if args.attribute_restart:
        print("abandoned set (difference this against the pre-restart scan):")
        for name, _ in sorted(abandoned):
            print(f"  abandoned  {name}")
        age = now - boot
        if age > 120:
            print(
                f"\nWARNING: this process started {age / 60:.0f} minutes ago, so any run this "
                "restart cut has probably been sealed by later traffic and is no longer listed. "
                "A clean reading here is not evidence of a clean restart.",
                file=sys.stderr,
            )

    if live:
        print("\nrestarting now would cut the runs listed above", file=sys.stderr)
        return 1
    if undecided:
        print(
            f"\ncannot compare: {len(undecided)} WAL(s) written since this process started "
            "were written by a newer broca build (newer-writer); this script cannot tell "
            "whether they hold a live run. Use the live-runs.py that ships with that build.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
