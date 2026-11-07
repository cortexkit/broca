#!/usr/bin/env python3
"""Run Cargo with no-progress and optional wall-clock watchdogs.

BROCA_CARGO_LOCK_TIMEOUT_SECONDS defaults to 300 seconds; after Cargo reports
that it is waiting for a named file lock, no further output for this long is
reported as a lock timeout (exit 125). BROCA_CARGO_IDLE_TIMEOUT_SECONDS
defaults to 900 seconds; no output for this long is an idle timeout (exit 124)
unless Cargo's processes are still using CPU.

Silence alone doesn't mean Cargo is stuck: a cold build links its largest test
binaries, or a compiler works on one crate, for many minutes without a line of
output. So when the idle timeout passes, the watchdog asks how much CPU the
processes in Cargo's process group used since it last looked. At least
BROCA_CARGO_BUSY_CPU_SECONDS (default 5) counts as work in progress: it says so
once, then waits another idle period. A step stuck on a lock or a wedged
compiler cache uses no CPU and is stopped as before. Being busy can keep a silent
step alive for at most BROCA_CARGO_BUSY_LIMIT_SECONDS (default 3600) past its
last output, so a process spinning forever is still stopped. Every idle timeout
names the group's processes and their CPU time, so the next stall says what hung.

All values may be fractional seconds for tests. An optional
--deadline-seconds keeps callers' existing hard wall-clock limits (also exit
124). Ordinary Cargo exit statuses pass through unchanged.

Cargo's exit is watched directly, not inferred from its output pipes closing. A
descendant that outlives Cargo (a leaked test daemon, say) keeps those pipes
open, and waiting for them would turn a finished step into an idle timeout.
After Cargo exits, output is drained for BROCA_CARGO_EXIT_DRAIN_SECONDS
(default 5); a pipe still open then is reported, the leftover processes in
Cargo's private group are stopped, and Cargo's own status is returned.
"""

import argparse
import math
import os
import re
import selectors
import signal
import subprocess
import sys
import threading
import time


LOCK_LINE = re.compile(rb"Blocking waiting for file lock on ([^\r\n]+)")
TERMINATE_GRACE_SECONDS = 0.25


def positive_seconds(name: str, default: str) -> float:
    raw = os.environ.get(name)
    if raw is None:
        raw = default
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"{name} must be a positive number, got {raw!r}")
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive number, got {raw!r}")
    return value


def terminate_group(process: subprocess.Popen) -> int:
    """Stop Cargo and every child in its private process group."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except OSError:
        pass

    deadline = time.monotonic() + TERMINATE_GRACE_SECONDS
    while process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.01)

    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except OSError:
        pass
    return process.wait()


def cpu_seconds(text: str) -> float:
    """Parse ps's cumulative CPU time: `[[dd-]hh:]mm:ss[.ff]` (Linux) or `mm:ss.ff` (macOS)."""
    days = 0
    if "-" in text:
        day_text, text = text.split("-", 1)
        days = int(day_text)
    seconds = 0.0
    for part in text.split(":"):
        seconds = seconds * 60 + float(part)
    return days * 86400 + seconds


def group_cpu(pgid: int) -> dict:
    """CPU seconds used so far by each living process in process group `pgid`, by pid, with its
    command name. Empty when the processes can't be listed; the caller then treats the group as
    idle. Linux is read from /proc, whose clock ticks are finer than ps's whole seconds there."""
    if os.path.exists("/proc/self/stat"):
        return proc_group_cpu(pgid)
    try:
        listing = subprocess.run(
            ["ps", "-A", "-o", "pgid=,pid=,time=,comm="],
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return {}
    usage = {}
    for line in listing.splitlines():
        fields = line.split(None, 3)
        if len(fields) < 3 or fields[0] != str(pgid):
            continue
        try:
            usage[int(fields[1])] = (cpu_seconds(fields[2]), fields[3] if len(fields) > 3 else "?")
        except ValueError:
            continue
    return usage


def proc_group_cpu(pgid: int) -> dict:
    ticks = os.sysconf("SC_CLK_TCK")
    usage = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as stat:
                text = stat.read()
        except OSError:
            continue
        # The command name sits in parentheses and may hold spaces, so fields count from the
        # last ')': state, ppid, pgrp, ... then utime and stime at 11 and 12.
        name, _, rest = text.rpartition(")")
        fields = rest.split()
        if len(fields) < 13 or fields[2] != str(pgid):
            continue
        usage[int(entry)] = ((int(fields[11]) + int(fields[12])) / ticks, name.partition("(")[2])
    return usage


def cpu_since(before: dict, after: dict) -> float:
    """CPU the group used between two `group_cpu` samples. A process that started since counts
    from zero, and one that ended since takes its CPU with it, so this can only undercount."""
    return sum(max(0.0, cpu - before.get(pid, (0.0, ""))[0]) for pid, (cpu, _) in after.items())


def describe_group(usage: dict) -> str:
    busiest = sorted(usage.values(), reverse=True)[:5]
    return ", ".join(f"{name.rsplit('/', 1)[-1]} {cpu:.0f}s" for cpu, name in busiest) or "none"


def shell_status(returncode: int) -> int:
    return returncode if returncode >= 0 else 128 - returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step", required=True, help="human-readable Cargo step name")
    parser.add_argument(
        "--deadline-seconds",
        type=float,
        help="optional hard wall-clock limit, independent of output progress",
    )
    parser.add_argument(
        "--discard-stderr",
        action="store_true",
        help="read but do not display Cargo stderr (watchdog failures remain visible)",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER, help="Cargo command after --")
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a command is required after --")
    if args.deadline_seconds is not None and (
        not math.isfinite(args.deadline_seconds) or args.deadline_seconds <= 0
    ):
        parser.error("--deadline-seconds must be a positive number")

    if os.name == "nt":
        # Windows has no SIGHUP or process groups, and selectors cannot watch pipes there. The
        # watchdog exists for the macOS stuck-lock case; on Windows run Cargo directly, keeping
        # its output and exit status, and let the CI job timeout catch a hang.
        try:
            return subprocess.run(command).returncode
        except OSError as error:
            print(f"cargo watchdog: could not start step '{args.step}': {error}", file=sys.stderr)
            return 127

    try:
        lock_timeout = positive_seconds("BROCA_CARGO_LOCK_TIMEOUT_SECONDS", "300")
        idle_timeout = positive_seconds("BROCA_CARGO_IDLE_TIMEOUT_SECONDS", "900")
        exit_drain = positive_seconds("BROCA_CARGO_EXIT_DRAIN_SECONDS", "5")
        busy_cpu = positive_seconds("BROCA_CARGO_BUSY_CPU_SECONDS", "5")
        busy_limit = positive_seconds("BROCA_CARGO_BUSY_LIMIT_SECONDS", "3600")
    except ValueError as error:
        parser.error(str(error))

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            start_new_session=True,
        )
    except OSError as error:
        print(f"cargo watchdog: could not start step '{args.step}': {error}", file=sys.stderr)
        return 127

    stdout = process.stdout
    stderr = process.stderr
    assert stdout is not None and stderr is not None
    selector = selectors.DefaultSelector()
    pending_lines = {}
    for stream, output in (
        (stdout, sys.stdout.buffer),
        (stderr, None if args.discard_stderr else sys.stderr.buffer),
    ):
        selector.register(stream, selectors.EVENT_READ, (stream, output))
        pending_lines[stream] = b""

    # A thread reaps Cargo and wakes the selector through this pipe, so its exit is seen as soon
    # as it happens rather than at the next timeout. The thread owns the write end and closes it
    # itself, so a late write can never land on a descriptor this process has reused.
    exit_read, exit_write = os.pipe()

    def report_exit():
        try:
            process.wait()
            os.write(exit_write, b"x")
        except OSError:
            pass
        finally:
            os.close(exit_write)

    selector.register(exit_read, selectors.EVENT_READ, None)
    threading.Thread(target=report_exit, daemon=True).start()
    output_streams = {stdout, stderr}
    exited_at = None

    started = time.monotonic()
    last_output = started
    # When the idle clock last restarted, by output or by the group still being busy, and the
    # group's CPU sample taken then. Output clears the sample rather than taking a new one, so a
    # chatty step never pays for listing processes; a silent step then takes its first sample
    # at the idle timeout and decides after a short probe.
    idle_since = started
    cpu_sample = group_cpu(process.pid)
    probe = min(60.0, idle_timeout / 4)
    lock_name = None
    lock_started = None
    interrupted = []

    def remember_signal(signum, _frame):
        interrupted.append(signum)

    previous_handlers = {}
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        previous_handlers[signum] = signal.signal(signum, remember_signal)

    try:
        while output_streams:
            now = time.monotonic()
            if interrupted:
                signum = interrupted[0]
                terminate_group(process)
                return 128 + signum

            if exited_at is not None and now - exited_at >= exit_drain:
                status = shell_status(process.returncode)
                print(
                    f"cargo watchdog: step '{args.step}' exited with status {status}, but a "
                    f"process it started still holds its output open {exit_drain:g}s later; "
                    "stopping what is left of Cargo's process group",
                    file=sys.stderr,
                    flush=True,
                )
                terminate_group(process)
                return status

            if lock_name is not None and lock_started is not None and now - lock_started >= lock_timeout:
                print(
                    f"cargo watchdog: step '{args.step}' is still waiting for file lock "
                    f"on {lock_name} after {lock_timeout:g}s; terminating Cargo process group "
                    "(exit 125)",
                    file=sys.stderr,
                    flush=True,
                )
                terminate_group(process)
                return 125

            idle = now - idle_since
            if idle >= idle_timeout and cpu_sample is None:
                cpu_sample = group_cpu(process.pid)
                idle_since = now - idle_timeout + probe
                continue
            if idle >= idle_timeout:
                usage = group_cpu(process.pid)
                used = cpu_since(cpu_sample, usage)
                silent = now - last_output
                if used >= busy_cpu and silent + idle_timeout <= busy_limit:
                    print(
                        f"cargo watchdog: step '{args.step}' has printed nothing for {silent:.0f}s "
                        f"but its processes used {used:.0f}s of CPU in the last {idle:.0f}s; "
                        f"still working ({describe_group(usage)})",
                        file=sys.stderr,
                        flush=True,
                    )
                    idle_since = now
                    cpu_sample = usage
                    continue
                reason = (
                    f"made no output progress for {silent:.0f}s while its processes kept using CPU, "
                    f"past the {busy_limit:g}s limit"
                    if used >= busy_cpu
                    else f"made no output progress for {silent:.0f}s and its processes used "
                    f"{used:.1f}s of CPU in the last {idle:.0f}s"
                )
                print(
                    f"cargo watchdog: step '{args.step}' {reason}; processes: "
                    f"{describe_group(usage)}; terminating Cargo process group (exit 124)",
                    file=sys.stderr,
                    flush=True,
                )
                terminate_group(process)
                return 124

            elapsed = now - started
            if args.deadline_seconds is not None and elapsed >= args.deadline_seconds:
                print(
                    f"gate: command exceeded its {args.deadline_seconds:g}s liveness deadline "
                    f"(wedged?): {args.step}; terminating Cargo process group (exit 124)",
                    file=sys.stderr,
                    flush=True,
                )
                terminate_group(process)
                return 124

            remaining = [idle_timeout - idle]
            if exited_at is not None:
                remaining.append(exit_drain - (now - exited_at))
            if lock_name is not None and lock_started is not None:
                remaining.append(lock_timeout - (now - lock_started))
            if args.deadline_seconds is not None:
                remaining.append(args.deadline_seconds - elapsed)
            events = selector.select(max(0.0, min(remaining)))
            if not events:
                continue

            for key, _ in events:
                if key.data is None:
                    selector.unregister(exit_read)
                    exited_at = time.monotonic()
                    continue
                stream, output = key.data
                data = os.read(stream.fileno(), 65536)
                if not data:
                    selector.unregister(stream)
                    output_streams.discard(stream)
                    stream.close()
                    continue

                if output is not None:
                    output.write(data)
                    output.flush()
                last_output = time.monotonic()
                idle_since = last_output
                cpu_sample = None

                buffered = pending_lines[stream] + data
                lines = buffered.split(b"\n")
                pending_lines[stream] = lines.pop()
                for line in lines:
                    lock_match = LOCK_LINE.search(line)
                    if lock_match:
                        observed_lock = lock_match.group(1).decode("utf-8", errors="replace").strip()
                        if observed_lock != lock_name:
                            lock_name = observed_lock
                            lock_started = time.monotonic()
                    elif lock_name is not None:
                        # Cargo has emitted output after the lock notice, so it acquired the lock.
                        lock_name = None
                        lock_started = None

        return shell_status(process.wait())
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
        selector.close()
        os.close(exit_read)
        for stream in (stdout, stderr):
            if not stream.closed:
                stream.close()


if __name__ == "__main__":
    raise SystemExit(main())
