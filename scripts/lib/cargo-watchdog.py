#!/usr/bin/env python3
"""Run Cargo with no-progress and optional wall-clock watchdogs.

BROCA_CARGO_LOCK_TIMEOUT_SECONDS defaults to 300 seconds; after Cargo reports
that it is waiting for a named file lock, no further output for this long is
reported as a lock timeout (exit 125). BROCA_CARGO_IDLE_TIMEOUT_SECONDS
defaults to 900 seconds; no output for this long is an idle timeout (exit 124).
Both values may be fractional seconds for tests. An optional
--deadline-seconds keeps callers' existing hard wall-clock limits (also exit
124). Ordinary Cargo exit statuses pass through unchanged.
"""

import argparse
import math
import os
import re
import selectors
import signal
import subprocess
import sys
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

    try:
        lock_timeout = positive_seconds("BROCA_CARGO_LOCK_TIMEOUT_SECONDS", "300")
        idle_timeout = positive_seconds("BROCA_CARGO_IDLE_TIMEOUT_SECONDS", "900")
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

    started = time.monotonic()
    last_output = started
    lock_name = None
    lock_started = None
    interrupted = []

    def remember_signal(signum, _frame):
        interrupted.append(signum)

    previous_handlers = {}
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        previous_handlers[signum] = signal.signal(signum, remember_signal)

    try:
        while selector.get_map():
            now = time.monotonic()
            if interrupted:
                signum = interrupted[0]
                terminate_group(process)
                return 128 + signum

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

            idle = now - last_output
            if idle >= idle_timeout:
                print(
                    f"cargo watchdog: step '{args.step}' made no output progress for "
                    f"{idle_timeout:g}s; terminating Cargo process group (exit 124)",
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
            if lock_name is not None and lock_started is not None:
                remaining.append(lock_timeout - (now - lock_started))
            if args.deadline_seconds is not None:
                remaining.append(args.deadline_seconds - elapsed)
            events = selector.select(max(0.0, min(remaining)))
            if not events:
                continue

            for key, _ in events:
                stream, output = key.data
                data = os.read(stream.fileno(), 65536)
                if not data:
                    selector.unregister(stream)
                    stream.close()
                    continue

                if output is not None:
                    output.write(data)
                    output.flush()
                last_output = time.monotonic()

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
        for stream in (stdout, stderr):
            if not stream.closed:
                stream.close()


if __name__ == "__main__":
    raise SystemExit(main())
