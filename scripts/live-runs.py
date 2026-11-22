#!/usr/bin/env python3
"""Report the runs broca is running right now, asked of the running daemon.

Answers the one question a deploy needs: will restarting broca cut work in progress? Every session
lives in the session store, whose logs are encrypted with a key only the daemon holds, so no file
read can answer it. The daemon can: its memory report lists every open session and names the run
each one has in progress. A session with no actor has no run in progress, because a run lives in
its session's actor.

An actor busy with a long command (an import, a view install) misses the report's two-second
wait. Its session may be running, so it counts against a restart like a live run: the report is
asked again a few times first, and an actor still silent is listed as unanswered.

Exit 0 when nothing is live, 1 when a run is live or an actor didn't answer, 2 when the question
cannot be answered at all because the daemon gave no report. A gate that cannot tell must not
read as a clear gate.

Output, one run per line, which place-release.sh reads:

    live runs          2
    unanswered         0
      live  <session address>  <run id, or ? from a daemon that does not name it>  <class>
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import subprocess
import sys
import time

from lib import state_root

# The memory report is module-wide; the session the command binds to only routes it.
PROBE = ("--project-root", "broca-unscoped-v1", "--harness", "broca", "--session", "live-runs-probe")


def cortexkit_home() -> pathlib.Path:
    """The cortexkit data folder the daemon's connection file and installed client live in."""
    env = dict(os.environ)
    # BROCA_STATE_ROOT moves only broca's own state, not the daemon's folders.
    env.pop(state_root.STATE_ROOT_ENV, None)
    return state_root.default_state_root(env).parent


def connection_file(home: pathlib.Path) -> pathlib.Path:
    """Where the daemon writes its connection file: the runtime folder when the platform has
    one, else the cortexkit data folder."""
    runtime = os.environ.get("XDG_RUNTIME_DIR")
    return pathlib.Path(runtime) / "subc-connection.json" if runtime else home / "run" / "subc-connection.json"


def memory_report(broca_session: str, subc: str, timeout: float) -> dict:
    """The daemon's memory report, or a ValueError saying why there is none."""
    try:
        answer = subprocess.run(
            [broca_session, "--subc", subc, "--json-events", "memory", *PROBE],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ValueError(f"broca-session did not answer: {error}") from error
    for line in answer.stdout.splitlines():
        if not line.startswith("{"):
            continue
        event = json.loads(line)
        if isinstance(event.get("sessions"), list):
            return event
    reason = (answer.stderr.strip() or answer.stdout.strip() or "no output").splitlines()[-1]
    raise ValueError(f"no memory report (exit {answer.returncode}): {reason}")


def main() -> int:
    home = cortexkit_home()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--broca-session",
        default=os.environ.get("BROCA_SESSION_BIN") or str(home / "bin" / "broca-session"),
    )
    parser.add_argument(
        "--subc",
        default=os.environ.get("SUBC_CONNECTION") or str(connection_file(home)),
    )
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--attempts", type=int, default=4, help="asks while an actor is silent")
    args = parser.parse_args()

    for attempt in range(max(args.attempts, 1)):
        if attempt:
            time.sleep(1)
        try:
            report = memory_report(args.broca_session, args.subc, args.timeout)
        except ValueError as error:
            print(f"cannot compare: {error}", file=sys.stderr)
            return 2
        unanswered = report.get("unanswered_sessions", 0)
        if not unanswered:
            break
    live = [session for session in report["sessions"] if session.get("run_in_progress")]
    print(f"sessions open      {len(report['sessions'])}")
    print(f"live runs          {len(live)}")
    print(f"unanswered         {unanswered}")
    for session in live:
        run_id = session.get("run_id") or "?"
        print(f"  live  {session['address']}  {run_id}  {session.get('session_class') or '-'}")
    if live:
        print("\nrestarting now would cut the runs listed above", file=sys.stderr)
    if unanswered:
        print(
            f"\n{unanswered} session(s) did not answer in {args.attempts} asks; each may be "
            "running, and restarting now would cut it",
            file=sys.stderr,
        )
    return 1 if live or unanswered else 0


if __name__ == "__main__":
    raise SystemExit(main())
