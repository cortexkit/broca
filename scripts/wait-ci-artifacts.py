#!/usr/bin/env python3
"""Wait for this run's artifacts without waiting for unrelated matrix producers."""

import argparse
import json
import os
import re
import subprocess
import sys
import time


class TransientQueryError(RuntimeError):
    """A GitHub API failure that says nothing about the artifacts: a 5xx or a timeout."""


def api(path):
    try:
        result = subprocess.run(["gh", "api", path], text=True, capture_output=True, timeout=60)
    except subprocess.TimeoutExpired as error:
        raise TransientQueryError(f"GitHub artifact query timed out: {error}") from error
    if result.returncode:
        message = result.stderr.strip()
        # A server error is GitHub failing to answer, not an answer about this run, so the
        # poll retries it until the deadline. Every other failure (auth, 404) is final.
        if re.search(r"HTTP 5\d\d", message):
            raise TransientQueryError(f"GitHub artifact query failed: {message}")
        raise RuntimeError(f"GitHub artifact query failed: {message}")
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("names", nargs="+")
    parser.add_argument("--timeout", type=float, default=1200)
    parser.add_argument("--poll-seconds", type=float, default=5)
    args = parser.parse_args()
    if args.timeout <= 0 or args.poll_seconds < 0:
        parser.error("timeout must be positive and poll-seconds nonnegative")
    repository = os.environ["GITHUB_REPOSITORY"]
    run_id = os.environ["GITHUB_RUN_ID"]
    base = f"repos/{repository}/actions/runs/{run_id}"
    deadline = time.monotonic() + args.timeout
    wanted = set(args.names)
    producers = {
        "fixtures-Linux": "Fixtures (Linux)",
        "fixtures-Windows": "Fixtures (Windows)",
        "integration-ubuntu": "Archive (ubuntu)",
        "integration-windows": "Archive (windows)",
    }
    if wanted - producers.keys():
        parser.error("unsupported CI artifacts: " + ", ".join(sorted(wanted - producers.keys())))
    while True:
        try:
            available = list_artifacts(base)
        except TransientQueryError as error:
            if time.monotonic() >= deadline:
                raise
            print(f"Retrying after: {error}", flush=True)
            time.sleep(min(max(args.poll_seconds, 1), max(0, deadline - time.monotonic())))
            continue
        missing = wanted - available
        if not missing:
            print("Ready: " + ", ".join(sorted(wanted)), flush=True)
            return
        # Completed failed producers cannot supply missing artifacts. Inspect jobs
        # rather than the run: the waiting shards themselves keep the run active.
        active_producers = False
        seen_producers = set()
        needed_producers = {producers[name] for name in missing}
        page = 1
        while True:
            try:
                jobs = api(f"{base}/jobs?per_page=100&page={page}")["jobs"]
            except TransientQueryError as error:
                if time.monotonic() >= deadline:
                    raise
                print(f"Retrying after: {error}", flush=True)
                active_producers = True
                break
            for job in jobs:
                name = job["name"].split(" / ")[-1]
                if name in needed_producers:
                    seen_producers.add(name)
                    if job["status"] != "completed":
                        active_producers = True
                    elif job["conclusion"] != "success":
                        raise RuntimeError(f"Producer failed: {job['name']} ({job['conclusion']})")
            if len(jobs) < 100:
                break
            page += 1
        if seen_producers == needed_producers and not active_producers:
            raise RuntimeError("Producers finished without artifacts: " + ", ".join(sorted(missing)))
        if time.monotonic() >= deadline:
            raise RuntimeError("Timed out waiting for artifacts: " + ", ".join(sorted(missing)))
        print("Waiting for: " + ", ".join(sorted(missing)), flush=True)
        time.sleep(min(args.poll_seconds, max(0, deadline - time.monotonic())))


def list_artifacts(base):
    available = set()
    page = 1
    while True:
        response = api(f"{base}/artifacts?per_page=100&page={page}")
        artifacts = response["artifacts"]
        available.update(a["name"] for a in artifacts if not a["expired"])
        if len(artifacts) < 100:
            return available
        page += 1


if __name__ == "__main__":
    try:
        main()
    except (KeyError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        sys.exit(f"CI artifacts: {error}")
