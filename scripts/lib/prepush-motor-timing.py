#!/usr/bin/env python3
"""Separate client request-to-start delay from actual runner execution time."""
import argparse
import json
from pathlib import Path


def summarize(path, requested_ms, checkout_ms, total_ms):
    started = terminal = None
    if path.exists():
        with path.open() as source:
            for line in source:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event.get('type') == 'started':
                    started = event
                elif event.get('type') == 'terminal':
                    terminal = event
    start_ms = started.get('started_at_ms') if started else None
    return {
        'checkout_ms': checkout_ms,
        'requested_at_ms': requested_ms,
        'started_at_ms': start_ms,
        'request_to_start_ms': start_ms - requested_ms if start_ms is not None else None,
        'run_ms': terminal.get('wall_ms') if terminal else None,
        'reported_queue_ms': terminal.get('queue_wait_ms') if terminal else None,
        'total_ms': total_ms,
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('events', type=Path)
    parser.add_argument('requested_ms', type=int)
    parser.add_argument('checkout_ms', type=int)
    parser.add_argument('total_ms', type=int)
    args = parser.parse_args()
    print(json.dumps(summarize(args.events, args.requested_ms, args.checkout_ms, args.total_ms)))
