#!/usr/bin/env python3
"""Separate client request-to-start delay from actual runner execution time."""
import argparse
import json
from pathlib import Path


def summarize(path, requested_ms, checkout_ms, total_ms, vcpus=None):
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
        'vcpus': vcpus,
    }


def critical_path(directory, started_ms, finished_ms, budget):
    completed = []
    incomplete = []
    cpu_work_ms = 0
    for path in sorted(directory.glob('*.timing.json')):
        timing = json.loads(path.read_text())
        name = path.name.removesuffix('.timing.json')
        if timing['started_at_ms'] is None or timing['run_ms'] is None:
            incomplete.append(name)
            continue
        completed.append({'lane': name, 'run_ms': timing['run_ms'], 'vcpus': timing.get('vcpus'),
                          'finish_after_start_ms': timing['started_at_ms'] + timing['run_ms'] - started_ms})
        if timing.get('vcpus') is not None:
            cpu_work_ms += timing['run_ms'] * timing['vcpus']
    longest = max(completed, key=lambda lane: lane['run_ms'], default=None)
    last = max(completed, key=lambda lane: lane['finish_after_start_ms'], default=None)
    return {'end_to_end_ms': finished_ms - started_ms, 'longest_runner': longest,
            'last_runner': last, 'known_reserved_cpu_ms': cpu_work_ms,
            'known_reservation_floor_ms': (cpu_work_ms + budget - 1) // budget,
            'incomplete_lanes': incomplete}


def print_critical_path(directory, started_ms, finished_ms, budget):
    report = critical_path(directory, started_ms, finished_ms, budget)
    (directory / 'critical-path.json').write_text(json.dumps(report) + '\n')
    print(f"prepush: measured end-to-end {report['end_to_end_ms'] / 1000:.3f}s")
    if report['last_runner'] is None:
        print('prepush: critical path UNKNOWN: no runner completion was observed')
        return
    last, longest = report['last_runner'], report['longest_runner']
    assert longest is not None
    print(f"prepush: measured critical path ends at {last['lane']}: "
          f"{last['finish_after_start_ms'] / 1000:.3f}s after gate start (includes admission/queue delay)")
    print(f"prepush: longest measured runner {longest['lane']}: {longest['run_ms'] / 1000:.3f}s "
          f"at {longest['vcpus']} vCPUs; known reservation-time floor at {budget} vCPUs: "
          f"{report['known_reservation_floor_ms'] / 1000:.3f}s")
    if longest['run_ms'] > 1_200_000:
        print(f"prepush: measured 20-minute floor exceeded: {longest['lane']} alone took "
              f"{longest['run_ms'] / 1000:.3f}s; unchanged measured work cannot finish below that")
    if report['incomplete_lanes']:
        print('prepush: timing coverage INCOMPLETE: ' + ', '.join(report['incomplete_lanes']))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('events', type=Path)
    parser.add_argument('requested_ms', type=int)
    parser.add_argument('checkout_ms', type=int)
    parser.add_argument('total_ms', type=int)
    parser.add_argument('--vcpus', type=int)
    parser.add_argument('--critical', action='store_true', help='report logs/start/end/budget instead of one lane')
    args = parser.parse_args()
    if args.critical:
        print_critical_path(args.events, args.requested_ms, args.checkout_ms, args.total_ms)
    else:
        print(json.dumps(summarize(args.events, args.requested_ms, args.checkout_ms, args.total_ms, args.vcpus)))
