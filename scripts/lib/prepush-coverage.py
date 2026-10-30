#!/usr/bin/env python3
"""Map former portable tests to CI lanes and audit their execution/skip guards."""
import argparse
import collections
import json
from pathlib import Path
import re

CLOCK = ('broca-module::it', 'test_clock::test_clock_feature_is_enabled_for_retention_tests')
STATUS = re.compile(r'^\s*(PASS|FAIL)\s+\[[^\]\n]+\]\s+(?:\(\s*\d+/\d+\)\s+)?(\S+)\s+(.+?)\s*$')
SKIP = re.compile(r'^\s*SKIP [a-z_]')
SUMMARY = re.compile(r'^test result: \S+\. (\d+) passed; (\d+) failed; (\d+) ignored; \d+ measured; (\d+) filtered out; finished in ([\d.]+)s')


def read_text(path):
    return re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]', '', path.read_text(errors='replace'))


def cases(path, selected=True):
    data = json.loads(path.read_text())
    return {(binary, name): case for binary, suite in data['rust-suites'].items()
            for name, case in suite['testcases'].items()
            if not selected or (case['filter-match']['status'] == 'matches' and not case['ignored'])}


def unit_map(path, exclusions):
    all_cases = cases(path, selected=False)
    for exclusion in exclusions:
        if sum(name == exclusion for _, name in all_cases) != 1:
            raise ValueError('unit exclusion must name exactly one discovered test: ' + exclusion)
    selected = {key: case for key, case in cases(path).items() if key[1] not in exclusions}
    if not selected:
        raise ValueError('zero former portable unit tests')
    return [{'binary': binary, 'test': name, 'destination': 'linux-unit'} for binary, name in sorted(selected)]


def audit_units(path, inventory, exclusions):
    suites = json.loads(inventory.read_text())['rust-suites']
    by_executable = {}
    for binary, suite in suites.items():
        executable = Path(suite['binary-path']).name
        if executable in by_executable:
            raise ValueError('ambiguous unit executable: ' + executable)
        by_executable[executable] = binary
    current_binary = current_test = None
    results = {}
    for line in read_text(path).splitlines():
        if line.startswith('==> doctests'):
            break
        header = re.match(r'^\s*Running unittests .*? \((.+)\)\s*$', line)
        if header:
            executable = Path(header[1]).name
            if executable not in by_executable:
                raise ValueError('unknown unit executable: ' + executable)
            current_binary, current_test = by_executable[executable], None
        output = re.match(r'^---- (.+) (?:stdout|stderr) ----$', line)
        if output:
            current_test = output[1]
        if SKIP.match(line) and current_test not in exclusions:
            raise ValueError('a former portable unit returned a passing fixture skip: ' + line.strip())
        summary = SUMMARY.match(line)
        if summary and current_binary is not None:
            # A re-executed child can print a summary; the parent summary is last.
            passed, failed, ignored, filtered = map(int, summary.groups()[:4])
            results[current_binary] = {'passed': passed, 'failed': failed, 'ignored': ignored,
                                       'filtered': filtered, 'seconds': float(summary[5])}
    if set(results) != set(suites):
        raise ValueError('unit binary execution inventory differs: missing=' + ','.join(sorted(set(suites) - set(results))))
    for binary, result in results.items():
        expected = sum(not case['ignored'] for case in suites[binary]['testcases'].values())
        if result['passed'] + result['failed'] != expected or result['filtered'] != 0:
            raise ValueError('unit execution count differs for ' + binary)
    for binary, result in sorted(results.items(), key=lambda row: -row[1]['seconds']):
        print(f"unit binary {binary}: reported={result['seconds']:.3f}s passed={result['passed']} failed={result['failed']}")
    return results


def integration_map(full, portable, partition):
    expected, selected = cases(full), cases(portable)
    if not expected or not selected:
        raise ValueError('zero CI or former portable integration tests selected')
    if not selected.keys() <= expected.keys():
        raise ValueError('former portable tests are missing from the CI partition')
    lane = 'linux-integration-' + partition.split('/')[0]
    return [{'binary': binary, 'test': name, 'destination': lane, 'partition': partition}
            for binary, name in sorted(selected)]


def audit_integration(path, full, portable):
    expected, selected = cases(full), cases(portable)
    current = None
    statuses = {}
    text = read_text(path)
    for line in text.splitlines():
        match = STATUS.match(line)
        if match:
            current = (match[2], match[3])
            if current in expected or current == CLOCK:
                statuses[current] = match[1]
        if SKIP.match(line) and current in selected:
            raise ValueError('a former portable integration returned a passing fixture skip: ' + line.strip())
    summaries = re.findall(r'^\s*Summary\s+\[[^\]]+\]\s+(\d+) tests? run', text, re.M)
    if list(map(int, summaries)) != [1, len(expected)]:
        raise ValueError(f'CI partition execution count differs: expected clock=1, partition={len(expected)}; observed={summaries}')
    if set(statuses) != set(expected) | {CLOCK} or statuses.get(CLOCK) != 'PASS':
        raise ValueError('CI partition execution identities or clock sentinel differ from inventory')
    return {'executed': len(expected), 'portable_executed': len(selected), 'clock': 'PASS'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest='mode', required=True)
    unit = modes.add_parser('unit')
    unit.add_argument('--inventory', type=Path, required=True)
    unit.add_argument('--exclude', action='append', default=[])
    integration = modes.add_parser('integration')
    integration.add_argument('--full', type=Path, required=True)
    integration.add_argument('--portable', type=Path, required=True)
    integration.add_argument('--partition', required=True)
    for mode in (unit, integration):
        mode.add_argument('--log', type=Path)
        mode.add_argument('--output', type=Path, required=True)
        mode.add_argument('--candidate', required=True)
        mode.add_argument('--feature', action='append', required=True)
    args = parser.parse_args()
    if args.mode == 'unit':
        entries = unit_map(args.inventory, args.exclude)
        execution = audit_units(args.log, args.inventory, args.exclude) if args.log else None
    else:
        if not re.fullmatch(r'[1-4]/4', args.partition):
            raise ValueError('expected CI partition K/4')
        entries = integration_map(args.full, args.portable, args.partition)
        execution = audit_integration(args.log, args.full, args.portable) if args.log else None
    report = {'format': 'broca-prepush-portable-coverage-v1', 'candidate': args.candidate,
              'features': args.feature, 'entries': entries, 'execution': execution}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    destinations = collections.Counter(entry['destination'] for entry in entries)
    print(f'portable coverage: {len(entries)} tests -> {dict(destinations)}; report: {args.output}')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, OSError) as error:
        raise SystemExit('prepush-coverage: ' + str(error))
