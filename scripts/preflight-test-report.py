#!/usr/bin/env python3
"""Report libtest/nextest counts; reject empty execution and passing fixture skips."""
import collections
import json
from pathlib import Path
import re
import sys

SENTINEL = 'test_clock::test_clock_feature_is_enabled_for_retention_tests'


def text(path):
    return re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]', '', Path(path).read_text(errors='replace'))


def inventory(path, selected=False):
    suites = json.loads(Path(path).read_text())['rust-suites']
    result = {}
    for binary, suite in suites.items():
        for name, case in suite['testcases'].items():
            if not selected or (case['filter-match']['status'] == 'matches' and not case['ignored']):
                result[(binary, name)] = case
    return result


def report(mode, paths):
    if mode == 'unit-list':
        names = re.findall(r'^(.+): test$', text(paths[0]), re.MULTILINE)
        if not names:
            raise ValueError('zero lib/bin tests listed')
        for exclusion in paths[1:]:
            if names.count(exclusion) != 1:
                raise ValueError('unit exclusion must name exactly one discovered test: ' + exclusion)
        print(f'lib/bin inventory: baseline={len(names)} explicit fixture exclusions={len(paths)-1}')
    elif mode == 'inventory':
        full, selected = inventory(paths[0]), inventory(paths[1], selected=True)
        if not selected:
            raise ValueError('zero integration tests selected')
        if not selected.keys() <= full.keys():
            raise ValueError('portable inventory contains tests outside the baseline')
        counts = collections.defaultdict(lambda: collections.Counter())
        for key, case in full.items():
            state = 'ignored' if case['ignored'] else 'selected' if key in selected else 'excluded'
            counts[key[0]][state] += 1
        totals = sum(counts.values(), collections.Counter())
        print(f'integration inventory: baseline={len(full)} selected={totals["selected"]} excluded={totals["excluded"]} ignored={totals["ignored"]}')
        for binary, count in sorted(counts.items()):
            print(f'  {binary}: selected={count["selected"]} excluded={count["excluded"]} ignored={count["ignored"]}')
        for binary, name in sorted(full.keys() - selected.keys()):
            print(f'  not run in preflight: {binary} {name}')
    elif mode in ('units', 'integrations'):
        output = text(paths[0])
        if re.search(r'^\s*SKIP [a-z_]', output, re.MULTILINE):
            raise ValueError('a selected test returned a passing fixture skip')
        if mode == 'units':
            summaries = re.findall(r'^test result: \S+\. (\d+) passed; (\d+) failed; (\d+) ignored; \d+ measured; (\d+) filtered out;', output, re.MULTILINE)
            executed = sum(int(row[0]) + int(row[1]) for row in summaries)
            if not executed:
                raise ValueError('zero lib/bin tests executed')
            print(f'lib/bin executed={executed} ignored={sum(int(row[2]) for row in summaries)} excluded={sum(int(row[3]) for row in summaries)} binaries={len(summaries)}')
        else:
            summaries = re.findall(r'Summary\s+\[[^\]]+\]\s+(\d+) tests? run', output)
            if len(summaries) != 1 or int(summaries[0]) == 0:
                raise ValueError('zero or unrecognised integration execution')
            expected = len(inventory(paths[1], selected=True))
            if int(summaries[0]) != expected:
                raise ValueError(f'integration execution count differs: expected {expected}, executed {summaries[0]}')
            pattern = r'^\s*PASS\s+\[[^\]\n]+\]\s+(?:\(\s*\d+/\d+\)\s+)?broca-module::it\s+' + re.escape(SENTINEL) + r'\s*$'
            if re.search(pattern, output, re.MULTILINE) is None:
                raise ValueError('portable clock sentinel did not execute successfully')
            print(f'integration executed={summaries[0]}; clock proof passed')
    else:
        raise ValueError('unknown preflight report mode: ' + mode)


if __name__ == '__main__':
    try:
        report(sys.argv[1], sys.argv[2:])
    except (ValueError, KeyError, IndexError, OSError) as error:
        sys.exit('linux-preflight: ' + str(error))
