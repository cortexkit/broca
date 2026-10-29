#!/usr/bin/env python3
"""Check file-only halves of Rust pins; executable Rust assertions still run later."""
import hashlib
import json
from pathlib import Path
import re
import sys
import tomllib


def require(condition, message):
    if not condition:
        raise ValueError(message)


def capture(pattern, source):
    match = re.search(pattern, source, re.S)
    require(match is not None, f'Rust invariant syntax changed: {pattern}')
    return match.group(1)


def check(root):
    crates = root / 'crates'
    replay = (crates / 'broca-replay/tests/bench_envelope_is_representative.rs').read_text()
    table = capture(r'const ACCEPTED:.*?= &\[(.*?)\n\];', replay)
    accepted = [set(pair) for pair in re.findall(r'\["([\d.]+)", "([\d.]+)"\]', table)]
    require(accepted, 'reviewed subc-protocol pin table is empty')
    lock = tomllib.loads((root / 'Cargo.lock').read_text())
    versions = {p['version'] for p in lock['package'] if p['name'] == 'subc-protocol'}
    require(0 < len(versions) <= 2, f'unexpected subc-protocol versions: {versions}')
    require(len(versions) == 1 or versions in accepted,
            f'subc-protocol pair needs replay envelope review: {versions}')
    print(f'PASS replay envelope pins: {sorted(versions)}')

    wire = crates / 'broca-wire'
    role = (wire / 'src/role.rs').read_text()
    pins = dict(re.findall(r'const (\w+_SHA256): &str\s*=\s*"([a-f0-9]{64})";', role))
    uses = re.findall(r'"([^"\n]+\.json)",\s*(\w+_SHA256)\s*,', role)
    for test in role.split('#[test]'):
        file = re.search(r'let file = golden\("([^"\n]+\.json)"\);', test)
        digest = re.search(r'Sha256::digest\(&file\)\),\s*(\w+_SHA256)', test)
        if file and digest:
            uses.append((file.group(1), digest.group(1)))
    require(pins and uses, 'wire golden pin list is empty or changed syntax')
    require(set(pins) == {pin for _, pin in uses}, 'wire golden pin definitions/uses differ')
    for name, pin in uses:
        actual = hashlib.sha256((wire / 'tests/golden' / name).read_bytes()).hexdigest()
        require(actual == pins[pin], f'wire golden hash mismatch: {name} ({pin})')
    lib = (wire / 'src/lib.rs').read_text()
    minimum = int(capture(r'checked >= (\d+),\s*"expected the full run.status golden inventory', lib))
    goldens = list((wire / 'tests/golden').glob('run_status.*'))
    require(len(goldens) >= minimum, f'run.status golden inventory: {len(goldens)} < {minimum}')
    print(f'PASS wire golden lists: {len(uses)} hashes, {len(goldens)} run.status files (minimum {minimum})')

    wal = crates / 'broca-wal'
    framing = (wal / 'src/framing.rs').read_text()
    test = framing.split('fn a_manifest_run_started_is_a_gated_frame_requiring_plan_manifest_v1_and_round_trips()', 1)[1].split('#[test]', 1)[0]
    tool_count = int(capture(r'assert_eq!\(manifest.tools.len\(\), (\d+)\)', test))
    fetch_count = int(capture(r'assert_eq!\(tools.len\(\), (\d+)\)', test))
    providers = json.loads('[' + capture(r'assert_eq!\(providers, \[(.*?)\]\)', test) + ']')
    manifest = json.loads((wal / 'tests/fixtures/plan_manifest_run_started.json').read_text())['manifest']
    require(len(manifest['tools']) == tool_count, 'WAL manifest fixture tool count changed')
    require(len(manifest['fetches'][0]['tools']) == fetch_count, 'WAL manifest first fetch tool count changed')
    require([f['provider'] for f in manifest['fetches']] == providers, 'WAL manifest provider list changed')
    print(f'PASS manifest counts: {tool_count} tools, {len(providers)} providers, {fetch_count} first-fetch tools')


if __name__ == '__main__':
    try:
        check(Path(__file__).resolve().parents[1])
    except (ValueError, OSError, KeyError, IndexError) as error:
        sys.exit(f'fast Rust invariants: {error}')
