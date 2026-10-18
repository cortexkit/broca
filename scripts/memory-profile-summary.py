#!/usr/bin/env python3
"""Symbolize allocation-site snapshots from memory-load-server (binutils addr2line)."""
import argparse
import json
from pathlib import Path
import subprocess
import re


def owner(frames):
    text = '\n'.join(frames)
    rules = [
        ('gzip', 'gzip request copy'),
        ('canonical_json_bytes', 'rendered retry bytes'),
        ('broca_catalog::runtime', 'catalog (10k)'),
        ('LastEpisodeV1 as core::clone::Clone', 'prior checkpoint clone'),
        ('resume_state_from_store', 'resume reconstruction'),
        ('reconstruct_prior', 'run prior reconstruction'),
        ('run_body_with_approval_history', 'canonical step options'),
        ('read_prefix', 'store read prefixes'),
        ('read_page', 'store read prefixes'),
        ('checkpoint', 'store/checkpoint decode'),
        ('admission', 'store/checkpoint decode'),
        ('broca_module::fanout', 'fanout rings/subscribers'),
        ('crates/broca-module/src/opener.rs:340', 'fanout rings/subscribers'),
        ('crates/broca-module/src/opener.rs:327', 'fanout rings/subscribers'),
        ('broca_module_serve::serve::handle_control_request', 'inbound route bindings'),
        ('broca_module_serve::serve::subscribe', 'subscribe forwarding'),
        ('broca_module::actor', 'actor tasks/channels'),
        ('broca_module::registry', 'registry/ref/retention'),
        ('broca_core::wal_writer', 'WAL tasks/refs'),
        ('broca_provider', 'provider HTTP/runtime'),
        ('tokio::', 'Tokio/runtime other'),
    ]
    for pattern, label in rules:
        if pattern in text:
            return label
    return 'other/unresolved'


def brief(frames):
    selected = [f for f in frames if '/crates/broca-' in f]
    if not selected:
        selected = frames[:2]
    result = []
    for frame in selected[:4]:
        function, _, location = frame.partition(' @ ')
        location = re.sub(r'^.*?/crates/', 'crates/', location)
        while re.search(r'<[^<>]*>', function):
            function = re.sub(r'<[^<>]*>', '', function)
        result.append(function[:100] + '@' + location)
    return ' <- '.join(result)


def symbolize(binary, profiles):
    addresses = sorted({address for profile in profiles.values() for site in profile['sites']
                        for address in site['stack'] if address})
    output = subprocess.run(['addr2line', '-C', '-f', '-e', str(binary)],
        input=''.join(f'{address:x}\n' for address in addresses), text=True,
        stdout=subprocess.PIPE, check=True).stdout.splitlines()
    assert len(output) == 2 * len(addresses)
    return {address: f'{output[2*i]} @ {output[2*i+1]}' for i, address in enumerate(addresses)}


def trace(site, symbols):
    frames = [symbols[address] for address in site['stack'] if address]
    return [frame for frame in frames if not frame.startswith(('??', 'broca_heap_profile',
        '<broca_heap_profile', 'alloc::', '<alloc::', 'core::', '<core::', 'std::', '<std::', '__rust', '___rust'))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=Path)
    parser.add_argument('profiles', type=Path)
    parser.add_argument('--phases', nargs='+', default=['steady', 'idle'])
    parser.add_argument('--compact', action='store_true')
    args = parser.parse_args()
    profiles = {path.stem: json.loads(path.read_text()) for path in args.profiles.glob('*.json') if path.stem != 'sqlite-memory'}
    symbols = symbolize(args.binary, profiles)
    (args.profiles / 'symbols.txt').write_text(json.dumps(symbols))
    for name, profile in sorted(profiles.items()):
        print(f'{name}: live={profile["live_bytes"]} blocks={profile["live_blocks"]} peak={profile["exact_peak_bytes"]} sampled_peak={profile["sampled_peak_bytes"]}')
    for name in args.phases:
        profile = profiles[name]
        groups = {}
        for site in profile['sites']:
            label = owner(trace(site, symbols))
            groups[label] = groups.get(label, 0) + site['live']['bytes']
        print('OWNERS ' + name + ': ' + json.dumps(groups, sort_keys=True))
        for peak in [False, True] if name == 'steady' else [False]:
            field = 'at_peak_bytes' if peak else None
            sites = sorted(profile['sites'], key=lambda s: s[field] if peak else s['live']['bytes'], reverse=True)
            print(f'\nTOP30 {name} {"simultaneous-peak" if peak else "live"}:')
            for site in sites[:30]:
                size = site[field] if peak else site['live']['bytes']
                blocks = site['at_peak_blocks'] if peak else site['live']['blocks']
                frames = trace(site, symbols)
                print(f'{size:12} {blocks:9} ' + (brief(frames) if args.compact else ' <- '.join(frames[:5])))
    # Keep every site, not just the top thirty, for subsequent owner accounting.
    for profile in profiles.values():
        for site in profile['sites']:
            site['symbols'] = trace(site, symbols)
    (args.profiles / 'symbolized.txt').write_text(json.dumps(profiles))


if __name__ == '__main__':
    main()
