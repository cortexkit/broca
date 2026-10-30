#!/usr/bin/env python3
"""Stage verified download/source inputs on the caller, never in an offline VM."""
import argparse
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import subprocess
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def settings(path):
    values = {}
    for line in path.read_text().splitlines():
        if line and not line.startswith('#'):
            key, value = line.split('=', 1)
            tokens = shlex.split(value)
            if len(tokens) != 1:
                raise ValueError(f'expected a literal input setting: {line}')
            values[key] = tokens[0]
    return values


def download(url, destination, algorithm, expected, cache):
    cached = cache / destination.name
    if destination.exists():
        data = destination.read_bytes()
    elif cached.exists():
        data = cached.read_bytes()
    else:
        with urllib.request.urlopen(url, timeout=60) as response:
            data = response.read()
    if hashlib.new(algorithm, data).digest() != expected:
        raise ValueError(f'{algorithm} mismatch for offline input {url}')
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(data)
    cache.mkdir(parents=True, exist_ok=True)
    if not cached.exists():
        cached.write_bytes(data)
    print(f'prepared {destination.name}: {len(data)} verified bytes', flush=True)


def checkout(repository, revision, destination):
    if not (destination / '.git').exists():
        subprocess.run(['git', 'init', '-q', str(destination)], check=True)
    current = subprocess.run(['git', '-C', str(destination), 'rev-parse', 'HEAD'],
                             text=True, capture_output=True)
    if current.returncode or current.stdout.strip() != revision:
        subprocess.run(['git', '-C', str(destination), 'fetch', '--depth', '1',
                        f'https://github.com/cortexkit/{repository}.git', revision], check=True)
        subprocess.run(['git', '-C', str(destination), 'checkout', '-q', '--detach', revision], check=True)
    actual = subprocess.check_output(['git', '-C', str(destination), 'rev-parse', 'HEAD'], text=True).strip()
    if actual != revision:
        raise ValueError(f'{repository}: expected {revision}, found {actual}')


def prepare(output, siblings, cache):
    assets = settings(ROOT / 'scripts/lib/prepush-assets.conf')
    for key in ('PYTHON_WINDOWS', 'NODE_WINDOWS'):
        url = assets[key + '_URL']
        download(url, output / url.rsplit('/', 1)[1], 'sha256',
                 bytes.fromhex(assets[key + '_SHA256']), cache)
    spec = importlib.util.spec_from_file_location('prepush_nats', ROOT / 'scripts/install-ci-nats.py')
    assert spec is not None and spec.loader is not None
    nats = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(nats)
    target, checksum = nats.ARCHIVES['Windows']
    nats_name = f'nats-server-v{nats.VERSION}-{target}.tar.gz'
    download(f'https://github.com/nats-io/nats-server/releases/download/v{nats.VERSION}/{nats_name}',
             output / nats_name, 'sha256', bytes.fromhex(checksum), cache)
    lock_bytes = (ROOT / 'package-lock.json').read_bytes()
    lock = json.loads(lock_bytes)
    packages = []
    for package in lock['packages'].values():
        if not package.get('resolved'):
            continue
        url, integrity = package['resolved'], package['integrity']
        algorithm, digest = integrity.split('-', 1)
        name = hashlib.sha256(url.encode()).hexdigest() + '.tgz'
        download(url, output / name, algorithm, base64.b64decode(digest), cache)
        packages.append(name)
    (output / 'manifest.json').write_text(json.dumps({
        'package_lock_sha256': hashlib.sha256(lock_bytes).hexdigest(),
        'npm_archives': packages, 'nats_archive': nats_name,
    }) + '\n')
    pins = settings(ROOT / 'scripts/ci-fixture-revisions.conf')
    checkout('subconscious', pins['SUBC_REV'], siblings / 'subconscious')
    checkout('claustrum', pins['CLAUSTRUM_REV'], siblings / 'claustrum')
    print(f'Windows offline inputs ready: {output}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--siblings', type=Path, required=True)
    parser.add_argument('--cache', type=Path, required=True)
    args = parser.parse_args()
    prepare(args.output, args.siblings, args.cache)
