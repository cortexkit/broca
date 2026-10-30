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
import tarfile
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


def checkout(repository, revision, destination, *, full_history=False, remote_url=None):
    remote = remote_url or f'https://github.com/cortexkit/{repository}.git'
    if not (destination / '.git').exists():
        subprocess.run(['git', 'init', '-q', str(destination)], check=True)
    current = subprocess.run(['git', '-C', str(destination), 'rev-parse', 'HEAD'],
                             text=True, capture_output=True)
    shallow = subprocess.check_output(['git', '-C', str(destination), 'rev-parse',
                                       '--is-shallow-repository'], text=True).strip() == 'true'
    if current.returncode or current.stdout.strip() != revision or (full_history and shallow):
        fetch = ['git', '-C', str(destination), 'fetch']
        if full_history and shallow:
            fetch.append('--unshallow')
        elif not full_history:
            fetch.extend(['--depth', '1'])
        subprocess.run([*fetch, remote, revision], check=True)
        subprocess.run(['git', '-C', str(destination), 'checkout', '-q', '--detach', revision], check=True)
    actual = subprocess.check_output(['git', '-C', str(destination), 'rev-parse', 'HEAD'], text=True).strip()
    if actual != revision:
        raise ValueError(f'{repository}: expected {revision}, found {actual}')


def source_archive(source, revision, archive):
    subprocess.run(['git', '-C', str(source), 'archive', '--format=tar',
                    '--output', str(archive.resolve()), revision], check=True)
    return hashlib.sha256(archive.read_bytes()).hexdigest()


def extract_source(inputs, repository, revision, destination):
    manifest = json.loads((inputs / 'manifest.json').read_text())
    entry = manifest['source_archives'][repository]
    name = entry['file']
    if entry['revision'] != revision or Path(name).name != name:
        raise ValueError(f'offline {repository} source does not match the pinned revision')
    archive = inputs / name
    if hashlib.sha256(archive.read_bytes()).hexdigest() != entry['sha256']:
        raise ValueError(f'offline {repository} source checksum mismatch')
    with tarfile.open(archive, 'r:') as package:
        if package.pax_headers.get('comment') != revision:
            raise ValueError(f'offline {repository} archive commit does not match the pinned revision')
        for member in package:
            relative = Path(member.name)
            if relative.is_absolute() or '..' in relative.parts or '.git' in relative.parts:
                raise ValueError(f'unsafe source archive member: {member.name}')
            output = destination / relative
            if member.isdir():
                output.mkdir(parents=True, exist_ok=True, mode=0o755)
            elif member.isfile():
                source = package.extractfile(member)
                if source is None:
                    raise ValueError(f'source archive member has no data: {member.name}')
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(source.read())
                output.chmod(0o755 if member.mode & 0o111 else 0o644)
            else:
                raise ValueError(f'unsupported source archive member: {member.name}')
    print(f'verified and extracted {repository} source {revision}: {archive.stat().st_size} bytes')


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
    pins = settings(ROOT / 'scripts/ci-fixture-revisions.conf')
    # Subc's build scripts embed Git HEAD and dirty state, with no override. Keep
    # that CI identity, and provide a complete graph for motor's Git bundle import.
    checkout('subconscious', pins['SUBC_REV'], siblings / 'subconscious', full_history=True)
    checkout('claustrum', pins['CLAUSTRUM_REV'], siblings / 'claustrum')
    vault_archive = output / 'claustrum-source.tar'
    vault_digest = source_archive(siblings / 'claustrum', pins['CLAUSTRUM_REV'], vault_archive)
    history_bundle = cache / f"subconscious-{pins['SUBC_REV']}.bundle"
    subprocess.run(['git', '-C', str(siblings / 'subconscious'), 'bundle', 'create',
                    str(history_bundle.resolve()), 'HEAD'], check=True)
    print(f'subconscious complete history bundle: {history_bundle.stat().st_size} bytes', flush=True)
    print(f'claustrum plain source archive: {vault_archive.stat().st_size} bytes sha256={vault_digest}', flush=True)
    (output / 'manifest.json').write_text(json.dumps({
        'package_lock_sha256': hashlib.sha256(lock_bytes).hexdigest(),
        'npm_archives': packages, 'nats_archive': nats_name,
        'source_archives': {'claustrum': {'revision': pins['CLAUSTRUM_REV'],
                                        'file': vault_archive.name, 'sha256': vault_digest}},
    }) + '\n')
    print(f'Windows offline inputs ready: {output}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--siblings', type=Path)
    parser.add_argument('--cache', type=Path)
    parser.add_argument('--extract-source', choices=['claustrum'])
    parser.add_argument('--inputs', type=Path)
    parser.add_argument('--revision')
    args = parser.parse_args()
    if args.extract_source:
        if args.inputs is None or args.revision is None:
            parser.error('--extract-source requires --inputs and --revision')
        extract_source(args.inputs, args.extract_source, args.revision, args.output)
    else:
        if args.siblings is None or args.cache is None:
            parser.error('preparation requires --siblings and --cache')
        prepare(args.output, args.siblings, args.cache)
