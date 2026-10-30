#!/usr/bin/env python3
"""Stage verified download/source inputs on the caller, never in an offline VM."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import tarfile
import tempfile
import tomllib
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
    if subprocess.check_output(['git', '-C', str(destination), 'status', '--porcelain'], text=True).strip():
        raise ValueError(f'{repository}: refusing dirty pinned source cache: {destination}')


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


def locked_packages(lockfile):
    return [package for package in tomllib.loads(lockfile.read_text())['package'] if package.get('source')]


def verify_vendor_coverage(packages, vendor):
    available = {}
    for directory in sorted(vendor.iterdir()):
        if not directory.is_dir():
            continue
        package = tomllib.loads((directory / 'Cargo.toml').read_text())['package']
        checksum = json.loads((directory / '.cargo-checksum.json').read_text())['package']
        key = (package['name'], package['version'], checksum)
        if key in available:
            raise ValueError(f'ambiguous vendored package: {key}')
        available[key] = directory.name
    inventory = []
    for package in packages:
        key = (package['name'], package['version'], package.get('checksum'))
        if key not in available:
            raise ValueError(f"missing vendored Cargo package: {package['name']} {package['version']} ({package['source']})")
        inventory.append({'name': package['name'], 'version': package['version'],
                          'source': package['source'], 'checksum': package.get('checksum'),
                          'directory': available[key]})
    return sorted(inventory, key=lambda package: (package['name'], package['version'], package['source']))


def prepare_cargo_vendor(output, sources, source_cache):
    graphs = {}
    archive = output / 'fixture-cargo-sources.tar.gz'
    # Separate graphs can patch the same crate/version to different Git revisions.
    with tempfile.TemporaryDirectory(prefix='cargo-vendor-', dir=source_cache) as temporary:
        root = Path(temporary)
        for name, source in sources.items():
            vendor = root / name
            config = subprocess.check_output(['cargo', 'vendor', '--locked', '--versioned-dirs',
                                              '--manifest-path', str(source / 'Cargo.toml'), str(vendor)],
                                             cwd=ROOT, text=True,
                                             env=dict(os.environ, CARGO_NET_OFFLINE='false'))
            lockfile = source / 'Cargo.lock'
            packages = verify_vendor_coverage(locked_packages(lockfile), vendor)
            for path in vendor.rglob('*'):
                if path.is_symlink():
                    raise ValueError(f'unsupported Cargo vendor source symlink: {path}')
            directory = tomllib.loads(config)['source']['vendored-sources']['directory']
            if Path(directory).resolve() != vendor.resolve():
                raise ValueError(f'{name}: Cargo vendor returned an unexpected source directory')
            config = config.replace(json.dumps(directory), json.dumps('@BROCA_VENDOR_ROOT@/' + name))
            config_file = output / f'fixture-cargo-{name}.toml'
            config_file.write_text(config)
            graphs[name] = {'config': config_file.name, 'packages': packages,
                            'lock_sha256': hashlib.sha256(lockfile.read_bytes()).hexdigest()}
            print(f'{name}: verified full locked Cargo source closure: {len(packages)} packages', flush=True)
        with tarfile.open(archive, 'w:gz', dereference=True) as package:
            for name in sources:
                package.add(root / name, arcname=name)
    return {'file': archive.name, 'sha256': hashlib.sha256(archive.read_bytes()).hexdigest(), 'graphs': graphs}


def extract_cargo_vendor(inputs, destination, locks):
    entry = json.loads((inputs / 'manifest.json').read_text())['cargo_vendor']
    archive = inputs / entry['file']
    if archive.name != entry['file'] or hashlib.sha256(archive.read_bytes()).hexdigest() != entry['sha256']:
        raise ValueError('offline Cargo source archive checksum/name mismatch')
    with tarfile.open(archive, 'r:gz') as package:
        for member in package:
            relative = Path(member.name)
            if relative.is_absolute() or '..' in relative.parts or '.git' in relative.parts:
                raise ValueError(f'unsafe Cargo source archive member: {member.name}')
            path = destination / relative
            if path.is_symlink():
                raise ValueError(f'unsafe cached Cargo source symlink: {path}')
            if member.isdir():
                path.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                data = package.extractfile(member)
                if data is None:
                    raise ValueError(f'Cargo source archive member has no data: {member.name}')
                path.parent.mkdir(parents=True, exist_ok=True)
                content = data.read()
                # Keep immutable source mtimes warm for Cargo's persistent target cache.
                if not path.exists() or path.read_bytes() != content:
                    path.write_bytes(content)
                path.chmod(0o755 if member.mode & 0o111 else 0o644)
            else:
                raise ValueError(f'unsupported Cargo source archive member: {member.name}')
    configs = destination / '.configs'
    if configs.is_symlink():
        raise ValueError('unsafe cached Cargo config directory symlink')
    configs.mkdir(exist_ok=True)
    for name, lockfile in locks.items():
        graph = entry['graphs'][name]
        if hashlib.sha256(lockfile.read_bytes()).hexdigest() != graph['lock_sha256']:
            raise ValueError(f'{name}: offline Cargo closure does not match the pinned lockfile')
        packages = verify_vendor_coverage(locked_packages(lockfile), destination / name)
        if packages != graph['packages']:
            raise ValueError(f'{name}: offline Cargo package inventory mismatch')
        config_file = graph['config']
        if Path(config_file).name != config_file:
            raise ValueError(f'{name}: unsafe Cargo source config name')
        config = (inputs / config_file).read_text()
        token = json.dumps('@BROCA_VENDOR_ROOT@/' + name)
        if token not in config:
            raise ValueError(f'{name}: Cargo source replacement directory is missing')
        config = config.replace(token, json.dumps((destination / name).as_posix()))
        path = configs / f'{name}.toml'
        if path.is_symlink():
            raise ValueError(f'{name}: unsafe cached Cargo config symlink')
        if not path.exists() or path.read_text() != config:
            path.write_text(config)
        print(f'{name}: verified extracted Cargo closure: {len(packages)} packages', flush=True)


def prepare(output, source_cache, cache, *, snapshot_visible=False):
    # A fresh folder cannot accidentally ship stale runtime archives or user data.
    output.mkdir(parents=True)
    if snapshot_visible:
        require_snapshot_visibility(output)
    assets = settings(ROOT / 'scripts/lib/prepush-assets.conf')
    for tool in ('NODE_WINDOWS', 'CMAKE_WINDOWS', 'NINJA_WINDOWS'):
        url = assets[tool + '_URL']
        download(url, output / url.rsplit('/', 1)[1], 'sha256',
                 bytes.fromhex(assets[tool + '_SHA256']), cache)
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
    checkout('subconscious', pins['SUBC_REV'], source_cache / 'subconscious', full_history=True)
    checkout('claustrum', pins['CLAUSTRUM_REV'], source_cache / 'claustrum')
    vault_archive = output / 'claustrum-source.tar'
    vault_digest = source_archive(source_cache / 'claustrum', pins['CLAUSTRUM_REV'], vault_archive)
    history_bundle = output / 'subconscious-source.bundle'
    subprocess.run(['git', '-C', str(source_cache / 'subconscious'), 'bundle', 'create',
                    str(history_bundle.resolve()), 'HEAD'], check=True)
    print(f'subconscious complete history bundle: {history_bundle.stat().st_size} bytes', flush=True)
    print(f'claustrum plain source archive: {vault_archive.stat().st_size} bytes sha256={vault_digest}', flush=True)
    cargo_vendor = prepare_cargo_vendor(output, {name: source_cache / name for name in ('subconscious', 'claustrum')},
                                        source_cache)
    (output / 'manifest.json').write_text(json.dumps({
        'package_lock_sha256': hashlib.sha256(lock_bytes).hexdigest(),
        'npm_archives': packages,
        'source_archives': {'claustrum': {'revision': pins['CLAUSTRUM_REV'],
                                        'file': vault_archive.name, 'sha256': vault_digest}},
        'source_bundles': {'subconscious': {'revision': pins['SUBC_REV'],
                                          'file': history_bundle.name,
                                          'sha256': hashlib.sha256(history_bundle.read_bytes()).hexdigest()}},
        'cargo_vendor': cargo_vendor,
    }) + '\n')
    files = []
    for path in sorted(output.iterdir()):
        if path.name == 'transport-manifest.json':
            continue
        if path.is_symlink() or not path.is_file():
            raise ValueError(f'offline input is not a regular file: {path}')
        files.append({'name': path.name, 'size': path.stat().st_size,
                      'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    (output / 'transport-manifest.json').write_text(json.dumps({'version': 1, 'files': files}) + '\n')
    if snapshot_visible:
        require_snapshot_visibility(output)
    print(f'Windows offline inputs ready: {output}', flush=True)


def require_snapshot_visibility(output):
    repository = Path(subprocess.check_output(['git', '-C', str(output), 'rev-parse',
                                               '--show-toplevel'], text=True).strip())
    relative = output.resolve().relative_to(repository.resolve())
    if relative != Path('prepush-inputs') or output.is_symlink():
        raise ValueError('offline inputs must be in the worktree-root prepush-inputs/ directory')
    ignored = subprocess.run(['git', '-C', str(repository), 'check-ignore', '--no-index', '--quiet',
                              '--', str(relative)])
    if ignored.returncode == 0:
        raise ValueError('offline input directory is ignored: prepush-inputs/')
    if ignored.returncode != 1:
        ignored.check_returncode()
    visible = set(subprocess.check_output(['git', '-C', str(repository), 'ls-files',
                                         '--others', '--exclude-standard', '--', str(relative)],
                                        text=True).splitlines())
    for path in output.iterdir():
        if path.is_symlink() or not path.is_file():
            raise ValueError(f'offline input is not a regular file: {path}')
        name = str(path.resolve().relative_to(repository.resolve()))
        if name not in visible:
            raise ValueError(f'offline input is ignored or tracked and cannot be sent as an untracked snapshot input: {name}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source-cache', type=Path)
    parser.add_argument('--cache', type=Path)
    parser.add_argument('--extract-source', choices=['claustrum'])
    parser.add_argument('--extract-cargo-vendor', action='store_true')
    parser.add_argument('--subconscious-lock', type=Path)
    parser.add_argument('--claustrum-lock', type=Path)
    parser.add_argument('--inputs', type=Path)
    parser.add_argument('--revision')
    parser.add_argument('--require-snapshot-visible', action='store_true')
    args = parser.parse_args()
    if args.extract_cargo_vendor:
        if args.inputs is None or args.subconscious_lock is None or args.claustrum_lock is None:
            parser.error('--extract-cargo-vendor requires --inputs and both fixture lockfiles')
        extract_cargo_vendor(args.inputs, args.output, {'subconscious': args.subconscious_lock,
                                                      'claustrum': args.claustrum_lock})
    elif args.extract_source:
        if args.inputs is None or args.revision is None:
            parser.error('--extract-source requires --inputs and --revision')
        extract_source(args.inputs, args.extract_source, args.revision, args.output)
    else:
        if args.source_cache is None or args.cache is None:
            parser.error('preparation requires --source-cache and --cache')
        prepare(args.output, args.source_cache, args.cache,
                snapshot_visible=args.require_snapshot_visible)
