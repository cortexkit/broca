#!/usr/bin/env python3
"""Install the checksum-pinned private broker used by CI's real-bus tests."""
import hashlib
import io
import os
from pathlib import Path
import platform
import subprocess
import tarfile
import urllib.request


VERSION = '2.10.26'
# From https://github.com/nats-io/nats-server/releases/download/v2.10.26/SHA256SUMS.
ARCHIVES = {
    'Linux': ('linux-amd64', '6cbac67107e69c9e5a352d6f1555027f9da6a6b3fa81cceafefa9598b4451512'),
    'Windows': ('windows-amd64', 'fa5f29094dd2d25d42060e5c9df7d86a8b8c0820b319e9c6566a0bffa923f35f'),
}


def install(destination, os_name, machine):
    if machine.lower() not in ('amd64', 'x86_64') or os_name not in ARCHIVES:
        raise RuntimeError(f'no pinned NATS archive for {os_name}/{machine}')
    target, checksum = ARCHIVES[os_name]
    stem = f'nats-server-v{VERSION}-{target}'
    url = f'https://github.com/nats-io/nats-server/releases/download/v{VERSION}/{stem}.tar.gz'
    with urllib.request.urlopen(url) as response:
        archive = response.read()
    if hashlib.sha256(archive).hexdigest() != checksum:
        raise RuntimeError(f'NATS archive checksum mismatch: {url}')
    name = 'nats-server.exe' if os_name == 'Windows' else 'nats-server'
    with tarfile.open(fileobj=io.BytesIO(archive), mode='r:gz') as package:
        member = package.getmember(f'{stem}/{name}')
        if not member.isfile():
            raise RuntimeError('NATS archive executable is not a regular file')
        source = package.extractfile(member)
        destination.mkdir(parents=True, exist_ok=True)
        binary = destination / name
        binary.write_bytes(source.read())
    binary.chmod(0o755)
    version = subprocess.run([str(binary), '--version'], check=True, capture_output=True, text=True)
    if version.stdout.strip() != f'nats-server: v{VERSION}':
        raise RuntimeError(f'unexpected NATS version: {version.stdout!r}')
    print(version.stdout.strip())
    return binary.resolve()


def main():
    binary = install(Path(os.environ['RUNNER_TEMP']) / 'broca-nats',
                     platform.system(), platform.machine())
    with open(os.environ['GITHUB_ENV'], 'a', encoding='utf-8') as env:
        env.write(f'CK_NATS_SERVER_BIN={binary}\n')
    print(f'CK_NATS_SERVER_BIN={binary}')


if __name__ == '__main__':
    main()
