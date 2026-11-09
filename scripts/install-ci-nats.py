#!/usr/bin/env python3
"""Install the checksum-pinned private broker used by CI's real-bus tests."""
import hashlib
import io
import os
from pathlib import Path
import platform
import subprocess
import tarfile
import time
import urllib.error
import urllib.request


VERSION = '2.10.26'
# From https://github.com/nats-io/nats-server/releases/download/v2.10.26/SHA256SUMS.
ARCHIVES = {
    'Linux': ('linux-amd64', '6cbac67107e69c9e5a352d6f1555027f9da6a6b3fa81cceafefa9598b4451512'),
    'Windows': ('windows-amd64', 'fa5f29094dd2d25d42060e5c9df7d86a8b8c0820b319e9c6566a0bffa923f35f'),
}
# GitHub's release download answers 5xx or drops the connection now and then; one such answer
# used to fail every integration shard of a CI run. The archive is checksum-pinned, so a retry
# can't install anything but the pinned bytes. A 4xx is a wrong URL or a removed release and is
# raised at once.
ATTEMPTS = 4
RETRY_DELAYS_S = (5, 15, 30)
REQUEST_TIMEOUT_S = 60


def download(url, sleep=time.sleep):
    for attempt in range(1, ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(url, timeout=REQUEST_TIMEOUT_S) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code < 500 or attempt == ATTEMPTS:
                raise
            reason = f'HTTP {error.code}'
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            if attempt == ATTEMPTS:
                raise
            reason = str(error)
        delay = RETRY_DELAYS_S[attempt - 1]
        print(f'NATS download attempt {attempt} failed ({reason}); retrying in {delay} s')
        sleep(delay)


def install(destination, os_name, machine, archive_path=None):
    if machine.lower() not in ('amd64', 'x86_64') or os_name not in ARCHIVES:
        raise RuntimeError(f'no pinned NATS archive for {os_name}/{machine}')
    target, checksum = ARCHIVES[os_name]
    stem = f'nats-server-v{VERSION}-{target}'
    url = f'https://github.com/nats-io/nats-server/releases/download/v{VERSION}/{stem}.tar.gz'
    if archive_path is None:
        archive = download(url)
    else:
        archive = Path(archive_path).read_bytes()
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
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', help='read a pre-staged archive; still verify checksum and version')
    args = parser.parse_args()
    binary = install(Path(os.environ['RUNNER_TEMP']) / 'broca-nats',
                     platform.system(), platform.machine(), args.archive)
    with open(os.environ['GITHUB_ENV'], 'a', encoding='utf-8') as env:
        env.write(f'CK_NATS_SERVER_BIN={binary}\n')
    print(f'CK_NATS_SERVER_BIN={binary}')


if __name__ == '__main__':
    main()
