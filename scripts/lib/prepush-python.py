#!/usr/bin/env python3
"""Provision pure-Python PyYAML without requiring pip/ensurepip on build hosts."""
import hashlib
import io
from pathlib import Path, PurePosixPath
import sys
import tarfile
import urllib.request

URL = ('https://files.pythonhosted.org/packages/05/8e/'
       '961c0007c59b8dd7729d542c61a4d537767a59645b82a0b521206e1e25c2/'
       'pyyaml-6.0.3.tar.gz')
SHA256 = 'd76623373421df22fb4cf8817020cbb7ef15c725b9d5e45f17e189bfc384190f'


def install(destination):
    with urllib.request.urlopen(URL, timeout=60) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != SHA256:
        raise ValueError('PyYAML source checksum mismatch')
    count = 0
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as archive:
        for member in archive:
            path = PurePosixPath(member.name)
            if path.parts[:3] != ('pyyaml-6.0.3', 'lib', 'yaml') or not member.isfile():
                continue
            relative = Path(*path.parts[2:])
            if '..' in relative.parts:
                raise ValueError('unsafe PyYAML archive path')
            output = destination / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            source = archive.extractfile(member)
            if source is None:
                raise ValueError(f'PyYAML archive member has no data: {member.name}')
            output.write_bytes(source.read())
            count += 1
    if count == 0:
        raise ValueError('PyYAML source archive had no modules')
    print(f'installed PyYAML 6.0.3: {count} checksum-verified source files')


if __name__ == '__main__':
    install(Path(sys.argv[1]))
