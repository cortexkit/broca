#!/usr/bin/env python3
"""Exercise the Windows npm bootstrap with LF-only inputs and an empty cache."""
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class WindowsOfflineNpm(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='prepush npm ')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'scripts').mkdir()
        shutil.copy(ROOT / 'scripts/prepare-prepush-windows.py', self.root / 'scripts/prepare-prepush-windows.py')
        self.inputs = self.root / 'prepush-inputs'
        self.inputs.mkdir()
        self.scratch = self.root / 'scratch'
        self.scratch.mkdir()
        pinned = os.environ.get('BROCA_PREPUSH_NPM_ARCHIVE')
        if pinned:
            package_bytes = (ROOT / 'package.json').read_bytes()
            lock_bytes = (ROOT / 'package-lock.json').read_bytes()
            lock = json.loads(lock_bytes)
            entry = lock['packages']['node_modules/@copilotkit/aimock']
            data = Path(pinned).read_bytes()
            algorithm, digest = entry['integrity'].split('-', 1)
            self.assertEqual(hashlib.new(algorithm, data).digest(), base64.b64decode(digest))
        else:
            package = {'name': 'offline-npm-fixture', 'private': True,
                       'devDependencies': {'@copilotkit/aimock': '1.32.0'}}
            metadata = {'name': '@copilotkit/aimock', 'version': '1.32.0',
                        'bin': {'aimock': 'dist/aimock-cli.js'}}
            archive = io.BytesIO()
            with tarfile.open(fileobj=archive, mode='w:gz') as output:
                for name, body in [('package/package.json', json.dumps(metadata).encode()),
                                   ('package/dist/aimock-cli.js', b'#!/usr/bin/env node\nconsole.log("offline fixture");\n')]:
                    member = tarfile.TarInfo(name)
                    member.size = len(body)
                    member.mode = 0o755 if name.endswith('.js') else 0o644
                    output.addfile(member, io.BytesIO(body))
            data = archive.getvalue()
            entry = dict(metadata, resolved='https://registry.invalid/aimock-1.32.0.tgz',
                         integrity='sha512-' + base64.b64encode(hashlib.sha512(data).digest()).decode(), dev=True)
            lock = {'name': package['name'], 'lockfileVersion': 3, 'requires': True,
                    'packages': {'': package, 'node_modules/@copilotkit/aimock': entry}}
            package_bytes = json.dumps(package).encode()
            lock_bytes = json.dumps(lock).encode()
        resolved = entry['resolved']
        assert isinstance(resolved, str)
        self.archive_name = hashlib.sha256(resolved.encode()).hexdigest() + '.tgz'
        (self.inputs / self.archive_name).write_bytes(data)
        (self.root / 'package.json').write_bytes(package_bytes)
        (self.root / 'package-lock.json').write_bytes(lock_bytes)
        self.manifest = {'package_lock_sha256': hashlib.sha256(lock_bytes).hexdigest(),
                         'npm_archives': [self.archive_name]}
        self.write_manifest()

    def write_manifest(self):
        (self.inputs / 'manifest.json').write_text(json.dumps(self.manifest))

    def write_list(self):
        return subprocess.run([sys.executable, str(self.root / 'scripts/prepare-prepush-windows.py'),
                               '--write-npm-archives', '--inputs', str(self.inputs),
                               '--output', str(self.scratch / 'npm-archives')],
                              cwd=self.root, capture_output=True, text=True)

    def test_windows_path_installs_staged_tarball_offline_from_empty_cache(self):
        self.assertIsNotNone(shutil.which('npm'), 'npm is a required script-test prerequisite')
        self.assertIsNotNone(shutil.which('bash'), 'Git Bash/bash is required for the Windows lane path')
        source = (ROOT / 'scripts/lib/prepush-lane.sh').read_text()
        function = re.search(r'^aimock\(\) \{.*?^\}', source, re.M | re.S)
        self.assertIsNotNone(function)
        assert function is not None
        command = ('set -euo pipefail\nroot="$TEST_ROOT"\nplatform=windows\n'
                   'python_exec="$TEST_PYTHON"\nscratch="$TEST_SCRATCH"\n'
                   'BROCA_PREPUSH_INPUTS="$TEST_INPUTS"\n' + function.group(0) + '\naimock\n')
        self.assertFalse((self.scratch / 'npm-cache').exists())
        env = dict(os.environ, TEST_ROOT=self.root.as_posix(), TEST_PYTHON=Path(sys.executable).as_posix(),
                   TEST_SCRATCH=self.scratch.as_posix(), TEST_INPUTS=self.inputs.as_posix(),
                   npm_config_update_notifier='false')
        result = subprocess.run(['bash', '-c', command], cwd=self.root, env=env,
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((self.scratch / 'npm-archives').read_bytes(), (self.archive_name + '\n').encode('ascii'))
        installed = self.root / 'node_modules/@copilotkit/aimock'
        metadata = json.loads((installed / 'package.json').read_text())
        self.assertEqual(metadata['version'], '1.32.0')
        for path in metadata['bin'].values():
            self.assertTrue((installed / path).is_file())

    def test_archive_list_is_binary_lf_not_platform_text_stdout(self):
        result = self.write_list()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout, '')
        self.assertEqual((self.scratch / 'npm-archives').read_bytes(), (self.archive_name + '\n').encode('ascii'))

    def test_cr_in_manifest_name_is_rejected_before_writing(self):
        self.manifest['npm_archives'] = [self.archive_name + '\r']
        self.write_manifest()
        result = self.write_list()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('unsafe offline npm input name', result.stderr)
        self.assertFalse((self.scratch / 'npm-archives').exists())

    def test_changed_lockfile_does_not_seed_an_unrelated_cache(self):
        (self.root / 'package-lock.json').write_bytes(b'{}')
        result = self.write_list()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('do not match package-lock.json', result.stderr)
        self.assertFalse((self.scratch / 'npm-archives').exists())


if __name__ == '__main__':
    unittest.main(verbosity=2)
