#!/usr/bin/env python3
"""Check the workflow gate graph and exercise its helpers with no GitHub or Apple calls."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import zipfile

import yaml

ROOT = Path(__file__).resolve().parents[2]
VERSION = '1.2.3'
TAG = f'v{VERSION}'
SECRETS = {
    'DEVELOPER_ID_APPLICATION_CERTIFICATE_BASE64',
    'DEVELOPER_ID_APPLICATION_CERTIFICATE_PASSWORD',
    'APP_STORE_CONNECT_API_KEY',
    'APP_STORE_CONNECT_API_KEY_ID',
    'APP_STORE_CONNECT_API_ISSUER_ID',
}


class Workflow(unittest.TestCase):
    def test_dispatch_only_and_publish_depends_on_verified_draft(self):
        workflow = yaml.load((ROOT / '.github/workflows/release.yml').read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(set(workflow['on']), {'workflow_dispatch'})
        self.assertEqual(workflow['on']['workflow_dispatch']['inputs']['version']['required'], 'true')
        jobs = workflow['jobs']
        self.assertEqual(jobs['verify']['needs'], 'gate')
        self.assertEqual(jobs['draft']['needs'], 'verify')
        self.assertEqual(jobs['build']['needs'], 'draft')
        self.assertEqual(set(jobs['verify-assets']['needs']), {'draft', 'build'})
        self.assertEqual(set(jobs['publish']['needs']), {'draft', 'verify-assets'})
        for name, mode in [('draft', 'draft'), ('verify-assets', 'verify'), ('publish', 'publish')]:
            job = jobs[name]
            self.assertNotIn('if', job, 'a failed dependency must block this job')
            steps = [step for step in job['steps'] if f'github-assets.sh {mode}' in step.get('run', '')]
            self.assertEqual(len(steps), 1)
            self.assertNotIn('continue-on-error', steps[0])
            self.assertNotIn('if', steps[0])
        matrix = jobs['build']['strategy']['matrix']['include']
        self.assertEqual({row['target'] for row in matrix}, {
            'aarch64-apple-darwin', 'x86_64-apple-darwin', 'aarch64-unknown-linux-gnu',
            'x86_64-unknown-linux-gnu', 'aarch64-pc-windows-msvc', 'x86_64-pc-windows-msvc',
        })
        sign_step, = [step for step in jobs['build']['steps'] if 'macos-sign-notarize.sh' in step.get('run', '')]
        self.assertEqual(sign_step['if'], "runner.os == 'macOS'")
        self.assertEqual(sign_step['env'], {name: '${{ secrets.' + name + ' }}' for name in SECRETS})


class ReleaseSteps(unittest.TestCase):
    def setUp(self):
        scratch = ROOT / 'target/scratch'
        scratch.mkdir(parents=True, exist_ok=True)
        tmp = tempfile.TemporaryDirectory(prefix='public-release-jobs-', dir=scratch)
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        for name in ('scripts/release/github-assets.sh', 'scripts/release/macos-sign-notarize.sh',
                     'scripts/release/wait-workflow.sh', 'scripts/release/release.py', 'scripts/lib/exit-guard.sh'):
            (self.root / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, self.root / name)
        self.notes = self.root / '.cortexkit/release-notes' / (TAG + '.md')
        self.notes.parent.mkdir(parents=True)
        self.notes.write_text('Curated notes.\n\nWindows: release incomplete (native suite has not run).\n')
        manifest = json.loads((ROOT / 'release-manifest.json').read_text())
        (self.root / 'release-manifest.json').write_text(json.dumps({**manifest, 'requires_core': '2.3.4'}))
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.calls = self.root / 'calls'
        self.env = {name: value for name, value in os.environ.items() if name not in SECRETS}
        self.env.update(PATH=f'{self.bin}:{os.environ["PATH"]}', CALLS=str(self.calls), TAG=TAG,
                        GITHUB_REPOSITORY='cortexkit/broca', GITHUB_OUTPUT=str(self.root / 'output'), RELEASE_ID='42',
                        RUNNER_TEMP=str(self.root))
        self.stub('gh', '''
if args[:3] == ['api', '--paginate', '--slurp']:
    print('[[]]')
elif args[:2] == ['release', 'create']:
    assert args[2] == os.environ['TAG'] and '--verify-tag' in args and '--draft' in args
    assert pathlib.Path(args[args.index('--notes-file') + 1]).read_text().startswith('Curated notes.')
elif args[:2] == ['release', 'view']:
    print(42)
elif args[:2] == ['release', 'upload']:
    assert len([arg for arg in args if arg.startswith('assets/')]) == 25
elif args == ['api', 'repos/cortexkit/broca/releases/42']:
    assets = json.loads(pathlib.Path('fixture-inventory.json').read_text())
    if os.getenv('REPLACE_ASSET'):
        assets[0]['id'] += 100
    print(json.dumps({'tag_name': os.environ['TAG'], 'draft': True, 'assets': assets}))
elif args[:3] == ['api', '-H', 'Accept: application/octet-stream']:
    asset_id = int(args[-1].split('/')[-1])
    asset = next(a for a in json.loads(pathlib.Path('fixture-inventory.json').read_text()) if a['id'] == asset_id)
    payload = (pathlib.Path('assets') / asset['name']).read_bytes()
    sys.stdout.buffer.write(b'tampered' if os.getenv('CORRUPT') == asset['name'] else payload)
elif args[:3] == ['api', '--method', 'PATCH']:
    assert args[3:6] == ['repos/cortexkit/broca/releases/42', '-F', 'draft=false']
    print('published')
else:
    sys.exit(f'unexpected gh call: {args}')
''')

    def stub(self, name, body):
        path = self.bin / name
        path.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ['CALLS'], 'a') as log:
    log.write(json.dumps([pathlib.Path(sys.argv[0]).name, args]) + '\\n')
''' + body)
        path.chmod(0o755)

    def run_step(self, script, *args, **env):
        return subprocess.run(['bash', f'scripts/release/{script}.sh', *args], cwd=self.root,
                              env={**self.env, **env}, text=True, capture_output=True, timeout=20)

    def events(self, name):
        rows = self.calls.read_text().splitlines() if self.calls.exists() else []
        return [args for executable, args in map(json.loads, rows) if executable == name]

    def prepare_assets(self):
        assets = self.root / 'assets'
        assets.mkdir()
        for binary in ('ck-broca', 'broca-session'):
            for platform in ('darwin', 'linux', 'windows'):
                for arch in ('arm64', 'x64'):
                    name = f'{binary}-{platform}-{arch}.zip'
                    data = f'fixture bytes for {name}'.encode()
                    (assets / name).write_bytes(data)
                    (assets / (name + '.sha256')).write_text(f'{hashlib.sha256(data).hexdigest()}  {name}\n')
        names = sorted([asset.name for asset in assets.iterdir()] + ['release-manifest.json'])
        inventory = [{'id': idx, 'name': name, 'state': 'uploaded'} for idx, name in enumerate(names, 100)]
        (self.root / 'fixture-inventory.json').write_text(json.dumps(inventory))

    def test_draft_refuses_missing_blank_or_unmarked_notes_before_github(self):
        for content, reason in [(None, 'missing or empty'), (' \n', 'missing or empty'),
                                ('Curated notes without Windows status.\n', 'must contain the line')]:
            with self.subTest(content=content):
                if content is None:
                    self.notes.unlink()
                else:
                    self.notes.write_text(content)
                run = self.run_step('github-assets', 'draft')
                self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
                self.assertIn(reason, run.stderr)
        self.assertEqual(self.events('gh'), [])

    def test_draft_creates_unpublished_release_and_emits_id(self):
        run = self.run_step('github-assets', 'draft')
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertEqual((self.root / 'output').read_text(), 'release_id=42\n')
        self.assertEqual([cmd[:2] for cmd in self.events('gh')],
                         [['api', '--paginate'], ['release', 'create'], ['release', 'view']])

    def test_all_25_assets_are_downloaded_before_publish(self):
        self.prepare_assets()
        run = self.run_step('github-assets', 'verify')
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        calls = self.events('gh')
        self.assertEqual(calls[0][:2], ['release', 'upload'])
        self.assertEqual(len([call for call in calls if call[:2] == ['api', '-H']]), 25)
        self.assertNotIn('PATCH', [arg for call in calls for arg in call])
        self.stub('git', '''
if args[0] == 'rev-parse':
    print('verified-commit')
elif args[:4] != ['fetch', '--quiet', '--no-tags', 'origin']:
    sys.exit(f'unexpected git call: {args}')
''')
        run = self.run_step('github-assets', 'publish')
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertEqual(self.events('gh')[-1][:3], ['api', '--method', 'PATCH'])

    def test_missing_asset_refuses_before_upload(self):
        self.prepare_assets()
        (self.root / 'assets/broca-session-windows-arm64.zip').unlink()
        run = self.run_step('github-assets', 'verify')
        self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIn('wrong release asset set', run.stderr)
        self.assertEqual(self.events('gh'), [])

    def test_corrupt_download_does_not_produce_publish_inventory(self):
        self.prepare_assets()
        run = self.run_step('github-assets', 'verify', CORRUPT='release-manifest.json')
        self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIn('downloaded release-manifest.json differs', run.stderr)
        self.assertFalse((self.root / 'verified-assets/inventory.txt').exists())

    def test_publish_refuses_changed_inventory(self):
        self.prepare_assets()
        verify = self.run_step('github-assets', 'verify')
        self.assertEqual(verify.returncode, 0, verify.stdout + verify.stderr)
        run = self.run_step('github-assets', 'publish', REPLACE_ASSET='yes')
        self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIn('assets changed after verification', run.stderr)
        self.assertNotIn('PATCH', [arg for call in self.events('gh') for arg in call])

    def test_each_missing_macos_credential_is_a_hard_failure(self):
        self.stub('security', 'sys.exit("must not touch a keychain without credentials")\n')
        for missing in sorted(SECRETS):
            with self.subTest(secret=missing):
                run = self.run_step('macos-sign-notarize', **{name: 'unused fixture' for name in SECRETS - {missing}})
                self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
                self.assertIn(f'missing Actions secret: {missing}', run.stderr)
        self.assertEqual(self.events('security'), [])

    def prepare_macos(self):
        self.stub('security', '''
if args[0] == 'create-keychain':
    pathlib.Path(args[-1]).touch()
elif args[0] == 'find-identity':
    print('1) ' + 'A' * 40 + ' "Developer ID Application: fixture"')
''')
        self.stub('codesign', '''
if args[0] == '--force':
    assert args[1:4] == ['--options', 'runtime', '--timestamp']
elif args[0] == '--verify':
    assert '--strict' in args
elif args[0] == '--display':
    print('CodeDirectory flags=0x10000(runtime)', file=sys.stderr)
else:
    sys.exit(f'unexpected codesign call: {args}')
''')
        self.stub('xcrun', '''
assert args[:2] == ['notarytool', 'submit']
archive = pathlib.Path(args[2])
assert archive.is_file() and not archive.with_suffix('.zip.sha256').exists()
print(json.dumps({'id': 'fixture', 'status': os.getenv('NOTARY_RESULT', 'Accepted')}))
''')
        source = self.root / 'source'
        source.mkdir()
        for binary in ('ck-broca', 'broca-session'):
            executable = source / binary
            executable.write_text(f'#!/bin/sh\necho "{binary} {VERSION}"\n')
            executable.chmod(0o755)
        return {name: 'Zml4dHVyZQ==' if name.endswith('BASE64') else 'fixture' for name in SECRETS}

    def test_macos_notarizes_each_signed_binary_before_writing_checksums(self):
        credentials = self.prepare_macos()
        run = self.run_step('macos-sign-notarize', 'source', 'dist', 'darwin-arm64', VERSION, **credentials)
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertEqual(len(self.events('xcrun')), 2)
        self.assertEqual([args[0] for args in self.events('codesign')],
                         ['--force', '--verify', '--display'] * 2)
        for binary in ('ck-broca', 'broca-session'):
            archive = self.root / 'dist' / f'{binary}-darwin-arm64.zip'
            self.assertEqual(archive.with_suffix('.zip.sha256').read_text(),
                             f'{hashlib.sha256(archive.read_bytes()).hexdigest()}  {archive.name}\n')
            with zipfile.ZipFile(archive) as bundle:
                self.assertEqual(bundle.namelist(), [binary])
        self.assertEqual(self.events('security')[-1][0], 'delete-keychain')
        self.assertEqual(list(self.root.glob('broca-sign.*')), [])

    def test_macos_refuses_an_invalid_notary_result_even_when_notarytool_exits_zero(self):
        credentials = self.prepare_macos()
        run = self.run_step('macos-sign-notarize', 'source', 'dist', 'darwin-arm64', VERSION,
                            NOTARY_RESULT='Invalid', **credentials)
        self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIn('was not accepted', run.stderr)
        self.assertEqual(list((self.root / 'dist').glob('*.sha256')), [])
        self.assertEqual(list(self.root.glob('broca-sign.*')), [])

    def test_wait_recovers_from_a_cli_error_then_awaits_completion(self):
        self.stub('gh', '''
assert args[:3] == ['run', 'view', '123']
call = pathlib.Path(os.environ['CALLS']).read_text().count('"gh"')
if call == 1:
    sys.exit(17)
print('in_progress/' if call == 2 else 'completed/success')
''')
        self.stub('sleep', 'assert args == ["15"]\n')
        run = self.run_step('wait-workflow', 'cortexkit/broca', '123', OPERATOR_GH=str(self.bin / 'gh'))
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertEqual(len(self.events('gh')), 3)
        self.assertEqual(len(self.events('sleep')), 2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
