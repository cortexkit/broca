#!/usr/bin/env python3
"""Exercise the public operator preflight against a scratch bare origin and offline tools."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
VERSION = '1.2.3'
TAG = f'v{VERSION}'
FLOOR = '2.3.4'
NOTES = f'.cortexkit/release-notes/{TAG}.md'


class Release(unittest.TestCase):
    def setUp(self):
        scratch = ROOT / 'target/scratch'
        scratch.mkdir(parents=True, exist_ok=True)
        tmp = tempfile.TemporaryDirectory(prefix='release-preflight-', dir=scratch)
        self.addCleanup(tmp.cleanup)
        parent = Path(tmp.name)
        self.repo = parent / 'broca'
        self.repo.mkdir()
        self.git('init', '-q', '-b', 'master')
        self.git('config', 'user.email', 'test@example.invalid')
        self.git('config', 'user.name', 'Release test')
        for name in ('scripts/release.sh', 'scripts/ci-status.sh', 'scripts/release-ci-gate.sh',
                     'scripts/lib/exit-guard.sh', 'scripts/lib/operator-gh.sh', 'scripts/release/release.py'):
            self.repo.joinpath(name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, self.repo / name)
        self.repo.joinpath('.gitignore').write_text('/target/\n')
        self.repo.joinpath('Cargo.toml').write_text(f'[workspace.package]\nversion = "{VERSION}"\n')
        package = self.repo / 'crates/broca-module-serve/Cargo.toml'
        package.parent.mkdir(parents=True)
        package.write_text('[package]\nversion.workspace = true\n')
        self.repo.joinpath(NOTES).parent.mkdir(parents=True)
        self.repo.joinpath(NOTES).write_text('Release notes.\n\nWindows: release incomplete (native suite has not run).\n')
        self.manifest = json.loads((ROOT / 'release-manifest.json').read_text())
        self.write_floor(FLOOR)
        origin = parent / 'origin.git'
        subprocess.run(['git', 'init', '-q', '--bare', str(origin)], check=True)
        self.git('remote', 'add', 'origin', str(origin))
        self.land()
        core = parent / 'fixture.zip'
        with zipfile.ZipFile(core, 'w') as archive:
            archive.writestr('ck-subc', b'fixture-daemon')
        self.events = parent / 'events'
        bins = parent / 'bin'
        bins.mkdir()
        self.stub(bins / 'gh', '''
if args[:3] == ['api', '--paginate', '--slurp']:
    assert args[3] == 'repos/cortexkit/broca/releases?per_page=100', args
    print(os.environ.get('RELEASES', '[[]]'))
elif args[:2] == ['run', 'list']:
    assert os.environ['GH_REPO'] == 'cortexkit/broca'
    assert args[args.index('--commit') + 1] == os.environ['EXPECT_SHA']
    assert 'event' in args[args.index('--json') + 1].split(',')
    run = {'name': 'CI', 'databaseId': 1, 'headSha': os.environ['EXPECT_SHA'],
           'status': 'completed', 'conclusion': os.environ.get('CI_CONCLUSION', 'success'), 'event': 'push'}
    print(json.dumps([run, dict(run, event='schedule', conclusion='failure')]))
else:
    sys.exit(f'unexpected GitHub command during dry run: {args}')
''')
        self.stub(bins / 'curl', '''
source, output = args[-3], pathlib.Path(args[-1])
assert args[-2] == '-o', args
assert re.fullmatch(r'https://github.com/cortexkit/subconscious/releases/download/subc-core-v2\\.3\\.4/'
                    r'ck-subc-(darwin|linux)-(arm64|x64)\\.zip(\\.sha256)?', source), source
payload = pathlib.Path(os.environ['CORE_ZIP']).read_bytes()
if source.endswith('.sha256'):
    digest = '0' * 64 if os.environ.get('BAD_CORE') else hashlib.sha256(payload).hexdigest()
    output.write_text(f'{digest}  {output.name.removesuffix(".sha256")}\\n')
else:
    output.write_bytes(payload)
''')
        self.stub(bins / 'cargo', '''
if args[0] == 'check':
    assert args[1:-1] == ['--locked', '-p', 'broca-module-serve', '--bin'], args
    assert args[-1] in ['ck-broca', 'broca-session']
else:
    assert args == ['nextest', 'run', '--locked', '-p', 'broca-module-serve', '--no-tests=fail',
                    '-E', 'binary(it) & test(seal_line_is_filterable_e2e::)'], args
    binary = pathlib.Path(os.environ['SUBC_CORE_BIN'])
    assert binary.read_bytes() == b'fixture-daemon' and os.access(binary, os.X_OK)
    assert os.environ['BROCA_REQUIRE_SUBC'] == '1'
    sys.exit(int(os.environ.get('NEXT_EXIT', '0')))
''')
        self.env = {**os.environ, 'PATH': f'{bins}:{os.environ["PATH"]}', 'EVENTS': str(self.events),
                    'CORE_ZIP': str(core), 'OPERATOR_GH_FALLBACK_PATHS': ''}

    def stub(self, path, body):
        path.write_text('''#!/usr/bin/env python3
import hashlib, json, os, pathlib, re, sys
args = sys.argv[1:]
with open(os.environ['EVENTS'], 'a') as log:
    log.write(json.dumps([pathlib.Path(sys.argv[0]).name, args]) + '\\n')
''' + body)
        path.chmod(0o755)

    def git(self, *args):
        return subprocess.run(['git', *args], cwd=self.repo, text=True, capture_output=True, check=True).stdout.strip()

    def land(self, *, tag=True):
        self.git('add', '-A')
        self.git('commit', '-qm', 'landed version')
        self.git('push', '-q', 'origin', 'master')
        if tag:
            self.git('tag', '-fa', TAG, '-m', 'deploy tag')
            self.git('push', '-qf', 'origin', f'refs/tags/{TAG}')
        self.sha = self.git('rev-parse', 'HEAD')

    def write_floor(self, floor):
        self.repo.joinpath('release-manifest.json').write_text(json.dumps({**self.manifest, 'requires_core': floor}))

    def release(self, version=VERSION, **env):
        return subprocess.run(['bash', 'scripts/release.sh', version, '--dry-run'], cwd=self.repo,
                              env={**self.env, 'EXPECT_SHA': self.sha, **env}, text=True, capture_output=True, timeout=15)

    def assert_refused(self, run, reason):
        self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIn(reason, run.stdout + run.stderr)

    def commands(self, name):
        rows = self.events.read_text().splitlines() if self.events.exists() else []
        return [args for executable, args in map(json.loads, rows) if executable == name]

    def test_dry_run_checks_two_separate_bins_and_tests_core_without_changing_refs(self):
        local = self.git('show-ref')
        remote = self.git('ls-remote', 'origin')
        run = self.release()
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIn('dry run passed', run.stdout)
        self.assertEqual([command[-1] for command in self.commands('cargo')[:2]], ['ck-broca', 'broca-session'])
        self.assertEqual(self.commands('cargo')[2][:2], ['nextest', 'run'])
        self.assertEqual(len(self.commands('curl')), 2)
        self.assertEqual(self.git('status', '--porcelain'), '')
        self.assertEqual(self.git('show-ref'), local)
        self.assertEqual(self.git('ls-remote', 'origin'), remote)
        self.assertFalse(list(self.repo.glob('target/scratch/public-release.*')))

    def test_dirty_checkout_refuses_before_download(self):
        self.repo.joinpath(NOTES).write_text('dirty')
        self.assert_refused(self.release(), 'checkout is dirty')
        self.assertEqual(self.commands('curl'), [])

    def test_other_branch_refuses(self):
        self.git('switch', '-qc', 'dev')
        self.assert_refused(self.release(), 'checkout must be on master')

    def test_unlanded_commit_refuses(self):
        self.git('commit', '-qm', 'unmerged', '--allow-empty')
        self.assert_refused(self.release(), 'freshly fetched origin/master')

    def test_wrong_tag_commit_refuses(self):
        self.repo.joinpath(NOTES).write_text(self.repo.joinpath(NOTES).read_text() + '\nMore notes.\n')
        self.land(tag=False)
        self.assert_refused(self.release(), 'does not name master commit')

    def test_local_tag_cannot_stand_in_for_missing_remote_tag(self):
        self.git('push', '-q', 'origin', '--delete', TAG)
        self.assert_refused(self.release(), 'does not exist on origin')

    def test_version_mismatch_refuses(self):
        self.assert_refused(self.release('1.2.4'), 'version is 1.2.3, not 1.2.4')

    def test_placeholder_floor_refuses_before_download(self):
        self.write_floor('SET_PUBLIC_CORE_VERSION_BEFORE_RELEASE')
        self.land()
        self.assert_refused(self.release(), 'requires_core')
        self.assertEqual(self.commands('curl'), [])

    def test_existing_draft_published_or_newer_release_refuses(self):
        for tag, draft, reason in [(TAG, True, 'already exists (draft)'), (TAG, False, 'already exists (published)'),
                                   ('v1.3.0', False, 'must be newer')]:
            with self.subTest(tag=tag, draft=draft):
                releases = [[], [{'tag_name': tag, 'draft': draft}]]
                self.assert_refused(self.release(RELEASES=json.dumps(releases)), reason)
        self.assertEqual(self.commands('curl'), [])

    def test_failed_ci_refuses_before_download(self):
        self.assert_refused(self.release(CI_CONCLUSION='failure'), 'not concluded success')
        self.assertEqual(self.commands('curl'), [])

    def test_bad_core_checksum_refuses_before_cargo(self):
        self.assert_refused(self.release(BAD_CORE='1'), 'checksum mismatch')
        self.assertEqual(self.commands('cargo'), [])

    def test_daemon_test_failure_is_not_a_successful_dry_run(self):
        run = self.release(NEXT_EXIT='17')
        self.assertEqual(run.returncode, 17, run.stdout + run.stderr)
        self.assertNotIn('dry run passed', run.stdout)


if __name__ == '__main__':
    unittest.main(verbosity=2)
