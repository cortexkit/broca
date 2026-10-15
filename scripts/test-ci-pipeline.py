#!/usr/bin/env python3
"""Exercise archive/shard guards and the workflow dependency graph without compiling."""
import json
import io
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tarfile
import unittest
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
CARGO = r'''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ['EVENTS'], 'a') as f:
    f.write(json.dumps(args) + '\n')
if args[:2] == ['nextest', 'archive']:
    pathlib.Path(args[args.index('--archive-file') + 1]).write_text('fixture archive')
elif args[:2] == ['nextest', 'list']:
    print(json.dumps({'test-count': int(os.environ.get('NEXTEST_COUNT', '1'))}))
elif args[:2] == ['nextest', 'run']:
    if not os.environ.get('NO_CLOCK_PASS'):
        print('PASS [0.01s] ( 1/1) broca-module::it test_clock::test_clock_feature_is_enabled_for_retention_tests')
    print('Summary [0.01s] 1 test run: 1 passed')
    sys.exit(int(os.environ.get('RUN_EXIT', '0')))
elif args[0] == 'test' and '--list' in args:
    for binary in ('fixture', 'live', 'live_e2e'):
        print(f'Running tests/{binary}.rs (target/debug/{binary})')
        print('1 test, 0 benchmarks')
else:
    sys.exit('unexpected cargo: ' + repr(args))
'''


class ArchiveShards(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='broca-ci-archive-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'scripts').mkdir()
        for name in ('ci-integration.sh', 'rust-test-gate.sh'):
            shutil.copy2(ROOT / 'scripts' / name, self.root / 'scripts' / name)
        bins = self.root / 'bin'
        bins.mkdir()
        for name, text in (('cargo', CARGO), ('cargo-nextest', '#!/bin/sh\nexit 0\n'),
                           ('sleep', '#!/bin/sh\n/bin/sleep 0.01\n')):
            path = bins / name
            path.write_text(text)
            path.chmod(0o755)
        self.events = self.root / 'events'
        self.env = {**os.environ, 'PATH': f'{bins}:{os.environ["PATH"]}',
                    'EVENTS': str(self.events), 'GITHUB_ACTIONS': 'true', 'NEXTEST_PROFILE': 'ci'}
        self.archive = self.root / 'tests.tar.zst'

    def run_mode(self, *args, **env):
        return subprocess.run(['bash', 'scripts/ci-integration.sh', *args], cwd=self.root,
                              env={**self.env, **env}, text=True, capture_output=True, timeout=10)

    def commands(self):
        return [json.loads(line) for line in self.events.read_text().splitlines()]

    def test_build_checks_independent_inventories_before_archive(self):
        result = self.run_mode('build', str(self.archive))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.archive.read_text(), 'fixture archive')
        commands = self.commands()
        self.assertEqual(commands[0][0], 'test')
        self.assertEqual(commands[1][:2], ['nextest', 'list'])
        self.assertEqual(commands[2][:2], ['nextest', 'archive'])
        self.assertIn('--locked', commands[2])
        self.assertIn('broca-module-serve/test-clock', commands[2])

    def test_inventory_mismatch_refuses_archive(self):
        result = self.run_mode('build', str(self.archive), NEXTEST_COUNT='2')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('differs from independent libtest', result.stderr)
        self.assertFalse(self.archive.exists())

    def test_run_uses_archive_partition_and_original_selection_without_compiling(self):
        result = self.run_mode('run', str(self.archive), '3/4')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        commands = self.commands()
        self.assertEqual(len(commands), 2)
        for args in commands:
            self.assertEqual(args[:2], ['nextest', 'run'])
            self.assertIn('--archive-file', args)
            # The script resolves its root (`pwd -P`); on macOS /var is a symlink to /private/var.
            self.assertEqual(Path(args[args.index('--extract-to') + 1]).resolve(), self.root.resolve())
            self.assertIn('--success-output', args)
        shard = commands[1]
        self.assertEqual(shard[shard.index('--partition') + 1], 'hash:3/4')
        self.assertEqual(shard[shard.index('-E') + 1], 'kind(test) and not (binary(=live) or binary(=live_e2e))')

    def test_missing_clock_pass_refuses_shard(self):
        result = self.run_mode('run', str(self.archive), '1/4', NO_CLOCK_PASS='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('sentinel did not execute successfully', result.stderr)
        self.assertEqual(len(self.commands()), 1)

    def test_test_failure_is_not_masked_by_tee(self):
        result = self.run_mode('run', str(self.archive), '1/4', RUN_EXIT='19')
        self.assertEqual(result.returncode, 19)

    def test_shard_reports_failure_output_immediately_without_stopping_other_tests(self):
        result = self.run_mode('run', str(self.archive), '1/4')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        shard = self.commands()[1]
        self.assertEqual(shard[shard.index('--failure-output') + 1], 'immediate-final')
        config = (ROOT / '.config/nextest.toml').read_text()
        import tomllib
        self.assertFalse(tomllib.loads(config)['profile']['ci']['fail-fast'])


class WorkflowGraph(unittest.TestCase):
    @staticmethod
    def workflow(name):
        return yaml.load((ROOT / '.github/workflows' / name).read_text(), Loader=yaml.BaseLoader)

    def test_portable_lanes_start_without_waiting_for_tests(self):
        jobs = self.workflow('ci.yml')['jobs']
        for name in ('lint', 'scripts', 'integration-build', 'fixtures'):
            self.assertNotIn('needs', jobs[name], name)
        self.assertEqual(jobs['integration']['strategy']['matrix']['shard'], ['1', '2', '3', '4'])
        self.assertEqual(jobs['integration']['strategy']['matrix']['os'], ['ubuntu', 'windows'])

    def test_shards_prepare_immediately_and_wait_only_for_platform_artifacts(self):
        jobs = self.workflow('ci.yml')['jobs']
        self.assertNotIn('needs', jobs['integration'])
        steps = jobs['integration']['steps']
        wait = next(i for i, step in enumerate(steps) if 'wait-ci-artifacts.py' in step.get('run', ''))
        self.assertTrue(any(step.get('run') == 'npm ci' for step in steps[:wait]))
        self.assertIn('"fixtures-${{ runner.os }}" "integration-${{ matrix.os }}"', steps[wait]['run'])
        self.assertEqual(steps[wait]['env']['GH_TOKEN'], '${{ github.token }}')
        downloads = [i for i, step in enumerate(steps) if step.get('uses', '').startswith('actions/download-artifact@')]
        self.assertTrue(downloads)
        self.assertTrue(all(i > wait for i in downloads))
        self.assertEqual(self.workflow('ci.yml')['permissions']['actions'], 'read')
        self.assertEqual(set(jobs['test']['needs']),
                         {'lint', 'scripts', 'unit', 'integration-build', 'fixtures', 'integration', 'linux-deploy'})

    def test_scripts_prefetch_locked_dependencies_before_offline_suites(self):
        steps = self.workflow('ci.yml')['jobs']['scripts']['steps']
        fetch = next(i for i, step in enumerate(steps) if step.get('run') == 'cargo fetch --locked')
        suites = next(i for i, step in enumerate(steps) if step.get('run') == 'scripts/script-suites.sh')
        self.assertLess(fetch, suites)
        self.assertTrue(any(step.get('uses', '').startswith('dtolnay/rust-toolchain@') for step in steps[:fetch]))

    def test_fixtures_are_pinned_and_shared_without_changing_mutation_selection(self):
        fixtures = self.workflow('fixtures.yml')['jobs']['fixtures']
        pins = (ROOT / 'scripts/ci-fixture-revisions.conf').read_text()
        for key in ('SUBC_REV', 'CLAUSTRUM_REV'):
            self.assertRegex(pins, key + r'=[a-f0-9]{40}\b')
        load = next(s for s in fixtures['steps'] if s.get('name') == 'Load shared fixture pins')
        self.assertIn('source broca/scripts/ci-fixture-revisions.conf', load['run'])
        build = next(s for s in fixtures['steps'] if s.get('name') == 'Build fixtures once')
        self.assertIn('"subconscious/target/debug/ck$suffix"', build['run'])
        verify = next(s for s in fixtures['steps'] if s.get('name') == 'Verify and package fixtures')
        self.assertIn('for name in ck-subc fake-aft-stub ck ck-claustrum ck-auth', verify['run'])
        nightly = next(s for s in fixtures['steps'] if s.get('name') == 'Resolve nightly sibling revisions')
        self.assertEqual(nightly['if'], "github.event_name == 'schedule'")
        self.assertEqual(nightly['run'].count('git ls-remote'), 2)
        mutations = self.workflow('mutations.yml')['jobs']
        for name in ('mutations', 'mutations-windows'):
            job = mutations[name]
            # Shards wait for the shared fixtures and for the static platform check, so an
            # invalid platform claim never starts a native shard.
            self.assertEqual(job['needs'], ['fixtures', 'mutation-platforms'])
            count = 32 if name == 'mutations' else 4
            self.assertEqual(job['strategy']['matrix']['shard'], [str(i) for i in range(1, count + 1)])
            script = '\n'.join(s.get('run', '') for s in job['steps'])
            self.assertIn('ckdev-mutate check', script)
            self.assertIn('ckdev-mutate run --all --shard', script)
            self.assertIn('ckdev-mutate run --diff', script)
        self.assertIn('--broad', '\n'.join(s.get('run', '') for s in mutations['mutations']['steps']))

    def test_required_status_refuses_failed_skipped_cancelled_or_empty_lanes(self):
        job = self.workflow('ci.yml')['jobs']['test']
        self.assertEqual(job['if'], 'always()')
        script = job['steps'][0]['run']
        for result in ('success', 'failure', 'skipped', 'cancelled', 'empty'):
            lanes = {} if result == 'empty' else {'integration': {'result': result}}
            run = subprocess.run(['bash', '-c', script], env={**os.environ, 'NEEDS_JSON': json.dumps(lanes)},
                                 text=True, capture_output=True, timeout=5)
            self.assertEqual(run.returncode == 0, result == 'success', run.stdout + run.stderr)


class MutationReplay(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='broca-ci-mutations-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.events = self.root / 'events.jsonl'
        self.tool('ckdev-mutate', '''#!/usr/bin/env python3
import json, os, sys
with open(os.environ['EVENTS'], 'a') as f:
    f.write(json.dumps(sys.argv[1:]) + '\\n')
''')
        self.env = {**os.environ, 'PATH': str(self.bin) + os.pathsep + os.environ['PATH'],
                    'EVENTS': str(self.events), 'EVENT': 'push', 'REF': 'refs/heads/master',
                    'PR_BASE': '', 'PUSH_BASE': '', 'SHARD': '1', 'SHARDS': '32'}
        self.job = WorkflowGraph.workflow('mutations.yml')['jobs']['mutations']
        self.replay = next(s for s in self.job['steps'] if s.get('name') == 'Replay selected proofs')

    def tool(self, name, source, directory=None):
        path = (directory or self.bin) / name
        path.write_text(source)
        path.chmod(0o755)
        return path

    def run_replay(self, **env):
        self.events.unlink(missing_ok=True)
        run = subprocess.run(['bash', '-c', self.replay['run']], cwd=self.root,
                             env={**self.env, **env}, text=True, capture_output=True, timeout=5)
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        return [json.loads(line) for line in self.events.read_text().splitlines()]

    def test_linux_replay_modes_keep_full_master_and_nightly_breadth(self):
        cases = [
            ({}, ['--all'], 'all'),
            ({'EVENT': 'workflow_dispatch'}, ['--all'], 'all'),
            ({'EVENT': 'schedule'}, ['--all', '--broad'], 'broad'),
            ({'EVENT': 'pull_request', 'PR_BASE': 'pr-base'}, ['--diff', 'pr-base'], 'diff'),
            ({'REF': 'refs/heads/train/probe', 'PUSH_BASE': 'push-base'}, ['--diff', 'push-base'], 'diff'),
            ({'REF': 'refs/heads/train/new', 'PUSH_BASE': '0' * 40}, ['--diff', 'origin/master'], 'diff'),
        ]
        for env, selection, report in cases:
            with self.subTest(env=env):
                self.assertEqual(self.run_replay(**env), [
                    ['run', *selection, '--shard', '1/32', '--report', f'target/mutations/{report}.json']])

    def test_linux_matrix_uses_same_shard_denominator(self):
        shards = self.job['strategy']['matrix']['shard']
        self.assertEqual(shards, [str(i) for i in range(1, 33)])
        self.assertEqual(self.job['timeout-minutes'], "${{ github.event_name == 'schedule' && 180 || 90 }}")
        self.assertEqual(self.replay['env']['SHARDS'], '${{ strategy.job-total }}')
        for shard in shards:
            with self.subTest(shard=shard):
                args, = self.run_replay(SHARD=shard, SHARDS=str(len(shards)))
                self.assertEqual(args[args.index('--shard') + 1], f'{shard}/{len(shards)}')

    def test_windows_command_proofs_resolve_git_bash_before_wsl(self):
        steps = WorkflowGraph.workflow('mutations.yml')['jobs']['mutations-windows']['steps']
        setup = next(i for i, s in enumerate(steps) if s.get('name') == 'Select Git Bash for native command proofs')
        check = next(i for i, s in enumerate(steps) if s.get('name') == 'Check native command interpreter')
        validate = next(i for i, s in enumerate(steps) if s.get('name') == 'Validate catalogue')
        self.assertLess(setup, check)
        self.assertLess(check, validate)
        git_bin = self.root / 'Git Bash'
        git_bin.mkdir()
        real_bash = shutil.which('bash')
        assert real_bash is not None, 'the workflow suite requires Bash'
        self.tool('bash', '#!/bin/sh\necho "WSL has no installed distributions" >&2\nexit 1\n')
        self.tool('bash', f'#!/bin/sh\nexec "{real_bash}" "$@"\n', git_bin)
        self.tool('cygpath', f'#!/bin/sh\nprintf "%s\\n" "{git_bin / "bash"}"\n')
        scripts = self.root / 'scripts'
        scripts.mkdir()
        shutil.copyfile(ROOT / 'scripts/mutation-bash.py', scripts / 'mutation-bash.py')
        env = {**self.env, 'GITHUB_ENV': str(self.root / 'github-env')}
        env.pop('BROCA_MUTATION_BASH', None)
        before = subprocess.run([real_bash, '-c', steps[check]['run']], cwd=self.root,
                                env=env, text=True, capture_output=True, timeout=5)
        self.assertNotEqual(before.returncode, 0, before.stdout + before.stderr)
        run = subprocess.run([real_bash, '-c', steps[setup]['run']], cwd=self.root,
                             env=env, text=True, capture_output=True, timeout=5)
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        for setting in Path(env['GITHUB_ENV']).read_text().splitlines():
            key, value = setting.split('=', 1)
            env[key] = value
        after = subprocess.run([real_bash, '-c', steps[check]['run']], cwd=self.root,
                               env=env, text=True, capture_output=True, timeout=5)
        self.assertEqual(after.returncode, 0, after.stdout + after.stderr)
        self.assertIn('GNU bash', after.stdout)

    def test_catalogue_shell_rows_use_native_launcher(self):
        import tomllib
        rows = tomllib.loads((ROOT / 'mutations.toml').read_text())['control']
        commands = [row.get('command', []) for row in rows]
        self.assertTrue(any(command[:3] == ['python3', '-B', 'scripts/mutation-bash.py']
                            for command in commands))
        self.assertFalse(any(command[:1] == ['bash'] for command in commands))

    def test_native_launcher_keeps_shell_argv_and_exit_status(self):
        bash = shutil.which('bash')
        assert bash is not None
        run = subprocess.run([sys.executable, '-B', str(ROOT / 'scripts/mutation-bash.py'),
                              '-c', 'printf "%s\\n" "$1"; exit 7', '--', 'a test with spaces'],
                             env={**self.env, 'BROCA_MUTATION_BASH': bash},
                             capture_output=True, text=True, timeout=5)
        self.assertEqual(run.returncode, 7, run.stdout + run.stderr)
        self.assertEqual(run.stdout, 'a test with spaces\n')

    def test_native_launcher_refuses_windows_without_explicit_path(self):
        spec = importlib.util.spec_from_file_location('mutation_bash', ROOT / 'scripts/mutation-bash.py')
        assert spec is not None and spec.loader is not None
        launcher = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(launcher)
        with patch.dict(os.environ, {}, clear=True), patch.object(launcher.sys, 'platform', 'win32'):
            with self.assertRaisesRegex(SystemExit, 'BROCA_MUTATION_BASH'):
                launcher.bash_path()
        for configured in ('relative/bash', str(self.root / 'missing-bash')):
            with self.subTest(configured=configured), patch.dict(os.environ, {'BROCA_MUTATION_BASH': configured}):
                with self.assertRaisesRegex(SystemExit, 'existing absolute Bash'):
                    launcher.bash_path()


class ArtifactWait(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('wait_ci_artifacts', ROOT / 'scripts/wait-ci-artifacts.py')
        assert spec is not None and spec.loader is not None
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)

    @staticmethod
    def artifacts(*names):
        return {'artifacts': [{'name': name, 'expired': False} for name in names]}

    @staticmethod
    def job(name, status='in_progress', conclusion=None):
        return {'name': name, 'status': status, 'conclusion': conclusion}

    def run_wait(self, replies, timeout=10.0):
        with patch.object(self.module, 'api', side_effect=replies) as api, \
             patch.object(self.module.time, 'sleep'), \
             patch('sys.argv', ['wait-ci-artifacts.py', 'fixtures-Windows', 'integration-windows',
                                '--timeout', str(timeout), '--poll-seconds', '0']), \
             patch.dict(os.environ, GITHUB_REPOSITORY='cortexkit/broca', GITHUB_RUN_ID='42'):
            self.module.main()
            return api

    def test_windows_ready_does_not_wait_for_linux_or_producer_post_steps(self):
        api = self.run_wait([self.artifacts('fixtures-Windows', 'integration-windows')])
        self.assertEqual(api.call_count, 1)
        self.assertEqual(api.call_args.args[0], 'repos/cortexkit/broca/actions/runs/42/artifacts?per_page=100&page=1')

    def test_waits_for_windows_archive_but_ignores_failed_linux_producer(self):
        api = self.run_wait([self.artifacts('fixtures-Windows'),
                             {'jobs': [self.job('Archive (windows)'), self.job('Archive (ubuntu)', 'completed', 'failure')]},
                             self.artifacts('fixtures-Windows', 'integration-windows')])
        self.assertEqual(api.call_count, 3)

    def test_failed_required_producer_is_reported_by_name(self):
        with self.assertRaisesRegex(RuntimeError, r'Producer failed: fixtures / Fixtures \(Windows\) \(failure\)'):
            self.run_wait([self.artifacts('integration-windows'),
                           {'jobs': [self.job('fixtures / Fixtures (Windows)', 'completed', 'failure')]}])

    def test_completed_producers_without_artifacts_refuse(self):
        with self.assertRaisesRegex(RuntimeError, 'Producers finished without artifacts: integration-windows'):
            self.run_wait([self.artifacts('fixtures-Windows'),
                           {'jobs': [self.job('Archive (windows)', 'completed', 'success')]}])

    def test_expired_archive_is_not_replayed(self):
        response = self.artifacts('fixtures-Windows', 'integration-windows')
        response['artifacts'][1]['expired'] = True
        with self.assertRaisesRegex(RuntimeError, 'without artifacts: integration-windows'):
            self.run_wait([response, {'jobs': [self.job('Archive (windows)', 'completed', 'success')]}])

    def test_queued_producer_has_bounded_wait(self):
        with self.assertRaisesRegex(RuntimeError, 'Timed out waiting for artifacts: integration-windows'):
            self.run_wait([self.artifacts('fixtures-Windows'), {'jobs': []}], timeout=0.000001)

    def test_query_failure_refuses_instead_of_replaying(self):
        with patch.object(self.module.subprocess, 'run', return_value=subprocess.CompletedProcess([], 1, '', 'forbidden')):
            with self.assertRaisesRegex(RuntimeError, 'GitHub artifact query failed: forbidden'):
                self.module.api('repos/cortexkit/broca/actions/runs/42/artifacts')

    def test_paginates_artifacts_before_deciding_windows_is_missing(self):
        response = self.artifacts(*['other-' + str(i) for i in range(100)])
        api = self.run_wait([response, self.artifacts('fixtures-Windows', 'integration-windows')])
        self.assertTrue(api.call_args.args[0].endswith('page=2'))


class FixtureUnpack(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='broca-fixture-unpack-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.archive = self.root / 'archive with spaces.tar.gz'
        self.destination = self.root / 'destination with spaces'
        self.destination.mkdir()
        content = b'#!/bin/sh\nexit 0\n'
        with tarfile.open(self.archive, 'w:gz') as tar:
            member = tarfile.TarInfo('fixtures/bin/ck')
            member.mode = 0o755
            member.size = len(content)
            tar.addfile(member, io.BytesIO(content))
        bins = self.root / 'bin'
        bins.mkdir()
        cygpath = bins / 'cygpath'
        cygpath.write_text(r'''#!/usr/bin/env python3
import os, sys
assert sys.argv[1] == '-u'
assert sys.argv[2].startswith('D:'), 'must translate the original native drive path'
if os.environ.get('CYGPATH_EXIT'): sys.exit(int(os.environ['CYGPATH_EXIT']))
print(os.environ['TEST_ARCHIVE'] if sys.argv[2].endswith('.tar.gz') else os.environ['TEST_DESTINATION'])
''')
        cygpath.chmod(0o755)
        self.env = {**os.environ, 'PATH': f'{bins}:{os.environ["PATH"]}',
                    'TEST_ARCHIVE': str(self.archive), 'TEST_DESTINATION': str(self.destination)}

    def run_unpack(self, os_name, **env):
        archive, destination = ((r'D:\temp\fixtures.tar.gz', r'D:\workspace') if os_name == 'Windows'
                                else (str(self.archive), str(self.destination)))
        return subprocess.run(['bash', str(ROOT / 'scripts/unpack-fixtures.sh'), archive, destination],
                              env={**self.env, 'RUNNER_OS': os_name, **env}, text=True, capture_output=True, timeout=10)

    def test_linux_archive_extracts_with_executable_permissions(self):
        result = self.run_unpack('Linux')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        binary = self.destination / 'fixtures/bin/ck'
        self.assertEqual(binary.read_bytes(), b'#!/bin/sh\nexit 0\n')
        self.assertTrue(os.access(binary, os.X_OK))

    def test_windows_drive_paths_are_converted_before_tar(self):
        result = self.run_unpack('Windows')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue((self.destination / 'fixtures/bin/ck').exists())

    def test_path_conversion_failure_refuses_without_extraction(self):
        result = self.run_unpack('Windows', CYGPATH_EXIT='17')
        self.assertEqual(result.returncode, 17, result.stdout + result.stderr)
        self.assertFalse((self.destination / 'fixtures/bin/ck').exists())


if __name__ == '__main__':
    unittest.main(verbosity=2)
