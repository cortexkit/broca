#!/usr/bin/env python3
"""Exercise archive/shard guards and the workflow dependency graph without compiling."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

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
            self.assertEqual(args[args.index('--extract-to') + 1], str(self.root))
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


class WorkflowGraph(unittest.TestCase):
    @staticmethod
    def workflow(name):
        return yaml.load((ROOT / '.github/workflows' / name).read_text(), Loader=yaml.BaseLoader)

    def test_portable_lanes_start_without_waiting_for_tests(self):
        jobs = self.workflow('ci.yml')['jobs']
        for name in ('lint', 'scripts', 'postgres-conformance', 'integration-build', 'fixtures'):
            self.assertNotIn('needs', jobs[name], name)
        self.assertEqual(jobs['integration']['strategy']['matrix']['shard'], ['1', '2', '3', '4'])
        self.assertEqual(jobs['integration']['strategy']['matrix']['os'], ['ubuntu', 'windows'])

    def test_fixtures_are_pinned_and_shared_without_changing_mutation_selection(self):
        fixtures = self.workflow('fixtures.yml')['jobs']['fixtures']
        pins = (ROOT / 'scripts/ci-fixture-revisions.conf').read_text()
        for key in ('SUBC_REV', 'CLAUSTRUM_REV'):
            self.assertRegex(pins, key + r'=[a-f0-9]{40}\b')
        load = next(s for s in fixtures['steps'] if s.get('name') == 'Load shared fixture pins')
        self.assertIn('source broca/scripts/ci-fixture-revisions.conf', load['run'])
        nightly = next(s for s in fixtures['steps'] if s.get('name') == 'Resolve nightly sibling revisions')
        self.assertEqual(nightly['if'], "github.event_name == 'schedule'")
        self.assertEqual(nightly['run'].count('git ls-remote'), 2)
        mutations = self.workflow('mutations.yml')['jobs']
        for name in ('mutations', 'mutations-windows'):
            job = mutations[name]
            self.assertEqual(job['needs'], 'fixtures')
            self.assertEqual(job['strategy']['matrix']['shard'], ['1', '2', '3', '4'])
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


if __name__ == '__main__':
    unittest.main(verbosity=2)
