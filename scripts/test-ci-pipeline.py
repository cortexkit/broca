#!/usr/bin/env python3
"""Exercise archive/shard guards and the workflow dependency graph without compiling."""
import json
import hashlib
import io
import importlib.util
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import tarfile
import tomllib
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
    if os.environ.get('UNRUN_SHARD') and '--partition' in args:
        print('warning: 1/2 tests were not run')
        print('error: test run failed')
        sys.exit(100)
    sys.exit(int(os.environ.get('RUN_EXIT', '0')))
elif args[0] == 'test' and '--list' in args:
    for binary in ('fixture', 'live', 'live_e2e'):
        print(f'Running tests/{binary}.rs (target/debug/{binary})')
        print('1 test, 0 benchmarks')
else:
    sys.exit('unexpected cargo: ' + repr(args))
'''


class FastRustInvariants(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('fast_invariants', ROOT / 'scripts/check-fast-rust-invariants.py')
        assert spec is not None and spec.loader is not None
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.tmp = tempfile.TemporaryDirectory(prefix='broca-invariants-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        paths = (
            'Cargo.lock',
            'crates/broca-replay/tests/bench_envelope_is_representative.rs',
            'crates/broca-wire/src/role.rs', 'crates/broca-wire/src/lib.rs',
            'crates/broca-wal/src/framing.rs',
            'crates/broca-wal/tests/fixtures/plan_manifest_run_started.json',
        )
        for name in paths:
            destination = self.root / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(ROOT / name, destination)
        shutil.copytree(ROOT / 'crates/broca-wire/tests/golden',
                        self.root / 'crates/broca-wire/tests/golden')

    def test_current_file_invariants(self):
        self.module.check(self.root)

    def test_unreviewed_protocol_pair_fails(self):
        lock = self.root / 'Cargo.lock'
        lock.write_text('[[package]]\nname="subc-protocol"\nversion="0.1.0"\n'
                        '[[package]]\nname="subc-protocol"\nversion="9.9.9"\n')
        with self.assertRaisesRegex(ValueError, 'needs replay envelope review'):
            self.module.check(self.root)

    def test_changed_golden_bytes_fail(self):
        golden = self.root / 'crates/broca-wire/tests/golden/session_head.json'
        golden.write_bytes(golden.read_bytes() + b'\n')
        with self.assertRaisesRegex(ValueError, 'golden hash mismatch'):
            self.module.check(self.root)

    def test_missing_golden_inventory_fails(self):
        for path in (self.root / 'crates/broca-wire/tests/golden').glob('run_status.*'):
            path.unlink()
        with self.assertRaisesRegex(ValueError, 'run.status golden inventory'):
            self.module.check(self.root)

    def test_manifest_count_drift_fails(self):
        fixture = self.root / 'crates/broca-wal/tests/fixtures/plan_manifest_run_started.json'
        record = json.loads(fixture.read_text())
        record['manifest']['tools'].pop()
        fixture.write_text(json.dumps(record))
        with self.assertRaisesRegex(ValueError, 'manifest fixture tool count'):
            self.module.check(self.root)


class PrepushDispatch(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='broca-prepush-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'scripts').mkdir()
        shutil.copy(ROOT / 'scripts/prepush.sh', self.root / 'scripts/prepush.sh')
        (self.root / 'scripts/lib').mkdir()
        shutil.copy(ROOT / 'scripts/lib/prepush-report.sh', self.root / 'scripts/lib/prepush-report.sh')
        shutil.copy(ROOT / 'scripts/lib/prepush-motor-timing.py', self.root / 'scripts/lib/prepush-motor-timing.py')
        (self.root / 'scripts/prepare-prepush-windows.py').write_text('''import argparse
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('--output'); p.add_argument('--siblings'); p.add_argument('--cache')
a=p.parse_args(); Path(a.output).mkdir(parents=True, exist_ok=True)
''')
        (self.root / '.gitignore').write_text('target/\nlane-pool/\n.prepush-runtime/\nbin/\nevents\n*.started\n')
        subprocess.run(['git', 'init', '-q', str(self.root)], check=True, capture_output=True)
        subprocess.run(['git', '-C', str(self.root), 'add', 'scripts', '.gitignore'], check=True, capture_output=True)
        subprocess.run(['git', '-C', str(self.root), '-c', 'user.name=Test',
                        '-c', 'user.email=test@example.invalid', '-c', 'core.hooksPath=/dev/null',
                        'commit', '-qm', 'fixture'], check=True, capture_output=True)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        ck = self.bin / 'ck'
        ck.write_text('''#!/usr/bin/env python3
import os, pathlib, sys, time
lane = sys.argv[-1].split()[-1]
root = pathlib.Path(os.environ['FIXTURE'])
record = pathlib.Path(sys.argv[sys.argv.index('--record-file') + 1])
record.write_text('{"type":"accepted"}\\n')
with (root / 'events').open('a') as out:
    out.write(' '.join(sys.argv[1:]) + '\\n')
if lane == os.environ.get('REFUSE_LANE'):
    print('ck motor run: refused before start: fixture admission refusal')
    sys.exit(200)
(root / (lane + '.started')).touch()
with record.open('a') as events:
    import json
    events.write(json.dumps({'type':'started','started_at_ms':time.time_ns()//1000000})+'\\n')
    events.write(json.dumps({'type':'terminal','wall_ms':17,'queue_wait_ms':1})+'\\n')
if lane == 'linux-unit':
    deadline = time.monotonic() + 5
    while not (root / 'windows.started').exists():
        if time.monotonic() >= deadline:
            sys.exit('Windows was not launched in parallel')
        time.sleep(.01)
if lane == os.environ.get('FAIL_LANE'):
    if lane == 'linux-msrv':
        print('error[E0599]: fixture MSRV compiler error')
    print('FAIL broca-module::it permissions::warming_route')
    sys.exit(19)
print('Summary: 1 test run: 1 passed')
''')
        ck.chmod(0o755)

    def run_prepush(self, **extra):
        env = dict(os.environ, PATH=f'{self.bin}:{os.environ["PATH"]}',
                   FIXTURE=str(self.root), BROCA_PREPUSH_LANE_POOL=str(self.root / 'lane-pool'), **extra)
        return subprocess.run(['bash', 'scripts/prepush.sh'], cwd=self.root,
                              env=env, capture_output=True, text=True)

    def test_fast_failure_prevents_heavy_dispatch_and_replays_failure(self):
        result = self.run_prepush(FAIL_LANE='fast')
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        events = (self.root / 'events').read_text().splitlines()
        self.assertEqual(len(events), 1)
        self.assertIn('--platform linux --net', events[0])
        self.assertIn('--lane fast', events[0])
        self.assertIn('heavy lanes NOT RUN', result.stdout)
        self.assertIn('FAILED fast', result.stdout)
        self.assertGreaterEqual(result.stdout.count('permissions::warming_route'), 2)

    def test_heavy_lanes_parallel_and_windows_failure_not_hidden(self):
        result = self.run_prepush(FAIL_LANE='windows')
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        events = (self.root / 'events').read_text().splitlines()
        self.assertEqual(len(events), 14)
        self.assertIn('--lane fast', events[0])
        self.assertTrue(any('--vcpus 16' in e and '--lane linux-portable' in e for e in events))
        self.assertTrue(any('--platform windows' in e and '--lane windows' in e for e in events))
        windows = next(e for e in events if '--platform windows' in e)
        self.assertNotIn('--net', windows)
        self.assertEqual(windows.count('--sibling'), 2)
        linux = [shlex.split(e) for e in events if '--lane linux-' in e]
        self.assertEqual(len(linux), 11)
        self.assertEqual(len({e[e.index('--cwd') + 1] for e in linux}), 11)
        self.assertTrue(any('--lane linux-operator' in e for e in events))
        self.assertIn('FAILED windows', result.stdout)
        self.assertNotIn('FAILED linux', result.stdout)
        self.assertGreaterEqual(result.stdout.count('permissions::warming_route'), 2)

    def test_success_reports_every_lane_and_wall_time(self):
        result = self.run_prepush()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('prepush lane summary', result.stdout)
        self.assertIn('total wall', result.stdout)
        for lane in ('fast', 'linux-operator', 'linux-portable', 'linux-unit', 'linux-msrv', 'windows', 'macos'):
            self.assertRegex(result.stdout, rf'{lane}\s+PASS\s+exit=0\s+wall=\d+s')
        self.assertIn('request-to-start=', result.stdout)
        self.assertIn('run=0.017s', result.stdout)
        common = subprocess.check_output(['git', '-C', str(self.root), 'rev-parse',
                                          '--path-format=absolute', '--git-common-dir'], text=True).strip()
        lane_common = subprocess.check_output(['git', '-C', str(self.root / 'lane-pool/linux-unit'),
                                               'rev-parse', '--path-format=absolute', '--git-common-dir'], text=True).strip()
        self.assertEqual(lane_common, common)

    def test_dirty_persistent_worktree_is_not_checked_out_or_dispatched(self):
        pool = self.root / 'lane-pool'
        pool.mkdir()
        lane = pool / 'fast'
        subprocess.run(['git', '-C', str(self.root), 'worktree', 'add', '-q', '--detach', str(lane), 'HEAD'],
                       check=True, capture_output=True)
        dirty = lane / 'scripts/prepush.sh'
        dirty.write_text(dirty.read_text() + '\n# local work\n')
        result = self.run_prepush()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('lane worktree is dirty', result.stdout)
        self.assertIn('NOT-RUN fast', result.stdout)
        self.assertTrue(dirty.read_text().endswith('# local work\n'))
        self.assertFalse((self.root / 'events').exists())

    def test_clean_persistent_lane_is_reused_at_the_exact_new_candidate(self):
        pool = self.root / 'lane-pool'
        pool.mkdir()
        lane = pool / 'fast'
        subprocess.run(['git', '-C', str(self.root), 'worktree', 'add', '-q', '--detach', str(lane), 'HEAD'],
                       check=True, capture_output=True)
        inode = lane.stat().st_ino
        (self.root / 'candidate.txt').write_text('new candidate\n')
        subprocess.run(['git', '-C', str(self.root), 'add', 'candidate.txt'], check=True, capture_output=True)
        subprocess.run(['git', '-C', str(self.root), '-c', 'user.name=Test',
                        '-c', 'user.email=test@example.invalid', '-c', 'core.hooksPath=/dev/null',
                        'commit', '-qm', 'new candidate'], check=True, capture_output=True)
        expected = subprocess.check_output(['git', '-C', str(self.root), 'rev-parse', 'HEAD'], text=True).strip()
        result = self.run_prepush()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(lane.stat().st_ino, inode)
        actual = subprocess.check_output(['git', '-C', str(lane), 'rev-parse', 'HEAD'], text=True).strip()
        self.assertEqual(actual, expected)
        self.assertEqual((lane / 'candidate.txt').read_text(), 'new candidate\n')

    def test_refused_windows_is_not_run_and_fails_gate_by_name(self):
        result = self.run_prepush(REFUSE_LANE='windows')
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertRegex(result.stdout, r'windows\s+NOT-RUN\s+exit=200')
        self.assertIn('NOT-RUN windows: complete output', result.stdout)
        self.assertNotIn('FAILED windows', result.stdout)
        self.assertFalse((self.root / 'windows.started').exists())

    def test_operator_failure_is_required_but_does_not_block_native_dispatch(self):
        result = self.run_prepush(FAIL_LANE='linux-operator')
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('FAILED linux-operator', result.stdout)
        self.assertTrue((self.root / 'linux-unit.started').exists())
        self.assertTrue((self.root / 'windows.started').exists())

    def test_msrv_failure_summary_prints_first_error_and_complete_output(self):
        result = self.run_prepush(FAIL_LANE='linux-msrv')
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        summary = result.stdout.split('==> prepush lane summary', 1)[1]
        self.assertIn('FIRST ERRORS linux-msrv', summary)
        self.assertIn('error[E0599]: fixture MSRV compiler error', summary)
        self.assertIn('FAILED linux-msrv: complete output', summary)

    def test_msrv_uses_installed_patch_toolchain_not_syncing_minor_alias(self):
        shutil.copy(ROOT / 'scripts/lib/prepush-lane.sh', self.root / 'scripts/lib/prepush-lane.sh')
        cargo = self.bin / 'cargo'
        cargo.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
with (pathlib.Path(os.environ['FIXTURE']) / 'cargo-calls').open('a') as out:
    out.write(json.dumps(sys.argv[1:]) + '\\n')
(pathlib.Path(os.environ['FIXTURE']) / 'supplied-target').write_text(os.environ['CARGO_TARGET_DIR'])
if '+1.96' in sys.argv:
    sys.exit('error: could not create temp file: Read-only file system')
print('fixture cargo completed')
''')
        cargo.chmod(0o755)
        for name, body in {'rustc': 'echo fixture rustc', 'git': 'exit 0',
                           'uname': 'echo Linux'}.items():
            path = self.bin / name
            path.write_text('#!/bin/sh\n' + body + '\n')
            path.chmod(0o755)
        home = self.root / 'home'
        home.mkdir(mode=0o700)
        env = dict(os.environ, PATH=f'{self.bin}:{os.environ["PATH"]}',
                   HOME=str(home), FIXTURE=str(self.root),
                   CARGO_TARGET_DIR=str(self.root / 'target-cache'))
        result = subprocess.run(['bash', 'scripts/prepush.sh', '--lane', 'linux-msrv'],
                                cwd=self.root, env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        calls = [json.loads(line) for line in (self.root / 'cargo-calls').read_text().splitlines()]
        self.assertIn(['+1.96.0', 'check', '--locked', '--workspace', '--all-targets'], calls)
        self.assertFalse(any('+1.96' in args for args in calls))
        self.assertEqual((self.root / 'supplied-target').read_text(), str(self.root / 'target-cache'))

    def test_missing_file_tool_refuses_before_running_checks(self):
        lib = self.root / 'scripts/lib'
        lib.mkdir(exist_ok=True)
        shutil.copy(ROOT / 'scripts/lib/prepush-lane.sh', lib / 'prepush-lane.sh')
        tools = self.root / 'minimal-tools'
        tools.mkdir()
        for tool in ('bash', 'dirname', 'mkdir', 'mktemp', 'chmod', 'cat'):
            binary = shutil.which(tool)
            assert binary is not None
            (tools / tool).symlink_to(binary)
        uname = tools / 'uname'
        uname.write_text('#!/bin/sh\necho Linux\n')
        uname.chmod(0o755)
        home = self.root / 'home'
        home.mkdir(mode=0o700)
        env = dict(os.environ, PATH=str(tools), HOME=str(home))
        result = subprocess.run([str(tools / 'bash'), 'scripts/prepush.sh', '--lane', 'fast'],
                                cwd=self.root, env=env, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('MOTOR Linux build image is missing file(1)', result.stdout)
        self.assertNotIn('==> fast: fmt', result.stdout)
        self.assertNotIn('operator scripts, pins and dependency guards', result.stdout)

    def run_private_scratch_probe(self):
        lib = self.root / 'scripts/lib'
        lib.mkdir(exist_ok=True)
        shutil.copy(ROOT / 'scripts/lib/prepush-lane.sh', lib / 'prepush-lane.sh')
        logs = self.root / 'target/scratch'
        logs.mkdir(parents=True)
        logs.chmod(0o775)
        checkout = logs / 'checkouts/worker'
        (checkout / 'scripts/lib').mkdir(parents=True)
        for name in ('scripts/prepush.sh', 'scripts/lib/prepush-lane.sh', 'scripts/lib/prepush-report.sh'):
            shutil.copy(ROOT / name, checkout / name)
        home = self.root / 'home'
        home.mkdir(mode=0o700)
        tools = self.root / 'minimal-tools'
        tools.mkdir()
        for tool in ('bash', 'dirname', 'mkdir', 'mktemp', 'chmod', 'cat'):
            binary = shutil.which(tool)
            assert binary is not None
            (tools / tool).symlink_to(binary)
        for name, body in {
            'uname': 'echo Linux',
            'cc': 'exit 0',
            'file': 'printf "TEST_TMPDIR=%s\\nTEST_UMASK=%s\\n" "$TMPDIR" "$(umask)"; exit 1',
        }.items():
            path = tools / name
            path.write_text('#!/bin/sh\n' + body + '\n')
            path.chmod(0o755)
        env = dict(os.environ, PATH=str(tools), HOME=str(home))
        result = subprocess.run([str(tools / 'bash'), 'scripts/prepush.sh', '--lane', 'fast'],
                                cwd=checkout, env=env, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result, logs

    def test_test_processes_use_ci_umask_not_private_setup_umask(self):
        result, _ = self.run_private_scratch_probe()
        ambient = next(line.split('=', 1)[1] for line in result.stdout.splitlines()
                       if line.startswith('TEST_UMASK='))
        self.assertEqual(int(ambient, 8), 0o022,
                         'owner-only tests must distinguish explicit modes from the ambient default')

    def test_daemon_tmpdir_is_private_and_outside_group_writable_log_root(self):
        result, logs = self.run_private_scratch_probe()
        temporary = Path(next(line.split('=', 1)[1] for line in result.stdout.splitlines()
                              if line.startswith('TEST_TMPDIR=')))
        self.assertNotIn(logs, temporary.parents,
                         'daemon connection files must not descend from the 0775 log directory')
        self.assertEqual(temporary.stat().st_mode & 0o777, 0o700)
        for parent in temporary.parents:
            if parent == self.root:
                break
            self.assertEqual(parent.stat().st_mode & 0o022, 0, str(parent))

    def test_windows_scratch_bootstrap_never_requests_unix_modes(self):
        shutil.copy(ROOT / 'scripts/lib/prepush-lane.sh', self.root / 'scripts/lib/prepush-lane.sh')
        real_mkdir = shutil.which('mkdir')
        assert real_mkdir is not None
        stubs = {
            'uname': 'echo MINGW64_NT',
            'cygpath': 'for value; do :; done; printf "%s\\n" "$value"',
            'mkdir': 'for value; do case "$value" in -m|--mode*) echo "POSIX mode refused" >&2; exit 77;; esac; done\n'
                     f'exec {shlex.quote(real_mkdir)} "$@"',
            'chmod': 'echo "POSIX chmod refused" >&2; exit 77',
            'rustc': 'printf reached > "$FIXTURE/toolchain-reached"; exit 66',
        }
        for name, body in stubs.items():
            path = self.bin / name
            path.write_text('#!/bin/sh\n' + body + '\n')
            path.chmod(0o755)
        cargo = self.bin / 'cargo'
        cargo.write_text('''#!/usr/bin/env python3
import os,pathlib,sys
assert 'prepush_scratch' in sys.argv and '--offline' in sys.argv
root=pathlib.Path(os.environ['FIXTURE'])
(root/'native-helper-called').touch()
scratch=root/'native-private-temp'; scratch.mkdir()
print(scratch)
''')
        cargo.chmod(0o755)
        env = dict(os.environ, PATH=f'{self.bin}:{os.environ["PATH"]}', FIXTURE=str(self.root),
                   CARGO_TARGET_DIR=str(self.root / 'cargo-cache'))
        result = subprocess.run(['bash', 'scripts/prepush.sh', '--lane', 'windows'],
                                cwd=self.root, env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 66, result.stdout + result.stderr)
        self.assertTrue((self.root / 'native-helper-called').exists())
        self.assertTrue((self.root / 'toolchain-reached').exists())
        self.assertNotIn('POSIX', result.stderr)


class MotorTiming(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('motor_timing', ROOT / 'scripts/lib/prepush-motor-timing.py')
        assert spec is not None and spec.loader is not None
        self.timing = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.timing)
        self.tmp = tempfile.TemporaryDirectory(prefix='prepush-timing-')
        self.addCleanup(self.tmp.cleanup)
        self.events = Path(self.tmp.name) / 'events.jsonl'

    def test_request_gap_is_not_the_runner_reported_queue(self):
        self.events.write_text('\n'.join(json.dumps(event) for event in (
            {'type': 'accepted'},
            {'type': 'started', 'started_at_ms': 1_501_000},
            {'type': 'terminal', 'wall_ms': 227_000, 'queue_wait_ms': 8},
        )))
        result = self.timing.summarize(self.events, 1000, 300, 1_728_000)
        self.assertEqual(result['request_to_start_ms'], 1_500_000)
        self.assertEqual(result['run_ms'], 227_000)
        self.assertEqual(result['reported_queue_ms'], 8)

    def test_refused_job_has_no_execution_time(self):
        self.events.write_text('{"type":"accepted"}\n')
        result = self.timing.summarize(self.events, 1000, 100, 500)
        self.assertIsNone(result['request_to_start_ms'])
        self.assertIsNone(result['run_ms'])

    def test_partial_record_does_not_hide_known_metadata_or_clock_skew(self):
        self.events.write_text('{"type":"started","started_at_ms":900}\n{"partial":')
        result = self.timing.summarize(self.events, 1000, 0, 500)
        self.assertEqual(result['request_to_start_ms'], -100)
        self.assertIsNone(result['run_ms'])


class ArchiveShards(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='broca-ci-archive-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'scripts').mkdir()
        library = self.root / 'scripts/lib'
        library.mkdir()
        shutil.copy2(ROOT / 'scripts/lib/cargo-watchdog.py', library / 'cargo-watchdog.py')
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

    def test_unrun_warning_fails_shard_even_if_every_printed_test_passed(self):
        result = self.run_mode('run', str(self.archive), '2/4', UNRUN_SHARD='1')
        self.assertEqual(result.returncode, 100, result.stdout + result.stderr)
        self.assertIn('tests were not run', result.stdout + result.stderr)

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
                         {'lint', 'scripts', 'unit', 'production-features', 'store-contention-macos',
                          'integration-build', 'fixtures', 'integration', 'linux-deploy',
                          'wal-reader'})

    def test_master_and_nightly_runs_are_never_cancelled_by_each_other(self):
        """A master push cancelled the nightly run on master, or a later push cancelled an
        earlier master commit's run, and release tagging then found a cancelled CI run."""
        concurrency = self.workflow('ci.yml')['concurrency']
        self.assertIn('${{ github.event_name }}', concurrency['group'])
        self.assertIn('${{ github.ref }}', concurrency['group'])
        # A shared master group keeps one pending run and drops the older one, so master and
        # nightly runs are grouped by commit. (`cond && '' || x` always yields x in GitHub's
        # expressions, so the commit suffix must be the truthy arm.)
        self.assertIn("${{ (github.event_name != 'pull_request' && "
                      "!startsWith(github.ref, 'refs/heads/train/')) && "
                      "format('-{0}', github.sha) || '' }}", concurrency['group'])
        self.assertEqual(
            concurrency['cancel-in-progress'],
            "${{ github.event_name == 'pull_request' || startsWith(github.ref, 'refs/heads/train/') }}",
        )

    def test_shard_budget_covers_the_slowest_producer_then_the_replay(self):
        """Shards start alongside their producers and wait for them. The wait must outlast the
        longest producer budget, and the shard's job budget must outlast the wait plus a
        replay, or a shard is cancelled while its producer is still inside its allowance."""
        jobs = self.workflow('ci.yml')['jobs']
        producers = {
            'integration-build': int(jobs['integration-build']['timeout-minutes']),
            'fixtures': int(self.workflow('fixtures.yml')['jobs']['fixtures']['timeout-minutes']),
        }
        wait_step = next(step for step in jobs['integration']['steps']
                         if 'wait-ci-artifacts.py' in step.get('run', ''))
        words = shlex.split(wait_step['run'])
        self.assertIn('--timeout', words, 'the artifact wait must state its own budget')
        wait_seconds = float(words[words.index('--timeout') + 1])
        self.assertGreaterEqual(wait_seconds, 60 * max(producers.values()), producers)
        replay_minutes = 10
        self.assertGreaterEqual(int(jobs['integration']['timeout-minutes']) * 60,
                                wait_seconds + 60 * replay_minutes)

    def test_production_clock_code_is_linted_and_tested_and_msrv_checked(self):
        """Every other lane enables test-clock, so the release clock path had no lint or test."""
        job = self.workflow('ci.yml')['jobs']['production-features']
        runs = [step.get('run', '') for step in job['steps']]
        crates = '-p broca-core -p broca-module -p broca-module-serve'
        clippy = next(run for run in runs if run.startswith('cargo clippy'))
        tests = next(run for run in runs if run.startswith('cargo test'))
        for command in (clippy, tests):
            self.assertIn(crates, command)
            self.assertNotIn('test-clock', command)
            self.assertNotIn('cargo_feature_args', command)
            self.assertNotIn('-F', command.split())
        self.assertIn('--lib --bins', tests)
        # Real-daemon tests in these crates must fail, not skip, when their binaries are missing.
        for name in ('SUBC', 'AIMOCK', 'VAULT'):
            self.assertEqual(job['env'][f'BROCA_REQUIRE_{name}'], '1')
        rust_version = tomllib.loads((ROOT / 'Cargo.toml').read_text())['workspace']['package']['rust-version']
        msrv = next(step for step in job['steps'] if step.get('with', {}).get('toolchain'))
        self.assertEqual(msrv['with']['toolchain'], rust_version)
        self.assertIn(f'cargo +{rust_version} check --locked --workspace --all-targets', runs)

    def test_store_contention_runs_on_all_three_native_platforms(self):
        jobs = self.workflow('ci.yml')['jobs']
        macos = jobs['store-contention-macos']
        self.assertEqual(macos['runs-on'], 'macos-latest')
        self.assertTrue(any(step.get('run') == 'cargo test --locked -p broca-store-contention-tests --lib'
                            for step in macos['steps']))
        self.assertEqual({row['os'] for row in jobs['unit']['strategy']['matrix']['include']},
                         {'ubuntu', 'windows'})
        self.assertTrue(any('cargo test --locked --workspace --lib' in step.get('run', '')
                            for step in jobs['unit']['steps']))

    def test_scripts_prefetch_locked_dependencies_before_offline_suites(self):
        steps = self.workflow('ci.yml')['jobs']['scripts']['steps']
        fetch = next(i for i, step in enumerate(steps) if step.get('run') == 'cargo fetch --locked')
        suites = next(i for i, step in enumerate(steps) if step.get('run') == 'scripts/script-suites.sh')
        self.assertLess(fetch, suites)
        self.assertTrue(any(step.get('uses', '').startswith('dtolnay/rust-toolchain@') for step in steps[:fetch]))

    def test_real_bus_jobs_install_and_require_the_pinned_broker(self):
        for workflow, name in (('ci.yml', 'integration'),):
            job = self.workflow(workflow)['jobs'][name]
            self.assertEqual(job['env']['CK_BUS_TESTKIT_REQUIRE'], '1', workflow)
            install = next(i for i, step in enumerate(job['steps'])
                           if step.get('run') == 'python3 scripts/install-ci-nats.py')
            self.assertNotIn('if', job['steps'][install], workflow)
            replay = next(i for i, step in enumerate(job['steps'])
                          if 'ci-integration.sh run' in step.get('run', ''))
            self.assertLess(install, replay, workflow)
        gate = (ROOT / 'scripts/gate.sh').read_text()
        self.assertIn('export CK_BUS_TESTKIT_REQUIRE=1', gate)
        self.assertIn('/opt/homebrew/bin:$PATH', gate)

    def test_fixtures_are_pinned_and_shared(self):
        fixtures = self.workflow('fixtures.yml')['jobs']['fixtures']
        pins = (ROOT / 'scripts/ci-fixture-revisions.conf').read_text()
        for key in ('SUBC_REV', 'CLAUSTRUM_REV'):
            self.assertRegex(pins, key + r'=[a-f0-9]{40}\b')
        load = next(s for s in fixtures['steps'] if s.get('name') == 'Load shared fixture pins')
        self.assertIn('source broca/scripts/ci-fixture-revisions.conf', load['run'])
        build = next(s for s in fixtures['steps'] if s.get('name') == 'Build fixtures once')
        self.assertIn('"subconscious/target/debug/ck$suffix"', build['run'])
        verify = next(s for s in fixtures['steps'] if s.get('name') == 'Verify and package fixtures')
        self.assertIn('names="ck-subc fake-aft-stub ck ck-claustrum ck-auth"', verify['run'])
        self.assertIn('if [[ "$RUNNER_OS" != Windows ]]; then names="$names fake-surface"; fi', verify['run'])
        nightly = next(s for s in fixtures['steps'] if s.get('name') == 'Resolve nightly sibling revisions')
        self.assertEqual(nightly['if'], "github.event_name == 'schedule'")
        self.assertEqual(nightly['run'].count('git ls-remote'), 2)

    def test_private_content_standin_is_prebuilt_and_forwarded(self):
        fixtures = self.workflow('fixtures.yml')['jobs']['fixtures']
        build = next(s for s in fixtures['steps'] if s.get('name') == 'Build fixtures once')['run']
        self.assertIn('cargo build --locked --manifest-path broca/crates/broca-module-serve/tests/support/fake-surface/Cargo.toml --target-dir fake-surface-target', build)
        self.assertIn('cp fake-surface-target/debug/fake-surface fixtures/bin/', build)
        # The stand-in uses Unix descriptors and its tests run only on Linux and macOS, so the
        # Windows fixture job must never try to compile it.
        guard = build.index('if [[ "$RUNNER_OS" != Windows ]]; then')
        self.assertLess(guard, build.index('fake-surface/Cargo.toml'))
        cache = next(s for s in fixtures['steps'] if s.get('id') == 'binaries')['with']['key']
        self.assertIn('fixtures-v3-', cache)
        self.assertIn("hashFiles('broca/crates/broca-module-serve/tests/support/fake-surface/**')", cache)

        def fixture_environments(node):
            if isinstance(node, dict):
                if 'FAKE_AFT_STUB_BIN' in node:
                    yield node
                for value in node.values():
                    yield from fixture_environments(value)
            elif isinstance(node, list):
                for value in node:
                    yield from fixture_environments(value)

        environments = list(fixture_environments(self.workflow('ci.yml')))
        self.assertEqual(len(environments), 3)
        for env in environments:
            self.assertEqual(env['FAKE_SURFACE_BIN'],
                             env['FAKE_AFT_STUB_BIN'].replace('fake-aft-stub', 'fake-surface'))

    def test_release_requires_only_ci(self):
        release = (ROOT / 'scripts/release.sh').read_text()
        calls = [shlex.split(line) for line in release.splitlines()
                 if line.startswith('bash scripts/ci-status.sh ')]
        self.assertEqual(len(calls), 1)
        required = calls[0][3:calls[0].index('||')]
        self.assertEqual(required, ['--event', 'push', '--only', 'CI'])

    def test_required_status_refuses_failed_skipped_cancelled_or_empty_lanes(self):
        job = self.workflow('ci.yml')['jobs']['test']
        self.assertEqual(job['if'], 'always()')
        script = job['steps'][0]['run']
        for result in ('success', 'failure', 'skipped', 'cancelled', 'empty'):
            lanes = {} if result == 'empty' else {'integration': {'result': result}}
            run = subprocess.run(['bash', '-c', script], env={**os.environ, 'NEEDS_JSON': json.dumps(lanes)},
                                 text=True, capture_output=True, timeout=5)
            self.assertEqual(run.returncode == 0, result == 'success', run.stdout + run.stderr)

    def test_ci_uploads_survivor_dumps_even_after_failure(self):
        for job in ('unit', 'integration'):
            with self.subTest(job=job):
                steps = self.workflow('ci.yml')['jobs'][job]['steps']
                uploads = [step for step in steps if step.get('uses', '').startswith('actions/upload-artifact@')]
                dump = next((step for step in uploads
                             if 'broca/target/rig-survivors/*.txt' in step.get('with', {}).get('path', '').splitlines()), None)
                self.assertIsNotNone(dump, f'{job} must upload survivor dumps')
                self.assertEqual(dump['if'], 'always()')

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

    def test_a_server_error_on_either_query_is_retried_until_the_artifacts_arrive(self):
        # GitHub answered one poll with HTTP 502 and every waiting shard failed with it.
        transient = self.module.TransientQueryError('gh: Server Error (HTTP 502)')
        api = self.run_wait([transient,
                             self.artifacts('fixtures-Windows'),
                             transient,
                             self.artifacts('fixtures-Windows', 'integration-windows')])
        self.assertEqual(api.call_count, 4)

    def test_api_classifies_only_server_errors_and_timeouts_as_transient(self):
        def failed(stderr):
            return subprocess.CompletedProcess(['gh'], 1, stdout='', stderr=stderr)

        with patch.object(self.module.subprocess, 'run', return_value=failed('gh: Server Error (HTTP 502)')):
            with self.assertRaises(self.module.TransientQueryError):
                self.module.api('repos/x/actions/runs/1/artifacts')
        with patch.object(self.module.subprocess, 'run', return_value=failed('gh: Not Found (HTTP 404)')):
            with self.assertRaises(RuntimeError) as raised:
                self.module.api('repos/x/actions/runs/1/artifacts')
            self.assertNotIsInstance(raised.exception, self.module.TransientQueryError)
        timeout = subprocess.TimeoutExpired(['gh'], 60)
        with patch.object(self.module.subprocess, 'run', side_effect=timeout):
            with self.assertRaises(self.module.TransientQueryError):
                self.module.api('repos/x/actions/runs/1/artifacts')

    def test_a_server_error_past_the_deadline_still_fails(self):
        transient = self.module.TransientQueryError('gh: Server Error (HTTP 502)')
        with self.assertRaises(self.module.TransientQueryError):
            self.run_wait([transient], timeout=1e-9)

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


class NatsInstall(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('install_nats', ROOT / 'scripts/install-ci-nats.py')
        self.installer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.installer)
        self.tmp = tempfile.TemporaryDirectory(prefix='broca-nats-install-')
        self.addCleanup(self.tmp.cleanup)
        self.destination = Path(self.tmp.name) / 'broker with spaces'

    def archive(self, os_name, content=b'broker fixture'):
        target = self.installer.ARCHIVES[os_name][0]
        name = 'nats-server.exe' if os_name == 'Windows' else 'nats-server'
        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode='w:gz') as package:
            member = tarfile.TarInfo(f'nats-server-v{self.installer.VERSION}-{target}/{name}')
            member.size = len(content)
            package.addfile(member, io.BytesIO(content))
        return data.getvalue()

    def test_installs_each_platform_archive_and_checks_its_version(self):
        for os_name in ('Linux', 'Windows'):
            with self.subTest(os_name=os_name):
                archive = self.archive(os_name)
                target = self.installer.ARCHIVES[os_name][0]
                with patch.dict(self.installer.ARCHIVES, {
                    os_name: (target, hashlib.sha256(archive).hexdigest())
                }), patch.object(self.installer.urllib.request, 'urlopen',
                                 return_value=io.BytesIO(archive)), \
                        patch.object(self.installer.subprocess, 'run', return_value=
                                     subprocess.CompletedProcess([], 0, 'nats-server: v2.10.26\n')) as run:
                    binary = self.installer.install(self.destination, os_name, 'AMD64')
                self.assertEqual(binary.read_bytes(), b'broker fixture')
                self.assertEqual(binary.suffix, '.exe' if os_name == 'Windows' else '')
                # install() returns the resolved path; compare resolved, since macOS temp dirs
                # sit under a /var -> /private/var symlink.
                executed, *args = run.call_args.args[0]
                self.assertEqual(Path(executed).resolve(), binary)
                self.assertEqual(args, ['--version'])
                self.assertTrue(run.call_args.kwargs['check'])

    def test_rejects_wrong_checksum_before_writing_or_executing(self):
        with patch.object(self.installer.urllib.request, 'urlopen',
                          return_value=io.BytesIO(self.archive('Linux'))), \
                patch.object(self.installer.subprocess, 'run') as run:
            with self.assertRaisesRegex(RuntimeError, 'checksum mismatch'):
                self.installer.install(self.destination, 'Linux', 'x86_64')
        self.assertFalse(self.destination.exists())
        run.assert_not_called()

    def test_refuses_an_unpinned_platform_before_downloading(self):
        with patch.object(self.installer.urllib.request, 'urlopen') as download:
            with self.assertRaisesRegex(RuntimeError, 'no pinned NATS archive'):
                self.installer.install(self.destination, 'Linux', 'aarch64')
        download.assert_not_called()

    def test_windows_staged_archive_is_verified_without_network(self):
        archive = self.archive('Windows')
        source = Path(self.tmp.name) / 'staged.tar.gz'
        source.write_bytes(archive)
        target = self.installer.ARCHIVES['Windows'][0]
        with patch.dict(self.installer.ARCHIVES, {
            'Windows': (target, hashlib.sha256(archive).hexdigest())
        }), patch.object(self.installer.urllib.request, 'urlopen') as download, \
                patch.object(self.installer.subprocess, 'run', return_value=
                             subprocess.CompletedProcess([], 0, 'nats-server: v2.10.26\n')):
            binary = self.installer.install(self.destination, 'Windows', 'AMD64', source)
        self.assertEqual(binary.read_bytes(), b'broker fixture')
        download.assert_not_called()

    def test_corrupt_staged_archive_refuses_without_network_or_execution(self):
        source = Path(self.tmp.name) / 'corrupt.tar.gz'
        source.write_bytes(b'corrupt')
        with patch.object(self.installer.urllib.request, 'urlopen') as download, \
                patch.object(self.installer.subprocess, 'run') as run:
            with self.assertRaisesRegex(RuntimeError, 'checksum mismatch'):
                self.installer.install(self.destination, 'Windows', 'AMD64', source)
        download.assert_not_called()
        run.assert_not_called()


class WindowsOfflineInputs(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('prepush_inputs', ROOT / 'scripts/prepare-prepush-windows.py')
        assert spec is not None and spec.loader is not None
        self.inputs = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.inputs)
        self.tmp = tempfile.TemporaryDirectory(prefix='broca-offline-inputs-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_input_cache_is_verified_before_use_without_downloading_again(self):
        cache = self.root / 'cache'
        cache.mkdir()
        (cache / 'asset').write_bytes(b'fixture')
        with patch.object(self.inputs.urllib.request, 'urlopen') as download:
            self.inputs.download('https://example.invalid/asset', self.root / 'output/asset',
                                 'sha256', hashlib.sha256(b'fixture').digest(), cache)
        self.assertEqual((self.root / 'output/asset').read_bytes(), b'fixture')
        download.assert_not_called()

    def test_corrupt_offline_input_is_not_silently_replaced(self):
        cache = self.root / 'cache'
        cache.mkdir()
        (cache / 'asset').write_bytes(b'bad')
        with patch.object(self.inputs.urllib.request, 'urlopen') as download:
            with self.assertRaisesRegex(ValueError, 'mismatch for offline input'):
                self.inputs.download('https://example.invalid/asset', self.root / 'output/asset',
                                     'sha256', hashlib.sha256(b'fixture').digest(), cache)
        self.assertFalse((self.root / 'output/asset').exists())
        download.assert_not_called()

    def test_runtime_pins_are_literal_and_complete(self):
        assets = self.inputs.settings(ROOT / 'scripts/lib/prepush-assets.conf')
        for prefix in ('PYTHON_WINDOWS', 'NODE_WINDOWS', 'NODE_LINUX'):
            self.assertTrue(assets[prefix + '_URL'].startswith('https://'))
            self.assertRegex(assets[prefix + '_SHA256'], r'^[a-f0-9]{64}$')

    def test_shallow_bundle_refusal_is_reproduced_then_staged_history_imports(self):
        origin = self.root / 'origin'
        subprocess.run(['git', 'init', '-q', str(origin)], check=True, capture_output=True)
        for text in ('first\n', 'pinned\n'):
            (origin / 'source.txt').write_text(text)
            subprocess.run(['git', '-C', str(origin), 'add', 'source.txt'], check=True, capture_output=True)
            subprocess.run(['git', '-C', str(origin), '-c', 'user.name=Test',
                            '-c', 'user.email=test@example.invalid', '-c', 'core.hooksPath=/dev/null',
                            'commit', '-qm', 'fixture'], check=True, capture_output=True)
        pin = subprocess.check_output(['git', '-C', str(origin), 'rev-parse', 'HEAD'], text=True).strip()
        parent = subprocess.check_output(['git', '-C', str(origin), 'rev-parse', 'HEAD^'], text=True).strip()
        siblings = self.root / 'siblings'
        siblings.mkdir()
        shallow = siblings / 'subconscious'
        subprocess.run(['git', 'clone', '-q', '--depth', '1', origin.as_uri(), str(shallow)],
                       check=True, capture_output=True)
        bad_bundle = self.root / 'shallow.bundle'
        subprocess.run(['git', '-C', str(shallow), 'bundle', 'create', str(bad_bundle), 'HEAD'],
                       check=True, capture_output=True)
        receiver = self.root / 'receiver'
        subprocess.run(['git', 'init', '-q', str(receiver)], check=True, capture_output=True)
        refused = subprocess.run(['git', '-C', str(receiver), 'fetch', str(bad_bundle), 'HEAD'],
                                 capture_output=True, text=True)
        self.assertNotEqual(refused.returncode, 0)
        self.assertIn(parent, refused.stderr)
        self.assertIn('Failed to traverse parents of commit ' + pin, refused.stderr)
        self.assertIn('did not send all necessary objects', refused.stderr)
        print('reproduced shallow-input refusal:', refused.stderr.strip())

        output, cache = self.root / 'inputs', self.root / 'cache'
        real_settings, real_checkout = self.inputs.settings, self.inputs.checkout

        def settings(path):
            return {'SUBC_REV': pin, 'CLAUSTRUM_REV': pin} if path.name == 'ci-fixture-revisions.conf' else real_settings(path)

        def checkout(repository, revision, destination, **kwargs):
            return real_checkout(repository, revision, destination, remote_url=origin.as_uri(), **kwargs)

        def download(url, destination, algorithm, expected, cache):
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(b'fixture')
            cache.mkdir(parents=True, exist_ok=True)

        with patch.object(self.inputs, 'settings', side_effect=settings), \
                patch.object(self.inputs, 'checkout', side_effect=checkout), \
                patch.object(self.inputs, 'download', side_effect=download):
            self.inputs.prepare(output, siblings, cache)
        self.assertEqual(subprocess.check_output(['git', '-C', str(shallow), 'rev-parse',
                                                 '--is-shallow-repository'], text=True).strip(), 'false')
        good_bundle = cache / f'subconscious-{pin}.bundle'
        fetched = subprocess.run(['git', '-C', str(receiver), 'fetch', str(good_bundle), 'HEAD'],
                                 capture_output=True, text=True)
        self.assertEqual(fetched.returncode, 0, fetched.stderr)
        self.assertEqual(subprocess.check_output(['git', '-C', str(receiver), 'show',
                                                 'FETCH_HEAD:source.txt'], text=True), 'pinned\n')
        extracted = self.root / 'plain-vault'
        self.inputs.extract_source(output, 'claustrum', pin, extracted)
        self.assertEqual((extracted / 'source.txt').read_text(), 'pinned\n')
        self.assertFalse((extracted / '.git').exists())

    def test_source_archive_checksum_and_pin_are_required_before_extracting(self):
        inputs = self.root / 'inputs'
        inputs.mkdir()
        (inputs / 'source.tar').write_bytes(b'corrupt')
        manifest = {'source_archives': {'claustrum': {
            'revision': 'a' * 40, 'file': 'source.tar',
            'sha256': hashlib.sha256(b'approved').hexdigest(),
        }}}
        (inputs / 'manifest.json').write_text(json.dumps(manifest))
        destination = self.root / 'extracted'
        with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
            self.inputs.extract_source(inputs, 'claustrum', 'a' * 40, destination)
        with self.assertRaisesRegex(ValueError, 'pinned revision'):
            self.inputs.extract_source(inputs, 'claustrum', 'b' * 40, destination)
        self.assertFalse(destination.exists())


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
