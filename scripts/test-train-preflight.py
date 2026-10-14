#!/usr/bin/env python3
"""Check remote invocation, failure propagation and Linux commands using stand-ins."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]

GIT = r'''#!/usr/bin/env python3
import os, pathlib, sys
args = sys.argv[1:]
if args[:1] == ['status']:
    if os.environ.get('GIT_EXIT'): sys.exit(int(os.environ['GIT_EXIT']))
    if os.environ.get('DIRTY_CACHE'): print(' M fixture.rs')
else:
    raise SystemExit('unexpected git: ' + repr(args))
'''

CARGO = r'''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ['EVENTS'], 'a') as f: f.write(json.dumps(args) + '\n')
if args[0] == 'clippy': sys.exit(int(os.environ.get('CLIPPY_EXIT', '0')))
if args[:2] == ['nextest', 'list']:
    selected = 'broca-subc' in args[args.index('-E') + 1]
    cases = {'test_clock::test_clock_feature_is_enabled_for_retention_tests': {'ignored': False, 'filter-match': {'status': 'matches'}}}
    if not selected: cases['real_daemon::fixture'] = {'ignored': False, 'filter-match': {'status': 'matches'}}
    if selected and os.environ.get('ZERO_SELECTION'): cases = {}
    print(json.dumps({'rust-suites': {'broca-module::it': {'testcases': cases}}}))
elif args[:2] == ['nextest', 'run']:
    if not os.environ.get('NO_CLOCK_PASS'):
        print('PASS [0.01s] (1/1) broca-module::it test_clock::test_clock_feature_is_enabled_for_retention_tests')
    print('Summary [0.01s] 1 test run: 1 passed')
elif '--doc' in args and '--list' in args:
    print('0 tests, 0 benchmarks')
elif '--list' in args:
    if os.environ.get('LIST_EXIT'):
        print('fixture list compilation failed')
        sys.exit(int(os.environ['LIST_EXIT']))
    for name in ('fixture_unit', 'executor::tests::session_close_releases_hook_and_compaction_routes', 'vault::tests::list_scoped_strict_mirror_matches_claustrum_tip', 'tests::batched_projection_crosses_boundaries_and_updates_conflicts', 'tests::export_writers_lock_before_allocating_sequences', 'tests::postgres_store_conformance'):
        print(name + ': test')
elif '--version' in args: print('fixture cargo 1.99 / nextest 0.9')
elif args[0] == 'test':
    count = 0 if os.environ.get('ZERO_UNITS') else 1
    if os.environ.get('PASSING_SKIP'): print('SKIP fixture_arm: daemon absent')
    print(f'test result: ok. {count} passed; 0 failed; 0 ignored; 0 measured; 5 filtered out; finished in 0.01s')
    sys.exit(int(os.environ.get('UNIT_EXIT', '0')))
'''


class MotorBoundary(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='broca-motor-preflight-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        scripts = self.root / 'scripts'
        scripts.mkdir()
        for name in ('train-preflight.sh', 'linux-preflight.sh', 'portable-test-selection.conf', 'preflight-test-report.py'):
            shutil.copy2(ROOT / 'scripts' / name, scripts / name)
        self.bins = self.root / 'bin'
        self.bins.mkdir()
        self.write_executable('git', GIT)
        self.write_executable('ck', r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ['EVENTS'], 'a') as f: f.write(json.dumps(sys.argv[1:]) + '\n')
print('motor fixture output')
sys.exit(int(os.environ.get('MOTOR_EXIT', '0')))
''')
        self.write_executable('cargo', CARGO)
        self.write_executable('cargo-nextest', '#!/bin/sh\nexit 0\n')
        self.write_executable('rustc', '#!/bin/sh\necho fixture-rustc-1.99\n')
        self.write_executable('uname', '#!/bin/sh\necho "${FIXTURE_OS:-Linux}"\n')
        self.write_executable('sleep', '#!/bin/sh\n/bin/sleep 0.01\n')
        self.events = self.root / 'events'
        self.env = {**os.environ, 'PATH': f'{self.bins}:{os.environ["PATH"]}', 'EVENTS': str(self.events),
                    'CARGO_TARGET_DIR': str(self.root / 'warm-target')}

    def write_executable(self, name, content):
        path = self.bins / name
        path.write_text(content)
        path.chmod(0o755)

    def run_script(self, script, **env):
        return subprocess.run(['bash', 'scripts/' + script], cwd=self.root, env={**self.env, **env},
                              text=True, capture_output=True, timeout=10)

    def commands(self):
        return [json.loads(line) for line in self.events.read_text().splitlines()]

    def test_wrapper_calls_motor_without_siblings_or_local_cargo(self):
        result = self.run_script('train-preflight.sh')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        commands = self.commands()
        self.assertEqual(len(commands), 1)
        args = commands[0]
        self.assertEqual(args[:2], ['motor', 'run'])
        self.assertEqual(args[args.index('--vcpus') + 1], '8')
        self.assertNotIn('--sibling', args)
        self.assertEqual(args[-1], 'bash scripts/linux-preflight.sh')

    def test_remote_refusal_and_unknown_outcome_preserve_exit_and_output(self):
        for code in (17, 200, 201, 202):
            result = self.run_script('train-preflight.sh', MOTOR_EXIT=str(code))
            self.assertEqual(result.returncode, code, result.stdout + result.stderr)
            self.assertIn('motor fixture output', result.stdout)

    def test_dirty_snapshot_refuses_without_cargo(self):
        result = self.run_script('linux-preflight.sh', DIRTY_CACHE='1')
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.events.exists())
        self.assertIn('snapshot must be committed and clean', result.stderr)

    def test_unreadable_snapshot_status_refuses_without_cargo(self):
        result = self.run_script('linux-preflight.sh', GIT_EXIT='17')
        self.assertEqual(result.returncode, 2)
        self.assertIn('cannot read snapshot status', result.stderr)
        self.assertFalse(self.events.exists())

    def test_missing_motor_cli_refuses_without_local_fallback(self):
        (self.bins / 'ck').unlink()
        result = self.run_script('train-preflight.sh', PATH=f'{self.bins}:/usr/bin:/bin')
        self.assertEqual(result.returncode, 200, result.stdout + result.stderr)
        self.assertIn('no local fallback', result.stderr)
        self.assertFalse(self.events.exists())

    def test_remote_lane_runs_both_clippy_targets_and_reports_test_counts(self):
        result = self.run_script('linux-preflight.sh')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        commands = self.commands()
        clippy = [c for c in commands if c[0] == 'clippy']
        self.assertEqual(len(clippy), 2)
        self.assertNotIn('--target', clippy[0])
        self.assertIn('x86_64-pc-windows-gnu', clippy[1])
        for args in clippy:
            self.assertIn('--locked', args)
            self.assertIn('broca-module-serve/test-clock', args)
        self.assertTrue(any('--lib' in c and '--bins' in c for c in commands))
        self.assertTrue(any(c[:2] == ['nextest', 'run'] for c in commands))
        unit_run = next(c for c in commands if c[0] == 'test' and '--list' not in c)
        self.assertIn('--no-fail-fast', unit_run)
        self.assertEqual(unit_run.count('--skip'), 5)
        self.assertIn('lib/bin executed=1', result.stdout)
        self.assertIn('integration executed=1', result.stdout)
        self.assertIn('excluded=1', result.stdout)
        self.assertIn('not run in preflight:', result.stdout)

    def test_clippy_failure_stops_before_builds_or_tests(self):
        result = self.run_script('linux-preflight.sh', CLIPPY_EXIT='17')
        self.assertEqual(result.returncode, 17)
        self.assertFalse(any(c[0] in ('build', 'test') for c in self.commands()))

    def test_list_compilation_failure_prints_the_failing_output(self):
        result = self.run_script('linux-preflight.sh', LIST_EXIT='17')
        self.assertEqual(result.returncode, 17)
        self.assertIn('fixture list compilation failed', result.stderr)

    def test_unit_failure_preserves_status_after_count_reporting(self):
        result = self.run_script('linux-preflight.sh', UNIT_EXIT='17')
        self.assertEqual(result.returncode, 17)
        self.assertIn('lib/bin executed=1', result.stdout)

    def test_zero_unit_execution_refuses_preflight(self):
        result = self.run_script('linux-preflight.sh', ZERO_UNITS='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('zero lib/bin tests executed', result.stderr)

    def test_zero_integration_selection_refuses_preflight(self):
        result = self.run_script('linux-preflight.sh', ZERO_SELECTION='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('zero integration tests selected', result.stderr)

    def test_missing_clock_execution_refuses_preflight(self):
        result = self.run_script('linux-preflight.sh', NO_CLOCK_PASS='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('portable clock sentinel did not execute successfully', result.stderr)

    def test_passing_fixture_skip_refuses_preflight(self):
        result = self.run_script('linux-preflight.sh', PASSING_SKIP='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('passing fixture skip', result.stderr)

    def test_non_linux_refuses_without_cargo(self):
        result = self.run_script('linux-preflight.sh', FIXTURE_OS='Darwin')
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.events.exists())

    def test_selection_names_the_compiled_daemon_aliases_and_transform_rigs(self):
        selection = (ROOT / 'scripts/portable-test-selection.conf').read_text()
        for name in ('reply_deadline_real_daemon::', 'step_transforms::', 'binary(=step_transform_crash)'):
            self.assertIn(name, selection)

    def test_selection_excludes_live_usage_and_workstation_state_probes(self):
        selection = (ROOT / 'scripts/portable-test-selection.conf').read_text()
        for name in ('a_present_credential_is_not_consent_to_spend_it', 'anthropic_model_switch_keeps_or_strips_foreign_thinking_live',
                     'thinking_binding_beta_is_accepted_and_reports_edits_live', 'keep_all_thinking_echo_cache_live',
                     'chatgpt_websocket_live_full_input_cache_read', 'every_model_production_uses_still_resolves',
                     'the_placed_descriptor_matches_the_tracked_one', 'real_mc_compaction_client_uses_isolated_daemon_and_joint_setup'):
            self.assertIn(name, selection)


if __name__ == '__main__':
    unittest.main(verbosity=2)
