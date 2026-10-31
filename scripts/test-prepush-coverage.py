#!/usr/bin/env python3
"""The deduplicated gate must retain every portable test and its skip guards."""
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import subprocess
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
KIT_CASE = ('broca-module-serve::llm_runner_conformance', 'ck_broca_passes_the_llm_runner_conformance_suite')
# Captured conformance output, independently reviewed against kit 0.15.40 metadata.
KIT_SKIPS = '''SKIP session_retention_honoured (requires session_retention, queue, transcript_reads, run_ops; missing session_retention)
SKIP session_retention_days_and_archive (requires session_retention, queue, transcript_reads, run_ops, advance_retention_clock; missing session_retention)
SKIP session_retention_adopt_policy (requires session_retention, adopt, transcript_reads; missing session_retention, adopt)
SKIP session_retention_queued_never_deleted (requires session_retention, queue, transcript_reads, run_ops, advance_retention_clock; missing session_retention)
SKIP session_retention_required (requires session_retention, queue, transcript_reads, run_ops; missing session_retention)
SKIP session_retention_legacy_required (requires session_retention, retention, queue, transcript_reads, run_ops; missing session_retention)
SKIP session_retention_conflict (requires session_retention, queue, transcript_reads, run_ops; missing session_retention)
SKIP session_retention_owner_can_release (requires session_retention, queue, transcript_reads, run_ops; missing session_retention)
SKIP session_retention_parent_cannot_release (requires session_retention, queue, transcript_reads, run_ops; missing session_retention)
SKIP session_retention_release_idempotent (requires session_retention, queue, transcript_reads, run_ops; missing session_retention)
SKIP session_retention_deletion_observable (requires session_retention, queue, transcript_reads, run_ops; missing session_retention)
SKIP session_retention_media_deleted (requires session_retention, queue, transcript_reads, run_ops, attachments; missing session_retention, attachments)
SKIP session_retention_active_never_deleted (requires session_retention, queue, transcript_reads, run_ops, advance_retention_clock; missing session_retention)
SKIP session_retention_backup_none_refused (requires session_retention, queue, transcript_reads, run_ops; missing session_retention)
SKIP session_retention_dumps_preserved (requires session_retention, queue, transcript_reads, run_ops; missing session_retention)
SKIP session_retention_runner_replaced_once (requires session_retention, queue, transcript_reads, run_ops; missing session_retention)
SKIP session_retention_pending_durable (requires session_retention, queue, transcript_reads, run_ops, attachments, kill_at:ReleasePending; missing session_retention, attachments, kill_at:ReleasePending)
SKIP step_events_one_per_recorded_step (requires step_events, queue, transcript_reads; missing step_events)
SKIP step_events_match_durable_position (requires step_events, queue, transcript_reads; missing step_events)
SKIP step_events_after_durability (requires step_events, queue, transcript_reads; missing step_events)
SKIP user_commands_history_isolation (requires user_commands, queue, run_ops, transcript_reads; missing user_commands)
SKIP user_commands_forward_once (requires user_commands, queue, run_ops; missing user_commands)
SKIP user_commands_no_person (requires user_commands, queue, run_ops; missing user_commands)
SKIP user_commands_collision (requires user_commands, queue; missing user_commands)
SKIP user_commands_progress_order (requires user_commands, queue, run_ops; missing user_commands)
SKIP user_commands_unsafe_host_menu (requires user_commands, queue, run_ops; missing user_commands)
SKIP user_commands_owner_only (requires user_commands, queue, run_ops; missing user_commands)
SKIP user_commands_outcome_unknown (requires user_commands, queue, run_ops, kill_at:UserCommandForwarded; missing user_commands, kill_at:UserCommandForwarded)
SKIP user_commands_cutoff_after_forward (requires user_commands, queue, run_ops; missing user_commands)
SKIP user_commands_cutoff_before_forward (requires user_commands, queue, run_ops; missing user_commands)'''


class CoverageGuards(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('coverage', ROOT / 'scripts/lib/prepush-coverage.py')
        assert spec is not None and spec.loader is not None
        self.helper = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.helper)
        self.tmp = tempfile.TemporaryDirectory(prefix='prepush-coverage-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def inventory(self, name, suites):
        path = self.root / name
        path.write_text(json.dumps({'rust-suites': {
            binary: {'binary-path': '/tmp/' + binary.replace(':', '_') + '-hash',
                     'testcases': {test: {'ignored': ignored, 'filter-match': {'status': status}}
                                   for test, ignored, status in tests}}
            for binary, tests in suites.items()}}))
        return path

    def unit_inventory(self):
        return self.inventory('unit.json', {
            'alpha': [('tests::portable', False, 'matches'), ('tests::excluded', False, 'matches'),
                      ('tests::ignored', True, 'matches')], 'zero': []})

    def unit_log(self, *, skip='tests::excluded', parent_passed=2, filtered=0, zero=True):
        output = ('     Running unittests src/lib.rs (/tmp/alpha-hash)\n'
                  'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.01s\n'
                  f'---- {skip} stdout ----\nSKIP fixture: optional input absent\n'
                  f'test result: ok. {parent_passed} passed; 0 failed; 1 ignored; 0 measured; {filtered} filtered out; finished in 2.00s\n')
        if zero:
            output += ('     Running unittests src/main.rs (/tmp/zero-hash)\n'
                       'test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n')
        path = self.root / 'units.log'
        path.write_text(output)
        return path

    def test_unit_mapping_retains_every_nonignored_nonexcluded_case(self):
        path = self.unit_inventory()
        self.assertEqual(self.helper.unit_map(path, ['tests::excluded']),
                         [{'binary': 'alpha', 'test': 'tests::portable', 'destination': 'linux-unit'}])
        with self.assertRaisesRegex(ValueError, 'exactly one discovered'):
            self.helper.unit_map(path, ['missing'])

    def test_unit_audit_uses_parent_summary_and_keeps_portable_skip_guard(self):
        with patch('sys.stdout', new_callable=io.StringIO) as output:
            result = self.helper.audit_units(self.unit_log(), self.unit_inventory(), ['tests::excluded'])
        self.assertEqual(result['alpha']['seconds'], 2.0)
        self.assertIn('unit binary alpha: reported=2.000s', output.getvalue())
        with self.assertRaisesRegex(ValueError, 'passing fixture skip'):
            self.helper.audit_units(self.unit_log(skip='tests::portable'), self.unit_inventory(), ['tests::excluded'])

    def test_missing_binary_or_filtered_units_cannot_count_as_full_ci_execution(self):
        for label, make_log in (('missing binary', lambda: self.unit_log(zero=False)),
                                ('missing test', lambda: self.unit_log(parent_passed=1)),
                                ('filtered test', lambda: self.unit_log(filtered=1))):
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, 'unit binary execution inventory|unit execution count'):
                    self.helper.audit_units(make_log(), self.unit_inventory(), ['tests::excluded'])

    def test_real_cargo_headers_and_parent_summaries_match_nextest_inventory(self):
        project = self.root / 'cargo workspace (labels)'
        (project / 'src').mkdir(parents=True)
        (project / 'Cargo.toml').write_text('[package]\nname="coverage-fixture"\nversion="0.1.0"\nedition="2021"\n')
        (project / 'src/lib.rs').write_text('''#[cfg(test)] mod tests {
#[test] fn output() { println!("visible passing output"); }
#[test] #[ignore] fn ignored() {}
}
''')
        (project / 'src/main.rs').write_text('fn main() {}\n')
        env = dict(os.environ, CARGO_TARGET_DIR=str(project / 'target'))
        def cargo(*args):
            result = subprocess.run(['cargo', *args], cwd=project, env=env, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return result
        cargo('generate-lockfile', '--offline')
        listed = cargo('nextest', 'list', '--offline', '--locked', '--workspace', '--lib', '--bins', '-T', 'json')
        inventory = self.root / 'real-unit.json'
        inventory.write_text(listed.stdout)
        log = self.root / 'real-unit.log'
        # Cargo's stderr labels must stay ordered with libtest's stdout summaries.
        combined = subprocess.run(['cargo', 'test', '--offline', '--locked', '--workspace', '--lib', '--bins',
                                   '--no-fail-fast', '--', '--quiet', '--show-output'], cwd=project, env=env,
                                  text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.assertEqual(combined.returncode, 0, combined.stdout)
        log.write_text(combined.stdout)
        with patch('sys.stdout', new_callable=io.StringIO):
            results = self.helper.audit_units(log, inventory, [])
        self.assertEqual(sum(row['passed'] for row in results.values()), 1)

    def integration_inventories(self):
        tests = [('portable_case', False, 'matches'), ('other_case', False, 'matches')]
        full = self.inventory('full.json', {'pkg::it': tests})
        portable = self.inventory('portable.json', {'pkg::it': [tests[0], ('other_case', False, 'mismatch')]})
        return full, portable

    def integration_log(self, *, skip='other_case', count=2, omit_portable=False, clock='PASS'):
        binary, sentinel = self.helper.CLOCK
        output = f'        {clock} [0.01s] {binary} {sentinel}\nSummary [0.01s] 1 test run\n'
        if not omit_portable:
            output += '        PASS [0.10s] (1/2) pkg::it portable_case\n'
        if skip == 'portable_case':
            output += '  stdout ───\n    SKIP fixture: absent\n'
        output += '        PASS [0.10s] (2/2) pkg::it other_case\n'
        if skip == 'other_case':
            output += '  stdout ───\n    SKIP optional_private_fixture: absent\n'
        output += f'Summary [0.20s] {count} tests run\n'
        path = self.root / 'integration.log'
        path.write_text(output)
        return path

    def test_partition_mapping_names_destination_without_rejecting_nonportable_skips(self):
        full, portable = self.integration_inventories()
        self.assertEqual(self.helper.integration_map(full, portable, '3/4'),
                         [{'binary': 'pkg::it', 'test': 'portable_case',
                           'destination': 'linux-integration-3', 'partition': '3/4'}])
        result = self.helper.audit_integration(self.integration_log(), full, portable)
        self.assertEqual(result, {'executed': 2, 'portable_executed': 1, 'clock': 'PASS'})

    def test_portable_passing_skip_missing_execution_count_and_clock_still_fail(self):
        full, portable = self.integration_inventories()
        for label, make_log in (('passing skip', lambda: self.integration_log(skip='portable_case')),
                                ('wrong count', lambda: self.integration_log(count=1)),
                                ('missing test', lambda: self.integration_log(omit_portable=True)),
                                ('missing clock', lambda: self.integration_log(clock='FAIL'))):
            with self.subTest(label=label):
                with self.assertRaises(ValueError):
                    self.helper.audit_integration(make_log(), full, portable)

    def test_portable_only_test_is_not_silently_dropped(self):
        full, _ = self.integration_inventories()
        portable = self.inventory('extra.json', {'pkg::it': [('portable_only', False, 'matches')]})
        with self.assertRaisesRegex(ValueError, 'missing from the CI partition'):
            self.helper.integration_map(full, portable, '1/4')

    def audit_kit_skip(self, line, enclosing=KIT_CASE):
        binary, test = enclosing
        suite = {binary: [(test, False, 'matches')]}
        full = self.inventory('kit-full.json', suite)
        portable = self.inventory('kit-portable.json', suite)
        clock_binary, sentinel = self.helper.CLOCK
        log = self.root / 'kit.log'
        log.write_text(f'PASS [0.01s] {clock_binary} {sentinel}\nSummary [0.01s] 1 test run\n'
                       f'PASS [0.10s] (1/1) {binary} {test}\n  stdout ───\n    {line}\n'
                       'Summary [0.10s] 1 test run\n')
        return self.helper.audit_integration(log, full, portable)

    def test_real_session_retention_skip_is_expected_without_exempting_the_suite(self):
        line = ('SKIP session_retention_honoured (requires session_retention, queue, '
                'transcript_reads, run_ops; missing session_retention)')
        self.assertEqual(self.audit_kit_skip(line),
                         {'executed': 1, 'portable_executed': 1, 'clock': 'PASS'})

    def test_reviewed_optional_kit_cases_match_the_independent_transcript(self):
        lines = KIT_SKIPS.splitlines()
        self.assertEqual(len(lines), 30)
        self.assertEqual({line.split()[1] for line in lines}, set(self.helper.KIT_SKIP_EXPECTATIONS))
        for line in lines:
            with self.subTest(case=line.split()[1]):
                self.assertEqual(self.audit_kit_skip(line)['portable_executed'], 1)

    def test_expected_kit_skip_requires_the_exact_enclosing_binary_and_test(self):
        line = KIT_SKIPS.splitlines()[0]
        for enclosing in [('other::llm_runner_conformance', KIT_CASE[1]),
                          (KIT_CASE[0], 'another_conformance_test'),
                          (KIT_CASE[0], KIT_CASE[1] + '_extra')]:
            with self.subTest(enclosing=enclosing):
                with self.assertRaisesRegex(ValueError, 'passing fixture skip'):
                    self.audit_kit_skip(line, enclosing)

    def test_arbitrary_session_and_legacy_retention_skips_stay_strict(self):
        lines = [
            'SKIP fixture: absent',
            'SKIP session_unknown (requires queue; missing queue)',
            KIT_SKIPS.splitlines()[0].replace('session_retention_honoured', 'session_retention_new_case'),
            'SKIP step_events_new_case (requires step_events, queue, transcript_reads; missing step_events)',
            'SKIP user_commands_new_case (requires user_commands, queue; missing user_commands)',
            'SKIP retention_honoured (requires retention, queue, transcript_reads, run_ops; missing retention)',
        ]
        for line in lines:
            with self.subTest(line=line):
                with self.assertRaisesRegex(ValueError, 'passing fixture skip'):
                    self.audit_kit_skip(line)

    def test_kit_skip_requires_exact_full_and_missing_capability_sets(self):
        line = KIT_SKIPS.splitlines()[0]
        lines = [line.replace('missing session_retention', 'missing session_retention, queue'),
                 line.replace('missing session_retention', 'missing retention'),
                 line.replace('missing session_retention', 'missing '),
                 line.replace('missing session_retention', 'missing session_retention, session_retention'),
                 line.replace('session_retention, queue,', 'session_retention,'),
                 line.replace('session_retention, queue,', 'session_retention, queue, queue,'),
                 line.replace('run_ops;', 'run_ops, retention;'),
                 line + ' unrelated output',
                 KIT_SKIPS.splitlines()[5].replace('missing session_retention', 'missing session_retention, retention')]
        for invalid in lines:
            with self.subTest(line=invalid):
                with self.assertRaisesRegex(ValueError, 'passing fixture skip'):
                    self.audit_kit_skip(invalid)
        reordered = line.replace('session_retention, queue, transcript_reads, run_ops;',
                                 'run_ops, queue, session_retention, transcript_reads;')
        self.assertEqual(self.audit_kit_skip(reordered)['portable_executed'], 1)
        reordered_missing = KIT_SKIPS.splitlines()[2].replace('missing session_retention, adopt',
                                                             'missing adopt, session_retention')
        self.assertEqual(self.audit_kit_skip(reordered_missing)['portable_executed'], 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
