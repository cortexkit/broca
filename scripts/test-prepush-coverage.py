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
# Kit 0.15.40 transcript cases; the two per-session media cases use the missing
# sets after attachments becomes declared.
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
SKIP session_retention_media_deleted (requires session_retention, queue, transcript_reads, run_ops, attachments; missing session_retention)
SKIP session_retention_active_never_deleted (requires session_retention, queue, transcript_reads, run_ops, advance_retention_clock; missing session_retention)
SKIP session_retention_backup_none_refused (requires session_retention, queue, transcript_reads, run_ops; missing session_retention)
SKIP session_retention_dumps_preserved (requires session_retention, queue, transcript_reads, run_ops; missing session_retention)
SKIP session_retention_runner_replaced_once (requires session_retention, queue, transcript_reads, run_ops; missing session_retention)
SKIP session_retention_pending_durable (requires session_retention, queue, transcript_reads, run_ops, attachments, kill_at:ReleasePending; missing session_retention, kill_at:ReleasePending)
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
SKIP user_commands_cutoff_before_forward (requires user_commands, queue, run_ops; missing user_commands)
SKIP max_output_tokens_caps_response (requires max_output_tokens, run_usage, queue, transcript_reads, run_ops; missing max_output_tokens)
SKIP read_stable_across_host_metadata_change (requires queue, transcript_reads, host_metadata_change; missing host_metadata_change)
SKIP attachments_lifecycle (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_restart_gone (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_chunk_cap (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_commit_size (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_model_unsupported (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_model_mime (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_frozen_model (requires attachments, transcript_reads, queue, run_ops, steer, hold_tool_calls; missing attachments)
SKIP attachments_send_uncommitted (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_send_unknown (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_send_other_session (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_upload_other_session_status (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_upload_other_session_commit (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_send_gone (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_default_read (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_tool_result (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_media_unreferenced (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_media_other_session (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_media_read_admission (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_admission (requires attachments, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_plan_sender (requires attachments, delivery_order, transcript_reads, queue, run_ops; missing attachments)
SKIP attachments_retention (requires attachments, advance_retention_clock, transcript_reads, queue, run_ops; missing attachments)
SKIP run_context_ceiling_wait_is_not_a_stall (requires queue, transcript_reads, hold_context_ceiling; missing hold_context_ceiling)
SKIP adopt_binds_owner_without_a_run (requires adopt, transcript_reads; missing adopt)
SKIP adopt_exact_repeat_replays_reply (requires adopt, transcript_reads; missing adopt)
SKIP adopt_send_id_reuse_names_plan (requires adopt, transcript_reads; missing adopt)
SKIP adopt_plan_drift_refused (requires adopt, transcript_reads; missing adopt)
SKIP adopt_during_active_run (requires adopt, transcript_reads, hold_tool_calls; missing adopt)
SKIP adopt_other_owner_refused (requires adopt, transcript_reads; missing adopt)
SKIP adopt_same_owner_readopts (requires adopt, transcript_reads; missing adopt)
SKIP adopt_host_turn_is_a_run (requires adopt, transcript_reads, run_ops; missing adopt)
SKIP adopt_then_owner_sends (requires adopt, queue, transcript_reads; missing adopt)
SKIP run_cause_queued_start_names_its_send (requires run_cause, live_events, streaming, queue, transcript_reads; missing run_cause)
SKIP run_cause_send_started_runs_never_omit_send_id (requires run_cause, live_events, streaming, queue, transcript_reads; missing run_cause)
SKIP run_cause_interrupt_names_the_new_send (requires run_cause, live_events, streaming, queue, transcript_reads; missing run_cause)'''


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

    def audit_kit_skip(self, line, enclosing=KIT_CASE, slow_output=False, result='PASS', selected=True):
        binary, test = enclosing
        suite = {binary: [(test, False, 'matches')]}
        if slow_output:
            suite['other::it'] = [('another_portable_case', False, 'matches')]
        full = self.inventory('kit-full.json', suite)
        portable = self.inventory('kit-portable.json', suite if selected else {})
        clock_binary, sentinel = self.helper.CLOCK
        log = self.root / 'kit.log'
        count = 2 if slow_output else 1
        output = (f'PASS [0.01s] {clock_binary} {sentinel}\nSummary [0.01s] 1 test run\n'
                  f'{result} [0.10s] (1/{count}) {binary} {test}\n')
        if slow_output:
            output += ('PASS [0.11s] (2/2) other::it another_portable_case\n'
                       f'SLOW [147.383s] (1/2) {binary} {test}\n')
        output += f'  stdout ───\n    {line}\nSummary [0.10s] {count} tests run\n'
        log.write_text(output)
        return self.helper.audit_integration(log, full, portable)

    def test_real_session_retention_skip_is_expected_without_exempting_the_suite(self):
        line = ('SKIP session_retention_honoured (requires session_retention, queue, '
                'transcript_reads, run_ops; missing session_retention)')
        self.assertEqual(self.audit_kit_skip(line),
                         {'executed': 1, 'portable_executed': 1, 'clock': 'PASS'})

    def test_reviewed_optional_kit_cases_match_the_independent_transcript(self):
        lines = [line for line in KIT_SKIPS.splitlines() if not line.startswith('SKIP attachments_')]
        self.assertEqual(len(lines), 45)
        self.assertEqual({line.split()[1] for line in lines}, set(self.helper.KIT_SKIP_EXPECTATIONS))
        for line in lines:
            with self.subTest(case=line.split()[1]):
                self.assertEqual(self.audit_kit_skip(line)['portable_executed'], 1)

    def test_attachment_kit_cases_must_execute(self):
        lines = [line for line in KIT_SKIPS.splitlines() if line.startswith('SKIP attachments_')]
        self.assertEqual(len(lines), 21)
        lines += [
            'SKIP attachments_commit_sha256 (requires attachments, transcript_reads, queue, run_ops; missing attachments)',
            'SKIP attachments_new_case (requires attachments, transcript_reads, queue, run_ops; missing attachments)',
        ]
        for line in lines:
            with self.subTest(case=line.split()[1]):
                with self.assertRaisesRegex(ValueError, 'passing fixture skip'):
                    self.audit_kit_skip(line)

    def test_session_retention_media_skips_keep_the_reduced_missing_sets(self):
        for line in [KIT_SKIPS.splitlines()[11], KIT_SKIPS.splitlines()[16]]:
            with self.subTest(case=line.split()[1]):
                self.assertEqual(self.audit_kit_skip(line)['portable_executed'], 1)

    def test_expected_kit_skip_requires_the_exact_enclosing_binary_and_test(self):
        line = KIT_SKIPS.splitlines()[0]
        for enclosing in [('other::llm_runner_conformance', KIT_CASE[1]),
                          (KIT_CASE[0], 'another_conformance_test'),
                          (KIT_CASE[0], 'kit_compaction_cases_without_process_kills'),
                          (KIT_CASE[0], KIT_CASE[1] + '_extra')]:
            with self.subTest(enclosing=enclosing):
                with self.assertRaisesRegex(ValueError, 'passing fixture skip'):
                    self.audit_kit_skip(line, enclosing)

    def test_slow_output_header_attributes_real_kit_skip_to_its_owner(self):
        result = self.audit_kit_skip(KIT_SKIPS.splitlines()[0], slow_output=True)
        self.assertEqual(result, {'executed': 2, 'portable_executed': 2, 'clock': 'PASS'})

    def test_slow_header_is_not_a_passing_execution(self):
        with self.assertRaisesRegex(ValueError, 'execution identities'):
            self.audit_kit_skip(KIT_SKIPS.splitlines()[0], result='SLOW')

    def test_named_kit_case_guard_stays_strict_outside_portable_selection(self):
        result = self.audit_kit_skip(KIT_SKIPS.splitlines()[0], selected=False)
        self.assertEqual(result, {'executed': 1, 'portable_executed': 0, 'clock': 'PASS'})
        for line in ['SKIP fixture: absent',
                     'SKIP retention_honoured (requires retention, queue, transcript_reads, run_ops; missing retention)']:
            with self.subTest(line=line):
                with self.assertRaisesRegex(ValueError, 'passing fixture skip'):
                    self.audit_kit_skip(line, selected=False)

    def test_arbitrary_session_and_legacy_retention_skips_stay_strict(self):
        lines = [
            'SKIP fixture: absent',
            'SKIP session_unknown (requires queue; missing queue)',
            KIT_SKIPS.splitlines()[0].replace('session_retention_honoured', 'session_retention_new_case'),
            'SKIP step_events_new_case (requires step_events, queue, transcript_reads; missing step_events)',
            'SKIP user_commands_new_case (requires user_commands, queue; missing user_commands)',
            'SKIP retention_honoured (requires retention, queue, transcript_reads, run_ops; missing retention)',
            'SKIP tags_resolve_answers_the_current_lineage (requires tags, queue, transcript_reads, dispatch_attribution, kill_at:StepRecorded; missing kill_at:StepRecorded)',
            'SKIP structured_output_frozen_across_restart (requires structured_output, queue, transcript_reads, run_ops, kill_at:ToolResultRecorded; missing kill_at:ToolResultRecorded)',
            'SKIP run_result_interrupted_not_cancelled (requires run_ops, queue, transcript_reads, kill_at:DispatchIntent; missing kill_at:DispatchIntent)',
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
                 KIT_SKIPS.splitlines()[5].replace('missing session_retention', 'missing session_retention, retention'),
                 KIT_SKIPS.splitlines()[11].replace('missing session_retention', 'missing session_retention, attachments'),
                 KIT_SKIPS.splitlines()[16].replace('missing session_retention, kill_at:ReleasePending',
                                                   'missing session_retention, attachments, kill_at:ReleasePending')]
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
