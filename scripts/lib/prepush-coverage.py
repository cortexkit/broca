#!/usr/bin/env python3
"""Map former portable tests to CI lanes and audit their execution/skip guards."""
import argparse
import collections
import json
from pathlib import Path
import re

CLOCK = ('broca-module::it', 'test_clock::test_clock_feature_is_enabled_for_retention_tests')
STATUS = re.compile(r'^\s*(PASS|FAIL)\s+\[[^\]\n]+\]\s+(?:\(\s*\d+/\d+\)\s+)?(\S+)\s+(.+?)\s*$')
SLOW_OUTPUT = re.compile(r'^\s*SLOW\s+\[\s*\d+\.\d+s\]\s+(?:\(\s*\d+/\d+\)\s+)?(\S+)\s+(.+?)\s*$')
SKIP = re.compile(r'^\s*SKIP [a-z_]')
KIT_SUITE = ('broca-module-serve::llm_runner_conformance', 'ck_broca_passes_the_llm_runner_conformance_suite')
KIT_SKIP = re.compile(r'\s*SKIP ([a-z_]+) \(requires ([^;\n]+); missing ([^)\n]+)\)\s*')
# The kit's per-session retention policy case requires session_retention, which
# broca does not declare. Legacy retention_honoured still has to execute.
# StepEvents and UserCommands are also undeclared. Pin each optional case's full
# requires/missing sets from kit 0.15.40, not its group prefix or enclosing test.
# This suite's default subject omits media, adopt, run-cause and optional adapter
# capabilities. session_retention_pending_durable and user_commands_outcome_unknown
# also lack their ReleasePending/UserCommandForwarded kill_at hooks. Only those
# exact missing-hook combinations are allowed, not a general kill skip.
KIT_SKIP_EXPECTATIONS = {
    'session_retention_honoured': ('session_retention, queue, transcript_reads, run_ops', 'session_retention'),
    'session_retention_days_and_archive': ('session_retention, queue, transcript_reads, run_ops, advance_retention_clock', 'session_retention'),
    'session_retention_adopt_policy': ('session_retention, adopt, transcript_reads', 'session_retention, adopt'),
    'session_retention_queued_never_deleted': ('session_retention, queue, transcript_reads, run_ops, advance_retention_clock', 'session_retention'),
    'session_retention_required': ('session_retention, queue, transcript_reads, run_ops', 'session_retention'),
    'session_retention_legacy_required': ('session_retention, retention, queue, transcript_reads, run_ops', 'session_retention'),
    'session_retention_conflict': ('session_retention, queue, transcript_reads, run_ops', 'session_retention'),
    'session_retention_owner_can_release': ('session_retention, queue, transcript_reads, run_ops', 'session_retention'),
    'session_retention_parent_cannot_release': ('session_retention, queue, transcript_reads, run_ops', 'session_retention'),
    'session_retention_release_idempotent': ('session_retention, queue, transcript_reads, run_ops', 'session_retention'),
    'session_retention_deletion_observable': ('session_retention, queue, transcript_reads, run_ops', 'session_retention'),
    'session_retention_media_deleted': ('session_retention, queue, transcript_reads, run_ops, attachments', 'session_retention, attachments'),
    'session_retention_active_never_deleted': ('session_retention, queue, transcript_reads, run_ops, advance_retention_clock', 'session_retention'),
    'session_retention_backup_none_refused': ('session_retention, queue, transcript_reads, run_ops', 'session_retention'),
    'session_retention_dumps_preserved': ('session_retention, queue, transcript_reads, run_ops', 'session_retention'),
    'session_retention_runner_replaced_once': ('session_retention, queue, transcript_reads, run_ops', 'session_retention'),
    'session_retention_pending_durable': ('session_retention, queue, transcript_reads, run_ops, attachments, kill_at:ReleasePending', 'session_retention, attachments, kill_at:ReleasePending'),
    'step_events_one_per_recorded_step': ('step_events, queue, transcript_reads', 'step_events'),
    'step_events_match_durable_position': ('step_events, queue, transcript_reads', 'step_events'),
    'step_events_after_durability': ('step_events, queue, transcript_reads', 'step_events'),
    'user_commands_history_isolation': ('user_commands, queue, run_ops, transcript_reads', 'user_commands'),
    'user_commands_forward_once': ('user_commands, queue, run_ops', 'user_commands'),
    'user_commands_no_person': ('user_commands, queue, run_ops', 'user_commands'),
    'user_commands_collision': ('user_commands, queue', 'user_commands'),
    'user_commands_progress_order': ('user_commands, queue, run_ops', 'user_commands'),
    'user_commands_unsafe_host_menu': ('user_commands, queue, run_ops', 'user_commands'),
    'user_commands_owner_only': ('user_commands, queue, run_ops', 'user_commands'),
    'user_commands_outcome_unknown': ('user_commands, queue, run_ops, kill_at:UserCommandForwarded', 'user_commands, kill_at:UserCommandForwarded'),
    'user_commands_cutoff_after_forward': ('user_commands, queue, run_ops', 'user_commands'),
    'user_commands_cutoff_before_forward': ('user_commands, queue, run_ops', 'user_commands'),
    'max_output_tokens_caps_response': ('max_output_tokens, run_usage, queue, transcript_reads, run_ops', 'max_output_tokens'),
    'read_stable_across_host_metadata_change': ('queue, transcript_reads, host_metadata_change', 'host_metadata_change'),
    'attachments_lifecycle': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_restart_gone': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_chunk_cap': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_commit_size': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_model_unsupported': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_model_mime': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_frozen_model': ('attachments, transcript_reads, queue, run_ops, steer, hold_tool_calls', 'attachments'),
    'attachments_send_uncommitted': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_send_unknown': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_send_other_session': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_upload_other_session_status': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_upload_other_session_commit': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_send_gone': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_default_read': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_tool_result': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_media_unreferenced': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_media_other_session': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_media_read_admission': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_admission': ('attachments, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_plan_sender': ('attachments, delivery_order, transcript_reads, queue, run_ops', 'attachments'),
    'attachments_retention': ('attachments, advance_retention_clock, transcript_reads, queue, run_ops', 'attachments'),
    'run_context_ceiling_wait_is_not_a_stall': ('queue, transcript_reads, hold_context_ceiling', 'hold_context_ceiling'),
    'adopt_binds_owner_without_a_run': ('adopt, transcript_reads', 'adopt'),
    'adopt_exact_repeat_replays_reply': ('adopt, transcript_reads', 'adopt'),
    'adopt_send_id_reuse_names_plan': ('adopt, transcript_reads', 'adopt'),
    'adopt_plan_drift_refused': ('adopt, transcript_reads', 'adopt'),
    'adopt_during_active_run': ('adopt, transcript_reads, hold_tool_calls', 'adopt'),
    'adopt_other_owner_refused': ('adopt, transcript_reads', 'adopt'),
    'adopt_same_owner_readopts': ('adopt, transcript_reads', 'adopt'),
    'adopt_host_turn_is_a_run': ('adopt, transcript_reads, run_ops', 'adopt'),
    'adopt_then_owner_sends': ('adopt, queue, transcript_reads', 'adopt'),
    'run_cause_queued_start_names_its_send': ('run_cause, live_events, streaming, queue, transcript_reads', 'run_cause'),
    'run_cause_send_started_runs_never_omit_send_id': ('run_cause, live_events, streaming, queue, transcript_reads', 'run_cause'),
    'run_cause_interrupt_names_the_new_send': ('run_cause, live_events, streaming, queue, transcript_reads', 'run_cause'),
}
SUMMARY = re.compile(r'^test result: \S+\. (\d+) passed; (\d+) failed; (\d+) ignored; \d+ measured; (\d+) filtered out; finished in ([\d.]+)s')


def read_text(path):
    return re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]', '', path.read_text(errors='replace'))


def cases(path, selected=True):
    data = json.loads(path.read_text())
    return {(binary, name): case for binary, suite in data['rust-suites'].items()
            for name, case in suite['testcases'].items()
            if not selected or (case['filter-match']['status'] == 'matches' and not case['ignored'])}


def unit_map(path, exclusions):
    all_cases = cases(path, selected=False)
    for exclusion in exclusions:
        if sum(name == exclusion for _, name in all_cases) != 1:
            raise ValueError('unit exclusion must name exactly one discovered test: ' + exclusion)
    selected = {key: case for key, case in cases(path).items() if key[1] not in exclusions}
    if not selected:
        raise ValueError('zero former portable unit tests')
    return [{'binary': binary, 'test': name, 'destination': 'linux-unit'} for binary, name in sorted(selected)]


def audit_units(path, inventory, exclusions):
    suites = json.loads(inventory.read_text())['rust-suites']
    by_executable = {}
    for binary, suite in suites.items():
        executable = Path(suite['binary-path']).name
        if executable in by_executable:
            raise ValueError('ambiguous unit executable: ' + executable)
        by_executable[executable] = binary
    current_binary = current_test = None
    results = {}
    for line in read_text(path).splitlines():
        if line.startswith('==> doctests'):
            break
        header = re.match(r'^\s*Running unittests .*? \((.+)\)\s*$', line)
        if header:
            executable = Path(header[1]).name
            if executable not in by_executable:
                raise ValueError('unknown unit executable: ' + executable)
            current_binary, current_test = by_executable[executable], None
        output = re.match(r'^---- (.+) (?:stdout|stderr) ----$', line)
        if output:
            current_test = output[1]
        if SKIP.match(line) and current_test not in exclusions:
            raise ValueError('a former portable unit returned a passing fixture skip: ' + line.strip())
        summary = SUMMARY.match(line)
        if summary and current_binary is not None:
            # A re-executed child can print a summary; the parent summary is last.
            passed, failed, ignored, filtered = map(int, summary.groups()[:4])
            results[current_binary] = {'passed': passed, 'failed': failed, 'ignored': ignored,
                                       'filtered': filtered, 'seconds': float(summary[5])}
    if set(results) != set(suites):
        raise ValueError('unit binary execution inventory differs: missing=' + ','.join(sorted(set(suites) - set(results))))
    for binary, result in results.items():
        expected = sum(not case['ignored'] for case in suites[binary]['testcases'].values())
        if result['passed'] + result['failed'] != expected or result['filtered'] != 0:
            raise ValueError('unit execution count differs for ' + binary)
    for binary, result in sorted(results.items(), key=lambda row: -row[1]['seconds']):
        print(f"unit binary {binary}: reported={result['seconds']:.3f}s passed={result['passed']} failed={result['failed']}")
    return results


def integration_map(full, portable, partition):
    expected, selected = cases(full), cases(portable)
    if not expected or not selected:
        raise ValueError('zero CI or former portable integration tests selected')
    if not selected.keys() <= expected.keys():
        raise ValueError('former portable tests are missing from the CI partition')
    lane = 'linux-integration-' + partition.split('/')[0]
    return [{'binary': binary, 'test': name, 'destination': lane, 'partition': partition}
            for binary, name in sorted(selected)]


def expected_kit_skip(current, line):
    if current != KIT_SUITE:
        return False
    match = KIT_SKIP.fullmatch(line)
    if match is None:
        return False
    expected = KIT_SKIP_EXPECTATIONS.get(match[1])
    if expected is None:
        return False
    for observed, required in zip(match.groups()[1:], expected):
        tokens = observed.split(', ')
        if len(tokens) != len(set(tokens)) or set(tokens) != set(required.split(', ')):
            return False
    return True


def audit_integration(path, full, portable):
    expected, selected = cases(full), cases(portable)
    current = None
    statuses = {}
    text = read_text(path)
    for line in text.splitlines():
        match = STATUS.match(line)
        if match:
            current = (match[2], match[3])
            if current in expected or current == CLOCK:
                statuses[current] = match[1]
        else:
            # nextest replays a successful slow test's output under a SLOW header.
            # That header identifies the owner of stdout, not another execution.
            slow = SLOW_OUTPUT.match(line)
            if slow:
                current = (slow[1], slow[2])
        if (SKIP.match(line) and (current in selected or current == KIT_SUITE)
                and not expected_kit_skip(current, line)):
            raise ValueError('an audited integration returned a passing fixture skip: ' + line.strip())
    summaries = re.findall(r'^\s*Summary\s+\[[^\]]+\]\s+(\d+) tests? run', text, re.M)
    if list(map(int, summaries)) != [1, len(expected)]:
        raise ValueError(f'CI partition execution count differs: expected clock=1, partition={len(expected)}; observed={summaries}')
    if set(statuses) != set(expected) | {CLOCK} or statuses.get(CLOCK) != 'PASS':
        raise ValueError('CI partition execution identities or clock sentinel differ from inventory')
    return {'executed': len(expected), 'portable_executed': len(selected), 'clock': 'PASS'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest='mode', required=True)
    unit = modes.add_parser('unit')
    unit.add_argument('--inventory', type=Path, required=True)
    unit.add_argument('--exclude', action='append', default=[])
    integration = modes.add_parser('integration')
    integration.add_argument('--full', type=Path, required=True)
    integration.add_argument('--portable', type=Path, required=True)
    integration.add_argument('--partition', required=True)
    for mode in (unit, integration):
        mode.add_argument('--log', type=Path)
        mode.add_argument('--output', type=Path, required=True)
        mode.add_argument('--candidate', required=True)
        mode.add_argument('--feature', action='append', required=True)
    args = parser.parse_args()
    if args.mode == 'unit':
        entries = unit_map(args.inventory, args.exclude)
        execution = audit_units(args.log, args.inventory, args.exclude) if args.log else None
    else:
        if not re.fullmatch(r'[1-4]/4', args.partition):
            raise ValueError('expected CI partition K/4')
        entries = integration_map(args.full, args.portable, args.partition)
        execution = audit_integration(args.log, args.full, args.portable) if args.log else None
    report = {'format': 'broca-prepush-portable-coverage-v1', 'candidate': args.candidate,
              'features': args.feature, 'entries': entries, 'execution': execution}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    destinations = collections.Counter(entry['destination'] for entry in entries)
    print(f'portable coverage: {len(entries)} tests -> {dict(destinations)}; report: {args.output}')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, OSError) as error:
        raise SystemExit('prepush-coverage: ' + str(error))
