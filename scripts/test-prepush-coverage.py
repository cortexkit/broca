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


if __name__ == '__main__':
    unittest.main(verbosity=2)
