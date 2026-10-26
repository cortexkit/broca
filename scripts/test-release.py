#!/usr/bin/env python3
"""Exercise exact-master/CI/version release guards with local Git and a fake GitHub CLI."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class Release(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='broca-release-')
        self.addCleanup(self.tmp.cleanup)
        self.parent = Path(self.tmp.name)
        self.repo = self.parent / 'broca'
        self.repo.mkdir()
        self.git('init', '-q', '-b', 'master')
        self.git('config', 'user.email', 'test@example.invalid')
        self.git('config', 'user.name', 'Release test')
        scripts = self.repo / 'scripts'
        scripts.mkdir()
        for name in ('release.sh', 'ci-status.sh'):
            shutil.copy2(ROOT / 'scripts' / name, scripts / name)
        self.events = self.parent / 'events'
        stage = scripts / 'stage-release.sh'
        stage.write_text(r'''#!/bin/sh
python3 - "$@" <<'PY'
import json, os, subprocess, sys
with open(os.environ['EVENTS'], 'a') as f:
    f.write(json.dumps({'stage': sys.argv[1:], 'tag': subprocess.check_output(['git','describe','--exact-match','--tags','HEAD'], text=True).strip()}) + '\n')
sys.exit(int(os.environ.get('STAGE_EXIT', '0')))
PY
''')
        (scripts / 'place-release.sh').write_text('#!/bin/sh\necho UNEXPECTED-PLACE >&2\nexit 99\n')
        (self.repo / 'Cargo.toml').write_text('[workspace.package]\nversion = "0.3.198"\n')
        self.git('add', '-A')
        self.git('commit', '-q', '-m', 'versioned batch')
        self.sha = self.git('rev-parse', 'HEAD').stdout.strip()
        self.origin = self.parent / 'origin.git'
        subprocess.run(['git', 'init', '-q', '--bare', str(self.origin)], check=True)
        self.git('remote', 'add', 'origin', str(self.origin))
        self.git('push', '-q', 'origin', 'master')
        self.git('fetch', '-q', 'origin')
        bins = self.parent / 'bin'
        bins.mkdir()
        gh = bins / 'gh'
        gh.write_text(r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
with open(os.environ['EVENTS'], 'a') as f: f.write(json.dumps({'gh': args}) + '\n')
assert args[:2] == ['run', 'list'], 'release must only read CI; no reruns or dispatches'
assert args[args.index('--commit') + 1] == os.environ['EXPECT_SHA'], 'must query the full exact SHA'
assert args[args.index('--limit') + 1] == '100'
mode = os.environ.get('CI_MODE', 'green')
if mode == 'query-error': sys.exit(17)
assert 'event' in args[args.index('--json') + 1].split(','), 'release judges runs by event'
runs = [{'name': n, 'databaseId': i, 'headSha': os.environ['EXPECT_SHA'], 'status': 'completed', 'conclusion': 'success', 'event': 'push'} for i,n in enumerate(('CI', 'Unrelated audit'), 1)]
scheduled = dict(runs[0], databaseId=9, event='schedule')
if mode == 'empty': runs = []
elif mode == 'scheduled-failed': runs.append(dict(scheduled, conclusion='failure'))
elif mode == 'scheduled-cancelled': runs.append(dict(scheduled, conclusion='cancelled'))
elif mode == 'scheduled-pending': runs.append(dict(scheduled, status='in_progress', conclusion=''))
elif mode == 'only-scheduled': runs[0]['event'] = 'schedule'
elif mode == 'second-push-failed': runs.append(dict(runs[0], databaseId=8, conclusion='failure'))
elif mode == 'missing-ci': runs = runs[1:]
elif mode == 'missing-audit': runs = runs[:1]
elif mode == 'audit-failed': runs[1]['conclusion'] = 'failure'
elif mode == 'audit-pending': runs[1]['status'] = 'in_progress'; runs[1]['conclusion'] = ''
elif mode == 'failed': runs[0]['conclusion'] = 'failure'
elif mode == 'pending': runs[0]['status'] = 'in_progress'; runs[0]['conclusion'] = ''
elif mode == 'cancelled': runs[0]['conclusion'] = 'cancelled'
elif mode == 'wrong-sha': runs[0]['headSha'] = 'a' * 40
elif mode == 'truncated': runs = [runs[0]] * 100
print(json.dumps(runs))
''')
        gh.chmod(0o755)
        self.env = {**os.environ, 'PATH': f'{bins}:{os.environ["PATH"]}', 'EVENTS': str(self.events), 'EXPECT_SHA': self.sha}

    def git(self, *args):
        return subprocess.run(['git', *args], cwd=self.repo, text=True, capture_output=True, check=True)

    def release(self, version='0.3.198', *args, **env):
        return subprocess.run(['bash', 'scripts/release.sh', version, *args], cwd=self.repo,
                              env={**self.env, **env}, text=True, capture_output=True, timeout=15)

    def records(self):
        return [json.loads(line) for line in self.events.read_text().splitlines()] if self.events.exists() else []

    def assert_not_tagged_or_staged(self, result):
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.git('tag', '--list', 'v0.3.198').stdout, '')
        self.assertFalse(any('stage' in record for record in self.records()))

    def test_green_exact_master_tags_and_stages_without_gate_train_or_ci_rerun(self):
        result = self.release('0.3.198', '--marker', 'literal marker with spaces')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.git('rev-parse', 'v0.3.198^{commit}').stdout.strip(), self.sha)
        self.assertEqual(self.git('rev-parse', 'HEAD').stdout.strip(), self.sha)
        self.assertEqual(self.git('status', '--porcelain').stdout, '')
        records = self.records()
        self.assertEqual(len(records), 2)
        self.assertEqual(records[-1]['stage'], ['--marker', 'literal marker with spaces'])
        self.assertEqual(records[-1]['tag'], 'v0.3.198')
        self.assertNotIn('UNEXPECTED-PLACE', result.stderr)

    def test_unmerged_head_refuses_even_when_ci_for_it_is_green(self):
        self.git('commit', '-q', '--allow-empty', '-m', 'unmerged')
        new_sha = self.git('rev-parse', 'HEAD').stdout.strip()
        self.assert_not_tagged_or_staged(self.release(EXPECT_SHA=new_sha))

    def test_dirty_workspace_refuses(self):
        (self.repo / 'Cargo.toml').write_text('[workspace.package]\nversion = "0.3.199"\n')
        self.assert_not_tagged_or_staged(self.release())

    def test_version_mismatch_refuses(self):
        self.assert_not_tagged_or_staged(self.release('0.3.199'))

    def test_invalid_version_and_probe_only_refuse_before_tag(self):
        self.assert_not_tagged_or_staged(self.release('v0.3.198'))
        self.assert_not_tagged_or_staged(self.release('0.3.198', '--probe-only', '/tmp/fake'))

    def test_empty_ci_refuses(self):
        self.assert_not_tagged_or_staged(self.release(CI_MODE='empty'))

    def test_missing_ci_workflow_refuses(self):
        self.assert_not_tagged_or_staged(self.release(CI_MODE='missing-ci'))

    def test_unrelated_workflow_runs_never_hold_a_release(self):
        # Only CI gates a release; unrelated workflow runs must not block tagging or staging.
        for mode in ('missing-audit', 'audit-failed', 'audit-pending'):
            with self.subTest(mode=mode):
                self.git('tag', '-d', 'v0.3.198') if self.git('tag', '--list', 'v0.3.198').stdout else None
                result = self.release(CI_MODE=mode)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(self.git('rev-parse', 'v0.3.198^{commit}').stdout.strip(), self.sha)

    def test_a_scheduled_ci_run_never_holds_a_release(self):
        # The nightly CI run on the same commit uses floating sibling revisions; a failed,
        # cancelled or still-running one says nothing about the pinned push build.
        for mode in ('scheduled-failed', 'scheduled-cancelled', 'scheduled-pending'):
            with self.subTest(mode=mode):
                self.git('tag', '-d', 'v0.3.198') if self.git('tag', '--list', 'v0.3.198').stdout else None
                result = self.release(CI_MODE=mode)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn('ignored schedule run: CI', result.stdout)
                self.assertEqual(self.git('rev-parse', 'v0.3.198^{commit}').stdout.strip(), self.sha)

    def test_a_scheduled_success_cannot_stand_in_for_a_push_run(self):
        result = self.release(CI_MODE='only-scheduled')
        self.assert_not_tagged_or_staged(result)
        self.assertIn('missing required workflows: CI (no push run)', result.stderr)

    def test_any_failed_push_run_refuses(self):
        # A train commit carries two push runs (train ref, then master). Both must be green.
        self.assert_not_tagged_or_staged(self.release(CI_MODE='second-push-failed'))

    def test_failed_ci_refuses(self):
        self.assert_not_tagged_or_staged(self.release(CI_MODE='failed'))

    def test_pending_ci_refuses(self):
        self.assert_not_tagged_or_staged(self.release(CI_MODE='pending'))

    def test_cancelled_ci_refuses(self):
        self.assert_not_tagged_or_staged(self.release(CI_MODE='cancelled'))

    def test_wrong_sha_ci_refuses(self):
        self.assert_not_tagged_or_staged(self.release(CI_MODE='wrong-sha'))

    def test_truncated_ci_refuses(self):
        self.assert_not_tagged_or_staged(self.release(CI_MODE='truncated'))

    def test_ci_query_failure_refuses(self):
        self.assert_not_tagged_or_staged(self.release(CI_MODE='query-error'))

    def test_existing_tag_on_other_commit_is_not_moved(self):
        other = self.git('commit-tree', 'HEAD^{tree}', '-m', 'unrelated commit').stdout.strip()
        self.git('tag', 'v0.3.198', other)
        result = self.release()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.git('rev-parse', 'v0.3.198^{commit}').stdout.strip(), other)
        self.assertFalse(any('stage' in record for record in self.records()))

    def test_failed_staging_keeps_same_tag_for_retry_and_never_places(self):
        result = self.release(STAGE_EXIT='17')
        self.assertEqual(result.returncode, 17, result.stdout + result.stderr)
        self.assertEqual(self.git('rev-parse', 'v0.3.198^{commit}').stdout.strip(), self.sha)
        result = self.release()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn('UNEXPECTED-PLACE', result.stderr)


if __name__ == '__main__':
    unittest.main(verbosity=2)
