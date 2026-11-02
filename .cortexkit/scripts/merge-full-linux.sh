#!/usr/bin/env bash
set -euo pipefail
# Run from the repository root whatever directory invokes this, so the relative paths below
# (the slow-test list, the merge inputs) resolve.
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mkdir -p target/scratch
out=.cortexkit/internal-docs/investigations/merge-258769b37-linux
mkdir -p "$out"
features=(-F broca-provider/live -F broca-provider/test-harness -F broca-provider/portable-power-loss -F broca-catalog/live -F broca-module-serve/test-clock)
exclusions=(
 'binary(=llm_runner_conformance)'
 'binary(=step_transform_crash)'
 '(package(=broca-subc) & binary(=it))'
 '(package(=broca-run) & binary(=scope_real_daemon_e2e))'
 '(package(=broca-module-serve) & (binary(=classifier_conformance) | binary(=scope_real_daemon_e2e) | binary(=llm_tools_cerebellum) | binary(=tool_result_v1_real_daemon) | binary(=tool_routes_real_daemon)))'
 '(package(=broca-module-serve) & binary(=llm_tools_route) & not test(/^memory_/))'
 '(package(=broca-module-serve) & binary(=llm_tools_private_content) & not test(/^(an_empty_consumer_nonce_is_not_readiness|the_stand_in_pins_the_daemon_wire_versions)$/))'
 '(package(=broca-module-serve) & binary(=run_events_bus) & test(/^real_daemon_/))'
 '(package(=broca-module-serve) & binary(=llm_tools_permissions) & test(/^native_/))'
 'test(/(real_module_e2e::|aimock_guard::|all_families_smoke::|anthropic_e2e::|bedrock_e2e::|gemini_e2e::|mixed_family_concurrency_e2e::|openai_chat_e2e::|openai_responses_e2e::|stream_errors_e2e::|tool_arg_errors_e2e::|transport_errors_e2e::|account_selection_real_daemon::|captured_replay_e2e::|graceful_stop_real_daemon::|launch_nonce_e2e::|plan_fetch_real_daemon::|preset_real_daemon::|reply_deadline_real_daemon::|send_tool_reply_real_daemon::|step_transforms::|scope_real_daemon_e2e::|seal_line_is_filterable_e2e::|approval_gate::|single_turn_module_e2e::|retention_consent_wire::|real_daemon::two_real_daemons_|renderer_version_resume_real_daemon::)/)'
 'test(/plan_admission::(real_daemon_|step_transforms_admit_through_the_role_route_and_freeze_the_declaration)/)'
 'test(/^(executor::tests::session_close_releases_hook_and_compaction_routes|real_mc_compaction_client_uses_isolated_daemon_and_joint_setup|target_cingulate_admits_reserved_broca_with_private_content_card_scope|target_cingulate_admits_always_option_for_an_attested_project)$/)'
 # A real-daemon differential: it asserts (does not skip) on SUBC_CORE_BIN, so it belongs with
 # the other native lanes this gate does not stage.
 '(package(=broca-module-serve) & binary(=overhaul_real_daemon))'
 # The attempt-hook shards read one shared in-process capture and stamp the enclosing shard
 # into every image name, so nextest's per-test processes collide on that directory: 1025
 # shards in the recorded run. scripts/linux-preflight.sh selects none of them either (its
 # filter is `kind(test)`, and these are lib tests run on shared-process libtest), so this
 # gate excludes the same group master's own preflight never runs.
 '(package(=broca-provider) & test(/^transport::tests::attempt_hook_power_loss_portable::/))'
)
while IFS='|' read -r package name seconds; do
  [[ -z "$package" || "$package" == \#* ]] && continue
  exclusions+=("(package(=$package) & test(=$name))")
done < scripts/quickcheck-slow-tests.list
filter="not (${exclusions[0]}"
for exclusion in "${exclusions[@]:1}"; do filter+=" | $exclusion"; done
filter+=')'
printf '%s\n' "$filter" > "$out/filter.txt"
printf '%s\n' "${exclusions[@]}" > "$out/exclusion-expressions.txt"
export BROCA_REQUIRE_SUBC=1 BROCA_REQUIRE_VAULT=1 BROCA_REQUIRE_AIMOCK=1
export CK_CLAUSTRUM_CHECKOUT="$PWD/.cortexkit/scripts/merge-inputs/claustrum"
unset SUBC_CORE_BIN FAKE_AFT_STUB_BIN FAKE_SURFACE_BIN CK_CLAUSTRUM_BIN CK_AUTH_BIN SUBC_MODULE_ID SUBC_LAUNCH_NONCE
export HOME="$PWD/target/scratch/merge-empty-home"
export XDG_DATA_HOME="$HOME/.local/share"
mkdir -p "$HOME"
npm ci
cargo nextest list --workspace --locked "${features[@]}" --ignore-default-filter -E "$filter" -T json > target/scratch/merge-full-inventory.json
python3 - <<'PY'
import json
from pathlib import Path
suites=json.loads(Path('target/scratch/merge-full-inventory.json').read_text())['rust-suites']
selected=[]; excluded=[]; ignored=[]; by_binary={}
for binary,suite in suites.items():
    counts={'selected':0,'excluded':0,'ignored':0}
    for name,case in suite['testcases'].items():
        row={'binary':binary,'test':name}
        if case['filter-match']['status']!='matches':
            excluded.append(row); counts['excluded']+=1
        elif case['ignored']:
            ignored.append(row); counts['ignored']+=1
        else:
            selected.append(row); counts['selected']+=1
    by_binary[binary]=counts
for binary in ['broca-module::it','broca-module-serve::it','broca-store::it']:
    assert by_binary[binary]['selected']>0, (binary,by_binary.get(binary))
out=Path('.cortexkit/internal-docs/investigations/merge-258769b37-linux')
summary={'counts':{'selected':len(selected),'excluded':len(excluded),'cargo_ignored':len(ignored)},'by_binary':by_binary,'every_exclusion':sorted(excluded,key=lambda x:(x['binary'],x['test'])),'cargo_ignored':sorted(ignored,key=lambda x:(x['binary'],x['test']))}
(out/'inventory.json').write_text(json.dumps(summary,indent=2)+'\n')
print('Full Linux inventory:',summary['counts'])
for binary in ['broca-module::it','broca-module-serve::it','broca-store::it']:
    print(binary,by_binary[binary])
PY
set +e
cargo nextest run --workspace --locked "${features[@]}" --ignore-default-filter --no-tests fail -P ci --no-fail-fast -E "$filter" --success-output final --failure-output immediate-final 2>&1 | tee target/scratch/merge-full-linux.log
run_rc=${PIPESTATUS[0]}
python3 - <<'PY'
import json,re
from pathlib import Path
out=Path('.cortexkit/internal-docs/investigations/merge-258769b37-linux')
summary={'summaries':[], 'failures':[], 'passing_skip_output':[], 'unapproved_fixture_skips':[], 'failed_retirement_candidates':[], 'deployment_observations_not_applicable_by_parent_approval':['the_placed_descriptor_matches_the_tracked_one','every_model_production_uses_still_resolves']}
if Path('target/scratch/merge-full-linux.log').exists() and Path('target/scratch/merge-full-linux.log').stat().st_size:
    text=Path('target/scratch/merge-full-linux.log').read_text(errors='replace')
    text=re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]','',text)
    lines=text.splitlines()
    summary['summaries']=[x for x in lines if 'Summary [' in x]
    failures=sorted(set(x.strip() for x in lines if re.search(r'\b(?:FAIL|TIMEOUT|LEAK)\s+\[',x)))
    summary['unmatched_replays']=[x.strip() for x in lines if 'unmatched replays' in x]
    assert not failures, 'selected tests failed'
    assert not summary['unmatched_replays'], 'a selected test was dropped as a replayed run'
    skips=sorted(set(x.strip() for x in lines if re.match(r'\s*SKIP\b',x)))
    allowed=summary['deployment_observations_not_applicable_by_parent_approval']
    summary['unapproved_fixture_skips']=[x for x in skips if not any(name in x for name in allowed)]
    summary['passing_skip_output']=skips
(out/'outcomes.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
assert not summary['unapproved_fixture_skips'], 'selected test reported an unapproved passing prerequisite skip'
PY
report_rc=$?
set -e
if [[ $run_rc -ne 0 ]]; then exit "$run_rc"; fi
if [[ $report_rc -ne 0 ]]; then exit "$report_rc"; fi
cargo test --workspace --locked "${features[@]}" --doc --no-fail-fast --quiet
