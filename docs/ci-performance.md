# CI and release latency

## Baseline measured before changes (2026-10-14)

Data: [ci-performance-baseline.json](ci-performance-baseline.json). GitHub run/job
timestamps, Rust gate phase summaries, and deduplicated nextest PASS records are
retained there, including failed/cancelled runs. Commands used:

```sh
gh run list -R cortexkit/broca --workflow ci.yml --limit 20 --json databaseId,headSha,headBranch,createdAt,startedAt,updatedAt,status,conclusion,displayTitle,event,url
gh run view <run-id> -R <repository> --json jobs
gh api repos/<repository>/actions/runs/<run-id>/jobs?per_page=100
gh run view <run-id> -R <repository> --log
```

The same run-list, job-detail and log queries were made for `mutations.yml` and
for both workflows on `cortexkit/broca-private`. Only **10 public runs
per workflow exist**; all were retrieved, plus 20 private runs per workflow (60
runs total). There were 58 completed-run log requests; seven private logs were
unavailable and three cancelled public mutation runs had empty logs. Two public
runs were still active in the sample. A cancelled or failed run is not evidence
of a fast complete pipeline.

### Job/step timing table

Seconds; medians of successful jobs, not median sums. The sample includes 11
wholly green private CI runs: end-to-end median **2,026 s (33:46)**, range
**1,979–2,323 s (32:59–38:43)**. There is no wholly green public CI run or mutation
workflow in this sample. Public Windows failures predate pipeline optimization;
they cannot supply a successful-execution baseline.

| Lane | n successful jobs | Queue | Runtime | Checkout | Rust + nextest setup | Dependency/cache restore | subc build | vault build | Rust gate | Script suites | Postgres compile + test |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Private Ubuntu | 14 | 3 | 2,006.5 | ~6 | ~2 | no Cargo cache | 69.5 | 96 | 1,642 | 111 | — |
| Private Windows | 14 | 3 | 1,948 | ~12 | ~26.5 | no Cargo cache | 144 | — | 1,721.5 | — | — |
| Private Postgres | 18 | 3 | 116.5 | 2 | 1 | no Cargo cache | — | — | — | — | 83 |
| Public Ubuntu | 2 | 4 | 1,337 | ~6 | ~1 | no Cargo cache | 37.5 | 44.5 | 791.5 | 113 | — |
| Public Postgres | 9 | 3 | 79 | 1 | 1 | no Cargo cache | — | — | — | — | 50 |
| Public mutation Ubuntu shard 1 | 3 | 4 | 744 | ~5 | ~1 | see below | 30 | 40 | validate 391; replay 0 | — | — |
| Public mutation Ubuntu shard 2 | 5 | 3 | 714 | ~6 | ~1 | see below | 38 | 42 | validate 384; replay 12 | — | — |
| Public mutation Ubuntu shard 3 | 6 | 3 | 739.5 | ~6 | ~1 | see below | 35 | 45.5 | validate 419; replay 15 | — | — |
| Public mutation Ubuntu shard 4 | 4 | 3 | 691 | ~6 | ~1 | see below | 38.5 | 51.5 | validate 476.5; replay 4 | — | — |

These short mutation jobs are mostly diff selections, **not full catalogue
replays**. The latest public mutation run, [38492705095](https://github.com/cortexkit/broca/actions/runs/38492705095),
lasted **3,154 s (52:34)** and failed: Ubuntu replays reported 32–36 caught rows
per shard; Windows did not finish its validation successfully. Private mutation
workflow runtime ranged from effectively immediate refusal to **11,886 s
(198:06)**; all 20 concluded `failure`. Faster scheduling alone cannot turn those baseline
failures into verified coverage.

Other measured steps: Ubuntu disk cleanup costs 57 s private / 127 s public;
public Linux release-build/deploy costs 200 s; Postgres service initialization
26 s private / 21 s public; ordinary post-checkout steps on Ubuntu round to 0 s,
Windows checkout/Node post-steps total roughly 10 s. Mutation cache post-steps
cost roughly 10–15 s public, but roughly 66–100 s in the private successful
shards. Per-step timestamps in the data file allow inspection without assuming
all runs used the same runner/toolchain or workflow revision.

### Compile versus execution

The Rust gate already reports phases. Completed nextest summaries give execution
wall time independently of hidden compile/discovery work:

| Rust phase | Private Ubuntu | Private Windows | Public Ubuntu |
|---|---:|---:|---:|
| fmt | 4 s (16 logs) | 5 s (16) | 3 s (2) |
| clippy, including compile | 123 s (16) | 203 s (16) | 55.5 s (2) |
| lib/bin compile + execution | 290.5 s (16) | 599 s (14) | 140.5 s (2) |
| summed lib/bin harness execution | 84.095 s | 163.305 s | 50.43 s |
| integration compile/discovery + execution | 1,198 s (14) | 875 s (14) | 584 s (2) |
| nextest execution wall time | 1,011.099 s (14) | 512.274 s (14) | 506.559 s (2) |
| doctests | 3 s (14) | 7 s (14) | 3.5 s (2) |

The differences (~187 s Ubuntu / ~363 s Windows integration
compile/discovery) are **estimates from separate medians**, not pure rustc
measurements. Unit execution is a sum of per-binary harness times, not wall time
for nextest. Do not conflate either with compilation or subtract nextest test
duration sums from job wall time: concurrent tests overlap.

### Dominant tests and per-binary totals

Representative successful `cortexkit/broca-private` run
[38347218538](https://github.com/cortexkit/broca-private/actions/runs/38347218538):

| Platform / binary | Test count | Sum of test durations |
|---|---:|---:|
| Ubuntu `broca-module-serve::it` | 289 | 1,107.106 s |
| Ubuntu `broca-module-serve::llm_runner_conformance` | 25 | 473.559 s |
| Ubuntu `broca-provider::it` | 326 | 208.135 s |
| Ubuntu `broca-module-serve::scope_real_daemon_e2e` | 10 | 171.585 s |
| Windows `broca-module-serve::it` | 269 | 399.116 s |
| Windows `broca-provider::it` | 326 | 242.941 s |
| Windows `broca-module::it` | 124 | 107.084 s |
| Windows `broca-subc::it` | 46 | 61.698 s |

Slowest: Linux `ck_broca_passes_the_llm_runner_conformance_suite` **215.283 s**;
Windows `stream_errors_e2e::mid_stream_disconnect_is_error` **65.375 s**,
`truncated_stream_mid_text_finalizes_on_eof` **63.854 s**, and
`truncated_stream_mid_tool_call` **62.658 s**. Public Ubuntu's corresponding
conformance test is **119.030 s**; the three stream errors cost **55–69 s**.
Their durations are intentional timeout/fault proofs, not a reason to remove
them. Public Ubuntu run 38492705110 executed **1,442 integration tests**;
private green runs executed **1,379–1,388 Ubuntu / 1,323–1,327 Windows**.
Lib/bin PASS counts in the representative private run are **2,414 Ubuntu / 2,356
Windows** (public Ubuntu **2,462**). Counts differ with source/platform, not
because a workflow can discard tests.

### Cache hit rate

CI has no Cargo dependency/target cache. In the nonempty mutation logs retrieved
from the 10-public/20-private run samples above (6 public, 17 private):

| Repository / platform | Cargo target restores | Mutation-runner restores |
|---|---:|---:|
| Public Linux | 16/24 (66.7%) | 16/24 (66.7%) |
| Public Windows | 0/24 (0%) | 0/24 (0%) |
| Private Linux | 68/68 (100%) | 68/68 (100%) |

"Restored" includes prefix restores, not just exact-key hits: the latest public
Linux job says `Cache hit for` the new SHA but `Cache restored from key` names an
older SHA. The restored target does not contain the sibling repositories'
targets. Fixtures still build in every shard, and a successfully restored
target does not imply a Cargo freshness hit. A cache action's green step alone
is not a hit-rate measurement.

### Baseline critical path

Private green runs are limited by both Linux and Windows serial Rust gates,
not queues (~3 s) or Postgres (~2 min). Public Ubuntu's successful jobs take
**22:17 median**, including a ~9 min integration execution phase and serial
release/deploy + scripts. The observed full mutation replay is a separate
**52:34 red** critical path. No under-15-minute full-green claim is justified by
these measurements.

## Runner probe

`ck-motor` is not on the operator's PATH. The installed CLI exposes
`ck motor run --vcpus N --timeout SECONDS --cwd PATH -- 'shell command'`.
A real script-callable probe succeeded with rustc **1.99.0** and Cargo **1.99.0**,
`CARGO_TARGET_DIR=/run/motor/target` and `CARGO_HOME=/motor-home/cargo`.
The CLI returns the command's exit status; 200–204 denote refusal, unknown
outcome, timeout, cancellation, or invalid arguments. A preflight must preserve
those failures, never run a local fallback. This probe establishes reachability,
not a measured warm-build hit rate.

## CI layout selected from the measurements

Integration execution dominates the serial Linux job, so CI now builds a
nextest archive per platform and runs four hash partitions per platform. The
original live-binary exclusions, live/test-clock features, required fixture
guards, nextest concurrency groups, leak/slow timeouts, and libtest-versus-
nextest split remain intact. The independent libtest/nextest inventory check
runs before archiving. Each shard executes the clock sentinel additionally:
four shards cover the original integration inventory plus four clock proofs.
Library/binary tests stay in libtest and doctests retain independent discovery.

Lint (native Linux and Windows), scripts, Postgres, archive compilation, and
fixture compilation start independently. Real Linux release/deployment runs in
its own lane. Required `Test (ubuntu)` / `Test (windows)` status names now
aggregate **all** these lanes and refuse failure, cancellation, and skipping.
Neither a successful archive nor a successful unit job alone is a green train.
The eight mutation shards retain their diff/all/broad selection and native
Windows coverage verifier; none waits behind a Rust-test, lint or script job.
They depend only on the shared fixture producers.

Both workflows build siblings once per platform/run (not once per shard) and
share an exact-revision binary cache. Push/PR rigs are pinned; the daily lane
still resolves floating sibling tips and caches their exact resolved revisions.
Tar transport preserves Unix execute bits and includes the vault enrollment
fixture. Cargo dependency/target caching is separate by lane/platform; mutation
cache saves survive failed replays, including the successfully installed
mutation runner. Summaries expose exact-hit telemetry. Post-change hit rates
and CI wall times must come from real train runs, not from these configuration
choices.

A real Linux archive proof found **1,449 integrations in both independent
inventories**, archived 90 binaries / 129 files in **16.98 s**, and extracted in
**29.58 s**; its feature-clock proof passed in **0.003 s**. These are OVH numbers,
not GitHub-hosted timings. Archive creation includes auxiliary/non-test binaries;
only the original integration filter executes them. Hosted jobs must use the
same absolute checkout/target layout: some tests embed `CARGO_BIN_EXE` at compile
time, so archive consumers extract into that checkout, not a random temporary
directory. Full cross-runner replay still requires the train proof.

No larger-runner change is made. The measured ~3 s queue does not justify it;
more cores cannot remove the intentional 55–215 s individual fault/conformance
proofs. No measured larger-runner sample exists, so a quantified speedup from a
larger runner would be invented. The full mutation catalogue still pays serial
recompilation per row; its 52:34 failed baseline cannot support a promise of a
15-minute full-green release. Train measurements below determine the remaining
critical path.

## Remote train preflight

Before any train/probe commit push, `train-push.sh` invokes the installed
`ck motor run` through `scripts/train-preflight.sh`. Failure prints the runner
output and refuses the push; no Mac fallback exists. A successful preflight is
reused only for unchanged, clean HEAD within that process. A moved-master
rebase earns another preflight. The first-use CI trigger probe is a synthetic
commit with the same tree as HEAD and requires the same preflight first.

The runner CLI is reachable (rustc/Cargo 1.99.0), but its sibling snapshot
transport rejects task-local nested checkouts: siblings must sit beside the
main checkout, even when invoked from a linked worktree. A real attempt returned
`200 snapshot_failed` before running anything. Those jobs also cannot resolve
GitHub to fetch fixture sources. Rather than touching operator checkouts or
building on the Mac, the preflight
was explicitly narrowed to **cheap, fixture-independent checks**.

It requests 8 vCPUs and runs fmt, Linux and Windows-GNU-target workspace/all-
target clippy, lib/bin tests with **`--no-fail-fast`**, and portable integrations.
`broca-provider/live`, `broca-catalog/live`, and
`broca-module-serve/test-clock` remain enabled, so lint checks the transport and
retention-clock code as well as the default code. Five fixture-dependent unit
tests are explicitly excluded:

- `executor::tests::session_close_releases_hook_and_compaction_routes` checks
  route cleanup against a real subconscious daemon.
- `vault::tests::list_scoped_strict_mirror_matches_claustrum_tip` compares the
  enrollment wire contract with the vault checkout's fixture.
- The three `broca-store-postgres` tests for batched projection, export-writer
  locking and store conformance require a live database; they run in the
  Docker-backed Postgres CI lane.

The remaining unit inventory is printed,
then actual libtest execution counts (including exclusions and ignores) are
reported. Integration discovery prints baseline/selected/excluded/ignored counts
per binary and every excluded test name. Zero discovery, zero execution,
unexpected passing fixture skips, missing clock PASS proof and count mismatch
all refuse. Fixture requirement flags remain armed to catch selector drift.

`scripts/portable-test-selection.conf` names the integration families kept out
of this cheap preflight: `broca-subc` session/resume/tool-fault daemon proofs,
serve conformance and
scope-daemon targets, the real-module/daemon/vault families, real-daemon plan
admission proofs, and aimock provider families. **All still run in CI and the
Mac pre-merge gate**, with the original fixture requirements. Doctests, scripts,
Postgres and native Windows execution remain separate full CI lanes. The Mac
gate continues exercising native Mac daemon/vault/mock variants; Linux CI
covers their Linux variants, not native signing, Gatekeeper or Mac facilities.

Eight environment-dependent integration probes are also excluded from the cheap
preflight: four opt-in paid/live-provider probes, credential-store consent,
production-catalog resolution, comparison of the deployed and tracked tool
descriptors, and the real Magic Context client. CI retains their existing invocation/skip behavior; running
their assertions requires the corresponding operator state, binary or explicit
live-test opt-in. They are not portable proofs merely because they return PASS
without that environment.

Motor supplies persistent `/run/motor/target` and `/motor-home/cargo` paths.
Successive real archive-build jobs reused compilation (`Finished test ...
0.07 s`, then **0.06 s**), while archive compression/extraction still incurred
work. This is observed warm-cache reuse, not a claim that every revision or
Windows target is already warm. No sibling checkout or npm installation is
required by the narrowed preflight.
