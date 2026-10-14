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

Lint (native Linux and Windows), scripts, archive compilation, and
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
retention-clock code as well as the default code. Before the newer-master merge,
five fixture-dependent unit tests were explicitly excluded:

- `executor::tests::session_close_releases_hook_and_compaction_routes` checks
  route cleanup against a real subconscious daemon.
- `vault::tests::list_scoped_strict_mirror_matches_claustrum_tip` compares the
  enrollment wire contract with the vault checkout's fixture.
- The three `broca-store-postgres` tests for batched projection, export-writer
  locking and store conformance require a live database; they run in the
  historical Docker-backed Postgres CI lane. Newer master deleted that backend,
  so the current selector retains only the first two unit exclusions.

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
Mac pre-merge gate**, with the original fixture requirements. Doctests stay in
the lib/bin jobs; scripts have an independent job. Native Windows execution
remains a separate full CI lane. The Mac
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

The real script-callable preflight completed in **144 s (2:24)** on the 8-vCPU
Linux runner: **2,526 lib/bin tests** executed (2,535 listed, five explicit
service-dependent exclusions, four ignored), and **1,191 integrations** passed
in **94.272 s** (1,449 baseline, 254 explicit exclusions, four ignored). Its
captured output had no passing fixture skips and included a PASS record for
`test_clock::test_clock_feature_is_enabled_for_retention_tests`, which actually
exercises the enabled retention-test clock.
Both Linux and Windows-GNU clippy passed; warm compiler checks took fractions
of a second. These are measured pre-push timings, not estimates of GitHub CI.

## Release procedure

1. Bump the workspace version **and Cargo.lock in the change batch**, before
   training. Make code, dependency pins, version, and lock one reviewed batch.
2. Run the Mac pre-merge gate once for native Mac daemon/vault/mock coverage.
   Run `scripts/train-push.sh <name>` for the complete batch; it first runs the cheap Linux preflight,
   then waits for CI and fast-forwards only the tested SHA to `master`.
3. On a clean checkout at the exact `origin/master` commit, run
   `scripts/release.sh <version> [--marker TEXT] [--control TEXT]`.
   It fetches master, checks the workspace version, and reuses `ci-status.sh`
   to require successful CI **and Mutation catalogue** runs for the full SHA.
   Missing workflows, another SHA, incomplete/failed/cancelled runs, query
   failures, or a potentially truncated run list refuse before a tag is made.
4. The script creates (or reuses) the exact local `v<version>` tag and calls
   `stage-release.sh`, retaining the existing locked-build/provenance/signing/
   artifact checks. It never bumps version, runs a second gate, trains, dispatches
   or reruns CI, or calls placement. A failed staging attempt retains the tag
   on the same SHA so staging can be retried without moving the release.
5. After successful staging, publish the tag through the operator's approved
   Git path if needed. **Placement/restart is still a separate approved
   operation** using `scripts/place-release.sh` and the staged artifact card.

The workflows still observe pushes to master as well as trains. The existing
`ci-status.sh` policy requires *all* observed runs for the exact SHA to be
complete/successful, including the automatic master observation. The release
script does not initiate another run or weaken that policy to hide a failure.
Scheduled compatibility failures and optional workstation/live probes are
distinct from a proof of native Mac behavior; the Mac gate remains necessary.

## Fleet lessons

Measure execution separately from build/discovery; summed concurrent test
durations are not wall time. Cache-restored is not necessarily exact-hit or
Cargo-fresh. Save useful dependency/tool caches even when later tests fail.
Compile integration artifacts once, keep lib/bin tests on their existing native
Rust test harness (including process-global state/concurrency behavior),
and count independent discovery before partitioning. Carry executable bits and
auxiliary binaries when transporting archives. Cheap remote checks should be
fixture-independent and publish their exclusions, not manufacture passing skips.
Version before CI and tag the verified commit; keep staging separate from the
approval boundary for restart. No larger runner or quantified larger-runner
speedup is asserted without a controlled measurement.

## Probe reconciliation with newer master

The first real probe tests **8cce024bdcece162948862764b8d7733885a6124** on
`train/ci-perf-probe`, merging the implementation tip with newer master. Its
upstream changes include Windows retention fixes, the test-clock configuration
fix, a Windows `setsid` fallback, and **deletion of the PostgreSQL backend**.
That probe merge removed the obsolete Postgres CI job and aggregate dependency
because its upstream master no longer contains the backend.
The historical measurements above retain that lane; its removal is an upstream
product change, not a performance tradeoff. The portable selector and workflow
self-tests now follow that updated package inventory.

Probe run [38563395563](https://github.com/cortexkit/broca/actions/runs/38563395563)
exposed a prerequisite hidden by the old serial job: the independent script
lane's offline dependency-graph check lacked Cargo's pinned git sources. It
failed after **6 s**, naming `test_dependency_graph_stays_transport_free` and
the unavailable offline tungstenite checkout. The script lane now installs the
same Rust toolchain and prefetches **`cargo fetch --locked`**, with its own
dependency cache, before the unchanged offline checks. It still starts without
waiting behind compilation or tests. No check was relaxed to make that probe
pass.

The failed probe finished in **772 s (12:52)**, versus the private green
baseline median **2,026 s (33:46)**. This is **not a green speedup claim**:
Windows consumers stopped before executing tests. GNU tar interpreted the
native `D:` archive path as a remote host (`Cannot connect to D`). All fixture
consumers now use a shared unpack helper that converts Windows paths with
`cygpath`; Linux extraction and executable permissions are unchanged. Tests
extract a real tiny archive through both paths and reject conversion failure.

Completed first-probe stages: Linux/Windows archives **223 / 408 s**, native
lint **88 / 187 s**, Linux/Windows fixture producers **96 / 224 s**, Linux
lib/bin/doc **200 s**, and real Linux deployment **267 s**. The four Linux
integration shards passed **354 + 379 + 372 + 340 = 1,445 tests**, plus four
executed clock proofs; independent archive inventories agreed at **1,449**
(four ignored). Windows archive inventories agreed at **1,390**, but execution
coverage was **zero** because unpacking failed. Archive/unit caches were cold
and both fixture lookups missed. The first critical path was the Windows
archive (**6:48**) followed by Linux shard 2 (**5:50**, including **297.768 s** of
test execution). A corrected train must finish successfully before its timings
can be reported as a green CI speedup or its warm-cache hit rate measured.

### Corrected extraction probe and earlier shard preparation

The second probe, **700e118809b076d4556172ed3cc71d432ec574cb**, ran as
[CI 38571258967](https://github.com/cortexkit/broca/actions/runs/38571258967)
and [Mutation catalogue 38571259043](https://github.com/cortexkit/broca/actions/runs/38571259043),
the separate workflow that validates and replays the catalogue's fault controls.
Its job/step timestamps, execution summaries and failure names are retained in
[ci-performance-probes.json](ci-performance-probes.json).

| Second-probe lane | Runtime | Execution / outcome |
|---|---:|---|
| Linux / Windows archive | 209 / 419 s | Both independent inventories agree |
| Linux / Windows fixtures | 20 / 47 s | Exact binary-cache hits |
| Linux / Windows lint | 51 / 155 s | Passed |
| Operator scripts | 172 s | Passed, including offline dependency checks |
| Linux / Windows lib/bin/doc | 182 / 500 s | 2,531 / 2,484 passed |
| Linux shard 1 / 2 / 3 / 4 | 253 / 485 / 221 / 279 s | 354 / 379 / 372 / 340 passed, plus four clock proofs |
| Windows shard 1 / 2 / 3 / 4 | 161 / 139 / 198 / 172 s | 346 / 358 / 359 / 323 passed; the ACL test named below failed |
| Real Linux deployment | 117 s | Passed |
| Linux mutation shards | 549–605 s | All four passed |
| Windows mutation shards | 797–1,000 s | All four refused invalid platform metadata |

CI took **919 s (15:19), red**, from run creation to the last job completion.
The critical path was the Windows archive, the cross-platform matrix barrier,
then Linux shard 2: it spent **299.351 s** executing tests but **485 s** in the
job, including setup/extraction. All **1,445 Linux / 1,387 Windows** integrations
executed; Windows had **1,386 passed / one failed**, in addition to the clock
proofs. `broca-store::windows_private_mode::windows_private_mode_refuses_unreadable_acl`
was that failure. The merge through `c39240c` included an earlier fixture
correction, but the same test failed again on the early-start probe. The final
upstream fix arrived later: `c78a150e` replaced the unreliable ACL fixture with
an injected-denial test, and `d9262b15` corrected its path comparison. The
reconciled probe must include those changes. Neither failed probe is a green
after-measurement.

Before replay, catalogue validation checks that each mutation's expected failing
test exists in the compiled inventory. It refused `serve-boot-terminal-withdrawal-debt` because its
`approval_gate::process_kill_seals_interrupted_and_boot_withdraws_terminal_debt`
test is absent on Windows. The entire `approval_gate` module is `cfg(unix)`, so both catalogue rows
using that module now declare `platforms = ["macos", "linux"]`. Their Unix tests
and mutation edits remain unchanged. This corrects nonexistent Windows coverage;
it does not remove an executable Windows test. A catalogue regression test
checks both rows and their exclusion from Windows eligibility.

Probe **bafebee5f1698001742806fb3bc1fb3e8e3ec504** starts all eight integration
consumer jobs immediately. Checkout, Rust/nextest setup and npm installation
overlap archive/fixture production. Consumers then poll for **only their own
platform's artifacts**, with a bounded wait and named producer-failure refusal,
before using the unchanged download/unpack/replay steps. They no longer wait
for other-platform matrix legs or producer post-cache saves. The required status
explicitly still depends on archive and fixture producers, so consuming an
uploaded artifact cannot hide a later producer failure. Nextest prints failure
output immediately and at the end, while `fail-fast = false` still executes the
remaining tests.

### Remaining limits

Four hash shards preserve deterministic selection but do not balance duration:
the second probe's Linux shard 2 took **299.351 s** of execution versus shard
1's **73.422 s**. Sharding reduced serial execution without removing intentional
timeout tests, but the slowest shard, archive compilation/transport and libtest
units still determine wall time. Full mutation catalogue replay is a separate
workload from a push's diff selection; neither a green diff replay nor faster
fixture setup establishes a sub-15-minute full catalogue replay.

Starting consumers early uses more idle hosted runners rather than repeating
compilation. Public runner minutes are free, and the measurements show queueing
was not the baseline bottleneck. Cache misses and toolchain/revision churn still
require cold sibling builds. Cache hit rates must name the lane, exact versus
prefix restore, and measured run; a successful cache step is not a hit.

The release helper removes operator-triggered duplicate gates/trains, not
GitHub's existing automatic master-push observations. The current all-runs-for-
SHA status policy still waits for those observations if they exist. Changing
that policy would be a separate correctness decision, not an unmeasured speed
claim. Mac-native signing, Gatekeeper and daemon/vault behavior remain the Mac
pre-merge gate's responsibility. `place-release.sh` installs the staged binary
and restarts the daemon, so it remains separate and requires operator approval.
