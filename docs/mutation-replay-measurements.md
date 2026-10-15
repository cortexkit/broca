# Mutation replay timing and Windows diagnostics

## Original push run

The master push's focused catalogue replay was cancelled at its job deadline.
These timings come from run `38678258208`, commit `ad2986d`, Linux job
`116236895731`, whose logs retain the step timestamps:

| Phase | Observed wall time |
| --- | ---: |
| Job start through runner installation/cache restore | 2 min 15 s |
| `ckdev-mutate check` | 5 min 12 s |
| Focused replay, cut off by the job deadline | 82 min 55 s |
| Total job | 90 min 25 s |

The log prints the whole shell conditional, not two executed commands. A master
push runs **only** `run --all`; only a scheduled run uses `--all --broad`. Dropping
`--broad` from pushes would therefore save nothing. No completed Linux row report
was uploaded, so this run cannot supply a measured mean per completed row. Four
cancelled Linux jobs cost approximately 360 runner-minutes without a full proof.
The downloaded Windows reports contain 812 catalogue rows, 203 per shard (not
204); shard 1 contains 41 ERROR outcomes.

## Linux replay measurements

Measured with `ckdev-mutate 0.12.1` at the workflow pin, Cargo 1.99.0,
`CARGO_BUILD_JOBS=4`, `CARGO_INCREMENTAL=0`, and
`CARGO_PROFILE_DEV_DEBUG=line-tables-only` on the Linux build runner. These are
wall-clock measurements, not GitHub-hosted runner guarantees.

Before editing the workflow, an eight-row sample copied these controls unchanged
from the repository's `mutations.toml`:

- `account-changed-at-step-start`
- `actor-refused-live-steer-next-turn`
- `admission-expiry-failure-joins-attempt`
- `retention-tombstone-before-move`
- `subc-probe-egress-budget`
- `wal-full-length-corrupt-tail`
- `wal-tools-frame-digest`
- `wire-newer-writer-code`

Commands were `ckdev-mutate run --catalogue SAMPLE --all --allow-dirty --report
REPORT`, with and without `--broad`. The dirty flag allowed untracked measurement
files; the source rows were unchanged.

| Phase (seconds) | Focused sample | Initial broad sample | Fixture-backed broad sample |
| --- | ---: | ---: | ---: |
| Baseline build | 100.085 | 118.340 | 129.492 |
| Baseline tests | 248.764 | 357.363 | 696.235 |
| Mutant build | 68.917 | 93.761 | 78.247 |
| Mutant tests | 259.668 | 381.242 | 384.436 |
| Wall time | 677.467 | 950.746 | 1288.728 |
| Wall time / selected row | 84.68 | 118.84 | 161.09 |

All eight focused rows were caught. The first broad sample caught seven rows
and caught `wire-newer-writer-code` broadly. **Neither initial sample is a green
full-baseline audit**: the first ignored one vault mirror baseline failure, and
the first broad run additionally had unavailable daemon fixtures. Re-extracting
the exact run's Linux fixture artifact within the same remote job removed those
baseline failures. That broad replay caught seven rows but the eighth's baseline
failed with `step_transform_crash: libtest summary counts disagree with per-test
output`. Its mutant did not run. Treat its timing as partial, not a passing
nightly audit or an upper bound.

Broad mode builds and tests every package test target, retaining explicit
targets; command rows without a configured broad command still run only their
named tests. `select = "expected"` rows also remain focused at this pin. Compatible
Cargo baselines are **already shared within a session**; ordinary command rows
still baseline each expected test. There is no additional baseline-cache flag
to turn on. Reports and outcomes are emitted after the session, explaining the
long silent interval in the cancelled job.

After changing to 32 shards, `ckdev-mutate run --all --shard 1/32 --allow-dirty
--report REPORT`, with the run's Linux fixture artifact and all three
`BROCA_REQUIRE_*` switches enabled, selected 26 rows:

| Phase | Seconds |
| --- | ---: |
| Baseline build | 154.454 |
| Baseline tests | 631.500 |
| Mutant build | 181.213 |
| Mutant tests | 694.755 |
| Wall time | 1662.089 (27.70 min) |

There were 23 catches, two legitimate Windows-only platform skips and one
test-suite anchor-count error after adding a regression test. Updating that
suite's expected count fixed the error; a separate replay caught
`script-wal-staging-ancestry` (baseline test 0.824 seconds), completing the 24
eligible proofs without rerunning the 23 completed proofs. No baseline test
failures were ignored in this shard. The 23 completed mutants averaged 38.09 seconds
for mutant build plus tests, with another 13.10 minutes of shared baselines.

These timing runs preceded the native Bash launcher change. After that change,
`retention-tombstone-before-move`, `vault-routed-account-validated-before-lkg`
and `wal-tools-frame-digest` were each replayed with an explicit Bash path and
were caught. Parsing both catalogues and normalizing only the 121 launcher
prefixes showed all 812 row definitions otherwise equal to `ad2986d`: no mutation
edits, expected failures, count requirements, platforms or deadlines changed.

## Budget and cost

32 shards preserve every row and every event's original selection. Adding the
original job's 7.45-minute preparation/check overhead to the measured 27.70-minute
shard gives **35.15 minutes**, leaving **54.85 minutes** inside the focused
90-minute limit. This extrapolates to approximately **1125 Linux runner-minutes**
per full focused replay, excluding the independently shared fixture/preflight
jobs. It is an estimate from one partition, not a measurement of all 32 jobs.
The actual after-run on GitHub must confirm the slowest shard and total cost.

The tradeoff is more duplicated setup and baselines: at this partition's mix,
32 shards spend about 419 baseline runner-minutes and 238 preparation/check
runner-minutes. The mutant work itself is about 467 runner-minutes regardless
of shard count. Holding that baseline mix fixed would put four complete shards
near 137 minutes each, or 550 total runner-minutes; the old 90-minute limit
instead paid for incomplete work. Larger shards can share more baselines, so
this is a cost model, not a measured four-shard completion time.

The fixture-backed broad sample is slower and incomplete; linear scaling of
its observed 161.09 seconds per row to a 26-row partition plus setup is already
about 77 minutes (2465 runner-minutes across 32 shards). The nightly limit is
therefore 180 minutes rather than betting on the focused 90-minute budget.
Neither row deadlines nor failure/count requirements have been relaxed.

## Windows root causes

Across all four original Windows reports, the 153 ERROR outcomes split into:

- **116 command-baseline count errors:** captured output is UTF-16 WSL's
  `Windows Subsystem for Linux has no installed distributions`. `bash` in the
  catalogue's command argv resolved to System32's WSL launcher, despite Actions
  itself using Git Bash. No tests executed, so the strict count check correctly
  refused the baseline. Rust searches System32 before the parent's `PATH`, so a
  PATH-only fix would still choose WSL. All 121 Bash command rows now use a Python
  launcher with the original shell script/arguments unchanged; Windows supplies
  the running Git Bash's absolute executable in `BROCA_MUTATION_BASH`. The
  preflight invokes that same launcher before catalogue replay. Missing explicit
  configuration refuses on Windows instead of falling back to WSL.
- **34 Rust test-output parser errors:** every one contains the test-support
  process cleanup component's clean
  `survivors=0 errors=0` diagnostic embedded in a test status. The quiet-cleanup
  fix suppresses precisely those writes; routine skips use captured `eprintln!`.
- **Two further libtest parser errors:** `mklink /J` printed `Junction created`
  past capture in `windows_private_mode_never_follows_junctions`. Capture the
  second junction-creation command's output, as the first already does.
- **One actual baseline failure:** `script-wal-staging-ancestry` invokes
  `find-empty-completions.py --hours 1000000`. That cutoff predates 1970, which
  Windows `localtime` refuses before the first report is printed. Exit 1 then
  accidentally equals the expected suspect exit code, but stdout is empty.
  Report an unrepresentable calendar value as Unix seconds instead; a regression
  injects each supported conversion error while checking both live WALs are
  still reported. The original wide-window and staging checks remain intact.

Replaying the mutation catalogue on native Windows is still required to confirm
the Bash launcher, cleanup capture, junction-output capture and calendar-rendering
fixes eliminate the corresponding baseline and parser failures.

### Proof findings from run 38678258208

The five mutations that left their guarding tests green, the approval-marker
mutation that timed out, and the archived-read mutation caught by an unrelated
assertion are separate from the output-capture errors above:

| Row | Cause and correction |
| --- | --- |
| `registry-read-proof-cache-bound` | The read-proof cache requires an identity that detects WAL replacement. Live Windows WALs expose no persistent OS file ID and never populate the cache, but archived WALs have a content-based identity on every platform. The eviction test now reads a real archive so Windows must exercise the bound, while retaining live-WAL eviction checks on Unix; a separate Windows test checks live reads repeat flush barriers without caching proofs. |
| `wal-anchored-damaged-length-before-record` | The anchored suffix is never entered without a stable file identity. A Unix-only anchored test now requires an anchor instead of conditionally skipping its assertion; the portable full-replay corruption test remains. |
| `approval-audit-late-byte-cap` | The outer response pager hid an unbounded intermediate late-result projection. The test now checks the projection's own cap and cursor before checking the final response. |
| `move-retention-delete` | A second migration check in deletion completion could refuse after a tombstone had already been written. The test now also requires that no tombstone was created. |
| `windows-vault-open-acl-check` | The fixture contained `{}`, which always failed parsing because `handles` is required. It now contains a valid handle map, succeeds with a private ACL, and requires an inspection error for unsafe ACLs. |
| `core-approval-marker-from-call-state` | The expiry driver waited for the very marker entry the mutation removed. Repeated 120-second hang guards exhausted the suite's 600-second deadline, independently of WAL storage. The driver now advances after a question-plane poll, so the marker assertions can fail without stalling expiry. No deadlines were raised. |
| `archived-read-never-restores-live` | Restoring a live Windows WAL removes the archive's synthetic byte anchor. The test unwrapped that missing anchor before checking non-extraction. It now checks non-extraction immediately after each ordinary read, before inspecting anchors. |

The live anchored-suffix row has a cfg-gated Unix test, and the platform verifier's
regression removes its platform restriction to confirm Windows claims are rejected.
The remaining corrections retain Windows eligibility; native Windows CI must
confirm their catches and the uncached-read test.

A nine-row Linux replay with ckdev-mutate 0.12.1 caught all eight eligible rows,
including the crash-prefix control, and skipped the Windows-only vault ACL row.
The approval-marker mutant completed its full library target in 7.1 seconds with
the expected marker failure and eight other marker assertions, not a timeout.

The downloaded Windows shard-1 report and job 116236895570 log from run
38678258208 record `wire-newer-writer-code` as
`CAUGHT`, not a parser error. The `step_transform_crash` count disagreement above
was from the separate Linux breadth measurement. Its crash child inherited the
parent's stdout and emitted a nested libtest status line before being killed.
Child stdout and stderr now go to fixture-local files, preserving diagnostics
without adding an unfinished test to the parent's result stream. The wire golden
and its mutation remain unchanged.
