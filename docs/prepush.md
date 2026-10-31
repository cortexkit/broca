# Pre-push checks and CI parity

Run from the Mac checkout, before pushing a committed candidate:

```sh
bash scripts/prepush.sh
```

The dispatcher uses `ck motor run` (the same build service as AFT's `runon`). It
never builds on the Mac as a fallback. First it runs the fast Linux static guards.
Only when those pass does it admit the remaining jobs through a **32-vCPU
concurrent budget**, including Windows and macOS. The dispatcher prints that
budget and the queued plan's total (76 vCPUs) before dispatch. Fast uses two
alone. The initial wave is full CI unit/doc (4), production (4), Windows (8),
integration shard 4 (8) and shard 2 (8): exactly 32. The remaining integration
jobs, lint, minimum supported Rust version (MSRV) checking and deployment get
eight CPUs; macOS and `linux-small` use two. Jobs use released slots as soon as they fit, rather than holding
reservations in Motor's admission queue or waiting for a whole-wave barrier.
An UNKNOWN job keeps its reservation because it may still be running. If those
reservations prevent any remaining job from fitting, the remaining jobs are
reported NOT-RUN rather than exceeding the budget or waiting indefinitely.
The small lane runs only the unchanged operator suite and WAL checks. An operator
failure is recorded but does not suppress WAL. Full-workspace MSRV checking and
the deployment release build are independent eight-CPU jobs. Fixture compilation
stays with its consumers: unit, production, each integration job, Windows and
deployment. It no longer occupies the small job.

The preceding full unit job took **981.169s at four CPUs**, not eight: about
168s of setup, then an 813s lib/bin phase. Its reported libtest summaries total
436.68s; about 376s is compilation/launch residual that the old quiet Cargo log
does not attribute further. The largest binaries were:

| Unit binary | Reported execution |
| --- | ---: |
| `broca-crash-harness` library | 115.12s |
| `broca-module` library | 118.97s |
| `broca-store` library | 120.52s |
| Remaining binary summaries | 82.07s |

Unit now starts in the first wave. Its compilation/discovery is a separate
inventory step; Cargo's executable labels and successful test output remain
visible, and the audit reports each binary's duration/counts. No crate-selection
flags alter the full workspace feature graph. Nextest execution still has four
slots.

Each summary writes `critical-path.json`: end-to-end time, the last runner's
finish relative to gate start (including admission/queue delay), the longest
runner, and the known reservation-time floor `sum(vCPUs * run_time) / 32`.
Missing terminal records are named as incomplete, never invented. The floor is
for observed work, including work before a failed job stopped; it is not a
forecast of a green gate or a quieter pool. The preceding gate ended at 1735s;
its booked work alone gives a 1225.293s floor at 32 CPUs, before accounting for
serial preparation. Splitting unit while duplicating its full-workspace build
does not remove that floor. Deduplication removes portable's
duplicate executions, retaining its lint in `linux-lint` and its tests in CI
lanes as mapped below. A list-schedule model using the measured durations,
166s for lint (73s + 88s clippies plus startup), and 82s quiet-pool preparation
projects **1203.097s (20m03)**. Windows's 545.618s measurement stopped at npm;
its remaining suites are unknown, so this is a known-prefix estimate, not a
full-green under-20-minute claim. A capacity refusal is
still NOT-RUN with its exact reason, never retried or counted as coverage.
Each launch documents its size rather than reserving 12/16 CPUs to influence
placement. Each remote lane has a persistent detached Git worktree at
`~/Work/Projects/CortexKit-scratch/broca-prepush-lanes/<checkout-id>/LANE`.
The checkout ID is the invoking checkout's basename plus the first 12 hex digits
of its absolute path's SHA-256, keeping separate callers' caches isolated.
Set `BROCA_PREPUSH_LANE_POOL` to override that checkout-specific pool root.
The default never derives a side-checkout location from the invoking checkout's
parent: core owns its worker-worktree pool, so persistent lanes belong in the
scratch project area. A lane is checked for dirtiness **before** checkout and must
match the exact candidate SHA. Unrelated repositories or dirty lanes fail closed;
the gate never resets or cleans them.

MOTOR guarantees that each checkout (including a Git worktree) is its own host
workspace, with its own target folder. It serializes jobs within a workspace;
different workspaces never share a writable target. The gate therefore uses
MOTOR's supplied `CARGO_TARGET_DIR` unchanged, rather than adding colder target
subfolders. Keep the lane pool across runs so host-side caches remain warm.

Each Linux integration job builds/checks its own archive and replays its one
unchanged CI partition. Windows builds one archive and replays all four
partitions. Tests, CI profile, clock sentinel and partition boundaries are
unchanged. The clock sentinel is an executed retention test proving that the
controllable test-clock feature is enabled, not merely listed in the archive.
A failing shard does not stop the other shards or unit job. These
stable checkout/cache locations amortize compilation on subsequent pushes;
cold compilation and build-server admission can still dominate wall time.

Prepush caps nextest execution at four concurrent slots. nextest 0.9.138 has a
reproduced mixed-group scheduling bug at eight slots: a weight-four aimock test
never starts, yet every printed result is PASS and the run exits 100 with an
unrun-test warning. The disposable positive-test fixture and commands for this
mixed-group scheduling bug are kept under `.cortexkit/internal-docs/investigations/`.
No broca test, partition, profile, group cap, timeout or retry policy changes.
An unrun-test warning still fails the lane. Remove the concurrency cap only
after a fixed nextest passes the synthetic regression at eight slots.

## Portable deduplication coverage

Normal dispatch uses `linux-lint` for fmt, native Linux clippy and Windows-GNU
target clippy. It reuses the persistent `linux-portable` lane checkout so the
measured warm clippy cache is not discarded merely by renaming the job.
The full standalone `--lane linux-portable` command and
`linux-preflight.sh` are unchanged and remain available for fixture-free
diagnostics. CI's full unit/doc and four integration partitions still execute.

The compiled inventory maps every formerly executed portable test, with the
same workspace feature combination (`broca-provider/live`,
`broca-provider/test-harness`, `broca-catalog/live`,
`broca-module-serve/test-clock`):

| Former portable selection | Destination | Named tests |
| --- | --- | ---: |
| Nonignored lib/bin tests minus the two explicit fixture exclusions | `linux-unit` | 4816 |
| Integration tests in hash partition 1/4 | `linux-integration-1` | 462 |
| Integration tests in hash partition 2/4 | `linux-integration-2` | 484 |
| Integration tests in hash partition 3/4 | `linux-integration-3` | 483 |
| Integration tests in hash partition 4/4 | `linux-integration-4` | 428 |

No test or feature combination is portable-only. Per-test names and destinations
are in `prepush-portable-coverage-LANE.json` under each lane's
`.cortexkit/internal-docs/investigations/`, returned by Motor to its lane checkout.
The unit inventory still requires each explicit exclusion to exist exactly once.
Runtime audits reject missing/filtered unit binaries, partition count/identity
mismatches, missing clock execution and passing fixture skips from the former
portable selection. Existing optional/live skips outside that selection keep
their CI behavior; they are not mistaken for portable coverage.

Each check prints its complete output, exit status and wall time. The final
summary prints the first error lines and replays complete failed-lane output,
including failing test names, rather
than trying to extract names with a lossy log filter. Dispatcher logs are under
`target/scratch/prepush.*`; copy evidence outside `target/` if it must survive
build cleanup. A motor refusal before start is **NOT-RUN** by lane name, not a
test failure or pass; connection-loss outcomes are **UNKNOWN**. Either fails the
gate. Setup failures explicitly mark dependent suites **NOT RUN** and return
failure, never a passing skip. No retries or timeout increases mask tests.

A non-admin Windows token may be unable to assign a foreign file owner. The
owner-preservation integration makes that actual ownership probe; on refusal
it runs every remaining assertion and prints
`owner-distinct check not run: token cannot assign a foreign owner`. The Windows
lane and dispatcher summaries label that result **PARTIAL**, not a full PASS.
GitHub's admin Windows CI still constructs the Administrators-owned fixture
and proves the strong distinct-owner preservation case. Fixture writes change
only the discretionary access-control list (DACL), never the system audit
access-control list (SACL); cleanup can replace a DACL without reading it.

Daemon/test temporary files are separate from each lane worktree's logs and
artifacts. On Unix, `$HOME/.broca-prepush-runtime` and its per-job leaves are
explicitly 0700. Windows instead creates a fresh native-temp directory through
`broca-owner-only`'s protected owner/SYSTEM ACL creator, then validates its
inheritable ACL. It never applies Unix modes or adopts the old shared HOME path.
Windows explicitly exports Motor's `CARGO_TARGET_DIR` (normally `C:/t`) inside
the command. Archive creation uses that target directory. Replay takes its
target layout from the archive, with no incompatible build-selection flags,
and extracts under the private `TMPDIR` rather than through the checkout's
`target/` junction. GitHub CI retains its existing checkout extraction default.
Test processes use CI's **umask 022**, not 077: owner-only tests must distinguish
an explicit private-file mode from the ambient default.
`TMPDIR`, `TMP` and `TEMP` point at the private directory,
so `broca_test_support::scratch_root()` and daemon connection files do not inherit
`target/scratch`'s potentially group-writable ancestors, including when the
entire managed checkout is nested below `target/scratch`. Step logs remain outside
the test temporary root, include the command/exit code even for silent errors,
and are replayed before the remote job disappears. Run from a checkout on a
healthy internal disk so the caller's retained logs are not lost to volume stalls.
An integrating agent must use its own clean scratch checkout, not another
agent's leased worktree, which Motor rejects with an ownership-authorization
error named `lease_mismatch`. Each dispatched job prints
`MOTOR_RUNNER_INSTANCE`; the Windows lane repeats that host name in its final
summary so a host-specific failure remains identifiable. Its motor event record is retained beside the lane log
as `LANE.motor.jsonl`. A refusal before job start has no user-process instance
banner; retain its exact refusal and event record instead of guessing a host.
`LANE.timing.json` and the final table separately record checkout preparation,
client request-to-start delay and actual runner execution. The runner's reported
queue metric is retained but is not treated as the complete pre-start wait.

The Linux ELF placement regression waits for a byte written by the fixture from
`main` before capturing `/proc/PID/exe`. On the runner, an immediate post-Popen
read sometimes still named the forked Python interpreter (inode 1528), not the
ELF fixture. The runner's scratch was already on real XFS disk, not overlay or
tmpfs. Readiness fixes that startup race; the running-image/rollback inode and
byte assertions are unchanged, and no filesystem workaround hides them.

## Prerequisites and the fast boundary

Use a clean committed checkout (also required by `linux-preflight.sh`). The build
machines need stable Rust with rustfmt/clippy, cargo-nextest, Git, Python 3.11+
with PyYAML, Node **20**, npm, and GNU tar. The MOTOR Linux image must provide
**`file(1)` and a C compiler (`cc`)** for the operator ELF fixtures and deployment
checks. The fast lane refuses immediately, before running tests, if those image
prerequisites are missing; it does not install OS packages or skip their tests.
Linux additionally needs the
`x86_64-pc-windows-gnu` Rust target/linker and toolchain `1.96.0` for the MSRV check.
Motor supplies `CARGO_TARGET_DIR` with the per-workspace isolation above.
Only Linux jobs request outbound network. No GitHub Actions
token or artifact credentials are needed.
If Linux's Python lacks PyYAML, the fast lane provisions checksum-pinned pure
Python `PyYAML==6.0.3` under its scratch directory (no system pip needed).
The Linux unit, integration and production jobs provision checksum-pinned
Node 20.20.2 when Node 20 is absent. These
are setup-action equivalents, not substitutes for checks. Cold tool downloads
need network on the caller/Linux runner; no host installation is changed.

### Offline Windows inputs

Windows motor jobs do **not** support `--net`. The dispatcher runs
`scripts/prepare-prepush-windows.py` on the caller to download and verify every
npm tarball in `package-lock.json` and prepare the pinned fixture sources. All
bytes are staged after the clean-tree check in the Windows worktree's
**untracked, non-ignored `prepush-inputs/`**. Git snapshot visibility is checked
for every file. The dispatcher refuses any tracked `prepush-inputs` path, and a
script regression also checks the index so generated inputs cannot be committed.
The dispatcher passes no `ck motor run --sibling` source-checkout arguments:
older runners require those repositories beside the main checkout, not beside a
linked worktree. No runtime archives are committed.
The exact input folder is removed after a known finished/refused run; an unknown
outcome retains it and makes the next clean-tree check fail closed.
An existing input path, including an ignored or empty directory, is never adopted
or deleted. Input preparation requires a fresh folder, rejects an ignored folder
before downloading, and rechecks every completed file against Git's index and
excludes before dispatch.

Motor deliberately excludes ignored files and top-level `target/`; there is no
include option. Source/download caches may live under target, but shipped bytes
must not. `transport-manifest.json` records each shipped name, size and SHA-256.
The first remote action verifies every entry with native PowerShell and exits
NOT-RUN (200), naming the first missing/mismatched file, before tests/bootstrap.
Malformed or incomplete inventories and unlisted input files also refuse the lane.

Windows requires the image's Python **3.14** (`python`), nats-server **2.15.0**
and Bun **1.4.x** (stable patch releases). Bun is checked only as an image
prerequisite; fixture builds use Cargo and aimock setup uses npm, with no
patch-specific Bun behavior. After manifest verification, the Windows pre-push
check probes Node **24** with npm,
CMake **4.4.5** and Ninja **1.13.2**, preferring matching image tools. Missing or
unsupported tools use the checked offline ZIPs staged by the caller: Node
**24.11.1** (including npm), CMake **4.4.5**, Ninja **1.13.2**. Node's image probe
accepts the 24.x family; CMake and Ninja require the exact pins. Extracted tools
are version-checked again and their paths are passed back to Git Bash before
Cargo or any tests run. A missing/bad input or unusable fallback is NOT-RUN (exit 200).
There are no runtime downloads on Windows. Linux retains the CI's
Node20/pinned-NATS setup. These approved image versions are an explicit Windows
environment difference from GitHub's setup actions, not a claimed byte match.
The runtime-download helper remains Linux-only. Windows fallback extraction
happens only after every input byte has passed manifest verification.

The npm archive-name file is written as binary ASCII with LF line endings, not
through Windows text stdout. A retained CR after `.tgz` makes npm classify the
otherwise valid pinned aimock tarball as a directory, producing the misleading
`DirFetcher requires either a tree or an Arborist constructor to pack` error.
The lockfile has no file/directory dependency to repack. Both the Windows branch
and an empty-cache offline install are exercised by `test-prepush-npm.py`.

Subc stays a Git checkout because its selected `subc-core` and `subc-daemon`
build scripts embed Git HEAD and dirty state without a supported override.
Plain sources would lose the CI build identity. Its complete pinned ancestry is
staged: an existing shallow cache is deepened, so its Git bundle can be imported.
Claustrum's selected packages require no Git metadata; only a checksummed
`git archive` source tree is sent, and Windows verifies the digest and pinned
archive commit before extracting it. The helper prints both transport sizes.

The Windows lane imports the verified Subc Git bundle locally, extracts the
checked Claustrum archive, seeds npm's cache
from the verified local tarballs, runs `npm ci --offline`, and builds the pinned
fixtures with `CARGO_NET_OFFLINE=true`. The caller runs `cargo vendor --locked
--versioned-dirs` separately for each pinned fixture graph, including registry
packages, Git patches such as `tryhard` from commons, and target-specific packages
in both committed lockfiles. Separate graphs preserve distinct patch revisions.
Every non-path lockfile package must match a staged package's name/version and
registry checksum; Git source identities are retained in the inventory. The
source TAR and generated Cargo replacement configs join the transport manifest.
On Windows the extracted inventory and pinned lockfile hashes are checked again,
then each fixture build receives its own `--offline --config` source replacement.
Extracted vendor sources live at a stable, revision-keyed path under Motor's
target cache. Verified byte-identical files retain their mtimes, so source
restaging does not by itself invalidate Cargo's warm dependency artifacts.
No ambient Cargo registry/Git cache is required for those fixture graphs.
The caller needs Cargo for source resolution/copying, not compilation, and an
installed Python with `tomllib` (3.11+). The dispatcher selects a compatible
interpreter even when the caller's default `python3` is older. Source staging
may fetch missing locked packages on the caller; the VM builds remain offline.
Windows never fetches fixture Git repositories or downloads runtimes/NATS.
A cache miss never counts as a passing skip. Only Node/npm, CMake and Ninja have shipped offline fallbacks;
Python, NATS and Bun are required image prerequisites.

After the image-prerequisite check, the fast lane starts with fmt, file-only Rust
pins/counts/goldens, `cargo fetch --locked`, and an offline locked
dependency resolution. Train preconditions, path dependencies, dependency-feature
graphs and the portable-selection fixture guard also finish before native jobs
start. The unchanged shared operator suite runs in required `linux-small`,
overlapping the native jobs; it still checks fixture SHAs, consent pins,
test-group/guard lists, workflow contracts and every operator script. Those
guards are read-only, and a failed operator job fails the final gate.
On a warm dependency cache a broken fmt, lock or static pin fails before any
workspace test compilation; cold downloads and the complete operator suites are
not promised to finish in seconds. `check-fast-rust-invariants.py` reads Rust's
existing reviewed protocol-pair table, named golden SHA pins and inventory
minimum, and WAL manifest fixture count assertions (no second expected list).
Runtime Rust manifest builders/codec goldens and the
full portable-selection inventory require compiled test binaries and are not
misrepresented as static checks: they run in the heavy lanes, described below.

Fixture setup follows `fixtures.yml`: fetch the exact SHAs from
`scripts/ci-fixture-revisions.conf`, build both siblings with `--locked`, build
fake-surface on Linux only, verify binaries, package a tarball and use
`unpack-fixtures.sh`. It does not substitute this Mac's `.test-rigs` or moving
sibling checkouts. Windows deliberately does not require vault or fake-surface,
exactly as in CI. Both platforms require subc, aimock and the bus testkit;
Linux also requires vault. `npm ci` is run in the remote snapshot, not locally.

## Split dispatch with AFT

If the caller has no `ck`, or is already inside a runner that cannot launch
another job, dispatch the machine-side lanes from AFT instead. Do **not** put
`runon` on the outer dispatcher and expect nested dispatch to work:

1. `bash scripts/prepush.sh --lane fast` with `runon: "linux,2c,net"`.
2. Only on success, concurrently dispatch from **separate checkouts**:
    - `--lane linux-lint`, all four integration shards, `--lane linux-msrv`
      and `--lane linux-deploy` with `runon: "linux,8c,net"`;
    - `--lane linux-unit` and `--lane linux-production`
      with `runon: "linux,4c,net"`;
    - `--lane linux-small` with `runon: "linux,2c,net"` (operator/WAL);
   - `--lane windows` with `runon: "windows"`, after supplying the prepared input
     inside that worktree's non-ignored `prepush-inputs/`.
   - `bash scripts/prepush.sh --lane macos` with `runon: "macos"` (uses `nice -n 10`).
3. Keep the sum of in-flight requests at or below 32, counting Windows as eight
   and macOS as two. Follow the dispatcher priority above and admit deferred
   jobs only as slots are released. Wait for **every** job, retain all exit statuses and print complete
   failure logs. A missing/refused/timed-out lane is a failure, not coverage.

The individual `linux-operator` and `linux-wal` lanes remain available for
diagnostics but are not separately dispatched.
`--lane linux` remains an explicitly sequential diagnostic aggregate; it is not
dispatched by the gate. It runs the same steps
but cannot demonstrate the parallel wall-time budget. Direct AFT dispatch does
not add ignored files to snapshots: stage the explicit visible input folder or use the
ordinary dispatcher from a machine with the daemon connection. Do not re-enable
Windows network as a workaround.

## Parity audit

Audit source: all jobs in `.github/workflows/ci.yml` and the called
`.github/workflows/fixtures.yml`. CI is unchanged. `--lane` commands below are
machine-side commands and must run on the named platform; the normal dispatcher
runs fast, the split Linux jobs, windows and macos. The caller checks its committed
candidate before and after dispatch, and Git-backed Linux/macOS lanes retain
CI's clean-worktree check. Windows is a source-only export without `.git`, so
it does not run a meaningless Git command there.
Stable-toolchain version and host OS image must also match CI when comparing a
failure; Windows-target clippy on Linux is **not** native Windows coverage.

| CI job / expanded lanes | Pre-push equivalent | Selection and remaining differences |
| --- | --- | --- |
| `fixtures` → `fixtures.yml: fixtures` (Linux, Windows) | Fixture setup in Linux unit/integration/production/deploy and Windows jobs | Same pinned source SHAs and locked build/package/unpack. Windows receives Subc's full Git source and a checked plain Claustrum archive, then builds offline. |
| Scheduled fixture variant (Linux, Windows) | **Not part of the pre-push gate** | Schedule resolves both siblings' current `master` tips. A pre-push result cannot attest to future moving tips at 06:00 UTC. Daily compatibility remains CI-owned; push/PR fixtures are pinned. |
| `lint` (Ubuntu, Windows) | `linux-lint` / Windows native clippy | `cargo fmt --all -- --check`; `cargo clippy --locked --workspace --all-targets` with the four features from `rust-test-gate.sh`, `-- -D warnings`. Linux preflight additionally cross-clippies `x86_64-pc-windows-gnu`. |
| `scripts` (Ubuntu) | `fast` guards then the required `linux-small` job | Train preconditions and locked fetch remain before native jobs. `linux-small` runs unchanged `bash scripts/script-suites.sh`; same shared inventory and self-tests. |
| `store-contention-macos` | `bash scripts/prepush.sh --lane macos` | `nice -n 10 cargo test --locked -p broca-store-contention-tests --lib`. Native macOS; not substituted by Linux/Windows unit results. |
| `wal-reader` (Ubuntu, Windows) | WAL steps in `linux-small` / windows | `cargo test --locked -p broca-wal-reader`; `cargo test --locked -p broca-wal --lib framing::tests::v4`, including golden v4 codec and open-reader unlink semantics. |
| `wal-reader` (macOS) | `bash scripts/prepush.sh --lane macos` | Same two commands under `nice -n 10`, on native macOS, alongside store contention. |
| `integration-build` / Archive (Ubuntu, Windows) | Archive step in each `linux-integration-K` and windows | `bash scripts/ci-integration.sh build ARCHIVE`: independent libtest/nextest nonzero inventory equality, then archive with CI's four features. Linux also runs `python3 scripts/test-portable-test-selection.py` against built inventory. |
| `integration` (Ubuntu shards 1–4/4; Windows shards 1–4/4) | `linux-integration-1` through `-4` / four Windows replays | `bash scripts/ci-integration.sh run ARCHIVE K/4`; unchanged full CI filter, hash partitioning, sentinel, profile, pinned NATS, aimock and required fixtures. Linux partitions are parallel, Windows sequential. No portable-only filter on Windows. |
| `unit` / Lib/bin/doc (Ubuntu, Windows) | `linux-unit` / CI lib/bin/doc step in windows | Path-dependency checker + tests; `cargo test --locked --workspace --lib --bins --no-fail-fast` with the four features; independent doctest count and full doctests if positive. Includes store contention; no portable unit exclusions. |
| `production-features`: stable clippy | `linux-production` | `cargo clippy --locked -p broca-core -p broca-module -p broca-module-serve --all-targets -- -D warnings`, without test-clock or other test-gate features. |
| `production-features`: stable lib/bin | `linux-production` | `cargo test --locked -p broca-core -p broca-module -p broca-module-serve --lib --bins --no-fail-fast`, without test-clock, with required Linux fixtures/aimock. |
| `production-features`: MSRV | `linux-msrv` | CI installs `1.96` and runs `cargo +1.96 check --locked --workspace --all-targets`. Motor runs the same check using its preinstalled `+1.96.0` compiler: the uninstalled minor alias tries to sync into read-only RUSTUP_HOME. This still checks the promised 1.96 MSRV, not the default compiler. |
| `linux-deploy` / real ELF deployment | `linux-deploy` | Same locked release build of ck-broca, broca-session, broca-run; `BROCA_REQUIRE_LINUX_DEPLOY=1`, actual subc and `LinuxDeployTests.test_real_linux_subc_stage_place_restart_verify -v`; uses motor's target directory rather than checkout-local target. |
| Windows owner-preservation fixture | Windows integration replay; full distinct-owner proof on GitHub admin Windows CI | Non-admin Motor tokens that cannot assign Administrators as owner retain every repair assertion but name the unproved owner-distinct property as PARTIAL. |
| CI clean-worktree checks | Caller candidate checks before/after dispatch; Git-backed fast/Linux/macOS checks | The Windows source export has no `.git`; no Windows Git check is skipped elsewhere or replaced with a separate integrity mechanism. |
| `test` aggregate (Ubuntu, Windows display names) | Dispatcher's nonzero result on any lane failure | Includes targeted macOS results as well as both full fixture platforms. A missing, cancelled or failed lane never counts as success. |

Actions-only checkout/cache telemetry, wait/download/upload-artifact plumbing,
GitHub status aggregation, permissions and concurrency scheduling have no local
service equivalent: they operate on a workflow run that does not exist until
push. `test-ci-pipeline.py` exercises those script/graph contracts in the operator
lane. Building and replaying the archives locally tests the artifact content,
not GitHub's transport/availability or hosted-image provisioning. A green gate
does not predict a GitHub outage or a future stable compiler/image update.
The dispatcher allows 7200 seconds for each combined motor lane, including
setup; CI splits that work across jobs with separate 15–90 minute budgets.
The nextest CI profile and its per-test/resource-group limits are unchanged.

The existing Mac-only `.cortexkit/scripts/native-release-suites.sh` is an
**additional release gate**, not a job in either workflow. Only the integrating
agent runs its real-daemon, conformance/kill and crash-harness lanes, once per
merged batch. Do not launch it from `prepush.sh`, duplicate it on this Mac, or
claim that a portable preflight replaces it. The macOS operator placement-restart
suite is also native-only and reported NOT RUN by Linux's `script-suites.sh`;
the integrating Mac gate runs it. Neither is silently counted as Linux coverage.
