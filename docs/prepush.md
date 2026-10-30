# Pre-push checks and CI parity

Run from the Mac checkout, before pushing a committed candidate:

```sh
bash scripts/prepush.sh
```

The dispatcher uses `ck motor run` (the same build service as AFT's `runon`). It
never builds on the Mac as a fallback. First it runs the fast Linux lane. Only
when that passes does it launch independent Linux portable, unit/doc,
integration-1 through integration-4, production, MSRV, WAL and deployment jobs,
alongside native Windows and targeted macOS. Portable and deployment reserve
16 vCPUs; the other Linux jobs reserve 8 each. Each job has a separate managed
checkout under `target/scratch/prepush-checkouts` and a separate Cargo target
namespace. Motor serializes jobs from one workspace: merely backgrounding
several jobs against the original checkout would not parallelize the gate.

Each Linux integration job builds/checks its own archive and replays its one
unchanged CI partition. Windows builds one archive and replays all four
partitions. Tests, CI profile, clock sentinel and partition boundaries are
unchanged. The clock sentinel is an executed retention test proving that the
controllable test-clock feature is enabled, not merely listed in the archive.
A failing shard does not stop the other shards or unit job. These
stable checkout/cache locations amortize compilation on subsequent pushes;
cold compilation and build-server admission can still dominate wall time.

Each check prints its complete output, exit status and wall time. The final
summary prints the first error lines and replays complete failed-lane output,
including failing test names, rather
than trying to extract names with a lossy log filter. Dispatcher logs are under
`target/scratch/prepush.*`; copy evidence outside `target/` if it must survive
build cleanup. A motor refusal before start is **NOT-RUN** by lane name, not a
test failure or pass; connection-loss outcomes are **UNKNOWN**. Either fails the
gate. Setup failures explicitly mark dependent suites **NOT RUN** and return
failure, never a passing skip. No retries or timeout increases mask tests.

Daemon/test temporary files are separate from logs: the runner's
`$HOME/.broca-prepush-runtime` and its per-job leaves are 0700, under umask 077.
`TMPDIR`, `TMP` and `TEMP` point there,
so `broca_test_support::scratch_root()` and daemon connection files do not inherit
`target/scratch`'s potentially group-writable ancestors, including when the
entire managed checkout is nested below `target/scratch`. Step logs remain outside
the test temporary root, include the command/exit code even for silent errors,
and are replayed before the remote job disappears. Run from a checkout on a
healthy internal disk so the caller's retained logs are not lost to volume stalls.

## Prerequisites and the fast boundary

Use a clean committed checkout (also required by `linux-preflight.sh`). The build
machines need stable Rust with rustfmt/clippy, cargo-nextest, Git, Python 3.11+
with PyYAML, Node **20**, npm, and GNU tar. The MOTOR Linux image must provide
**`file(1)` and a C compiler (`cc`)** for the operator ELF fixtures and deployment
checks. The fast lane refuses immediately, before running tests, if those image
prerequisites are missing; it does not install OS packages or skip their tests.
Linux additionally needs the
`x86_64-pc-windows-gnu` Rust target/linker and toolchain `1.96.0` for the MSRV check.
Motor supplies `CARGO_TARGET_DIR`; concurrent lanes use different stable
subdirectories. Only Linux jobs request outbound network. No GitHub Actions
token or artifact credentials are needed.
If Linux's Python lacks PyYAML, the fast lane provisions checksum-pinned pure
Python `PyYAML==6.0.3` under its scratch directory (no system pip needed).
The unit, integration, production and Windows jobs provision checksum-pinned
Node 20.20.2 when Node 20 is absent; Windows additionally provisions Python 3.13.5
when `python3` is absent. These
are setup-action equivalents, not substitutes for checks. Cold tool downloads
need network on the caller/Linux runner; no host installation is changed.

### Offline Windows inputs

Windows motor jobs do **not** support `--net`. The dispatcher runs
`scripts/prepare-prepush-windows.py` on the caller to download and verify the
Python/Node/NATS archives and every npm tarball in `package-lock.json`, and to
check out the pinned fixture sources. It sends a separate `prepush-inputs`
repository and the two source repositories with `--sibling`. Binary inputs are
untracked in that input repository, never committed or placed in broca's source
tree. Download bytes are cached under `target/scratch/prepush-offline-cache` and
revalidated, not silently replaced after checksum failure.

The Windows lane only copies/verifies/extracts those archives, seeds npm's cache
from the verified local tarballs, runs `npm ci --offline`, and builds the pinned
fixtures with `CARGO_NET_OFFLINE=true`. It never fetches fixture Git repositories
or downloads runtimes/NATS inside the VM. Motor must stage the locked Cargo
sources for broca and its two siblings; a missing crate/revision fails with
Cargo's exact offline diagnostic in the summary. No tests are skipped for a
cache miss. Installed Python/Node 20 are reused when already present.

After the image-prerequisite check, the fast lane starts with fmt, file-only Rust
pins/counts/goldens, `cargo fetch --locked`, and an offline locked
dependency resolution; the shared operator suites then check the fixture SHAs,
consent pins, path dependencies, dependency-feature graphs, test-group lists,
guard lists, workflow contracts and operator scripts. Those guards are read-only.
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

1. `bash scripts/prepush.sh --lane fast` with `runon: "linux,8c,net"`.
2. Only on success, concurrently dispatch from **separate checkouts**:
   - `--lane linux-portable` and `--lane linux-deploy` with `runon: "linux,16c,net"`;
   - `--lane linux-unit`, `--lane linux-integration-1` through `-4`,
     `--lane linux-production`, `--lane linux-msrv` and `--lane linux-wal`
     with `runon: "linux,8c,net"`;
   - `--lane windows` with `runon: "windows"`, after supplying the prepared input
     and fixture-source sibling repositories (or an equivalent provisioned image).
   - `bash scripts/prepush.sh --lane macos` with `runon: "macos"` (uses `nice -n 10`).
3. Wait for **every** job, retain all exit statuses and print complete
   failure logs. A missing/refused/timed-out lane is a failure, not coverage.

`--lane linux` remains an explicitly sequential diagnostic aggregate; it is not
dispatched by the gate. It runs the same steps
but cannot demonstrate the parallel wall-time budget. Direct AFT dispatch does
not expose `--sibling`: stage equivalent offline inputs separately or use the
ordinary dispatcher from a machine with the daemon connection. Do not re-enable
Windows network as a workaround.

## Parity audit

Audit source: all jobs in `.github/workflows/ci.yml` and the called
`.github/workflows/fixtures.yml`. CI is unchanged. `--lane` commands below are
machine-side commands and must run on the named platform; the normal dispatcher
runs fast, the split Linux jobs, windows and macos. All lanes finish with CI's clean-worktree check.
Stable-toolchain version and host OS image must also match CI when comparing a
failure; Windows-target clippy on Linux is **not** native Windows coverage.

| CI job / expanded lanes | Pre-push equivalent | Selection and remaining differences |
| --- | --- | --- |
| `fixtures` → `fixtures.yml: fixtures` (Linux, Windows) | Fixture setup in Linux unit/integration/production/deploy and Windows jobs | Same pinned sibling SHAs and locked build/package/unpack. Linux sources have stable per-job cache paths; Windows receives sources as siblings and builds offline. |
| Scheduled fixture variant (Linux, Windows) | **Not part of the pre-push gate** | Schedule resolves both siblings' current `master` tips. A pre-push result cannot attest to future moving tips at 06:00 UTC. Daily compatibility remains CI-owned; push/PR fixtures are pinned. |
| `lint` (Ubuntu, Windows) | `linux-portable` / Windows native clippy | `cargo fmt --all -- --check`; `cargo clippy --locked --workspace --all-targets` with the four features from `rust-test-gate.sh`, `-- -D warnings`. Linux preflight additionally cross-clippies `x86_64-pc-windows-gnu`. |
| `scripts` (Ubuntu) | `bash scripts/prepush.sh --lane fast` | `python3 scripts/check-train-preconditions.py`, `cargo fetch --locked`, `bash scripts/script-suites.sh`; same shared suite inventory and self-tests. |
| `store-contention-macos` | `bash scripts/prepush.sh --lane macos` | `nice -n 10 cargo test --locked -p broca-store-contention-tests --lib`. Native macOS; not substituted by Linux/Windows unit results. |
| `wal-reader` (Ubuntu, Windows) | `linux-wal` / WAL steps in windows | `cargo test --locked -p broca-wal-reader`; `cargo test --locked -p broca-wal --lib framing::tests::v4`, including golden v4 codec and open-reader unlink semantics. |
| `wal-reader` (macOS) | `bash scripts/prepush.sh --lane macos` | Same two commands under `nice -n 10`, on native macOS, alongside store contention. |
| `integration-build` / Archive (Ubuntu, Windows) | Archive step in each `linux-integration-K` and windows | `bash scripts/ci-integration.sh build ARCHIVE`: independent libtest/nextest nonzero inventory equality, then archive with CI's four features. Linux also runs `python3 scripts/test-portable-test-selection.py` against built inventory. |
| `integration` (Ubuntu shards 1–4/4; Windows shards 1–4/4) | `linux-integration-1` through `-4` / four Windows replays | `bash scripts/ci-integration.sh run ARCHIVE K/4`; unchanged full CI filter, hash partitioning, sentinel, profile, pinned NATS, aimock and required fixtures. Linux partitions are parallel, Windows sequential. No portable-only filter on Windows. |
| `unit` / Lib/bin/doc (Ubuntu, Windows) | `linux-unit` / CI lib/bin/doc step in windows | Path-dependency checker + tests; `cargo test --locked --workspace --lib --bins --no-fail-fast` with the four features; independent doctest count and full doctests if positive. Includes store contention; no portable unit exclusions. |
| `production-features`: stable clippy | `linux-production` | `cargo clippy --locked -p broca-core -p broca-module -p broca-module-serve --all-targets -- -D warnings`, without test-clock or other test-gate features. |
| `production-features`: stable lib/bin | `linux-production` | `cargo test --locked -p broca-core -p broca-module -p broca-module-serve --lib --bins --no-fail-fast`, without test-clock, with required Linux fixtures/aimock. |
| `production-features`: MSRV | `linux-msrv` | CI installs `1.96` and runs `cargo +1.96 check --locked --workspace --all-targets`. Motor runs the same check using its preinstalled `+1.96.0` compiler: the uninstalled minor alias tries to sync into read-only RUSTUP_HOME. This still checks the promised 1.96 MSRV, not the default compiler. |
| `linux-deploy` / real ELF deployment | `linux-deploy` | Same locked release build of ck-broca, broca-session, broca-run; `BROCA_REQUIRE_LINUX_DEPLOY=1`, actual subc and `LinuxDeployTests.test_real_linux_subc_stage_place_restart_verify -v`; uses motor's target directory rather than checkout-local target. |
| `test` aggregate (Ubuntu, Windows display names) | Dispatcher's nonzero result on any lane failure | Includes targeted macOS results as well as both full fixture platforms. A missing, cancelled or failed lane never counts as success. |

Actions-only checkout/cache telemetry, wait/download/upload-artifact plumbing,
GitHub status aggregation, permissions and concurrency scheduling have no local
service equivalent: they operate on a workflow run that does not exist until
push. `test-ci-pipeline.py` exercises those script/graph contracts in the fast
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
