# Pre-push checks and CI parity

Run from the Mac checkout, before pushing a committed candidate:

```sh
bash scripts/prepush.sh
```

The dispatcher uses `ck motor run` (the same build service as AFT's `runon`). It
never builds on the Mac as a fallback. First it runs the fast Linux lane. Only
when that passes does it launch **Linux, 16 vCPUs**, and **native Windows** in
parallel, alongside a targeted macOS store-contention/WAL lane. These use
different platform workspaces/caches. Within each platform,
CI's four integration partitions run sequentially from one nextest archive;
the selected tests, CI profile, clock sentinel and partition boundaries are
unchanged. A failing shard does not prevent the other shards or lib tests from
running. This is intentionally more work than `linux-preflight.sh` alone.

Each check prints its complete output, exit status and wall time. The final
summary replays complete failed-lane output, including failing test names, rather
than trying to extract names with a lossy log filter. Dispatcher logs are under
`target/scratch/prepush.*`; copy evidence outside `target/` if it must survive
build cleanup. Setup failures explicitly mark dependent suites **NOT RUN** and
return failure, never a passing skip. No retries or timeout increases mask tests.

## Prerequisites and the fast boundary

Use a clean committed checkout (also required by `linux-preflight.sh`). The build
machines need stable Rust with rustfmt/clippy, cargo-nextest, Git, Python 3.11+
with PyYAML, Node **20**, npm, and GNU tar. The MOTOR Linux image must provide
**`file(1)` and a C compiler (`cc`)** for the operator ELF fixtures and deployment
checks. The fast lane refuses immediately, before running tests, if those image
prerequisites are missing; it does not install OS packages or skip their tests.
Linux additionally needs the
`x86_64-pc-windows-gnu` Rust target/linker and toolchain `1.96` for the MSRV check.
Motor supplies `CARGO_TARGET_DIR`. Jobs enable outbound network access for locked
Cargo/npm dependencies, checksum-pinned NATS and the public fixture repositories.
No GitHub Actions token or artifact credentials are needed.
If Linux's Python lacks PyYAML, the fast lane provisions checksum-pinned pure
Python `PyYAML==6.0.3` under its scratch directory (no system pip needed).
Heavy lanes provision checksum-pinned Node 20.20.2 when Node 20 is absent;
Windows additionally provisions Python 3.13.5 when `python3` is absent. These
are setup-action equivalents, not substitutes for checks. Cold tool downloads
need network too; no host installation is changed.

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
2. Only on success, concurrently dispatch:
   - `bash scripts/prepush.sh --lane linux` with `runon: "linux,16c,net"`;
   - `bash scripts/prepush.sh --lane windows` with `runon: "windows"`.
   - `bash scripts/prepush.sh --lane macos` with `runon: "macos"` (uses `nice -n 10`).
3. Wait for **all three** jobs, retain all exit statuses and print complete
   failure logs. A missing/refused/timed-out lane is a failure, not coverage.

The Windows runner must permit the prerequisite network fetches. If policy
disables network there, provision the same pinned inputs on that runner first;
the lane fails rather than falling back to Linux or skipping integrations.

## Parity audit

Audit source: all jobs in `.github/workflows/ci.yml` and the called
`.github/workflows/fixtures.yml`. CI is unchanged. `--lane` commands below are
machine-side commands and must run on the named platform; the normal dispatcher
runs fast, linux, windows and macos. All lanes finish with CI's clean-worktree check.
Stable-toolchain version and host OS image must also match CI when comparing a
failure; Windows-target clippy on Linux is **not** native Windows coverage.

| CI job / expanded lanes | Pre-push equivalent | Selection and remaining differences |
| --- | --- | --- |
| `fixtures` → `fixtures.yml: fixtures` (Linux, Windows) | `bash scripts/prepush.sh --lane linux` / `--lane windows`, fixture setup | Same pinned sibling SHAs, `cargo build --locked --manifest-path … -p subc-core --bins`, `-p credentials-module --bins`; Linux fake-surface; verify/package/unpack. Local scratch/cache layout replaces Actions artifact transport. |
| Scheduled fixture variant (Linux, Windows) | **Not part of the pre-push gate** | Schedule resolves both siblings' current `master` tips. A pre-push result cannot attest to future moving tips at 06:00 UTC. Daily compatibility remains CI-owned; push/PR fixtures are pinned. |
| `lint` (Ubuntu, Windows) | Linux portable preflight / Windows native clippy in the respective lanes | `cargo fmt --all -- --check`; `cargo clippy --locked --workspace --all-targets` with the four features from `rust-test-gate.sh`, `-- -D warnings`. Linux preflight additionally cross-clippies `x86_64-pc-windows-gnu`. |
| `scripts` (Ubuntu) | `bash scripts/prepush.sh --lane fast` | `python3 scripts/check-train-preconditions.py`, `cargo fetch --locked`, `bash scripts/script-suites.sh`; same shared suite inventory and self-tests. |
| `store-contention-macos` | `bash scripts/prepush.sh --lane macos` | `nice -n 10 cargo test --locked -p broca-store-contention-tests --lib`. Native macOS; not substituted by Linux/Windows unit results. |
| `wal-reader` (Ubuntu, Windows) | WAL reader + v4 framing steps in linux/windows | `cargo test --locked -p broca-wal-reader`; `cargo test --locked -p broca-wal --lib framing::tests::v4`, including golden v4 codec and open-reader unlink semantics. |
| `wal-reader` (macOS) | `bash scripts/prepush.sh --lane macos` | Same two commands under `nice -n 10`, on native macOS, alongside store contention. |
| `integration-build` / Archive (Ubuntu, Windows) | Archive step in linux/windows | `bash scripts/ci-integration.sh build ARCHIVE`: independent libtest/nextest nonzero inventory equality, then archive with CI's four features. Linux also runs `python3 scripts/test-portable-test-selection.py` against built inventory. |
| `integration` (Ubuntu shards 1–4/4; Windows shards 1–4/4) | Four replay steps in linux/windows | `bash scripts/ci-integration.sh run ARCHIVE K/4`; unchanged `kind(test) and not (binary(=live) or binary(=live_e2e))`, hash partitioning, test-clock sentinel, CI nextest profile, pinned NATS, Node/aimock and required fixtures. Shards are sequential per OS locally, parallel in Actions. No portable-only filter on Windows. |
| `unit` / Lib/bin/doc (Ubuntu, Windows) | CI lib/bin/doc step in linux/windows | Path-dependency checker + its tests; `cargo test --locked --workspace --lib --bins --no-fail-fast` with the four shared features; independent doctest count, then `cargo test --locked --workspace --doc` if positive. Includes store contention on these platforms; no Linux-preflight unit exclusions applied here. |
| `production-features`: stable clippy | Production clippy in linux | `cargo clippy --locked -p broca-core -p broca-module -p broca-module-serve --all-targets -- -D warnings`, without test-clock or other test-gate features. |
| `production-features`: stable lib/bin | Production lib/bin in linux | `cargo test --locked -p broca-core -p broca-module -p broca-module-serve --lib --bins --no-fail-fast`, without test-clock, with required Linux fixtures/aimock. |
| `production-features`: MSRV | MSRV step in linux | `cargo +1.96 check --locked --workspace --all-targets`. Toolchain must be provisioned, just as CI's install step does. |
| `linux-deploy` / real ELF deployment | ELF deployment step in linux | Same locked release build of ck-broca, broca-session, broca-run; `BROCA_REQUIRE_LINUX_DEPLOY=1`, actual subc and `LinuxDeployTests.test_real_linux_subc_stage_place_restart_verify -v`; uses motor's target directory rather than checkout-local target. |
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
