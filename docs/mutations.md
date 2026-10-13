# Mutation catalogue

## Replay one row

[`mutations.toml`](../mutations.toml) records executable safety proofs, not machine-specific timing measurements. The runner is `cortexkit-mutate` from the cortexkit/commons repository, pinned to one commit so every repository replays with the same runner. broca does not build its own. The pin lives in one place, `CK_MUTATE_REV` in [`.github/workflows/mutations.yml`](../.github/workflows/mutations.yml), so install that exact commit:

```sh
rev=$(sed -n 's/^  CK_MUTATE_REV: //p' .github/workflows/mutations.yml)
cargo install --locked --git https://github.com/cortexkit/commons --rev "$rev" cortexkit-mutate
mkdir -p target/mutations
timeout 3600 ckdev-mutate run --only wal-reader-never-truncates --report target/mutations/one.json
```

Run serially from a clean checkout. The runner collects a fresh green baseline, applies each exact-once anchor, builds with `--locked`, requires the named tests to fail, and restores source bytes and checks `Cargo.lock`. Missing anchors/tests, compile errors, signals and timeouts are not catches. Never check out a file while replay is running. Reports belong under gitignored `target/mutations/`; `ckdev-mutate check` validates the catalogue and exact test names. The row deadlines use the pinned runner defaults, not measurements from a shared workstation.

The drain notice, busy declaration, restart request-byte and tool-route refusal rows boot a real daemon: set `SUBC_CORE_BIN` and `FAKE_AFT_STUB_BIN` to the built fixtures, with the `ck` CLI beside `ck-subc`, and `BROCA_REQUIRE_SUBC=1` so missing fixtures fail instead of skipping. For a full replay or breadth audit, install aimock (the local mock model-provider server under `node_modules`, used by the provider integration tests) with `npm ci`, and provide `CK_CLAUSTRUM_BIN`, `CK_AUTH_BIN` and `CK_CLAUSTRUM_CHECKOUT` for the vault arms (the tests that run against a real claustrum vault); enable `BROCA_REQUIRE_AIMOCK=1` and `BROCA_REQUIRE_VAULT=1`. The checkout variable matters when cached vault binaries are outside their source checkout. CI builds these fixtures with the same steps as the existing integration job.

## Add a row

Choose a silent, costly failure in crash safety, authorization, wire contracts or data retention. Copy the `old` anchor from current production code, choose a minimal real break, and resolve full test names with `cargo test --locked -p PACKAGE --test TARGET -- --list` (or `--lib`). Add a unique `[[control]]` ID, `guards`, edit, `test_file`, runner/package/target and `expect_red`; code that compiles only under a Cargo feature also needs `features`, for example `features = ["live"]` for broca-provider's transport and broca-catalog's live resolver, whose tests are otherwise never built; use `expect_message` when a test might fail for an unrelated reason. Mark planted breaks `NON-VACUITY BREAK`: this comment identifies a deliberate test of the guard, not a real change, so readers and scans can distinguish planted violations from ordinary code. Replay the new row alone with `ckdev-mutate run --only ID --report target/mutations/ID.json` and keep it only when the intended test actually goes red. An exactly-once proof must include a real production path, not only a test-local reconstruction. A source-scan guard needs a planted violation. Record missing guards as gaps instead of substituting weak tests. See `crates/cortexkit-mutate/README.md` in cortexkit/commons, at the pinned commit, for multi-file edits and failure identity.

## CI and review rules

The catalogue runs in its own [mutation workflow](../.github/workflows/mutations.yml), separately from the commit gate.

The Ubuntu job replays touched rows on changes, the full catalogue on pushes to master, and a nightly full `--broad` audit. Diff selection compares committed base to HEAD: source/edit paths, the guarding `test_file`, and changed parsed rows select proofs; helper/fixture changes not named there need the full replay. Broad replay runs every test target in the row's package. Narrow unintended collateral; `hub` names the shared property deliberately asserted by several test targets, and `hub_targets` lists the other targets observed catching it in a broad replay. Add these fields only after reviewing that replay, never merely to silence a warning. No test may hang: its own timeout must stop leftover tasks/processes and fail under its name, rather than relying on the runner's last-resort process-tree deadline as a catch. Set or tighten time budgets only from reproducible runners. Artifact reports under `target/mutations/` retain red/green names, collateral and baseline failures for review.

### Native Windows coverage

The Windows mutation job replays the full root catalogue across four shards nightly and on pushes to master. Pull requests, train pushes and other events replay touched rows using `--diff` with the same committed-base selection as Ubuntu. A Windows proof must run on Windows before it can count as covered. The Ubuntu job keeps its existing diff, full and nightly breadth selection.

Both jobs install the same `CK_MUTATE_REV`. At that pin, an absent `platforms` list matches every OS; a present list must contain Rust's OS name `windows` to match Windows. The runner sorts the selected catalogue rows by ID and assigns zero-based positions round-robin to the four shards **before** platform filtering. Before replay, the verifier independently records that selection in `windows-selection.json`, rather than inferring it from the rows that survived into the replay report. Each shard refuses an omitted or skipped eligible selected row, even though the runner itself considers `SKIPPED_PLATFORM` a passing outcome. Only `CAUGHT`, `CAUGHT_BROADLY` and `HUB` count as replayed: these outcomes denote a mutation caught by failing tests, rather than an exemption such as `EQUIVALENT` or `DESK_ONLY`. Each accepted row must include every expected test in its observed `red` names. An expected test name in the catalogue alone is not evidence of a replay. Recheck the verifier's selection, eligibility, sharding and outcome rules whenever changing the runner pin.

For touched runs, the verifier follows the pinned runner's `select_diff`: compare `<base>` to committed `HEAD` with `git diff --name-only -z`, and read the base catalogue with `git show`. A current row is selected when its `test_file` or any edit path changed, its parsed row differs from the base row, or the global prebuild list differs. New rows count; deleted rows do not. Explicit serde defaults do not change parsed-row equality. Missing base catalogues are accepted only when the catalogue path appears in the diff. A new train branch's all-zero push base, or an absent base, falls back to `origin/master`, as on Ubuntu. An empty touched selection is valid, but a missing report or a missing eligible selected row is not.

The job builds native `.exe` fixtures and sets `BROCA_REQUIRE_SUBC`, `BROCA_REQUIRE_VAULT` and `BROCA_REQUIRE_AIMOCK` to `1`. These switches make missing daemon, vault or aimock dependencies fail their tests instead of returning early and reporting a passing skip. Windows artifacts are named `mutations-windows-EVENT-shard-N`: `windows-replay.json` is the pinned runner's original row report (including per-test failure output), `windows-selection.json` records the expected selected IDs, mode, base, HEAD, runner revision and shard, and `windows-coverage.json` adds every accepted row and its observed red tests. The same ID/test table appears in the job summary. Verification and artifact upload run even after replay failure; a missing report is a failure, not an empty success.

The report verifier lives in [`scripts/verify-windows-mutations.py`](../scripts/verify-windows-mutations.py). Its `--self-test` mode runs through `scripts/script-suites.sh`, shared by CI and the local `scripts/gate.sh` entry point, and before mutation replay in the Windows job. Its Windows-only catalogue controls run named tests directly from the same file:

| Row ID | Expected failing test |
| --- | --- |
| `windows-mutations-require-complete-report` | `CoverageTests.test_rejects_filtered_eligible_row` |
| `windows-mutations-reject-eligible-skip` | `CoverageTests.test_rejects_skipped_eligible_row` |
| `windows-mutations-report-observed-red` | `CoverageTests.test_requires_observed_expected_red_test` |
| `windows-mutations-pin-shard-rule` | `CoverageTests.test_pinned_shards_sort_before_platform_filtering` |
| `windows-mutations-pin-platform-rule` | `CoverageTests.test_pinned_platform_rule` |
