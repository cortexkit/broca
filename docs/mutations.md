# Mutation catalogue

## Replay one row

[`mutations.toml`](../mutations.toml) records executable safety proofs, not machine-specific timing measurements. The runner is `cortexkit-mutate` from the cortexkit/commons repository, pinned to one commit so every repository replays with the same runner. broca does not build its own. Read `CK_MUTATE_REV` from [`.github/workflows/mutations.yml`](../.github/workflows/mutations.yml) and install that exact commit; the Windows workflow keeps the same pin, checked by the script suites:

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

The Linux catalogue runs in its own [Mutation catalogue workflow](../.github/workflows/mutations.yml), separately from the local commit gate. Releases require `CI` and `Mutation catalogue`; native Windows proofs run in the separate [Mutation catalogue (Windows) workflow](../.github/workflows/mutations-windows.yml), which is not a release-required workflow.

The Ubuntu job replays touched rows on changes, the full catalogue on pushes to master, and a nightly full `--broad` audit. Diff selection compares committed base to HEAD: source/edit paths, the guarding `test_file`, and changed parsed rows select proofs; helper/fixture changes not named there need the full replay. Broad replay runs every test target in the row's package. Narrow unintended collateral; `hub` names the shared property deliberately asserted by several test targets, and `hub_targets` lists the other targets observed catching it in a broad replay. Add these fields only after reviewing that replay, never merely to silence a warning. No test may hang: its own timeout must stop leftover tasks/processes and fail under its name, rather than relying on the runner's last-resort process-tree deadline as a catch. Set or tighten time budgets only from reproducible runners. Artifact reports under `target/mutations/` retain red/green names, collateral and baseline failures for review.

Ubuntu uses 32 independent checkouts, with the shard denominator derived from the matrix size. Focused jobs retain a 90-minute budget; the nightly breadth audit has 180 minutes. The pinned runner already reuses the unmodified build and test results between compatible Cargo rows within each shard. Skipping those checks for individual rows would not add the reuse: it is already enabled.

### Layered consent authorization and stop fences

Consent checks at different boundaries can deliberately overlap. A mutation of
only the earlier check can survive while the same safety property still holds at
the later boundary. Joint-fence rows disable both checks in production code;
they do not claim that either check alone is the sole protection.

`core-consent-live-feed-complete-before-approval` and
`core-consent-live-adoption-proves-recorded-duplicate` were retired as
single-guard proofs after `18d4510` centralized held and late authorization in
`consent::authorize_candidate` and `4c73dff` required a current-head tail read
before minting authorization. That authorizer reads settlement continuations
and proves a recorded duplicate closed before adopting a different card. Thus
truncating the live intake scan or bypassing its duplicate check alone no longer
authorizes an unsafe call. The replacement rows
`core-consent-live-feed-and-authorizer-complete-before-approval` and
`core-consent-live-adoption-duplicate-proof-fences` also disable the corresponding
shared authorizer check. The original end-to-end tests and assertions remain
unchanged, including the requirement that no approval or provider send occurs
before conflicting decisions are resolved.

The authorizer also has independent, direct-call proofs:
`core-consent-authorizer-live-feed-complete` and
`core-consent-authorizer-proves-recorded-duplicate`. They disable only the
authorizer's tail read or duplicate proof, respectively. Their tests call
`consent::authorize_candidate` without entering the run loop, so the redundant
live-intake guards cannot mask a missing authorizer check. The run-level guards
remain audited defense in depth; the authorizer is the shared authorization
choke point.

`core-consent-late-stop-during-intent-sync` and
`core-consent-late-stop-during-route-open` were retired as single-guard proofs.
They disabled only `late_stopped`, but approved dispatch now also carries cancel
and stop tokens to the adapter's physical-send boundary. This signal propagation
was added by `f14bc889` (approved run signals), not by the later test-wait or
approval-expiry driver changes. Both original mutations survive because the
adapter still refuses the send, not because the tests miss their stop windows.
The intent-sync test holds the real WAL sync, verifies that the intent is not
durable yet, and cancels before releasing it; the route-open test cancels inside
`batch_started` before that call returns.

The replacement rows `core-consent-late-intent-sync-stop-fences` and
`core-consent-late-route-open-stop-fences` preserve those tests and assertions.
Each disables the core check **and** drops `send_late`'s forwarded signals, so
neither fence can mask the other. The breaks affect production code, not the
test adapter's required signal check. These are joint-fence proofs: they do not
claim that losing either fence alone permits a physical send.

For an adapter honoring `ToolPlane::dispatch_approved`'s signal contract, there is
no no-send interleaving protected only by `late_stopped`: the adapter checks
later, including after its own waits. The core checks remain as defense in depth
and avoid entering route setup or dispatch after an observed stop. The separate
`subc-approved-pre-send-cancel-after-route-wait` and
`subc-approved-pre-send-cancel-after-permit-wait` rows target the production
adapter's post-wait check. `subc-approved-writer-queue-stop-fence` targets the
production connection's cancellation wait while reserving writer capacity,
guarded by `approved_cancel_and_stop_while_writer_queue_is_full_are_not_sent`
in `broca-subc`. That still-later enqueue boundary must remain `NotSent` rather
than outcome-unknown when interrupted.

`core-consent-late-intent-sync-core-stop-fence` and
`core-consent-late-route-open-core-stop-fence` separately prove the core fence.
They disable only `late_stopped` and use a deliberately non-compliant test
adapter that ignores `DispatchSignals`. The tests reuse every assertion from
the corresponding compliant-adapter stop tests, including both cancel and stop.
The production adapter's writer-queue test calls `SubcToolPlane::dispatch_approved`
directly, without the run loop or `late_stopped`, so its existing
`subc-approved-writer-queue-stop-fence` row independently proves the adapter layer.

### Native Windows coverage

The Windows workflow replays the full root catalogue across twelve native shards nightly and on manual dispatch. Each shard targets about 60-75 minutes; the 360-minute job ceiling remains to accommodate cold builds and uneven proof costs. Pull requests and pushes to master or `train/**` start it only when the catalogue, mutation workflows/tooling, or fixture setup changes, and use `--diff` with the same committed-base selection as Ubuntu. The workflow's `paths` filters list these inputs; catalogue changes conservatively trigger it even when only a non-Windows row changed, in which case Windows-eligible diff coverage can be empty. Source-only changes receive Windows proofs in the next nightly run, or by manual dispatch.

Ordinary master pushes do not start the Windows workflow. broca ships only on macOS, so native Windows mutation proofs remain a compatibility audit rather than adding hours to every release. The Linux workflow retains its master-push full replay, 32 shards and nightly breadth audit. Release-required names exclude `Mutation catalogue (Windows)`, but `ci-status.sh` still requires every run attached to a release commit to succeed: an explicitly dispatched, nightly, or path-triggered Windows run on that same commit must finish too. Its all-runs semantics are unchanged.

Both workflows install the same `CK_MUTATE_REV`. At that pin, an absent `platforms` list matches every OS; a present list must contain Rust's OS name `windows` to match Windows. The runner sorts the selected catalogue rows by ID and assigns zero-based positions round-robin to the twelve shards **before** platform filtering. Selection and replay both derive the denominator from the matrix size. Before replay, the verifier independently records that selection in `windows-selection.json`, rather than inferring it from the rows that survived into the replay report. Each shard refuses an omitted or skipped eligible selected row, even though the runner itself considers `SKIPPED_PLATFORM` a passing outcome. Only `CAUGHT`, `CAUGHT_BROADLY` and `HUB` count as replayed: these outcomes denote a mutation caught by failing tests, rather than an exemption such as `EQUIVALENT` or `DESK_ONLY`. Each accepted row must include every expected test in its observed `red` names. An expected test name in the catalogue alone is not evidence of a replay. Recheck the verifier's selection, eligibility, sharding and outcome rules whenever changing the runner pin.

Shell command rows use `python3 -B scripts/mutation-bash.py` rather than a bare `bash` executable name. On Windows, set `BROCA_MUTATION_BASH` to Git Bash's absolute executable path (for example, `C:\Program Files\Git\bin\bash.exe`); CI records the Bash executable running its setup step. Rust searches System32 before the parent's `PATH`, so merely prepending Git Bash to `PATH` cannot exclude the WSL launcher. The Python helper requires the explicit path on Windows and preserves the shell command and test arguments. Linux/macOS may omit the variable and use Bash from `PATH`.

For touched runs, the verifier follows the pinned runner's `select_diff`: compare `<base>` to committed `HEAD` with `git diff --name-only -z`, and read the base catalogue with `git show`. A current row is selected when its `test_file` or any edit path changed, its parsed row differs from the base row, or the global prebuild list differs. New rows count; deleted rows do not. Explicit serde defaults do not change parsed-row equality. Missing base catalogues are accepted only when the catalogue path appears in the diff. A new train branch's all-zero push base, or an absent base, falls back to `origin/master`, as on Ubuntu. An empty touched selection is valid, but a missing report or a missing eligible selected row is not.

The job builds native `.exe` fixtures and sets `BROCA_REQUIRE_SUBC`, `BROCA_REQUIRE_VAULT` and `BROCA_REQUIRE_AIMOCK` to `1`. These switches make missing daemon, vault or aimock dependencies fail their tests instead of returning early and reporting a passing skip. Windows artifacts are named `mutations-windows-EVENT-shard-N`: `windows-replay.json` is the pinned runner's original row report (including per-test failure output), `windows-selection.json` records the expected selected IDs, mode, base, HEAD, runner revision and shard, and `windows-coverage.json` adds every accepted row and its observed red tests. The same ID/test table appears in the job summary. Verification and artifact upload run even after replay failure; a missing report is a failure, not an empty success.

The static platform preflight lives in [`scripts/verify-mutation-platforms.py`](../scripts/verify-mutation-platforms.py). Run it without flags to check every root catalogue row. It resolves the Cargo target, enclosing module declarations, file-level gates and test attributes, evaluating `unix`, `windows`, `target_os`, `test`, `feature`, `not`, `all`, `any` and conditional `cfg_attr` gates. Cargo default features and the row's requested features (including Cargo commands wrapped by shell or Python) determine feature gates; explicit targets also enforce `required-features`. Omitted `platforms` claims all three supported OSes: Linux, macOS and Windows. Unknown predicates, unresolved tests and unsupported command forms are errors, not exemptions. Python command tests have no Rust compilation gates, but their named methods must exist. Literal bare `bash` subprocess launches anywhere in the Python test file (including shared helpers) reject a Windows claim: the outer native launcher cannot fix nested launches that resolve to WSL. This is deliberately conservative at file scope, not a Python call graph or a proof of runtime portability; computed commands, imported helpers and missing third-party modules still need native replay. Operator fixtures with POSIX paths, locks and shell shims restrict their rows to Linux/macOS.

Windows process cleanup diagnostics use libtest's captured stderr and also append to `target/rig-survivors/<sanitized-test-name>.txt` (or `$CARGO_TARGET_DIR/rig-survivors/` when set). Each record includes the original test name and test process ID; survivor records retain the image, PID, parent PID and job membership. CI unit/integration jobs and Windows mutation shards upload these dumps even after failures, since passing libtest output hides captured diagnostics. Windows mutation artifacts now share the `target/` root: report JSONs are under `mutations/` and dumps under `rig-survivors/`. A missing dump means no diagnostic was recorded, not proof that every descendant exited. The privileged-token ACL observation is explicitly ignored with a reason under libtest; running it with `--ignored` must actually reach `PermissionDenied` or fail. Injected ACL-inspection failures remain the automated refusal proof on elevated runners.

Both the whole-catalogue check and its planted-fixture regression tests run through `scripts/script-suites.sh`, shared by CI's script job and the local gate. Each mutation workflow also requires its own Linux preflight job before launching replay shards, so an invalid Windows claim is rejected before native replay. The check does not establish native replay coverage or validate runtime prerequisites.

The report verifier lives in [`scripts/verify-windows-mutations.py`](../scripts/verify-windows-mutations.py). Its `--self-test` mode runs through `scripts/script-suites.sh`, shared by CI and the local `scripts/gate.sh` entry point, and before mutation replay in the Windows job. Its Windows-only catalogue controls run named tests directly from the same file:

| Row ID | Expected failing test |
| --- | --- |
| `windows-mutations-require-complete-report` | `CoverageTests.test_rejects_filtered_eligible_row` |
| `windows-mutations-reject-eligible-skip` | `CoverageTests.test_rejects_skipped_eligible_row` |
| `windows-mutations-report-observed-red` | `CoverageTests.test_requires_observed_expected_red_test` |
| `windows-mutations-pin-shard-rule` | `CoverageTests.test_pinned_shards_sort_before_platform_filtering` |
| `windows-mutations-pin-platform-rule` | `CoverageTests.test_pinned_platform_rule` |
