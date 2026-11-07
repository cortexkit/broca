# Working in broca

Standing rules for every worker in this repository. A brief can tighten them but never loosen
them. Start with `CONTRIBUTING.md` and the public contracts under `docs/`.

## Build and test on the local machine

- Run builds, tests and lint locally. Start every cargo command with
  `nice -n 10 env CARGO_INCREMENTAL=0`, so builds yield to the person using the machine. Never
  clear the compiler wrapper: if your shell sets `RUSTC_WRAPPER` or `CARGO_BUILD_RUSTC_WRAPPER`
  empty, `unset` them first.
- Use the build server only for what needs its platform: Windows-only behaviour
  (`runon: "windows"`), or a test that runs only on Linux (`runon: "linux,8c"`). A remote Linux
  job can't change files git tracks, so edit tracked files locally, and name any file a remote job
  regenerates in `return_paths`.
- Never run the native kill-conformance, real-daemon or crash-harness release lanes yourself. The
  integrating agent runs them once per merged batch. List the native tests your change needs.

## Test only what you changed

The integrating agent runs every full suite once over each batch of merged deliveries. Your
testing proves your change, in minutes, not hours:

- Run only the tests for the code and behaviour you changed, with
  `cargo nextest run --locked -p <crate> -E '<filterset>'` plus the features and
  `BROCA_REQUIRE_*` variables the test needs. A bug fix adds a test and shows it failing once
  without the fix.
- Never run a whole crate's tests, an integration suite, `scripts/gate.sh`,
  `scripts/linux-preflight.sh` or `scripts/prepush.sh`.
- A failure that looks unrelated: rerun that one test by name on your base commit, never a suite.
- A filtered run longer than about 15 minutes means the filter is too broad. Narrow it.
- Don't run clippy or fmt separately while iterating: the commit gate runs both once. When you
  change `cfg` code, also run clippy once with `--target x86_64-pc-windows-gnu`.
- Keep the normal build directory. Never set a separate `CARGO_TARGET_DIR` for a run.
- Update any pin, golden, manifest count or lockfile check your change covers.

Commit only through `scripts/gated-commit.sh`, never a plain `git commit`. Stage the change and
run it: it runs fmt, clippy and the unit tests of the crates it derives from your diff, locally.
That is the only crate-wide run you make. A root manifest or lockfile change adds `broca-subc`.

Report each command, its pass and fail counts, and every failing test name.

## Pre-approved, so don't ask

- Updating a test your intended change breaks, keeping what the test checks.
- Regenerating a golden or fixture when the new output differs only by the cause you state.
- Mechanical compile fixes outside your fence: a new field initialised to `None` in another
  test's literal, a new trait method forwarded through every wrapper (the crash harness's
  included), or a match arm for a new record type handled explicitly and covered by a test.
- Adding a new log feature name to every pinned feature list, kept sorted.
- Test-only seams behind the existing features (`test-clock`, `test-attachments`,
  `fs-override`, `writer-id-faults`), never reachable from a shipping build.
- Replacing a sleep or a private deadline with `broca_test_support::wait_until` or
  `wait_until_sync` under the shared hang guard.

## Never

- Change behaviour or bytes on today's (legacy) state roots unless the brief says so.
- Add a WAL record, a record field or a must-understand log feature without asking. New
  features ship reader first, one release ahead of any writer.
- Loosen an assertion, raise a timeout or add a retry to get green. Find the cause.
- Add mutation work: no `mutations.toml` rows, no planted-break proofs.
- Open, copy or write the live stores under `~/.local/share/cortexkit/`. Work on copies you were
  given.
- Force-add ignored `.cortexkit/` files, or put task or slice labels in code comments.

## Ask when

A decision changes behaviour, a wire or log contract, what the model sees, prompt-cache bytes,
or what an older broca can roll back to. Otherwise decide, and record the decision in your
report.

## Integration prerequisites

Some integration tests need built external binaries or a mock provider server. If the checkout
has a `.test-rigs/` folder, these are staged there (`ck-subc`, `fake-aft-stub`, `fake-surface`,
`ck-claustrum`, `ck-auth`, `claustrum-checkout`); point `SUBC_CORE_BIN`, `FAKE_AFT_STUB_BIN`,
`FAKE_SURFACE_BIN`, `CK_CLAUSTRUM_BIN`, `CK_AUTH_BIN` and `CK_CLAUSTRUM_CHECKOUT` at them. Install
the mock provider with `npm ci` when running aimock tests.

Use `BROCA_REQUIRE_SUBC=1`, `BROCA_REQUIRE_VAULT=1` and `BROCA_REQUIRE_AIMOCK=1` in the
corresponding lanes, so a missing prerequisite fails instead of passing as a skip. A test that
skips a required dependency is not evidence for that integration.
