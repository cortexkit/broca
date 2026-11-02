# Working in broca

Standing rules for every worker in this repository. A brief can tighten them but never loosen
them. Start with `CONTRIBUTING.md` and the public contracts under `docs/`.

## Build and test remotely

- Run builds, tests and lint on the build server (`runon: "linux,8c"`, or `linux,16c` for
  whole-workspace jobs). Any job that runs `broca-crash-harness` tests also takes `linux,16c`:
  the harness keeps every crash image in memory, and one filtered run used 16.5 GiB at 8c,
  over the build server's coming limit of 2 GiB per vCPU. Never go below 8c for the commit
  check (`scripts/linux-quickcheck-attest.sh`). On the local machine, run only what needs it,
  at `nice -n 10`.
- Remote jobs that write logs start with `mkdir -p target/scratch &&`.
- A remote Linux job can't change files git tracks. Its writes fail with "Operation not
  permitted", and before that rule they were silently lost when the next job reset the tree.
  So edit tracked files locally with the edit tools, never inside a job: no `python3`, `sed -i`
  or `perl -pi` edits, no `cargo fmt` (the commit check runs `--check`), and no `git show` or
  `git checkout` restores. For a red-first proof, revert the fix locally, run the test remotely,
  then reapply it. To regenerate goldens, captures or fixtures remotely, name every file the job
  rewrites in `return_paths` so they come back to your checkout, or regenerate locally.
- Never run the native kill-conformance, real-daemon or crash-harness release lanes yourself. The
  integrating agent runs them once per merged batch. List the native tests your change needs.

## Test only what you changed

The integrating agent runs every full suite once over each batch of merged deliveries. Your
testing proves your change, in minutes, not hours:

- Run only the tests for the code and behaviour you changed, on the build server, with
  `scripts/linux-test.sh '<nextest filterset>'`. It uses the commit attestation's build, so the
  final attestation recompiles nothing new. A bug fix adds a test and shows it failing once
  without the fix.
- Never run a whole crate's tests, an integration suite, `scripts/gate.sh`,
  `scripts/linux-preflight.sh` or `scripts/prepush.sh`.
- A failure that looks unrelated: rerun that one test by name on your base commit, never a suite.
- A filtered run longer than about 15 minutes means the filter is too broad. Narrow it, rather
  than repeatedly waiting up to 30 minutes on a background build job (`bash_watch`).
- Don't run clippy or fmt separately while iterating: the attestation runs both once. When you
  change `cfg` code, also run clippy once with `--target x86_64-pc-windows-gnu`.
- Keep the normal build directory. Never set a separate `CARGO_TARGET_DIR` for a run.
- Update any pin, golden, manifest count or lockfile check your change covers.

Commit only through `scripts/gated-commit.sh`, never a plain `git commit`. To commit on a Linux
attestation: stage the change, run `bash scripts/linux-quickcheck-attest.sh --output - <crates>`
remotely once, write its five-line record to `$(git rev-parse --git-path
broca-linux-quickcheck-attestation)` after checking its tree equals `git write-tree`, then run the
gated commit. The attestation runs fmt, clippy and the touched crates' unit tests on the warm
build; that is the only crate-wide run you make. The crate list must match the set the gate
derives from the diff. A root manifest or lockfile change adds `broca-subc`.

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
