# Working in broca

Standing rules for every worker in this repository. A brief can tighten them but never loosen
them. Start with `CONTRIBUTING.md` and the public contracts under `docs/`.

## Build and test remotely

- Run builds, tests and lint on the build server (`runon: "linux,8c"`, or `linux,16c` for
  whole-workspace jobs). On this Mac, run only what needs macOS, at `nice -n 10`.
- Remote jobs that write logs start with `mkdir -p target/scratch &&`.
- Never run the native kill-conformance, real-daemon or crash-harness release lanes yourself. The
  integrating agent runs them once per merged batch. List the native tests your change needs.

## Gate before you deliver

For the pre-push entry point, platform split and CI parity table, see [`scripts/prepush.sh`](../scripts/prepush.sh) and [`docs/prepush.md`](../docs/prepush.md).

Filtered tests alone are never a gate. Before delivering, run on Linux:

- `cargo fmt --all -- --check`, and clippy with `--all-targets --locked -- -D warnings` for every
  crate you touched. Add `--target x86_64-pc-windows-gnu` when you change `cfg` code.
- The library, binary and integration tests of every crate you touched. A bug fix adds a test
  and shows it failing once without the fix.
- Any pin, golden, manifest count or lockfile check your change covers.

Commit only through `scripts/gated-commit.sh`, never a plain `git commit`. To commit on a Linux
attestation: stage the change, run `bash scripts/linux-quickcheck-attest.sh --output - <crates>`
remotely, write its five-line record to `$(git rev-parse --git-path
broca-linux-quickcheck-attestation)` after checking its tree equals `git write-tree`, then run the
gated commit. The crate list must match the set the gate derives from the diff. A root manifest
or lockfile change adds `broca-subc`.

Report each command, its pass and fail counts, and every failing test name. If a failure isn't
yours, show that it fails the same way on your base commit.

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
