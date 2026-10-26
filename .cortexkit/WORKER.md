# Build and test guidance

Start with `CONTRIBUTING.md` and the public contracts under `docs/`.

## Cargo verification

Run the filtered tests for what you touched. For a bug fix, add a test that fails
without the fix and show that once; deliver. The integrating agent runs the full
suites once per batch.

Run independent checks serially, and use `--locked` to keep dependency resolution
read-only. Useful targeted commands include:

```sh
cargo fmt --all -- --check
cargo check -p <package> --all-targets --locked
cargo test --locked -p <package> <filter>
```

`scripts/quickcheck.sh <package> ...` runs formatting, package clippy, library
and binary tests, and doctests. `scripts/gate.sh` runs the workspace gate.
Use `scripts/gated-commit.sh <message-file> [package ...]` to commit only after
its checks pass. Set `CARGO_BUILD_JOBS` to limit concurrent compilation.

Commit only through `scripts/gated-commit.sh`, never a plain `git commit`.

## Integration prerequisites

Some integration tests need built external binaries or a mock provider server.
Set `SUBC_CORE_BIN` and `FAKE_AFT_STUB_BIN` for daemon/tool-provider tests;
set `CK_CLAUSTRUM_BIN`, `CK_AUTH_BIN`, and `CK_CLAUSTRUM_CHECKOUT` for vault tests.
If the checkout has a `.test-rigs/` folder, these binaries are staged there
(`ck-subc`, `fake-aft-stub`, `ck-claustrum`, `ck-auth`, `claustrum-checkout`).
Install the mock provider with `npm ci` when running aimock tests.

Use `BROCA_REQUIRE_SUBC=1`, `BROCA_REQUIRE_VAULT=1`, and
`BROCA_REQUIRE_AIMOCK=1` for the corresponding verification lanes so missing
prerequisites fail instead of returning a passing skip. A test that skips a
required external dependency is not evidence for that integration contract.

Use Linux for portable compilation, linting, and unit tests.
Run tests requiring native macOS facilities on macOS. Cross-platform checks
include clippy with `--target x86_64-pc-windows-gnu`; native Windows tests are
needed to verify Windows-only behavior.
