# broca

broca is a durable LLM agent-loop runner that runs as a module of the
subconscious daemon. It calls a model, dispatches requested tools and feeds their
results back until the run stops. A write-ahead log records the run so it can
resume after a restart without repeating a tool call that may have had a side
effect.

## Repository map

| Crate | Role |
|---|---|
| `broca-protocol`, `broca-wire` | Canonical types and client-facing JSON contracts. |
| `broca-wal` | WAL records, framing, storage, archives and verification. |
| `broca-core` | Run loop, replay, checkpoints, import, billing and leases. |
| `broca-module` | Session actors, registry, handlers and subscribers. |
| `broca-module-serve` | `ck-broca` module binary and `broca-session` CLI. |
| `broca-subc` | Daemon connection, tool/transform routing and vault client. |
| `broca-provider` | Provider wire families, rendering, normalization and transport. |
| `broca-catalog`, `broca-fetch-plan` | Catalog resolution/cache and first-send fetch-plan admission. |
| `broca-ck` | Message codec for prompt-reduction transforms. |
| `broca-store` | Materialized session store and export index. |
| `broca-owner-only` | Private-state file permissions and validation. |
| `broca-diagnostics` | Blocking-job and egress instrumentation. |
| `cortexkit-chatgpt-policy` | Shared ChatGPT/Codex account-id, refusal and quota-header policy. |
| `ck-session-import`, `ck-import` | Log import readers and CLI. |
| `broca-run`, `broca-bench`, `broca-replay` | Standalone runner, benchmarks and replay fixtures. |
| `broca-test-support`, `broca-crash-harness` | Test support and crash-cut recovery checks. |

See [Architecture](docs/architecture.md) for the data flow and invariants.

## Build and test

Use Rust 1.96 or newer. Building broca and running library tests need only the
Rust toolchain; no sibling source checkout or Node installation is needed:

```sh
cargo build --locked -p broca-module-serve
cargo check --workspace --all-targets --locked
cargo test --locked -p broca-provider --lib
```

The build produces `target/debug/ck-broca` and `target/debug/broca-session`.
`ck-broca` is supervised by subconscious, not a standalone daemon. The
[module surface](docs/module-surface.md) describes the routes it serves.
For changes to a package, [scripts/quickcheck.sh](scripts/quickcheck.sh) runs
formatting, clippy, library/binary tests and doctests for the named packages.
The [operator script suites](scripts/script-suites.sh) use Python 3.11+ and the
script tools they exercise, separately from Cargo tests.

The full [gate](scripts/gate.sh) also needs `cargo-nextest`, Python 3.11+, Node
and the pinned mock provider installed with `npm ci`. Real-daemon and vault
integration tests need binaries built from the public companion repositories:

- subconscious: https://github.com/cortexkit/subconscious
- claustrum: https://github.com/cortexkit/claustrum

With those checkouts alongside this one, build the fixtures and run the gate:

```sh
npm ci
cargo build --locked --manifest-path ../subconscious/Cargo.toml -p subc-core --bins
cargo build --locked --manifest-path ../claustrum/Cargo.toml -p credentials-module --bins

export SUBC_CORE_BIN="$PWD/../subconscious/target/debug/ck-subc"
export FAKE_AFT_STUB_BIN="$PWD/../subconscious/target/debug/fake-aft-stub"
export CK_CLAUSTRUM_BIN="$PWD/../claustrum/target/debug/ck-claustrum"
export CK_AUTH_BIN="$PWD/../claustrum/target/debug/ck-auth"
export CK_CLAUSTRUM_CHECKOUT="$PWD/../claustrum"
export BROCA_REQUIRE_SUBC=1 BROCA_REQUIRE_VAULT=1 BROCA_REQUIRE_AIMOCK=1
scripts/gate.sh
```

Use `.exe` binary names on Windows. The `BROCA_REQUIRE_*` settings make missing
fixtures fail instead of silently skipping the corresponding tests. Tests of
native OS behavior need that OS; Linux is suitable for portable checks. Live
provider tests require credentials and are not part of the default gate.

## Documentation

- `docs/` — public contracts and design notes, starting with the [architecture](docs/architecture.md).
- [CONTRIBUTING.md](CONTRIBUTING.md) — contribution workflow and verification.
- [SECURITY.md](SECURITY.md) — private reporting and local trust boundaries.
- [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md) — adapted code and vendored data notices.

## License

broca is licensed under the [MIT License](LICENSE). Code and data adapted from other
projects keep their own licenses; see [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).
