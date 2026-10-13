# Dependency features across package selections

Cargo's resolver 2 unifies features within a build, not across separate package
selections. A workspace build can therefore enable more features on shared crates
than a later `-p` build, producing different fingerprints and recompilation.

The workspace manifests share the normal feature sets for Tokio, serde_json,
tokio-util, futures, async-trait, sha2 and the pinned tungstenite clients. Members
inherit those definitions rather than adding their own features. Optional provider
dependencies remain optional, behind `live`; inheritance does not enable them.

Tokio's `test-util` is **dev-only**, declared identically in every member. It must
not enter the production graph: its paused-clock support adds synchronization to
`Instant::now()` even when the program never pauses time. Normal builds and test
builds may still differ from each other, since tests activate dev-dependencies.

The replay clock uses rustix's safe absolute `CLOCK_MONOTONIC` API on Unix, with
only the `time` feature requested and defaults disabled. This preserves
`#![forbid(unsafe_code)]` without substituting a process-relative clock. The
non-Unix fallback remains process-relative; the cross-process bench is Unix-only.

## Inspecting the production graph

Cargo tree includes dev-dependencies by default. To inspect only the binary's
normal/build graph, use:

```sh
cargo tree --locked -p broca-module-serve -e features,no-dev -i tokio --target all
```

`broca-module-serve` owns the `ck-broca` binary. This tree must contain no
`tokio feature "test-util"`; a tree including dev-dependencies should contain it.
Similarly, `cargo tree --locked -p broca-provider -e normal,build` without `live`
must remain free of reqwest, rustls, ring, hyper and tungstenite.

`python3 scripts/test-dependency-features.py` checks these graphs, their non-empty
roots, and consistent dev Tokio declarations across workspace members. CI runs it
in the operator script suites after installing the Rust toolchain. It uses Cargo
metadata rather than a Python-version-dependent TOML parser.
