# Agent-run scope writer verification

The broca daemon module advertises `agent-run-scopes/v1` together with
`AGENT_RUN_SCOPES_WRITER_ENABLED`. Only `reserved:basal`, the Basal module identity
attested by the daemon from its supervised launch credentials, may drive an
agent-run scope, and both `agent_id` and `run_id` are frozen. Declared tools
and planned tool items are forbidden. A wrong carrier or driving owner receives
`scope_owner_mismatch`; changed identifiers receive `scope_run_mismatch`.

## Admission before any write

Opening an actor initializes a WAL lineage header even when no episode is
admitted. `LineageRegistry::send` therefore checks the run-scope sender and tools
before routing to an actor when the read-only ownership summary establishes that
the session has no episodes, using the existing
read-only ownership preflight. Unknown/unreadable history is not treated as
fresh. Existing sessions retain the actor's authoritative frozen-ownership check.

The real-daemon tests in `tests/scope_real_daemon_e2e.rs` launch a fake Basal module
with an attested nonce, use stamped binds and a loopback model, and check:

- accepted scope/principal on `RunStarted`, the agent-run WAL feature, and the
  exported `agent_id`/`agent_run_id` alongside broca's distinct episode `run_id`;
- tools and plan tool items on fresh and completed sessions;
- wrong carrier and driving owner on fresh and completed sessions;
- changed run and changed agent on the completed session;
- no WAL file for fresh refusals, byte-identical WAL for existing refusals,
  and unchanged run-index rows and export fact bytes for every refusal;
- ordinary exported facts omit both new labels and reserialize to their stored
  bytes. The daemon-free billing and export-index fixture tests additionally pin
  the pre-agent-run bytes and the unchanged `fact_id` hash.

Run the real-daemon target locally:

```sh
BROCA_REQUIRE_SUBC=1 BROCA_REQUIRE_AGENT_RUN_READER=1 \
BROCA_AGENT_RUN_READER_BIN=/path/to/staged-reader-v0.3.194/ck-broca \
CARGO_BUILD_JOBS=2 RUST_TEST_THREADS=2 \
SUBC_CORE_BIN=/path/to/ck-subc \
FAKE_AFT_STUB_BIN=/path/to/fake-aft-stub \
cargo test --locked -p broca-module-serve --test scope_real_daemon_e2e
```

## Rollback

`released_reader_reads_agent_run_writer_session_and_refuses_advancement` starts
this writer, completes a run-scoped send, stops it, and starts an independently
built reader against the same state. It pins reader version `ck-broca 0.3.194`
and checks that the reader is not the writer's bytes, reading the writer through
`broca_test_support::ckdev_binary`. The reader returns the identical transcript
and completed status, refuses another send with `agent_run_scopes_unsupported`,
leaves WAL and export bytes untouched, and still serves reads after the refusal.
The rollback send uses an owner's route opened without a scope selector because the reader deliberately
lacks the capability required for daemon run-scoped routing.

Stage the independently released `ck-broca 0.3.194` binary before running this target; the test
receives it through `BROCA_AGENT_RUN_READER_BIN` and does not build it from a tag in this repository.
Rollback checks use staged release binaries so they do not depend on this repository's historical tags.

## Wire compatibility

The serve-path test `production_executor_installs_hooks_and_describes_ordered_phases`
compares the served role description with fixtures in
`crates/broca-wire/tests/golden/`.

`agent_run_scopes_unsupported` and its pinned wire bytes remain for older
callers/readers. This writer has no production path that produces it: scope
admission, the work-scope requirement, the actor fallback and the direct-core
run guard all produce it only with agent-run writing disabled. Encoding an
explicitly constructed refusal remains possible, but no writer send constructs
one.
