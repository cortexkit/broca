# Real Magic Context async-compaction test

This opt-in lane runs `ck-broca` and the staged `ck-mc` under a private
`ck-subc`, backed by `cortexkit-bus-testkit`'s private NATS. The conversation
uses the existing conformance scripted model. No external model or production
daemon is used.

The conversation runs the plan a worker ships with: Magic Context's tool,
system-text and compaction items all on preset `worker`, the tool and
system-text items with `params: {"compacting": true}`, and `render_tags` on.
A worker compaction writes no summaries, so no historian model is configured
and no summarizer child run can be made. The worker tool preset needs a
daemon-attested scope, so the conformance owner syncs one (listing `broca` as
its carrier) and the conversation's route opens under it.

The lane expects a Magic Context 0.5 build whose worker catalog accepts
`params: {"compacting": true}`; the first one MC staged for it is commit
`f6da7169` of the cortexkit/magic-context repository. The
operator names the binary and its store fixture, with the sha256 MC published
for each. The test checks both checksums and copies both inputs before
launching anything.

Use only scratch copies of the supplied inputs. This native lane needs the
staged platform's executable format, `nats-server`, `lsof`, and the daemon rigs.
For the supplied macOS build, run on macOS:

```sh
SUBC_CORE_BIN="$PWD/.test-rigs/ck-subc" \
FAKE_AFT_STUB_BIN="$PWD/.test-rigs/fake-aft-stub" \
BROCA_REQUIRE_SUBC=1 \
BROCA_MC_E2E_BIN="/absolute/scratch/ckdev-mc" \
BROCA_MC_E2E_BIN_SHA256="<sha256 MC published for the binary>" \
BROCA_MC_E2E_DB="/absolute/scratch/context.db" \
BROCA_MC_E2E_DB_SHA256="<sha256 MC published for the fixture>" \
BROCA_MC_E2E_ARTIFACT_DIR="$PWD/.cortexkit/internal-docs/investigations/mc-e2e-new-run" \
CARGO_BUILD_JOBS=4 \
cargo test --locked -p broca-module-serve --features test-clock \
  --test llm_runner_conformance mc_e2e::real_mc_async_compaction_end_to_end -- --nocapture
```

The artifact directory must be fresh. The lane retains the report, WAL records,
bus observations, private module logs and state, and (if reached) the request
bytes on both sides of SIGKILL. It verifies open MC database paths with `lsof`.

The run checks, in order:

- the first run's frozen plan carries the worker items and params above;
- each later turn waits until the previous run's last advance notice is in the
  WAL and observed on the bus;
- after three tool outputs, the scripted model calls `ctx_reduce` on the `§N§`
  tags of the two oldest, and Magic Context acknowledges it;
- turns with more tool output continue until the advance notice's estimate
  passes 50% of the 128,000-token window (`execute_threshold_percentage` 50);
- Magic Context may publish a view after any advance notice, including the
  first, with or without drops; broca applies such early views as usual and the
  test makes no assumption about them;
- once broca's log has handled every `view_ready` seen so far, the receipt gate
  is armed for the next one, just before the step after the crossing. Magic
  Context then publishes a `view_ready` above the newest view broca has applied.
  If none reaches the gate within the shared hang guard, the test fails naming
  the session ref, the announced versions and any published before the gate;
- with that view's ACK held, broca's store holds a durable receipt for exactly
  that `(session_ref, view_version)`, or its WAL already records a decision on
  it (applied, refused, or passed over for a newer view), since a decision
  clears the receipt; then the broker ACK, the applied view, and that broca
  applied the newest announced version;
- every view broca applied satisfies Magic Context's worker mode-3 drop rules:
  a drop outside the protection window lands on the first view after its
  stamp, a drop inside it may stay held until newer tool output pushes it
  out, no view ever
  brings a dropped part back, and either legal drop shape counts; every
  dropped part is gone from the newest applied view, the stamped outputs are
  no longer sent, and the next model request is smaller than the one before
  it;
- a real process kill mid-step seals the run `interrupted` at boot (the
  crash-orphan seal): resending its send_id reads that terminal back without
  reaching the model, the held step's unfinished reply is nowhere in the
  transcript, and a new send_id starts a new episode whose first request
  renders the held compaction-applied history plus the new user message, with
  the applied view intact and the dropped reads still not sent.

Not checked: views Magic Context hands over during a ride (a rewrite broca asks
for when the request prefix changes anyway), its reminder text, and the context
ceiling beyond Setup recording one.

Without `BROCA_MC_E2E_BIN` this test prints a loud skip, even in the usual
required-daemon lane. Once the binary is named, missing inputs, bad checksums,
missing native prerequisites, startup failures and provider refusals all fail
rather than skip. A failed run is **not** evidence for downstream assertions.
In particular a ceiling refusal with `view_timeout` is correct runner behavior
when no usable view arrives; it is not proof of successful compaction.

## Read-only diagnostic replay

To diagnose intake without sending another conversation turn, point
`BROCA_MC_E2E_REPLAY_ROOT` at a **preserved scratch artifact directory**, never
production state, and run `mc_e2e::replay_preserved_mc_read` in the same target.
The replay starts a fresh private broker and the same private module state,
republishes the last durable advance, and saves raw `session.read` reply bytes
for the conversation's `broca` harness and the distinct `runner` harness. It
asserts that the former has a lineage and the latter is empty. This is a narrow
regression diagnostic for the staged build, not a substitute for the full E2E.
The opt-in replay does not include transport instrumentation; decoded MC
request/route captures require a separately instrumented diagnostic binary.

The portable `mc_e2e::output_tags_reads_the_tags_rendered_on_named_tool_outputs`
and `mc_e2e::compaction_log_reads_fleet_logger_lines` tests run without MC.
