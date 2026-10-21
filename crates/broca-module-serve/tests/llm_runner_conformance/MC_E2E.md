# Real Magic Context async-compaction test

This opt-in lane runs `ck-broca` and the staged `ck-mc` under a private
`ck-subc`, backed by `cortexkit-bus-testkit`'s private NATS. The conversation
uses the existing conformance scripted model. Historian requests use the
model-free responder pinned against `../fixtures/scripted-historian/pairs/`.
No external model or production daemon is used.

The pinned build is `ck-mc 0.49.13`, commit
`b2bc512a1eecfd39273589023b310ad850b4bf04`, fences `context.db=102 store.db=74`.
The test authenticates and copies both inputs before launching anything:

- Binary SHA256: `8eeeaae339d2db3d1942f5b6b151ba9735a9b202cb59a598a99f41104ff9bfdc`
- Pristine context fixture SHA256: `c84e26b0d4e394dcc2288dc9290539633e890c0c564bd7dba7309d5e5edd2060`

Use only scratch copies of the supplied inputs. This native lane needs the
staged platform's executable format, `nats-server`, `lsof`, and the daemon rigs.
For the supplied macOS build, run on macOS:

```sh
SUBC_CORE_BIN="$PWD/.test-rigs/ck-subc" \
FAKE_AFT_STUB_BIN="$PWD/.test-rigs/fake-aft-stub" \
BROCA_REQUIRE_SUBC=1 \
BROCA_MC_E2E_BIN="/absolute/scratch/ckdev-mc.b2bc512a" \
BROCA_MC_E2E_DB="/absolute/scratch/ckdev-mc.b2bc512a.context-v102.db" \
BROCA_MC_E2E_ARTIFACT_DIR="$PWD/.cortexkit/internal-docs/investigations/mc-e2e-new-run" \
CARGO_BUILD_JOBS=4 \
cargo test --locked -p broca-module-serve --features test-clock \
  --test llm_runner_conformance mc_e2e::real_mc_async_compaction_end_to_end -- --nocapture
```

The artifact directory must be fresh. The lane retains the report, WAL records,
bus observations, private module logs and state, and (if reached) the request
bytes on both sides of SIGKILL. It verifies open MC database paths with `lsof`.
It pauses bus intake after receipt commit to inspect the durable receipt before
ACK, then releases it and checks the broker ACK and applied view. It checks
WAL-backed advance sequences against actual bus messages, a smaller rendered
request containing the real view, and a real process kill followed by identical
request bytes. It does not force a ceiling by modifying MC's policy.

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

The portable `mc_e2e::historian_matches_mc_reference_pairs` test runs without
MC. The original TypeScript reference can also be checked with
`bun check-pairs.ts` in `tests/fixtures/scripted-historian/`.
