# Scripted historian for THALAMUS rigs

A model-free responder intended to answer ck-mc's historian prompt with valid
historian output, allowing a rig with no historian model to exercise folding,
view creation and native markers. These are intended downstream effects, not
an end-to-end result established by the reference pairs. The broca real-provider
run against the staged b2bc512a build stopped during reverse reads, before those
effects could be observed.

## What is here

| File | What it is |
|---|---|
| `scripted-historian.ts` | The reference responder. Plain TypeScript, no imports. `scriptedHistorianResponse(prompt)` returns the response text, or `null` when the text is not a historian prompt. |
| `GRAMMAR.md` | The prompt grammar the responder parses, and everything a response must satisfy to pass ck-mc's validation. Written for a port. |
| `pairs/pair-{1,2,3}.prompt.txt` | Three prompts, byte for byte as ck-mc's async-lane assembler builds them: three consecutive chunks (raw ordinals 1-5, 6-8, 9-13) of one 14-message coding session. Pairs 2 and 3 carry the earlier compartments as `<session_references>`. |
| `pairs/pair-{1,2,3}.response.xml` | The exact response the responder gives to each prompt. |
| `check-pairs.ts` | `bun check-pairs.ts`: the TypeScript responder reproduces every recorded response byte for byte. |

The same pairs are committed in the ck-mc repository at
`crates/mc-module/testdata/scripted-historian/`. The mc-module lib test
`tests::scripted_historian_pairs::scripted_historian_pairs_validate_and_fold_three_consecutive_chunks`
(no Bun, no network) builds the three prompts through ck-mc's real provider
assembler (`assemble_provider_historian_firing`, the one the async lane uses),
requires them to equal the recorded prompt files byte for byte, runs a Rust
port of this responder on them, requires its answers to equal the recorded
response files byte for byte, validates each answer with ck-mc's own
`validate_historian_output`, and stores the validated compartment so the next
fold starts after it. A second test,
`scripted_historian_unprocessed_from_cases`, pins the `<unprocessed_from>`
cases in `GRAMMAR.md` against the same validator.

A Rust port is checked the same way as `check-pairs.ts`: for each pair, its
answer to `pair-N.prompt.txt` must equal `pair-N.response.xml` byte for byte.
The Rust port inside the mc-module test (`scripted_historian_response`) is a
working example of one.

## How the call reaches the rig

Under the async compaction lane, ck-mc does not call a model itself. It sends
`session.send` to the session's own runner (the runner route the conversation
was set up under), with:

- `system`: ck-mc's historian system prompt (`HISTORIAN_SYSTEM_PROMPT`, the
  file `crates/mc-module/testdata/historian-system-prompt.txt`, plus a
  content-language section when one is configured). The responder ignores it.
  If the rig needs to recognise historian sends, the system prompt begins
  `# Historian\n\nYou are Historian — the hippocampus of a long-running coding
  agent.` (at `67e09b9a`; check the file at your pinned ck-mc commit), and the
  `prompt` always contains `<new_messages>\n\nMessages `.
- `prompt`: the user message. This is what `pairs/*.prompt.txt` record and
  what the responder parses.
- `model`: `{provider, model}` split from the configured historian model
  string at its first `/` (see the rig config below), `tools: []`, a
  `max_output_tokens`, `parent` (the conversation's session) and
  `stop_when.max_steps`.

The runner's scripted provider answers that send with the responder's text as
the assistant reply (one text part). ck-mc reads the reply, validates it and
publishes the compartment.

Source: `crates/mc-module/src/historian_producer.rs` (`session_send_request`,
`for_session_runner`), `crates/mc-module/src/historian_chunk.rs`
(`AssembledHistorianFiring::as_fire_request`: `system` is the historian system
prompt, `prompt` the assembled prompt), `crates/mc-module/src/historian_runner.rs`
(the runner lane picks the session runner unless Broca is configured).

## Rig config: make ck-mc fire the historian early

Two settings live in ck-mc's user-tier config,
`$XDG_CONFIG_HOME/cortexkit/magic-context.jsonc` (model choice is read from the
user tier only; a project-tier `historian` model is ignored):

```jsonc
{
  // Required. With no historian model the chain is empty and ck-mc never fires
  // the historian (no-fire cause "no_models"). Any "provider/model" string works;
  // the runner receives it split at the first "/" and should route it to the
  // scripted provider.
  "historian": { "opencode": { "model": "scripted/historian" } },

  // Fire early. The proactive trigger fires once context usage reaches this
  // percentage minus 2 points; the default is 65.
  "execute_threshold_percentage": 20
}
```

For an OpenCode 2 session the `opencode` block is read too; Pi reads
`historian.pi`, OMP `historian.omp` falling back to `historian.pi`.

And on the rig's side, in what it sends ck-mc:

- **A small context window.** Usage is the advance notice's `est_tokens` over
  its `window` (or Setup's `context_window`). A smaller window reaches the
  threshold with fewer tokens: with `context_window: 32000` and a 20%
  threshold the proactive trigger arms at 18% usage, about 5,800 estimated
  tokens.
- **Enough history behind the protected tail.** Besides usage, the proactive
  trigger needs a non-empty eligible head (messages older than the protected
  tail) that is "meaningful": at least 12 messages, or at least 6,000
  estimated tokens. The protected tail is a few thousand tokens at these
  settings, so after roughly 15-20 short turns (or fewer long ones) the
  historian fires.
- Independently of usage, a tail-size trigger fires once the eligible head
  reaches three times the trigger budget. The trigger budget is derived, not
  configurable: 5% of `context_window x threshold`, clamped to 5,000-50,000
  tokens, so this path needs at least 15,000 eligible tokens. The proactive
  path above is the early one.

Defaults that already suit the rig: the minimum chunk size is 0
(`DEFAULT_HISTORIAN_MIN_CHUNK_TOKENS`), and the chunk budget is 25% of the
historian context limit (32,000 tokens at the default 128,000), far more than a
rig session needs, so a chunk normally runs from the first unsummarized
message to the protected tail.

Sources: `crates/mc-module/src/boundary.rs` (`check_compartment_trigger_with_index`,
`derive_trigger_budget`, `derive_protected_tail_token_target`),
`crates/mc-module/src/config.rs` (`DEFAULT_EXECUTE_THRESHOLD_PERCENTAGE`,
`execute_threshold_at`, `derive_historian_chunk_tokens`),
`crates/mc-module/src/providers/historian_config.rs` (`resolve`, `attempts`),
`crates/mc-module/src/providers/async_provider.rs` (`schedule_async_historian`:
the historian settings and usage the async lane evaluates with),
`crates/mc-module/src/lib.rs` (`prepare_historian_fire`: the `model_chain_missing`
and `no_models` refusals).

These config values come from reading the code at `67e09b9a`; they were not run
in a live rig here. If the historian does not fire, ck-mc records the no-fire
cause (`trigger_false` with eligible/bar/protected numbers, `no_models`,
`model_chain_missing`); that is the first thing to read.

## Limits of the responder

- One compartment per chunk, covering it fully. That is always valid and never
  hits discard-last (which withholds a last compartment only when there are two
  or more). It does not exercise partial coverage; `GRAMMAR.md` describes that
  shape for a responder that wants to.
- It emits no facts, events, observations or primer candidates. All are
  optional.
- If ck-mc ever sends a repair prompt (only after an invalid answer, so not with
  this responder), the repair prompt starts with the original prompt, and the
  responder answers it the same way.
