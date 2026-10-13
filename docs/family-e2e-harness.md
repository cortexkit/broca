# Provider-family + error-mode e2e harness

This suite exercises the **full provider stack** — render → SSE/normalize → error
classify — by driving the real `run_loop` against a mock LLM server (aimock) with a
deterministic in-process tool plane. It is a **different level** than the conformance
tests (which check render/normalize in isolation against golden bytes): here the whole
provider stack drives the loop with real wire responses and error injection.

**No subc, no AFT.** The variable under test is the provider response shape; subc/AFT
are provider-agnostic, so they are not booted (that whole-spine proof lives in the
cortexkit-e2e M1/M2 tests). These tests are pure `broca-provider` + node + aimock.

## Prerequisite: install aimock

aimock is a pinned devDep (`package.json` at the repo root, `@copilotkit/aimock`
version-locked). `node_modules` is gitignored, so in a fresh checkout/worktree run
**`npm ci` from the repo root first** — otherwise the rig finds no aimock and the
tests skip (a visible skip, never a false pass).

## The rig (do not modify it)

The shared rig is `crates/broca-provider/tests/family_e2e/mod.rs`. It is owned centrally
— **do not edit it**. If a test genuinely needs a rig capability it lacks, STOP and
report what's missing rather than working around it in your test file (a per-test
workaround is the seed of divergence across the suite).

Entry point:

```rust
mod family_e2e;
use family_e2e::{run_family_e2e, RunSpec, RunOutcome};

// run_family_e2e(family, frozen_config, spec) -> Option<RunOutcome>
//   None  => aimock/node absent — the caller returns (skips cleanly)
//   Some  => the run completed; assert on the outcome
```

- `RunSpec::new(fixtures_json)` — the aimock fixtures document (a JSON string, see
  below). Builders: `.prompt(text)`, `.tool(name, canned_result)` (offers a tool AND
  registers its deterministic StubToolPlane result in one call).
- `RunOutcome` accessors: `.summary` (`RunSummary { reason, steps, usage }`),
  `.tool_call_names()`, `.tool_result_texts()`, `.final_text()`, `.first_error()`
  (`Option<ProviderError>` with a `.class: ErrorClass`), and `.events` (the full
  `Vec<OutputEvent>` for custom assertions).

The rig points each family at aimock via the per-family base_url rule automatically
(bare host for most, `host/v1beta` for Gemini) — your test never sets base_url.

Family frozen-config helpers (from `broca_provider::families`):
`anthropic_frozen_config(model)`, `openai_chat_frozen_config(model)`,
`openai_responses_frozen_config(model)`, `gemini_frozen_config(model)`,
`bedrock_frozen_config(model)`. Pass the family struct alongside:
`AnthropicMessages`, `OpenAiChat`, `OpenAiResponses`, `Gemini`, `BedrockConverse`.

## aimock fixture schema

A fixtures document is `{"fixtures": [ {match, response, <error-knobs>}, ... ]}`.
**First match wins; order matters; `{}` is a catch-all.** aimock encodes the same
logical response into each family's native wire shape, so one fixture drives any
family.

**match keys:** `hasToolResult` (bool — the 2-step driver: false → first turn, true →
after a tool result), `turnIndex`, `toolName`, `toolCallId`, `userMessage`,
`systemMessage`, `model`, `sequenceIndex` (nth occurrence). `{}` matches anything.

**response forms:**
- `{ "content": "text" }` — assistant text.
- `{ "toolCalls": [ { "name": "...", "arguments": { ... } } ] }` — a tool call
  (`arguments` is an object, auto-stringified, or a JSON string).
- Anthropic thinking (on a text OR toolCalls response): `"reasoning": "plaintext
  thinking"`, `"reasoningSignature": "<sig>"` (the cryptographic signature replayed on
  the thinking block), `"redactedThinking": ["<opaque-data>", ...]` (replayed as
  `redacted_thinking` blocks).
- Error: `{ "error": { "message": "...", "type": "...", "code": "..." }, "status":
  500, "retryAfter": 2 }` — `status` sets the HTTP code, `retryAfter` the `Retry-After`
  header on a 429.

**Per-fixture error knobs (deterministic — preferred over global chaos):**
- `"status": <code>` on an error response — the HTTP status to return.
- `"retryAfter": <secs>` — the `Retry-After` header value (429).
- `"truncateAfterChunks": <n>` — cut the stream after n chunks (no terminal event /
  mid-tool-call) → exercises normalizer finalize-on-EOF.
- `"disconnectAfterMs": <ms>` — destroy the connection mid-stream → transport error.
- `"chaos": { ... }` — probabilistic injection (reserve for resilience tests only;
  prefer the deterministic knobs for taxonomy assertions).

## Error taxonomy — what to assert

The loop does **not** retry — model-call retry/circuit-breaking lives in the provider
module by design, not the loop. So an error-mode test asserts:
1. the wire error **classifies** to the right `ErrorClass` (`.first_error().class`),
   and
2. the run terminates `RunFinishReason::Error` carrying it.

`ErrorClass` (from `broca_protocol`): `Transient` (500/502/503/529/429), `Auth`
(401/403), `Permanent` (400/404), `ContextOverflow` (**not produced today** — out of
scope), `ProviderUnavailable`. The HTTP→class mapping lives in
`crates/broca-provider/src/transport.rs::http_error`. Assert the class + terminal
`Error`, never "retry fires".

## The bar every test file must clear

Each test file is a new `crates/broca-provider/tests/<name>_e2e.rs` (disjoint — Rust
auto-discovers it, no Cargo.toml change). It must:
1. **build** clean: `cargo build -p broca-provider --features live --tests`.
2. **clippy** clean (force a recompile so a cached green doesn't hide a warning):
   `touch` the file, then `cargo clippy -p broca-provider --all-targets --features live`.
3. **rustfmt** clean on your own file: `rustfmt --edition 2021 --check <file>`.
4. **actually run green against live aimock** — not just compile:
   `npm ci` (once), then
   `cargo test -p broca-provider --features live --test <name>_e2e`.
   A test that skips ("aimock absent") is NOT a pass — verify it genuinely ran.

Assertions must key on real content (a specific decoded string, a specific
`ErrorClass`), never on "didn't error" — a CRC-passing-but-empty decode or a
silently-skipped test must not read as green.
