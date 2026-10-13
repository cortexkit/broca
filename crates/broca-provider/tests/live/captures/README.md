# Synthetic live-harness artifacts

All 20 non-empty JSON artifacts are synthetic. The two empty Bedrock placeholders
remain unchanged. These files retain the original response event order, field names
and nesting, usage counters and absent/null/zero distinctions. IDs, prose, timestamps,
fingerprints and opaque signatures are invented. Opaque strings retain their original
length class; they are not valid provider signatures.

The harness in `../../live_common/mod.rs` can still re-record real traffic for local
drift inspection. **Re-recorded captures must be scrubbed before committing.** Response
bodies can echo prompts, and signed thinking can encode stable identifying values even
when no request/auth header is recorded. Replace prose, identifiers and opaque data
in both `raw_response_text` and `normalized_events`, not just the visible response text.

Offline consumers live in `../../it/synthetic_capture_replay.rs`. Each test checks the
entire ordered display/control snapshot, including canonical content and terminal
usage, feeding small byte chunks through a fresh normalizer for each model step.
After intentionally replacing response bytes, regenerate the synthetic event snapshots
with `BROCA_UPDATE_SYNTHETIC_CAPTURE_EVENTS=1 cargo test --locked -p broca-provider
--test it synthetic_capture_replay::`, then run again without the variable. This is
not a scrubber and must not be used to bless unreviewed live traffic.

The table's tests are prefixed `synthetic_capture_replay::`. Their mutation controls
are named `synthetic-replay-<test-name-with-hyphens>` in root `mutations.toml`; each
corrupts the family's output-usage parser by one token and must fail only its named
test under an exact filter. Existing service-tier readers continue checking the same
field precedence independently of these new snapshots.

| Artifact | Offline test | Preserved response shape |
|---|---|---|
| `anthropic/signed_thinking.json` | `anthropic_signed_thinking` | message start, thinking/text block deltas, opaque signature, usage and end-turn |
| `anthropic/tool_loop_nonce.json` | `anthropic_tool_loop_nonce` | signed thinking, split tool JSON, tool-use finish, second text step |
| `gemini/text_completes.json` | `gemini_text_completes` | text candidate, STOP, usage metadata and body service tier |
| `gemini/tool_call.json` | `gemini_tool_call` | signed functionCall args followed by split second-step text |
| `openai_chat/text_completes.json` | `openai_chat_text_completes` | reasoning/text deltas, terminal choice and usage-only chunk, no body tier |
| `openai_chat/tool_call.json` | `openai_chat_tool_call` | reasoning, fragmented indexed tool arguments, tool-calls finish, second text step |
| `openai_responses/text_completes.json` | `openai_responses_text_completes` | created/in-progress, output text parts and completed, auto-to-default tier |
| `openai_responses/tool_call.json` | `openai_responses_tool_call` | indexed function-call deltas/done/completed item, second text step |
| `provider_switch/ant_gem_anthropic_leg_a.json` | `switch_ant_gem_anthropic_a` | signed thinking and split read-record tool call |
| `provider_switch/ant_gem_gemini_leg_b.json` | `switch_ant_gem_gemini_b` | split text candidate and STOP usage |
| `provider_switch/ant_oai_anthropic_leg_a.json` | `switch_ant_oai_anthropic_a` | signed thinking and split read-record tool call |
| `provider_switch/ant_oai_openai_chat_leg_b.json` | `switch_ant_oai_chat_b` | reasoning/text chunks and terminal usage |
| `provider_switch/anthropic_leg_a.json` | `switch_anthropic_a` | thinking/signature and split read-record tool call |
| `provider_switch/anthropic_leg_b.json` | `switch_anthropic_b` | split text and end-turn usage |
| `provider_switch/gemini_leg_b.json` | `switch_gemini_b` | split text candidate and STOP usage |
| `provider_switch/oai_ant_anthropic_leg_b.json` | `switch_oai_ant_anthropic_b` | split text and end-turn usage |
| `provider_switch/oai_ant_openai_chat_leg_a.json` | `switch_oai_ant_chat_a` | reasoning and fragmented indexed read-record tool args |
| `provider_switch/oai_gem_gemini_leg_b.json` | `switch_oai_gem_gemini_b` | text candidate and STOP usage |
| `provider_switch/oai_gem_openai_chat_leg_a.json` | `switch_oai_gem_chat_a` | reasoning and fragmented indexed read-record tool args |
| `provider_switch/openai_chat_leg_a.json` | `switch_chat_a` | reasoning and fragmented indexed read-record tool args |

Additional existing readers in `service_tier_used`:
- `anthropic/signed_thinking.json`: `anthropic_records_usage_service_tier_from_a_recorded_stream`.
- `openai_responses/text_completes.json`: `responses_records_the_terminal_tier_not_the_created_echo`.
- `openai_chat/text_completes.json`: `chat_without_a_reported_tier_records_none`.
- `gemini/text_completes.json`: `gemini_records_the_response_header_tier`,
  `gemini_records_the_body_tier_from_a_recorded_stream`,
  `gemini_header_wins_over_the_body_when_they_differ`,
  `code_assist_reads_the_same_header_and_records_none_without_it`, and the `live`-feature
  `end_to_end::{gemini_header_tier_reaches_the_wal,gemini_without_the_header_records_no_tier_key}`.

Live tests create artifacts rather than replaying them. The unprefixed switch names
are historical artifacts; the prefixed switch names are the current harness outputs.
