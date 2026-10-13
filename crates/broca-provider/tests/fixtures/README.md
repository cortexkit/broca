# Synthetic wire fixtures

The eight fixtures below contain invented prompts, identifiers and opaque data, not
provider traffic. They preserve the structure of production messages: field names/nesting,
ordered SSE event types, absent versus zero usage counters, JSON-string tool inputs,
and opaque-byte round trips. Tool inventories are intentionally one or two sample
tools, with no skills or plugin instructions. Opaque values retain their length;
they are not valid provider signatures or ciphertext.

Historical test names containing `real` or `captured` remain unchanged so test filters
and mutation controls stay compatible. Updating expected sample text, tool names or
identifiers does not weaken the structural, usage or round-trip assertions.

| Fixture | Readers and preserved regression | Mutation that must fail the named test (root `mutations.toml`) |
|---|---|---|
| `codex_toolcall_turn_function_call_pairing.json` | conformance `responses_decode::captured_codex_tool_call_turn_pairs_by_call_id`: id-less call, JSON-string arguments, plain output, name recovery by `call_id` | `synthetic-codex-pairing` removes the recovered result name |
| `codex_resume_turn_with_echoed_reasoning.json` | conformance `responses_decode::captured_codex_resume_turn_decodes_with_verbatim_reasoning` and `captured_codex_reasoning_renders_the_native_continuation_item`: System/System/User/User/Assistant/User roles, id-less opaque reasoning followed by answer, 1444-byte opaque continuation, lossless CK wire serialization | `synthetic-codex-echoed-reasoning` loses the opaque reasoning type |
| `codex_reasoning_turn_response.sse` | conformance `responses_decode::captured_codex_terminal_envelope_preserves_output_contract`, `captured_codex_reasoning_sse_preserves_the_complete_item_in_output_order`, `captured_codex_reasoning_renders_the_native_continuation_item`, `captured_codex_reasoning_is_stripped_for_a_different_target_model`; it `responses_cache_write_usage::codex_capture_without_the_cache_write_key_leaves_it_absent_{typed,generic}_path`, `service_tier_used::chatgpt_codex_records_the_terminal_tier_from_a_recorded_stream`, `service_tier_used::end_to_end::priority_chatgpt_run_records_the_tier_the_response_reported`: complete reasoning item before text, 1444 opaque bytes, input 4267/cache-read 9600/output 178/reasoning 98, absent writes, auto-to-default tier | `synthetic-codex-complete-reasoning` drops the complete item; `synthetic-codex-absent-write-{typed,generic}` invent zero writes |
| `codex_spark_toolcall_done_only.sse` | it `normalize_fixture::normalizes_spark_done_only_tool_arguments`: empty added arguments, no argument deltas, full arguments on done and completed item | `synthetic-spark-done-only` discards the assembled arguments before decoding |
| `codex_cache_write_zero_response.sse` | it `responses_cache_write_usage::codex_capture_keeps_a_reported_zero_cache_write_{typed,generic}_path`: input 14, read 0, explicitly written 0, output 6, reasoning 0 | `synthetic-codex-zero-write` and `synthetic-codex-zero-write-generic` drop reported writes |
| `openai_responses_cache_write_response.sse` | it `responses_cache_write_usage::metered_capture_splits_cache_write_out_of_fresh_input_{typed,generic}_path`: inclusive input 3173 splits into fresh 3/write 3170/read 0, output 5/reasoning 0 | `synthetic-responses-metered-write` and `synthetic-responses-write-generic` drop the write subset |
| `anthropic-tool-call.sse` | it `normalize_fixture::normalizes_real_anthropic_tool_call_stream`; module-serve it `captured_replay_e2e::real_module_completes_against_captured_anthropic_bytes`: signature-only thinking, two text deltas, split JSON tool input, thinking/text/tool block order, input 453/output 149 and tool-use finish | `synthetic-anthropic-signature` drops the signature delta |
| `cc_system_reminder_request.json` | it `anthropic_decode::real_cc_turn_with_mid_array_system_decodes`, `anthropic_encode::real_cc_fixture_is_decode_encode_decode_fixpoint` and `anthropic_encode::encode_is_deterministic`: three top-level system blocks, eight wire messages, in-place mid-array System, thinking and paired tool result | `synthetic-cc-mid-array-system` misclassifies the mid-array System as User |

Other files in this directory are existing renderer goldens or hand-built fixtures.
Regenerate renderer goldens only through their existing test regeneration paths.
