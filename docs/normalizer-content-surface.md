# Normalizer content-delivery surface

## The invariant

**Every path by which a provider can deliver assistant text, tool calls, or reasoning is
read by the normalizer that serves it.**

If a content-bearing path is not decoded, the assembled assistant message may
omit text, tool calls, or reasoning without a normalization error.

This document enumerates, per family, every content-bearing inbound path the provider can
use, and marks each read or unread with its source line. It is decode-side only: streamed
deltas, finalized/done events that supersede those deltas, completed-item or completed-response
payloads that repeat content in full, and terminal events that carry content alongside their
stop reason. Request encoding, rendering, and durability are out of scope.

The runner consumes only the assembled `ProviderControl::AssistantMessage`; it never
re-assembles display deltas. So "read" means: the path feeds the assembled message
(the text / refusal / reasoning / tool-call buffers), not merely the lossy display lane.

For each family, enumerate dispatcher event literals and fixture event types,
then compare the resulting paths against the decoder. Repeat both checks when
adding or revisiting a family to detect paths missing from either source.

## openai_responses.rs — `OpenAiResponsesNormalizer` (also serves ChatGPT/Codex)

The ChatGPT/Codex variant (`openai_responses_chatgpt.rs`) wraps this normalizer unchanged
(`ChatgptNormalizer` forwards `push`/`finish`), so it inherits the identical surface.

| Event | Content carried | Status | Source |
| --- | --- | --- | --- |
| `response.output_text.delta` | text fragment | read | `openai_responses.rs:569` |
| `response.output_text.done` | finalized text (authoritative `insert`) | read | `openai_responses.rs:587` |
| `response.refusal.delta` | refusal fragment | read | `openai_responses.rs:593` |
| `response.refusal.done` | finalized refusal (authoritative replace) | read | `openai_responses.rs:614` |
| `response.output_item.added` (`function_call`) | call_id / name / arguments | read | `openai_responses.rs:623` |
| `response.function_call_arguments.delta` | arguments fragment | read | `openai_responses.rs:640` |
| `response.function_call_arguments.done` | finalized arguments (authoritative replace) | read | `openai_responses.rs:658` |
| `response.output_item.done` (`function_call`) | full call (id/name/arguments) | read | `openai_responses.rs:672` |
| `response.output_item.done` (`reasoning`) | whole raw item → `RedactedReasoning` | read | `openai_responses.rs:687` |
| `response.output_item.done` (`message`) | content array: `output_text` + `refusal` parts (fallback) | read | `openai_responses.rs:695` |
| `response.completed` / `response.incomplete` | usage / status / incomplete_details (no content) | read | `openai_responses.rs:526` (fast path `:743`), `:719` (generic) |
| `response.created` | response id only (control) | n/a | `openai_responses.rs:539` |
| `response.in_progress` | nothing — `response.output` is empty, `status: in_progress` | n/a | no handler; verified empty in both captured fixtures |
| `error` / `response.failed` | error (control) | n/a | `openai_responses.rs:557` |
| `response.content_part.added` / `.done` | a message content part | unread — redundant | no handler |
| `response.reasoning_summary_text.delta` / `.done`, `response.reasoning.delta` / `.done` | reasoning summary text | unread — out of scope | no handler |

Notes on the two unread rows:
- `content_part.*` is NOT a sole carrier. A content part is one element of the completed
  message item's `content` array, which the `output_item.done` message fallback already reads
  (`message_item_text` / `message_item_refusal`). Any text a `content_part.done` could carry is
  recovered there.
- The reasoning-summary events are display-only. The durable reasoning item is captured whole
  at `output_item.done` (`:687`), summary field included. The summary text is therefore
  recoverable from the persisted item; streaming it incrementally into the display lane is a
  display nicety, not a content-delivery gap.

## anthropic — `AnthropicNormalizer` (`normalize.rs`; `anthropic_messages.rs` delegates)

Anthropic streaming has no completed-message repeat: the message is assembled purely from
content blocks, and `content_block_stop` carries no content. There is no finalized event that
supersedes the deltas.

| Event | Content carried | Status | Source |
| --- | --- | --- | --- |
| `message_start` | message id + usage | read | `normalize.rs:129` |
| `content_block_start` (`text`) | initial text | read | `normalize.rs:149` |
| `content_block_start` (`thinking`) | initial thinking + signature | read | `normalize.rs:156` |
| `content_block_start` (`redacted_thinking`) | encrypted blob (verbatim) | read | `normalize.rs:168` |
| `content_block_start` (`tool_use`) | id / name / start input | read | `normalize.rs:177` |
| `content_block_delta` (`text_delta`) | text fragment | read | `normalize.rs:208` |
| `content_block_delta` (`thinking_delta`) | reasoning fragment | read | `normalize.rs:222` |
| `content_block_delta` (`signature_delta`) | reasoning signature fragment | read | `normalize.rs:235` |
| `content_block_delta` (`input_json_delta`) | tool argument fragment | read | `normalize.rs:247` |
| `message_delta` | stop_reason + usage | read | `normalize.rs:268` |
| `message_stop` | assemble (no content) | n/a | `normalize.rs:280` |
| `error` | error (control) | n/a | `normalize.rs:299` |

The `tool_use` start input is used as the authoritative fallback when a zero-argument call
sends no `input_json_delta` (`normalize.rs:393`).

## gemini.rs — `GeminiNormalizer` (google_codeassist.rs delegates via `handle_response_value`)

Each streamed chunk is a `GenerateContentResponse`; parts are incremental and the terminal
chunk carries `finishReason`. There is no separate superseding done event. Code-Assist unwraps
its `{response: ...}` envelope and feeds the inner value through this same decoder
(`google_codeassist.rs:296`), so it inherits the identical surface.

| Path | Content carried | Status | Source |
| --- | --- | --- | --- |
| `usageMetadata` | usage | read | `gemini.rs:672` |
| `promptFeedback.blockReason` (no candidate) | content-filter terminal | read | `gemini.rs:683` |
| `candidates[0].finishReason` | stop reason | read | `gemini.rs:695` |
| part `functionCall` | name / args / thoughtSignature | read | `gemini.rs:720` |
| part `thought:true` text | reasoning text | read | `gemini.rs:749` |
| part `thought` signature-only | redacted reasoning | read | `gemini.rs:759` |
| part `text` (non-thought) | answer text | read | `gemini.rs:765` |

`inlineData` / `executableCode` parts are not assistant text, tool calls, or reasoning, so they
are outside this invariant.

## openai_chat.rs — `OpenAiChatNormalizer`

Chat Completions streaming delivers content only via `choices[].delta`; there is no
completed-item repeat in the stream. The message is assembled from deltas at `finish()` (EOF).

| Path | Content carried | Status | Source |
| --- | --- | --- | --- |
| `delta.reasoning_content` | reasoning fragment | read | `openai_chat.rs:422` |
| `delta.content` | text fragment | read | `openai_chat.rs:432` |
| `delta.refusal` | refusal fragment | read | `openai_chat.rs:443` |
| `delta.tool_calls` | id / name / arguments by index | read | `openai_chat.rs:455` |
| trailing `usage` chunk | usage | read | `openai_chat.rs:395` |
| `finish_reason` | stop reason | read | `openai_chat.rs:408` |

## bedrock_converse.rs

Bedrock delivers text and reasoning fragments via `contentBlockDelta`; tool identity arrives in
`contentBlockStart.start.toolUse`. Ignoring the value of `start.text` (block-type detection
only) does not discard streamed text.

| Event | Content carried | Status | Source |
| --- | --- | --- | --- |
| `messageStart` | control | n/a | `bedrock_converse.rs:927` |
| `contentBlockStart` (`toolUse`) | id / name | read | `bedrock_converse.rs:943` |
| `contentBlockStart` (`text`) | block init (no text value) | read | `bedrock_converse.rs:956` |
| `contentBlockDelta` (`text`) | text fragment | read | `bedrock_converse.rs:975` |
| `contentBlockDelta` (`toolUse.input`) | tool argument fragment | read | `bedrock_converse.rs:978` |
| `messageStop` | stopReason | read | `bedrock_converse.rs:900` |
| `metadata` | usage | read | `bedrock_converse.rs:909` |
| `*Exception` / `*Error` frames | error (control) | n/a | `bedrock_converse.rs:886`, `:916` |
| `contentBlockDelta.delta.reasoningContent` | reasoning text, signature fragments, base64 redacted bytes | read when frozen `bedrock_reasoning` is true | `push_reasoning_delta` |

Bedrock reasoning uses distinct buffers at the original content-block indices. Signature-only
blocks survive, and independently encoded redacted deltas are decoded before their bytes are
concatenated. The assembled block is encoded once for replay. Older frozen configs without
`bedrock_reasoning` retain their original reasoning-free normalized output and request bytes.
