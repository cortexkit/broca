# Cache evidence

A model attempt's record optionally carries `cache_evidence`: how this request compares with
the session's previous model request, so a reader (Magic Context's attribution classifier,
an operator) can tell whether a cache miss came from a changed prefix. broca reports
coordinates, never a verdict.

The first attempt, or one without a verified predecessor, omits it. Per-session memory keeps
only the preceding request's segment hashes and coordinates, breakpoint, send time, and
diagnostic identity, policy and view metadata, never its body. On the first run after a
restart, broca rebuilds the last request once from the session's records and accepts it only if
its canonical digest matches the one recorded. An unavailable rebuild is a gap, never a
comparison against an older attempt. Hashing runs on the blocking pool after transport handoff,
concurrently with the response, and is joined before the attempt is written. It never changes
request bytes.

Live divergences omit the optional `field` detail, because the previous bytes are not
retained. **Absent means not recorded**, never "no field changed". `full_input_reason` is the
recorded, open-string transport reason, never inferred from elapsed time. `idle_gap_ms` uses
send times, never response-end times.

To read the bodies of a run's requests, use `session.request_dump` (see
[module surface](module-surface.md), "Request bodies of one run").

The shared `broca_provider::cache_evidence::fingerprint` API underlies the comparison. It borrows raw JSON segments
from deterministic renderer output and hashes their bytes. Only segments with
cache markers or the named rotating settings need owned JSON normalization.
The segment vector grows geometrically; it does not allocate a JSON tree per
message/content part. Bodies with different arbitrary whitespace/key order are
not an input contract: these comparisons consume broca's canonical renderers.

### Evidence coordinates and prefix rules

- `extends_previous` means the previous normalized request is a prefix of the
  current request, with all settings and tool/system segments unchanged. A clean
  message-tail append is `(true, null)` for `(extends_previous, first_divergence)`.
  An edit, removal, insertion before the end, or changed setting is false with
  its first divergence, **even after the previous cache marker**. The classifier,
  not broca's comparison, decides whether that coordinate is inside cached input.
- Message divergences additionally carry optional `message_ordinal`, the durable
  transcript ordinal used by `session.read` and the `m<ordinal>` component of
  compaction-view addresses. It comes from the actual emitted row/part's source
  `Message.ordinal`, never from a rendered index or parsed prompt text. A view can
  move `m2` to wire position zero; the ordinal stays 2. The current request's source
  is reported when a current segment exists; a removed tail uses the previous
  request's source. System, tools, provider settings, and content without a unique
  durable source omit it. Grouped Gemini tool rows resolve part-level origins,
  while their ambiguous row header has no single ordinal. The wire `part_index`
  is still a provider-array position, not automatically a durable `#<part>` key.
  The optional member is absent by default and older evidence readers ignore it.
- `previous_last_breakpoint` uses the same coordinate space as `first_divergence`:
  `{region, message_index?, part_index?}`. Regions are `system`, `tools`,
  `messages`, and `provider_state`. There are no synthetic message indexes.
  Indexes are zero-based wire-array positions (including system/developer items
  embedded in Chat/Responses arrays). Tool indexes use `part_index`; Bedrock's
  standalone `cachePoint` uses its own block index. Non-positional settings have
  a region and optional changed `field`. `prefix_tokens_before` is omitted:
  JSON segments do not establish tokenizer boundaries.

The trace-aware render entry points accept an optional borrowed provenance sink.
The existing entry points use the same builders and emitters with no sink. With
provenance enabled, actual row/part emission records only integer coordinates and
source ordinals in flat vectors, including filtering, tool-item splitting and
grouping, synthetic instructions, and prepared-media rendering. No identity is
inserted into request JSON, and fingerprints never retain transcript bodies.
- A null breakpoint with `ttl_kind: "explicit"` means the previous request had
  no explicit marker. With `ttl_kind: "implicit"`, a null breakpoint is normal.
  TTL policy is taken from the previous request's frozen config, including
  omitted-on-wire Anthropic default TTLs; unknown implicit lifetimes stay null.
- Explicit prefix region order is **tools → system → messages**, for both
  Anthropic Messages and Bedrock Converse, regardless of JSON key order.
  Anthropic documents this hierarchy in [Prompt caching: structuring your
  prompt](https://platform.claude.com/docs/en/build-with-claude/prompt-caching).
  AWS states “Cache checkpoints are processed in this order: tools → system →
  messages” and that earlier-section edits invalidate later sections in
  [Prompt caching for faster model inference](https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html).
  Broca's Converse renderer builds `toolConfig.tools`, `system`, and
  `messages[].content` separately (`families/bedrock_converse.rs`,
  `build_request_body_with_media`); its marker planner inserts standalone
  `cachePoint` blocks in system/messages (`apply_bedrock_marker_policy_to_body`).
  Serialization order is not the cache-prefix order. Provider settings are
  separate evidence; they are not another position in this three-region prefix.

### Segmentation and normalization

| Family | System / tools / messages |
| --- | --- |
| Anthropic | `system[]` / `tools[]` / `messages[].role` and `content[]` |
| Bedrock Converse | `system[]` / `toolConfig.tools[]` / `messages[].role` and `content[]` |
| OpenAI Chat | system/developer `messages[]` / `tools[]` / other `messages[]` and `content` |
| OpenAI Responses, ChatGPT | `instructions` and system/developer `input[]` / `tools[]` / other `input[]` and `content[]` |
| Gemini | `systemInstruction.parts[]` / `tools[]` / `contents[].role` and `parts[]` |
| Code Assist | Gemini segmentation inside `request`; remaining envelope fields are provider state |

Remaining request settings and opaque reasoning items are preserved. The only
normalizations are cache markers (`cache_control`, Bedrock `cachePoint`), output
caps (`max_tokens`, `max_completion_tokens`, `max_output_tokens`,
`inferenceConfig.maxTokens`, `generationConfig.maxOutputTokens`), Code Assist's
per-step `requestId`, the Anthropic billing-header line (body signature and
request/episode/turn correlation), `metadata.user_id`'s device/session IDs, and
ChatGPT's named client-metadata routing/turn IDs. Comments at each normalization
name the fields and why they rotate. Output caps are not cached prompt input;
broca intentionally clamps them near the window limit. Model, cache keys,
tool choice, reasoning settings/budgets, signed thinking (including
`thinking.signature`), provider text state, and unknown settings remain raw.

ChatGPT comparison uses its canonical full logical input, whose digest excludes socket framing.
The attempt separately records `continuation` and `full_input_reason`; a continuation's
transmitted suffix is not mistaken for a deleted prefix.
