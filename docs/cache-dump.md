# Offline cache dumps

`broca-session cache-dump` reads the state root's live and archived WALs without a daemon or write handles on the source. It reconstructs requests with the shared offline renderer and writes prompt-bearing bodies only after their SHA-256 matches the completed attempt's recorded digest.

```sh
broca-session cache-dump --session alfonso:example --out ./private-dump
broca-session cache-dump --since 2026-10-01T00:00:00Z --out ./private-dump
```

Repeat `--session` for multiple session names or bare task ids. Name selection uses the same read-only session-ref inventory lookup as `trajectory` and `read`: try the supplied name first, then `alfonso:<id>` for an unprefixed name. It scans only the compact inventory table with a SQL name predicate, never run-index history, and adds no database index or migration. Unknown names and ambiguous names refuse before output creation, listing the searched names or candidate bind triples. An incomplete inventory cannot prove absence or uniqueness and asks for explicit bind flags. The command does not scan all WALs as a fallback when name lookup cannot resolve a session. To bypass lookup, provide both `--project-root <path>` and `--harness <name>` with the full recorded `--session` name; explicit selection keeps the name literal. The shared `session_addr` derives each triple's storage address, and the WAL store reads that session's live WAL or its archived copy; no other WAL is read. `--since` preselects runs by the index's `state_changed_ms`, which is written on a terminal transition after WAL sync and therefore bounds that run's last activity. Active and paused rows are always included because their state timestamp is not a completion bound. Terminal rows with no timestamp are legacy rows from before `state_changed_ms` existed; they are excluded from time selection and counted as `undated_runs_not_selected`. `--session` still reaches them. The selected bind triples are deduplicated by address. A final WAL check requires each call's selected attempt to have `ts_ms >= since`; only those calls are exported. Earlier history is still replayed so their bodies remain exact. Calls excluded by this cutoff are counted as `calls_before_since`. Unlike `--since`, `--session` exports full histories. `--since` accepts RFC3339 timestamps or epoch milliseconds. `--state-root` overrides normal state-root resolution. Do not pass daemon connection arguments to this offline command.

Each completed model step uses its last completed attempt with a response. The filename prefix uses the live transport's shared `request_dump_filename` function: the sanitized run id and `-s<step>`. Production run ids already contain the `run-sid-` prefix. Attempts are recorded in metadata, not filenames. Metadata records the original run id, UTC attempt timestamp and filenames. Bodies are the renderer's original bytes, not pretty-printed JSON. Usage is restored to the family's own wire field names and semantics under `usage`; the WAL's normalized usage is also carried unchanged under `broca_usage`. Unreported counts are omitted in both.

| Family | `usage` fields |
| --- | --- |
| Anthropic Messages | `input_tokens` (fresh input only), `cache_read_input_tokens`, `cache_creation_input_tokens`, `output_tokens` |
| OpenAI Responses and ChatGPT | `input_tokens` (fresh input plus cache reads and writes), `input_tokens_details.cached_tokens`, `output_tokens` |
| Gemini and Google Code Assist (Antigravity) | camelCase `usageMetadata` names: `promptTokenCount` (fresh input plus cached content), `cachedContentTokenCount`, `candidatesTokenCount` (visible output, excluding thoughts), `thoughtsTokenCount` |

Code Assist responses wrap Gemini's body in `{response: ...}`, but the exported `usage` is the inner `usageMetadata` shape, the same as Gemini's. Broca stores Gemini output as candidates plus thoughts, so `candidatesTokenCount` is that output minus `thoughtsTokenCount`. When the two are equal, the response reported thoughts without candidates; Google's JSON omits zero counts, so `candidatesTokenCount` stays omitted. A prompt count needs a stored input count; a cache count alone exports only `cachedContentTokenCount`.

On Unix, new output directories use mode 0700 and files use mode 0600. Existing directories with any group or world permissions, symlinks and existing output files are refused. Output parents must already exist. Treat dumps as sensitive prompt data; Windows filesystem ACLs must be managed by the operator. No prompt or body is printed in diagnostics or the JSON summary.

Digest mismatches produce metadata with `verified: false`, both digests and no body or response. The reason is `transformed_request` when the attempt carries `input_transformations`, otherwise `digest_mismatch`. Prompt-reduction transforms are not rerun: their output is not durably available, and their requests can only be exported if the raw replay happens to match exactly. A response-bearing step without durable usage counts as examined and, when no stronger failure applies, skipped under `missing_step_usage`; it gets metadata and a body only when the digest verifies, never a response file. Failed retries of completed steps are folded into the final completed attempt. Other families get metadata but no body or response. Renderer/replay failures are counted as `render_unavailable`; unsupported families as `family not exported`.

The summary reports candidate sessions read, undated terminal runs excluded from time selection, calls examined, excluded by the attempt-time cutoff, exported and verified, digest mismatches, and skips by reason. The legacy `sessions_not_found` field remains present but empty on successful selection; missing names now refuse before a summary or export is produced. Each skipped call has exactly one reason matching its metadata: rendering unavailable first, then digest mismatch/transformed request, then unsupported family, then missing step usage. The verified count includes unsupported-family calls whose request digests match; the exported count includes only verified calls in supported families with a body, response and metadata file. Zero examined calls exits nonzero even if the selection or output directory is valid. Source read failures also exit nonzero. A concurrent writer may advance after the read snapshot; rerun to a fresh output directory to include later calls.

## Offline cache diagnosis

```sh
broca-session cache-diagnose alfonso:example
broca-session cache-diagnose example --state-root ./copied-state --out ./diagnosis.jsonl
```

`cache-diagnose` uses the same read-only session/task lookup, explicit
`--project-root`/`--harness` binding, retention checks, and live/archive WAL reader.
It prints one JSONL `cache_attempt` row for **every** `ModelAttemptFinished`, including
failed retries, rather than selecting one response per step. No daemon, network,
request-body export, or WAL changes are involved. `--out` creates a new owner-only
file; stdout is the default. The output contains account references and other
diagnostic identifiers, but no prompt text, thinking text, signatures, or body bytes.

The shared `broca_provider::cache_evidence::compare_rendered_requests` compares
canonical logical requests reconstructed by `wal_render`, after checking their
recorded SHA-256. A missing or mismatching reconstruction makes `cache_evidence`
null with a `reason`, including on the following comparison; it never silently
compares against an older valid attempt. The first attempt has null evidence and
`reason: "first_attempt"`. `compared_to` is the preceding attempt's
`<lineage_id>:<wal_seq>` in this lineage, across run boundaries. No verdict is
emitted: MC retains its shared attribution classifier.

### Evidence coordinates and prefix rules

- `extends_previous` means the previous normalized request is a prefix of the
  current request, with all settings and tool/system segments unchanged. A clean
  message-tail append is `(true, null)` for `(extends_previous, first_divergence)`.
  An edit, removal, insertion before the end, or changed setting is false with
  its first divergence, **even after the previous cache marker**. The classifier,
  not broca's comparison, decides whether that coordinate is inside cached input.
- `previous_last_breakpoint` uses the same coordinate space as `first_divergence`:
  `{region, message_index?, part_index?}`. Regions are `system`, `tools`,
  `messages`, and `provider_state`. There are no synthetic message indexes.
  Indexes are zero-based wire-array positions (including system/developer items
  embedded in Chat/Responses arrays). Tool indexes use `part_index`; Bedrock's
  standalone `cachePoint` uses its own block index. Non-positional settings have
  a region and optional changed `field`. `prefix_tokens_before` is omitted:
  JSON segments do not establish tokenizer boundaries.
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

ChatGPT comparison uses its canonical full logical input, whose digest excludes
socket framing. Rows separately expose the recorded `continuation` and
`full_input_reason`; a continuation's transmitted suffix is not mistaken for a
deleted prefix. The offline command does not reconstruct socket receipts.

`idle_gap_ms` uses send times, never response-end times. Required numeric evidence
uses zero when timestamps are missing; the row's `idle_gap_known: false` prevents
interpreting that as a measured zero gap. Similarly, `account_comparison_known`
distinguishes unavailable account identity from known equality. Attempt billing
usage/corrections take precedence; only the final complete attempt of a legacy
step without a billing envelope inherits step usage. Failed retries never borrow
that meter. Retention is unmetered when needed counts are missing.

`retention.expected` is `prev.cache_read + prev.cache_write` when the previous
request wrote cache tokens, otherwise `prev.cache_read + prev.input`. The ratio
is current cache read divided by expected: `stable` ≥95%, `warning` ≥80% and
<95%, `bust` <80%, and `full_bust` for zero read after cache existed. Before a
cache existed the status is `base`. Ratios are fractions, not percentages.
