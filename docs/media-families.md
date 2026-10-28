# Media family facts

This is the table later media work cites for provider facts. For each wire family
(`WireFamilyId`, `crates/broca-provider/src/spec.rs`) and each `openai_chat` provider
profile, it records how a provider's request format carries images, PDFs, audio and
video, with which limits, and how broca estimates their token cost. It also records the
fixed per-kind store maximum and Fusiform's input-modality fact.

Facts were read from the cited primary sources on 2026-10-22. A value marked
**broca choice** is not a provider fact: it is the value broca uses where the provider
documents nothing, or documents something broca cannot evaluate (for example a
per-page or per-second rate, when broca does not parse page counts or durations).

Rules every entry follows:

- Transport is inline base64 only. Provider file-upload APIs, `file_id`, `fileData`
  and fetched URLs are out of scope, so their limits are not listed.
- "Encoded bytes" means the size after base64, which is what counts against a request
  cap. Base64 makes a payload about 4/3 of its raw size.
- A cell reading **not documented** means the cited sources say nothing. A profile
  treats an undocumented carrier as unavailable (marker for tool media, `carrier`
  refusal for a person's media) until a live check against the real provider (a few
  requests, run on a maintainer's machine, never from a test sandbox) says otherwise.
- "Tool result" means the provider's native tool-result slot. Where that slot cannot
  hold a kind, the profile uses the follow-up user message (only after a live check
  shows the provider accepts it and prompt caching stays stable) or a marker.

### Running the live checks

Only an operator runs live checks, on the Mac outside a worker sandbox.
The bounded probe uses the explicit prepared-media renderer, the family's real
endpoint and normalizer, and broca's HTTP client with redirects and automatic
retries disabled. It never prints a credential, handle, request headers, response
body or provider error message. Unknown provider codes are `other_redacted`.

```sh
# Default dry-run: 6 serving families, at most 18 planned requests; sends nothing.
cargo run --locked -p broca-provider --features live --example media-live-check -- --dry-run
# The checked-in plan is crates/broca-provider/examples/media-live/plan.json.
# Preview any operator edits before explicitly requesting live traffic.
cargo run --locked -p broca-provider --features live --example media-live-check -- --dry-run --plan <plan.json>
BROCA_LIVE_TESTS=1 cargo run --locked -p broca-provider --features live --example media-live-check -- --live --plan crates/broca-provider/examples/media-live/plan.json --subc "$HOME/.local/share/cortexkit/run/subc-connection.json"
```

The checked-in plan uses operator-confirmed serving models and vault credential
names, not an automatically refreshed catalog snapshot: Anthropic Haiku 5.5,
OpenAI GPT-6 Luna on Chat, Responses and ChatGPT, Gemini 3.8 Flash, and its
Antigravity route. Recheck these selections when serving routes change. An
operator override may set the wire model, named dev-tools credential, provider
`quirk` (for a compatible endpoint), and any `base_url` or `aws_region`.
An optional non-secret `frozen` object supplies a serving `FrozenRenderConfig`
including provider-specific identity and quirks instead of family defaults. No
token or capability handle belongs in the plan. Credentials are read by name only
through the dev-tools handle map and subc, and only after `--live` and
`BROCA_LIVE_TESTS=1`. `--subc` and `--timeout-secs` are optional.

Bedrock is absent from the serving plan because the operator has no Bedrock
credential; the summary records it as not live-checked with reason
`no_credential`. Code Assist's account-project discovery is not part of this
standalone probe. It still renders all three dry-run requests, but a live run
without an explicit resolved `project_id` reports `skipped: project_id_required`
before reading any credential or sending anything. The offline preview uses an
empty project field only in its temporary render config; no account project ID
is hard-coded or inferred, and the unresolved live config is unchanged.

One route consumes all three request slots for its family; duplicate families
are refused before credential access. Request 1 checks person PNG/PDF input where
the profile carries it. Request 2 checks a tool PNG/PDF with different readable
values, using the candidate follow-up user carrier where necessary. Request 3
extends the same history to measure cache stability. ChatGPT also checks user
and tool audio in request 1 (`audio/wav`) and request 2 (`audio/mpeg`), with four
different spoken values. Candidate MIME types are not claims of acceptance.
Fixtures are synthetic and checked in beside the example; no session data is read.

Dry-run JSON lines list family, wire model, profile, carrier, media kind, MIME,
decoded byte size, digest, and complete rendered request size/digest. Live lines
report HTTP status, allowlisted provider code, usage including cached tokens, and
fixed-string media-check booleans, never the answer. `cache_stable` requires a
positive reported cache hit on request 2 and no decrease on request 3; missing
usage, zero hits or interrupted runs are inconclusive, not passes. Media checks
must also pass independently. A provider rejection spends a slot and is never
retried. The probe only changes candidate profiles in memory, never the production
registry or a running admission's frozen config.

Paste results beside the matching profile below in this format (one row per
request; use `not serving` for a family not exercised):

| operator-run UTC / revision | wire model / endpoint profile | request / carrier / MIME | HTTP / provider code | media checks | input / cached / cache-write / output tokens | cache stable | decision |
|---|---|---|---|---|---|---|---|
| pending | pending | pending | pending | pending | pending | unmeasured | keep gate unset |

Record refusals as well as passes. A pass for one compatible provider or Gemini
generation does not enable another profile. A ChatGPT audio pass records exactly
the accepted MIME set in the frozen profile, as well as the carrier flag. Code
Assist's current all-kind flag must not be enabled from PNG/PDF-only evidence:
audio/video remain unverified by those checks. Untested profiles retain markers.

## Kinds, MIME types and catalog modalities

broca stores four kinds. The MIME type is detected from the bytes at capture.

| kind | MIME types broca captures | catalog modality that gates it |
|---|---|---|
| `image` | `image/png`, `image/jpeg`, `image/gif`, `image/webp` | `image` |
| `document` | `application/pdf` | `pdf` |
| `audio` | per family below; the union is `audio/wav`, `audio/mpeg` (MP3), `audio/aac`, `audio/ogg`, `audio/flac`, `audio/aiff`, `audio/mp4` (M4A) | `audio` |
| `video` | per family below; the union is `video/mp4`, `video/mpeg`, `video/quicktime` (MOV), `video/webm`, `video/x-flv`, `video/x-ms-wmv`, `video/3gpp`, `video/x-matroska` | `video` |

A family accepts a MIME type only if its entry below lists it. Any other type is a
`mime` refusal for a person's send and a `mime` marker for tool media. Some providers
spell a type differently from its registered name (Gemini's `audio/mp3` and
`video/mov`, Kimi's `video/mov`); the entry lists the provider's spelling, and the
renderer writes that spelling for the type broca detected.

## Fixed per-kind store maximum

These maximums are the same for every profile, model and family. A payload above its
kind's maximum is not stored (reason `too_large`). A payload at or below it is stored
even when the frozen profile cannot show it, so it can render after a later model
switch. The profile's own per-item limit (below) is checked separately: at send time
for a person's media, at render time for stored tool media.

| kind | store maximum (decoded bytes) |
|---|---|
| `image` | 33,554,432 (32 MiB) |
| `document` | 52,428,800 (50 MiB) |
| `audio` | 52,428,800 (50 MiB) |
| `video` | 67,108,864 (64 MiB) |

**broca choice.** The llm-runner role contract (`cortexkit-role-llm-runner` 0.5.1)
sets no upload or per-kind size, so these values are broca's. Each one covers the
per-item limits the profiles below document for that kind, up to what a profile's
request cap can carry once base64-encoded:

- image: 32 MiB is DeepSeek's per-image limit, the largest except Meta's 50 MB; a
  32 MiB image is about 43 MiB encoded, which still fits Meta's 50 MB request cap,
  while a 50 MB image would not.
- document and audio: 50 MiB covers OpenAI's and Gemini's 50 MB PDF limits and
  Meta's 50 MB inline file limit.
- video: 64 MiB is about 85 MiB encoded, which fits the largest request cap below
  (Kimi's 100 MB); no profile could send a much larger inline video.

## Fusiform input-modality fact

broca admits sends from Fusiform's catalog facts on the live serving path.

- **Fact key:** `capability.input_modalities`, requested through `catalog.get` with
  the `fact_prefixes` argument (an array of prefix strings). broca's current prefix
  list (`FUSIFORM_FACT_PREFIXES`, `crates/broca-catalog/src/runtime.rs:41-55`) does not
  yet include it.
- **Value:** a JSON array of strings, or `null` when the upstream published no
  modalities. An empty array is a distinct statement from `null`.
- **Vocabulary:** `text`, `image`, `audio`, `video`, `pdf`. Fusiform keeps any other
  string verbatim as an unknown modality instead of rejecting it; broca ignores
  unrecognized entries.
- **broca reading:** `image` gates kind `image`, `pdf` gates `document`, `audio` gates
  `audio`, `video` gates `video`. An absent, `null` or malformed value reads as unknown
  for every modality; it never excludes the row.

Sources (Fusiform repository at commit `75d934eb3caffe4e9ed3fbaf7c19665130776e20`):
`crates/fusiform-module/src/lib.rs:219-244` (the `catalog.get` tool schema and its
`fact_prefixes` argument), `docs/wire-contract.md:117-128` and
`crates/fusiform-protocol/src/lib.rs:1069-1077` (the fact and its value),
`crates/fusiform-core/src/lib.rs:127-153` (recognized spellings),
`crates/fusiform-store/src/ingest.rs:425-427, 524-550` (null versus empty list, unknown
strings kept).

The models.dev snapshot path reads `modalities.input` with the same vocabulary
(https://github.com/anomalyco/models.dev, the `[modalities] input` list in each
`providers/<provider>/models/<model>.toml`).
The two sources are never merged.

## Fixed token estimates where a provider documents no usable rule

**broca choice.** Used per item when a family's entry says "fixed estimate", either
because the provider documents no rule or because its rule needs data broca does not
extract (page count, duration, frame rate).

| kind | fixed estimate (tokens per item) |
|---|---|
| `image` | 4,800 |
| `document` | 30,000 |
| `audio` | 16,000 |
| `video` | 64,000 |

The image value sits just above the largest fixed per-image cost documented below
(Anthropic's high-resolution tier cap, 4,784; Gemini 3's default is 1,120 and
DeepSeek's cap 1,024). OpenAI's patch rule can exceed it, but OpenAI families use their
own rule. The others are round budgets: about ten dense PDF pages, about eight minutes
of audio at Gemini's 32 tokens per second, and about four minutes of video at Gemini's
263 tokens per second.

## Profile selection

A profile is chosen from the frozen `WireFamilyId`, a frozen provider quirk, and a
per-family wire-model-pattern table for documented generation splits. Two splits
exist today: Gemini 3 versus earlier Gemini (tool results), and the ChatGPT backend's
`input_audio` (live-gated flag).

Providers on `openai_chat` share one grammar, so the family entry below gives the
grammar, and each provider profile is told apart by a frozen media quirk keyed by the
provider. Today's frozen quirks cannot do this alone: DeepSeek and Kimi carry identical
`reasoning_echo` fields, and the Llama API carries no `openai_chat` quirk at all
(`crates/broca-provider/src/policy.rs:44-79`, `crates/broca-catalog/src/lib.rs:519-593`).

| profile | media quirk value | catalog provider ids (`crates/broca-catalog/data/overlay.json`) |
|---|---|---|
| OpenRouter | `openrouter` | `openrouter` |
| xAI | `xai` | `xai`, only for models the overlay overrides to `openai_chat` (its provider default is `openai_responses`) |
| DeepSeek | `deepseek` | `deepseek` |
| Kimi | `kimi` | `moonshotai`, `moonshotai-cn`, `kimi-code-plan-global`, `kimi-code-plan-cn` |
| Meta | `meta` | `llama` (Llama API, `https://api.llama.com/compat/v1/`) |

A provider on any family with no profile entry gets the conservative profile: a
person's media is refused (`carrier`) and tool media renders as markers.

---

## `anthropic_messages` (Anthropic Messages API)

**Live check:** pending operator-run results; native image/PDF carriers are documented,
not yet verified by this probe.

Sources: https://platform.claude.com/docs/en/build-with-claude/vision ·
https://platform.claude.com/docs/en/build-with-claude/pdf-support ·
https://platform.claude.com/docs/en/agents-and-tools/tool-use/handle-tool-calls ·
https://platform.claude.com/docs/en/api/overview (request size limits)

| | image | document | audio | video |
|---|---|---|---|---|
| user-message shape | `{"type":"image","source":{"type":"base64","media_type":M,"data":B64}}` | `{"type":"document","source":{"type":"base64","media_type":"application/pdf","data":B64}}` | not supported | not supported |
| MIME types | png, jpeg, gif, webp (animated: first frame only) | `application/pdf` (no passwords or encryption) | — | — |
| tool result | native: the same `image` block inside `tool_result.content` | native: the same `document` block inside `tool_result.content` | marker | marker |
| per-item limit | 10 MB base64-encoded; 8000×8000 px | covered by the request cap | — | — |
| count limit per request | 600 images (100 for models with a 200k-token context window) | 600 PDF pages (100 when the request's context window is under 1M tokens); pages, not documents | — | — |

- **Count-dependent dimension limit:** when a request holds more than 20 `image`
  blocks, every image must be at most 2000 px on each side. Images from earlier turns
  and images nested in `tool_result` count toward the 20.
- **Request cap:** 32 MB (Messages endpoint).
- **Tool results for computer-use and browser-use toolset members** accept only
  `text` and `image` blocks, and the API rejects an oversized `tool_result` image there
  instead of downscaling it. broca's tool results are ordinary client tools, so this
  does not apply unless broca sends those toolsets.
- **Token estimate:**
  - image: `min(⌈w/28⌉ × ⌈h/28⌉, 4,784)`. Anthropic bills `⌈w/28⌉ × ⌈h/28⌉` visual
    tokens after downscaling to the model's tier (high-resolution tier, Claude 4.7 and
    later: at most 2,576 px long edge and 4,784 tokens; standard tier: 1,568 px and
    1,568 tokens). The cap at 4,784 makes the estimate an upper bound for both tiers.
    The older documented approximation `w × h / 750` gives almost the same value
    (28² = 784) and is superseded by the patch rule.
  - document: fixed estimate (30,000). Anthropic documents 1,500–3,000 text tokens per
    page plus the page's image cost, which needs a page count broca does not extract.

## `bedrock_converse` (AWS Bedrock Converse)

**Live check:** pending operator-run results; native image/PDF carriers are documented,
not yet verified by this probe. The current serving plan omits Bedrock because
there is no vault credential (`no_credential`); it is not live-checked.

Sources: https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_Message.html ·
https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_ContentBlock.html ·
https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_ImageBlock.html ·
https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_DocumentBlock.html ·
https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_AudioBlock.html ·
https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_VideoBlock.html ·
https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_ToolResultContentBlock.html ·
https://docs.aws.amazon.com/nova/latest/userguide/modalities-video.html ·
https://platform.claude.com/docs/en/api/overview (Bedrock request size)

| | image | document | audio | video |
|---|---|---|---|---|
| user-message shape | `{"image":{"format":F,"source":{"bytes":B64}}}` | `{"document":{"format":"pdf","name":N,"source":{"bytes":B64}}}` | `{"audio":{"format":F,"source":{"bytes":B64}}}` | `{"video":{"format":F,"source":{"bytes":B64}}}` |
| formats (`format` value ← MIME) | `png`, `jpeg`, `gif`, `webp` | `pdf` ← `application/pdf` (other document formats are out of scope) | `wav`, `mp3`, `aac`, `flac`, `ogg`, `m4a`, `mpeg` (the API also lists opus, mp4, mkv, mka, x-aac, mpga, pcm, webm) | `mp4`, `mov`, `webm`, `flv`, `mpeg`, `mpg`, `wmv`, `three_gp`, `mkv` |
| tool result | native: `image` in `toolResult.content` (documented for Amazon Nova and Anthropic Claude 3 and 4) | native: `document` in `toolResult.content` | marker (`ToolResultContentBlock` has no audio member) | native: `video` in `toolResult.content` |
| per-item limit | 3.75 MB; 8000×8000 px | 4.5 MB | not documented | one video per request whose whole payload stays within 25 MB (Amazon Nova) |
| count limit per request | 20 images | 5 documents | not documented | 1 (Amazon Nova) |

- **Count-dependent dimension limit:** for Claude models on Bedrock, Anthropic's rule
  applies, and document blocks count toward the 20: above 20 image-or-document blocks,
  each image must be at most 2000 px per side. The Converse message limit of 20 images
  already keeps most requests at or below that threshold.
- **Request cap:** 20 MB (Anthropic's stated Bedrock request limit). Amazon Nova's
  25 MB payload limit is larger, so 20 MB covers both.
- Images and documents may appear only in `user` messages.
- The Converse reference notes the `image` member as supported by Anthropic Claude 3
  models. Whether a given Bedrock model accepts a kind is decided by the catalog
  modality, not by this table.
- **Token estimate:** image: the Anthropic rule `min(⌈w/28⌉ × ⌈h/28⌉, 4,784)` (an
  upper bound for Claude; Bedrock documents no rule for other models). Document, audio,
  video: fixed estimates.

## `openai_responses` (OpenAI Responses API)

**Live check:** pending operator-run results; native image/PDF carriers are documented,
not yet verified by this probe. The Meta Responses profile is separately untested
and its follow-up-user carrier remains a marker.

Sources: https://developers.openai.com/api/docs/guides/images-vision ·
https://developers.openai.com/api/docs/guides/file-inputs ·
https://developers.openai.com/api/docs/guides/function-calling ·
https://developers.openai.com/api/reference/resources/responses/methods/create

| | image | document | audio | video |
|---|---|---|---|---|
| user-message shape | `{"type":"input_image","image_url":"data:M;base64,B64"}` | `{"type":"input_file","filename":N,"file_data":"data:application/pdf;base64,B64"}` | not supported (no `input_audio` part in the Responses reference) | not supported |
| MIME types | png, jpeg, webp, non-animated gif | `application/pdf` (other `input_file` types are out of scope) | — | — |
| tool result | native: `function_call_output.output` as an array of `input_text` / `input_image` / `input_file` parts | native: `input_file` in that array | marker | marker |
| per-item limit | at most 30,000 32-px patches after the model's resizing; above that the API rejects the image | under 50 MB per file | — | — |
| count limit per request | 1,500 images | all files together at most 50 MB | — | — |

- **Count-dependent dimension limit:** none documented.
- **Request cap:** 512 MB total payload is documented for images, but files are
  limited to 50 MB combined. **broca choice:** the profile uses 50 MB, the smaller
  documented bound, for every request.
- **Token estimate:** image: `⌈1.2 × min(⌈w/32⌉ × ⌈h/32⌉, 30,000)⌉`. OpenAI's current
  models bill 32-px patches times a 1.2 multiplier, after resizing to the model's
  detail budget (for example 2,500 patches at `high`, 10,000 at `original` on
  `gpt-5.5`; `auto` is `original` on the newest models). Using the raw patch count,
  capped at the 30,000-patch rejection limit, gives an upper bound for patch-billed
  models. Older tile-billed models (`gpt-4o`, `gpt-4.1`, `gpt-5.1`) use base tokens
  plus tokens per 512-px tile instead; `gpt-4o-mini`'s large per-tile count (5,667) can
  exceed this bound. Document: fixed
  estimate (PDF input bills extracted text plus page images, which needs a page count).
- Provider `meta` (`https://api.meta.ai/v1`) is on this family. Its limits are the
  Meta Model API's (see the Meta profile under `openai_chat`), not OpenAI's.

## `openai_responses_chatgpt` (ChatGPT / Codex backend)

**Live check:** pending operator-run results. `input_audio` remains off and its frozen
MIME allowlist empty. Probe candidates are `audio/wav` and `audio/mpeg`, in both user
messages and tool results; neither is accepted until measured live.

The ChatGPT backend takes the Responses body at the ChatGPT Codex endpoint and has no
public reference. The primary source is OpenAI's own Codex client, which serializes
these items directly into that body:
https://github.com/openai/codex/blob/ffda54f5b8d5595eb0963594de32e206f98cc77f/codex-rs/protocol/src/models.rs
(`ContentItem`, lines 883-912; `FunctionCallOutputContentItem`, lines 2249-2269).

| | image | document | audio | video |
|---|---|---|---|---|
| user-message shape | `{"type":"input_image","image_url":"data:M;base64,B64"}` | not evidenced (Codex sends no `input_file`) | `{"type":"input_audio","audio_url":"data:M;base64,B64"}`, **live-gated** | not supported |
| MIME types | as `openai_responses` | — | not documented; the live check fixes the accepted types | — |
| tool result | native: `input_image` in `function_call_output.output` | marker | `input_audio` in `function_call_output.output`, **live-gated** | marker |
| per-item limit | as `openai_responses` | — | not documented | — |
| count limit per request | as `openai_responses` | — | not documented | — |

- **`input_audio` flag:** off until a live check against the real backend passes.
  With the flag off, a person's audio is refused (`carrier`) and a tool's audio is a
  marker.
- **Count-dependent dimension limit:** none documented.
- **Request cap:** not documented. **broca choice:** 50 MB, as `openai_responses`.
- **Token estimate:** image: the `openai_responses` rule. Audio: fixed estimate.

## `openai_chat` (OpenAI Chat Completions grammar)

**Live check:** pending operator-run acceptance and positive cache-stability results.
OpenAI's tool-media follow-up-user carrier remains a marker.

Sources: https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create ·
https://developers.openai.com/api/docs/guides/images-vision ·
https://developers.openai.com/api/docs/guides/file-inputs ·
https://developers.openai.com/api/docs/guides/audio-chat-completions

This entry is the grammar every provider profile below uses, and the profile for
OpenAI's own Chat Completions endpoint.

| | image | document | audio | video |
|---|---|---|---|---|
| user-message shape | `{"type":"image_url","image_url":{"url":"data:M;base64,B64"}}` | `{"type":"file","file":{"filename":N,"file_data":"data:application/pdf;base64,B64"}}` | `{"type":"input_audio","input_audio":{"data":B64,"format":"wav"\|"mp3"}}` | not supported |
| MIME types | png, jpeg, webp, non-animated gif | `application/pdf` only | `audio/wav`, `audio/mpeg` | — |
| tool result | not supported: a `tool` message's content is a string or `text` parts only. Follow-up user message (live-gated) or marker | same | same | marker |
| per-item limit | as `openai_responses` | under 50 MB | not documented | — |
| count limit per request | 1,500 images | 50 MB for all files together | not documented | — |

- **Count-dependent dimension limit:** none documented.
- **Request cap:** **broca choice** 50 MB, as `openai_responses`.
- **Token estimate:** image: the `openai_responses` rule. Document and audio: fixed
  estimates.

### `openai_chat` profile: OpenRouter (quirk `openrouter`)

**Live check:** pending operator-run acceptance and cache-stability results; tool media
remain markers. Another provider's pass does not enable this profile.

Sources: https://openrouter.ai/docs/guides/overview/multimodal/image-understanding ·
https://openrouter.ai/docs/guides/overview/multimodal/pdfs ·
https://openrouter.ai/docs/guides/overview/multimodal/audio ·
https://openrouter.ai/docs/guides/overview/multimodal/videos ·
https://openrouter.ai/docs/api/reference/overview

| | image | document | audio | video |
|---|---|---|---|---|
| user-message shape | `image_url` with a data URL | `file` with `file_data` data URL (works on any model; OpenRouter parses the PDF when the model has no native file input) | `input_audio` with `data` and `format` (base64 only) | `{"type":"video_url","video_url":{"url":"data:M;base64,B64"}}` |
| MIME types | png, jpeg, webp, gif | `application/pdf` | `wav` documented; other formats vary by upstream | `video/mp4` documented; others vary by upstream |
| tool result | not supported: the API reference types `tool` message content as a string ("ContentParts are only for the user role"). Follow-up user message (live-gated) or marker | same | same | same |
| per-item limit | varies by upstream; not documented | not documented | not documented | not documented |
| count limit per request | varies by upstream; not documented | not documented (the `mistral-ocr` engine forwards at most 8 page images) | not documented | not documented |

- **Count-dependent dimension limit:** not documented.
- **Request cap:** not documented. **broca choice:** 20 MB, the smallest request cap
  among the upstream families in this table (Bedrock, Gemini), since OpenRouter routes
  to them.
- **Token estimate:** fixed estimates for every kind; cost depends on the upstream
  model.

### `openai_chat` profile: xAI (quirk `xai`)

**Live check:** pending operator-run acceptance and cache-stability results; tool media
remain markers. Another provider's pass does not enable this profile.

Sources: https://docs.x.ai/developers/model-capabilities/images/understanding ·
https://docs.x.ai/developers/rest-api-reference/inference/chat-completions

| | image | document | audio | video |
|---|---|---|---|---|
| user-message shape | `image_url` with a data URL | not documented on Chat Completions | not documented | not documented |
| MIME types | `image/jpeg`, `image/png` only | — | — | — |
| tool result | not documented: follow-up user message (live-gated) or marker | marker | marker | marker |
| per-item limit | 20 MiB | — | — | — |
| count limit per request | no limit | — | — | — |

- **Count-dependent dimension limit:** none documented.
- **Request cap:** not documented. **broca choice:** 20 MiB, one maximum-size image.
- **Token estimate:** image: fixed estimate (xAI documents no rule).
- Most xAI models are on `openai_responses` in the overlay. This profile covers only
  the models the overlay overrides to `openai_chat`.

### `openai_chat` profile: DeepSeek (quirk `deepseek`)

**Live check:** pending operator-run acceptance and cache-stability results; tool media
remain markers. Another provider's pass does not enable this profile.

Sources: https://api-docs.deepseek.com/guides/vision ·
https://api-docs.deepseek.com/api/create-chat-completion

| | image | document | audio | video |
|---|---|---|---|---|
| user-message shape | `image_url` with a data URL (or `file` with `file_data` holding an image) | not supported | not supported | not supported |
| MIME types | jpeg, png, gif, webp (detected from content) | — | — | — |
| tool result | conflicting: the API reference lets a `tool` message carry image parts, but the vision guide says images are supported in user messages only. Follow-up user message (live-gated) or marker until a live check settles it | marker | marker | marker |
| per-item limit | 32 MiB | — | — | — |
| count limit per request | 600 images; 64 MiB of inline images in total | — | — | — |

- **Count-dependent dimension limit:** at most 8192 px per side; at most 4096 px per
  side when the request holds 15 or more images.
- **Request cap:** 48 MiB request body.
- **Token estimate:** image: 1,024 (DeepSeek resizes every image and states an upper
  bound of 1,024 tokens per image, counted independently).

### `openai_chat` profile: Kimi (quirk `kimi`)

**Live check:** pending operator-run acceptance and cache-stability results; tool media
remain markers. Another provider's pass does not enable this profile.

Sources: https://platform.kimi.ai/docs/guide/use-kimi-vision-model ·
https://platform.kimi.ai/docs/api/chat

| | image | document | audio | video |
|---|---|---|---|---|
| user-message shape | `image_url` with a data URL | not supported | not supported | `{"type":"video_url","video_url":{"url":"data:M;base64,B64"}}` |
| MIME types | jpeg, png, gif, webp (also bmp, heic, heif, outside broca's image set). Animated GIF and WebP may be billed as video | — | — | `video/mp4`, `video/mpeg`, `video/mov`, `video/avi`, `video/x-flv`, `video/mpg`, `video/webm`, `video/wmv`, `video/3gpp` |
| tool result | not documented (examples show text only): follow-up user message (live-gated) or marker | marker | marker | follow-up user message (live-gated) or marker |
| per-item limit | not documented; recommended at most 4096×2160 | — | — | not documented; recommended at most 1920×1080 |
| count limit per request | no limit on images | — | — | not documented |

- **Count-dependent dimension limit:** none documented.
- **Request cap:** 100 MB request body.
- **Token estimate:** fixed estimates (Kimi documents dynamic counting through its
  estimate-tokens endpoint, not a formula).

### `openai_chat` profile: Meta (quirk `meta`)

**Live check:** pending operator-run acceptance and cache-stability results; tool media
remain markers. This does not cover the separate Meta Responses profile.

The `llama` provider's Llama API documentation now redirects to the Meta Model API
documentation, which covers both Chat Completions and Responses.

Sources: https://dev.meta.ai/docs/image-understanding ·
https://dev.meta.ai/docs/file-handling ·
https://dev.meta.ai/docs/video-understanding ·
https://dev.meta.ai/docs/protocols/chat-completions

| | image | document | audio | video |
|---|---|---|---|---|
| user-message shape | `image_url` with a data URL | `file` with `file_data` data URL | `input_audio` with `format` `wav` | `video_url` with a data URL |
| MIME types | jpeg, png, gif, webp (also `image/x-icon`, outside broca's set) | `application/pdf` | `audio/mpeg`, `audio/wav` | `video/mp4` |
| tool result | not documented: follow-up user message (live-gated) or marker | same | same | same |
| per-item limit | 50 MB | 50 MB inline | 50 MB inline | 50 MB inline |
| count limit per request | 50 images; a PDF contributes up to its first 50 page images to this budget | (page images count toward the 50) | not documented | not documented |

- **Count-dependent dimension limit:** none documented.
- **Request cap:** not documented beyond the 50 MB inline file limit. **broca
  choice:** 50 MB.
- **Token estimate:** fixed estimates. Meta documents only measured examples (a
  ~1280 px image adds roughly 1,300–1,500 tokens) and points to its token-count
  endpoint.

## `gemini` (Gemini API and Vertex AI `generateContent`)

**Live check:** pending operator-run results. Gemini 3's native image/PDF tool carrier
is documented but unverified here; earlier Gemini's follow-up-user carrier stays
a marker until its own acceptance and cache-stability check passes.

Sources: https://ai.google.dev/gemini-api/docs/image-understanding ·
https://ai.google.dev/gemini-api/docs/document-processing ·
https://ai.google.dev/gemini-api/docs/audio ·
https://ai.google.dev/gemini-api/docs/video-understanding ·
https://ai.google.dev/gemini-api/docs/file-input-methods ·
https://ai.google.dev/gemini-api/docs/tokens ·
https://ai.google.dev/gemini-api/docs/media-resolution ·
https://docs.cloud.google.com/vertex-ai/generative-ai/docs/multimodal/function-calling
(section "Multimodal function responses") ·
https://docs.cloud.google.com/vertex-ai/generative-ai/docs/multimodal/image-understanding ·
https://docs.cloud.google.com/vertex-ai/generative-ai/docs/multimodal/audio-understanding ·
https://docs.cloud.google.com/vertex-ai/generative-ai/docs/multimodal/video-understanding ·
https://github.com/googleapis/python-genai/blob/main/google/genai/types.py
(`FunctionResponsePart`, `FunctionResponseBlob`)

| | image | document | audio | video |
|---|---|---|---|---|
| user-message shape | part `{"inlineData":{"mimeType":M,"data":B64}}` | same `inlineData` part, `mimeType` `application/pdf` | same `inlineData` part | same `inlineData` part |
| MIME types | png, jpeg, webp (also heic, heif, outside broca's set; gif is not listed) | `application/pdf` | `audio/wav`, `audio/mp3`, `audio/mpeg`, `audio/aiff`, `audio/aac`, `audio/ogg`, `audio/flac`, `audio/m4a` | `video/mp4`, `video/mpeg`, `video/mov`, `video/avi`, `video/x-flv`, `video/mpg`, `video/webm`, `video/wmv`, `video/3gpp` |
| tool result, Gemini 3 and later | native: `{"functionResponse":{"name":…,"response":{…},"parts":[{"inlineData":{"mimeType":M,"data":B64}}]}}` (preview on Vertex); accepted types image/png, image/jpeg, image/webp | native: `functionResponse.parts` with `application/pdf` | marker (not an accepted `functionResponse.parts` type) | marker (not accepted) |
| tool result, earlier Gemini | not supported: `functionResponse` has no `parts`. Follow-up user message (live-gated) or marker | same | marker | marker |
| per-item limit | 7 MB inline (Vertex AI; the Gemini API states only the request cap) | 50 MB; 1,000 pages | bounded by the request cap | bounded by the request cap |
| count limit per request | 3,000 images (Vertex AI; the Gemini API allows 3,600) | 1,000 pages over all documents | 1 audio file (Vertex AI) | 10 video files (Vertex AI; the Gemini API states 10 for Gemini 2.5 and later) |

- **Wire-model pattern:** wire model ids starting `gemini-3` (and later major
  versions) use the Gemini 3 tool-result row; every other Gemini model uses the
  earlier row. A `functionResponse` part that carries media in its structured
  `response` may also reference a part by `{"$ref": displayName}`; broca does not need
  that form.
- **Count-dependent dimension limit:** none documented.
- **Request cap:** the image, audio and video guides state 20 MB total for inline
  data (prompt, system instruction and inline bytes); the file-input-methods guide
  states 100 MB per request (50 MB for PDFs). **broca choice:** 20 MB, the smaller
  documented bound.
- **Token estimate:**
  - image, Gemini 3: 1,120 per image (the default `unspecified` media resolution).
  - image, earlier Gemini: 258 when both sides are at most 384 px; otherwise
    `258 × ⌈w/768⌉ × ⌈h/768⌉` (768×768 tiles at 258 tokens each).
  - document: fixed estimate. Gemini 3 bills 560 tokens per page by default and
    earlier models 258 per page, which needs a page count.
  - audio: fixed estimate. Documented rate: 32 tokens per second (Gemini 3: 25 per
    second), which needs a duration.
  - video: fixed estimate. Documented rate: 263 tokens per second (258 per frame at
    1 frame per second plus 32 for audio; Gemini 3 default: 70 per frame), which needs
    a duration.

## `google_codeassist` (Antigravity / Code Assist)

**Live check:** pending operator-run results for both generation profiles. All Code
Assist production media gates remain unset; a PNG/PDF pass cannot enable untested
audio/video carriers. Without a resolved project ID, the standalone probe reports
`skipped: project_id_required` before credential access; its dry-run still renders
the family's three planned requests.

The Code Assist endpoint (`cloudcode-pa`) wraps a Gemini `generateContent` request in
an envelope and has no public reference. The primary source is Google's own client,
which converts a `generateContent` request into that envelope unchanged:
https://github.com/google-gemini/gemini-cli/blob/main/packages/core/src/code_assist/converter.ts
(`toGenerateContentRequest`).

- **Shapes, MIME types and tool-result rows:** as `gemini`, including the Gemini 3
  versus earlier split. Every media row is **live-gated**: the endpoint is
  undocumented, so a person's media is refused (`carrier`) and tool media is a marker
  until a live check passes for that kind.
- **Per-item and count limits:** not documented. **broca choice:** the `gemini`
  values.
- **Request cap:** not documented. **broca choice:** 20 MB, as `gemini`.
- **Token estimate:** the `gemini` rules.
