# broca as a subc module

This document describes the module's session and run contracts, durable lineage
model, and coordinator invariants.

---

## 1. What it is

broca is an **orchestration tier**: a subc module that runs agentic LLM loops for
consumer modules while consuming tools, credentials, and model-routing services.
Its agentic core is shared with the `broca-run` CLI.

### 1a. Runtime precondition: a model catalog must be reachable ONCE

A FRESH broca CANNOT SERVE ANY MODEL UNTIL IT HAS A CATALOG. This is a precondition on the
environment, not on the caller, and it does not appear in any wire contract — so a harness that
builds broca from source in a clean environment meets it as an admission failure reading
`model catalog unavailable: no in-memory catalog`, with nothing naming the cause.

The catalog is served by `fusiform` and cached on disk at
`$BROCA_STATE_ROOT/catalog/fusiform-lkg.json`. The LKG survives restarts and the
poller refreshes it every 30 minutes. A new state root needs a catalog before it
can serve models.

Either of these satisfies it:

- `fusiform` reachable over subc at least once, so the poller can write the LKG; or
- a seeded `fusiform-lkg.json` in the state root, which is what broca's own e2e rig does.

A build-from-source deployment must provide a reachable catalog or seed the LKG
file before serving models.

## 2. Two consumer roles

- **Drivers** start or steer work and inject prompts.
- **Observers** read session content and watch it live.

A consumer can be both. The surface is **session-centric**: consumers address
*sessions*; a *run* is an episode.

## 3. Foundational model: a session is a LINEAGE OF RUNS (turns)

- **Session / lineage** — the durable conversation. One single-writer **lease**, one
  **WAL**, one **store** partition, keyed `{project_root, harness, session}`.
- **Run = one turn's loop execution** — a user message in → the agent loops until its
  response → a terminal. Each run begins with a `RunStarted` **episode marker** (input +
  frozen config + `origin` + `submission_id`). Runs **accumulate** in the lineage WAL:
  `RunStarted₁ …turn-1… RunFinished₁ RunStarted₂ …turn-2… …`. **One ACTIVE run per
  lineage at a time** (lease-enforced); idle between runs. **Chosen: one run per turn.**
- **Episode state machine:** replay walks the WAL
  as a sequence of episodes. Each `RunStarted` **closes/validates the previous episode,
  resets per-episode state (terminal flag, open tool batches, finish state), and appends
  the previous episode's messages to the lineage PREFIX.** Only the **last** episode is a
  resume/continue target; all prior episodes are immutable conversation prefix. A durable
  `RunFinished` for episode N must NOT mark the lineage finished — it marks episode N
  finished. There is an explicit **append-new-episode** path (idle `session.send` after a
  finished episode writes a fresh `RunStarted`), distinct from resume-the-last-unfinished.
- **Conversation history** = the prefix across all closed episodes ⊕ the current episode.
- **Submission** = one prompt injection. PENDING (durably queued, retractable) → consumed
  into a run (coordinator writes `RunStarted{submission_id}`) → ACTIVE → TERMINAL.
- **Session coordinator** (§12) — the in-process lineage owner that drains the durable
  queue and starts each run.
- **Fork** (future) — a separate lineage `SeededFrom` a parent; v4 lands the durable
  `origin` field (§18); the cross-lineage prefix read is deferred.

## 4. The wire surface

Unary responses that exceed subc's 64 MiB frame-body cap fail only their correlated
call with an `Error` whose code is `response_too_large`. No oversized reply bytes
are sent, and sibling calls keep using the connection. This also bounds oversized
error messages. The cap is `subc-protocol` 0.31.0's `MAX_FRAME_BODY_LEN`.

A TCP reset on either half instead ends the connection with the classified
`reset` serve cause, not a generic serve error. Sibling calls cannot survive an
RST: [RFC 9293 §3.10.7.4](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.10.7.4)
requires outstanding SEND/RECEIVE operations to receive reset responses and moves
TCP to CLOSED. An unrelated write error remains a serve error.

```
session.read { from_ordinal?, limit?, include_tools?, after_mid?, lineage_id?, max_bytes?,
                 include_originals? }
    → { messages: [{ ordinal, mid, message,
                     tool_calls?: [{ tool_call_id, call_key?, dispatched_to?, indeterminate }] }],
        next_from_ordinal?, head?, lineage_id?, lineage_state,
        tools?: [{ name, description, input_schema, module? }], tools_run_id? }
                                                                     (OBSERVER; §15)

session.head {} → { lineage_id?, last_ordinal?, head?, last_run_state?, updated_at?,
                       cache?: { last_sent_at_ms, ttl_ms?, expires_at_ms?, expiry_basis },
                       title? }
session.warm { lineage_id? }
    → { request_body_sha256, warm_body_sha256, cache_read_tokens?, cache_write_tokens?,
        input_tokens?, ttl_ms, expires_at_ms, expiry_basis: "estimated" } (§4a)
run.result { run_id } → { run_id, state, reason?, error?, final_message?: { ordinal, mid, text } }
session.baseline {} → { state: "not_yet" | "ready", baseline? } (owner only)
role.describe {} → the llm-runner/v1 major, its ops and capability groups, max_bytes

session.subscribe { from?: "start" | "live" | cursor } (dedicated route; §9)
    → stream of events (control reliable+replayable, display live+lossy); see §7,§8,§16

session.send { session, prompt, system?, send_id?, cache?, model?, tool_providers?, stop_when?, seed_from?, context_management?, delivery?, conversation_key?, title? }
    title?: 1-200 Unicode scalar values, not blank, no control characters; else invalid_params {field: "title"}
    → { state: "active" | "pending" | "finished", run_id?, submission_id?, reason?, baseline? } (DRIVER; §11)

session.retract { session, submission_id }
    → { retracted: true } | { retracted: false, reason: "already_started" } (§11)

run.cancel { run_id } → { ack } (cooperative → Interrupted; §13)
run.status { run_id | session }
    → { state, run_id?, step?, last_error?, head: cursor, title? } (state enum §14)
```

The `llm-runner/v1` role ops (`role.describe`, `session.head`, `run.result`,
`session.baseline`) and the newer `session.read` members follow commons
`cortexkit-role-llm-runner/CONTRACT.md`. In broca's terms:

- `after_mid` reads strictly after the message `m<ordinal>` of the lineage `lineage_id` names;
  a read naming another lineage is refused `lineage_changed` whatever its mode, an unknown mid
  `unknown_mid`, and misuse `invalid_params` with `detail.field`. Every refusal writes nothing.
- A transcript page is also capped by bytes, measured over the complete serialized reply body
  (default and maximum **3 MiB / 3,145,728 bytes**). Larger `max_bytes` requests are capped to
  that maximum. Messages and late approvals share the budget, including tools, attribution,
  head fields and continuation cursors. A range page stops early with `next_from_ordinal`;
  a tail page keeps its newest messages. One message and one late entry, when present, are
  always kept whole, never truncated. Oversized atomic items, their combined minimum, or
  unpaged metadata can therefore exceed the budget; this is not a strict frame-size guarantee.
- `tool_calls[].call_key` is `<lineage_id>:<wal_seq>` of the call's own `tool_dispatch_intent`,
  derived on read and never stored. `dispatched_to` is that intent's `module_id`, absent on an
  intent written before broca recorded it. Sending `call_key` to providers waits until each
  provider decodes it. A call is tied to the step record that produced its message, never
  matched by content, so a message an import brought in has no dispatch facts. Facts of a turn
  in the read's WAL tail come with the page; an older turn's come from a per-session cache that
  one pass over the whole WAL fills the first time a read needs them.
- `run.result`'s `final_message.text` joins the final assistant message's text parts with
  nothing inserted, leaving reasoning out; a completed run with no text answers `""`.
  It is unpaged and not bounded by `session.read.max_bytes`.
- `session.baseline` and the admitting send's `baseline`: sessions are admitted without a fetch
  plan, so `items` is empty and `composition_digest` absent; `tool_names` are the first
  episode's tools; both generations are 0. The send reply carries it only when the session's
  first episode runs under the send's own run id, from what the session's actor holds; it
  reads nothing.
- `session.head` reads the store's last row and the WAL past the store's watermark. For a
  retained session, `updated_at` is the durable activity that sets its expiry: lifecycle/send
  records advance it, but cache warms and other maintenance do not. Retained content is
  WAL-only, so that tail holds its policy. For sessions without retention, the value still
  uses the live WAL file's write time; only a folded session whose tail holds no timestamped
  record reads its whole WAL. Its `title` is the newest title in that WAL tail, else the one
  the store's projection folded (`meta.last_title`).
- A `steer` or `interrupt` into a session that has an episode takes every runner parameter it
  omits (`model` with its variant, `tool_choice`, `generation`, `stop_when`, `cache`, `budget`,
  `context_limit`, `work_class`, `service_tier`) from the session's latest frozen config, so its
  turn renders the session's request prefix. `tools` and `context_management` already inherit
  on omission for every send. Any other send must name `model`. When such a send is queued
  behind a running turn, its recorded request carries the frozen model itself, so a build
  that does not know the inherited set still runs it on the session's model.

### 4a. `session.warm`

A provider caches a request's prefix for a fixed lifetime from when the request was sent. A
caller whose next round arrives later than that pays to write the whole prefix again. A warm
re-sends the session's most recent model request changed only so the provider generates nothing
(Anthropic: `max_tokens: 0`, no `stream`); everything that shapes the cached prefix (system,
tools, messages, `cache_control` markers and their TTLs, adaptive thinking, effort, speed) is
byte-identical. The caller schedules and caps warms; broca performs each one exactly and reports
what it cost.

- **Base.** While a run is generating, the exact bytes its in-flight attempt sent (their digest
  is the `request_body_sha256` that attempt's `model_attempt_finished` records). Otherwise the
  session's last request, rebuilt from the WAL and rendered from the frozen config; it is sent
  only if it renders to the digest its step recorded, else refused `warm_prefix_unavailable`. A
  running run is never a refusal.
- **Refusals**, each decided before anything is sent and with a pinned golden body under
  `crates/broca-wire/tests/golden/warm_refusal.*.json` (`code` equals `detail.kind`):
  `lineage_changed`, `run_paused`, `account_walled` (the latest attempt hit
  `account_limit_exhausted` and `resets_at_ms` is still ahead), `warm_in_flight` (another warm
  of the session has not finished; one runs at a time), `account_changed` (the
  credential, read again as a run reads it, belongs to another account than the session froze),
  `no_step_yet`, `warm_unsupported` (any provider but first-party Anthropic; Bedrock Converse,
  which has no documented zero-output request; manual thinking, a forced `tool_choice` or a
  structured output format), `cache_expired` (the estimated expiry is unknown, or under 5 s
  away: a warm then would be a full cache write) and `warm_prefix_unavailable`. A warm that was
  sent and that the provider refused answers `warm_failed` with the provider's error.
- **Concurrency.** The session's actor only decides refusals and snapshots what the warm needs;
  the warm itself runs on its own task, so `run.cancel`, sends, steers and status for the
  session are never held behind a slow provider. Its completion comes back to the actor, which
  writes the record through the session's single writer and moves the clock; a warm finishing
  after the next run started is still recorded and billed under the run it re-sent.
- **Expiry.** The lifetime runs from the warmed request's send start; a successful warm restarts
  it at its own send start. `expiry_basis` is always `"estimated"`. The clock lives in memory, so
  after a restart it is unknown (and warms are refused `cache_expired`) until the session sends
  again.
- **Records.** Each successful warm writes one informational, ungated `cache_warmed` WAL record
  (digests, usage, account, the raw `anthropic-ratelimit-unified-*` headers, expiry). Replay,
  `session.read` and projection skip it; it is never a transcript message. Billing turns it into
  its own export fact marked `warm: true`, priced from its own usage, whose id is taken over the
  record's WAL sequence so the run's ordinary facts keep their ids. Absent usage stays absent.
- **Head and declaration.** `session.head.cache` reports the clock from memory, with no WAL
  scan. The admitting send's `baseline` and `session.baseline` carry `cache {ttl_ms,
  ttl_options_ms, read_refreshes_ttl, warm: "supported" | "unsupported", basis}`, every member
  absent when unknown; `basis` is `requested` when the session's cache request named the
  lifetime and `provider_default` when the provider's default applies.

A **cursor** is a durable `(wal_seq, sub_index)` (§7). `subscribe` and `read.head` share
this one cursor domain. A cursor attach starts strictly after that unit, so
`read → subscribe(from: head)` is a gapless, duplicate-free handoff. `head` is absent only
when the lineage has no control unit.

`session.send.system` is an OPTIONAL leading `Role::System` message for the turn: one
byte-exact text block, delivered role-scoped before the user prompt; absent, empty, or
whitespace-only means no system message.

`session.send.send_id` is an OPTIONAL client-chosen idempotency key, unique per LOGICAL
send. A retry of the same logical send reuses it VERBATIM; the module derives the same
`run_id`, so a byte-identical retry is an idempotent replay of the same run. Reusing a
`send_id` with DIFFERENT content is a typed error (the old run_id/send_id was already used
by another send). Omitting `send_id` leaves idempotency off and the server mints a fresh
`run_id`. `title` is not part of the request identity: a keyed resend with an edited title
returns the recorded answer and keeps the first title.

A keyed resend of a terminal run returns `{state:"finished", run_id, reason}` without
starting another episode. `reason` is the recorded `RunFinishReason` snake_case value
(for example, `completed` or `interrupted`). Active retries remain `active`; paused
retries resume and return `active`. Callers must decode `state` as an open set and
handle unknown states without assuming a run is active or waiting indefinitely.

A new send with non-empty legacy `tools` is refused as `scoped_tools_require_plan` with
`detail: {class: "permanent"}` when it would run under the route's stamped scope or the
session's frozen scope without a fetch plan. Tools on a scoped session must come from a
fetch plan, because tool providers require a per-call preset on scoped routes; only the
frozen plan manifest supplies those presets. A send carrying a plan, or using a session's
already-frozen manifest, keeps the existing plan checks. Scope and plan refusals take
precedence, and keyed resends of already-admitted runs keep their existing answer.
This refusal precedes any WAL write or catalog fetch. Unscoped legacy sends and scoped
sends with absent or empty tools remain admitted, including steers, interrupts and queued
sends that declare no tools. Flow sends retain their own `invalid_params` tool refusal.
On a scoped, planless session admitted by an older build, a later send declaring non-empty
tools is refused before it can freeze that list, even if it queues or steers; it cannot
retrofit a plan (the existing `plan_drift` rule still applies). Omitting tools does not
revalidate or replace an older run's frozen tool list.

`session.send.conversation_key` optionally selects routing's provider-account affinity across
sessions. When present it must be a string of 1–256 bytes, all printable ASCII `0x21`–`0x7E`
(no spaces). Invalid values, including null or non-strings, are refused before any WAL write
with `invalid_params {field: "conversation_key"}`. Absent inherits the latest episode's frozen
key from the durable admission snapshot, without an additional full-WAL read. If that episode
predates key recording, the key is derived from the session as before; older episodes are not
searched. Episodes admitted with a recorded key carry it forward for subsequent sends.
The key is part of send identity: reuse of a `send_id` with a different key is refused as
`send_id_reuse` naming `conversation_key`. A queued send retains it durably until admission,
including across a crash. A steer joining a running run accepts the key but ignores it for
routing, since no admission takes place; an interrupt admits a new episode with its own key
(or the inherited key if absent). `RunStarted.auth_selection.account_decision` records the
frozen `conversation_key` and `conversation_key_source: "caller" | "session" | "inherited"`,
absent on old records. The source is an open string so future values survive replay.
Raw keys are stored in the WAL on `Queued` and `RunStarted`, not in usage exports.

`session.send.title` optionally names the work a send carries, for dashboards and logs, on
every delivery mode. When present it must be a string of 1–200 Unicode scalar values, not all
whitespace, with no control character (newline and tab included). Anything else, including null
or a non-string, is refused before any WAL write with `invalid_params {field: "title"}`; a title
is never truncated. It is recorded on the record the send writes (`run_started`, `queued` or
`steered`, absent when the send carried none) and on the run's index row. A queued send's title
is carried onto the `run_started` that drains it. It is INFORMATIONAL ONLY: never rendered into
a provider request, and not part of routing, `conversation_key`, cache keys or send identity, so
a retry under a known `send_id` with a different title gets the recorded answer, is not refused
`send_id_reuse`, and keeps the first title. Run-keyed `run.status` returns the run's `title`
when one was recorded (from the run index, else the run's `run_started`), and `session.head`
returns the session's newest title across all three record kinds. Absent stays absent in both,
never `""`. Usage export facts do not carry it.

`session.send.delivery` is OPTIONAL: `"queue"` (the default; absent means this), `"steer"` or
`"interrupt"`. It decides what the message does when a run is already active on the session.
Any other value is refused as `invalid_params` naming `context_management`. The mode is part of the send's
identity: the same `send_id` with another mode is refused `send_id_reuse` naming `delivery`.

- `queue`: the message is durably queued and started as the next turn once the active run
  ends; the reply is `pending`. Unchanged from a send without the field, byte for byte.
- `steer`: the message joins the RUNNING turn. It is written as a `steered` WAL record (synced
  before the reply) and the model sees it as a user message at the turn's next step boundary,
  never between a tool call and its result; a final answer given while it is pending is
  followed by one more step to render it, within the run's step and budget limits. The reply
  is `active` naming the RUNNING run (the run that carries the message, not a new one). A steer
  acknowledged this way is guaranteed to be in the transcript; it reaches a model in that turn
  only if another step follows (a cancel, error or limit ends the turn first, and the next turn
  then sees it as history). No new turn is started for it. A retry answers `active` while that
  run lives and its `finished` once it has ended. With no run active it is a plain send; with
  the last run PAUSED it is refused `run_paused` (`detail: {kind: "run_paused", run_id,
  reason}`), nothing written.
- `interrupt`: the message is durably queued FIRST (a `queued` record with `delivery:
  "interrupt"`), then the running run is cancelled through the `run.cancel` path (it ends
  `cancelled`; a running tool call is waited on up to its deadline, never killed), then the
  message starts as the next turn, ahead of any plain queued sends. The reply is `pending`. A
  paused run is ended the same way. With no run active it is a plain send. A crash at any
  point neither loses the message nor runs it twice; an unfinished interrupted run is sealed
  `interrupted` on restart.

A session whose last run is PAUSED (awaiting re-authentication) takes no new message: any
`session.send` that is not the paused run's own retry is refused `run_paused` (`detail: {kind:
"run_paused", run_id, reason}`, nothing written), for `queue` and `steer` alike. The caller
re-authenticates and resends the paused run's own `send_id` (which resumes it), cancels it with
`run.cancel`, or sends with `delivery: "interrupt"`, which ends it and starts the message.

`session.send.cache` is OPTIONAL admission-only metadata: `{role?: "primary" | "ephemeral",
ttl?: "5m" | "1h" | "24h"}`. `primary` means an interactive or long-lived session and resolves to
the long tier when the provider supports it; `ephemeral` means a subagent-class session and
resolves to the short tier. The resolved policy is frozen before the run starts, survives queued
submissions, and is ignored on resume because the prior frozen render config is authoritative.
`"24h"` is accepted only on OpenAI's own API-key routes (Responses and Chat), where it renders
`prompt_cache_retention: "24h"`; the ChatGPT/Codex backend rejects that field with HTTP 400, so
there, and on every other provider, the send is refused at admission.

`session.send.context_management` is OPTIONAL and picks how the session stays inside the model's
context window (design and rationale: `docs/context-management-design.md`):

- `"none"`: no management. A run that reaches the window ends with a context-overflow error.
- `"compaction"`: broca-native summarise-and-continue. Accepted by the parser, and refused at
  admission (`context_management_unavailable`) until the mode exists. Never run as `"none"`.
- `{"transform": "<module id>"}`: a context-management value broca does not serve. It still
  parses, because older `RunStarted` records carry it. A send naming it, a later send inheriting
  it, and a resume of an episode frozen onto it are all refused by name with
  `context_management_unavailable` before anything runs, rather than run as `"none"`.

Any other value is refused as `invalid_params` naming `context_management`. Unknown FIELDS on
`session.send` are still tolerated; only this field's value is strict.

The session's FIRST episode binds the value, and it is frozen onto every `RunStarted`
(`config.context_management`). A later send that omits the field inherits it; one carrying
the same value is accepted; one carrying a different value is refused permanently with
`context_management_changed`, naming both values. A resume always runs under the frozen
value and never reads process configuration. Every refusal happens before `RunStarted`, so a
refused send leaves no episode. A send that queues behind an active run is admitted when it
drains, so its refusal happens then (recorded as a failed queued admission), exactly as a
queued changed system prompt is.

With the field omitted on a session's first send, the first match wins:

1. `broca.jsonc` `context_management.projects["<project_root>"]`, keyed by the session's
   `project_root` as broca receives it (the daemon canonicalises it; a trailing `/` is ignored);
2. `broca.jsonc` `context_management.default`;
3. `"none"`.

```jsonc
{"context_management": {
    "default": "none",
    "projects": {"/workspace/scratch": "none"}
}}
```

A malformed section, a `"compaction"` default, or a `{"transform":...}` default refuses boot
by name. Changing `broca.jsonc` later never changes a session that already bound its value.

A session whose last `RunStarted` carries no `context_management` binds its next new episode to
`"none"` and freezes it; `broca.jsonc` defaults never reach it, and a differing explicit value
is refused like any change. A resumed episode of such a session runs as `"none"`.

`session.send.model.variant` is an OPTIONAL reasoning variant name, sent inside the `model`
object as OpenCode sends it: `"model": {"provider": "openai", "model": "gpt-5.6-luna",
"variant": "xhigh"}`. There is no top-level spelling. The variant travels through the queue,
is recorded on the run's `RunStarted` config (`variant`, absent when not sent), and a resume
keeps the recorded value. A different variant under a reused `send_id` is a different request,
reported as a changed `variant`.

**Which names a model accepts** comes from its catalog `capability.reasoning_options` (fusiform
serves models.dev's `reasoning_options` verbatim), derived exactly as OpenCode derives its
variants (`packages/core/src/models-dev.ts`, `reasoningVariants`):

- an `effort` entry gives one variant per string value, in order, plus `none` first when the
  model also has a `toggle` and its family has a toggle mapping; a `null` inside `values` is
  never a variant;
- otherwise a `budget_tokens` entry gives `high` (half the maximum, at least the published
  minimum) and `max` (the smallest of the published max, the model's output limit minus one,
  and 31,999);
- otherwise a lone `toggle` gives `none` and `thinking`;
- an entry type broca does not know contributes nothing and is logged once (`broca.catalog`).

**What each name sends** is decided by the wire family (`crates/broca-provider/src/variant.rs`,
following OpenCode's `settingsForEffort` / `toggleVariants` / `settingsForBudget`):

| Family | effort `<e>` | toggle `none` / `thinking` | budget `<n>` |
|---|---|---|---|
| Anthropic Messages | `output_config: {effort: <e>}`, plus `thinking: {type: adaptive, display: summarized}` on Claude 4.6 and later | `thinking: {type: disabled}` / `thinking: {type: adaptive, display: summarized}` | `thinking: {type: enabled, budget_tokens: <n>}` |
| OpenAI Responses and ChatGPT codex | `reasoning: {effort: <e>, summary: auto}`, `include: [reasoning.encrypted_content]` | none | none |
| OpenAI Chat and compatibles | `reasoning_effort: <e>` | none | none |
| Gemini (Code Assist: see below) | `generationConfig.thinkingConfig: {includeThoughts: true, thinkingLevel: <e>}` | `{includeThoughts: false, thinkingBudget: 0}` / `{includeThoughts: true, thinkingBudget: -1}` | `{includeThoughts: true, thinkingBudget: <n>}` |
| Bedrock Converse | none | none | none |

A variant replaces the Anthropic OAuth default `thinking: {type: enabled, budget_tokens: 1024}`.
The resolved parameters are frozen into the run's render config at admission; a resume renders
from the frozen value and never re-reads the catalog.

**Refusal.** A variant the model does not define is refused at admission, before anything is
written, naming the model and its variants: `model 'anthropic/claude-opus-4-6' does not define
variant 'max'; its variants are [none, low, medium, high]`. A model whose options are `null`
(or not served) or `[]` defines no variants, so any variant is refused. These permanent
`session.send` refusals use error code `variant_unsupported` and structured detail:
`{"model":"anthropic/claude-opus-4-6","variant":"max","defined":["none","low","medium","high"]}`.
`defined` is empty when the model defines no variants. The human message keeps its existing
`open failed: run resolution failed: ` prefix and wording. No `RunStarted` or other WAL record
is written, and no provider request is made. Also refused (as `open_failed`, not
`variant_unsupported`): an Anthropic budget variant when the output cap does not exceed the budget. A ChatGPT `-pro` id is not a variant: it resolves
only if the catalog lists that exact model, and maximum reasoning is `model.variant: "max"`.
Omitting the variant renders exactly what the same send rendered before variants existed.

**Code Assist (Antigravity) renders a variant as a backend tier.** Which levels a model
accepts still comes from its catalog row; fusiform serves alias rows for several Antigravity ids
(`antigravity-gemini-3.8-flash` carries `gemini-3.8-flash`'s facts). How a level is sent is the
family's: Code Assist selects a different backend model id by tier suffix, so `high` on
`antigravity-gemini-3.8-flash` sends `gemini-3.8-flash-high` with that tier's thinking settings,
exactly as if the caller had spelled the tier in the id. With no variant the bare id keeps its
default tier (`gemini-3.8-flash-medium`). A level the row defines but the route has no tier for
(`minimal` on `antigravity-gemini-3.6-flash`) is refused with the same `variant_unsupported`
code and detail, naming the tiers the route has in the message. `defined` still lists the
model's variants, not the route's tiers. A
level the row does not define is refused as above.

Two cases are NOT refused and NOT applied: the id has no catalog row at all (e.g.
`antigravity-gemini-3.5-flash`, `antigravity-gemini-3.1-pro`), or the family has no tiers for
the model (`antigravity-claude-sonnet-4-6-thinking`). The variant is recorded on `RunStarted`,
the request is exactly the one sent without a variant, and the module warns once per (model,
variant) (`broca.wire`: "recorded on the run but NOT applied").

## 5. Reliability model (the egress reality)

subc module→client egress is best-effort `try_send` into a 64-deep buffer; a full client
queue **CLOSES** that client (verified subc-core router.rs/server.rs). So **"reliable
control lane" = DURABLE + REPLAYABLE**, never guaranteed-live — a dropped consumer
**re-subscribes from its last cursor and replays**. **Display = live-only + lossy**
(coalesce/drop + `DisplayGap`; no per-token durability).

### 5.1 Display loses bytes two ways, and only one of them announces itself

`DisplayGap` is not the enumeration of display loss, it is one member of it.

1. **Ring lag** — the subscriber falls behind, broca drops deltas and delivers an in-band
   `DisplayGap { dropped }`. Self-announcing: the evidence is in the stream.
2. **Stream end** — the subscription ends (detach, route teardown, reconnect) and a new one
   begins at the **live edge of the display ring**, which bears no relation to the client's
   last processed byte. **The amount lost is unbounded and unrelated to how long the reattach
   took**: it spans everything from the client's last delta to the current write position,
   including deltas that were already buffered before the disconnect. Reasoning "the
   reconnect was fast, so little was lost" is the wrong model. And **the new stream contains
   no evidence that anything preceded it** — there is no marker broca can send, because the
   gap exists *between* two subscriptions rather than within one, and the engine cannot know
   whether a client that reattached is the same renderer resuming or a fresh one starting.

**Why this is a correctness requirement and not a fidelity nicety.** Concatenating display
text across a stream end is not merely lossy at the byte level — it is corrupting at the
*parse* level. An unterminated construct before the hole can adopt a delimiter from after it
(an unclosed code fence closing on a later fence), so the render is **wrong rather than
short**, and a reader cannot tell anything is missing. Any lossy lane feeding a stateful
parser converts subtractive loss into structural corruption; that is a property of the
pairing, so neither side's tests find it alone.

**What a client must do.** The only witness to a stream-end gap is the client's own reattach.
That imposes a *structural* requirement, not a behavioural one:

- **The reattach must be observable to whatever holds the render state.** A transport layer
  that reconnects transparently is behaving correctly for every other purpose — reattach is
  genuinely irrelevant to dispatch, ordering, and control-lane delivery — so a layer boundary
  drawn on any other axis will cut this event off. It has to cross a boundary it has no other
  reason to cross, which is why this is easy to violate without making a mistake.
- **Mark when a run is in flight, not when you hold partial text.** The second rule is exactly
  wrong in the case where the loss was total: a reattach during an active run with an empty
  live tail is indistinguishable from one where everything streamed so far was lost. Marking
  under that uncertainty is the honest reading.
- **Do not mark an idle-session reattach.** There is no torn construct to repair, and a false
  gap in unbroken prose is its own fidelity bug.
- **Evaluate the in-flight test against state as of the disconnect, not after processing the
  resumed stream.** The control replay may carry the run's own terminal, which would
  retroactively classify a torn run as an idle one and skip the mark in exactly the case that
  needed it. This is an ordering property rather than a state one, so it is invisible at the
  call site: a client whose guard happens to sit before its resubscribe is correct by
  accident and stays correct only until someone reorders the two.

**The repair is guaranteed only on a completing run.** The control lane's `AssistantMessage`
carries the authoritative text and replaces the streamed tail wholesale, so on a normal run a
marker is visible only for the seconds between reattach and that message. A run cancelled or
failed mid-step never produces one — the assembled message is projected from
`ModelStepFinished`, which that run never reaches — so **the torn tail is what the transcript
keeps, and the marker is permanent**. Do not size the effort by the seconds-long case.
Display is a latency optimisation, never a source of truth — which is also why the lane will not be made replayable: durable
per-token replay would let a slow subscriber backpressure the single-writer loop, and the
reliable lane already carries the complete text.

## 6. Multi-client fan-out + per-subscriber backpressure

subc is 1:1, no broadcast → **broca owns the fan-out**. Per subscriber: **display
lossy** (coalesce/drop + `DisplayGap`); **control bounded-then-DETACH** the route (it
resubscribes from its cursor) — never blocks the run, never silently drops control. One
slow subscriber never affects the run or co-subscribers.

## 7. The control stream: durable cursor + in-process live tail

The control stream is sourced from the **enriched WAL** (§18 adds the missing facts) via
a **deterministic projection** `WAL+store → Vec<ControlUnit>`, addressed by a **durable
cursor `(wal_seq, sub_index)`** — one record can project to several ordered ControlUnits,
disambiguated by `sub_index`. This cursor is **durable and lineage-scoped**: it is what a
consumer holds and **resubscribes with**, and it **survives the coordinator's
release/re-open** (§12) because it is derived from the WAL, not from process memory.

For the **not-yet-fsynced live tail** of the active run, the coordinator keeps a small
**in-process event log**: the loop appends each event to it **non-blocking** (NOT
`await out.send` per event — the #13 producer-stall fix), and the fan-out task drains it.
On fsync, those events acquire their durable `(wal_seq, sub_index)`. The **attach
barrier** (under the coordinator lock): capture the durable head cursor `H` and the live
tail; replay the projection `≤ H`, then stream the live tail, **deduplicating by the
durable cursor** for any tail event that crossed `H` during attach. **The in-process live
seq is internal only** — it is never handed to a client and never used as a resubscribe
cursor (so a re-open's reset cannot break a client). A cursor attach is strictly after the
named unit. `from:"live"` resolves to strictly after the last projected unit at `H`, so it
replays no history and waits for future units. Display events are live-only (not in the
projection — a finished turn shows assembled text, not re-typed tokens).

## 8. Replay scope (honest)

`from:"start"` (or a cursor) = the durable projection (§7) from that point, then the live
tail. The projection is **semantically equivalent, not event-identical** to the original
live stream (display tokens are not reproduced). The §18 WAL enrichment is what makes it
faithful for step boundaries, finish reasons, tool names, and error detail.

## 9. Subscribe routing

`session.subscribe` is a **dedicated long-lived stream route**, never multiplexed with
`cancel`/`status`/`read`. `from:"live"` is future-only and remains quietly attached on an
idle or terminal lineage until a later run publishes units (§16).

Stamped `route.bind` authorization runs outside frame ingress. Until that check succeeds
and broca acknowledges the bind, its channel/epoch is not published: a request arriving
early is dropped as unbound (or stale if another epoch is still published), not queued
for dispatch after authorization. That never drops a real request, because the daemon
forwards nothing on a route before the module acknowledges its bind: `route.open` waits
for the module's `RouteBindAck` before it returns the channel and epoch to the client,
and while the bind is pending the daemon refuses any request on that channel with
`unknown_channel` instead of forwarding it. broca records the route as bound before it
sends `RouteBindAck`, so a client's first request after the acknowledgement finds it.

A route `GOODBYE` closes only the channel/epoch it names. A pending replacement can be
canceled without closing the published route or its stream, and closing the published
route does not cancel authorization of a different pending epoch. After the replacement
is accepted, a `GOODBYE` naming the old epoch is stale.

Route requests authorize the caller before method parameter validation or migration
checks. In particular, an unauthorized `session.subscribe` reports the authorization
refusal even when its parameters are invalid. A replacement subscription cancels the
active stream only after authorization and validation both succeed; a refusal leaves
deliveries on the original stream correlation.

## 10. Admission barrier + early failure

Idle `session.send` returns `{state:"active", run_id}` **only after** the run acquires
the lease and writes durable `RunStarted`. **Model/auth resolution happens BEFORE
`RunStarted`** so a resolution failure is a typed `session.send` error, not a ghost run.
A failure *after* `RunStarted` is a legitimate short terminal run answerable via
`run.status`, error persisted.

### 10.1 A vanished project root refuses WRITES and still serves READS

If a session's `project_root` no longer exists on disk, the split is by operation class,
not by session:

| refused at admission | still served |
| --- | --- |
| `session.send`, `session.import` | `session.read`, `session.subscribe`, `run.status`, `run.cancel`, `session.retract` |

The refusal is a typed `invalid_project_root` naming the exact path. Recreating the
directory revives the session; nothing about it was destroyed.

**This is deliberate.** Writes create durable state under a key derived from the bind
triple, and appending new state beneath a root that is gone (or running tools there) is
how a lineage ends up describing a place that does not exist. Reads have no such
problem: the WAL is the archive and does not live under the project root, so a
transcript outlives the directory it was recorded in.

**What a consumer must not infer.** `!root.exists()` does NOT mean the session is dead
or unopenable. It opens, pages its full history, and only continuation fails. A UI that
treats a missing root as a corpse and offers to prune it has the advice backwards: the
transcript still reads, and forgetting the record is then the only action that loses it.


### 10.2 A WAL written by a newer broca is refused by name

If a session's WAL holds a complete frame that requires WAL features this build does not
know, a newer broca wrote it. Every operation that reads or opens that WAL refuses with
error code `wal_newer_writer`: `session.read` (which reads the WAL without opening the
session) and every operation that opens the session, `session.send`, `run.status`,
`session.subscribe`, `session.retract`, `run.cancel` and `session.import`, including a
send refused at admission for the same reason. The body is:

```json
{"code":"wal_newer_writer","message":"open failed: wal range-read failed: wal written by a newer broca (NewerWriter): the version 3 frame at offset 69 requires features this build does not know: probe-unknown/v1","detail":{"kind":"newer_writer","missing":["probe-unknown/v1"]}}
```

`detail.missing` names the features the refused frame requires. The class is `permanent`:
the bytes are intact, nothing was written, and retrying cannot help, so the remedy is to
place a broca that knows every missing feature. Branch on `code`, then `detail.kind`. The
message keeps the wording the refusal carried before it had its own code and is never
parsed. These exact bytes are pinned by `crates/broca-wire/tests/golden/wal_newer_writer.json`.

Every other failure to open a session's WAL, corruption included, still answers
`open_failed`, with the same message. When the producer knows the remedy, it adds
`detail.class`: `transient`, `permanent`, `auth_required`, or `context_overflow`.
Admission and run-resolution failures use `RunError::admission_error_class`; for example,
an OpenAI Chat provider that cannot honour cache preferences returns `permanent`.
Temporary WAL read interruptions return `transient`. Corruption and errors whose causes
were erased remain unclassified: `detail` is absent and the entire legacy body is unchanged.
Absent is not a retry recommendation; consumers retain their fallback only for absent classes.
The served classed bytes are pinned by `open_failed_permanent.json` and
`open_failed_transient.json` under `crates/broca-wire/tests/golden/`.
One path keeps its existing code: when the session is already open,
`session.subscribe` attaches and then replays the WAL in the stream, and a frame it cannot read
there, a newer writer's included, ends the stream with `subscribe_replay_failed`, as before.

## 11. Submission lifecycle + durable queue + retract

- **Idle send** → starts a run (active, run_id).
- **Running send** → durably queued as **`Queued{submission_id, message}`** (a WAL
  record carrying IDENTITY, not just the message). State "pending".
- **Retract** → a durable **`Retracted{submission_id}`** tombstone, taken **atomically
  under the coordinator lock** (remove-if-pending vs `already_started`). Crash-safe.
- **Consume** → the coordinator writes **`RunStarted{submission_id, run_id, …}`**
  referencing the submission + emits `SubmissionStarted{submission_id, run_id}`.
  A drained run's id is `"run-" + submission_id` (the submission id itself carries a
  `sub-` prefix, so the id reads `run-sub-…`). The string formula is diagnostics-only:
  consumers correlate a queued send to its drained run by matching `submission_id`,
  never by parsing the run id.
- **Replay is unambiguous:** the pending set = `Queued − Retracted − consumed-by-a-RunStarted`.
  A crash BEFORE the consuming `RunStarted` ⇒ still pending (re-drained on re-open); a
  crash AFTER ⇒ that run is active/resumable, never double-consumed. FIFO; each queued
  prompt becomes its own episode.

## 12. Session coordinator = a lineage actor

One in-process **lineage actor** per open session with states **`Opening | Open |
Closing | Closed`**. ALL `session.send`/`retract`/`subscribe` for a lineage go through
its actor; a teardown cannot start while an op holds the actor. Lease lifetime: the actor
holds the lease while there are **subscribers OR pending work OR within a short idle
TTL**; otherwise it transitions `Closing` → releases the lease → `Closed`; the next op
re-opens (`Opening` → reacquire → replay). Races handled by the actor:
- A send arriving as the actor is `Closing` → it waits/re-opens, never races a half-torn
  state.
- Two concurrent re-opens of one lineage → the actor registry admits exactly one
  `Opening` (the other awaits it).
- A subscriber across a release → its route is closed on `Closing`; it resubscribes from
  its durable cursor on re-open.
**Defense-in-depth:** the lease's OS lock is the primary single-writer guard, and WAL append
checks the file-integrity high-water mark so a draining old actor is rejected after a new actor
has written. Each record also carries the lease epoch as a forensic tenure stamp; FileWal does
not compare that stamp with an authority or reject an append solely because it is stale.

## 13. Cancel token + provider watchdog (core change)

Add a **`CancellationToken`** into `run_loop_with_store`, checked at turn boundaries and
around dispatch → `Interrupted` → `RunFinished{Interrupted}` at a safe WAL boundary
(cooperative; the durability core's fail-to-doctor — never a false `Cancelled`). Add a
**per-run provider-call timeout/watchdog** (today the model-stream await is unbounded and
pins the lease until process death; the tool path already has a 10s timeout) that
cooperatively cancels at a safe boundary, releasing the lease.

## 14. run.status enum + durable run index + busy-vs-stale

Startup recovery seals runs left in flight by a previous process as `interrupted`, with
an optional `error` diagnostic on `run.status`: `{class: "transient", provider_code:
"previous_process_ended_unsealed", message: "the previous broca process ended without sealing
this run: host crash, kill, or power loss"}`. This uses the existing WAL `RunFinished.error`,
not a new terminal reason. The control lane projects the diagnostic as `ControlUnit::Error`
before `RunFinished{reason: Interrupted}`; that final reason remains authoritative. Export
facts carry `terminal_reason: "interrupted"`, but no interruption diagnostics. Graceful seals
keep their existing behavior and omit this diagnostic.

- **State enum:** `Idle | Active{run_id, step} | Paused{run_id, reason} | Interrupted{run_id} |
  Terminal{run_id, reason} | Error{run_id, detail}`. `run.status(session)` resolves the
  in-process active run first, then WAL inference.
- **Durable run index** in `store.db`: `run_id → {session, lineage, submission_id, state,
  wal_path}` so run-keyed ops survive restart. Resume **reuses the original run_id**
  (today it mints `req.run_id`).
- **Busy-vs-stale:** `session.send` on a live-active lineage returns `session_busy{run_id,
  state}` vs `lease_stale`.
- **Eventual-consistency contract (run-keyed `run.status{run_id}`):** the response reflects
  the **coordinator's PROCESSED lifecycle state**, eventually consistent with the durable WAL
  within ~one command-loop iteration. The run-status resolution checks the live actor's state
  FIRST (it's authoritative for a running run), then the durable index, then WAL inference.
  Because a subscriber observes `RunFinished` from the WAL projection (the writer-observer
  path) which is **decoupled** from the actor's command loop, a run that JUST emitted
  `RunFinished` on its subscription stream may briefly report `Active` via `run.status` until
  the coordinator processes its own terminal command (which writes the index terminal and goes
  idle in one atomic handler). **The durable terminal is IMMEDIATELY authoritative on the
  WAL/stream** — only this convenience read-path lags. This window is structurally absent for
  the case `run.status{run_id}` exists to serve — querying a run NOT in a live subscription
  (a finished/crashed run): there the actor is already idle or closed, so its live state
  doesn't match and the index/WAL resolve the terminal directly with no lag. A consumer
  needing the synchronous terminal reads the stream; a consumer polling `run.status` for an
  unwatched run sees the terminal immediately. The decoupling is deliberate (coupling the
  projection to the actor state would reintroduce a double-publish), so this is a stated
  contract, not a defect.
- **`TransformUnavailable` is read, never written:** older WAL records and run-index rows carry
  it, and readers report it (`run.status`, `session.read`, the run index), but this build never
  ends a run with it. It is terminal, not a pause: a pause is only for a failure with a
  caller-side resolver contract (`AuthRequired` → the caller re-authenticates, then resumes the
  SAME run).
- **Active model-step progress:** run-keyed `run.status` can include an optional `step` object
  `{step_id, started_at_ms, attempt, attempt_started_at_ms, last_content_at_ms?}`. It is a cheap
  snapshot of the live actor's in-memory model call, never inferred from WAL. Timestamps are
  Unix milliseconds; `attempt` is zero-based and its content timestamp resets on reissue.
  No in-flight model step means no object, and no real content yet means no content timestamp.
  The real-serve fixture is `crates/broca-wire/tests/golden/run_status.active_with_step.json`.

### Attempt content telemetry

`model_attempt_finished` optionally carries `first_content_at_ms`, `last_content_at_ms`,
`content_bytes`, `non_content_bytes`, `event_types`, `non_content_bytes_after_last_content`,
and `events_after_last_content`. Provider and model come from the run's `run_started`.
These fields are informational and need no reader feature. Older readers ignore them.

Content is explicitly classified per wire family: text, reasoning/summary, function-argument,
and refusal deltas. Lifecycle events, completed item snapshots, signatures, usage, pings and
keepalive comments are not content. Unknown event types are non-content and carry `unknown:true`
in the tally. SSE and WebSocket data bytes count UTF-8 payloads (including JSON envelopes), not
SSE framing; SSE keepalives count comment bytes. Bedrock counts event JSON payload bytes.
The tally holds the top sixteen event types by count, ties ordered by name, plus an `other`
bucket for remaining types. Each entry has `event_type`, `count`, measured `bytes`, and optional
`unknown`. WebSocket ping/pong are exposed by the socket pump without their payload lengths:
their counts are observed but their byte count, and any aggregate containing it, stay absent.
Exact counts are retained until the attempt ends so a late frequent event can enter the top sixteen.

No observed content means absent content timestamps, content byte count and suffix counters,
not synthetic zeroes. Once content is observed, the suffix counters reset to measured zero;
subsequent non-content records increment them. A byte class never observed stays absent.
This telemetry and the attempt log's `last_content_ms` do **not** change the existing idle or
content deadlines, retry policy, or rate-floor policy.

## 15. session.read consistency + pagination

`session.read` cuts each page from one atomic materialized-store snapshot plus its exact
bounded WAL tail. The response's optional `head: {wal_seq, sub_index}` is the last durable
control unit covered by that same snapshot. `session.subscribe {from: head}` therefore
starts strictly after the read snapshot without a race, gap, or duplicate.

Paging uses message ordinals, independently of the control cursor. With no arguments, read
returns the **newest** page: `start = prompt_len - limit` (saturating at zero). An explicit
`from_ordinal` returns the forward range `[from_ordinal, from_ordinal + limit)`, capped at
`prompt_len`. `next_from_ordinal` is present only when the returned range ends before
`prompt_len`. To walk backward, first fetch the newest page, take its first ordinal `s`,
then request `from_ordinal = max(0, s - limit)` and repeat from each page's first ordinal.
There is no direction field, and `session.read` refuses one with `invalid_params`.

**Leniency is per-operation, not a property of the surface.** `run.status` and
`session.read` are STRICT, because absence there changes the question: a misspelled `run_id`
produces `run_id: None` (SESSION status), and a misspelled `from_ordinal` produces the newest
page instead of the range asked for. Either way the caller gets a well-formed answer about the
wrong subject. `session.send` stays lenient for the opposite reason: it accepts additive optional fields, and a newer caller against an older daemon must degrade
rather than fail every call. Lenient is not silent: the module logs each top-level
`session.send` key it does not read, and each unread key inside `model`, at warn (`broca.wire`),
naming the key and the caller's harness, once per key per process. `model.variant` is read, so it
is never reported as unknown. Before making an operation strict, verify that its
callers do not send unsupported fields and check compatibility tests.

Each item carries both `ordinal` and `mid: "m<ordinal>"`; `mid` is the MC-coordinate
spelling used by consumers. Chunking is required for large transcripts because subc has a
per-frame cap and closes a full egress queue.

**Declared tools (opt-in).** `include_tools: true` adds `tools` and `tools_run_id` to the
response: the tool list frozen on the durable `RunStarted` of the session's LATEST episode
(`name`, `description`, `input_schema`, and `module` when the episode recorded one), in
recorded order, and the run whose `RunStarted` it came from. It is copied from the WAL record
as written, never rebuilt (the run index keeps only a digest of the tools). The list comes
from the same snapshot as the page; when the latest episode began inside the materialized
prefix, this costs one extra WAL read bounded at the store's watermark, which is why it is
opt-in. A session with no episode omits both fields (never `tools: []`); an episode admitted
with no tools returns `tools: []`. If the store holds history but the WAL can no longer show
the `RunStarted`, the read fails rather than omit `tools`, since omission would claim the
session never ran. Without the flag the response bytes are exactly what they were before the
flag existed, and the request omits `include_tools` when false. `include_tools` is a known
field only in that spelling: `ReadParams` stays strict, so an older daemon refuses it with
`invalid_params` instead of silently answering without tools.

## 16. Subscribe to a terminal run

Terminal state does not special-case attach behavior. `from:"start"` or a historical cursor
replays every strictly eligible durable unit, including `RunFinished`/`Paused`, and then
stays quietly attached for a later episode. `from:"live"` resolves at the attach barrier to
strictly after the current durable head, so it replays no terminal or other history and
stays quietly attached. Idle pinning keeps either stream open; a subsequent `session.send`
can append another episode whose units arrive on the same subscription.

## 17. Auth failure mid-run = RunPaused, resumable

A mid-run `needs_reauth`/revoke writes a **NON-terminal durable `RunPaused{run_id,
reason: AuthRequired, error}`** record (NOT `RunFinished` — replay treats `RunFinished`
as terminal/no-op, so a "resumable terminal" would seal the run). Replay gains a
`ReplayResolution::AuthRequired`: the episode is **paused, not finished**; after re-auth,
resume **continues the SAME run_id from the last durable boundary**; no new episode starts
until the pause is resolved or cancelled. The error detail is persisted for `run.status`.
(Auto pause→refresh→continue is deferred; the lineage is preserved + manually resumable.)

## 17b. The metering lane: direct read-only storage

`usage.export` was removed in release **0.3.156**. Requests to that method receive the
normal `unknown_method` refusal; there is no compatibility flag or replacement RPC.
Consumers read `export_facts` in `run-index.db`, opening SQLite with `mode=ro`.
`export_meta` retains the source identity and high-water mark for incremental readers.
The table spans all sessions in the daemon.

Facts are usage-only — no pricing, no money — because a consumer prices them with its own table and a
price frozen here would be wrong the moment that table changed.

**Reading a fact's timestamps.** `occurred_at_ms` is optional and `occurred_at_precision` does
NOT tell you whether it is present: the precision field describes how a timestamp would be
derived, so it reads `terminal` even on facts that carry none. A consumer deciding whether a
fact can be placed in a time window MUST test `occurred_at_ms` directly. The undateable facts
are a closed historical set (the backfill that re-derived facts for runs predating envelope
timestamps), so the share shrinks as traffic accumulates and cannot grow. `pricing_at_precision`
is the field that does track its timestamp: every fact without `pricing_at_ms` reads `degraded`.

**Cursor identity.** `source_instance_id` binds a cursor to one export database; it is minted
once at database creation and survives every process restart, so a changed value means the
database was replaced, not that the daemon bounced. An incremental reader whose saved identity
no longer matches must reset its position rather than continue from a sequence in the previous
database. `export_seq` is insertion order, NOT event order — a catch-up backfill assigns fresh
high sequence numbers to old runs — so ordering a report by `export_seq` orders by when broca
learned a fact, and ordering by `occurred_at_ms` orders by when it happened.

**Delivery.** Facts are insert-once and never rewritten, so a re-read of the same range returns
the same bytes. A terminal run is expected to produce its facts; a run that ends without any is
a defect on this side, not a consumer-side filter to work around.

## 18. WAL enrichment + fork origin (real, now)

Record changes:
- `RunStarted` gains `origin: Fresh | SeededFrom{source_lineage, at_seq}` (default
  `Fresh`; `seed_from` reserved on the wire, v1 rejects non-Fresh — the fork seam is real,
  cross-lineage read is future) and `submission_id` (§11), and is run_id-authoritative.
- Replay-fidelity additions: a `StepStarted` marker; `finish_reason` on
  `ModelStepFinished`; `tool_name` on `ToolDispatchIntent`; typed `error: Option<ProviderError>`
  on `RunFinished`.
- Queue identity: `Queued{submission_id, message}`, `Retracted{submission_id}` (wire the
  formerly-dead `SteerQueued`/`FollowUpQueued` into this shape).
- `RunPaused{run_id, reason, error}` (§17).

## 19. SubcToolPlane hardening

Fence keyed `(session, tool_call_id)` **with eviction** on batch/run terminal; **per-route
window** resolved from the specific provider at route-open (not memoized once); **route
eviction** (TTL/LRU + GOODBYE on idle) against `u16` channel exhaustion; **route
invalidation on GOODBYE/status** (learn a dead route); document the per-module
`store.db` `Mutex<Connection>` as a scalability limit (read/write split or pool if it
bites).

**Reverse requests are refused, not dropped.** Broca opens its tool and vault
routes with no reverse-request capabilities, because it fronts no person who could answer
an elicitation or a sampling request. A `Request` a module sends toward broca on a route
channel is answered at once with an `Error` frame carrying code `reverse_request_unhandled`
and "no reverse-request handler is registered for <family>", the same reply the subc Rust
consumer SDK sends. It never reaches a pending call, even one with the same corr, and each
refusal is counted in `health.check` as `reverse_requests_refused`.

## 20. Persistence + storage

```
~/.local/share/cortexkit/broca/
  store.db ← sessions + the durable run index + projection cursor state
  wal/<project-id>/<session>.wal ← per-session lineage WAL (sequence of runs)
  leases/<project-id>/… ← single-writer lease files
```
WAL = source of truth (portable unit); DB = derived projection. Resume = DB-prefix ⊕
WAL-tail.

The state root shown above is the default. It resolves to `BROCA_STATE_ROOT` when set and
non-empty, else `$XDG_DATA_HOME/cortexkit/broca`, else `~/.local/share/cortexkit/broca`, and
broca refuses to start when none is available (`crates/broca-module-serve/src/state_root.rs`).
Astrocyte reads `run-index.db` at that data-home path, so the location is a contract. Older
builds defaulted to `broca-state` beside the subc connection file; when only that old
directory holds a `wal/` or `run-index.db` and `BROCA_STATE_ROOT` is unset, broca refuses to
start and names both paths.

## 21. Core, WAL, and store invariants

Additive WAL changes must retain the existing compatibility rules:
1. **Episode-aware replay state machine**: per-`RunStarted` episode reset; prior
   episodes → prefix; last-unfinished resumable; explicit append-new-episode path.
   **`recover_run_config` MUST return the LAST active/resumable episode's
   config/run_id/counters, not the first `RunStarted`** (today it returns the first —
   correct for a single-run WAL, wrong for a lineage). Replay classification: last
   episode `RunPaused` → `AuthRequired`; last episode `RunFinished` → idle/appendable;
   last episode open → resume it.
2. **WAL enrichment** (§18): `origin`+`submission_id` on `RunStarted`; `StepStarted`;
   `finish_reason`; `tool_name`; typed error; `Queued`/`Retracted`/`RunPaused`.
3. **Durable cursor + projection**: `WAL+store → Vec<ControlUnit>` keyed by
   `(wal_seq, sub_index)`; non-blocking in-process live tail; attach-barrier dedup by the
   durable cursor. **The durable head MUST be the last FSYNCED WAL seq, NOT
   `FileWal::last_seq()`** — `append(sync=false)` advances `next_seq` before fsync, so a
   client cursor must only advance past the fsync barrier (the live tail backpatches its
   events with assigned seqs once fsynced; `WalHandle::record`/`sync` must surface the
   assigned seq, which they discard today). Cursor state does NOT reset at episode
   boundaries (WAL seq is monotonic per lineage file across episodes).
4. **Durable queue + identity**: wire the queue records, replay = queued − retracted
   − consumed.
5. **AuthRequired = RunPaused**: non-terminal record + `ReplayResolution::AuthRequired`
   + same-run_id resume.
6. **Cancel token + provider watchdog** (§13).
7. **Mid-turn input channel:** the loop accepts follow-up turns from the coordinator.
8. **Durable run index** (§14).
9. **WAL lease-tenure stamps and file-integrity rejection**.

## 22. Standalone CLI coexistence

`broca-run` stays as-is; a single-turn CLI run is a lineage with one episode. Shared core,
two front-ends.

## 23. Thin-core fit (subc)

**Zero subc-CORE changes**. All `session.*`/`run.*` are opaque
management-surface RPCs; fan-out/replay/backpressure/coordinator/run-index are
module-side. The §21 changes are **broca-core/WAL/store**, not subc-core.

## 24. Conformance coverage

- Multi-episode crash-cut resume, including a cut within a later episode, must
  preserve prior episodes and never let an episode terminal seal the lineage.
- Queue retract/consume races must remain unambiguous across crashes; durable
  cursors must remain continuous across reopening; authentication continuation
  retains the run_id; cancellation seals Interrupted.
- Module tests cover actor admission, fan-out backpressure, the attach barrier,
  durable run indexing, and tool-route lifecycle.
- End-to-end tests cover multiple clients, reopening, multiple turns, terminal
  subscriptions, status, and session-read pagination.

## 25. Explicit deferrals

Fork UX (cross-lineage prefix read) · interrupt-steering (v1 = queue + retract) · auto
pause→refresh→continue (v1 = manual resume of `RunPaused`) · router-driven model
selection · cross-machine portability (distributed lease) · consumer-provided custom
tools.

## Graceful restart policy

`session.send` accepts `on_restart: "resume" | "interrupt"`. Omission means
`interrupt`; there is no daemon-level default knob. Invalid values are refused as
`invalid_params` naming `on_restart` before any WAL write. The policy belongs to
one send: durable queued submissions retain it in their request, and `RunStarted`
freezes it for the episode. It is not inherited from an earlier send and is part
of keyed-send identity.

On the daemon's channel-0 `module.draining` push, broca stops new admission and
queued dispatch immediately, without starting its final seal timer. A resume-enabled
run finishes its current work and writes
`RunPaused{Restart}` at the next clean step-entry boundary. The gated frame requires
`restart-pause/v1`; the writer raises the local WAL feature floor before appending
it. Interrupt-enabled runs seal `Interrupted` at the same boundary. GOODBYE (or
connection loss) starts the final 10 s grace and 20 s total shutdown budget: a tool
gets the daemon's drain window **plus** the grace to reach that boundary. A tool
still in flight after grace is sealed `Interrupted` with `outcome_unknown` even
when it opted into resume. Crashes keep their terminal interruption behavior.
The manifest declares `runs_in_flight`, `opening`, and `closing` as busy health
gauges; paused actors are idle, so a restart pause does not hold the drain open.
See [module lifecycle and shutdown](module-lifecycle.md) for timings and the
daemon's irreversible drain contract.

After the serving connection registers, boot recovery resumes restart-paused
sessions through the existing admission checks: the recorded model must still
resolve, its recorded account must match the served account, tool routes must be
available, and stamped scopes must pass synchronization and access checks. It
continues the same run and send IDs, records `resume_started`, and reconstructs
provider request bytes from the WAL. A readable session refused by admission is
sealed `Interrupted` with the refusal reason. An unreadable newer-writer WAL is
only logged at WARN with its session and missing features: no bytes or metadata
are changed, and other sessions still resume.

Restart and authentication pauses diverge in the actor's `on_send`, `send_plain`,
and `steer_submission` paths. Restart pauses answer keyed retries `paused`, queue
plain sends behind the unfinished episode, and persist steers for its next step.
Auth pauses still refuse unrelated plain sends and steers as `run_paused`; their
own keyed retry remains an explicit re-auth resume. Both pauses support cancel.
`run.status` reports restart pauses with `state: "paused"`, `reason: "restart"`
and `pause_reason: "restart"`. Authentication-pause `run.status` responses retain
their existing format (`reason: "auth_required"`, without `pause_reason`).
