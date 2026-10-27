# Durability contract

This document defines the authoritative record set and replay semantics.
See `docs/wal-and-resume-design.md` for on-disk encoding and task mechanics.

## 1. The one invariant + the v1 policy

Side-effecting tools are **at-most-once across a crash**, via an **intent-log +
durability barrier** — NOT step-boundary fsync. The naive rule ("skip a tool
whose result is in the WAL, else re-run") is broken: a tool can have STARTED (its
edit/bash/API-charge applied) without its result persisted, so re-running
duplicates the side effect.

**v1 INDETERMINATE policy — interrupt / operator recovery.** On ANY
non-idempotent ambiguous side effect (it MAY have applied but no durable outcome),
the run INTERRUPTS to the operator. It does **NOT** feed an `unknown_outcome`
back into another model step — the model would react with a NEW mutating call
under a DIFFERENT `tool_call_id`, a SEMANTIC duplicate the fence cannot catch. A
provider-valid synthetic `tool_result{is_error, reason}` MAY be written for
inspection/persistence, but **continue-on-unknown is an explicit opt-in**, only
for tools declared pure/idempotent. AFT has NO durable executor fence — so interrupt/operator recovery is the only correct v1 default.
On resume the synthetic result IS written, and the episode is then sealed Interrupted with no
model call (§4, *Resume close-out*): the run never continues on its own, and the unknown
outcome reaches the model only through the owner's next send, which is the owner's decision
to continue.

Tokens NEVER hit disk; only assembled durable units. The model leg writes no
record until it completes, so re-running an interrupted model step is idempotent —
the barrier guards only the **tool leg**.

## 2. Record set

Append-only, per session. Each record is FRAMED with: a monotonic **1-BASED `seq`**
(so a `watermark_seq` of 0 unambiguously means "nothing materialized" and the
cold-start merge includes the first record), the lease `fence` epoch, and
a strong per-record **digest (SHA-256/BLAKE3, NOT crc32)** over header +
payload. Replay validates monotonic `seq` and the digest; `fence` is retained as a
forensic lease-tenure stamp and is not a replay gate.
Fence/dedup keys are **COMPOUND**: `session/run/batch/tool_call_id`, never
bare `tool_call_id`. Encoding + size caps: `docs/wal-and-resume-design.md` §1.
Every session WAL has a total-byte ceiling of **1 GiB (1,073,741,824 bytes)**,
shared by append and recovery. Admission reserves lineage metadata and a bounded
terminal before accepting ordinary records. A byte-budget refusal writes nothing,
does not poison the writer, and leaves room for a permanent `run_finished{reason:
error}` terminal: `session_too_large` for ordinary sessions, or the unchanged
`retained_session_too_large` for retained sessions.

`tool-times/v1` gates optional Unix-millisecond observations on
`tool_dispatch_intent.dispatched_at_ms`, `tool_no_dispatch.dispatched_at_ms`,
`tool_result.finished_at_ms`, `tool_call_deferred.deferred_at_ms`, and
`approval_recorded.closed_at_ms` (only for a closing result); the writer publishes
the feature floor before the first timed record. These observations never enter
model requests; an older untimed record remains untimed rather than acquiring a
replay-time replacement.

| Record | fsync point | Purpose |
|---|---|---|
| `run_started{run_id, session, config, input}` | **ONE atomic record, synced BEFORE the first provider call** / | run open + the AUTHORITATIVE config + user input in a SINGLE record. Atomic-by-construction: a torn start leaves EITHER nothing OR a fully-started run, never a partial (config/input missing) that resume would reconstruct as a divergent (empty-prompt) request. `run_id` is the start-run idempotency key. An optional `scope{owner, ref, scope_epoch}` freezes the scope a scoped session was admitted under, on every one of its episodes; it is MUST-UNDERSTAND (feature `scope/v1`), so a record carrying it is written only in a `ver` 3 frame requiring that name, and a build without it refuses the WAL as NewerWriter rather than ignore the scope and admit a send from someone other than the scope's owner. The writer adds `scope/v1` to the state root's `wal-feature-floor` before the first such frame, and refuses the append if it cannot. A send the owner check refuses (or that adoption refuses) writes nothing. An optional `manifest` freezes what a session's first send fetched from its tool providers (the plan exactly as received and its identity, each provider's answered tools and text or the reason an optional text-only provider was absent, the tools offered to the model, the joined system text, digests and versions); it is MUST-UNDERSTAND (feature `plan-manifest/v1`), written only in a `ver` 3 frame requiring that name, so a build without it refuses the WAL as NewerWriter rather than render the session without its frozen tools. This build reads it and writes none. |
| `model_attempt_finished{step_id, attempt, request/response body len + sha256, http_status?, content_type?, response_body_cap?, outcome, ts_ms}` | none of its own: appended as each provider HTTP attempt ends, durable at the next barrier | one per attempt (failed, cut and cancelled ones included), each before its step's `model_step_finished` or the run's terminal. INFORMATIONAL ONLY: every reader skips it like an unknown type, so replay, prompt, projection, billing and export are identical without it |
| `model_step_finished{step_id, assistant_message, usage}` | after provider terminal, before next send | the assembled assistant unit |
| `tool_batch_started{batch_id, step_id, planned_calls[]}` | before any dispatch | brackets the batch; `planned_calls` is the durable PLAN |
| `tool_dispatch_intent{batch_id, tool_call_id, args, module_id?, schema_pin?}` | **BEFORE** the dispatch leaves | the barrier's "before" half — ONLY for calls planned dispatchable. `module_id` (the tool provider module the call went to) and `schema_pin` (`{schema_digest, semantics}`) are MUST-UNDERSTAND (feature `dispatch-module/v1`): an intent carrying either is a `ver` 3 frame, and a build without the name refuses the WAL as NewerWriter, because skipping them would report a dispatched call as never dispatched. The writer adds `dispatch-module/v1` to `wal-feature-floor` before the first such frame. Absent on intents written before broca recorded them |
| `tool_no_dispatch{batch_id, tool_call_id, reason}` | before next provider send | durable terminal for denied / validation-failed / unavailable-before-send / policy-blocked calls — replay NEVER re-dispatches these |
| `tool_result{batch_id, tool_call_id, output, is_error, reason?}` | **BEFORE** the next provider send | the barrier's "after" half; a `SentButOutcomeUnknown` dispatch records `output = unknown_outcome`. `reason` is an optional, UNGATED typed reason name: today only a closed scoped route writes it, naming what `scope.describe` said about the scope when the route closed (`scope_ended`, `scope_not_live`, `scope_parent_ended`, `scope_agent_changed`, `scope_unverifiable`); the outcome (`completed` = `route_drained`, never sent; `unknown` = sent, no result) is what replay acts on, so a reader that skips it computes the same session |
| `tool_batch_finished{batch_id}` | after EXACTLY one terminal per planned call | batch commit marker |
| `resume_started{episode, resumed_from_seq, replayed{messages, model_steps, tool_results}, redone[], indeterminate[]}` | written FIRST by each resume of an episode with no durable `run_finished`; synced with the resume's first barrier (its own, or the one holding the indeterminate calls' results) | what the resume rebuilt without calling anyone, what it redoes, and which calls it found indeterminate. INFORMATIONAL ONLY: every reader skips it like an unknown type and it never enters a rendered request |
| `turn_finished{step_id}` | synced after the turn's batch commits/repairs | turn boundary |
| `steered{send_id?, request_identity_digest?, delivery?, title?, input, hooks?}` | **synced BEFORE the steering send is answered** | a message a caller added to the RUNNING turn. MUST-UNDERSTAND (feature `steer-queue/v1`): always a `ver` 3 frame, and a build without the name refuses the WAL as NewerWriter, because skipping it would drop a user message from the transcript. The writer adds `steer-queue/v1` to `wal-feature-floor` before the first such frame. Placement comes from WAL order alone: the message joins the prompt at the episode's next `step_started`, or at its `run_finished` if no step follows, so it is never between a tool call and its result and nothing earlier changes. A steer acknowledged `active` is guaranteed to be in the transcript; it reaches a model within the same turn only if another step follows. `hooks` optionally records pre-user hook metadata alongside the prepared `input`, which includes any pre-user transformations; a missing `hooks` field decodes as absent. Replay renders `input` exactly as written and never re-runs hooks. |
| `run_finished{reason, usage}` | **synced BEFORE success + lease release** | run close. A typed `error.provider_details` on `run_finished`, `run_paused` or `retracted.failure.error` is MUST-UNDERSTAND (feature `google-rpc-error-details/v1`): the writer places only those records in a `ver = 3` frame requiring that name, so older readers refuse instead of dropping quota diagnostics. The feature floor is raised before the first such append. |
| `run_cancel_requested{run_id, at}` | **synced BEFORE cancellation acknowledgement** | pending boundary cancellation, or its escalation to `now`. MUST-UNDERSTAND (`run-cancel-at/v1`): an older reader refuses the WAL rather than silently resume a cancelled run. Does not replace tool dispatch/result evidence or itself publish a terminal; recovery closes the current batch and seals `Cancelled` without another model step. For a run with an open provider-held call (below), live or paused, it is written and synced for both `at: now` and `at: step_boundary`, and the cancel is answered then, before the seal: no step boundary comes before the held call's delivery, so both mean an immediate close-out. |
| `tool_call_provider_held{batch_id, tool_call_id, call_key, module, reason, detail_digest?, held_at_ms, sequential}` | **synced BEFORE broca acts on anything else in the held reply** | a tool provider answered the call with a `held` marker (tool-provider contract §4.1, `held-reply/v1`), and the real result arrives later as a `late_results` entry for `call_key` (§7). Follows the call's `tool_dispatch_intent`; the call's eventual terminal is an ordinary `tool_result` under the same `batch_id`. `reason` is the provider's open `held.reason`, informational only; `detail_digest` is the SHA-256 of the held `detail` object, which is not stored; the interim content is never stored and never enters a prompt. `sequential` is true when the batch dispatches one call at a time. MUST-UNDERSTAND (feature `provider-held/v1`): always a `ver` 3 frame, and a build without the name refuses the WAL as NewerWriter, because skipping the record would leave an intent without a terminal, class the call indeterminate and seal the run Interrupted while the provider still owes its result. The writer adds `provider-held/v1` to `wal-feature-floor` before the first such frame, and refuses the append if it cannot. |
| `run_paused{run_id, reason: tool_held, ts_ms?}` | **synced BEFORE the run's final `Paused` unit** | the run waits only on provider-held calls (and on calls behind them): every call is terminal, provider-held, a held successor, or approved and awaiting dispatch behind the hold, at least one provider-held call is open, and no approval card is open. Not terminal and never a crash orphan; broca resumes the same run itself when a held call's late result arrives, and the caller never resumes it. MUST-UNDERSTAND under the same `provider-held/v1`: `PauseReason` is a closed enum, so an older reader refuses the frame rather than misread the pause. `run_paused` with `auth_required` or `restart` keeps its earlier frame. |

Close-out terminals of a provider-held call carry an ungated `tool_result.reason`, next to the
`run_cancel_requested` record that may precede them. `run.cancel` writes `outcome: interrupted,
reason: provider_held_cancelled`; `run.stop` writes `outcome: max_steps, reason:
provider_held_stopped`; the mixed-crash rule (another call in the batch is indeterminate) writes
`outcome: interrupted, reason: provider_held_interrupted`, never `unknown`. A delivered late
result, including a `not_started`, `expired` or reduced entry, is always `outcome: completed`, so
`interrupted` or `max_steps` on a call with a `tool_call_provider_held` record comes only from a
close-out, and marks its episode as closing; recognition reads the outcome, never the `reason`.

`provider-held/v1` covers exactly those two values. A run that is never provider-held writes
the same frames, byte for byte, as before the feature existed, and never raises the floor for
it; no renderer version changes.

**Status of provider-held calls.** This build defines both values and their gating, but
omits `provider-held/v1` from its known-feature table: it refuses a WAL carrying either value
as NewerWriter, and no production path writes them. The two rows above, the close-out reasons,
the sequential sync rule (§3 step 8) and the provider-held and held-successor rows of §4 are
the contract the writer, replay and recovery must meet when they ship. The feature joins the
known-feature table with the replay that classes held calls as §4 says, before any build
writes one.

Durable **runtime-state** records (not runtime-only queues):
`queued{submission_id, request,..., title?, delivery?}`, `retracted`, `steered` (above),
`approval_requested{tool_call_id}`, `approval_resolved{tool_call_id, decision}`.

`queued.delivery` is UNGATED: it is `"interrupt"` on the message an interrupting send
queued, and the rebuilt queue drains pending interrupts ahead of plain entries. A build that
predates the field drains in plain WAL order. That is safe for delivery (every message runs
exactly once) but not for order: after a rollback an interrupt runs behind the plain sends
queued before it.

`step_committed` is an in-memory writer ACK at the step boundary, NOT an on-disk
record.

Assistant `text_with_state` blocks retain opaque, format-tagged provider metadata in their
original item/part order, including signature-only empty text. Records carrying these blocks
(input, queued/steered input, completed model output, or compaction replacements), and run
configs opting into `provider_text_state`, require `provider-text-state/v1`. This release both
reads and writes that feature; the writer publishes it to `wal-feature-floor` before appending
a gated frame. Older readers refuse rather than silently discard context. Frozen configs without
the opt-in continue dropping Responses phases and Gemini text signatures, retaining their recorded
outbound bytes on resume. State is echoed only when its format tag names the target endpoint's
protocol and the assistant message's recorded provider/model IDs both match the target model.
Unknown origins fail closed; visible text remains portable.

Media references and ordered multipart tool outputs require **`media-content/v1`**. Any
`Parts` output requires it, even one containing only failure-marker text: older readers cannot
decode the new output kind. A record without the new grammar keeps its prior features and bytes.
The reader and writer ship in the same release, an exception to the reader-first skip contract.
Before the first media frame, the writer durably raises `wal-feature-floor`; if that fails, it
appends nothing. Placement blocks rollback to a build lacking the feature, and such a reader
refuses the WAL as `NewerWriter` naming `media-content/v1` before decoding content.

| Carrying record | `media-content/v1` condition |
|---|---|
| `run_started` | Input contains a media block or Parts output, or a hook subject carries either |
| `steered` | Input contains a media block or Parts output, or a hook subject carries either |
| `queued` | Request input contains a media block or Parts output, or a hook subject carries either |
| `model_step_finished` | Assistant content contains a media block or Parts output, or a hook subject carries either |
| `tool_result` | Result uses Parts, or a post-tool hook subject uses Parts; pre-tool refusal subjects are arguments, not native content |
| `late_result` | Result uses Parts |
| `approval_recorded` | Closing result uses Parts |
| `send_delivered` | Carried input contains a media block or Parts output |
| `compaction_message` | Replacement contains a media block or Parts output |
| `compaction_view_applied` | The opaque view's native edit or replacement content carries a media block or Parts output |

Hook subjects are interpreted by phase: pre-user subjects are message arrays, post-assistant
subjects are assistant messages, and post-tool subjects are tool results. Pre-tool subjects on
dispatch intents, no-dispatch records and intentless refusal results are opaque arguments;
media-looking data inside them or inside tool JSON never adds `media-content/v1`.

OpenRouter's `redacted_reasoning` blocks with `provider_format: "open_router"` and frozen
`reasoning_echo.format: "open_router_details"` policies require **`openrouter-reasoning/v1`**.
The reader and writer for this feature ship together. It is distinct from
`provider-text-state/v1`: it protects structured reasoning arrays, encrypted entries and their
echo policy, not text-part phases or signatures. Every record carrying that content (run input,
queued or steered input, model output, compaction replacement) or policy (run start or refreeze)
is gated; the feature floor is raised before the frame is appended. Older readers refuse by
feature name before attempting to deserialize the closed `provider_format` enum. Records without
OpenRouter state retain their previous frame requirements and bytes, including scalar reasoning
policies for DeepSeek and Kimi. Legacy frozen configs still render their recorded bytes; this
addition does not change a renderer version. Older binaries ignore an unknown `reasoning_echo`
field in typed config decoding, but actual resumes refuse on recorded/current config drift
rather than silently sending a request without the echo.

New ChatGPT admissions freeze a `chatgpt_wire` profile with admitting-host User-Agent,
originator and sourced conversation/installation/window/turn identity. Run starts and auth
refreezes carrying it require **`chatgpt-codex-wire/v1`**; the WAL writer publishes the feature
floor before appending either frame. Older readers refuse rather than silently dropping the
recorded policy. Resume restores recorded presence or absence, without discovering new host
values. Missing profiles retain legacy bytes; the renderer version does not change.

Codex-profile WebSocket attempts also require this feature when their frame digest carries
continuation details. These describe only live encoding: canonical full requests are still
rendered and hashed once, while frame bytes can carry a compatible suffix and a real send-time
stamp. Socket response IDs are not durable continuation authority, and the first request
after resume always sends full input.

Asynchronous compaction (compaction-provider/v1 0.3) requires **`compaction-view/v1`** for three
things. `compaction_view_applied` records a provider view at the step boundary before the step
that first renders it, with how it was applied: as published, or with the companion strip of
signed reasoning a model provider binds to its prefix. A resumed run renders it from that record
alone, byte for byte, without asking the provider again. The decision to apply a view is made
over the prompt the request is rendered from (runner tags and loop guidance included). The view's
own bytes are judged against the stored transcript a provider copies from and sent exactly as
given (compaction-provider §6), so a verbatim copy of a stored message is accepted, and the
change its untagged bytes make is stripped or deferred like any other. A recorded view that no
longer assembles fails the step rather than render anything else.
`compaction_notice` records an advance notice, with its sequence number and headers, before the
bus publisher can project and publish it, so a republish after a crash reuses its number. A
notice is recorded after the writes of each step boundary (input, steers, tool results, an
applied view) and after a run's last step, before its terminal: its final answer, or the step a
max-steps or budget stop or a structured-output submit ends on, in that stop's own barrier.
Only a session whose plan names a compaction provider reads the tombstones for the notice floor. A Setup `compaction_message` that carries
the provider's ceiling policy is gated too. A Setup record without one keeps the
`compaction/v1` frame the removed synchronous client wrote. That client's records
(`compaction_request`, step `compaction_message`s, `compaction_answer`, `compaction_wait`) are
still read and folded, so its sessions replay the bytes they rendered.

Ceiling waits and decisions require **`context-ceiling/v1`**. `context_ceiling_wait` records
the step and absolute `deadline_ms`; its later `unavailable` value records whether the ride
could reach the provider. Recovery keeps that deadline and does not repeat a completed ride.
`context_ceiling_decided` records the step's permission to send (`detail: null`) or its terminal
`{window, size, cause}` refusal, before model I/O. Recovery honors that decision rather than
starting another wait. `run_finished` with reason `context_ceiling` requires the same gate.
Waiting is active with reason `context_ceiling_wait`, never an authentication-style pause.
The wait uses Setup's `wait_ms` (at most 300,000 ms), including the ceiling ride and view fetches.
After the bound a request still at the ceiling is refused even if inside the window; a request
over the window is never sent. The terminal carries no provider error or model backoff signal.

A lowered output cap requires **`output-reservation/v1`**. `output_reservation_clamped`
records a step's output-token cap when it is lower than the run's frozen cap, synced before
model I/O. A resume of the step renders the recorded cap again, and a cache warm or cache dump
rebuilds the step with it, so the rebuilt bytes match the recorded digest. A build without the
feature refuses the WAL instead of resuming the step with the frozen cap, which would be a
different request from the one recorded.

A step whose estimated input plus the frozen output cap would pass the run's context limit is
sent with a lower cap, recorded first as `output_reservation_clamped`. The estimate is the
previous step's measured input plus the request's growth in rendered bytes since, at two bytes
per token, and a margin of 1,024 tokens or 1/256 of the window is kept free. A resume lowers a
recorded cap further only if the request now needs less, and records the lower cap first. When
not even the provider's minimum output reservation fits (4,096 tokens, the caller's smaller
explicit cap, or one token above an Anthropic thinking budget), the run ends `error` with class
`context_overflow` and provider code `output_reservation_exceeds_context` without sending.

Send admission checks the durable estimate plus the incoming native messages without calling
the provider. An already accepted queued head blocked by the ceiling remains pending; holding
it writes no record. A durable view's updated notice or an effective window change wakes the
actor through `LineageRegistry::route_compaction_changed`, which refreshes admission before
retrying the drain. An active run's ordinary completion already refreshes and retries it.
Ordinary boundary `view.get` failures have a 30-second in-memory cooldown; ceiling waits bypass
that cooldown while retaining their original absolute bound. None of these writes or calls
occur for sessions without a compaction provider.

The startup `bus_run_events_enabled` switch enables both compaction notice publication and
view-ready consumption. With it off, discovery omits `compaction` and `model_view`, and a plan
with a compaction item is refused before admission writes. With it on, broca only attaches to
the pre-provisioned `m_broca` durable pull consumer; it never creates or updates bus topology.
The consumer uses explicit acknowledgements, deliver-all, a 30-second acknowledgement wait,
and at most 1,000 pending deliveries, filtering `event.magic-context.compaction.>`.

A valid newer announcement for a known compaction session is committed to `run-index.db`
before entering the bounded in-memory inbox and before its bus acknowledgement. One receipt
per session retains the lineage, version, coverage, digest, and receive time, not view content.
Startup rebuilds the inbox from these receipts before serving; excess sessions stay durable
and retry through the bounded background reconciler. Applied/refused/superseded receipts are
cleared only after the WAL's notice records their outcome; completed lineage deletion clears
that lineage's receipts too. An idle session can apply a validated fitting view using its frozen
offline renderer, record the updated estimate, then wake and await queue admission before
clearing the receipt. A parked unfinished episode is not an idle rewrite boundary.

Announcement intake and receipt reconciliation read a small `compaction_facts` projection,
not the conversation. The existing durable turn and bus projections fold Setup, applied
versions, notice acknowledgements and refusals into it. When the live WAL's file identity and
size match the projection's stored identity and verified byte end, no newer frames were appended.
A missing or behind projection
uses at most the final 256 KiB plus the small lineage header, then flushes the read handle
before updating facts. If a gap, oversized frame or unavailable live suffix prevents proof,
the facts stay unknown and retry later; this path never widens to a full WAL or archive replay. Applying an idle
view still reconstructs its prompt once, under the actor's single writer.

Runner state is never lost apart from the transcript (compaction-provider/v1 §5.3). Tags, the
Setup answer, applied views and notices all live in the session's one WAL. The next tag is the
WAL's highest tag plus one, and retention deletion carries a tag floor to the next lineage. So
they cannot be lost or restored separately from the messages they describe. A new lineage starts
with no compaction state: it runs Setup again and renders Setup's initial message, and its
notices name no parent lineage and no applied view until one of its own applies. Notice sequence
numbers are per session, not per lineage (the bus id and the provider's duplicate check key on
the session), so retention deletion records the next notice number on its tombstone
(`next_notice_seq`, beside `next_tag`) and a new lineage numbers its notices from there. Nothing detects
a session's WAL restored by hand to an older copy.

Runner tags (**`runner-tags/v1`**): every message part broca stores has a per-session tag
number (`§N§` in the llm-runner contract), assigned by the session's writer as it appends the
record carrying the part and stored in that record's `tags` member: `run_started` and
`steered` (and a `steer` `send_delivered`) hold one array per input message,
`model_step_finished` one per assistant block, and `tool_result`, `tool_no_dispatch`, a closing
`approval_recorded` and `budget_fired` one each. A `tool_result` delivery's appended text is
part of the result it rides on and has no tag of its own.
Tags are strictly increasing in WAL order and never reused: the next tag is the highest the WAL
holds plus one, so restarts, resumes and agent-data moves (which carry the WAL bytes) continue
the count, and retention deletion records the next tag on its tombstone (`next_tag`) for the
address's next lineage to start from. Tags are on for every lineage whose first episode a
tag-writing build admits; a lineage first admitted earlier stays untagged and keeps its exact
bytes. A run frozen with `run_config.render_tags` (from the plan's `render_tags`) renders each
tagged user text block and text tool result with a `§N§ ` prefix in the request only, and on the
`anthropic_messages` family each plain assistant text block too (a leading tag mark the model
wrote is replaced by the part's own tag). Stored records never change, and signed thinking,
encrypted reasoning, tool calls and text with provider state are never rendered with tags; other
families keep assistant tags as metadata. Every tagged record, and every run start frozen with
`render_tags`, is a gated frame requiring `runner-tags/v1`, which an older reader refuses by
name. Records without tags keep their previous bytes and requirements, and a run without
`render_tags` renders byte for byte as before, so no renderer version changes.

Bedrock Converse's frozen `bedrock_reasoning` switch enables decode and replay of reasoning
text/signatures and encrypted redacted content for new episodes. Missing/false retains the old
reasoning-free normalizer and request shape. These use existing `reasoning` and untagged
`redacted_reasoning` blocks; no new closed enum value or WAL feature is needed. Renderer versions
are unchanged. Meta Responses now freezes `store:false` and encrypted-reasoning `include` in
`static_options`, just like OpenAI API-key and xAI Responses. Old Meta configs keep their previous
store/include omissions and legacy reasoning-item shape when resumed.

Meta's `store:false` policy controls response persistence, not prompt-cache affinity. Its
session-stable `prompt_cache_key` remains an independent frozen render input, retained across
tool continuations and later user turns. Keep that key when validating stateless replay against
production: successful encrypted-item replay alone does not establish cache reuse.

## 3. The barrier (per turn's tool batch)

On Unix, the first WAL barrier after **every open**, including recovery of an
existing file, also syncs its parent directory and the parents of directories
created by that open. A directory-open or directory-sync error refuses the barrier;
the pending pass is cleared only after success. Feature-floor publication likewise
syncs the renamed floor's parent (including `.` for a bare relative path), and a
readable floor still needs that proof once per process before a gated append. Only
after a successful file and directory barrier does the process memoize the floor's
names for that absolute path; covered appends then do no floor filesystem I/O.
The floor only grows, including across placement and rollback: deleting it or removing
names while a writer is alive is unsupported. A new name always takes the locked
durable path. Non-Unix directory sync is an explicit no-op, not a best-effort
attempt to open a directory as a file.
Unmemoized feature-floor raises hold both an in-process mutex and a cross-process advisory
lock on a stable sibling `wal-feature-floor.lock` throughout read, union, write,
rename and directory sync. The lock file is never renamed or removed by writers;
session-specific leases alone do not serialize that machine-wide union.

1. **Resolve a per-call PLAN first** (capability validate + approval): each call is
   either *dispatchable* or *no_dispatch{reason}* (denied / schema-invalid /
   oversized / plane-unavailable-before-send / policy-blocked).
2. fsync `tool_batch_started{planned_calls}` + ALL `tool_dispatch_intent` for the
   dispatchable calls, in ONE fsync; await it; THEN dispatch — so every call that
   could leave the process has a durable intent first.
3. `ToolPlane::dispatch` returns one of:
   - **`Completed(result)`** → fsync `tool_result`.
   - **`NotSent`** → it never left → a safe normal error result → fsync
     `tool_result{is_error}`.
   - **`SentButOutcomeUnknown`** → fsync `tool_result{is_error, reason:
      unknown_outcome}` AND trigger the §1 interrupt/operator recovery policy (the side
      effect MAY have applied; ambiguous LIVE transport failure is the same hazard
      class as crash-after-side-effect).
   - **`ScopeDrained{reason, sent}`** (the call's scoped route closed while the call was
     in flight; the plane reads the scope's current state with `scope.describe`, and the
     daemon's `route.closed` push decides nothing): not sent, and the scope or its parent
     gone, or the scope now stamping another agent or epoch, or nothing could be told →
     `tool_result{outcome: completed, is_error, reason}` (`route_drained`), never
     re-dispatched. Not sent with the scope live under the same epoch and agent: the
     plane re-sends it on a fresh route and it comes back `Completed` like any call.
     Sent → `tool_result{outcome: unknown, is_error, reason}` and the run stops
     Interrupted, never Cancelled.
   - A route that closed with NOTHING in flight: at the next batch's start, before any
     intent, the tool plane notices the closed route and asks the daemon about the scope
     with `scope.describe`. When the daemon says the scope or its parent is gone
     (`scope_ended`, `scope_not_live`, `scope_parent_ended`), every call of that batch
     gets a `route_drained` result naming the reason and the run stops Interrupted; a
     live scope is ordinary route loss and the batch re-opens its route.
4. A `no_dispatch` call → fsync `tool_no_dispatch{reason}` (a provider-valid
   `tool_result{is_error}` is still surfaced to the model for a denial).
5. **Parallel batch + ANY INDETERMINATE/unknown → do NOT auto-run the remaining
   side-effecting calls** ; interrupt the whole batch unless all unresolved
   calls are declared safe (running B while A is unknown may advance a
   partially-unknown operation).
6. Caller-visible tool-result / control units are emitted **AFTER** the WAL result
   fsync — never a result the caller saw that resume reports as lost.
7. fsync `tool_batch_finished` ONLY after EXACTLY one terminal (`result` /
   `no_dispatch` / `unknown`) per planned call.
8. **Provider-held calls** (the contract for when they ship; see the status note in §2). A
   call whose provider answered `held` gets its
   `tool_call_provider_held` synced before anything else happens; in a parallel batch the
   other calls run and finish and the batch stays open. **Sequential sync rule:** in a batch
   that dispatches one call at a time, the calls after a provider-held call keep their
   step-2 intents but are not sent until it has a terminal. When its late result arrives,
   the held call's `tool_result` is appended and synced BEFORE any later call is sent, on
   both the live and the resumed path (an ordinary sequential batch instead writes every
   result at the commit barrier); the later calls then dispatch in order with their
   existing keys and no new intent. A crash after that sync, with a later call lacking a
   terminal, resolves Interrupted: that call gets `unknown` and is never re-sent, and the
   held call keeps its result. A held call's intake registration is pruned only once
   `tool_batch_finished` and `turn_finished` are synced, not at its terminal.

**Step boundary**: the loop awaits the cheap `step_committed` ack before starting
step N+1 (a barrier enqueue off the per-token path) — the one-step crash bound.

## 4. Replay — TURN-GRANULAR

Replay reconstructs the prompt from **COMPLETE or REPAIRED TURNS, never bare
records** — a bare assistant `tool_use` with no matching results is
PROVIDER-INVALID (Anthropic rejects it).

1. **Acquire the lease** (new lease-tenure stamp — §5). Open the WAL; validate each
   record's digest + monotonic `seq` (the `fence` is retained as forensic metadata, not a
   replay gate). Discard a torn/corrupt TRAILING record (never committed); **FAIL LOUD on
    non-tail corruption**. A digest-verified frame that this reader cannot interpret
    is intact authority data, not evidence of crash damage: it refuses replay/open,
    even at EOF, and is never repaired or truncated. Trailing lineage metadata
    (reserved frames carrying the WAL's durable 128-bit identity, rather than run
    records) may accompany a discarded damaged record only if claimed identities
    are consistent with any identity proven in the prefix and with one another.
    Archive lookups with a missing or invalid index must
    prove coverage of the entire sealed member region before reporting absence or
    caching membership; damaged member headers never justify serving older history.
2. **`run_config` is authoritative** — rebuild the request config from it; reject
   if caller-supplied config conflicts. Provider construction restores the entire recorded
   frozen render bundle, including its endpoint, wire family, model id, limits and reasoning
   choices. It does not re-resolve the model against today's catalog; a changed or withdrawn
   catalog row therefore cannot move or refuse the admitted request. Credentials may refresh,
   but must keep the auth shape selected by the recorded route, including Bearer auth for an
   opaque explicit endpoint.

   Renderer compatibility comes from the current family's declared version table, never a
   version number copied from the WAL. An unsupported version produces permanent
   `renderer_version_drift`; a null or undecodable bundle produces permanent
   `frozen_render_config_unverifiable`. These guards run before any model call. For a supported
   earlier version, the current renderer's version-specific blob must still equal the recorded
   blob; otherwise the permanent refusal is `frozen_render_config_drift`. The existing explicit
   auth-at-start refreeze rules remain separate and apply only before any model attempt.
3. **Fold by TURN:**
   - `run_started` (config + input) → base prompt.
   - a `model_step_finished` assistant message with NO tool calls → prompt-commit.
     This is a **NATURAL END**: such a step always ends the loop. If it's the last
     durable step and `run_finished` is missing (crash between it and finalization),
     the run ENDED naturally — resume FINALIZES it (writes `run_finished`) and does
     NOT re-run the model (a re-run would generate a divergent extra turn). A
     durable `run_finished` makes resume a pure idempotent no-op.
   - a `model_step_finished` assistant message WITH tool calls is **NOT
     prompt-committed until its batch is COMPLETE or REPAIRED**.
4. **Per batch (by `batch_id`), classify each PLANNED call:**

   | durable state | meaning | replay action |
   |---|---|---|
   | `tool_result` (Completed) | done, durable | include the result |
   | `tool_no_dispatch` | denied / invalid / never-sent terminal | include the no-dispatch result; **NEVER re-dispatch** |
   | `intent ∧ ¬terminal`, or `unknown_outcome` | INDETERMINATE — side effect may have applied | **NEVER re-dispatch.** The result is written, then the episode is sealed Interrupted: see *Resume close-out* below |
   | `intent ∧ tool_call_provider_held ∧ ¬terminal` | PROVIDER-HELD — the provider confirmed it holds the call and owes a late result; **not indeterminate**, and excluded from `resume_started.indeterminate` (this and the next row take effect with the replay that lists `provider-held/v1`; until then the WAL is refused, §2) | never re-dispatch; the run stays paused `tool_held` (writing a missing `run_paused`) until the late result is delivered into this batch, or a cancel or stop closes it out. If any other call of the batch is indeterminate, the indeterminate handling wins: the held call is closed `interrupted` with `reason: provider_held_interrupted` and the run is sealed Interrupted |
   | `intent ∧ ¬terminal` in a `sequential` batch, planned after a call that is PROVIDER-HELD or whose terminal is a close-out (`interrupted` or `max_steps`) | HELD-SUCCESSOR — never sent, because sequential dispatch sends a call only after its predecessor resolves and close-out sends nothing; **not indeterminate** | never re-dispatch on replay; it is sent once, under the sequential sync rule (§3), after the held call's delivered terminal is synced, or closed out with the not-dispatched `interrupted` result. Behind a DELIVERED (`completed`) held terminal the same intent is ordinary INDETERMINATE: nothing durable separates a crash before its send from one after it |
   | `¬intent ∧ ¬no_dispatch ∧ planned-dispatchable` | never left the process | safe to (re-)dispatch by id |

   - A batch is COMMITTABLE only when EVERY planned call has exactly one terminal,
     no duplicates, source-order reconstruction, matching batch/step/run ids.
   - **Stop / max-steps firing with PENDING calls** → write synthetic
     `tool_result{is_error, reason: interrupted|max_steps}` for EVERY pending call,
     commit the batch, THEN `run_finished` — so the persisted conversation stays
     provider-valid (this closes the `run.rs` hard-stop TODO).
   - A batch with a non-idempotent INDETERMINATE call → the run **interrupts to the
     operator**, never silently continues.
   - **Resume close-out.** A resume that finds indeterminate calls makes no model call. In one
     barrier, after its `resume_started`, it writes `tool_result{outcome: unknown, is_error}`
     (text: the call has no recorded result) for each indeterminate call that has no result
     yet, `tool_result{interrupted}` for each planned call of that batch that never left the
     process, and the batch's `tool_batch_finished` + `turn_finished`. THEN it seals the episode
     `run_finished{Interrupted}` in a barrier of its own. The call is never re-sent. No model
     step of THIS run sees the unknown outcome; the owner's NEXT send renders the call paired
     with that result, so no request ever pairs a tool call with nothing and the model learns
     the outcome is uncertain before it would repeat the call. A crash between the two
     barriers resumes into the same close-out, which writes no second result for a call that
     already has one.
5. **In-flight model step** (no `model_step_finished`) → re-run cleanly (no record);
   `stream_gen` abandons dead-generation assembly.
6. **Effective context = materialized-store prefix ⊕ in-WAL tail, by WAL SEQ
   watermark** (read ATOMICALLY — see `wal-and-resume-design.md` §4), NOT by
   `step_id` (too coarse: it drops or double-includes same-step records).

## 5. Single writer + finalization (lease)

`start_run` acquires the durable single-writer **lease** (OS lock plus a monotonic
lease-tenure stamp); FAILS if a live writer holds it. Hard requirements:

- **Lease-tenure stamp**: the lease file records the monotonic epoch for forensic
  correlation. The current lease implementation flushes this file but does not use the stamp
  as an append gate; single-writer safety is the OS lock plus the WAL file-integrity check.
- **Writer lifetime tied to the lease**: drain + sync + close the WAL
  writer BEFORE dropping `LeaseGuard`, so an old writer task can't append after a
  new epoch acquires the session.
- **Finalization barrier**: `turn_finished` / `run_finished` are fsynced
  BEFORE emitting terminal success and releasing the lease — else a completed run
  resumes as incomplete.
- **`run_id` idempotency**: a retried `start_run` after a crash must not
  append a duplicate `user_input`.

Every append carries the lease `fence` as a forensic tenure stamp; FileWal does not compare
it with an authority or reject a stale value. The OS-lock lease can't be stolen from a live
holder, and the WAL file-integrity check rejects a late append after a foreign write, so no
per-barrier epoch comparison is needed. A FUTURE CAS/TTL lease MUST re-check ownership before
each barrier fsync.

## 6. Failure handling

- **WAL write failure** → FAIL LOUD + surface to user + operator.
- **Final stop outcome** covers the actor generations still present at stop and failures
  during that stop, within its fixed budget. A completed earlier idle close does not make a
  later, clean shutdown incomplete. Process-lifetime failure events retain the session id,
  stage, timestamp and error; unresolved recovery debt is reported separately, even when
  the current shutdown is clean. Only successful recovery and sync of the affected session
  clears its debt, never stopping the registry or recovering another session. These are
  process-local diagnostics; they do not replace WAL replay after a process restart.
- **Non-idempotent INDETERMINATE** → interrupt / operator recovery, never a
  silent continue.
- **Non-tail WAL corruption** (digest/seq mismatch mid-log) → FAIL LOUD.
