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
| `steered{send_id?, request_identity_digest?, delivery?, title?, input, hooks?}` | **synced BEFORE the steering send is answered** | a message a caller added to the RUNNING turn. MUST-UNDERSTAND (feature `steer-queue/v1`): always a `ver` 3 frame, and a build without the name refuses the WAL as NewerWriter, because skipping it would drop a user message from the transcript. The writer adds `steer-queue/v1` to `wal-feature-floor` before the first such frame. Placement comes from WAL order alone: the message joins the prompt at the episode's next `step_started`, or at its `run_finished` if no step follows, so it is never between a tool call and its result and nothing earlier changes. A steer acknowledged `active` is guaranteed to be in the transcript; it reaches a model within the same turn only if another step follows. `hooks` is reserved and not written; replay renders `input` as written and never runs hooks over a record written without them. |
| `run_finished{reason, usage}` | **synced BEFORE success + lease release** | run close |
| `run_cancel_requested{run_id, at}` | **synced BEFORE cancellation acknowledgement** | pending boundary cancellation, or its escalation to `now`. MUST-UNDERSTAND (`run-cancel-at/v1`): an older reader refuses the WAL rather than silently resume a cancelled run. Does not replace tool dispatch/result evidence or itself publish a terminal; recovery closes the current batch and seals `Cancelled` without another model step. |

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

Bedrock Converse's frozen `bedrock_reasoning` switch enables decode and replay of reasoning
text/signatures and encrypted redacted content for new episodes. Missing/false retains the old
reasoning-free normalizer and request shape. These use existing `reasoning` and untagged
`redacted_reasoning` blocks; no new closed enum value or WAL feature is needed. Renderer versions
are unchanged. Meta Responses now freezes `store:false` and encrypted-reasoning `include` in
`static_options`, just like OpenAI API-key and xAI Responses. Old Meta configs keep their previous
store/include omissions and legacy reasoning-item shape when resumed.

## 3. The barrier (per turn's tool batch)

On Unix, the first WAL barrier after **every open**, including recovery of an
existing file, also syncs its parent directory and the parents of directories
created by that open. A directory-open or directory-sync error refuses the barrier;
the pending pass is cleared only after success. Feature-floor publication likewise
syncs the renamed floor's parent (including `.` for a bare relative path), and a
readable floor still needs that proof before a gated append. Non-Unix directory
sync is an explicit no-op, not a best-effort attempt to open a directory as a file.
Feature-floor raises hold both an in-process mutex and a cross-process advisory
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
   if caller-supplied config conflicts.
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
