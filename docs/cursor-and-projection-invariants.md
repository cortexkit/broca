# Cursor and projection invariants

This design specifies durable cursor and projection, queue transitions, pause and
continue, cancellation, watchdogs, lease-tenure stamps, and file-integrity checks.
The module contract is in `docs/module-surface.md`.

A lineage WAL is a sequence of episodes. Episode-aware replay and the records
`Queued`, `Retracted`, `RunPaused`, and `StepStarted` form the substrate.

---

## 0. Two distinct positions

The materialization watermark and control-stream cursor are different positions
and must not be conflated:

| | Materialization watermark | Control-stream cursor |
|---|---|---|
| What | highest WAL seq folded into the store PREFIX | a consumer's read position in the control STREAM |
| Granularity | TURN boundary only (whole committed turns) | per-ControlUnit: `(wal_seq, sub_index)` |
| Who holds it | the store (read atomically with the prefix) | a subscriber (resubscribes with it) |
| Resets at episode boundary? | no (seq is monotonic per lineage file) | no (same) |
| Anchored where | a turn/episode-resumable boundary | any fsynced ControlUnit |

**The boundary invariant, stated explicitly:**
- The **materialization watermark** stays TURN-boundary-anchored (the persister only
  advances it at `TurnFinished`). A resume tail therefore opens at an episode-resumable
  boundary — EXCEPT the orphan-`RunFinished` lead (the prior episode's terminal sits in
  the tail when the watermark is at that episode's last `TurnFinished`). Replay
  tolerates this: `run_config`/`run_id` are recovered from the FULL WAL via
  `recover_*().rev()` and threaded into `resume_state_from_store` SEPARATELY from
  `replay(tail)`, so `replay(tail)` yielding `None` for them is already handled. The cursor
  does NOT move the materialization watermark off turn boundaries.
- The **control-stream cursor** is monotonic over FSYNCED seqs across the whole lineage
  file; it never resets at an episode boundary (WAL seq is lineage-global). It is derived
  from the WAL, so it survives the coordinator's release/re-open. It is NEVER
  the in-process live seq (that is internal-only).

So: the watermark answers "what's materialized for resume"; the cursor answers "where is
this reader in the event stream". Cursor tracking does not change the
watermark's turn-boundary anchoring.

**The anti-conflation rule (the trap to design against):** no code path may use one
position where the other belongs. Concretely, two failure modes to avoid by construction:
- the **projection must NOT key off the materialization watermark** — it produces the
  control stream from a record range addressed by `(wal_seq, sub_index)`, derived from the
  records themselves, never "from the store watermark forward". A subscriber replaying from
  `from:"start"` or a cursor projects from THAT point, independent of how far the persister
  has materialized.
- the **control-stream cursor must NOT assume an episode-boundary tail** — it is a position
  in a lineage-global, monotonic fsynced-seq space and is valid at ANY fsynced ControlUnit,
  including mid-episode. (The materialization watermark is the one that's turn-boundary
  anchored; the cursor is not.)
These two are kept apart at the type level: `StorePrefix.watermark_seq` (materialization)
and `Cursor(wal_seq, sub_index)` (stream) are distinct types with no implicit conversion,
so a path can't silently pass one as the other.

## 1. Durable cursor and projection

### 1a. Surfacing the fsynced sequence

The durable head a cursor may advance to is the **last FSYNCED seq**, not
`FileWal::last_seq()`, which includes appended records not yet durable. The writer
tracks `last_fsynced_seq` and updates it only after a successful sync. A sync
barrier returns that durable head; record publication must retain its assigned
sequence so live entries and replayed entries share the same cursor. Existing
fsync-everything-pending-then-ack barrier semantics remain unchanged.

The coordinator's record-before-publish and register-before-sync ordering is
specified in `docs/lineage-actor-coordinator.md` §2.

### 1b. The deterministic projection `WAL+store → Vec<ControlUnit>`

A pure function `project_control(records: &[FramedRecord]) -> Vec<(Cursor, ControlUnit)>`
where `Cursor = (wal_seq, sub_index)`. One WAL record can project to SEVERAL ordered
ControlUnits (e.g. a `ModelStepFinished` → `StepStarted`? no — see below; a
`ToolBatchStarted`+intents → a `ToolCall` per call), disambiguated by `sub_index` (0-based
within the record). This is the SAME assembled-unit shape the live loop already emits to
`out` (caller.rs `ControlUnit`), so a replayed stream is semantically equivalent to the
live one (display tokens excepted — they're live-only, never projected).

Mapping (record → ControlUnits), using the enriched record fields:
- `RunStarted` → `RunStarted{run_id}` (the enrichment gives run_id authoritatively).
- `StepStarted` → `StepStarted{step_id}`.
- `ModelStepFinished` → `AssistantMessage{message}` then `StepFinished{step_id, usage,
  finish_reason}` (finish_reason from the enrichment; sub_index 0 and 1).
- `ToolBatchStarted` → one `ToolCall{call}` per planned call (tool_name from the intent
  enrichment / the assistant message; sub_index per call).
- `ToolResult`/`ToolNoDispatch` → `ToolResult{result}` (the reconstructed result).
- `RunFinished` → `RunFinished{reason, usage}` (+ `Error{error}` first if the typed error
  is present).
- `RunPaused` → a terminal-for-this-attach `RunFinished`-shaped pause signal? NO — a
  distinct surfaced unit (see §3); the projection emits it so a subscriber to a paused run
  sees the pause as the final event.
- `Queued`/`Retracted`/`ApprovalRequested`/`ApprovalResolved` → submission/approval
  control units as the surface needs (the queue ones feed `SubmissionStarted`, §2).

The projection is DETERMINISTIC (same records → same cursors+units), tested with a golden
like the render goldens. It reads ONLY the durable records — never process state — so it's
identical live-side and re-open-side (the survives-reopen property).

### 1c. The non-blocking live tail + attach barrier

The active run has a small, non-blocking live event ring. Every replayable entry
carries `(wal_seq, sub_index)`; process-local positions are never resubscribe
cursors. Attachment registers a subscriber buffer before the sync barrier,
projects durable records through head H, then skips buffered entries already
covered by H. See `docs/lineage-actor-coordinator.md` §2 for the ordering proof.

Conformance tests project through H, drop and reopen the writer, then project
from H and assert no gap or duplicate. Fan-out attachment uses the coordinator lock.

## 2. Durable queue and identity

The schema records exist (`Queued{submission_id, message}`, `Retracted{submission_id}`,
and `RunStarted{submission_id}` for the consume link). These records define the pending
set and its atomic transitions.

**Pending set (pure, replay-derived):**
`pending = Queued − Retracted − consumed`, where `consumed = { submission_id: ∃
RunStarted{submission_id} }`. A function `pending_submissions(records) -> Vec<(SubmissionId,
Message)>` in FIFO order (queue order = WAL order of the `Queued` records). This is
crash-unambiguous BY CONSTRUCTION:
- crash BEFORE the consuming `RunStarted` ⇒ the submission is still in `Queued − Retracted`
  and not consumed ⇒ still pending ⇒ re-drained on re-open.
- crash AFTER ⇒ a `RunStarted{submission_id}` exists ⇒ consumed ⇒ never double-started.

**The atomicity requirement:** retract and consume both
mutate the pending set and MUST be serialized so a submission can't be both retracted and
consumed. The serialization point is the **WAL append
order itself** + the single-writer lease: retract appends `Retracted{id}`; consume appends
`RunStarted{id}`. Whichever record is fsynced FIRST wins, and replay's set algebra makes
the loser a no-op (a `Retracted` after a `RunStarted` for the same id retracts nothing — the
submission is already consumed; a `RunStarted` after a `Retracted` would be the coordinator
consuming a retracted submission, which the coordinator must NOT do — it checks the pending
set under its lock before consuming). The replay-derived functions are:
- `pending_submissions()` + a `retract(id) -> Retracted | AlreadyStarted` decision that is
  a PURE function of the current durable records (remove-if-pending-else-already_started),
  so the coordinator appends the decided record under its lock.
- Conformance: queue/retract/consume races across a crash cut — for a `Queued` then
  `Retracted` then a crash at every boundary, assert the pending set is exactly what the
  durable records justify (retracted ⇒ gone; consumed ⇒ gone; neither ⇒ pending), and a
  consume never resurrects a retracted submission.

The coordinator's drain loop calls the replay-derived decision functions. Crash
safety is tested at the record level.

## 3. AuthRequired pause and continue

`RunPaused` and `AuthRequired` retain a resumable pause. Resume after reauthentication
continues the same run_id:
- A new resume path branch: `ReplayResolution::AuthRequired` + a caller signal "auth is now
  resolved" → continue the SAME run_id from the last durable boundary (the prompt is
  `replay.prompt`, counters are the paused episode's, NO new `RunStarted`). Without the
  resolved signal, resume stays paused (surfaces the pause, returns).
- The pause is WRITTEN by the loop when a provider step returns an Auth-class error
  mid-run: write `RunPaused{run_id, AuthRequired, error}` + sync BEFORE returning (a
  durable, resumable boundary), instead of `RunFinished`. This is the one new WRITE site.
- v1 is MANUAL continue (a caller re-invokes resume with the resolved signal after
  re-auth); auto pause→refresh→continue is deferred (spec §25). Conformance: pause mid-run
  → resume-without-signal stays paused → resume-with-signal continues the same run_id to a
  clean terminal, byte-identical sent bytes to a no-pause reference.

## 4. Cancel token + provider watchdog (§13)

- Cancellation uses a `CancellationToken` in
  `run_loop_with_store`/`run_body`, checked at turn boundaries and around dispatch. On
  cancel: cooperatively stop at a SAFE WAL boundary → `RunFinished{Interrupted}` (the
  durability core's fail-to-doctor; NEVER a false `Cancelled` mid-batch that strands a
  tool_use). This threads through the same barrier discipline as everything else.
- Provider watchdog: a per-run provider-call timeout must cooperatively
  cancel at a safe boundary (the same path as cancel) and releases the lease. The tool
  path already has a 10s timeout (SubcToolPlane); this closes the model-stream gap.
- Cancel/watchdog are ADDITIVE knobs with safe defaults (no token ⇒ no cancellation; no
  timeout ⇒ unbounded await), so every existing call site is unchanged.
  Conformance: cancel mid-loop → Interrupted at a safe boundary, no stranded tool_use,
  resume sees a clean interrupt; watchdog expiry → same.

## 5. WAL lease-tenure stamp and file-integrity guard (§21.9, defense-in-depth)

The file WAL stamps the lease epoch on every append for forensic tenure correlation. It does
not compare the stamp with an epoch authority and does not reject a stale value. Single-writer
safety is provided by the lease's OS advisory lock plus `FileWal`'s file-integrity high-water
check, which rejects an append after a foreign writer has changed the file. The focused
conformance case is therefore an append at an arbitrary older stamp that succeeds and remains
visible in replay.

## 6. Implementation surfaces

- `crates/broca-core/src/wal_writer.rs` — `sync()` returns last-fsynced-seq; `record()`
  surfaces the would-be seq; the writer tracks `last_fsynced_seq`.
- `crates/broca-core/src/projection.rs` — `project_control` (deterministic
  WAL→ControlUnit+cursor) + the `Cursor` type + the live-tail buffer.
- `crates/broca-core/src/queue.rs` — `pending_submissions` +
  `retract` decision (pure, replay-derived).
- `crates/broca-core/src/run.rs` — the `RunPaused` write site; the cancel-token +
  watchdog hooks; the AuthRequired-continue resume branch.
- `crates/broca-wal/src/file_wal.rs` — lease-tenure stamping and file-integrity rejection.
- Tests: projection golden + cursor continuity, queue race crash-cut, pause-continue,
  cancel/watchdog, lease-tenure stamp acceptance. Resume execution must stay byte-identical.

## 7. Hard invariants

Byte determinism + the resume exit gates (resume_exec, materialized_resume) MUST stay
green — cursor, queue, and cancel wrap the loop without changing the
rendered request bytes or the prompt reconstruction. The projection is a READ over durable
records (observability), never an input to the prompt. If a gate goes red, the change is
wrong.
