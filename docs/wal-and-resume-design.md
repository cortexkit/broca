# WAL and resume design

Record semantics and replay rules are authoritative in `docs/durability-contract.md`.
This document describes the WAL, writer, replay, materialized store, and renderer.

Implementation invariants:
> - `run_started` is ONE ATOMIC record `{run_id, session, config, input}` (was 3) —
> a torn start is all-or-nothing, never a partial-empty-prompt resume.
> - WAL `seq` is 1-BASED — watermark 0 = "nothing materialized"; the cold-start
> merge includes the first record.
> - replay tracks NATURAL-END: a terminal turn without `run_finished` finalizes on
> resume (writes `run_finished`) without re-running the model.
> - The persister projects INLINE after each turn barrier (records already fsynced)
> — eliminates the writer/persister shutdown race by
> construction; non-fatal (`let _ = store.project`), so full-WAL replay stays the
> correctness fallback. The persister watermark advances ONLY on `turn_finished`.
> - TWO exit gates re-run `run_loop` from truncated WALs asserting byte-identical
> provider-SENT bytes: `crates/broca-core/tests/resume_exec.rs` (full-replay path)
> + `crates/broca-store/tests/materialized_resume.rs` (store-merge path).

## 0. Scope

In: the intent-log WAL (source of truth), the writer task + step-committed barrier,
TURN-granular replay with the v1 interrupt/fail-to-doctor INDETERMINATE policy, the
durable per-call PLAN, `run_config`, effective-context = materialized-prefix ⊕
in-WAL tail by WAL-seq watermark, the deterministic provider-request renderer
(the cache-hash domain), the swappable SQLite materialized store, durable
steer/approval/follow-up records.

The durability boundary includes `ToolPlane::dispatch` outcomes, hard-stop
synthetic results, and durable per-call plans. Steering, cancellation, context
transforms, and separate-process provider transport are outside this design.

## 1. WAL file format + record encoding

**One append-only WAL file per session**, co-located with the lease file (same
FNV-1a-of-BindIdentity key used by `FileLeaseStore`), runner-exclusive
writer.

**Record framing — torn-tail-detectable + integrity-checked:**

```
record:= [len: u32 LE][ver: u8][seq: u64 LE][fence: u64 LE][digest: 32 bytes][payload: len bytes]
payload:= JSON (one record body, #[serde(tag="type")])
digest := SHA-256 (or BLAKE3) over (header-fields ‖ payload)
```

- **Digest is SHA-256/BLAKE3, NOT crc32** — this is a source-of-truth log;
  records are low-volume (off the hot path) so a strong digest is cheap. On replay,
  validate the digest + monotonic `seq` while retaining `fence` as forensic metadata.
  A torn/corrupt TRAILING
  record (crash mid-append) is discarded by legacy recovery and read-only replay;
  their **non-tail corruption FAILS LOUD**. Overhaul writable recovery uses the
  explicit policy below.
- **Overhaul recovery policy is explicit.** `broca.jsonc` accepts
  `"wal_recovery": "repair"` (default) or `"refuse"`; an unknown value refuses
  boot by name. This setting affects only version 4 logs. Legacy bytes and
  recovery behavior are unchanged, including the existing ambiguity between a
  damaged final append and corruption of a synced final frame.

  Recovery is enabled only by the state format decided at boot and supplied to
  the opener. Unconfigured `FileWal::open` and the low-level
  `OwnedWriterWal::open` wrapper are legacy-only:
  they do not inspect `.head` or recovery manifests. Selecting overhaul format
  after an unconfigured open is a typed boot-configuration error, not a second
  recovery pass. Diagnostics receives the same boot decision and scans recovery
  evidence only for overhaul roots.
  Materialized-store readers receive that same boot configuration. Unconfigured
  SQLite stores retain legacy queries, including the old transaction-free run
  lookup and lineage-only projection metadata read; they do not probe the root
  to infer recovery policy.

  Repair retains every complete, checksum-valid contiguous record before the
  first real corruption boundary. The normal streaming scan decides whether a
  repair is needed; whole-log buffering and manifest-prefix hashing occur only
  for damage or existing manifests. An ordinary torn final append with no intact
  data after it, outside a valid proven prefix, is silently truncated with no cut,
  tombstones, interruption or warning. The writer syncs each tool-dispatch intent
  before allowing execution. The power-loss model assumes a successful fsync
  preserves all preceding writes across the cut, so an incomplete final intent
  could not have authorized dispatch. Without a usable proof, later corruption
  of a synced final frame cannot be distinguished from an unacknowledged torn
  append. The existing final-tail discard rule therefore cannot provide a
  stronger guarantee against post-sync storage corruption.

  A previously repaired log still pays a recurring verification cost. For a
  clean, unchanged writable `FileWal` open with completed same-lineage manifests
  and no projection hooks, recovery performs two complete WAL read/decode passes,
  one retained-prefix SHA-256 pass per applicable manifest, one parent-directory
  sync call, and one WAL-file sync. It does not reread the raw removed suffix.
  With the default 1 GiB WAL cap and one completed cut near EOF, that is up to
  2 GiB of WAL reads plus 1 GiB of prefix hashing on every later open, separately
  from reading and validating the manifest. Each manifest is individually capped
  at the supplied WAL cap; directory entries, manifest count and aggregate
  manifest bytes are not bounded by that cap. For WAL length L and M matching
  manifests of sizes J_i, content reads are 2L + sum(J_i), and prefix-hash input
  is at most K times L for K applicable manifests. These are source-level byte
  budgets, not measured physical I/O or latency guarantees. Projection hooks,
  full admission replay and writer-lifecycle publication add work outside this
  core-open budget. Run admission still requires complete WAL replay rather than
  resuming execution from a stored message-prefix checkpoint for a repaired history.
  Owner-completion preflight adds one more full-WAL read at the synced writer
  head on each repaired session open (up to another 1 GiB at the default cap),
  even when every completion is already present. Its in-memory owner/terminal
  fold and replay also scale with that history. These checks are not cached as
  an assumption that an old completed manifest makes later opens free.
  Real repair handles both unpublished non-tail damage and
  damage within a published prefix. The `.head` proof is evidence for the WARN
  reason (`unpublished_tail`, `published_region_corruption`, or
  `unproven_corruption`), never permission to destroy the only copy of a suffix.
  A proof is only a durability lower bound: fsync acknowledgements can precede
  its unsynced publication. No new proof sync is added to acknowledged batches.
  A cut that removes bytes covered by an old head claim durably removes that
  claim before truncation; the next writer republishes its new synced head.
  An unreadable initial lineage under a covering head claim refuses rather than
  guessing permission to discard the entire file.
  Intact conflicting lineage or unknown must-understand features still refuse;
  repair must not erase replacement evidence or downgrade a newer writer.

  Before shortening the log, recovery writes **all removed bytes verbatim** to
  `<log>.cut-<unix_ms>` through a uniquely named temporary file, file sync,
  rename and directory sync. An immutable `.json` companion records the cut id,
  retained-prefix hash, raw suffix hash/length, session and call attribution; it
  also binds the exact recovery-marker bytes and their append offset to the
  retained-prefix hash and the resulting WAL lineage. Cut filenames include that
  lineage, and a session name alone never authorizes replay into a fresh log.
  Missing logs cannot authorize old manifest replay. Recovery refuses writable
  open and names the manifest path when it cannot read the required marker
  evidence, rather than guessing how a repair should resume. The JSON
  follows the same durable publication sequence. Both must be durable before
  truncate and file sync. Recovery never deletes those evidence files. Whole-session
  retention deletion captures their exact basenames under the session lease before
  writing deletion progress, including orphan raw files and temporary raw/JSON
  files. Completion removes that captured inventory and syncs the directory before
  declaring deletion complete. Retries replay the same names, never a fresh
  lineage's inventory; lineage-namespaced filenames prevent name reuse. The
  existing check that defers retention for overhaul roots remains in place;
  this cleanup does not enable destructive background retention on those roots.
  A pending manifest is finished on the next writable open, even when the log
  already looks clean after a failed or interrupted truncate. A successful
  `RecoveryCut` append is synced before writable open completes. Retrying a
  matching completed cut does not create duplicate recovery records. Later
  retries re-establish the manifest-directory barrier before changing the log.
  A torn or zero-filled prefix of the precommitted marker at its bound offset
  is completed with the same bytes, cut id and sidecar pair, not a second cut.
  Other bytes are new damage. Loss of the raw sidecar after its durable
  publication does not discard the manifest's call identities or block recovery;
  diagnostics warns about the preserved evidence without exposing its contents.
  Later
  damage that removes an earlier marker can recover its attributed calls from
  the immutable session manifest. Failed repair is not transactional: an I/O
  error can leave the live file shortened, but the durable backup and manifest
  remain available for retry.

  Bound materialized stores participate before the physical cut: an immediate
  SQLite transaction durably invalidates the session's rebuildable transcript,
  admission/head checkpoints, compaction views and run lifecycle summaries. A
  pending fence lists required cut ids and advances the context epoch, so a
  projection captured before the cut cannot restore the old state. If that
  transaction fails, recovery refuses the writable open without shortening the
  log. Both invalidation and completed reprojection enforce and verify SQLite
  `synchronous=FULL` on the actual materialized-store connection before BEGIN.
  Invalidation commits after raw-byte preservation but before manifest publication,
  so a durable manifest is never visible ahead of the pending reader fence. Cut
  identity depends on the logical session and the same prefix/suffix image, not a
  clock or backup filename; a crash before publication can recreate the fence id.
  Lease-free run lookup reads the fence with its summary and refuses pending
  state. Diagnostics withholds only the affected session's rows before applying
  scan limits and reports a session-named pending or unreadable-evidence warning;
  unrelated sessions remain available. Historical index rows are not rewritten
  during invalidation. Rebuild updates only runs evidenced in the current WAL
  lineage or its cuts. Unfinished cut runs are Interrupted; explicit surviving terminal
  states remain unchanged. Already published usage export facts remain immutable during recovery;
  rebuilding the transcript must not revise the billing history.
  Independent compaction-facts writes and lazy checkpoint seeding validate the
  session lineage, context epoch and pending fence in the transaction that writes
  the facts. Bounded compaction readers capture an epoch token before reading WAL
  bytes and validate it when publishing facts. Inputs without that token must
  carry the required recovery markers; an old range cannot bootstrap removed
  Setup or view outcomes after a rebuild.

  After the cut and synced recovery marker, recovery rebuilds the session from
  the complete surviving WAL and publishes its checkpoints in one transaction.
  A failed rebuild leaves the pending fence and invalidated views for the next
  open to retry; a crash between invalidation and truncation resumes from the
  durable manifest. A matching completed checkpoint avoids full reprojection on
  subsequent opens. Logs without recovery markers do no recovery projection
  reads or writes.

  `RecoveryCut` and `RecoveryCallClosed` may be written only to version 4 logs
  on boot-selected `broca-state/2` roots. Both require `wal-recovery/v1`, raised in the
  machine feature floor before writing recovery history. This durable list of
  required log features prevents rollback to a reader that cannot understand the
  recovery tombstones. The record includes readable removed ordinary/late
  dispatch intents and results, and every unresolved tool call in a
  surviving `ModelStepFinished`, including a call whose intent was removed.
  These tombstones are consulted before `NotSent` can become a redispatch plan,
  and before actual dispatch. Approval-gated planning also checks restored
  Prepared and intent keys; a surviving approval cannot override a tombstone.
  Close-out writes `outcome_unknown_result` with
  `DispatchOutcome::Unknown`, never a never-executed result, and the existing
  `ResumeStarted`/terminal projection and `run.status` report the indeterminate
  calls. Close-out consumes the attributed call set from every durable cut,
  grouped by owner rather than only by the currently surviving episode. An intact
  assistant turn keeps ordinary start/result/finish records; if its batch start
  was removed, close-out writes the missing start before its Unknown results.
  A removed model turn or wholly removed owner instead receives a prompt-free
  `RecoveryCallClosed`: cut id, owner run, batch/step identity when known,
  model-facing call id, exact call key when one survived,
  and explicit Unknown result. It never invents `RunStarted`, input or assistant
  content. A missing exact physical key stays absent, not guessed from a name.
  Durable ordinary/late terminals and completion receipts take precedence over
  repeated cut attribution; an earlier expiry result does not settle a possible
  subsequently authorized late effect.
  If a real cut contains undecodable bytes, every still-late-open Prepared call
  whose ask permits late Execute is frozen, even when its approval record is
  missing: the unreadable bytes might have contained both approval and a synced
  dispatch intent. Notify-only asks, recorded closed late decisions and retained
  late terminals do not enter that set. Fully decoded cuts keep the prior
  approved-only boundary; ordinary silent final-tail recovery creates no such cut.

  A completion with missing live approval evidence carries the existing error
  code `late_approval_evidence_lost` and an Unknown message that does not claim the
  call definitely ran. A later genuine answer is authenticated and recorded as
  `LateApprovalRecorded`, but cannot override the completion or create another
  result/intent. The existing approval execution report remains `outcome_unknown`;
  unrelated new runs continue. A reader finds the named execution rejection on
  `session.read` at `$.result.late_approvals[i].result.output.code`, with outcome
  `unknown`. This is a new value in the existing optional open-string code field,
  not a new reply member or a top-level RPC error. It is dormant on legacy logs.
  Model-facing IDs such as Gemini's `call_0` can recur in another step of the same
  run. A terminal must match the owner, known batch/step scope and known exact
  physical key; an earlier step's same-ID result cannot satisfy it. Entries with
  neither scope nor key remain separate under their cut, with durable receipt
  multiplicity surviving a second crash. Unprovable identity may produce two
  Unknown results for one physical effect; losing an attributable call by merging
  it into another step is not an acceptable alternative.

  Required close-out frames are prepared together using a temporary copy of the
  numbering state that assigns stable numeric tags to message parts. A refusal
  cannot consume numbers from the real state. Frames are admitted against their
  aggregate byte cost, including existing
  lifecycle/terminal reserves, before any frame is enqueued. A byte-cap refusal
  writes no partial group and publishes no `RunFinished`; the writable open or
  run admission refuses. Admission is atomic, not the individual disk writes:
  a second crash can preserve a frame prefix. The cut remains the durable work
  set, and a later open writes only missing terminals/receipt records. The group
  is synced before a caller may append `RunFinished` for the current run.

  Default `session.read` gets no message or ordinary tool-result event from a
  `RecoveryCallClosed` record. `run.status` continues to use the cut's existing Interrupted and
  indeterminate-call evidence for a removed owner; an unrelated surviving
  completed owner is not reopened. Existing late-approval metadata/reporting can
  show `outcome_unknown`, as it did for a durable Unknown late result, without a
  new field or a new provider-prompt part. Recovery
  tombstones match exact Prepared/intent `call_key` values or an owning run and
  call ID, never a bare model-facing ID across the session. A later run may use
  its own Gemini-style `call_0`. After a repair, physical keys include the latest
  cut identity as a generation: reusing a removed WAL sequence cannot make a new
  call inherit a removed call's identity or suppress new work. Retained Prepared keys keep their original
  generation, and ordinary intents persist the generated key under the writer's
  sequence-allocation lock. Unrepaired and legacy key spellings are unchanged.
  Readable evidence without a recovered run is
  recorded in `unowned_calls` as outcome unknown, including any readable or
  derived keys. A bare ID there is diagnostic evidence only: without a readable
  or derived key naming the old physical call, it suppresses nothing.

  Undecodable bytes set `unattributed_tool_risk: true`, interrupt the in-flight
  run and produce a session-named WARN plus a `diagnostics.findings` warning
  under `wal_recovery_cut`; the flag never blocks later episodes. Broca prevents
  repeats of attributed calls, not external effects whose identifiers were
  destroyed by storage damage. Operators who require refusal rather than this
  lost-attribution exposure set `"wal_recovery": "refuse"`.
  Fully readable sequence damage does not set that risk flag, but a real cut still
  interrupts its unfinished active episode: a removed answer is not silently
  requested from the model again merely because its bytes remain attributable.
  Refuse creates no new cut. New damage uses the original prefix decoder's
  typed errors and final-tail behavior, including discarding an incomplete final
  append that the old decoder accepted. A previously started, manifest-evidenced
  cut is finished under its original identity even under refuse, and the warning
  names that identity. Unrelated new damage never obtains permission from an old
  cut. Read-only replay does not repair.
- **`len` is capped + header-validated before allocation** — never allocate
  on `len` alone.
- **`fence`** = the writer's lease-tenure epoch (§5), recorded for forensic correlation only;
  stale stamps are not rejected. Single-writer safety is the OS lease lock plus the WAL
  file-integrity check.
- **Fence/dedup keys are COMPOUND**: `session/run/batch/tool_call_id`, never
  bare `tool_call_id`.
- JSON body (debuggable, append-stable, off the hot path). The exact body shapes
  are the record set in `durability-contract.md` §2 — incl. the NEW `run_config`
   and `tool_no_dispatch` records.

## 2. Writer task + the step-committed barrier

The loop NEVER fsyncs inline; it hands durable units to a **WAL-writer task** over
an mpsc channel and continues. Channel protocol:

```
WalCommand { record: WalRecord, ack: Option<oneshot::Sender<WalAck>> }
```

- `ack = None` → coalesced into the next fsync (non-barrier records).
- `ack = Some(tx)` → append, **fsync the whole pending batch**, then signal. One
  fsync acks all coalesced appends.

**Barrier points the loop awaits** (per `durability-contract.md` §3):
1. `tool_batch_started{planned_calls}` + ALL intents → ONE fsync → await → THEN
   dispatch.
2. each terminal (`tool_result` / `tool_no_dispatch` / `unknown_outcome`) → fsync
   before the next provider send. **Caller-visible results/control emitted AFTER
   this fsync**.
3. `step_committed` at the step boundary (fsync up to here, await) — the one-step
   crash bound.

The model leg writes no record until `model_step_finished`, so a mid-stream
re-run is idempotent (no barrier there).

**`ToolPlane::dispatch` outcome:** `Completed(result) | NotSent | SentButOutcomeUnknown`. `NotSent` → safe
normal error result. `SentButOutcomeUnknown` (intent fsynced, request sent, then
timeout/reset/AFT-crash before the result) → durable `unknown_outcome` + the §1
interrupt policy — NEVER a normal "tool unavailable" error (which the model would
retry → DUPLICATE side effect), NEVER an auto-retry.

Keep the step-committed barrier. Measure fsync latency when tuning, and adjust
only the non-barrier coalesce window.

## 3. Replay state machine (TURN-granular)

The authoritative classification table + the v1 policy live in
`durability-contract.md` §4. The load-bearing mechanics:

- **Replay by TURN, never bare records**: a `model_step_finished` assistant
  message WITH tool calls is NOT prompt-committed until its batch is COMPLETE or
  REPAIRED — a bare `tool_use` with no matching results is provider-invalid.
- **Classify on the durable PLAN, not just intent-presence**: `¬intent`
  means "safe to dispatch" ONLY for a call planned dispatchable; a call with a
  durable `tool_no_dispatch` terminal (denied/invalid/never-sent) is NEVER
  re-dispatched.
- **v1 INDETERMINATE = INTERRUPT / fail-to-doctor**: `intent ∧
  ¬terminal` (or a stored `unknown_outcome`) for a non-idempotent call → the run
  interrupts; it does NOT feed an `unknown_outcome` back into a model step (the
  model would issue a NEW mutating call under a different id = a semantic dup the
  fence can't catch). Continue-on-unknown is an explicit pure/idempotent-only
  opt-in.
- **Parallel batch + ANY INDETERMINATE → don't auto-run the remaining
  side-effecting calls**.
- **Stop/max-steps with pending calls → synthesize `interrupted`/`max_steps`
  results for every pending call, commit the batch, THEN `run_finished`** —
  closes the `run.rs` hard-stop TODO and keeps the persisted conversation valid.
- **Committed-batch validation**: `tool_batch_finished` is not enough —
  require exactly one terminal per planned call, no duplicates, source order,
  matching ids.
- In-flight model step (no `model_step_finished`) re-runs cleanly; `stream_gen`
  abandons dead-generation assembly.

An in-memory tool fence cannot survive restart. Without a durable executor fence,
ambiguous non-idempotent side effects must interrupt.

## 4. Effective context = materialized prefix ⊕ in-WAL tail, by WAL SEQ

The persister tails the WAL → store ASYNCHRONOUSLY, so the store LAGS during an
active run. **Watermark = WAL SEQ/OFFSET, NOT `step_id`** (a step_id
watermark is too coarse: the persister can materialize PART of step N and advance
to N while that step's tool-result/batch records are still WAL-only → the merge
drops or double-includes them).

**One ATOMIC read:**
```
store.read_prefix_with_watermark(session) -> { prefix, watermark_seq, context_epoch }
wal.tail_after_seq(watermark_seq) -> [records the store hasn't projected yet]
effective_context = prefix ⊕ render(tail)
```
The prefix + watermark come from ONE consistent store transaction — NEVER two
racing calls. `context_epoch` is bumped only on bust commits.

The WAL-sequence tail includes tool-call messages and results not yet projected
into the store. Reading the store alone would drop them from the next step.

## 5. Storage objects, finalization, renderer

**Two named objects, distinct traits** (§13):
- **(a) WAL log** — `FileWal`. Trait `Wal { append(record, sync) -> ack;
  replay() -> impl Iterator<Item=WalRecord>; tail_after_seq(seq) }`. Source of truth.
- **(b) Materialized store** — `SqliteStore`. Trait `MaterializedStore {
  read_prefix_with_watermark(session) -> {prefix, watermark_seq, context_epoch};
  apply_reduction(...) }`. Runner is EXCLUSIVE writer; MC writes via a runner API.
- **Persister** — a separate task: projection = **ONE SQLite transaction per WAL
  range** (idempotent upserts/reductions, record applied seqs, advance the
  watermark, commit). On restart, resume from the last committed seq (idempotent +
  restartable). Off the hot path.

**Lease and finalization** (`FileLeaseStore`):
- **Lease-tenure stamp**: the lease file records the monotonic epoch for forensic
  correlation. It is not an append gate; single-writer safety comes from the OS lock and
  the WAL file-integrity check.
- **Writer lifetime tied to the lease**: drain + sync + close the WAL writer
  BEFORE dropping `LeaseGuard`.
- **Finalization barrier**: `turn_finished`/`run_finished` fsynced BEFORE
  terminal success + lease release.
- **`run_id` idempotency**: a retried `start_run` after crash doesn't
  duplicate `user_input`.

## 5a. THE deterministic provider-request renderer (the cache-hash domain)

Cache stability depends on the **exact bytes sent to the provider**, not semantic
struct equality. Canonical-JSON-of-structs is INSUFFICIENT if the provider module
serializes/orders/`cache_control`-places differently.

**There is ONE deterministic provider-request renderer**, used for live send,
replay, AND tests. The resume/cache hash is computed over the **ACTUAL
provider-request prefix BYTES that renderer produces** — the renderer IS the hash
domain. Requirements:
- fixed key order + stable number/float formatting through the whole render,
  especially for opaque `Value` fields where `provider_options` + tool-input args
  live (serde_json key order isn't guaranteed there);
- `run_config` stores enough (canonical bytes for opaque fields, tool ORDER,
  schemas, tiers, `provider_options`, renderer_version) to reproduce the bytes
  EXACTLY on resume; the WAL config is authoritative;
- the hash and the bytes agree by construction (hash over the rendered bytes).

If wrong, resume "works" functionally but silently busts the cache — the precise
§7/§8 failure mode.


## 8. Test invariants + the durability exit gate

- **Resume invariant = byte-identical prefix**, never token counts. A resume test
  asserts the exact provider-request bytes a no-crash run would send, not merely
  a stable hash. Usage can vary independently of request bytes.
- **Durability tests are deterministic + offline**: scripted assistant messages
  (fixed bytes) + the stub tool plane; the LIVE
  provider is the nondeterministic INPUT the WAL persists, never the thing under test.
- **EXIT GATE — crash-cut tests at EVERY record boundary**: kill between every pair of records — including
  intent-without-result, hard-stop-with-pending-calls, `SentButOutcomeUnknown`
  ambiguous dispatch, denied-call-before-no_dispatch-fsync, and persister
  mid-projection — and assert: provider-VALID replayed prompt, NO duplicate side
  effects, NO sparse/orphan batch, correct interrupt-to-doctor on non-idempotent
  unknowns, byte-identical rendered prefix.
