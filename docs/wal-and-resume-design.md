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
  record (crash mid-append) is discarded (never committed); **non-tail
  corruption FAILS LOUD**.
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
