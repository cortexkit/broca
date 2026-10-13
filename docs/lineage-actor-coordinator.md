# Lineage-actor coordinator

The module front-end uses episode-aware replay, durable queue decisions, control
projection, fsynced sequence tracking, cancellation, and file-integrity checks.
One in-process actor owns each open session behind the subc serve surface.
See `docs/module-surface.md` for the module contract.

---

## 0. Module boundary

The crate `broca-module` owns the coordinator + fan-out + the serve loop + the
`session.*`/`run.*` SERVE TYPES (request/response Rust types). It depends on `broca-core`
(the loop, replay, queue, projection, cancel) and `broca-subc` (the subc serve/transport
plumbing + `SubcToolPlane`). `broca-core` stays transport-agnostic (no serve types leak
into it).

The ownership boundary:
- **subc owns the WIRE mechanics** ("HOW you reach a module"): the envelope/frame codec,
  `route.open`/catalog/supervisor mechanics, the transport handshake. The managed
  subc-client `call(module_id, method, params)` is method-AGNOSTIC — it ships
  `{method, params}` as an OPAQUE body on a route channel; subc never parses it.
- **`broca` owns `session.*`/`run.*`** ("WHAT you say to the module"): they are this
  module's application surface, carried as opaque request bodies. A consumer calls
  `call("broca", "session.send", {…})`; the method name + the params/response schema
  are OURS (the canonical Rust types live in `broca-module`). Zero subc-CORE changes (spec
  §23) — subc routes frames by channel without interpreting the application method.
- `docs/module-surface.md` defines method names and payload semantics. Canonical
  Rust request and response types live in `broca-module`.

Registration: `broca` registers as a subc MODULE under `module_id="broca"` and
serves a management surface; `session.*`/`run.*` ride as opaque bodies on the served route
channel. (`session.subscribe` is a dedicated long-lived stream route, never multiplexed
with `cancel`/`status`/`read` — §9.) This note specs the module-side impl, not the wire
bytes.

## 1. The lineage-actor coordinator (state machine)

### 1a. Model: one actor per open session, a registry of actors

```
LineageRegistry // process-global: session-key → actor handle
  actors: Mutex<HashMap<LineageKey, LineageEntry>>

LineageEntry // the admission slot for one session
  state: Opening | Open(LineageActor) | Closing | Closed
  // plus the in-flight "opening" future others await (1c)
```

`LineageKey` = the lease key triple `{project_root, harness, session}` (the SAME key the
lease + WAL + store partition use — one identity for the whole session). Every
`session.*`/`run.*` op for a session is routed through its registry entry, so a teardown
can't race a concurrent op (the registry mutex serializes admission; the actor serializes
ops on an open session).

### 1b. The actor: a single-task command loop (NOT a shared-mutex object)

The actor is an owned task with an `mpsc` command channel — the established
single-writer-task pattern (same shape as `WalHandle`). ALL ops on an open session are
COMMANDS to this task; the task processes them ONE AT A TIME, so there is no intra-actor
lock contention and no interleaving WITHIN the actor — the concurrency is purely at the
registry admission boundary (1c) and in the fan-out tasks the actor spawns (§2).

```
enum LineageCmd {
    Send { req, reply: oneshot<SendOutcome> }, // §10/§11
    Retract { submission_id, reply: oneshot<RetractOutcome> },
    Subscribe { from: Cursor|Start|Live, sink, reply }, // §2 attach
    Status { reply: oneshot<RunStatus> }, // §14
    Cancel { run_id, reply: oneshot<Ack> }, // §13
    // internal:
    RunFinished {... }, // the run task tells the actor a run ended (drain the queue)
    IdleTick, // the idle-TTL timer
}

enum LineageState { // INSIDE the actor task (not the registry's coarse state)
    Idle, // lease held, no active run
    Active { run_id, run_task, cancel: CancellationToken },
    Draining, // run ending, about to drain the queue or close
}
```

The actor OWNS the lease for its lifetime (acquired in `Opening`, released in `Closing`).
It runs at most ONE run at a time (the one-active-run-per-lineage invariant); a `Send`
while `Active` is durably queued (`pending_submissions`/the queue decisions), a `Send` while
`Idle` starts a run.

### 1c. Admission: the registry admits exactly one Opening (double-reopen coalesce)

The registry mutex is the serialization point for OPEN/CLOSE, held only for the fast
state check + slot transition (NOT across the slow lease-acquire/replay). The op flow:

```
op_for(key):
    loop {
        guard = registry.lock()
        match entry(key):
          None | Closed | Failed:
             ready = broadcast::channel()
             insert Opening{ ready } // claim the slot
             drop guard
             // Opening runs in a DETACHED task, NOT this op's (cancelable) future, so a
             // cancelled/panicking op can't strand the slot.
             spawn open_task(key, ready)
             match ready.subscribe().recv().await { Ok(h)=>return h, Err(_)=>continue }
          Opening{ ready }:
             drop guard
             match ready.subscribe().recv().await { Ok(h)=>return h, Err(_)=>continue }
          Open{ gen, handle }: drop guard; return (gen, handle.clone())
          Closing{ done }: drop guard; done.await; continue // wait the close, re-open (1e)
    }

open_task(key, ready): // detached; ALWAYS resolves the slot + ready
    result = acquire_lease + replay + spawn_actor_task(gen = next_generation())
    guard = registry.lock()
    match result {
      Ok(actor) => entry = Open{ gen, handle }; ready.broadcast(Ok(handle))
      Err(e) => entry = Failed; ready.broadcast(Err(e)) // wake ALL waiters
    }
```

Exactly one lease acquire / replay / actor per session. The `ready` carries
`Result<Handle, OpenError>` and `open_task` ALWAYS resolves it AND always transitions the
slot out of `Opening`.
Each `Open` slot carries a GENERATION number (the actor's identity, used in 1e.5).

### 1d. Lease lifetime + idle close (synchronous teardown)

The actor holds the lease while: a run is `Active`, OR pending submissions exist, OR there
is at least one SUBSCRIBER, OR within a short idle TTL after the last activity. On
`IdleTick` with NONE of those, the actor initiates close. Close is a SYNCHRONOUS TEARDOWN
in a fixed order:

```
close():
    registry: Open{gen} → Closing{done} // under the registry mutex; new ops now wait+reopen
    drain the command channel: every still-buffered command gets a `Reopen` reply
                               (NEVER silently dropped)
    wal.sync().await // flush anything queued
    if pending_submissions(wal.replay()) non-empty OR a subscriber raced in:
        registry: Closing → Open{gen}; serve those commands; ABORT close
    cancel + AWAIT all fan-out subscriber tasks (close their sinks, join)
    wal_handle.shutdown().await // drain + final fsync + join the WRITER
    drop(lease) // ONLY after the writer is fully shut down
    registry: Closing → Closed; done.signal() // wake the 1c waiters
    actor task exits
```

The ORDER is load-bearing: **subscribers cancelled+joined → writer shutdown → lease drop →
registry Closed**. So once the registry shows `Closed` (and a new op may re-open +
reacquire the lease), there is provably NO live subscriber route on the old actor and NO
live writer — closing the lease-vs-subscriber and lease-vs-writer windows. `wal_handle.shutdown().await` before `drop(lease)` applies to both the run
and idle-close paths.

### 1e. The races + how the actor closes them

1. **Send/subscribe arriving as the actor is Closing** — the registry sees `Closing` and
   the op AWAITS `done`, THEN retries → re-opens fresh. A command already buffered in the
   actor when close began gets a `Reopen` reply during the drain; the caller treats BOTH a send-failure AND a `Reopen` reply as
   "re-route through the registry". The close can also ABORT (1d) if pending work or a
   subscriber raced in. No half-torn state.
2. **Two concurrent re-opens** — 1c: exactly one `Opening`; the others await its `ready`.
3. **Subscriber across a release** — close cancels+joins every subscriber task (1d) BEFORE
   the lease drops; the subscriber observes its route end and resubscribes, re-routing
   through the registry (re-opens if needed) and attaching from its last-delivered DURABLE
   cursor (§2a) — WAL-derived, so it survived the release. No event loss.
4. **Idle-close vs just-queued Send** — a `Send` that durably queues a
   submission does `record + sync` BEFORE replying "pending" (§3); the close path re-derives
   `pending_submissions` AFTER its own `sync` and ABORTS the close if non-empty. The actor
   never closes with durable pending work undrained.
5. **Unexpected actor exit** — the actor task has a FINALIZER guard: on ANY
   exit (panic, fatal WAL error) it CAS-transitions the registry `Open{gen} → Closed` for
   its OWN generation only (never clobbering a newer actor) and drops the lease. A stale
   `Open{gen}` handle a racing op grabbed fails on send AND the generation mismatch tells
   the op to evict + re-open (not retry the same dead handle).

### 1f. The stale-writer belt

The file-integrity check (WAL) is the defense-in-depth backstop: if a draining
old actor's WAL writer somehow appends after a new actor opened and wrote, the new actor's
record already grew the file, so the old writer's append is rejected (`ForeignWrite`). The
PRIMARY guard is the lease + the synchronous close ordering (1d): the old actor's writer is
`shutdown().await`-ed BEFORE its lease drops, so it cannot still be appending when the new
actor acquires. The belt covers the residual case where a foreign writer has already changed this inode. The
record's epoch is a forensic tenure stamp, not a comparison that closes a pre-write handover
window.

## 2. Fan-out + per-subscriber backpressure + the attach barrier

The WAL writer is a separate task from the actor. Actor commands do not serialize
writer progress or live-ring publication; the attach barrier therefore relies on
record-before-publish and register-before-sync ordering.

subc is 1:1 (no broadcast), so `broca` owns the fan-out. The actor keeps a small
**in-process live event ring** for the active run; the loop appends each `OutputEvent`
NON-BLOCKING (the producer-stall fix — the loop never `await`s a slow subscriber). Each
subscriber has its OWN bounded per-subscriber buffer fed from the ring by a per-subscriber
TASK (NOT the actor task: the actor must not block on `sink.send` during a
slow/large replay).

### 2.0 The two load-bearing invariants (what makes it correct)

**I1 — record-before-publish, with the seq known at publish time.** The run task records
an event to the WAL (`writer.record`, which queues it to the writer task) BEFORE that event
is published to the fan-out ring, AND each published ring entry carries its durable
`wal_seq` IMMEDIATELY. The seq is a deterministic monotonic counter OWNED BY THE HANDLE
(caller-side): `WalHandle::record` does `seq = counter.fetch_add(1)`, sends
`Append(seq, record)` to the writer, and RETURNS `seq` synchronously. The writer VALIDATES
the seq it receives equals its own expected next (FIFO + both increment by 1 ⇒ they agree)
and POISONS on a mismatch — monotonicity is now a validation, not a writer-side assignment.
So the ring entry's cursor `(wal_seq, sub_index)` is FINAL at publish — no backpatch, no
stale-cursor window. This requires EVERY replayable control
unit to have a durable WAL record, including `StepStarted`. The loop writes this
record so the control stream is fully projectable. (Display deltas are exempt — live-only, never
projected, §2.1.)

**Live delivery is OPTIMISTIC (ahead of fsync) — by design, for latency.** The ring is a
non-blocking fast-path (I2), so an already-attached live subscriber receives an event from
the ring BEFORE its record is fsynced. Consequence: a predicted seq `N` CAN be delivered
live, and then a catastrophic write failure (disk error appending `N`) poisons the writer
and tears the run down — so on a write-failure the live tail MAY be momentarily ahead of
the durable projection. This is BENIGN and bounded: (a) it bites only on a fatal write
failure (which ends the run regardless); (b) it affects ONLY the OBSERVABILITY stream — tool
EXECUTION durability is gated independently by the tool durability barriers (intent-fsync-before-
dispatch, result-fsync-before-next-send) on a separate durable path, so at-most-once is
untouched; (c) a resubscribe reconciles to the WAL (I2 — the WAL is the source of truth, the
ring is never it). The attach barrier for a NEW subscriber is NOT optimistic: `sync()`
returns the write error, so a failed record is never replayed to a fresh attach. So the
optimism is confined to an already-attached watcher's live view on a dying run, which the
resubscribe corrects.

**I2 — control is reliable via the WAL, not via the ring.** The ring + per-subscriber
buffer are a LATENCY fast-path, never the durability path. A subscriber that overflows OR
detects a cursor gap DETACHES and resubscribes from its last-delivered durable cursor,
replaying the projection from the WAL. So a bounded ring dropping events is fine for
control BY CONSTRUCTION — the slow path (projection replay) is always correct and lossless;
the WAL is the source of truth. Only DISPLAY is lossy.

### 2.1 Per-subscriber discipline

- **Display events (lossy, never durable):** coalesce/drop on a full per-subscriber buffer
  + emit a `DisplayGap` marker. Display is live-only, NOT in the projection (a finished
  turn shows assembled text, not re-typed tokens), so it's never replayed.
- **Control events (reliable = durable+replayable, NOT guaranteed-live):** each
  per-subscriber buffer is bounded AND GAP-DETECTING. On overflow OR on a detected gap
  (the next event the subscriber needs was evicted from the ring before its task drained
  it), DETACH the route. The subscriber resubscribes from its last-delivered durable cursor
  and replays the projection. NEVER a silent contiguous-looking drop: a
  subscriber must never consume a post-gap event as if it followed the pre-gap one.

One slow subscriber never affects the run or co-subscribers (the run only touches the
non-blocking ring; each subscriber task is independent and self-detaches).

### 2a. The attach barrier (gap-and-dup-free by ordering)

`session.subscribe{from}` attaches a new subscriber. The barrier is split: a SMALL atomic
step in the actor (register the live buffer + take the barrier), then the SLOW replay in
the subscriber's own task.

```
on Subscribe{from, sink}: // IN the actor — small, no blocking sends
    buf = new per-subscriber buffer
    register buf as a live subscriber of the ring // (R) from now, EVERY new ring
                                                     // event is copied into buf
    H = wal.sync().await // (barrier) fsync everything queued so far; returns
                                    // the last fsynced seq. By I1, every event that
                                    // was in the ring BEFORE (R) was record()-queued
                                    // before this sync, so it is now durable with
                                    // seq <= H.
    spawn subscriber_task(from, H, buf, sink) // SLOW replay off-actor

subscriber_task(from, H, buf, sink):
    // 1. replay the durable projection from `from` up to and INCLUDING H.
    records = wal.replay() filtered to from < seq <= H // explicit seq<=H filter
    for (cursor, unit) in project_control(records): sink.send(unit@cursor) // bounded; detach on backpressure
    // 2. drain buf, SKIPPING any event whose durable cursor <= H (already projected),
    // delivering the rest in order; then stream ongoing live events.
    for (cursor, unit) in buf.drain_and_follow():
        if cursor.wal_seq <= H { continue } // dedup: covered by the projection
        sink.send(unit@cursor)
```

**Why no GAP:** by I1, any event in the ring before (R) was recorded before the `sync()`
call, so the FIFO writer fsyncs it before acking `sync()` → its seq ≤ H → it's in the
projection (step 1). Any event published after (R) is in `buf` → delivered (step 2). An
event published before (R) but fsynced after H cannot exist: published-before-(R) ⇒
recorded-before-(R) (I1) ⇒ recorded-before-sync ⇒ fsynced-by-sync ⇒ seq ≤ H. So every event
is in step 1 or step 2.

**Why no DUP:** every buf entry carries its FINAL durable cursor `(wal_seq, sub_index)` at
publish time — `WalHandle::record` returned the seq synchronously (I1), so there is NO
backpatch and no "once its record is fsynced" timing subtlety. The dedup is a single
comparison: a buf entry with `wal_seq <= H` is already in the projection (step 1) → SKIP;
one with `wal_seq > H` is after the projection → DELIVER. So an event is in exactly one of
step 1 / step 2, never both. The in-process per-record sub-ordering is by `sub_index`; the
cursor is the only identity a client ever holds, so a re-open's ring reset can't break a
client.

**H comes from a real writer barrier (`wal.sync().await`), not a stale read of
`last_fsynced_seq`**: the sync is the single consistency point that both
flushes the pre-(R) ring AND returns the matching H. The replay reads the WAL file and
HARD-FILTERS `seq <= H`.

`from:"start"` = project from seq 1; `from:cursor` = project strictly after that cursor;
`from:"live"` resolves to strictly after the last projected unit at barrier head `H`. Live
therefore replays no history and remains quietly attached for future units, including on an
idle or terminal lineage.

## 3. Admission barrier (no ghost run_id)

`session.send` on an idle lineage returns `{state:"active", run_id}` ONLY AFTER the run
acquires the lease (it's the actor, which already holds it) and writes durable
`RunStarted`. CRITICAL ordering: model/auth resolution happens BEFORE `RunStarted` — so a
resolution failure (bad model, auth-not-resolvable-at-start) is a TYPED `session.send`
ERROR, not a ghost run that wrote `RunStarted` then died. A failure AFTER `RunStarted`
(mid-stream) is a legitimate short terminal run, answerable via `run.status`, error
persisted (the typed `RunFinished.error` / `RunPaused`). The actor does the
resolve-then-RunStarted in order inside the `Send` command, before replying `active`.

**Index-before-reply ordering**: the actor commits the durable run-index
`Active{run_id}` row (§4) BEFORE replying `{active, run_id}` and before the first provider
call — so a `run.cancel`/`run.status` racing immediately after the send always resolves the
run_id (the index row is durable before the client even has the run_id). The terminal index
update is committed BEFORE emitting the terminal/closing. On an index miss (a crash in the
narrow window), the lookup falls back to scanning open actors then the WAL.

`run.cancel{run_id}` → the actor looks up its `Active{run_id, cancel}` and trips the
`CancellationToken` (cooperative cancellation); the run stops cooperatively → `Interrupted`. A
cancel for a non-active run_id is answered from the durable run index (§4). A cancel whose
actor is mid-`Closing` re-routes through the registry (1e) and re-opens to serve it.

## 4. Durable run index (§14)

A table in `store.db`: `run_id → {session, lineage, submission_id, state, wal_path}`, so
run-keyed ops (`run.cancel`/`run.status`) survive a restart and resolve a run_id to its
session without scanning. Resume recovers the original run_id via
`recover_run_id`. The actor updates the index at run start (Active, BEFORE replying
active — §3) and run terminal (Terminal/Error/Interrupted/Paused, BEFORE emitting the
terminal). `run.status(session)` resolves the in-process active run first, then WAL/index
inference; the state enum is `Idle | Active{run_id, step} | Paused{run_id, reason} |
Interrupted{run_id} | Terminal{run_id, reason} | Error{run_id, detail}`.

**Cursor ownership:** the durable cursor a subscriber resubscribes with is
CLIENT-owned — it is the client's LAST-PROCESSED position, advanced by the client as it
consumes, never the server's last-SENT position. The server never assumes delivery; on a
detach/reconnect the client resubscribes from its own last-processed cursor and the server
replays AT-LEAST-ONCE from there. Re-delivery of an already-processed cursor is the client's
to ignore (the cursor is a total order, so "I've seen ≤ C" is a clean idempotency check) —
this is the §5 reliability model (a dropped consumer re-subscribes and replays), made
precise: never advance a client cursor on the server's behalf, or a detach could skip an
undelivered event.

## 5. SubcToolPlane hardening (§19)

The tool plane requires session-keyed bounded deduplication and route lifecycle
management:
- **Fence keyed `(session, tool_call_id)`** (not the id alone — two sessions can mint the
  same provider id) **with eviction** on batch/run terminal (so it's bounded — the fence
  only needs to dedup WITHIN a run's lifetime, the WAL is the cross-run durability).
- **Per-route window resolved at route-open** from the specific provider (not memoized
  once globally — different providers/routes have different windows).
- **Route eviction (TTL/LRU + GOODBYE on idle)** against `u16` channel exhaustion, and
  **route invalidation on GOODBYE/status** (learn a dead route and re-open on next use).
- Document the per-module `store.db` `Mutex<Connection>` as a scalability limit
  (read/write split or pool if it bites).

These are `broca-subc` changes, independent of the coordinator; they can be a separate
commit.


## 7. Hard invariants (unchanged)

Byte determinism + the durability exit gates stay green. The actor/fan-out/serve is a
layer ABOVE the loop; the loop, replay, and projection are untouched (the coordinator
CALLS them). One active run per lineage (lease-enforced). The control stream is durable +
replayable (never guaranteed-live); display is live-only + lossy. No ghost run_id
(resolve-before-RunStarted).
