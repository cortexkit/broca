# Session actors and the registry

How `broca-module` serves an open session: one actor per session, reached through a registry,
holding the session's lease for its whole life, and fanning its output out to subscribers. The
methods and their payloads are in [the module surface](module-surface.md), the rules a run keeps
across a crash in [the durability contract](durability-contract.md), and the storage in
[the session store](session-store.md).

## 1. Module boundary

- **subc owns the wire mechanics**: frames, `route.open`, the catalog and the supervisor. A
  caller's `call("broca", "session.send", {…})` reaches broca as an opaque body on a route; subc
  never parses it.
- **broca owns `session.*` and `run.*`**: the method names and payloads are this module's
  surface. Their wire types live in `broca-wire`.
- **`broca-module`** owns the registry, the session actors, the fan-out to subscribers and the
  serve handlers. The run loop is `broca-session-loop`; the actor calls it and does not change
  how it renders or records.

## 2. The registry: one actor per open session

The registry maps a session's triple `(project_root, harness, session)` to its actor
(`crates/broca-module/src/registry.rs`). A session's slot is in one of three states, and an
absent slot means closed:

- `Opening`: an open is in progress. The slot carries a broadcast channel that will carry the
  open's result.
- `Open`: the actor is serving. The slot carries the actor's handle and a generation number that
  identifies this actor among the session's successive ones.
- `Closing`: the actor is tearing down. The slot carries a channel that fires when it is done.

The first caller to find no slot inserts `Opening` and performs the open. A caller that finds
`Opening` waits on its channel and receives the same result, so concurrent callers share one
open. A caller that finds `Closing` waits for the close and then tries again.

An open that fails removes the slot before it sends the error, so every waiter gets the failure
and a later caller can try again. If the opening task is cancelled or dropped before it
resolves, a guard removes the slot and wakes the waiters with an error, so a slot is never left
`Opening` with nobody to finish it.

## 3. The actor and its lease

An actor is one task that processes its session's commands one at a time, so commands never
interleave inside a session.

**The lease.** Opening a session takes its writer lease (an OS advisory lock under `leases/`,
released by the OS if the process dies), and the actor holds it for its whole life. A second
process cannot open the session while it is held.

**When it closes.** An actor that has been idle for its idle period (30 seconds by default)
closes, unless something holds it open: a running run, a live subscriber, a warm-up the
caller asked for (requests that keep the provider's prompt cache from expiring), the timer that
resumes a run paused on an account limit, or a sign-in check that is still owed. If subscribers
are the only hold for a session idle for five minutes, the actor closes once they finish catching
up and the registry keeps their streams open. On reopen the new actor attaches them before any
command, after the last control it delivered (or from the original attach point if it delivered
none). Only the actor can write records, so none are missed while the session is closed.
A queued send that will start a run keeps the actor serving through that run. An actor also stops
when its command channel closes, at shutdown, when retention expires its session, and when a
move to another computer cuts the session.

**Teardown order.** The actor marks its slot `Closing`, stops its work, runs the executor's close
hook (bounded at five seconds), closes the session, releases the lease, and only then clears the
slot. A caller arriving meanwhile waits for the close, so it never opens the session while the
old actor still holds the lease.

## 4. Sends, the queue and cancel

**A send on an idle session.** Admission resolves everything the run needs (model, credential,
plan, cache policy, limits) before any record is written, so a send it refuses writes nothing
and returns a typed error. The reply that names the new run's `run_id` is sent only after the
run's `RunStart` is synced. A caller that immediately asks about that run therefore always finds
it.

**A send while a run is active** is queued: it is written as a `QueuedSend` record and starts a
run when the current one ends. A `Retraction` withdraws it. A queued send is consumed when a
`RunStart` names its submission id, or ended when a `SendEnded` record closes it (a send whose
admission failed, for example). The pending set is the queued sends that have none of these,
read from the records, so a crash on either side of a start or a retraction leaves it
unambiguous. The actor is the session's only writer, so a retraction and a start that race are
ordered by whichever record it writes first.

**Finding a run.** `run.status` finds a run by its id through the session store's id index,
which covers both sealed history and the open log, and walks the run's records from its
`RunStart`. `run.cancel` applies to the session's active or paused run: the actor writes a
`CancelRequest` and syncs it before it acknowledges the cancel, and the run ends `Cancelled`.

## 5. Subscriptions

Subscribers get two lanes. The **control lane** is reliable: its units are projected from the
session's records, the same units whether they are replayed or sent live, each addressed by a
cursor `(wal_seq, sub_index)` (the record's sequence number and the unit's index within that
record's projection) plus the log's incarnation. A recreated log refuses a stale cursor with
`cursor_expired`. The **display lane** carries token deltas; it is lossy and never replayed.

**Attaching without a gap or a duplicate** (`crates/broca-module/src/store_fanout.rs`). Under the
publisher's lock, the attach subscribes to the live ring and takes the publisher's current
position as its barrier, `head`, in one step. For an attach from a cursor, the subscriber then
replays the records after that cursor up to and including `head`, in chunks. After the replay it
reads the live ring it registered at the barrier and delivers only units after `head`. A unit
at or before `head` came from the replay, and a unit after it can only arrive live, so each unit
is delivered exactly once. An attach from `live` replays no history: it starts from the open
step's units as of the barrier.

**A slow subscriber.** The control ring holds 1024 units. A subscriber that misses a control unit
is detached rather than handed a stream that only looks contiguous; it resubscribes from its
last cursor and replays from the records. A slow reader never slows the run or other
subscribers. On the display lane a lagging subscriber gets an in-band `DisplayGap` with the
number of deltas dropped.

**The cursor belongs to the client.** A subscriber resubscribes with the last position it
processed, never one the server assumes it received. The server replays at least once from
there, and the client skips anything at or before a cursor it has already seen.
