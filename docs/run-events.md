# Run lifecycle events

Run notifications use ck-bus's **claim-check** transport: the broker carries an
identifier, SHA-256 digest and routing headers, **not** a record body. This is an
optional acceleration of `run.status`, not a replacement for authoritative reads.

## Enablement

`bus_run_events_enabled` in `broca.jsonc` is a boolean, **false by default**. Leave
it false until the subconscious daemon/bus owner (SUBC) confirms that ck-bus
0.4.4 or later is installed and prefrontal-core's durable EVENT consumer has been created
for this machine account. Otherwise an event could be published before its
intended consumer exists.
The daemon obtains its account, JWT, server URL and inbox prefix from
`ckbus.credential` over its daemon-attested `reserved:broca` route; the private
key stays in ck-bus. Nonces use `ckbus.nonce_sign`; JWTs renew five minutes
before they expire with `ckbus.credential_renew`, and every connect and reconnect
presents the current JWT. No bus credential file or credential environment
variable is accepted.

ck-bus keeps one current key per module, and each `ckbus.credential` call retires
the previous one. So the process holds exactly **one** credential, created at boot
and shared by every NATS connection it opens for run event publication.
Compaction v2 exchanges requests over the session-scoped `compaction-provider/v2`
route instead of consuming bus announcements (see [module surface](module-surface.md)).
When `ckbus.nonce_sign` or `ckbus.credential_renew` refuses
with `ckbus_credential_superseded` or `ckbus_credential_revoked`, the holder
issues one replacement for every connection (concurrent callers share it), with
exponential backoff from 1 to 60 seconds between failed issues; a connection the
broker kicked reconnects naming the new key. Before the next publication, a key
change replaces the connection and its EVENT stream with ones using the new
key's inbox prefix. Retained publisher facades follow the replacement. The old
connection is then closed with a two-second deadline; a pending close logs WARN
and continues draining in the background when the broker reconnects. Its old-inbox
permission violations are not observed as publisher health or diagnostics.
The swap logs INFO with only the first eight characters of the old and new public
keys. Same-key JWT renewal keeps the connection and does not close or redial it.
On `ckbus_generation_not_live` it issues nothing: the process is being replaced,
so it logs once and makes no further ck-bus calls. Every ck-bus failure keeps its
refusal code and is logged at WARN once per distinct cause and method.

Every subject is constructed and validated by the `bus-naming` crate the workspace pins.
That linked validator is the dotted-event support contract; there is no invented server
capability field. Disabled means no publisher task, no event derivation and no outbox writes.
Recording an event needs the machine account, which is known once the bus has connected; until
then events wait in memory, in order, up to 10,000 batches, after which the oldest batch is
dropped. Once the account is known, a broker outage does not stop events reaching the outbox.

### What becomes an event

Events describe what happens while the switch is on; history never becomes an event. Each
session's writer derives them as an append observer, from the records that a barrier makes
durable, so no event names a record a crash could take back. A session's events begin with the
first record appended after its writer opens with events on. A run seeded by an import or a fork
never calls a provider and has no events.

## Subjects and payload

The machine account comes from ck-bus, not the caller:

| Subject | Additional JCS fields |
| --- | --- |
| `ck.{acct}.event.broca.run.started.v1` | none |
| `ck.{acct}.event.broca.run.paused.v1` | `pause_reason` |
| `ck.{acct}.event.broca.run.ended.v1` | `terminal`, optional `error_class`, `structured_output` |
| `ck.{acct}.event.broca.run.steer_consumed.v1` | `step`, `consumed`, `send_ids`, `truncated` |
| `ck.{acct}.event.broca.run.progress.v1` | `step`, optional `last_content_at_ms`, `max_content_gap_ms` |
| `ck.{acct}.event.broca.run.skipped.v1` | see "Stale backlog"; its body has its own fields |

All bodies contain `v:1`, `session_ref` (the shared llm-runner 32-hex handle),
`run_id`, `seq` and `at_ms`. Lifecycle `seq` is the causing record's sequence, and `at_ms`
the record's own timestamp, or the wall clock when the record has none. Steer consumption is
caused by the `StepStart` that places the steer, never by steer admission or by a terminal that
merely places undelivered steers in the transcript. Progress is an in-flight observation, not
a record: its `seq` is the session's last durable record, and its observation time
distinguishes samples.
`run.status` is the authoritative read for a step's phase and transport deadlines. Those fields
are not added to the progress event.

`terminal` uses the `run.status` vocabulary (`completed`, `cancelled`,
`interrupted`, `error`, `max_steps`, `budget_exhausted`, `transform_unavailable`).
Error classes use only `transient`, `permanent`, `auth_required`,
`context_overflow`. Structured output is `passed`, `failed` or `absent`; values
and validation errors never leave the session read surface.

Bodies are RFC 8785 JCS, **strictly less than 512 bytes**. Prompts, outputs, error
text and tool arguments are excluded. Oversize events are refused with a WARN and
counter, not silently cut. Only the prefix of `send_ids` may be shortened, with
`truncated:true`; `consumed` still counts every steer, including anonymous sends.
Pinned byte vectors live in `crates/broca-wire/tests/golden/bus_event.*.json`.

## Publish and fetch

`Stream::publish(subject, event_key, SHA256(jcs), headers)` uses these routing
headers: `kind` (for example `run.ended`), `run_id`, `session_ref`, `epoch` (the
session's incarnation, in hex), decimal `seq`, and `session`. `session` is the plain session name
of the session's bind triple, for a module that knows only its own route's session
name and so can't compute `session_ref`. It is left out when the name holds a
character outside printable ASCII, which a NATS header can't carry; such an event is
still published. The event key is lowercase SHA-256 of
the JCS tuple `[epoch,run_id,seq,subject,null]`; progress substitutes its `at_ms`
for `null`. A recreated session therefore cannot collide with an old sequence.

`run.event.get {"event_key":"<64 lowercase hex>"}` on the owning session returns
`{"state":"found","event_key":"...","jcs":"<exact UTF-8 JCS string>"}` or
`{"state":"absent"}`. Hash **the UTF-8 contents of `jcs`**, not the response
envelope or a JSON reserialization. The operation uses the same session
authorization and migration fence as `run.status`; keys owned by another session
are indistinguishable from missing keys. This broca-specific management extension
does not add a required operation to the shared llm-runner role.

## Guarantees and reconciliation

Lifecycle events are derived only from records a barrier has made durable. The session's
writer hands each durable batch's events to the outbox in `run-index.db` before it goes on, so
a run's `run.ended` is in the outbox before its end retires the session's recovery
registration. A crash can lose events only between the barrier and that write, or while they
wait for the machine account. Recording an event twice adds nothing: rows are keyed by epoch,
run, sequence and subject. One publisher drains in insertion order and marks
published **only after a JetStream `PublishReceipt`**, retrying failures with
exponential backoff (1–60 seconds). Each loop pass (every 250 ms) publishes until
the outbox is empty or 200 ms have passed. `bus_outbox_depth` is a counter moved
by each insert, acknowledgement and prune, and set from an indexed count only
when the publisher first learns its machine account from ck-bus. A crash after broker ack but before the mark
can republish the identical key/digest. Broker deduplication is finite; consumers
must also be idempotent.

### Stale backlog

While events are on, a queued lifecycle event leaves the outbox only by being
published or by being covered by a `run.skipped` marker for its session, which is
then queued and published like any event (the 24-hour retention prune is the only
exception). When a bus connection comes up (including
the first after boot) and after any failed publish, queued events whose own `at_ms`
is more than **2 minutes** old (`STALE_BACKLOG_MS`) are not published. Core
reconciles every run by polling while events are down, so such events tell it
nothing, and replaying them does harm: each stale `run.ended` triggers a status
poll, and a stale `run.progress` refreshes a worker's liveness clock with replay
time. A gap shorter than 2 minutes publishes normally.

Instead, one marker per affected session names what was skipped. Its JCS body is
`session_ref`, `from_seq` and `to_seq` (the first and last skipped events in record
order), `from_ms` and `to_ms` (the earliest and latest of their clocks) and
`count`. A session's skipped range runs from its first queued event through its
last stale one, so nothing published for the session predates its marker. The
marker is published with headers `kind` (`run.skipped`), `session_ref`, `epoch`
(of the last skipped event) and `seq` (`to_seq`), and no `run_id`. Its key is the
lowercase SHA-256 of the JCS tuple `[epoch,session_ref,from_seq,to_seq,subject]`,
and `run.event.get` serves its body like any run event. It goes out **before**
that session's younger events, which keeps per-run order; events of other
sessions are not reordered.

The skipped rows leave the outbox in the same transaction that inserts their
marker, so a crash leaves either the rows (the next connection replaces them) or
the marker (published like any event; a crash after its publish and before the
mark republishes the identical key and digest), never neither and never both.
Publishing waits until the replacement has succeeded. Consumers should treat an
unknown kind such as `run.skipped` as a reason to reconcile that session once.
Like the other run events, `run.skipped` is published without a manifest event
declaration: the daemon's HELLO validator that broca builds against refuses dotted
event names.

Acknowledged bodies remain in the database for claim-check reads. A prune pass
removes rows older than 24 hours and counts/WARNs once per pass for unacknowledged
lifecycle drops. Events already older than 24 hours when they are recorded are dropped and
counted. Startup crash-orphan seals and non-resumable shutdown seals derive `run.ended` with
`terminal:interrupted` from their durable `RunEnd` records; restart-policy pauses instead
produce `run.paused`.

Progress is best effort, only during an in-flight model step and at most once
per two minutes per active run. Its body is retained for audit, but a failed
publish is counted and not queued for retry. Repeated samples can share a sequence and
must not be treated as lifecycle transitions.

Consumers deduplicate lifecycle transitions on **`(run_id, seq)`**, scoped to `epoch` when
a session is recreated. Do **one reconcile read of active
runs at startup and after any delivery gap** (including expiry or loss of a
consumer). Headers suffice for ordinary lifecycle handling; Core can read
`run.result` after `run.ended`, and fetch the claim-check body only when needed.

`health.check` exposes `bus_events` with `bus_events_published`,
`bus_outbox_depth` (pending lifecycle rows, not retained bodies),
`bus_publish_failures`, `bus_events_oversize`, and `bus_outbox_dropped`.

Those counters move only once events reach the outbox, so `bus_events` also says
whether an enabled publisher is doing its work:

- `bus_account_known`: the machine account is known, so events can be recorded. The
  log announces it once as `run events armed`.
- `bus_store_batches_recorded`, `bus_store_batches_waiting` and
  `bus_store_batches_dropped`: batches of events sessions' writers handed over that reached
  the outbox, that wait for the account, and that were dropped because too many waited.
- `bus_send_state_batches_dropped` and `bus_route_fact_batches_dropped`: batches of send
  state notices and of route facts owed to routing that were dropped because too many waited
  for the account.
- `bus_loop_pass_age_ms`: milliseconds since the publisher loop began its latest
  pass. A running loop begins one every 250 ms; a value that keeps growing means a
  pass is stuck. Null when `bus_run_events_enabled` is off.
