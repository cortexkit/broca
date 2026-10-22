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
and shared by every NATS connection it opens (the run event publisher and the
compaction consumer). When `ckbus.nonce_sign` or `ckbus.credential_renew` refuses
with `ckbus_credential_superseded` or `ckbus_credential_revoked`, the holder
issues one replacement for every connection (concurrent callers share it), with
exponential backoff from 1 to 60 seconds between failed issues; a connection the
broker kicked reconnects naming the new key, and the publisher reconnects on it.
On `ckbus_generation_not_live` it issues nothing: the process is being replaced,
so it logs once and makes no further ck-bus calls. Every ck-bus failure keeps its
refusal code and is logged at WARN once per distinct cause and method.

Every subject is constructed and validated by bus-naming **0.2.3**. That linked
validator is the dotted-event support contract; there is no invented server
capability field. Disabled means no publisher task, no event projection and no
outbox writes. If credentials are unavailable, WAL recovery waits and retries;
once the account is known, a broker outage does not stop outbox projection.

### What becomes an event

Events describe what happens while the switch is on; history never becomes an
event. Each time the daemon starts with events on after a start with them off
(including the first start ever), a new **enabled period** begins and is
recorded in `run-index.db`. A new period discards unpublished lifecycle rows and
every projection checkpoint left by an earlier period (published bodies stay
until pruned) and counts the discarded rows in `bus_outbox_dropped`. A start
with events still on continues the recorded period. A start with them off
records that, so turning them on again begins a fresh period.

A session's events begin with its first run whose `RunStarted` is stamped at or
after the period began, by that record's own clock; earlier records are skipped,
not replayed. The publisher finds that run when it first projects the session,
reading only the end of the WAL: it starts with the last 256 KiB and widens the
read fourfold until it holds a timestamped record from before the period, or
the whole file. Once a session has a checkpoint, each projection reads back only
as far as the checkpoint. Nothing on the WAL write path records anything for
events. A run already in flight when events are turned on emits nothing.

Recovery after process loss visits only sessions whose projection has a run
open (started, not yet seen finished) and sessions whose run the run index shows
active or paused since the period began. A run that became durable but was
neither projected nor admitted to the run index before the process died is
missed, as the crash-orphan sweep, which reads the same run-index rows, misses
it. Recovery never walks every session in the store.

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
`run_id`, `seq` and `at_ms`. Lifecycle `seq` is the causing WAL record's sequence.
Steer consumption is caused by `StepStarted`, never by steer admission or by a
terminal that merely places undelivered steers in the transcript. `StepStarted`
has no recorded clock: its event uses the last pending steer's recorded admission
time. Legacy records without a clock use the latest known WAL clock, or zero.
Progress is an in-flight observation, not a new WAL record: its `seq` is the
latest projected durable WAL head, and its observation time distinguishes samples.

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

`Stream::publish(subject, event_key, SHA256(jcs), headers)` uses exactly these
routing headers: `kind` (for example `run.ended`), `run_id`, `session_ref`, `epoch`
(the WAL lineage UUID), and decimal `seq`. The event key is lowercase SHA-256 of
the JCS tuple `[epoch,run_id,seq,subject,null]`; progress substitutes its `at_ms`
for `null`. A recreated WAL therefore cannot collide with an old sequence.

`run.event.get {"event_key":"<64 lowercase hex>"}` on the owning session returns
`{"state":"found","event_key":"...","jcs":"<exact UTF-8 JCS string>"}` or
`{"state":"absent"}`. Hash **the UTF-8 contents of `jcs`**, not the response
envelope or a JSON reserialization. The operation uses the same session
authorization and migration fence as `run.status`; keys owned by another session
are indistinguishable from missing keys. This broca-specific management extension
does not add a required operation to the shared llm-runner role.

## Guarantees and reconciliation

Lifecycle events are derived only through a successful WAL durability barrier.
An independent event projection atomically updates the run index, its WAL
checkpoint, and the outbox in `run-index.db`; it does not wait for transcript
turn-boundary projection. Recovery re-derives rows idempotently by WAL epoch,
run, sequence and subject. One publisher drains in insertion/WAL order and marks
published **only after a JetStream `PublishReceipt`**, retrying failures with
exponential backoff (1–60 seconds). Each loop pass (every 250 ms) publishes until
the outbox is empty or 200 ms have passed. `bus_outbox_depth` is a counter moved
by each insert, acknowledgement and prune, and set from an indexed count only
when the publisher first learns its machine account from ck-bus or an enabled
period (see "What becomes an event") begins. A crash after broker ack but before the mark
can republish the identical key/digest. Broker deduplication is finite; consumers
must also be idempotent.

### Stale backlog

Within an enabled period, a queued lifecycle event leaves the outbox only by being
published or by being covered by a `run.skipped` marker for its session, which is
then queued and published like any event (the 24-hour retention prune and the
discard at a new period's start, described above, are the only exceptions). When a bus connection comes up (including
the first after boot) and after any failed publish, queued events whose own `at_ms`
is more than **2 minutes** old (`STALE_BACKLOG_MS`) are not published. Core
reconciles every run by polling while events are down, so such events tell it
nothing, and replaying them does harm: each stale `run.ended` triggers a status
poll, and a stale `run.progress` refreshes a worker's liveness clock with replay
time. A gap shorter than 2 minutes publishes normally.

Instead, one marker per affected session names what was skipped. Its JCS body is
`session_ref`, `from_seq` and `to_seq` (the first and last skipped events in WAL
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
lifecycle drops. Recovery also discards timestamped lifecycle events already older
than 24 hours. Startup crash-orphan seals and non-resumable shutdown seals derive
`run.ended` with `terminal:interrupted` from their durable `RunFinished` records;
restart-policy pauses instead produce `run.paused`.

Progress is best effort, only during an in-flight model step and at most once
per two minutes per active run. Its body is retained for audit, but a failed
publish is counted and not queued for retry. Repeated samples can share a WAL
sequence and must not be treated as lifecycle transitions.

Consumers deduplicate lifecycle transitions on **`(run_id, seq)`**, scoped to the
WAL `epoch` when a session log is recreated. Do **one reconcile read of active
runs at startup and after any delivery gap** (including expiry or loss of a
consumer). Headers suffice for ordinary lifecycle handling; Core can read
`run.result` after `run.ended`, and fetch the claim-check body only when needed.

`health.check` exposes `bus_events` with `bus_events_published`,
`bus_outbox_depth` (pending lifecycle rows, not retained bodies),
`bus_publish_failures`, `bus_events_oversize`, and `bus_outbox_dropped`.

Those counters move only once events reach the outbox, so `bus_events` also says
whether an enabled publisher is doing its work:

- `bus_account_known`: the machine account is known, so projection can run. The
  log announces it once as `run events armed`.
- `bus_hints_seen`: durable WAL barriers the publisher was told about.
- `bus_sessions_projected`: session projections into the outbox that completed.
  Barriers seen with nothing projected means projection is not running.
- `bus_recovery_pending`, `bus_recovery_walks` and `bus_recovery_visits`:
  sessions the current recovery walk has yet to visit, walks finished, and
  sessions visited in total. Recovery visits only sessions with a run open in
  the current period, a slice of at most 100 ms per loop pass, so live runs are
  projected and published while it is under way.
- `bus_loop_pass_age_ms`: milliseconds since the publisher loop began its latest
  pass. A running loop begins one every 250 ms; a value that keeps growing means a
  pass is stuck. Null when `bus_run_events_enabled` is off.
