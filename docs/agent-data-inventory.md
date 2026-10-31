# Agent-data reference discovery

Agent-data discovers shared session refs through an additive SQLite projection in
`run-index.db`, not a process-wide identity map or a JSON cache. The
`agent_data_session_ref` table maps the commons llm-runner `session_ref` (32 lowercase
hex characters) to the full bind triple and logical WAL address. Its primary key
indexes ref lookups; a compact address index supports the one-time migration.
There is no duplicate project-root index and no cached live filename, archive
filename, payload or byte offset.

## Completeness and admission

Session binds also use this inventory to retain an existing pair's original root.
An existing `(harness, session)` pair is resolved in a read transaction without
acquiring SQLite's write lock. Only an absent pair takes an immediate transaction,
rechecks completeness and the pair's roots, and reserves a candidate if still absent.
Competing first binds share that writer transaction, so they cannot select different
roots. A refused bind or an
interrupted first send can therefore leave an identity-only candidate without a WAL.
Candidates still do not prove that content exists or is locally owned.

Before a `RunStarted` append, ordinary and preencoded writers (including
provider-free imports) attempt to commit the candidate. The connection mutex and
SQLite write lock share a bounded wait budget matching the store's five-second
busy timeout, so ordinary migration contention waits rather than creating a gap.
SQLite contention beyond that budget or a
projection failure does **not** block the run: the writer instead creates and
syncs the owner-only, content-free `session-ref-projection.incomplete` marker in
the state root, then appends normally. Only failure to durably establish that
marker prevents the append. Move activation uses the same fallback before
installing verified bytes. Import staging alone does not claim a local copy. Only identities whose derived address matches the
WAL filename's address (or the archive member header's address) are eligible;
historical triples in re-keyed WALs do not
become aliases.

Writing the candidate first deliberately permits conservative extra rows after a
failed or interrupted append/installation. Lookup never proves that bytes are held:
the move resolves the current live/retained/archive location and verifies its actual
WAL bytes. WALs remain the authority. Retention's durable ledger remains the
authority for expired, non-exportable sessions. Bound cuts use their recorded
addresses rather than rediscovering identity.

A missing ref proves absence only when the query's SQLite snapshot contains both
migration completion markers, no pending legacy admissions, and no filesystem
gap marker. Until then it is
unknown, using the existing retryable response. Known candidates can still be
verified independently. SQLite failures likewise never prove absence. Empty ref
sets bypass discovery.

## One-time migration, not a boot inventory

Startup creates the additive schema, then schedules migration off the serve path
on a separate SQLite connection. It does not hold the serving connection's mutex
while backfilling. Run-index rows are read and decoded outside write transactions,
then committed in small keyset-paged batches with a durable run-id cursor. The
completion marker is recorded only after the source scan finishes. Invalid legacy
identities leave missing session refs
unknown rather than certifying that no local copy exists.

Migration is scheduled once by `AgentDataService::reconcile_boot`, not by refused
binds or a periodic repair loop. An in-flight startup migration can restore completeness
without another restart. Once that attempt has ended, a new gap marker, an older
writer's session identity not yet added to the inventory, or an uncovered WAL address
is not automatically repaired in the running process;
restart schedules the next automatic attempt. Fix unreadable or malformed source data
before expecting repair to complete. A temporary store error can clear on a later bind
without migration when the store recovers and its completeness evidence remains valid.
Do not remove a gap marker merely to silence refusals: repair verifies coverage and
briefly takes a per-root filesystem lease to detect concurrent WAL appends or
installations whose identities could not be recorded in SQLite before durably clearing
the marker.

Old imports, move activations and interrupted admissions may have WALs without
`run_index` rows. A one-time metadata census closes those gaps across `wal/`,
`wal-ephemeral/`, `wal-durable/` and sealed `wal-archive*/` members. Represented live
addresses get only stat and fixed head/tail lineage probes. Only uncovered
addresses get full verified WAL reads through `ReadOnlyWalReader`, which has no
writer, manifest, lease, extraction or repair API. Archive enumeration reads indexes/headers,
not represented member payloads. Unreadable or ambiguous locations prevent census
completion, never become absence.

All migration writes (schema, backfill, queueing, coverage, reset and completion)
use at most 64 work items per transaction and check a 10 ms work budget between
items. File reads and JSON decoding occur outside the write lock. Commit/fsync
cannot be preempted; completion logs and the copy-only timing tool report the
measured longest lock hold, including commit and excluding lock-acquisition wait.
Every transaction releases the lock, yields and leaves an unlocked pacing interval
before attempting the next batch. The migration connection does not run automatic
checkpoints inside its commits.

The migration connection waits up to the same five-second store budget for a
write lock, polling at a fixed one-millisecond interval. SQLite's default busy
handler backs off to much longer sleeps and can repeatedly miss the short unlocked
gaps in a burst of projection or export commits. Short polling avoids that
backoff-induced starvation without retrying failed batches or extending the wait
budget. Foreground connections keep their existing busy-timeout policy.

Census progress and in-flight addresses are durable. An interrupted address with
partially inserted candidates is replayed rather than mistaken for a fully
represented WAL. After interruption, enumeration adds any new addresses but
already-covered payloads and probes are skipped. Work yields between addresses
and pauses three times each batch's work duration, including SQLite commits.
Completed migration is not repeated on normal restarts. If the gap marker exists
at boot, migration reopens census completion and revisits unrepresented addresses,
including previously empty WALs; already-covered candidates are retained. After
the store is writable and census succeeds, the marker is unlinked and the state
root synced. A short per-root filesystem lease, shared across processes, prevents
a concurrent failed projection append or installation from clearing a marker
across the census. Each acquisition increments a persisted counter: repair records
that counter before scanning, reacquires the lease at completion, and clears the
marker only if the counter increased by exactly one for its own reacquisition.
Any intervening writer or repair leaves the marker in place. No lock is held for
the census duration and no session identities are cached in memory.
A failed or overlapped repair leaves the marker for the next boot, not a false
absence or a failed run.
Downgrade-then-reupgrade is outside the projection-completeness guarantee: an older
binary can add WAL-only sessions that the already-completed migration will not discover.
Legacy `run_index` writers
are tracked by triggers; current writers suppress those triggers only within the
same transaction that records the candidate. Legacy gaps invalidate completeness
and are reconciled on the next migration.

At boot, obsolete `move-inventory.json` and `move-inventory.tmp-*` files in the
state root are removed without following links or recursing. One INFO event
reports the count and bytes freed. There is no inventory thread, JSON checkpoint,
persistent RAM identity map or inventory-progress health metric.

## Read and send fencing

Ordinary reads check the canonical `.moved` sidecar, which records that another
machine owns the session and prevents exposing the old local bytes. Write checks also enforce
`.moving` and unfinished release/activation intents. At boot a small, durable
per-ref fence index is rebuilt from cut JSON. Cut saves publish fences before
intent, and remove them only after completion is durable. Checks then stat the
ready marker and open only the named ref's fence, not every cut JSON file.
Readers without initialization (including older binaries) retain the conservative
cut scan. Cut JSON remains the downgrade authority; this changes no WAL format.

Busy probes inspect the requested WAL for active runs, queued sends and unsettled
approvals, with the existing actor, lease and move-fence observations. INFO events
report op, cut id, ref count and elapsed milliseconds, including cancelled calls.
`health.check.metrics.agent_data_ops` reports each operation's cumulative count.
