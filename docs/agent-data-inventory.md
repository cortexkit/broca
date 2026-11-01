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
An existing `(harness, session)` pair is positive evidence and resolves in a read
transaction without acquiring SQLite's write lock or waiting for discovery repair.
Two recorded roots still refuse as ambiguous, including during repair. Only an absent pair takes an immediate transaction,
rechecks completeness and the pair's roots, and reserves a candidate if still absent.
Competing first binds share that writer transaction, so they cannot select different
roots. A refused bind or an
interrupted first send can therefore leave an identity-only candidate without a WAL.
Candidates still do not prove that content exists or is locally owned.

An identity reservation alone remains a new session, including after an interrupted
first bind. Subscribe-before-send may create its first lineage, preserving the
subscriber's cursor origin. Historical content is different: any run-index row,
materialized projection/meta row, covered log census entry, or deletion
ledger/tombstone for the exact key is content evidence. When that evidence exists
but neither a live log nor an archive member is available, reads and actor-opening
operations refuse `session_log_missing` (`detail.class: "permanent"`) unless removal
is proven deliberate. The same refusal applies to a verified lineage-only log
whose lineage differs from unexpired historical run or projection evidence.
File length is only a probe prefilter, never the lineage proof; ordinary
content-bearing logs take no additional content read. The refusal names the session and
`store-maintenance repair-session-root`; it never guesses an address from the
route's current folder. Expired reads keep `expired`; move fences keep
`session_moving` and `session_moved`. A send after completed expiry can still
start a fresh lineage.
`health.check.metrics.session_address_refusals.session_log_missing` counts served
refusals, `diagnostics.findings` reports an error naming the session, and a WARN is
limited to one per minute. An older deletion cannot excuse a missing newer,
unexpired run or a different materialized lineage.

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

## Migration and on-demand repair

Startup creates the additive schema, then schedules migration off the serve path
on a separate SQLite connection. It does not hold the serving connection's mutex
while backfilling. Run-index rows are read and decoded outside write transactions,
then committed in small keyset-paged batches with a durable run-id cursor. The
completion marker is recorded only after the source scan finishes. Invalid legacy
identities leave missing session refs
unknown rather than certifying that no local copy exists.

Boot schedules migration without blocking serve startup. A bind for an unrecorded
pair that finds incomplete inventory joins that migration, or starts one if none is
running. Concurrent binds share one repair and its readiness signal, rather than
polling SQLite. Known-session binds, reads and resumes keep serving. Each new bind
waits at most 60 seconds, then rechecks resolution; a timeout does not cancel the
shared repair. A later bind may start another attempt after a failed repair.

The refusal remains `session_address_unavailable`, with `detail.class: "transient"`
and `detail.reason` identifying `wait_expired`, `migration_failed`, or a remaining
`gap_marker`. The code is not in subc-protocol 0.33.4's retryable route-open set;
callers must not assume the transient class alone enables transport retries.
`health.check.session_address_refusals.repairs` counts started, completed and failed
attempts, and `diagnostics.findings` retains timestamped `session_inventory_repairs`
evidence. These counts describe repair attempts, not sessions or census rows.

Mid-run gap markers and legacy pending rows can therefore recover on the next
new-session bind without a restart. There is no periodic repair loop. Fix unreadable
or malformed source data before expecting repair to complete. A temporary store
error can clear on a later bind without migration when the store recovers and its
completeness evidence remains valid.
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
A failed or overlapped repair leaves the marker for the next repair attempt, not a false
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

## Offline reference audit

```sh
store-maintenance audit-session-refs STATE_ROOT
```

The audit is read-only, without schema initialization, leases, extraction or WAL
writers. It streams JSON lines for missing historical logs and verified forked
lineage-only stubs, then a `{"findings": N}` summary. Exit 0 means no findings,
1 means findings, and 2 means the audit could not complete. A bind reservation
holding only identity and address, with no historical-content evidence, is not
a finding. Runtime and audit share the decision whether historical content is
still reachable or was deliberately removed. Archive members remain readable;
expiry records prove removal. The canonical `.moved` ownership marker excludes
content owned by another machine, as described under **Read and send fencing**.

Each finding includes the reference and bind identity, the state (`missing` or
`forked_stub`), and any alternate logs with a matching recorded lineage. Candidate
root names must already be recorded in the store; log addresses cannot be
reversed into folder names, and the audit never guesses from a caller's current
folder. Without historical lineage evidence it reports a missing address without
claiming a matching alternate. It retains compact address/root sets and streams
references, rather than loading a join of the run history into memory. Findings
are evidence for repair preview, not permission to merge transcripts.

## Offline split-root repair

If a historical WAL re-key left a session's run-index rows and refs under two
project roots, inspect the stopped store with:

```sh
store-maintenance repair-session-root STATE_ROOT --harness HARNESS --session SESSION
```

This is **preview-only by default**. The JSON lists each terminal run row to
re-point (run ID, old and new identities, the matched episode's embedded identity
and terminal state), each v1 ref and v2 alias to remove
(including root and session), orphaned projection table row counts, the stale
meta row's lineage, watermark and last run ID, the projection evidence used, and
any obsolete address discovery rows. Keep a
backup, review that plan, then repeat with `--write` to apply it. The write prints
and flushes its plan before applying, rechecks under a SQLite writer transaction,
and refuses a changed preview. A repaired one-root pair is a no-op on repeat.

**Stop broca and its supervisor for the entire write operation.** Live or unknown
writer liveness refuses before SQLite opens. The repair additionally acquires
both canonical session leases to exclude standalone writers; these leases do not
replace the stopped-supervisor requirement. Preview uses a read-only connection,
without schema initialization or leases.

Repair requires a complete inventory, exactly two roots, and exactly one address
with a WAL across all live trees and sealed archive trees. It refuses deletion
ledgers/tombstones, expired lineage records, move/cut fences, unreadable sources,
and competing logs.
Each stale run must be terminal and match an episode by run ID in the surviving
verified WAL. That episode must end in a durable `run_finished` whose terminal
state matches the row. The embedded `run_started` triple is reported as historical
provenance, not used to match the run: folder and harness re-keys change the
address or run index without rewriting log records. Missing run IDs, unfinished
episodes and mismatched terminal states refuse repair rather than
deleting unproven run metadata. Successful repair changes only the proven run
identities, stale inventory/discovery mappings, and the stale key's orphaned
projection rows, in one transaction. It never merges projections, changes live
key rows, changes log bytes or extracts an archive. All other run metadata is
retained. A present stale meta lineage must match the verified surviving log's
lineage; a known different lineage refuses even if transcript bytes happen to
match. Legacy NULL meta lineage means unknown, not different: cleanup requires
every stale `message` row to have a live-key row at the same `ord` with
byte-identical stored `json`, and stale `meta.last_run_id` must occur in a verified
live-log `run_started`. Missing or differing rows refuse with
`null_lineage_prefix_mismatch`; a missing or unrepresented last run ID refuses
with `null_lineage_last_run_missing`.

Preview reports `stale_projection_evidence.kind` as `lineage_match` or
`null_lineage_prefix_match`, with `compared_message_rows`. The known-lineage proof
compares no message rows (count 0); the NULL-lineage proof reports the number of
stale rows compared, without JSON normalization. A run row's recorded lineage,
when present, must also match. The live key must have its own meta row,
or the verified log must support rebuilding the projection. Cleanup never
rebuilds or rewrites the live key itself.

The flat-session-key projection tables checked for cleanup are `meta`, `message`,
`message_tag`, `episode_span`, `retention_lineage`, `retained_head`,
`compaction_facts`, `bus_event_projection` and `bus_event_open`. Preview lists
every existing table with a `session` column and its exact stale-key row count;
`remove` identifies the known projections. An unrecognized table with stale-key
rows refuses rather than risking authoritative data. Tables using JSON identities
or bare session names do not match that flat key and are not deleted. Billing
facts, `export_imported_runs` records preventing double billing of imported runs,
`retention_withdrawal` provider-withdrawal work, deletion ledgers and move fences
remain intact. The existing `retention_lineage` delete trigger also removes its
derived `retention_discovery` hint; preview reports that count separately.

The bind-time ambiguity rule is unchanged: a missing log alone is not proof that
an inventory identity can be discarded. Interrupted admissions, expiry and moves
can legitimately leave identity candidates without local content. The legacy
`rekey-harness.py` refuses inventoried stores before changing anything; it does
not implement a general inventory-aware harness or project-root migration.
