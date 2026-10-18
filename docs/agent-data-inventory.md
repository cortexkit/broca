# Agent-data reference discovery

Agent-data discovers shared session refs through an additive SQLite projection in
`run-index.db`, not a process-wide identity map or a JSON cache. The
`agent_data_session_ref` table maps the commons llm-runner `session_ref` (32 lowercase
hex characters) to the full bind triple and logical WAL address. Its primary key
indexes ref lookups; a compact address index supports the one-time migration.
There is no duplicate project-root index and no cached live filename, archive
filename, payload or byte offset.

## Completeness and admission

A candidate is committed **before** a `RunStarted` append can reach disk, including
preencoded appends and provider-free imports. A projection write failure prevents
the WAL append and its acknowledgment. Move activation records eligible identities
from the verified incoming WAL before installing any bytes. Import staging alone
does not claim a local copy. Only identities whose derived address matches the
physical logical address are eligible; historical triples in re-keyed WALs do not
become aliases.

Writing the candidate first deliberately permits conservative extra rows after a
failed or interrupted append/installation. Lookup never proves that bytes are held:
the move resolves the current live/retained/archive location and verifies its actual
WAL bytes. WALs remain the authority. Retention's durable ledger remains the
authority for expired, non-exportable sessions. Bound cuts use their recorded
addresses rather than rediscovering identity.

A missing ref proves absence only when the query's SQLite snapshot contains both
migration completion markers and no pending legacy admissions. Until then it is
unknown, using the existing retryable response. Known candidates can still be
verified independently. SQLite failures likewise never prove absence. Empty ref
sets bypass discovery.

## One-time migration, not a boot inventory

Startup creates the additive schema, then schedules migration off the serve path
on a separate SQLite connection. It does not hold the serving connection's mutex
while backfilling. One transaction derives refs from distinct `run_index` triples
and records its completion. Invalid legacy identities leave missing session refs
unknown rather than certifying that no local copy exists.

Old imports, move activations and interrupted admissions may have WALs without
`run_index` rows. A one-time metadata census closes those gaps across `wal/`,
`wal-ephemeral/`, `wal-durable/` and sealed `wal-archive*/` members. Represented live
addresses get only stat and fixed head/tail lineage probes. Only uncovered
addresses get full verified WAL reads through `ReadOnlyWalReader`, which has no
writer, manifest, lease, extraction or repair API. Archive enumeration reads indexes/headers,
not represented member payloads. Unreadable or ambiguous locations prevent census
completion, never become absence.

The census queue and per-address coverage commit in batches of at most 128 rows.
After interruption, enumeration adds any new addresses but already-covered
payloads and probes are skipped. Work yields between addresses and pauses three
times each batch's total work duration, including decoding and SQLite commits.
Completed migration is not repeated on normal restarts. Legacy `run_index` writers
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
