# Agent-data reference discovery

Agent-data finds a session by its shared ref through the session-ref index in `run-index.db`,
not a process-wide identity map or a JSON cache. `agent_data_session_ref` maps the older
triple-derived ref (32 lowercase hex characters) to the full bind triple and the session's
address; `agent_data_session_ref_v2` maps the commons llm-runner `session_ref` to the address.
The `agent_data_census*` tables record the census that fills the index from the session store.
The index holds no session content, and the session store remains the authority: a row never
proves that a session is held here.

## Completeness and admission

Session binds also use this inventory to retain an existing pair's original root.
An existing `(harness, session)` pair is positive evidence and resolves in a read
transaction without acquiring SQLite's write lock or waiting for discovery repair.
Two recorded roots still refuse as ambiguous, including during repair. Only an absent pair takes an immediate transaction,
rechecks completeness and the pair's roots, and reserves a candidate if still absent.
Competing first binds share that writer transaction, so they cannot select different
roots. A refused bind or an
interrupted first send can therefore leave an identity-only candidate without a stored session.
Candidates still do not prove that content exists or is locally owned.

With subc-protocol 0.34, wire binds contain only `(harness, session)`. A uniquely
recorded pair keeps its original storage root byte-for-byte, scoped or unscoped,
even if the current stamp names a different workdir. For an absent pair whose
inventory is complete, the first reservation uses the scope's stamped `workdir`,
or `broca-unscoped-v1` when no workdir is stamped. The namespace is a storage key,
not a directory; the session's folders live in the session store under its address. Public v2
refs still omit the root, and old v1 aliases remain accepted. No existing session is
re-keyed by this choice.

Broca declares `bind-no-project/v1`; stray legacy `project_root` and `project_id`
members on inbound binds do not affect addressing or attribution. Only scope
stamps supply project attribution. Outgoing routes never serialize the storage
root or namespace as a bind project: tool/catalog routes select the frozen scope
when present, while unscoped routes (including module-level compaction, hook and
vault routes) have no folder.

An identity reservation alone remains a new session, including after an interrupted
first bind. Subscribe-before-send may create its first lineage, preserving the
subscriber's cursor origin. A session the session store holds is served from its head. One it
does not hold is unknown: reads answer it as unwritten, and only a creating send or an import
starts it. Expired reads keep `expired`; move fences keep `session_moving` and
`session_held_elsewhere`. A send after completed expiry can still start a fresh lineage.

Before a session's first record is written, its writer records the session's row. If the
index can't take it, the writer creates and syncs the owner-only, content-free
`session-ref-projection.incomplete` marker in the state root, then appends normally; only a
failure to make that marker durable prevents the append. Move activation records the row the
same way before it publishes the session's verified files. Only an identity whose derived
address matches the session's address is recorded. Writing the row first permits extra rows
after a failed or interrupted append or activation; a move resolves the session from the
session store's head, never from the row alone.

A missing ref proves absence only when the census is complete (its done marker, and no
uncovered or unreadable address) and no gap marker exists. Until then it is unknown, answered
with the existing retryable response. SQLite failures likewise never prove absence. Empty ref
sets bypass discovery.

## Census and on-demand repair

The census lists the session folders of every storage class's session store and reads each
folder's head for the bind identity it was created under, recording the rows the index lacks.
It runs on its own SQLite connection, off the serve path, so a move looking up refs meanwhile
reads the index as incomplete instead of waiting on the serving connection. The head is read
without its key, as every key-free reader of heads does to name a session; a move that follows
the index opens the head with its key and refuses one whose authentication fails. A head that
can't be read, or an address the pass finds no session at, is quarantined: it prevents census
completion and never becomes absence. A folder without a head holds no session and names none.

Boot schedules the census without blocking serve startup. A bind for an unrecorded
pair that finds incomplete inventory joins that census, or starts one if none is
running. Concurrent binds share one repair and its readiness signal, rather than
polling SQLite. Known-session binds, reads and resumes keep serving. Each new bind
waits at most 60 seconds, then rechecks resolution; a timeout does not cancel the
shared repair. A later bind may start another attempt after a failed repair.

The refusal remains `session_address_unavailable`, with `detail.class: "transient"`
and `detail.reason` identifying `wait_expired`, `migration_failed`, or a remaining
`gap_marker` (a durable warning that some sessions may be missing from the index).
Subc-protocol 0.34.1 classifies this code as retryable on route open;
the bounded route-open retry policy applies while discovery is unavailable.
`health.check.session_address_refusals.repairs` counts started, completed and failed
attempts, and `diagnostics.findings` retains timestamped `session_inventory_repairs`
evidence. These counts describe repair attempts, not sessions or census rows.

All census writes (queueing, coverage, reset and completion)
use at most 64 work items per transaction and check a 10 ms work budget between
items. Head reads occur outside the write lock. Commit/fsync cannot be preempted; the
completion log reports the measured longest lock hold, including commit and excluding
lock-acquisition wait.
Every transaction releases the lock, yields and leaves an unlocked pacing interval
before attempting the next batch. The census connection does not run automatic
checkpoints inside its commits.

The census connection waits up to the same five-second store budget for a
write lock, polling at a fixed one-millisecond interval. SQLite's default busy
handler backs off to much longer sleeps and can repeatedly miss the short unlocked
gaps in a burst of outbox or export commits. Short polling avoids that
backoff-induced starvation without retrying failed batches or extending the wait
budget. Foreground connections keep their existing busy-timeout policy.

Census progress is durable: queued addresses, in-flight addresses and their coverage. An
address interrupted after part of its row was written is replayed rather than counted as
covered. Completion is recorded only from the same writer snapshot that finds nothing
uncovered or unreadable, and is not repeated on normal restarts.

If the gap marker exists when a census starts, the census resets coverage for addresses that
have no row and visits them again. When it completes, it removes the marker and syncs the state
root, but only if no writer of the marker took the marker's per-root lease in between: each
acquisition advances a persisted epoch, and the census removes the marker only when the epoch
advanced by exactly its own reacquisition. A failed or overlapped repair leaves the marker for
the next attempt, not a false absence or a failed run. Do not remove the marker by hand to
silence refusals.

There is no inventory thread, JSON checkpoint, persistent RAM identity map or
inventory-progress health metric.

## Read and send fencing

Ordinary reads check the session head's residence, which records that another computer holds
the session. Write checks also refuse while a release or activation of the session is pending.
At boot a small, durable per-ref fence index (`agent-data/write-fences/`) is rebuilt from the
cut JSON files. A cut save publishes its fences before its intent, and removes them only after
completion is durable. Checks then read only the named ref's fence, not every cut JSON file. A
reader that finds the index uninitialized scans the cut files; cut JSON remains the authority.

Busy probes inspect the requested session for active runs, queued sends and held calls, with the existing actor, lease and move-fence observations. INFO events
report op, cut id, ref count and elapsed milliseconds, including cancelled calls.
`health.check.metrics.agent_data_ops` reports each operation's cumulative count.
