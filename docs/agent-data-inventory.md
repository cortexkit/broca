# Agent-data reference discovery

The move participant's process-local inventory maps a shared session ref to its
logical WAL address and bind identity. It is derived from WAL records, never an
authorization or busy-state cache. Bound cuts continue to use their durable
recorded addresses, not discovery.

`AgentDataService::reconcile_boot` starts one dedicated background thread before
serving. Requests never start or await that build. Until it finishes, unresolved
refs are unknown, not absent. Unreadable files, ambiguous addresses, failed
inventory enumeration and uncertain identity writes keep absence unknown for the
process lifetime; restart rebuilds from durable storage. Empty ref sets bypass
inventory altogether. Dropping the service cancels enumeration and the build
between addresses. A route GOODBYE, replacement, or connection EOF cancels its
in-flight agent-data handlers; the independent boot build has no caller to wait
for and continues for other routes.

The build inventories portable WALs, retained WALs and sealed archive members.
It checks every current location each boot, never using the cache's address list
as a census. Only triples whose derived address equals the physical address are
indexed. Historical triples in re-keyed WALs do not become aliases.

## Persistent restart hints

`move-inventory.json` sits beside `wal/`, with owner-only permissions. It is a
versioned, checksummed derived cache, written to a unique private temporary file,
synced and atomically renamed. A missing, corrupt, insecure or unknown-version
cache produces one WARN and a full rebuild, never a move-service failure. Cache
write failure only makes the next startup slower. A changed WAL-reader feature
set also rebuilds the cache, so a downgraded binary cannot reuse a newer reader's
successful scan of frames it would itself refuse. The cache checksum detects
accidental damage; it is not an authorization signature.

Per address it stores the eligible triples, logical source, lineage, last record
sequence and verified committed end. Each live WAL is statted and its fixed-size
lineage metadata is verified at the head and tail (at most 138 bytes). Reuse
requires matching lineage, committed length, file identity and modification time.
The tail probe also recognizes legacy WALs that gained lineage after their first
records. WALs with no verifiable lineage are conservatively fully reread. A changed
lineage, shrink or in-place same-length modification requires a full read.

A matching lineage that grew is read from the cached committed end using the
strict byte-anchor reader: it must prove the next sequence, otherwise a full read
replaces the hint. Torn tails do not advance the saved end. Platforms without
stable file anchors reuse unchanged files but fully read changed ones. Full and
suffix reads retain the existing size caps, sequence checks and frame digests.
They inspect every new record: stopping at the first RunStarted would miss a later
eligible triple after arbitrary historical content.

Sealed archive v2 indexes prove member coverage without reading member payloads;
older containers use header-only walks. Each member hint includes container file
identity, length and modification time, member offset and payload length (and the
header digest for older indexes). Unchanged members reuse triples without payload
reads or payload SHA-256 verification. Changed members use the ordinary verified
archive reader. Requested operations still verify their actual bytes regardless
of whether discovery reused a hint.

The thread handles one address at a time. This is **cooperative pacing, not OS
priority**: after payload work it pauses three times that work's elapsed duration,
targeting roughly 25% of one worker's CPU duty cycle. A production cold rebuild
that took 10.5 minutes unthrottled is therefore expected to take about 42 minutes,
leaving roughly 75% of that core free; I/O-bound work may use less CPU. Warm-cache
metadata checks only yield and do not incur those pauses. Cancellation checks run
between addresses and at least every 50 ms during a pacing pause.

## Incremental maintenance in the running process

Every WAL opened through FileWalStore is wrapped by the inventory publisher.
Successful synchronous appends and explicit sync barriers publish all pending
RunStarted triples before returning, so admission's first durable RunStarted is
visible even when the writer predates the boot scan. Updates accumulated during
the scan are merged at publication, never overwritten. Failed identity durability
marks the inventory incomplete rather than proving absence.

Move activation bypasses the normal writer. Its installation callback publishes
all eligible triples from the verified incoming WAL after durable installation
and before session exclusion is released. A partially failed installation marks
absence unknown. Import staging alone does not claim a local copy.

Archive folding and extraction preserve both the logical address and payload;
the map deliberately holds no live filename, archive filename or byte offset to
invalidate. Requested reads still resolve the current live/archive location and
verify it. A deleted session can leave a conservative candidate in the map, but
its bytes must still be present before it can be considered held. Retention's
durable ledger remains the authority for non-exportable expired sessions.

Each request looks up only its named refs and reads only their WALs. Busy probes
still inspect the complete requested WAL for active runs, queued sends and
unsettled approvals, and take the same actor/lease/move-fence observations. They
no longer compute unused export digests, format metadata or archive snapshots.
INFO events report op, cut id, ref count and elapsed milliseconds once, including
cancelled calls. `health.check.metrics.agent_data_ops` reports each op's cumulative
count since this service started, including zero.
