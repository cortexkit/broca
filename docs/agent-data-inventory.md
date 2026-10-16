# Agent-data reference discovery

The move participant's process-local inventory maps a shared session ref to its
logical WAL address and bind identity. It is derived from WAL records, never an
authorization or busy-state cache. Bound cuts continue to use their durable
recorded addresses, not discovery.

`AgentDataService::reconcile_boot` starts one blocking-pool background build before
serving. Requests never start or await that build. Until it finishes, unresolved
refs are unknown, not absent. Unreadable files, ambiguous addresses, failed
inventory enumeration and uncertain identity writes keep absence unknown for the
process lifetime; restart rebuilds from durable storage. Empty ref sets bypass
inventory altogether. Dropping the service cancels enumeration and the build
between addresses. A route GOODBYE, replacement, or connection EOF cancels its
in-flight agent-data handlers; the independent boot build has no caller to wait
for and continues for other routes.

The build inventories portable WALs, retained WALs and sealed archive members,
using the same live-first, size-limited, verified reads as the original discovery.
It reads the entire committed record stream: a later RunStarted can introduce a
different triple after arbitrary run content, so stopping at the first identity
would be unsound. Only triples whose derived address equals the physical address
are indexed. Historical triples in re-keyed WALs do not become aliases.

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
