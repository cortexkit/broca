# Architecture

## What broca is

broca is a durable LLM agent-loop runner. A caller sends a prompt to a session;
broca calls the model, dispatches requested tools, feeds their results back and
repeats until a stop condition. Each durable boundary is written to the session's
write-ahead log (WAL) before the corresponding action. After a crash or restart,
broca reconstructs the run from that log. A tool call that may already have had a
side effect is not dispatched again.

The `ck-broca` binary runs as a supervised module of the subconscious daemon,
which launches local modules and routes requests between them.
Clients reach broca over daemon management routes. It calls model providers over
HTTPS or WebSockets, tool modules over daemon routes, the claustrum credential
vault and a model catalog service. A prompt-reduction transform is optional.

## Components

**Sessions and admission** (`broca-module`). A session is keyed by the triple
`(project_root, harness, session)`. One actor processes its commands in order and
holds its writer lease. Admission resolves the send's idempotency key,
credentials, cache policy, stop conditions, budget and context limit before
writing any record. It freezes the accepted inputs into `RunStarted`; a refused
admission writes nothing.

**Run loop** (`broca-core`). Each model step renders canonical request bytes from
the frozen configuration, streams the response and commits `ModelStepFinished`.
`ToolDispatchIntent` is fsynced before tool dispatch and `ToolResult` is written
afterward. Runs end with a durable `RunFinished` or `RunPaused` record.

**WAL** (`broca-wal`). Each session has an append-only `wal/<session_addr>.wal`
file. The address is derived from `(project_root, harness, session)`. A random
`lineage_id` distinguishes a continued log from a recreated one. Only the lease
holder writes; readers never truncate.

**Replay and resume** (`broca-core`). Replay reads the WAL as a sequence of
episodes and rebuilds the conversation prefix. An intent with no result is
indeterminate: the resumed run stops as `Interrupted`, names the calls in
`indeterminate_tool_calls` and never dispatches them again. See the
[durability contract](durability-contract.md).

**Store** (`broca-store`). The run index, materialized
transcript prefix, admission checkpoints and usage-export facts are projections
of the WAL, updated asynchronously and bound to its `lineage_id`. The WAL wins when
the store disagrees, and the projection is rebuilt.

**Providers and catalog** (`broca-provider`, `broca-catalog`). Wire families
include Anthropic Messages, OpenAI Chat and Responses, ChatGPT/Codex, Gemini,
Code Assist and Bedrock Converse. Each has a renderer and stream normalizer;
the transport handles network policy. Model and provider specifications come
from the catalog service with a last-known-good disk cache. They are resolved
at admission and frozen into the episode.

**Daemon connection and tool routing** (`broca-subc`). Tool calls route to their
providing module. Tool, vault, catalog and transform route opens share connection
recovery. It retries only refusals the daemon marks as retryable. A refused open
has no side effect; a dispatched call is retried only when the daemon guarantees
it was refused before delivery.

**Credentials** (`broca-subc`, `broca-module-serve`). A provider auth method maps
to a credential ID that the claustrum vault resolves to a token under the
supervised module's grant. The selection is frozen into `RunStarted` and carried
into usage-export facts. Developer tools instead present bearer handles, whose
possession authorizes access independently of a supervised-principal grant.
See the [security policy](../SECURITY.md) for both trust boundaries.

**Output**. Subscribers receive a reliable control lane projected from the WAL,
addressed by a `(wal_seq, sub_index)` cursor: the WAL record sequence and the
control unit's index within that record's projection. A separate lossy display
lane carries token deltas and is never replayed. Display gaps are reported in-band. `session.read`
pages the durable transcript without acquiring the actor's writer lease.

**Usage export** (`broca-core`). Each run writes immutable spend facts: model,
credential, usage and charge basis. Provider measurements that were not reported
stay absent, rather than becoming zero.

**Archive** (`broca-core`, `broca-wal`). WALs eligible for archival can be packed
into sealed, digest-verified containers under `wal-archive/`, by an operator or
an opt-in scheduler. Reads resolve live WALs first. Appending to an archived session
restores its WAL before opening it for writes.

**Shutdown**. Daemon connection closure or SIGTERM stops admission. In-flight
runs have a short grace period to finish; remaining runs are sealed
`Interrupted`, not `Cancelled`. After SIGTERM, broca re-raises the signal so the
supervisor can respawn it.

## Crate map

| Crate | Role |
|---|---|
| `broca-protocol` | Canonical request, event, usage and error types; no I/O. |
| `broca-wire` | Client-facing JSON contracts for the management surface. |
| `broca-wal` | WAL records, framing, file storage, archive containers and `wal-verify`. |
| `broca-core` | Run loop, replay, admission checkpoints, import, billing, WAL folding and leases. |
| `broca-module` | Session actors, registry, serve handlers and subscriber fan-out. |
| `broca-module-serve` | `ck-broca`, daemon serve loop, configuration, routing, health and `broca-session`. |
| `broca-subc` | Daemon connection, route-open retry, tool/transform routing and vault client. |
| `broca-provider` | Wire families, rendering, normalization, retry transport and tool aliases. |
| `broca-catalog` | Model/provider resolution and catalog cache. |
| `broca-fetch-plan` | First-send catalog/system-text fetches and the admission manifest. |
| `broca-ck` | CK-Message wire types for prompt-reduction transforms. |
| `broca-owner-only` | Owner-only private-state creation and validation. |
| `broca-diagnostics` | Blocking-job and egress instrumentation. |
| `cortexkit-anthropic-policy` | Shared Anthropic auth, header and quota policy. |
| `broca-store` | Materialized store, export index and store maintenance. |
| `ck-session-import`, `ck-import` | Readers for other harnesses' logs and the import CLI. |
| `broca-run`, `broca-bench`, `broca-replay`, `broca-test-support` | Standalone runner, benchmarks, replay fixtures and test support. |
| `broca-crash-harness` | Crash-cut testing and recovery invariant checks. |

## Invariants

- **The WAL is authoritative.** Stores, indexes and export facts are rebuildable
  projections, not competing authorities.
- **At-most-once side effects.** Persist tool intent before dispatch. An intent
  without a result is indeterminate on resume, not permission to resend.
- **Freeze when accepting a run.** Inputs that shape rendered bytes are frozen into
  `RunStarted`. Resume uses the frozen values; incompatible renderer or catalog
  drift is refused rather than silently changing an admitted request.
- **Canonical storage, aliases at rendering.** Store caller tool IDs and names;
  provider-specific aliases exist only in the rendered request.
- **Absent is not zero.** Unknown measurements remain absent.
- **Export facts are immutable.** New optional fields do not change existing
  bytes or `fact_id`.
- **Interrupted is not Cancelled.** Cancellation is an explicit caller action;
  interruption describes an uncertain or externally stopped run.
- **Session identity is the triple.** Changing any bind component addresses a
  different WAL.
- **Classify errors at the source.** The wire classes are `transient`,
  `permanent`, `auth_required` and `context_overflow`; consumers do not infer them.

## Further reading

- [Module surface](module-surface.md)
- [Wire contract](wire-contract.md)
- [Durability contract](durability-contract.md)
- [Provider-family test harness](family-e2e-harness.md)
- [Error-class contract](error-class-contract.md)
- [On-disk data model](data-model.md)
- [Contributing and verification](../CONTRIBUTING.md)
