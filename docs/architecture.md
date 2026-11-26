# Architecture

## What broca is

broca is a durable LLM agent-loop runner. A caller sends a prompt to a session;
broca calls the model, dispatches requested tools, feeds their results back and
repeats until a stop condition. Each boundary in that loop is written to the
session's log in the session store before broca acts on it. After a crash or
restart, broca rebuilds the run from those records. A tool call that may already
have had a side effect is not dispatched again.

The `ck-broca` binary runs as a supervised module of the subconscious daemon,
which launches local modules and routes requests between them.
Clients reach broca over daemon management routes. It calls model providers over
HTTPS or WebSockets, tool modules over daemon routes, the claustrum credential
vault and a model catalog service. A compaction provider is optional.

## Components

**Sessions and admission** (`broca-module`, `broca-core`). A session is keyed by
the triple `(project_root, harness, session)`. One actor processes its commands in
order and holds its writer lease. Admission resolves the send's idempotency key,
credentials, cache policy, stop conditions, budget and context limit before
writing any record. It freezes the accepted inputs into `RunStart`; a refused
admission writes nothing.

**Run loop** (`broca-session-loop`). Each model step renders canonical request
bytes from the frozen configuration, streams the response and records `StepEnd`.
A `ToolIntent` is synced before tool dispatch and a `ToolOutcome` is written
afterward. Runs end with a durable `RunEnd`, or stop at a `Pause`.

**Session store** (`broca-session-store`). Each session has a head (its current
version), append-only logs and immutable parts, under a folder named by an address
derived from the triple. An encrypted session has its own data key, protected by
the deployment key that created it. broca loads the deployment keys it has read grants for
and uses its configured key for new sessions. A random lineage id distinguishes a continued
log from a recreated one. Only the
lease holder writes. History behind the compaction view is sealed into segments
and read back on demand. See [the session store](session-store.md).

**Resume** (`broca-session-loop`). Resume rebuilds the next request from the
session's records and the values admission froze, byte-identical to the request
first sent. While the runner is live, an indeterminate call is closed with a
durable `outcome_unknown` result and the turn continues without re-dispatch.
If the run's own scope has ended, the run stops after settling its batch. On
restart, sealing a run with a dispatch intent but no recorded outcome still
names the call in `indeterminate_tool_calls`. See the
[durability contract](durability-contract.md).

**SQLite stores** (`broca-store`). `run-index.db` holds the session-ref index,
scope reservations, bus outboxes and usage-export facts. `system-one.db` holds
decision batches and `grants.db` holds consent grants. `broca-module` owns
`late-results.db` for held calls and late-result intake. None of these stores holds
a session's records; where they derive something from them, the records win.

**Decision batches** (`broca-module-serve`). System One's `classifier/v1` route
serves batches without a session. Routing chooses the model when the caller
omits it, using the minimum decision grade (`iq: 1`) to select the cheapest
eligible graded model. A supplied model is a strict pin. The `llm_classify`
tool requires a pin so it knows the provider before checking consent. See
[System One routing](system-one.md).

State roots without a `state-format` marker keep today's format. The staging-only
`broca-state/2` redesign has been removed: a root whose marker names it refuses
startup, rather than being read as today's format. Start with a fresh state root.

**Providers and catalog** (`broca-provider`, `broca-catalog`). Wire families
include Anthropic Messages, OpenAI Chat and Responses, ChatGPT/Codex, Gemini,
Code Assist and Bedrock Converse. Each has a renderer and stream normalizer;
the transport handles network policy. Model and provider specifications come
from the catalog service with a last-known-good disk cache. They are resolved
at admission and frozen into the run.

**Daemon connection and tool routing** (`broca-subc`). Tool calls route to their
providing module. Tool, vault, catalog and transform route opens share connection
recovery. It retries only refusals the daemon marks as retryable. A refused open
has no side effect; a dispatched call is retried only when the daemon guarantees
it was refused before delivery.

**Credentials** (`broca-subc`, `broca-module-serve`). A provider auth method maps
to a credential ID that the claustrum vault resolves to a token under the
supervised module's grant. The selection is frozen into `RunStart` and carried
into usage-export facts. Developer tools instead present bearer handles, whose
possession authorizes access independently of a supervised-principal grant.
See the [security policy](../SECURITY.md) for both trust boundaries.

**Output**. Subscribers receive a reliable control lane projected from the
session's records, addressed by a cursor: a record sequence (`wal_seq`), the
control unit's index within that record's projection (`sub_index`), and the log's
incarnation, so a recreated log refuses a stale cursor. A separate lossy display
lane carries token deltas and is never replayed. Display gaps are reported in-band.
`session.read` pages the durable transcript without acquiring the actor's writer
lease.

**Usage export** (`broca-core`). Each run writes immutable spend facts: model,
credential, usage and charge basis. Provider measurements that were not reported
stay absent, rather than becoming zero.

**Shutdown**. On an accepting restart drain, broca keeps serving requests and
durably queues new sends without starting their runs. It finishes in-flight model
steps, tool batches and sessionless calls before signaling readiness, up to the
daemon's deadline. Ordinary drains, daemon connection closure and SIGTERM stop
admission; work still in flight after the post-GOODBYE grace is sealed
`Interrupted`, not `Cancelled`. After SIGTERM, broca re-raises the signal so the
supervisor can respawn it. See [module lifecycle](module-lifecycle.md).

## Crate map

| Crate | Role |
|---|---|
| `broca-protocol` | Canonical request, event, usage and error types; no I/O. |
| `broca-wire` | Client-facing JSON contracts for the management surface. |
| `broca-records` | Record types shared across crates, storage classes and their roots, the `state-format` marker and move fences. |
| `broca-session-store` | Heads, logs, objects and setup parts; the record codec; sealed history; moves. |
| `broca-session-loop` | The run loop: render, stream, dispatch tools, resume. |
| `broca-core` | Admission, queued sends and steers (sends that join a running run at its next step), compaction, hooks, budgets, media, billing facts and leases. |
| `broca-module` | Session actors, registry, serve handlers and subscriber fan-out. |
| `broca-module-serve` | `ck-broca`, daemon serve loop, configuration, routing, health and `broca-session`. |
| `broca-subc` | Daemon connection, route-open retry, tool/transform routing and vault client. |
| `broca-fetch-plan` | First-send catalog/system-text fetches and the admission manifest. |
| `broca-provider` | Wire families, rendering, normalization, retry transport and tool aliases. |
| `broca-catalog` | Model/provider resolution and catalog cache. |
| `broca-ck` | CK-Message wire types shared by the Anthropic and Responses codecs. |
| `broca-store` | The SQLite stores. |
| `broca-owner-only` | Owner-only private-state creation and validation. |
| `broca-diagnostics`, `broca-memory-relief`, `broca-heap-profile` | Blocking-job and egress instrumentation, returning freed heap pages after a peak, opt-in allocation profiling. |
| `ck-session-import`, `ck-import` | Readers for other harnesses' logs and the import CLI. |
| `broca-bench`, `broca-replay`, `broca-store-contention-tests`, `broca-test-support` | Benchmarks, replay fixtures, SQLite lock-contention harness and test support. |

## Invariants

- **The session's records are authoritative.** Indexes, read pages and export
  facts are projections, not competing authorities.
- **At-most-once side effects.** Persist tool intent before dispatch. An intent
  without an outcome is indeterminate on resume, not permission to resend.
- **Freeze at admission.** Inputs that shape rendered bytes are frozen into
  `RunStart`. Resume renders from that complete recorded configuration, not a new
  catalog resolution, so changed or withdrawn catalog facts do not refuse an admitted
  run. The current family's independent compatibility table must still reproduce the
  recorded renderer version; unsupported versions or unverifiable bundles refuse
  continuation rather than silently rendering today's bytes as an older version.
- **Canonical storage, aliases at rendering.** Store caller tool IDs and names;
  provider-specific aliases exist only in the rendered request.
- **Absent is not zero.** Unknown measurements remain absent.
- **Export facts are immutable.** New optional fields do not change existing
  bytes or `fact_id`.
- **Interrupted is not Cancelled.** Cancellation is an explicit caller action;
  interruption describes an uncertain or externally stopped run.
- **The session key is the resolved triple.** The session-ref index keeps an
  existing pair's original root; changing any component addresses a different
  session.
- **Classify errors at the source.** The wire classes are `transient`,
  `permanent`, `auth_required` and `context_overflow`; consumers do not infer them.

## Further reading

- [The session store](session-store.md)
- [Module surface](module-surface.md)
- [Wire contract](wire-contract.md)
- [Durability contract](durability-contract.md)
- [Provider-family test harness](family-e2e-harness.md)
- [Error-class contract](error-class-contract.md)
- [On-disk data model](data-model.md)
- [Contributing and verification](../CONTRIBUTING.md)
