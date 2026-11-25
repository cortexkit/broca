# broca as a subc module

This document describes the module's session and run contracts, durable lineage
model, and coordinator invariants.

## Session inspection CLI

`broca-session --subc CONNECTION --project-root ROOT --harness HARNESS --session NAME
admission --run-id RUN` prints the daemon's `run.admission` JSON reply without reading
encrypted session files in the client. It uses the same session authorization as
`run.status`. This is a read-only management query; it does not admit a turn.

### Frozen run admission

`run.admission {"run_id":"…"}` reads the requested run's own start, not the latest baseline
of its session. A known run returns:

```json
{"run_id":"…","session_class":"sidekick","started_at_ms":123,"storage":"store","fetches":[{"provider":"aft","declarations_frozen":{"late_results":true}}],"scope":{"owner":{"kind":"reserved","module_id":"core"},"ref":"worker-1","epoch":1,"principal":{"kind":"reserved","module_id":"core"}}}
```

`storage` is always `store` (the session's log format is no longer produced).
`session_class` and each fetch's `declarations_frozen` are omitted when absent in the recorded
admission; absence is not replaced by an empty value. `fetches` contains only provider names and
 their frozen declarations, not tools, messages, system text or credentials. A run admitted
without a fetch plan has an empty `fetches` array. An unknown run returns `{"state":"unknown"}`,
as `run.status` does. The required `run_id` is scoped to the bound session; unknown parameter
keys are refused.
An older start without a recorded timestamp is unreadable through this query, not assigned a
fabricated start time.

`scope` is omitted for an unscoped start, preserving its existing reply bytes. For a
scoped start, `scope.owner`, `scope.ref` and `scope.epoch` project the recorded scope's
owner principal, reference and `scope_epoch`; `scope.principal` is the recorded driving
principal, omitted when the start did not record one. These fields come from the
requested run's own start, never from the caller's current bind or
the session's latest ownership. They expose no tool arguments, transcript or credentials;
adding scope metadata does not change which callers may read `run.admission`. A worker
class or a fetch from the `aft` tool provider alone does not establish a scope.
The query uses the run-id index to find the run's `RunStart` and resolves the setup parts it
froze, without a transcript walk.

`broca-session --subc CONNECTION read <id>` (also `--session <id> read`) looks the session up
by name. It tries the supplied name first, then `alfonso:<id>` for an unprefixed name. The
lookup scans only the compact session-ref inventory, with the name predicate in SQL. Lookup
duration is logged at debug level. Missing names refuse with the searched spellings; an
incomplete inventory is identified rather than claiming the session is unknown, and a single
candidate is not treated as unique until the inventory is complete. Ambiguous names list up to
100 candidates' project-root, harness, and session values, say when more were omitted, and
refuse to choose one.

`--state-root DIR` overrides the normal state-root resolver for the lookup. Explicit
`--project-root ROOT --harness HARNESS` bypass lookup and keep the session name literal; use the
full recorded session name (including `alfonso:` when present) when selecting an ambiguous
candidate. Existing read calls with only one bind flag use lookup and require that flag to
match the recorded identity. Other daemon commands still accept their original bind flags. The
connection file is only an argument; its path and contents are not printed on connection
failures.

`broca-session --subc CONNECTION compose-plan --items FILE [--scope OWNER/REF/EPOCH]`
composes a tool-only plan from live provider catalogs without creating a session or sending
a turn. With `--scope`, every provider's catalog is fetched on a scoped `tool-provider: v1`
route as a Direct carrier. The scope owner must authorize `direct` as a carrier for those
providers. The command reads the private 32-byte
bind proof as exactly 64 hex characters on stdin, optionally followed by a newline, and
requires EOF. A missing or malformed proof is refused before connecting to the daemon;
the proof never belongs in argv, an environment variable, the items file, or diagnostics.
Without `--scope`, catalog fetches and plan output are unchanged and stdin is not read.
The flag belongs only to `compose-plan`: it does not scope `send`, `run`, `read`, or any
other `broca-session` route. Creating a scoped session remains the scope owner's job;
a carrier may compose its plan but cannot create the session.
The daemon must advertise `scopes/v1` and `scope-bind-proof/v1`; unsupported daemons
are refused rather than silently falling back to unscoped routing. Scope selectors
are sent only in `route.open`, never in catalog request bodies. Tool presets come
from the plan's tool items, so there is no `--preset` flag.

### Carry an owner's scope

For `compose-plan --scope <owner>/<ref>/<epoch>`, the owner is a reserved module id,
not `direct` ownership: a Direct process cannot register or own the scope. The
explicit epoch prevents a reused scope ref from silently selecting another session.

After the owner has synced and received acceptance for its record, it hands the
child a **32-byte random bind proof privately**, encoded as exactly 64 hex
characters on a private piped stdin, optionally followed by a newline, then closes
the pipe. Never put the proof in argv, an environment variable, a log, or a shell
command printed by an orchestrator. Missing or malformed input is refused as
`scope_bind_proof_required` or `scope_bind_proof_invalid` before any daemon connection.

```text
broca-session --subc <connection-file> compose-plan --items <items-file> \
  --scope rig-owner/run-42/1
```

The command deliberately contains no proof argument. The selector sent to subc
uses base64url without padding over the decoded 32 bytes. The proof hash is
SHA-256 over those raw bytes, **not** the 64-character hex representation.

For example, the reserved module `rig-owner` syncs this scope record, replacing
the folder with its canonical absolute rig folder and the hash with its proof's
lowercase 64-character SHA-256 digest:

```json
{
  "ref": "run-42",
  "scope_epoch": 1,
  "kind": "head",
  "workdir": "/absolute/rig-folder",
  "bind_proof_sha256": "<sha256-of-the-raw-32-byte-proof>",
  "carriers": [
    {
      "principal": {"kind": "direct"},
      "targets": ["aft"]
    }
  ],
  "attributes": {}
}
```

The owner is established by its supervised module connection, not by adding an
owner field to the record. A wrong proof is refused by subc as
`scope_bind_proof_mismatch`. Every provider catalog route opened for the plan
carries the selector and proof; the daemon stamps the record's workdir on that
route. A local `--project-root` does not override it. Keep `targets` narrow: the
owner must explicitly authorize every tool provider the plan needs. There is no
fallback to a folder claimed by the CLI.

The process-level **Fusiform catalog and vault routes stay unscoped and need no
carrier entry**. The proof lives only in the tool plane, never in the composed
plan, session identity, model input, or the session's records. Composing a plan does not create a
session or authorize a subsequent session route to carry the scope.

---

## 1. What it is

broca is an **orchestration tier**: a subc module that runs agentic LLM loops for
consumer modules while consuming tools, credentials, and model-routing services.
`broca-session` controls and inspects that supervised module rather than running
an agentic loop in-process.

### 1a. Runtime precondition: a model catalog must be reachable ONCE

A FRESH broca CANNOT SERVE ANY MODEL UNTIL IT HAS A CATALOG. This is a precondition on the
environment, not on the caller, and it does not appear in any wire contract — so a harness that
builds broca from source in a clean environment meets it as an admission failure reading
`model catalog unavailable: no in-memory catalog`, with nothing naming the cause.

The catalog is served by `fusiform` and cached on disk at
`$BROCA_STATE_ROOT/catalog/fusiform-lkg.json`. The LKG survives restarts and the
poller refreshes it every 30 minutes. A new state root needs a catalog before it
can serve models.

Either of these satisfies it:

- `fusiform` reachable over subc at least once, so the poller can write the LKG; or
- a seeded `fusiform-lkg.json` in the state root, which is what broca's own e2e rig does.

A build-from-source deployment must provide a reachable catalog or seed the LKG
file before serving models.

The poller always requests `catalog.get` with `paged: true`, the catalog fact
prefixes, and `include_retired: true`. It echoes opaque `next_page_token` strings
as `page_token`, omitting the token on page one. A response without a continuation
token is complete, including an older fusiform's unpaged response. Pages are
assembled before the existing catalog parser, monotonicity checks, and atomic
LKG write run; no partial catalog is installed.

All pages must agree on `catalog_version` and `resolved_at_ms`. A mismatch or a
response with code `catalog_changed` discards the assembly and restarts from page
one, at most twice. Every other refusal, including `refused`, remains terminal on
both first-page and continuation requests. Message wording is diagnostic only;
no message text controls restarts. A poll is bounded to 256
page requests and 64 MiB of serialized response JSON, including tool envelopes
and abandoned attempts. Exhaustion is a named refusal; malformed pages, duplicate
model identities or qualification rows, and repeated continuation tokens are
protocol errors. A failed poll retains the previous memory snapshot and LKG and
counts once toward catalog health, just as an unpaged failure does.

Routing is resolved from each model row's `wire.npm`, `wire.api` and optional
`wire.shape`; provenance (`from`) is an open string. An unstated endpoint or
unsupported package/shape refuses a new send with the model and missing fact
named. Native defaults are now published as curated facts, not local endpoint pins.
Code Assist and Meta subscription credentials permanently select their own required
client protocol and endpoint. Go rows that Pi does not route have no published
`wire.api`; they refuse because that fact is missing, with the model and missing fact
named, without a local model list or guessed provider-default URL. Unknown members such as `wire_corrections.notes`
remain diagnostic data and do not affect decoding or routing.

Code Assist reasoning tiers use `route.reasoning_wire_ids` and
`route.reasoning_default`. A historical tier-suffixed id is recognized only by
reverse lookup in the published tier map. A withdrawn model's `successor` is
replacement advice, never a redirect. `wire_corrections` is logged for diagnosis
and never selects a route. A resume restores its entire recorded render config
before any catalog lookup, so changed or withdrawn routing facts cannot change
the endpoint, wire family, model id or request bytes of an admitted run.

The fusiform catalog service's reference pagination request/response fixtures
have not yet been published. Until those fixtures confirm interoperability,
Broca makes these compatibility assumptions:
pages retain the existing flat `models` map keyed by `provider/model`, with whole
providers grouped into pages in provider-id order; `source` is unchanged;
`withheld` and `uncertain` are per-page arrays concatenated in page order, with
duplicate rows refused; every page carries the version and page one's resolved
instant; the final continuation token is absent, not null. Error codes arrive as
a routed error or a direct/tool-result JSON error document. A stale continuation
uses the distinct `catalog_changed` code; `refused` and other codes do not signal
a snapshot restart.

### 1b. Provider credentials

Real provider calls read credentials only from the claustrum vault, by credential id
and the supervised module's read grant. A missing vault configuration or refused
credential read never falls back to another application's credential store. Mock
providers selected with `BROCA_PROVIDER_BASE_URL` need no credential route.

Credential keys resolve from the daemon's cached `credential.list_scoped` rows, never from
`auth-methods.json` or `vault.credential_ids`. Those obsolete settings are ignored with one
boot warning each and can be deleted. Active exact ids win; otherwise one row must name both
the method and the catalog provider in `provider_ids` (not the vendor `provider` field).
Retired rows and rows without `provider_ids` are not eligible. Without an explicit method,
the default order is `chatgpt`, `oauth`, `antigravity`, then `apikey` among eligible methods.
Frozen vault ids are reused on resume; older selections without a resolved id look up their
frozen key by the same row rule, without changing their method.

New sends refuse with code `transient` and
`detail: {class: "transient", cause: "credentials_unavailable"}` while no listing has loaded:
the listing loads by itself, so the same send may be admitted on retry, and `transient` is the
runner contract's retryable code. An ambiguous fixed key refuses `credential_ambiguous` with
`detail: {class: "permanent", credential: <broca key>, candidate_ids: [<vault ids>]}`;
it never chooses a row silently. Existing missing-credential refusals are unchanged.

Foreground developer probes and live tests use a separately provisioned capability
handle, loaded through `vault_config::load_devtools_handle` and read through subc.
Live tests require `BROCA_LIVE_TESTS=1` before loading
any handle or reading a credential, and use the same dev-tools handle map
(`BROCA_DEVTOOLS_HANDLES` or `~/.config/cortexkit/broca/dev-tools-handles.json`).
`BROCA_LIVE_SUBC` or `SUBC_SOCKET` can select their subc connection file.

### 1c. Routing provider discovery

`account_selection.routing_module` in `broca.jsonc` is an optional explicit module id. When
set, broca opens exactly that id, without catalog discovery or an id fallback. When unset,
all routing calls use the daemon catalog's sole provider of the `routing/v1` capability.
Only entries with `nonce_verified: true` count: the daemon sets that field after verifying
the supervised module's launch nonce, so a self-registered process cannot claim routing.

Broca caches the discovered module id separately from its management route. An open refused
with `unknown_module`, `target_unavailable` or `module_removed` clears the id and re-reads
the catalog once; another disappearance refusal ends that open. A warming provider retains
bounded backoff without another catalog read. A stale route handle alone reopens the cached
provider, and requests are retried only when the stale handle proves non-delivery.
No verified provider gives the existing transient `routing_unavailable` admission refusal.
Multiple verified providers also refuse without opening either, with the distinct logged
diagnostic `routing_provider_ambiguous` and the same transient `routing_unavailable` refusal.

## 2. Two consumer roles

- **Drivers** start or steer work and inject prompts.
- **Observers** read session content and watch it live.

A consumer can be both. The surface is **session-centric**: consumers address
*sessions*; a *run* is an episode.

## 3. Foundational model: a session is a LINEAGE OF RUNS (turns)

- **Session / lineage** — the durable conversation. One single-writer **lease** and one session
  in the session store, keyed `{project_root, harness, session}`.
  The storage root is resolved before acknowledging a session bind: an existing
  `(harness, session)` pair keeps its unique inventory root; a new scoped pair uses
  stamped `workdir`. A new pair without stamped workdir uses the fixed storage namespace
  `broca-unscoped-v1`, not a directory. The session's folders live in the state root's
  session store, at the inventory-assigned address. A stamp's `project_id` never
  keys a session. Two inventory roots refuse `session_address_ambiguous` (permanent);
   Known pairs resolve immediately from positive inventory evidence even during repair.
   An absent pair waits on shared discovery readiness for at most 60 seconds; incomplete
   discovery then refuses `session_address_unavailable` (transient), never a guess.
   Its `detail.reason` distinguishes `wait_expired`, `migration_failed` and a remaining
   `gap_marker`. Boot and on-demand repairs are single-flight and do not block serve
   startup. Existing-pair lookups use a read transaction; only absent pairs take the
   write lock, recheck discovery, and reserve a root before any actor or session opens.
   A stamped bind that reserves a new pair also records its stamp's owner and scope `ref`
   (not its epoch), in the same transaction. Until that session's first episode,
   `session.send` and `session.import` are admitted only over a route stamped with a scope of
   that owner and `ref`, at any epoch; any other send or import, and a later stamped bind
   under another owner or `ref`, is refused `scope_owner_mismatch` (`detail.kind`
   `first_principal`, `first` naming the reserving owner) before anything is written. Pairs
   reserved without a stamp, or before this record existed, keep the earlier rules.
  Wire binds carry only `harness` and `session` on subc-protocol 0.34. Broca declares
  `bind-no-project/v1` and ignores stray legacy bind project members. Project attribution
  comes only from the daemon's scope stamp (`workdir`, optional `project_id`), never
  the inbound connection. Scoped outgoing tool/catalog routes select the frozen scope;
  unscoped routes carry no folder. Compaction, hook and vault module-level routes are
  unscoped. Neither storage roots nor the namespace are sent as wire bind projects.
  Tool routes to the plan's compaction or step-transform providers bind the session's own
  harness and session; other tool modules bind `runner` with the unchanged session string.
  AFT (`module_id: aft`) additionally receives the real harness `broca` only when its daemon
  catalog manifest declares `scope-background-tasks/v1`, so background-command completion
  notices can address that session. The existing catalog snapshot supplies this decision;
  no entry, no readable snapshot, or no capability means `runner`, with no discovery call per
  route. If a stale snapshot permits `broca` but the target refuses that bind, Broca tries
  `runner` once. A successful fallback is remembered until the snapshot refreshes; if it
  fails too, the first refusal is returned unchanged. Transport/decoding failures and the
  daemon's own routing, scope and permission refusals do not trigger this fallback; temporary
  refusals retain their existing retry policy. Snapshots refresh after a route is invalidated
  because it is no longer live, or once when batch routing cannot resolve a provider; there
  is no timer. Each plan fetcher uses a separate catalog snapshot, shared by that admission's
  provider lookups. Existing routes keep their bind.
  Other harnesses still become `runner`; this exception changes neither the attested
  principal, the frozen scope, nor the session string.
  Keeping one `(harness, session)` bind pair per session at a provider relies on the session
  string being unique per consumer and provider, which every caller today satisfies
  (Core names each session with its own task id).
- **Run = one turn's loop execution** — a user message in → the agent loops until its
  response → a terminal. Each run begins with a `RunStart` record (input, frozen config,
  `origin` and `submission_id`). Runs **accumulate** in the session's records:
  `RunStart₁ …turn-1… RunEnd₁ RunStart₂ …turn-2… …`. **One ACTIVE run per
  lineage at a time** (lease-enforced); idle between runs. **Chosen: one run per turn.**
- **Runs in sequence:** only the **last** run is a resume target; all earlier runs are
  immutable conversation history. A `RunEnd` for run N ends run N, not the session. An idle
  `session.send` after a finished run starts a fresh run with its own `RunStart`, distinct from
  resuming the last unfinished one.
- **Conversation history** = the prefix across all closed episodes ⊕ the current episode.
- **Submission** = one prompt injection. PENDING (durably queued, retractable) → consumed
  into a run (the actor writes `RunStart{submission_id}`) → ACTIVE → TERMINAL.
- **Session coordinator** (§12) — the in-process lineage owner that drains the durable
  queue and starts each run.
- **Fork** (future) — a separate lineage `SeededFrom` a parent; v4 lands the durable
  `origin` field (§18); the cross-lineage prefix read is deferred.

## 4. The wire surface

Unary reply frames larger than **4,194,304 bytes including the fixed 21-byte frame header** are
never sent: broca answers that correlated request with an error and keeps the connection open.
The 21-byte header is `subc-protocol` 0.32.4's `HEADER_LEN` (`src/lib.rs:417`), encoded by
`EnvelopeHeader::encode` (`src/lib.rs:714-725`). `session.read` and `run.result` use the typed
`reply_too_large` refusal when an indivisible message or group would cross that limit; other
unary replies use the transport adapter's generic oversize refusal. The codec's larger
`MAX_FRAME_BODY_LEN` is only an allocation ceiling, not the daemon's response-frame limit.

A TCP reset on either half instead ends the connection with the classified
`reset` serve cause, not a generic serve error. Sibling calls cannot survive an
RST: [RFC 9293 §3.10.7.4](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.10.7.4)
requires outstanding SEND/RECEIVE operations to receive reset responses and moves
TCP to CLOSED. An unrelated write error remains a serve error.

broca has no spend-cap operations. `cap.install` and `spend.delta` were removed: a call to
either gets the ordinary `unknown_method` refusal and writes nothing. Budget enforcement
across sessions is not built (see SECURITY.md).

```
session.read { from_ordinal?, limit?, include_tools?, after_mid?, history_version?, max_bytes?,
                 include_originals?, include_rich_ui? }
    → { messages: [{ ordinal, mid, message, written_at?, rich_ui?,
                     tool_calls?: [{ tool_call_id, call_key?, dispatched_to?, indeterminate }] }],
        next_from_ordinal?, head?, incarnation, history_version, lineage_state,
        tools?: [{ name, description, input_schema, module? }], tools_run_id? }
                                                                      (OBSERVER; §15)

session.history { cursor?, from_mid?, before_mid?, from_ordinal?, before_ordinal?,
                  limit?, max_bytes?, media?, include_originals?, view?, form?, limits? }
    → { messages: [{ ordinal, mid, written_at, message | row, part_keys?, ... }],
        incarnation, history_version, older?, newer? }
session.output_refs { set_id, after? } → { records, next? }
session.output_ref_read { ref_id, offset, length } → { format, size, data }

session.head {} → { incarnation, history_version, first_ordinal?, last_ordinal?, head?, last_run_state?, updated_at?,
                       cache?: { last_sent_at_ms, ttl_ms?, expires_at_ms?, expiry_basis },
                       title?, model?: { provider, model, variant? }, usage?,
                       sender?: { modes, interrupt, owner?: true }, last_run_rich_ui? }
session.warm { lineage_id? }
    → { request_body_sha256, warm_body_sha256, cache_read_tokens?, cache_write_tokens?,
        input_tokens?, ttl_ms, expires_at_ms, expiry_basis: "estimated" } (§4a)
run.result { run_id } → { run_id, state, usage, reason?, error?, final_message?: { ordinal, mid, text } }
run.tool_calls { run_id, after?, limit? }
    → { run_id, calls: [{ tool_call_id, call_key?, name, args, outcome,
                          result_bytes?, dispatched_to?, ordinal }], next? }
session.baseline {} → { state: "not_yet" | "ready", baseline? } (owner only)
role.describe {} → the llm-runner/v1 major, its ops and capability groups (including `run_usage`), max_bytes

session.subscribe { from?: "start" | "live" | cursor } (dedicated route; §9)
    → stream of events (control reliable+replayable, display live+lossy); see §7,§8,§16

    session.send { session, prompt, system?, send_id?, session_class?, retention?, cache?, model?, tool_providers?, stop_when?, seed_from?, context_management?, delivery?, conversation_key?, title? }
    title?: 1-200 Unicode scalar values, not blank, no control characters; else invalid_params {field: "title"}
    account_route?: {provider: nonempty string, model: nonempty string}, no other members;
        accepted for the listed routed_accounts methods only. These are the routing select
        answer's names for account admission, not the model-call names. Frozen per run:
        joining omission inherits, an explicit different route refuses invalid_params
        {field: "account_route"} before any write. An independently queued run freezes its
        own route. Malformed or unsupported account_route also refuses before any write.
    → { state: "active" | "pending" | "finished", run_id?, submission_id?, reason?, baseline? } (DRIVER; §11)

session.retract { session, submission_id }
    → { retracted: true } | { retracted: false, reason: "already_started" } (§11)

run.cancel { run_id, at?: "now" | "step_boundary" } → "ack" | "not_active" (§13)
run.status { run_id | session }
    auth_selection?: credential id exactly as frozen at admission (also on run.result); omitted, never empty, when none was frozen or its config is not cheaply readable; never re-derived from current routing.
send.status { send_id }
    → { state, run_id?, step?, last_error?, head: cursor, title?, usage } (state enum §14)
```

`session.history` follows llm-runner §4.6 and §4.7 (`host_history`, `history_display`):
absent `form` serves raw entries; `form: "display"` serves canonical rows with optional
UTF-8 tool-payload `limits`. Newest pages and `before_*` seeks page backward; `from_*`
seeks page forward. Entries are always oldest first, and opaque `older`/`newer` cursors
are bound to the session's incarnation and history version. Ordinals start at one;
normal appends keep the version, while destructive recovery advances it before serving history.
If the legacy `session.read` timestamp derivation has no creation time, history refuses the whole page with
`history_unrepresentable`, class `permanent`, and `detail: {ordinal, field}` naming
the first unrepresentable row and `written_at`. It never substitutes an
invented value or returns a partial page. Raw `session.read` remains the recovery path.
Mode-3 logs materialize creation times using the preceding entry's time, or zero for an
untimed prefix; later appends cannot change a time already served.
Under llm-runner 0.6.13 §4.7, assistant rows carry their recorded producing model.
Imported or adopted messages without one omit `model` (unknown); the projection
never guesses or copies the session's current model. Messages written by a run keep theirs.
Display rows alone expose the output-reference descriptors from §9.13: the reply
set on `row.output_refs`, and each opted-in call's set on its tool-call part.
Default `session.read`, `run.status`, stored messages and subscribe bytes are unchanged.

Read-entry `written_at` is an optional Unix-millisecond time in the **entry envelope**, never
inside the opaque `message`. It is part of default reads, with no request opt-in. New input
and steers record admission time; a completed assistant records the end of its successful model
attempt, before post-assistant hooks. Existing records are not rewritten: reads expose only
recorded admission or
successful-attempt times, and tool results' recorded finish times. Unknown times stay absent,
never zero.

`session.head.model` is the latest recorded next-send selection. Broca declares the
`head_selection` capability group in `role.describe`; it lets an admitted creating plan or
durably accepted refresh select
`plan.model: {provider, model, variant?, service_tier?}`. A changed choice appears on head
immediately, regardless of when the refresh applies its text or tools. An absent model member
leaves the selection alone; a present model's omitted tier means standard. Top-level
`session.head.service_tier` reports the non-standard default; absence means standard.

A run-starting send's own model wins over that recorded selection. Its tier is its explicit
`service_tier`, otherwise the latest recorded selection's default, otherwise standard. A send's
own model or tier applies only to its run and does not change the recorded next-run choice.
Broca has no separate host model picker or host-switch path. Joining sends retain the running
run's model and fixed tier. Sessions with no plan selection keep the legacy joining-only
model omission rule. Unsupported plan models (including unavailable provider sign-in)
refuse `invalid_params` with `{field:"plan.model", reason:"unsupported_model"}`; unsupported
plan tiers name `plan.model.service_tier` with reason `unsupported_tier`. These checks are
read-only and precede acceptance and writes. The selection is recovered from the creating
`RunStart`'s manifest and later accepted refreshes, not from a run's applied tier or a refetch of
the original plan.

`usage`, behind the `session_usage` group, is incremental durable session spend with
`since` naming the first counted run. Its selection keys, first-use ordering and lower-bound
totals are exactly run usage's: a field no counted step reported is absent; unreported steps
are counted, not silently treated as measured zeros. `reasoning_tokens` is already included in
`output_tokens`, not an additional charge; the session and run totals preserve that breakdown
without adding it again.

`sender`, behind `head_sender`, reports
the attested caller's own effective grant, using send admission's owner and plan checks. Owners
receive all supported modes and interruption; named senders receive `idle_start` and interruption
only when their entries grant them. An absent sender means no grant. `incarnation` is the UUID v4
minted with the session and kept in its head, preserved by restart and move. A session that
has never been written reads `"unwritten"`; a read never mints one.

Sessions recorded before attested owners were stamped, and direct or unverified sessions, retain
their pre-existing unrestricted admission: any attested caller is admitted as owner. Head therefore
reports the full owner grant to those callers too; reporting absence would incorrectly deny the
permission the next send actually has. This does not tighten or otherwise change legacy admission.
Flow sessions recorded without `agent_id_frozen` retain their historical admission that ignores `agent_id`; new flow freezes compare it, including absence.
After a rollback, an older build cannot send to a flow session created with an `agent_id` and does not enforce the absence freeze on new flow sessions; existing flow sessions are unaffected.

Tool and step-transform plan items may omit `preset`. Broca preserves the omission
on catalog and declaration calls; it never substitutes an empty string. Mode-3 compaction
items require a string `preset` at admission, before any write. Frozen legacy compaction
items may still omit it and make no provider calls. An explicit
`"worker"` tool preset still requires scope. A runner declaring `system_text_default` also accepts
a preset-less system-text item, but forwards it only when that provider's daemon catalog entry
declares `system-text-default/v1`. Otherwise admission refuses `system_text_preset_required`
with the provider named, before forwarding or writing. The resolved text is frozen at admission,
just like preset-selected text. A preset-less step declaration refused by its provider keeps the provider's code,
message and field; it does not invent a `preset_missing` difference for a nonexistent name.

### Health counters and per-request diagnostics

`health.check.metrics.tool_routes` is an in-memory census of the shared tool-provider route
cache. It reports `open_session_actors`, `cached_routes`, `cached_routes_per_provider`,
`oldest_route_idle_secs` (null for an empty cache), `routes_for_closed_sessions` (normally zero),
`goodbye_failures_since_boot`, `pending_goodbyes`, `goodbye_retry_overflows_since_boot` and
`goodbye_giveups_since_boot`. Idle age is time since a route was last
acquired for a batch or warm bind, not time since the last provider frame. Close failures count
failed or timed-out enqueue attempts, including retries; a successful enqueue is not a remote
acknowledgement. Failed closes retry independently of tool calls, even after the actor's bounded
close waiter ends. Session teardown and congested late/abandoned opens share one cleanup queue
per consumer connection. It holds at most 1,024 distinct route identities; overflow is counted and
warned. Duplicate close requests can allocate additional waiters, so this identity cap is not a
bound on total cleanup heap or task allocation.
Entries expire after 30 seconds with one give-up WARN and an increment to
`goodbye_giveups_since_boot`. Retry passes run in parallel with one shared five-second budget,
with a one-second pause between passes, shortened to the earliest pending expiry. Connection loss
synchronously removes entries for the lost connection generation and rejects later cleanup
enqueues for that generation;
cleanup tasks do not retain the connection owner. A give-up is not a remote acknowledgement:
it stops local retries, and the daemon may retain that channel until the connection ends.
No session names, project paths, prompts or tool arguments appear in this
block. `open_actor_reasons` counts non-exclusive hold reasons observed at each actor-loop turn:
active runs, held calls (or unknown held state), live subscribers, manual/source warming, pending
work/cancellation, withdrawals and startup recovery. Actors with no observed hold are counted
under `idle_ttl`; asynchronous holder changes become visible on the next actor turn, at latest
its ordinary idle tick. A paused held episode alone does not prevent idle-close.
Session-close INFO records use the hashed session address, provider module ids, successful
Goodbye count and failure count; each failed or timed-out Goodbye also emits a WARN.

`health.check` includes `metrics.management_ops`, a process-lifetime counter for every
request/response operation in the management manifest and the `session.subscribe` stream. The
object always contains every operation at zero or above; counts reset when broca restarts. A
request increments after authorization and migration checks, even when its parameters or handler
later return an error. Authorization and migration refusals are not counted. The existing
`session_reads` and `actor_opens` counters remain available with their existing meanings.

`health.check.metrics.views_over_cap` counts the views of closed sessions broca loaded and didn't
keep because each alone was larger than the view cache's byte cap (96 MiB, counted by walking
each view); each read of such a session while it is closed loads it again. `memory.report` gives
the same count for its process under `closed_session_views.views_over_cap`, and each such view
logs one INFO line with the session address and its counted size. Counts reset on restart.

`health.check.metrics.usage_observe` reports best-effort raw quota forwarding to insula:
`rows_sent` and `batches_sent` count rows and batches handed to the management transport
(not insula's accepted readings), `transport_failures` counts failed, refused, timed-out or
malformed batch replies, `rows_dropped` counts oversized rows or queue overflow/contention,
and `ignored` counts insula's `ignored[].reason` values. Counts reset on restart.
Only Anthropic `oauth` and OpenAI `chatgpt` runs with a frozen vault-resolved credential id
participate. The account, when present, is the run's frozen vault-returned account reference.
Broca forwards unified Claude headers and allowlisted Codex headers/raw `codex.rate_limits`
JSON, without interpreting quota values. The per-conversation `x-codex-turn-state` token
is never forwarded. No observation enters the session's records, rendering or billing lanes.

The always-on sender uses a management route to `insula` without a project scope, with the same attested
consumer connection as routing. Every 30 seconds it takes all pending readings and sends
one `usage.observe` batch, once, with a five-second budget and no retries. Each
`(credentialId, provider, headers-or-frame)` holds only its newest observation (the latest
received wins equal timestamps). At most 128 keys are pending; rows must serialize to less
than 16 KiB. New keys at capacity, oversized rows, unrepresentable non-UTF-8 headers and
rows arriving while the queue is locked are dropped and counted. Provider requests never
wait for the queue lock or insula IO. A failed batch is discarded, not requeued.

`health.check.metrics.session_readers` is a process-wide, in-memory census of authorized
read attempts, separate from the legacy numeric `metrics.session_reads`. Its `entries` array
contains `{principal, op, owner, count, last_ms}` for `session.read`, `session.history`,
`session.head`, `session.subscribe`, `run.status`, `run.admission`, `run.result`, `run.tool_calls`, `send.status`,
`session.send_status` and `session.media_read`. Authorization and migration refusals are excluded;
later parameter or handler errors still count. Subscriptions count once when opened, not once per frame.
The principal is rendered from the daemon's route stamp (`reserved:<module_id>`, `direct`,
`unverified`, or the existing principal type's other rendering); a bind from an older daemon
with no stamp is named `unattested`. Request parameters cannot change that attribution.
`owner` is true for the recorded first sender or scope owner, false for other callers, and
null when ordinary requests have not yet loaded ownership into the cache. Unknown reads
retain their own keys rather than being reclassified later. Telemetry never reads storage
or opens an actor to discover ownership. The key is `(principal, op, owner)` with at most 256
distinct keys; attempts introducing further keys increment `overflow`, while existing keys
continue counting. Counts, `overflow` and the epoch-millisecond `last_ms` values reset on
restart; `since_ms` is the daemon's process-start coverage timestamp. No census is written
to a log or store.

Sessions kept in the new session storage are encrypted unless they were created plain. The root
`broca.jsonc` setting `"session_storage": {"encryption": "off"}` makes the sessions created from
then on plain: stored without encryption, with an unkeyed check that catches damage but not a
deliberate edit, and served without the deployment key. `"on"` (the default) creates encrypted
sessions. A session keeps the mode it was created in, so changing the setting leaves existing
sessions as they are, and a deployment can hold both kinds.

Encrypted sessions are opened with a deployment key read from the vault
when broca starts: the static record `opaque:broca-deployment:<key_id>`, read by grant
(`credential.get_scoped`, an exact `read` grant to `reserved:broca`), whose value is 32 bytes
written as 64 lowercase hex characters. The root `broca.jsonc` setting
`"session_storage": {"deployment_key_id": 1}` names the key (1 when absent); a rotation adds a
record under a new id and never rewrites one. Startup never waits for or fails on the key. Until
it loads (the vault is unreachable, answers `not_found`, or holds a malformed value), today's
sessions and plain sessions are served as before, and an encrypted session kept in the new
storage is refused `open_failed`, class `transient`, its reason starting `key_unavailable:`;
broca reads the record again with backoff and serves those sessions once it loads, without a
restart. A deployment with encryption off and no encrypted sessions never needs the key. The
key is kept in memory only; logs name only its id, with one WARN while it is missing and one
INFO when it loads. `health.check.metrics.store` reports `{key_id, loaded, new_sessions, phases}`,
`new_sessions` being `"encrypted"` or `"plain"`.

`phases` reports process-lifetime monotonic timings for `open`, `append`, `sync`, `seal`,
`media_wait`, `request_prepare`, `media_fold`, `read`, `entries_page`, `collection` and
`collection_sweep`. Each has `{count, total_us, max_us, last_us}`, initially zero. An elapsed
duration smaller than one microsecond is reported as one; snapshots read independent relaxed
atomics, so concurrent completions can be partially reflected. Counts include failed operations
that entered a measured phase. `sync` measures the local log's disk barrier separately from
append; memory backends have no disk barrier. `request_prepare` ends at the run loop's final
`CallOptions` handoff, excludes its media wait, and does not include provider rendering or
transport. `seal` also sums `transcript_records`, `ledger_records`, `kept_records`,
`transcript_bytes`, `ledger_bytes`, `index_entries`, `merged_entries` and `forced_merges` from
successful seals. These metrics are never stored and reset on restart. Without a published
new-storage slot, `metrics.store` remains null.

A phase exceeding its budget emits one WARN on `session_storage`, with `phase`, `elapsed_us`,
`elapsed_ms` and the hashed session address (absent for a whole sweep). Budgets are open/seal
500 ms, sync 100 ms, request preparation 50 ms, media fold 1 s, read/entries page 200 ms and
collection/sweep 1 s. Append and media wait have counters but no slow-line policy. Normal phases
emit no lines.

### Repetition warnings

Fresh admissions freeze `repetition_after_input` in the render config: repetition warnings start
at each run's first model answer, not at assistant messages supplied as its input. A seeded import
with no model steps therefore adds no warning. Configs without the flag retain the previous
boundary, including on resume and offline request reconstruction; recorded request bytes are not
retroactively corrected. The conversation checkpoint may retain a derived, optional first-answer
ordinal, which older readers ignore. Older checkpoints use each message's entry facts to distinguish
model answers from caller-supplied input when available; when neither attribution nor the original
input boundary survives, rendering keeps the legacy
boundary and logs a warning naming the session rather than refusing the run.

### Per-request debug records

To see one debug record per served operation, add a `CK_LOG` value to broca's module environment
in `subc.jsonc`:

```json
{
  "modules": {
    "broca": {
      "env": {
        "CK_LOG": "info,broca.requests=debug"
      }
    }
  }
}
```

The root remains at `info`; only the `broca.requests` component is raised to `debug`. Each record
names the operation and the session string from the route bind, reports `actor_opened` as true only
when this request successfully opened a new actor, and gives elapsed microseconds. Request
parameters are not logged. Broca reads `CK_LOG` when its logger starts and does not reload the
filter, so restart the broca module after changing this setting; reloading configuration alone is
not enough.

The `llm-runner/v1` role ops (`role.describe`, `session.head`, `run.result`, `run.tool_calls`,
`session.baseline`) and the newer `session.read` members follow commons
`cortexkit-role-llm-runner/CONTRACT.md`. In broca's terms:

- `after_mid` continues at the supplied `history_version`. A mismatching version refuses
  `history_changed`; an unknown mid at the current version refuses `unknown_mid`. The old
  `lineage_id` input is accepted and discarded, not used to select history. Other unknown
  members and cursor misuse refuse `invalid_params` with `detail.field`.
- A transcript page is also capped by bytes, measured over the complete serialized reply body
  (default and maximum **3 MiB / 3,145,728 bytes**). Larger `max_bytes` requests are capped to
  that maximum. Messages and late approvals share the budget, including tools, attribution,
  head fields and continuation cursors. A single message and a late entry remain whole when they
  exceed the page target, but a complete encoded frame that exceeds the transport limit is refused
  as `reply_too_large` with the role detail `{bytes, limit, subject}`. A raw page names its
  indivisible message as `subject: {kind: "message", session, ordinal}`; a model-view page names the
  source entry as `subject: {kind: "model_group", source}`, where `source` is the published
  `EntrySource` (`message` or `replacement`). Message content is never included. Model-view pages
  use the same target and keep replacement groups and insertion/anchor groups whole. Source-less
  metadata overflow uses the adapter's generic `response_too_large` refusal because there is no
  message or model entry to name.
- `tool_calls[].call_key` preserves a recorded consent-preparation or dispatch key. Without
  such a key, a native call uses `<lineage_id>:<wal_seq>` of its own `ToolIntent` record, or of
  its outcome record when it never dispatched.
  Calls sharing a model ID still have distinct per-occurrence anchors. Imported calls and
  results synthesized into one run's input remain keyless: their shared input record cannot
  identify each call uniquely. `dispatched_to` remains the dispatch intent's recorded module,
  absent for calls never dispatched and for historical intents without that observation.
  The same durable key is served on call, read, display and live facts before and after restart.
- Every known run's `run.status` and `run.result` carry `usage` from completed `StepEnd` and
  billed `StepRefused` records, including while the run is
  active. It always has
  `steps` and `unreported_steps`; a run with no completed step reports exactly
  `{"steps":0,"unreported_steps":0,"by_model":[]}`. A step is unreported only if **all five** token
  measurements are absent; a partial report or an explicit zero is reported.
  The optional totals `input_tokens`, `cached_input_tokens`, `cache_write_tokens`,
  `output_tokens` and `reasoning_tokens` sum only reported contributions, staying absent
  until a step reports that measurement. Partial totals are lower bounds, not estimates.
  Input is fresh input only, summed across serving iterations when a step uses more than
  one model; reasoning is already included in output, never added to it.
  `by_model` is always present and empty until a step completes; then it lists contributions
  in first-seen order for each `{provider, model, variant?}`. Ordinary calls use the frozen
  episode model/variant; routed calls use their fallback model, and server handoffs use
  each provider-reported iteration model without an episode variant. When iterations are
  present, `steps` counts billed serving iterations rather than durable runner steps,
  and each iteration's measurements replace the last-iteration-only top-level counters.
  Each entry carries the same counters and optional token totals. An unnamed variant is
  omitted, never an empty string; each entry contributes to the run totals, including
  unreported steps. These counters and the model breakdown do not change subscriber
  `step_finished` / `run_finished` usage units or the record format.
  An optional `generation` list groups measured model calls by `{model, service_tier?, speed?}`
  in first-measured order. Each entry has `model_calls`, `output_tokens`,
  `sent_to_last_content_ms` and `first_to_last_content_ms`: both durations and the token total
  cover the same final attempts with reported output tokens and timestamps ordered
  `sent_at_ms <= first_content_at_ms <= last_content_at_ms`. Durations are sums of `last - sent`
  and `last - first`; they exclude tool time, failed retries and post-content bookkeeping.
  `reasoning_tokens` is included only when every counted call reports it, and is already part of
  output. Tier and speed are provider-reported facts, omitted when absent. Served models use the
  billing attribution; mixed-model calls are excluded because the log has no per-model timing
  split. Missing or reversed timestamps and unreported output tokens produce no sample, not zero.
  Empty groups and an entirely unmeasured `generation` member are omitted; older logs remain
  unmeasured. These client-side decoded-content intervals measure observed delivery, not the
  provider's internal token-generation clock.
- `run.result`'s `final_message.text` joins the final assistant message's text parts with
  nothing inserted, leaving reasoning out; a completed run with no text answers `""`.
  It is not paged and is not subject to `session.read.max_bytes`; when its complete encoded frame
  exceeds the transport limit, broca refuses `reply_too_large` and names the final message by
  session and ordinal without including its content. A run result without a final message uses
  the generic `response_too_large` refusal if its metadata alone cannot fit.
- `run.tool_calls` pages the run's durable calls in dispatch order. Every call in a
  batch has the `ordinal` of the assistant message that requested it, which is also
  the `session.read` ordinal. The assistant message is followed by one tool-role
  message at `ordinal + 1` containing all results in the order the calls were planned
  when the batch started (the same positions as the assistant message's tool-call
  blocks), even if calls completed in another order. Match calls to results by
  position in those messages: `tool_call_id` is a model display id and may repeat,
  while the per-call `call_key` identifies each prepared or dispatched call. A steer
  user message, when present, comes after the tool-result message.
  `limit` is capped to the
  transcript-read maximum (absent means its default; zero is `invalid_params` naming `limit`);
  `after` is an opaque continuation returned as `next`, and an unknown cursor is refused naming
  `after`. Pages share `session.read`'s 3 MiB serialized-body cap. A call's arguments stay whole:
  one call larger than that page target is served alone, while a complete frame larger than
  4,194,304 bytes is refused as `reply_too_large` with `subject: {kind: "tool_call", run_id,
  tool_call_id, call_key?}`. The answer includes only `result_bytes`, the byte length of the
  stored tool-result encoding; result content is never returned. Calls with a durable intent but
  no result read as `indeterminate`, not as a failed zero-byte result.
- `session.baseline` and the admitting send's `baseline`: sessions are admitted without a fetch
  plan, so `items` is empty and `composition_digest` absent; `tool_names` are the first
  episode's tools; both generations are 0. The send reply carries it only when the session's
  first episode runs under the send's own run id, from what the session's actor holds; it
  reads nothing.
- `session.head`'s `updated_at`, for a retained session, is the durable activity that sets its
  expiry: lifecycle and send records advance it, but cache warms and other maintenance do not.
  Its `title` is the newest title a send recorded.
- A `steer` or `interrupt` into a session that has an episode takes every runner parameter it
  omits (`model` with its variant, `tool_choice`, `generation`, `stop_when`, `cache`, `budget`,
  `context_limit`, `work_class`, `service_tier`) from the session's latest frozen config, so its
  turn renders the session's request prefix. An ordered delivery list inherits only if it
  contains a mode that can join an episode (`steer`, `tool_result`, or `turn_system`); an
  `idle_start`-only list takes defaults, like a plain queue send. A session with no episode
  has nothing to inherit: it requires `model` and takes defaults for other omitted runner
  parameters, even if the session already exists with no run. If the session's history cannot be
  read, a send needing it refuses with class `transient` rather than assuming the session has none. `tools` and `context_management` already inherit on
  omission for every send. Any other send must name `model`. When an inheriting send is
  queued behind a running turn, its recorded request carries the frozen model itself, so a
  build that does not know the inherited set still runs it on the session's model.
  With the service-tier writer enabled, `service_tier` follows the fixed-run rules below
  instead of inheriting from an ended run.

- Ordered delivery advertises `tool_result`, `turn_system`, `steer` and `idle_start` globally.
  A `turn_system` delivery requires a validated plan template and a legal position on the
  frozen provider route. Responses renders a `developer` input item in conversation order,
  never top-level `instructions`. Anthropic requires the provider's explicit
  `capability.mid_conversation_system` catalog fact and renders a persistent `system` message
  after a user turn or tool result, at the end of the request; it adds neither `clear_at` nor
  a beta header. An absent fact is unsupported, not an admission failure. Gemini and other
  unsupported routes skip to the next listed mode, or keep the send pending if none is
  possible. A stored system entry switched to an unsupported route renders as user text
  without changing the stored role, text or delivery mode.

`role.describe` declares `routed_accounts: ["oauth", "chatgpt", "apikey"]` with the matching
`routed_accounts` capability. Broca chooses subscription accounts and pooled API keys through
`route.account_for` and reports their runs through `route.account_observe`.
`antigravity` is not listed: its existing caller/configured credential selection is unchanged.
For these methods, broca asks routing even with one eligible credential. A subscription pin
without routing hints or a decision limits the offer to the named account; routing still admits
the run. If broca cannot list the candidates, it refuses rather than using a static credential.

For a listed method, `auth.pool` is sent as `route.account_for.pool`, and optional
`auth.credential_id` is sent only as `requested_account.credential_id`, a preference the router
may overrule. Either hint requires routing even with a single candidate. If the vault cannot
provide its candidate credential list, or routing answers `account_selection_disabled`, broca
refuses rather than using the configured default credential without honoring the hints.
A routing refusal reaches the caller with an outer code that says whether to wait:
`account_limit_exhausted` for an exhausted pool, a hold or unknown quota,
`account_selection_refused` for another refusal that may clear, and `account_selection_final`
for a permanent one (including every routing code broca does not know, and the hinted
`account_selection_disabled` refusal above), which waiting can't change. See the
[error class contract](error-class-contract.md).
Both hints are ignored for unlisted methods before request storage and identity comparison.
A strict `auth.account` pin cannot be combined with either hint for a listed method:
`invalid_params` names `auth.account` before storage or any routing request. Queued requests
retain their original hints across restart; a resumed run keeps its frozen credential/decision
and accepts no fresh auth pin.

The static exception is an API-key send that explicitly pins `auth.account` and supplies no
pool, credential hint, account route or routing decision. That pin must name an exact eligible
vault row; if it is no longer listed, broca refuses permanently rather than using another key.
Other API-key sends ask routing with
`mode: "admit"` and load the answered `account.credential_id`, never the credential hint.
Broca validates that the answer declares method `apikey` and names an active, readable API key
in its own vault listing. Its `provider_ids` must contain the credential provider, or be empty
(the legacy provider-unmapped case). Broca offers those keys as candidates and refuses an
answer outside the offered account/credential pairs. A key's account id is the vault's
`account_id`, or its credential id when that metadata is absent. Missing listings refuse
hinted admissions rather than bypassing
routing. `run_finished` with the admission id releases the routed key, just as for subscriptions.

For a run admitted by routing under any method (`oauth`, `chatgpt` or `apikey`), an uncleared
provider error classified at its source as `transient` reports one `transient_backoff` before
`run_finished`. The final model attempt's recorded outcome must be a failure, not a successful
or cancelled attempt.
Its stable event id is `<id>:backoff`, distinct from the release, and `retry_at_ms` is always
present: the final attempt's receipt time plus the structured provider retry delay
(`retry_after_secs`), or plus 5,000 ms when no delay is supplied. Broca never infers timing from
message text. A transient recovered by broca's own retries is not reported as backoff;
`account_limit_exhausted` and `credential_refused` keep their own paths and method rules.
The observation is sent only when the
router's `role.describe` declares `observe_backoff`. Discovery uses the management route's
capability cache, also used for `service_tier`, and refreshes when that route is reopened.
Renewal (`mode: "renew"` with `expected_pin`) is not currently implemented for any routed method.

For a routed admission ending in `billing_exhausted`, `credit_insufficient`,
`account_not_entitled` or `usage_not_included`, broca sends `credential_refused` before
`run_finished`, blocking that credential for every runner. API keys also report an unrecovered
`auth_required` HTTP 401/403, including an `AuthRequired` pause: the pause is preserved and sends
no `run_finished`. OAuth and ChatGPT auth pauses send no credential refusal. The report
names `admission_id`, includes `credential_id` when known and `provider_code` verbatim when present,
and never includes `retry_at_ms`. It is sent only when the router declares
`observe_credential_refused` and its prerequisite `observe_backoff`; resettable account limits
and transient rate limits keep their existing reports.

An exactly identified subscription spending or credit wall is instead reported through
`route.account_observe` as `account_limit_exhausted` with `limit_window: "credit"`.
Broca includes reset timing only when the provider supplied it; no reset is invented for a
top-up. A credit wall does not start an account-limit continuation, even with a retry hint.
Prepaid API-key balance refusals retain the `credential_refused` path above. Timed quotas
keep their existing window and reset evidence, and `route.report` has no `limit_window`.

Broca sends the effective `service_tier` on `route.account_for` only when the router's
`role.describe` declares `service_tier`. A tier-aware answer's `service_tier_chosen` must be a
known level, must be present for an explicit request, and cannot upgrade it. An unbound
admission may step down; broca applies that chosen level. A decision-bound non-standard request
must receive exactly its fixed level, not a second choice at a lower level. Legacy routers
receive no tier member and their absent chosen tier is not treated as confirmation of a
non-standard level. Broca does not report `service_tier_applied` or
`service_tier_served` to account routing.

#### Service-tier writer switch

The boot-only root setting in `broca.jsonc` is `"service_tier": {"enabled": true}`.
An absent block, an empty block or `"enabled": false` leaves it **off**. A bare boolean,
an unknown member in the block, or a non-boolean `enabled` refuses boot naming
`service_tier.enabled`. Other root settings remain owned by their existing loaders.
Like `anthropic_fallbacks.refusal_resend.enabled`, changing this switch requires a restart.

Off preserves the legacy `flex`/`default`/`priority` admission and frozen bundles, record bytes,
discovery, default replies and provider requests. It does not declare the `service_tier`
group or add tier members to default run usage. Queue reading remains available
even when the writer switch is off.

On declares `service_tier` and `role.describe.service_tiers`, and accepts only the abstract
`standard`, `fast` and `ultrafast` levels on `session.send`. A new run without the member
takes the latest recorded plan default, or standard when none was selected; a send into an
unfinished run inherits its fixed tier when absent.
Explicitly changing that tier, including explicitly asking for standard, refuses
`invalid_params` with detail `{field: "service_tier", reason: "run_tier_fixed"}`.
Unknown levels and unsupported routes refuse before a model call with reason
`unsupported_tier`, naming the level and, for route refusals, the model. Fast renders
OpenAI `priority`; ultrafast is available only on an OpenAI wire whose model row has priced
input and output ultrafast modes. Non-standard modes are never priced from base rates.
Discovery lists family support, not a promise that every model on that family prices a mode.

Tier-bearing queued sends retain the open `service_tier_level` string and require a reader
supporting `service-tier-queue/v1`, available since broca 0.3.298. Admitted runs render their recorded configuration on resume,
without consulting the current catalog for tier eligibility. The legacy closed
`service_tier_request` decoder stays for old render bundles; it does not gain abstract variants.

With the switch on, `run.status` and `run.result` add optional
`usage.service_tier_applied` (non-standard only) and `usage.service_tier_served`. Served comes
only from provider statements, including an explicitly reported standard. Missing reports do
not vote; conflicting reports leave served absent, including after restart. Unmapped served
strings remain verbatim. The existing durable run/step records rebuild this projection;
there is no new durable usage accumulator. A mode-3 compaction session sends `compaction.ride` with
cause `tier_change` before the first changed-tier request,
including a return to standard, and its next notice
carries the change event and non-standard condition. Unchanged tiers do not ride.

The service-tier switch does not depend on role-routing's route-selection and reporting
contract. Broca uses its own `route.account_for` / `route.account_observe` protocol instead.
Account requests carry the requested level only to tier-aware routers as described above;
observations still carry neither applied nor served tier. There is no second account-routing path.

#### Tool-name aliases

The operator's `broca.jsonc` may set `"tool_name_aliases": {"oauth:example": {"names": {"bash": "client_bash"}}}` or use `{"prefix_all": "client_"}` for that credential route to rename every declared tool. Each route accepts exactly one form and no other members; malformed shapes refuse boot. Keys are credential routing keys (`<method>:<provider>`), not account IDs. Aliases affect only provider requests and are restored on replies; stored tools, history and dispatch remain canonical. Invalid names or collisions refuse admission as permanent `tool_alias_invalid` with `detail.tool` naming the tool, before any write. The resolved map is frozen into each new run; resume and retry retain it even after the config changes or is removed. Restart to load operator config changes.

#### Session input-token cap

broca keeps no session input-token budget and does not declare `input_token_cap`. A send
carrying `budget.max_input_tokens` is refused `invalid_params` naming that field.

### 4a. `session.warm`

A provider caches a request's prefix for a fixed lifetime from when the request was sent. A
caller whose next round arrives later than that pays to write the whole prefix again. A warm
re-sends the session's most recent model request changed only so the provider generates nothing
(Anthropic: `max_tokens: 0`, no `stream`, no server-side `fallbacks` opt-in or its beta);
everything that shapes the cached prefix (system,
tools, messages, `cache_control` markers and their TTLs, adaptive thinking, effort, speed) is
byte-identical. The caller schedules and caps warms; broca performs each one exactly and reports
what it cost.

- **Base.** While a run is generating, the exact bytes its in-flight attempt sent (their digest
  is the `request_body_sha256` that attempt's `model_attempt_finished` records). Otherwise the
  session's last request, rebuilt from its records and rendered from the frozen config; it is sent
  only if it renders to the digest its step recorded, else refused `warm_prefix_unavailable`. A
  running run is never a refusal.
- **Refusals**, each decided before anything is sent and with a pinned golden body under
  `crates/broca-wire/tests/golden/warm_refusal.*.json` (`code` equals `detail.kind`):
  `lineage_changed`, `run_paused`, `account_walled` (the latest attempt hit
  `account_limit_exhausted` and `resets_at_ms` is still ahead), `warm_in_flight` (another warm
  of the session has not finished; one runs at a time), `account_changed` (the
  credential, read again as a run reads it, belongs to another account than the session froze),
  `no_step_yet`, `warm_unsupported` (any provider but first-party Anthropic; Bedrock Converse,
  which has no documented zero-output request; manual thinking, a forced `tool_choice` or a
  structured output format), `cache_expired` (the estimated expiry is unknown, or under 5 s
  away: a warm then would be a full cache write) and `warm_prefix_unavailable`. A warm that was
  sent and that the provider refused answers `warm_failed` with the provider's error.
- **Concurrency.** The session's actor only decides refusals and snapshots what the warm needs;
  the warm itself runs on its own task, so `run.cancel`, sends, steers and status for the
  session are never held behind a slow provider. Its completion comes back to the actor, which
  writes the record through the session's single writer and moves the clock; a warm finishing
  after the next run started is still recorded and billed under the run it re-sent.
- **Expiry.** The lifetime runs from the warmed request's send start; a successful warm restarts
  it at its own send start. `expiry_basis` is always `"estimated"`. The clock lives in memory, so
  after a restart it is unknown (and warms are refused `cache_expired`) until the session sends
  again.
- **Records.** Each warm writes one `Warmed` record (digests, usage, account, the raw
  `anthropic-ratelimit-unified-*` headers, expiry). `session.read` skips it; it is never a
  transcript message. Billing turns it into its own export fact marked `warm: true`, priced
  from its own usage. Absent usage stays absent. A failed warm never refreshes cache expiry.
- **Head and declaration.** `session.head.cache` reports the clock from memory, with no record
  scan. The admitting send's `baseline` and `session.baseline` carry `cache {ttl_ms,
  ttl_options_ms, read_refreshes_ttl, warm: "supported" | "unsupported", basis}`, every member
  absent when unknown; `basis` is `requested` when the session's cache request named the
  lifetime and `provider_default` when the provider's default applies.

A **subscribe cursor** is `{wal_seq, sub_index, incarnation, digest}`: the record's sequence
number, the unit's index within that record's projection, the log's incarnation, and a
16-hex-character identity of the record.
`subscribe`, `session.head.head` and `session.read.head` share this domain.
A cursor attach starts strictly after that unit, so `read → subscribe(from: head)` is a
gapless, duplicate-free handoff. `head` is absent when the log has no control unit.
Malformed `from` refuses `invalid_params` naming `from`; a parsed cursor with a missing or
different incarnation, a missing or different frame digest, an absent frame, or a
sub-index with no corresponding control unit refuses `cursor_expired` before any event. Position-only
wire cursors also expire. Normal restarts preserve cursors. Within the same log incarnation, a
cursor remains valid exactly when its record survives at the same sequence with the same
identity.
Missing or null `from`
means live. On either refusal, drop the cursor, reopen live, and read `session.head`.

The `broca-session --json-events subscribe` observer prints its last processed control
cursor in an `observed` event as an opaque string containing compact cursor JSON, alongside
the numeric `wal_seq` position. Pass the string unchanged as `subscribe --from-cursor
'<opaque cursor>'` to resume strictly after that control unit; a position-only `seq:index`
string is not a resume cursor. If no control event was delivered, both `cursor` and
`wal_seq` are null: there is no resume position, so use `subscribe --from start`.

Broca declares `notices`, with `read_display_facts`, `history_display` and `live_events`.
A notice delivered with a tool result is stored apart from the result and adds a `notices` list to its tool-result entry on opted-in reads and
to its display row, never a new transcript entry. Each fact names its stored result `part`,
carrying `call_key`, `send_id`, `sender`, exact `text` and `delivered_as: "tool_result"`.
That mode belongs to the notice fact, not the carrier: a notice never adds `delivered_as` to
the carrying transcript message, and message ordinals and mids are unchanged.
The corresponding `notice_delivered` live event names the same result and carrying call.
Carriers serve only the tool's own output on default and opt-in reads, raw and
display history, compaction entries and subscribe frames. The shared renderer adds the
recorded ride only when constructing model input; model request bytes do not change.
A delivery recorded before notices were stored apart keeps its joined read/history output and emits
neither a notice fact nor a notice event. Its subscribe result and compaction entry still
contain only the tool's own output, without riding text. Compaction's separate `delivered`
list retains the recorded send text and attribution.
An unkeyed send's notices name the submission ID broca's reply gave that send; client-supplied
send IDs take precedence, and a colliding minted notice ID is not reported.
A replay that cannot check earlier client-supplied send IDs refuses with
`notice_send_keys_unavailable`, rather than silently omitting notices or assuming
that generated submission IDs are unique.
Neither fact enters a checksum or changes `history_version`. Default reads omit them.
With `include_display_facts`, `sender` is served beside `mid` when recorded send attribution
names a module; raw history also serves that field. Absence does not identify a human
or disprove a sender.
Broca also declares `send_ids`: those module-send entries carry their caller's `send_id`
beside `sender`, or the returned `submission_id` for an unkeyed queued send. A direct
unkeyed send or steer that returned no submission id has none. These facts never enter
message bodies, content digests or `history_version`, and default reads omit them.
An unkeyed ordered `idle_start` admitted while idle also omits the field; if admission
failed transiently and answered pending before a later drain, that send serves no `send_id`
though its reply carried one, because the recorded facts cannot distinguish those replies.
Historical ids are recovered only from exact stored send receipts. Older imports that
discarded the source id, and converted steers without a stored send id, remain unknown
and omit the field. Builds before `dabddee12` wrote converted steer receipts one record
early; even an apparently valid reference can name an adjacent steer, so reads cannot
prove those ids from the receipts alone.
Broca does not declare `sender_attribution` and serves no `provider_executed` flags:
it stores no tools that a model provider executed internally.

`session.send.system` is an OPTIONAL leading `Role::System` message for the turn: one
byte-exact text block, delivered role-scoped before the user prompt; absent, empty, or
whitespace-only means no system message.

Both `send.status` and `session.send_status` are served as names for the same operation.
`send.status {send_id}` reads that send's durable fate and answers the same reply bytes as
an identical re-send, including its baseline and final delivery receipt. An auth-paused send
instead reports its current durable pause and reason; an explicit re-send still re-drives
that run's authentication. It never admits,
delivers or re-queues work, does not update retention activity, and does not reopen a closed
session actor. An unrecorded key for the caller is refused `unknown_send` (terminal).
`run.status` also continues to accept a queued send's submission-derived run id, reporting
its pending, delivered or ended fate as described in §14; callers can migrate to `send.status`
without losing that compatibility path.

Broca declares `send_status`, `max_steps`, `temperature` and `no_tools`. The role's positive-u64
`stop_when.max_steps` object is accepted beside the legacy stop-condition list. Zero or
malformed limits are refused `invalid_params` naming `stop_when.max_steps` or
`generation.max_output_tokens`. A temperature must be a finite number in `[0, 2]`, otherwise
it is refused `invalid_params` naming `generation.temperature`. At admission, temperature is
omitted for an unsupported model or an Anthropic subscription route with interleaved thinking,
clamped to the model's known
range, or clamped to `[0, 1]` when no complete range is known. Only the effective value or its
absence is recorded; resume never re-applies current catalog facts. A joining send's explicit
temperature is normalized using the run's frozen model and current catalog facts only to check
compatibility with that recorded setting; no rendered setting changes. If the model row is
unavailable, the join compares temperatures literally. An omitted temperature still inherits
the run's setting without re-normalizing it. Explicit output caps on supporting routes are bounded by
the catalog's model maximum. Broca does **not** declare `max_output_tokens`: ChatGPT routes
cannot enforce an output cap because their backend rejects the field, so they continue to
accept explicit caps without rendering them. `tools: []` offers no tools; on a plan session
any non-null `tools` on a later send is refused `invalid_params {field: "tools"}` before
queueing, steering or interrupting, even if it repeats the frozen surface.

`session.send.send_id` is an OPTIONAL client-chosen idempotency key, unique per LOGICAL
send. A retry of the same logical send reuses it VERBATIM; the module derives the same
`run_id`, so a byte-identical retry is an idempotent replay of the same run. Reusing a
`send_id` with DIFFERENT content is a typed error (the old run_id/send_id was already used
by another send). Omitting `send_id` leaves idempotency off and the server mints a fresh
`run_id`. `title` is not part of the request identity: a keyed resend with an edited title
returns the recorded answer and keeps the first title.

A keyed resend of a terminal run returns `{state:"finished", run_id, reason}` without
starting another episode. `reason` is the recorded `RunFinishReason` snake_case value
(for example, `completed` or `interrupted`). Active retries remain `active`; paused
retries resume and return `active`. Callers must decode `state` as an open set and
handle unknown states without assuming a run is active or waiting indefinitely.

A new send with non-empty legacy `tools` is refused as `scoped_tools_require_plan` with
`detail: {class: "permanent"}` when it would run under the route's stamped scope or the
session's frozen scope without a fetch plan. Tools on a scoped session must come from a
fetch plan, because tool providers require a per-call preset on scoped routes; only the
frozen plan manifest supplies those presets. A send carrying a plan, or using a session's
already-frozen manifest, keeps the existing plan checks. Scope and plan refusals take
precedence, and keyed resends of already-admitted runs keep their existing answer.
This refusal precedes any write or catalog fetch. Unscoped legacy sends and scoped
sends with absent or empty tools remain admitted, including steers, interrupts and queued
sends that declare no tools. Flow sends retain their own `invalid_params` tool refusal.
On a scoped, planless session admitted by an older build, a later send declaring non-empty
tools is refused before it can freeze that list, even if it queues or steers; it cannot
retrofit a plan (the existing `plan_drift` rule still applies). Omitting tools does not
revalidate or replace an older run's frozen tool list.

A session freezes its fetch plan on its first episode unless its owner explicitly refreshes it.
An ordinary later send cannot enroll compaction. A session whose first episode ran
without a plan, including one seeded by `session.import`, refuses a later send's plan naming a
compaction provider as `plan_drift`, and the refused send writes nothing.

The `session_change` group serves the owner-only `session.refresh`, `session.refresh_policy`
and `session.flush_prefix` operations defined by the [llm-runner contract](https://docs.rs/crate/cortexkit-role-llm-runner/0.7.14/source/CONTRACT.md)
(§10.2). Every ready baseline
states `rungs` for tools and system text, accepted/applied generations, and any pending/effective
change. Both surfaces support `immediately` (the next run) and `on_prefix_rebuild` (the first
run after an applied compaction view, model switch, or prefix flush). A new run alone is not a
rebuild. A flush is a durable rebuild boundary; with nothing pending it does not change request
bytes or promise a provider cache miss.

broca also declares `plan_notes`: a refresh may wait under the policy `notes_until_rebuild`,
with a short `system_note` exactly when it changes the system text. The applied plan's system
text and tools keep rendering, and each run tells the model of the change in notes: the system
note and the fixed wording for each removed tool, as system-role turns right after the run's
user turn. A call to a removed tool is refused with that wording. Notes are runner records, so
reads and history never show them. The change applies in full at the first run that loses the
prompt cache anyway (a flush, a model change, an effort change that loses the cache, a lapsed
cache, a refresh adding a compaction provider), once the oldest pending change is six hours old, or at
once when it changes the system text on a route with no system-role turn (`no_note_form`). Tool
additions and changed definitions get no note; they wait for that full apply.

In `session.baseline`, top-level composition and absence facts describe the frozen prefix.
After a refresh applies, `effective.composition_digest` copies the applied manifest's composition
digest (omitted when unavailable), and `effective.absent_items` reports its absent optional items
(omitted when empty). These may differ from the prefix after an `append` application; a `fold`
application rebuilds the prefix from the applied manifest. `effective.manifest_digest` remains the
identity of the whole applied manifest, not its composition digest. No `effective` object is
reported before any refresh applies.

Refresh admission uses the ordinary plan admitter and durably records the fetched manifest and
original reply before answering. Identical retries fetch and write nothing, even after restart
or application. Changes never alter a running episode: the applying `RunStart` freezes its
own manifest, and an earlier episode resumes from its own manifest. While a change is pending,
sends may carry either the effective or latest accepted plan; any other identity is `plan_drift`
against the latest accepted identity. Sessions never refreshed retain the first-plan drift rule.
A refresh carries no model. Before an episode can apply it, broca checks its model-bearing
items against that episode's resolved model, including an inherited model. A mismatch is
`plan_model_mismatch`, permanent, with `detail.generation` naming the pending refresh and
`detail.send_model` naming the resolved model. The refused send writes nothing and leaves the
change pending. The owner can supersede it with a newer refresh or select the model it expects;
carrying the old effective plan cannot bypass an immediately pending refresh.

`role.describe.capabilities` includes `send_tool_reply_bound` (a broca extension). A tool
declared directly in `session.send.tools` may carry `reply: {"max_ms": N}`, the longest one of
its calls may take to answer, with N from 1 to 86,400,000 (the bound a tool provider's catalog
`reply.max_ms` has). Broca then waits for each call of that tool by the same rule as a catalog
tool's `reply`: `max_ms`, never less than its default 120-second wait. Without `reply` the tool
keeps the default wait, after which a call with no answer is indeterminate and the run ends
`interrupted`. A frozen fetch plan's own `reply` for the tool takes precedence. The deadline is
frozen with the tool in the run's `RunStart` (a resume keeps it, and a later send or catalog
cannot change it); a send that omits `tools` inherits it with the rest of the tool. It is never
rendered into a provider request and never sent to the tool. `reply` is strict: a non-object,
a missing, zero, too large or non-integer `max_ms`, or any other member inside `reply` is
refused `invalid_params` naming the member (`tools[0].reply.max_ms`, `tools[0].reply.grace_ms`,
...) before anything is written. Other unknown members of a tool stay tolerated and ignored, as
before.

`role.describe.capabilities` includes `fetch-plan-optional-tools/v1`. A caller may mark
a plan's tool item `optional: true`; literal `false`, optional compaction/transform items,
and optional persona text remain invalid. Optional tool fetches have a five-second budget
inside the shared admission deadline. An unavailable, invalid or stale optional provider
is absent, not a launch refusal; its paired text is absent too. Broca removes that provider's
whole composition entry and fetches surviving system text again against the admitted
composition. When no optional tool provider is absent, tools and text are fetched in the
original single parallel round. The frozen baseline uses
the admitted digest and one absence entry per removed item (`kind: "tool"` or
`"system_text"`, a flat `reason`, and `provider_code` for provider refusals). Later sends
and resumes use the manifest without refetching.

`session.send.conversation_key` optionally selects routing's provider-account affinity across
sessions. When present it must be a string of 1–256 bytes, all printable ASCII `0x21`–`0x7E`
(no spaces). Invalid values, including null or non-strings, are refused before anything is
written, with `invalid_params {field: "conversation_key"}`. Absent inherits the latest episode's
frozen key from the durable admission snapshot. If that episode
predates key recording, the key is derived from the session as before; older episodes are not
searched. Episodes admitted with a recorded key carry it forward for subsequent sends.
The key is part of send identity: reuse of a `send_id` with a different key is refused as
`send_id_reuse` naming `conversation_key`. A queued send retains it durably until admission,
including across a crash. A steer joining a running run accepts the key but ignores it for
routing, since no admission takes place; an interrupt admits a new episode with its own key
(or the inherited key if absent). `RunStart`'s `auth_selection.account_decision` records the
frozen `conversation_key` and `conversation_key_source: "caller" | "session" | "inherited"`,
absent on old records. The source is an open string so future values survive replay.
Raw keys are stored on `QueuedSend` and `RunStart` records, not in usage exports.

For each `route.account_for` admission request, broca also sends the top-level integer
`hold_wait_max_s`: the longest wait it permits routing to hold for the session's previous
run's frozen account. It is `floor(max(0, min(setting, cache_expiry - now)) / 1 second)`,
with `max_same_account_wait` converted from minutes to duration and `"max"` omitting that
setting bound. The setting is loaded once at boot; changes take effect at the next restart.
The shared cache-expiry computation uses that account's last accepted 2xx
attempt send time plus the previous run's frozen TTL (explicit 5 minutes or 1 hour; unknown
means 5 minutes). Setting `0`, no previous run/account, or no accepted attempt sends `0`.
Admission reads only the existing frozen snapshot and in-memory account evidence, never the
log. After a restart, the first admission of each session sends `0` until that session has
run once. `health.check.metrics.account_limit_hold_cap_unknown` counts admissions that sent
`0` because evidence was not loaded, separately from known-zero allowances. These are broca's
outgoing request fields; routing decides whether and how to honor the allowance.

`session.send.title` optionally names the work a send carries, for dashboards and logs, on
every delivery mode. When present it must be a string of 1–200 Unicode scalar values, not all
whitespace, with no control character (newline and tab included). Anything else, including null
or a non-string, is refused before anything is written, with `invalid_params {field: "title"}`;
a title is never truncated. It is recorded on the record the send writes (`RunStart`,
`QueuedSend` or `Steer`, absent when the send carried none). A queued send's title is carried
onto the `RunStart` that drains it. It is INFORMATIONAL ONLY: never rendered into
a provider request, and not part of routing, `conversation_key`, cache keys or send identity, so
a retry under a known `send_id` with a different title gets the recorded answer, is not refused
`send_id_reuse`, and keeps the first title. Run-keyed `run.status` returns the run's `title`
when one was recorded (from the run's `RunStart`), and `session.head`
returns the session's newest title across all three record kinds. Absent stays absent in both,
never `""`. Usage export facts do not carry it.

`session.send.delivery` is OPTIONAL: `"queue"` (the default; absent means this), `"steer"` or
`"interrupt"`. It decides what the message does when a run is already active on the session.
Any other value is refused as `invalid_params` naming `context_management`. The mode is part of the send's
identity: the same `send_id` with another mode is refused `send_id_reuse` naming `delivery`.

- `queue`: the message is durably queued and started as the next turn once the active run
  ends; the reply is `pending`. Unchanged from a send without the field, byte for byte.
- `steer`: the message joins the RUNNING turn. It is written as a `Steer` record (synced
  before the reply) and the model sees it as a user message at the turn's next step boundary,
  never between a tool call and its result; a final answer given while it is pending is
  followed by one more step to render it, within the run's step and budget limits. The reply
  is `active` naming the RUNNING run (the run that carries the message, not a new one). A steer
  acknowledged this way is guaranteed to be in the transcript; it reaches a model in that turn
  only if another step follows (a cancel, error or limit ends the turn first, and the next turn
  then sees it as history). No new turn is started for it. A retry answers `active` while that
  run lives and its `finished` once it has ended. With no run active it is a plain send; with
  the last run PAUSED it is refused `run_paused` (`detail: {kind: "run_paused", run_id,
  reason}`), nothing written.
- `interrupt`: the message is durably queued FIRST (a `queued` record with `delivery:
  "interrupt"`), then the running run is cancelled through the `run.cancel` path (it ends
  `cancelled`; a running tool call is waited on up to its deadline, never killed), then the
  message starts as the next turn, ahead of any plain queued sends. The reply is `pending`. A
  paused run is ended the same way. With no run active it is a plain send. A crash at any
  point neither loses the message nor runs it twice; an unfinished interrupted run is sealed
  `interrupted` on restart.

A session whose last run is PAUSED (awaiting re-authentication) takes no new message: any
`session.send` that is not the paused run's own retry is refused `run_paused` (`detail: {kind:
"run_paused", run_id, reason}`, nothing written), for `queue` and `steer` alike. The caller
re-authenticates and resends the paused run's own `send_id` (which resumes it), cancels it with
`run.cancel`, or sends with `delivery: "interrupt"`, which ends it and starts the message.

`session.send.cache` is OPTIONAL admission-only metadata: `{role?: "primary" | "ephemeral",
ttl?: "5m" | "1h" | "24h"}`. `primary` means an interactive or long-lived session and resolves to
the long tier when the provider supports it; `ephemeral` means a subagent-class session and
resolves to the short tier. The resolved policy is frozen before the run starts, survives queued
submissions, and is ignored on resume because the prior frozen render config is authoritative.
`"24h"` is accepted only on OpenAI's own API-key routes (Responses and Chat), where it renders
`prompt_cache_retention: "24h"`; the ChatGPT/Codex backend rejects that field with HTTP 400, so
there, and on every other provider, the send is refused at admission.

`session.send.context_management` is OPTIONAL and picks how the session stays inside the model's
context window (design and rationale: `docs/context-management-design.md`):

- `"none"`: no management. A run that reaches the window ends with a context-overflow error.
- `"compaction"`: broca-native summarise-and-continue. Accepted by the parser, and refused at
  admission (`context_management_unavailable`) until the mode exists. Never run as `"none"`.
- `{"transform": "<module id>"}`: a context-management value broca does not serve. It still
  parses, because older `RunStart` records carry it. A send naming it, a later send inheriting
  it, and a resume of an episode frozen onto it are all refused by name with
  `context_management_unavailable` before anything runs, rather than run as `"none"`.

Any other value is refused as `invalid_params` naming `context_management`. Unknown FIELDS on
`session.send` are still tolerated; only this field's value is strict.

The session's FIRST episode binds the value, and it is frozen onto every `RunStart`
(`config.context_management`). A later send that omits the field inherits it; one carrying
the same value is accepted; one carrying a different value is refused permanently with
`context_management_changed`, naming both values. A resume always runs under the frozen
value and never reads process configuration. Every refusal happens before `RunStart`, so a
refused send leaves no episode. A send that queues behind an active run is admitted when it
drains, so its refusal happens then (recorded as a failed queued admission), exactly as a
queued changed system prompt is.

With the field omitted on a session's first send, the first match wins:

1. `broca.jsonc` `context_management.projects["<project_root>"]`, keyed by the session's
   resolved storage `project_root` (the inventory's existing root or stamped workdir for
   a new scoped session; a trailing `/` is ignored);
2. `broca.jsonc` `context_management.default`;
3. `"none"`.

```jsonc
{"context_management": {
    "default": "none",
    "projects": {"/workspace/scratch": "none"}
}}
```

A malformed section, a `"compaction"` default, or a `{"transform":...}` default refuses boot
by name. Changing `broca.jsonc` later never changes a session that already bound its value.

A session whose last `RunStart` carries no `context_management` binds its next new episode to
`"none"` and freezes it; `broca.jsonc` defaults never reach it, and a differing explicit value
is refused like any change. A resumed episode of such a session runs as `"none"`.

`session.send.model.variant` is an OPTIONAL reasoning variant name, sent inside the `model`
object as OpenCode sends it: `"model": {"provider": "openai", "model": "gpt-5.6-luna",
"variant": "xhigh"}`. There is no top-level spelling. The variant travels through the queue,
is recorded on the run's `RunStart` config (`variant`, absent when not sent), and a resume
keeps the recorded value. A different variant under a reused `send_id` is a different request,
reported as a changed `variant`.

**Which names a model accepts** comes from its catalog `capability.reasoning_options` (fusiform
serves models.dev's `reasoning_options` verbatim), derived exactly as OpenCode derives its
variants (`packages/core/src/models-dev.ts`, `reasoningVariants`):

- an `effort` entry gives one variant per string value, in order, plus `none` first when the
  model also has a `toggle` and its family has a toggle mapping; a `null` inside `values` is
  never a variant;
- otherwise a `budget_tokens` entry gives `high` (half the maximum, at least the published
  minimum) and `max` (the smallest of the published max, the model's output limit minus one,
  and 31,999);
- otherwise a lone `toggle` gives `none` and `thinking`;
- an entry type broca does not know contributes nothing and is logged once (`broca.catalog`).

**What each name sends** is decided by the wire family (`crates/broca-provider/src/variant.rs`,
following OpenCode's `settingsForEffort` / `toggleVariants` / `settingsForBudget`):

| Family | effort `<e>` | toggle `none` / `thinking` | budget `<n>` |
|---|---|---|---|
| Anthropic Messages | `output_config: {effort: <e>}`, plus `thinking: {type: adaptive, display: summarized}` on Claude 4.6 and later | `thinking: {type: disabled}` / `thinking: {type: adaptive, display: summarized}` | `thinking: {type: enabled, budget_tokens: <n>}` |
| OpenAI Responses and ChatGPT codex | `reasoning: {effort: <e>, summary: auto}`, `include: [reasoning.encrypted_content]` | none | none |
| OpenAI Chat and compatibles | `reasoning_effort: <e>` | none | none |
| Gemini (Code Assist: see below) | `generationConfig.thinkingConfig: {includeThoughts: true, thinkingLevel: <e>}` | `{includeThoughts: false, thinkingBudget: 0}` / `{includeThoughts: true, thinkingBudget: -1}` | `{includeThoughts: true, thinkingBudget: <n>}` |
| Bedrock Converse | none | none | none |

A variant replaces the Anthropic OAuth default `thinking: {type: enabled, budget_tokens: 1024}`.
The resolved parameters are frozen into the run's render config at admission; a resume renders
from the frozen value and never re-reads the catalog.

**Refusal.** A variant the model does not define is refused at admission, before anything is
written, naming the model and its variants: `model 'anthropic/claude-opus-4-6' does not define
variant 'max'; its variants are [none, low, medium, high]`. A model whose options are `null`
(or not served) or `[]` defines no variants, so any variant is refused. These permanent
`session.send` refusals use error code `variant_unsupported` and structured detail:
`{"model":"anthropic/claude-opus-4-6","variant":"max","defined":["none","low","medium","high"]}`.
`defined` is empty when the model defines no variants. The human message keeps its existing
`open failed: run resolution failed: ` prefix and wording. No `RunStart` or other record
is written, and no provider request is made. Also refused (as `open_failed`, not
`variant_unsupported`): an Anthropic budget variant when the output cap does not exceed the budget. A ChatGPT `-pro` id is not a variant: it resolves
only if the catalog lists that exact model, and maximum reasoning is `model.variant: "max"`.
Omitting the variant renders exactly what the same send rendered before variants existed.

**Code Assist (Antigravity) renders a variant as a backend tier.** Which levels a model
accepts still comes from its catalog row; fusiform serves alias rows for several Antigravity ids
(`antigravity-gemini-3.8-flash` carries `gemini-3.8-flash`'s facts). How a level is sent is the
family's: Code Assist selects the backend id published in `route.reasoning_wire_ids`, so `high` on
`antigravity-gemini-3.8-flash` sends `gemini-3.8-flash-high` with that tier's thinking settings,
exactly as if the caller had spelled the tier in the id. With no variant the bare id keeps its
default tier (`gemini-3.8-flash-medium`). A level the row defines but the live route has no tier
for is refused with the same `variant_unsupported` code and detail, naming the tiers the route
has in the message. For example, if a row defines `minimal` but Code Assist has only `[low, high]`
tiers for that route, the refusal identifies that route limit rather than claiming the catalog
lacks the variant. `defined` still lists the model's variants, not the route's tiers. A level the
row does not define is refused as above.

A variant is also refused with `variant_unsupported` if the id has no catalog row (its variants
cannot be known), or the Code Assist route has no tiers for the model (it cannot apply the
variant). The message identifies the missing catalog row or route tiers; `defined` is empty
without a catalog row and otherwise lists the catalog-defined variants. A fresh send without
a variant still requires a published tier map and default; no suffix or bare-id fallback is
guessed when those route facts are missing.

`antigravity-gemini-4-argon` defaults to `gemini-4-argon-medium`; its `low`, `medium`, and `high`
variants select the corresponding native tier ids. Argon tiers use uppercase `thinkingLevel`
(`LOW`, `MEDIUM`, `HIGH`) with `includeThoughts: true`, no `thinkingBudget`, and no `toolConfig`.

### 4b. `classifier/v1`: System One decision models

broca also serves the `classifier/v1` role (`cortexkit-role-classifier`, with the
batch rules of `cortexkit-model-batch`) from the same management surface, for the
decision models it routes: TypeSafe's `typesafe/jev-1.13.0`, Cloudflare Workers
AI's `cloudflare-workers-ai/@cf/cloudflare/clef` and `.../clef-flash`, and OpenAI's
`openai/gpt-6-luna-decisions` in this build.
The manifest lists `classifier/v1` in `capabilities.provides`.

- **One route, one role.** A `route.open` declaring `role_versions
  {"classifier": "v1"}` is a classifier route. On it `role.describe` answers the
  classifier describe (`{role, limits, models}`, its models resolved from the live
  catalog) and `classify.run` is served; every llm-runner op is refused
  `unknown_method`. A route without that declaration is an llm-runner route and
  refuses `classify.run` with `unknown_method`. A bind declaring both `classifier`
  and `llm-runner`, or a `classifier` version other than `v1`, is refused at bind
  with `role_versions_unsupported`.
- **No session.** A classifier route names no session: nothing opens a session,
  takes a lease, or touches the session store. Each call runs on its own task,
  capped per process and per model, and holds none of the connection's request slots.
- **Work scope.** `classify.run` requires a daemon-stamped run scope (`agent_id` and
  `run_id`) or flow scope (`flow_id`), otherwise `scope_required` before any write or
  provider call. `reserved:basal` requires a flow, even when it presents a run stamp,
  otherwise `flow_scope_required`. A batch freezes its first attribution; a retry
  under another scope gets `batch_scope_mismatch`, without changing its ceiling or
  stored outcomes. A flow is never attributed to its owner agent. `role.describe`
  remains available without a work scope.
- **Durable batches.** `system-one.db` under the state root records each admitted
  batch and every item outcome before the reply, and is the authority for them.
  Batches a dead process left `running` are released at startup. A batch and its
  outcomes are deleted seven days after first admission. The file is excluded from
  Engram backups.
- **Metering.** Each paid item commits an immutable measurement intent alongside its
  outcome, then publishes it to `run-index.db`'s `export_facts` before replying. Startup
  republishes pending intents idempotently. These sessionless facts carry
  `kind: "system_one"`; existing session facts keep their bytes and ids. Only reported
  tokens enter `usage`; estimates and prices stay inside batch ceiling accounting.
  Consumers must deploy kind-aware decoding before these facts are produced.
- **Health.** Channel-0 `health.check` metrics include `system_one`, with the in-flight
  batch gauge, answered/refused batch calls, newly recorded answered/failed items,
  nanodollars of batch-accounted spend newly exported per model since process start,
  and the last provider error code per provider. This spend uses frozen ceiling rates
  (and input estimates when unreported), not Astrocyte's pricing of measurements.
  It contains no item content. Replayed facts and
  served cached outcomes do not increase item or spend counters.
- **Catalog.** A row whose `model.kind` is `decision` is a classifier; another
  serving kind, `classifier`, is accepted too, as defined by the classifier contract.
  Any other stated kind, or no kind, is refused `model_not_classifier`; a model the catalog
  does not list is `model_unknown`.
- **Routed models.** A live routing readiness check for consumer `broca.classifier`
  adds `routed_models: {}` to describe. When a new `classify.run` omits `model`,
  routing picks the cheapest eligible model with a decision grade (`iq: 1`). A
  supplied model is a strict routing pin without fallback. One durable model,
  account and decision is frozen before provider dispatch. Replies name the model
  actually used and its `decision_id`, including on replay. Routing failures never
  fall back to a fixed credential. See [System One routing](system-one.md) for
  retry, admission and report recovery rules.
  The OpenAI Decisions adapter still has its own serving gate. Unlike
  `classify.run`, `llm_classify` requires a model pin to identify the provider
  before checking consent to send content or spend money.
- **Providers.** Jev is called at `POST https://api.typesafe.ai/v1/systemone` with
  the vault credential `apikey:typesafe`. Clef and Clef-flash are called at `POST
  https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/@cf/cloudflare/{model}`
  with `apikey:cloudflare-workers-ai`, whose vault record carries the account id
  beside the token; their answers are read from the `result` member of Cloudflare's
  reply envelope. OpenAI Decisions uses `POST https://api.openai.com/v1/decisions`
  with `gpt-6-luna` and the vault credential `apikey:openai`, resolved through
  row-based credential lookup like the other System One credentials.
  `providers.<id>.base_url` (`typesafe`, `cloudflare-workers-ai`, `openai`)
  replaces the corresponding base. The OpenAI override also applies to chat calls
  using that provider id. Decisions requests disable prompt caching with explicit
  mode and no marked boundaries, so frozen input-only billing remains correct.
  Unexpected nonzero cache usage warns without discarding answers; cache counts
  never enter the classifier reply. OpenAI 429/5xx are transient, 401 is
  `auth_required`, and other 4xx are permanent; its diagnostic error code stays
  in the message and provider health metric, and `Retry-After` is preserved.
  Workers AI errors map by their `errors[].code` first: a
  daily allocation used up (3036) or an account not entitled (5035, 5016, 3023)
  ends the whole batch. Routed batches retain account errors with their frozen
  choice, subject to the single
  pre-answer reselection described in [System One routing](system-one.md).

## 5. Reliability model (the egress reality)

subc module→client egress is best-effort `try_send` into a 64-deep buffer; a full client
queue **CLOSES** that client (verified subc-core router.rs/server.rs). So **"reliable
control lane" = DURABLE + REPLAYABLE**, never guaranteed-live — a dropped consumer
**re-subscribes from its last cursor and replays**. **Display = live-only + lossy**
(coalesce/drop + `DisplayGap`; no per-token durability).

### 5.1 Display loses bytes two ways, and only one of them announces itself

`DisplayGap` is not the enumeration of display loss, it is one member of it.

1. **Ring lag** — the subscriber falls behind, broca drops deltas and delivers an in-band
   `DisplayGap { dropped }`. Self-announcing: the evidence is in the stream.
2. **Stream end** — the subscription ends (detach, route teardown, reconnect) and a new one
   begins at the **live edge of the display ring**, which bears no relation to the client's
   last processed byte. **The amount lost is unbounded and unrelated to how long the reattach
   took**: it spans everything from the client's last delta to the current write position,
   including deltas that were already buffered before the disconnect. Reasoning "the
   reconnect was fast, so little was lost" is the wrong model. And **the new stream contains
   no evidence that anything preceded it** — there is no marker broca can send, because the
   gap exists *between* two subscriptions rather than within one, and the engine cannot know
   whether a client that reattached is the same renderer resuming or a fresh one starting.

**Why this is a correctness requirement and not a fidelity nicety.** Concatenating display
text across a stream end is not merely lossy at the byte level — it is corrupting at the
*parse* level. An unterminated construct before the hole can adopt a delimiter from after it
(an unclosed code fence closing on a later fence), so the render is **wrong rather than
short**, and a reader cannot tell anything is missing. Any lossy lane feeding a stateful
parser converts subtractive loss into structural corruption; that is a property of the
pairing, so neither side's tests find it alone.

**What a client must do.** The only witness to a stream-end gap is the client's own reattach.
That imposes a *structural* requirement, not a behavioural one:

- **The reattach must be observable to whatever holds the render state.** A transport layer
  that reconnects transparently is behaving correctly for every other purpose — reattach is
  genuinely irrelevant to dispatch, ordering, and control-lane delivery — so a layer boundary
  drawn on any other axis will cut this event off. It has to cross a boundary it has no other
  reason to cross, which is why this is easy to violate without making a mistake.
- **Mark when a run is in flight, not when you hold partial text.** The second rule is exactly
  wrong in the case where the loss was total: a reattach during an active run with an empty
  live tail is indistinguishable from one where everything streamed so far was lost. Marking
  under that uncertainty is the honest reading.
- **Do not mark an idle-session reattach.** There is no torn construct to repair, and a false
  gap in unbroken prose is its own fidelity bug.
- **Evaluate the in-flight test against state as of the disconnect, not after processing the
  resumed stream.** The control replay may carry the run's own terminal, which would
  retroactively classify a torn run as an idle one and skip the mark in exactly the case that
  needed it. This is an ordering property rather than a state one, so it is invisible at the
  call site: a client whose guard happens to sit before its resubscribe is correct by
  accident and stays correct only until someone reorders the two.

**The repair is guaranteed only on a completing run.** The control lane's `AssistantMessage`
carries the authoritative text and replaces the streamed tail wholesale, so on a normal run a
marker is visible only for the seconds between reattach and that message. A run cancelled or
failed mid-step never produces one — the assembled message is projected from
`StepEnd`, which that run never reaches — so **the torn tail is what the transcript
keeps, and the marker is permanent**. Do not size the effort by the seconds-long case.
Display is a latency optimisation, never a source of truth — which is also why the lane will not be made replayable: durable
per-token replay would let a slow subscriber backpressure the single-writer loop, and the
reliable lane already carries the complete text.

## 6. Multi-client fan-out + per-subscriber backpressure

subc is 1:1, with no broadcast, so broca owns the fan-out. Per subscriber, the display lane is
lossy (coalesce or drop, then `DisplayGap`) and the control lane is bounded, then detaches the
route so the subscriber resubscribes from its cursor. A slow subscriber never blocks the run or
co-subscribers, and control units are never silently dropped.

## 7. The control stream and its cursor

The control stream is projected from the session's records, addressed by the cursor
`(wal_seq, sub_index)` described in §4a. Because it comes from the records, not process memory,
a cursor survives the actor closing and reopening the session. How an attach replays and then
streams live without a gap or a duplicate is in [session actors](session-actors.md) §5.
`from:"live"` replays no history and waits for future units. Display events are live-only: a
finished turn shows assembled text, not re-typed tokens.

## 8. Replay scope

`from:"start"` (or a cursor) replays the projection from that point, then streams live. The
replay is semantically equivalent to the original live stream, not event-identical: display
tokens are not reproduced, but step boundaries, finish reasons, tool names and error detail are.

## 9. Subscribe routing

`session.subscribe` is a **dedicated long-lived stream route**, never multiplexed with
`cancel`/`status`/`read`. `from:"live"` is future-only and remains quietly attached on an
idle or terminal lineage until a later run publishes units (§16).

Stamped `route.bind` authorization runs outside frame ingress. Until that check succeeds
and broca acknowledges the bind, its channel/epoch is not published: a request arriving
early is dropped as unbound (or stale if another epoch is still published), not queued
for dispatch after authorization. That never drops a real request, because the daemon
forwards nothing on a route before the module acknowledges its bind: `route.open` waits
for the module's `RouteBindAck` before it returns the channel and epoch to the client,
and while the bind is pending the daemon refuses any request on that channel with
`unknown_channel` instead of forwarding it. broca records the route as bound before it
sends `RouteBindAck`, so a client's first request after the acknowledgement finds it.

A route `GOODBYE` closes only the channel/epoch it names. A pending replacement can be
canceled without closing the published route or its stream, and closing the published
route does not cancel authorization of a different pending epoch. After the replacement
is accepted, a `GOODBYE` naming the old epoch is stale; accepting the replacement is what
ends the old epoch's subscribe stream.

A stream unit whose frame would exceed the transport frame limit (4 MiB including the header),
or that cannot be encoded at all, is never put on the wire. The stream ends with a typed
`subscribe_unit_undeliverable` Error and no `StreamEnd`. Its detail names `reason`
(`too_large` or `unencodable`), the unit's control `cursor` (`{wal_seq, sub_index}`, or null for
a display event), and for `too_large` the `frame_bytes` and `limit`. The served bytes are pinned
by `crates/broca-module-serve/tests/golden/subscribe_unit_undeliverable.too_large.json`.
For a control unit, resubscribing from before its cursor reaches the same unit again.

Route requests authorize the caller before method parameter validation or migration
checks. In particular, an unauthorized `session.subscribe` reports the authorization
refusal even when its parameters are invalid. A replacement subscription cancels the
active stream only after authorization and validation both succeed; a refusal leaves
deliveries on the original stream correlation.

## 10. Admission barrier + early failure

Idle `session.send` returns `{state:"active", run_id}` only after the run's `RunStart` is
synced. Model and auth resolution happen before `RunStart`, so a resolution failure is a
typed `session.send` error, not a ghost run. A failure after `RunStart` is a short terminal
run answerable via `run.status`, with its error recorded.

### 10.1 A vanished project root refuses WRITES and still serves READS

If a session's `project_root` no longer exists on disk, the split is by operation class,
not by session:

| refused at admission | still served |
| --- | --- |
| `session.send`, `session.import` | `session.read`, `session.subscribe`, `run.status`, `run.cancel`, `session.retract` |

The refusal is a typed `invalid_project_root` naming the exact path. Recreating the
directory revives the session; nothing about it was destroyed.
The exact `broca-unscoped-v1` storage namespace is exempt: it is never a filesystem
directory, even if a directory with that name exists in the process's working directory.
Legacy directory-root sessions retain the guard.

**This is deliberate.** Writes create durable state under a key derived from the bind
triple, and appending new state beneath a root that is gone (or running tools there) is
how a lineage ends up describing a place that does not exist. Reads have no such
problem: the session store does not live under the project root, so a transcript outlives
the directory it was recorded in.

**What a consumer must not infer.** `!root.exists()` does NOT mean the session is dead
or unopenable. It opens, pages its full history, and only continuation fails. A UI that
treats a missing root as a corpse and offers to prune it has the advice backwards: the
transcript still reads, and forgetting the record is then the only action that loses it.


### 10.2 Open failures and their classes

A session that cannot be opened answers `open_failed`. When the producer knows the remedy, it
adds `detail.class`: `transient`, `permanent`, `auth_required`, or `context_overflow`.
Admission and run-resolution failures use `RunError::admission_error_class`; for example, an
OpenAI Chat provider that cannot honour cache preferences returns `permanent`. An open that may
succeed when repeated is not answered `open_failed`: it takes the contract's retryable code
`transient`, with `detail.class: "transient"` and `detail.cause` naming the cause
(`key_unavailable` while the session store's key is still loading after a start), because
callers decide retries by code alone. Corruption and errors whose causes were erased remain
unclassified: `detail` is absent. Absent is not a retry recommendation; consumers retain their
fallback only for absent classes. The served bytes are pinned by `open_failed_permanent.json`
and `open_transient.json` under `crates/broca-wire/tests/golden/`.

A log holding a record kind or a must-understand field this build does not know was written by
a newer broca. Release placement does not see record kinds: the state root's feature floor
names only the storage format, so it refuses a build older than the session store and nothing
finer. A new record kind or must-understand field therefore ships in a reader release before
any build writes it, so the build one release back reads every log a rollback meets.

When the session is already open, `session.subscribe` attaches and then replays its records in the
stream; a record it cannot read there ends the stream with `subscribe_replay_failed`.

## 11. Submission lifecycle + durable queue + retract

Broca declares `sender_succession`. A plan sender's optional `succeeds` names one
predecessor module. For keyed retries and `send.status`, that predecessor's durable
send record is checked before the caller's own record, even after a refresh removes
the predecessor from the plan. A conflicting retry is `send_id_reuse`, never a new
send. Succession is one hop and grants no delivery, cancellation or withdrawal
permission. The latest accepted refresh supplies the sender policy immediately.

- **Idle send** → starts a run (active, run_id).
- **Running send** → durably queued as a `QueuedSend{submission_id, input, …}` record, which
  carries the submission's identity, not just the message. State "pending".
- **Retract** → a durable `Retraction{submission_id}` record. The actor is the session's only
  writer, so a retraction and a start that race are ordered by whichever record it writes
  first (remove-if-pending vs `already_started`).
- **Consume** → the actor writes `RunStart{submission_id, run_id, …}` referencing the
  submission. A queued send whose admission fails is closed by a `SendEnded` record instead.
  A drained run's id is `"run-" + submission_id` (the submission id itself carries a
  `sub-` prefix, so the id reads `run-sub-…`). The string formula is diagnostics-only:
  consumers correlate a queued send to its drained run by matching `submission_id`,
  never by parsing the run id.
- **Recovery is unambiguous:** the pending set is the queued sends with no retraction, no
  consuming `RunStart` and no `SendEnded`. A crash before the consuming `RunStart` leaves the
  send pending (started on re-open); a
  crash AFTER ⇒ that run is active/resumable, never double-consumed. FIFO; each queued
  prompt becomes its own episode.

Broca declares `retract_own_send` with `retract`. On a session with a recorded owner,
`session.retract {submission_id}` also admits the submission's recorded daemon-attested
sender: the delegated sender for an ordered send, the attested principal for an ordinary one.
Request-body sender claims never grant access. Missing,
unverified or ambiguous attribution grants no exception to the owner's existing path.
Answers and durable tombstones are identical to an owner's withdrawal: `retracted`,
`already_started` or `not_pending`. A delivered steer is `not_pending`. This does not
extend `run.cancel` by itself, or change sessions without a recorded owner.

Plan-named senders may share a `send_id` without sharing a submission. The first sender
keeps the legacy run ID; a later delegated sender colliding with another recorded sender
uses `<legacy-run-id>-sender-<digest>`, where `digest` is the lowercase, 64-character
SHA-256 of the fixed bytes `v1\0reserved\0` followed by the daemon-stamped reserved
principal's `module_id` in UTF-8 (`\0` is a NUL byte). This versioned identity encoding
does not use serde or any other principal fields. Its submission ID remains `sub-`
followed by that run ID. Retries and `send.status` resolve the same ID from durable
attribution, including after restart. Owner IDs and the first episode's recorded model,
tool configuration, owner scope and plan manifest are unchanged.

## 12. Session actors

One in-process actor per open session, reached through a registry; every `session.*` and
`run.*` op for the session goes through it. The registry's slot states, the actor's lease and
when it closes, and how races between a send, a close and a reopen resolve are described in
[session actors](session-actors.md) §2-3. A subscriber across a close has its route closed and
resubscribes from its durable cursor on re-open. Durable queued work also wakes the actor, even
when the latest run has already finished, so a crash cannot leave the queue idle.

## 13. Cancel token + provider watchdog (core change)

`run.cancel` admits the session's owners. Broca also declares `run_cancel_by_interrupt`:
an attested named sender whose current effective grant has `interrupt: true` may cancel
with omitted `at` or `at: "now"`, but never `step_boundary`. The latest accepted refresh
controls this grant even before its execution plan applies; removed senders and callers
named only in another session are refused `scope_owner_mismatch` without writes. Sender
cancels use the same `"ack"` / `"not_active"` replies and `cancelled` terminal as owners,
with no sender field added to either. The accepted sender stop is durable before `"ack"`,
so recovery finishes the cancellation without dispatching an in-flight call again.

Omitted `at` (or `at: "now"`) retains immediate cancellation:
the model stream is cancelled and sent tool calls retain the existing at-most-once recovery
rules. `at: "step_boundary"` finishes the in-flight model step and its entire tool batch,
records the results, and writes no further `StepStart` before `RunEnd{Cancelled}`.
Discovery advertises `run_cancel.at: ["now", "step_boundary"]` under the existing `run_cancel`
capability group. An unlisted or non-string `at` is refused as `invalid_params` naming `at`,
with no writes. Both modes answer the JSON string `"ack"` or `"not_active"`.

A boundary request is durable (a `CancelRequest` record) before `"ack"`. Duplicate boundary requests are idempotent; a later immediate request durably
escalates it. Recovery never repeats a sent call or starts another model step: it completes
recorded tool work using the existing dispatch evidence, or repairs ambiguous calls with
their unknown-outcome results. A model response lost in a crash is not regenerated. A terminal
the current step reaches first, including `completed`, `max_steps` or `error`, still wins.

A paused run is already at a boundary and is cancelled without another model step. A queued
submission is not an active run: cancellation answers `"not_active"`; use `session.retract`
to remove it. An active tool batch awaiting human approval still owns that wait, so boundary
cancellation waits for the decision/expiry and the batch's results; an immediate cancellation
can end the wait. V2 Setup, notices and rides finish within their call budgets before the next model request.
They do not create durable provider-wait states.

The provider-call watchdog is separate from caller cancellation. A timed-out provider attempt
ends `Interrupted`, not `Cancelled`. A writer whose sync never returns remains a process-level
failure for the supervisor, not a bounded close-out; ownership is not released early.

## 14. run.status states and busy-vs-stale

Startup recovery seals runs left in flight by a previous process as `interrupted`, with
an optional `error` diagnostic on `run.status`: `{class: "transient", provider_code:
"previous_process_ended_unsealed", message: "the previous broca process ended without sealing
this run: host crash, kill, or power loss"}`. This is the `RunEnd` record's error, not a new
terminal reason. The control lane projects the diagnostic as `ControlUnit::Error` before the
`interrupted` terminal; that final reason remains authoritative. Export
facts carry `terminal_reason: "interrupted"`, but no interruption diagnostics. Graceful seals
keep their existing behavior and omit this diagnostic.

- **State enum:** `Idle | Active{run_id, step} | Paused{run_id, reason} | Interrupted{run_id} |
  Terminal{run_id, reason} | Error{run_id, detail}`. `run.status(session)` resolves the
  in-process active run first, then the session's records.
- **Timed account pauses:** the root `broca.jsonc` setting `max_same_account_wait` defaults to
  `15`. A non-negative integer caps the wait in minutes; `0` never pauses, and `"max"` removes
  the cap but still respects cache expiry. Other values refuse boot by name. The obsolete
  `account_limit_pause` key is ignored with one boot warning naming the replacement.
  The setting is loaded once at boot; changes take effect at the next restart, like other
  `broca.jsonc` settings. Each fresh run freezes only consent (`setting != 0`) on its
  `RunStart`; a new pause uses the executor's in-memory cap. For a source-classified transient
  account limit,
  `wake` is the latest blocking reset/retry floor (including any scheduling jitter),
  `cache_expiry` is the last 2xx attempt's send time on that account in this session plus the
  run's frozen cache TTL (explicit 5 minutes or 1 hour; unknown means 5 minutes), and
  `cap = now + setting` (`"max"` has no cap). Pause only if
  `wake <= min(cache_expiry, cap)`. A 2xx response counts even if its stream did not complete;
  a refusal cannot refresh the cache. No accepted response on this account means no pause.
  A declined wait ends with the existing transient `account_limit_exhausted` error and reset
  time, allowing Core to revive the worker and routing to choose an account. A committed pause
  replays its recorded wake without re-deciding these bounds and resumes on the same frozen
  selection, never through a routing account switch. `run.status` reports `state: paused`,
  `reason: account_limit`, `pause_reason: account_limit`, the original structured error and
  optional `resume_not_before_ms`. The durable subscribe `paused` control carries the same
  floor after its barrier; the subscription stays attached for ordinary resume progress.
  `session.head.last_run_state` remains reason-only, with no new ordinal or deadline member.
  An idempotent resend returns the pause/current continuation rather than scheduling a second
  execution. Unrelated sends and steers receive `run_paused`; cancel remains available.
  Newly stale reset refusals retain the terminal path and increment the always-present lifetime
  health metric `account_limit_stale_resets`. `health.check.metrics.account_limit_pause_declined`
  always reports lifetime counts for `disabled`, `no_accepted_response`, `cache_expires_first`
  and `beyond_cap`; each decline logs reset, wake, cache expiry, cap and reason at info level.
- **Run ids survive restart:** run-keyed ops find a run through the session store's id index,
  and a resumed run keeps its original `run_id`.
- **Busy-vs-stale:** `session.send` on a live-active lineage returns `session_busy{run_id,
  state}` vs `lease_stale`.
- **Reading a terminal promptly:** `run.status{run_id}` resolves the live actor's state first,
  then the session's records. A consumer that needs the terminal at the moment it is written
  reads the subscription stream.
- **`TransformUnavailable` is read, never written:** older records carry it, and readers report
  it (`run.status`, `session.read`), but this build never ends a run with it. It is terminal, not a pause: a pause preserves a resolvable nonterminal
  cause and the same run rather than sealing the episode.
- **Active model-step progress:** run-keyed `run.status` can include an optional `step` object
  `{step_id, started_at_ms, attempt, attempt_started_at_ms, last_content_at_ms?,
  max_content_gap_ms?, max_content_gap_ended_at_ms?}`. It is a cheap snapshot of the live actor's
  in-memory model call, never inferred from records. Timestamps are
  Unix milliseconds; `attempt` is zero-based and its content progress resets on reissue. The
  longest gap is measured only between consecutive real-content events, and its end time is the
  later event's timestamp. With fewer than two content events both gap fields are absent.
  No in-flight model step means no object, and no real content yet means no content timestamp.
  The real-serve fixture is `crates/broca-wire/tests/golden/run_status.active_with_step.json`.
- **A queued send's run id (broca extension).** A queued send's reply names only its
  `submission_id`, `sub-<run_id>`; the run it would start takes that `run_id`. Core derives the
  run id from the submission id and polls `run.status` and `session.read` on it, so broca answers
  that id for as long as the send exists, even when the send never becomes a run of its own (a
  send using the `delivery_order` group can be delivered into the run already going). The
  llm-runner role contract does not define these answers: there an id naming no run answers
  `unknown`, and llm-runner will not adopt them. Core (the
  session's owner) needs them until it tracks each send by its `send_id` instead of by this run
  id. The answers come from the session's records alone, so they are the same on a live session
  and a closed one, before and after a restart:
  - while the send is queued: `{state: "pending"}`
    (`run_status.provisional_pending.json`);
  - delivered into a run X that was already going (as a steer, with a tool result, or as a
    turn-scoped system message): X's whole status, exactly as `run.status {run_id: X}` answers
    it, plus `delivered: {as: "step", ref: X}` (`run_status.provisional_delivered_steer.json`,
    taken after a restart). `run.cancel` on the send's run id answers `not_active` and never
    cancels X;
  - started its own run (`idle_start`, or the plain queue): that run's status, with no
    `delivered` (`run_status.provisional_started.json`);
  - ended undelivered: `{state: "superseded"}`, `{state: "expired"}` or
    `{state: "retracted"}`, for good.
  A re-send of the queued send's `send_id` answers the same way (§9 of the role contract): once
  delivered into X it answers X's state with `run_id: X` and `delivered: {as: "step", ref: X}`;
  once an ordered send started its own run, that run with `delivered: {as: "turn", ref: <its
  run id>}` (a plain queued send's re-send carries no receipt). Both are rebuilt from the
  records (the `Delivery` record, and the run's `RunStart` beside its `DeliveryOrdered`), so a
  re-send after a restart never answers `pending` again.

### Run waits on the new storage

Sessions kept by `broca-session-store` (see "Temporary storage cutover" above) additionally
report llm-runner's optional `awaits` object on
`run.status`, `session.head.last_run_state` and subscribe status/paused controls. Legacy
storage omits it, and broca does not yet declare the `run_awaits` capability. A held tool is
`active` on all three surfaces with `party: runner`, `reason: tool_held`. The internal durable
tool-held pause records where to continue the same held tool batch after a restart; it is
not a caller-owned pause. Account-limit and
restart pauses wait on the runner; sign-in pauses wait on a person. Known automatic resume
times appear as `next_ms` only while future. The old `reason`, `deadline_ms` and
`resume_not_before_ms` fields retain their meanings.

In-step provider retry waits are actor-held memory, never durable records. They appear once
the wait has lasted a second, or immediately when a delay of at least a second is known.
Subscribe carries these snapshots as display events, `event.type: run_status`, so they have
no durable cursor and are never replayed as history. A subscriber joining during a wait gets
the current display snapshot, rebuilt from the actor-held retry slot rather than a captured
frame; a cleared wait or a passed `next_ms` cannot reappear on replay. Older readers decode
an unfamiliar display event as
`DisplayReset`, clearing their buffered token text. Broca emits retry snapshots only after
abandoning a failed generation and before another attempt produces text. An old reader's
`DisplayReset` therefore clears only abandoned or empty text, never a live generation.
The snapshot is cleared before the next attempt and when the step ends.

Terminal errors carry the same ending `awaits` on status, head, error/run-finished controls
and `run.result`, using the recorded run-end time. Other terminals omit it, and nonterminal
results never carry it. When a stored error has no code, the shared served-error boundary
supplies one from its caller-facing class: `transient`, `auth_required`, or `context_overflow`;
all other classes use `provider_error`. This applies on both storages and does not rewrite
stored records. Existing codes and finer provider diagnostics stay unchanged. Error endings
on `broca-session-store` use that normalized error for `awaits`. Terminal reason fields keep each
surface's existing encoding.
An account display name is the frozen credential key's explicit label, else a cached vault
row's nonblank email, never an opaque account or credential ID. Quoted messages retain their
known provider/runner origin; unknown provenance and deliberately withheld WebSocket text
remain absent.

### Attempt content telemetry

`model_attempt_finished` optionally carries `first_content_at_ms`, `last_content_at_ms`,
`max_content_gap_ms`, `max_content_gap_ended_at_ms`, `content_bytes`, `non_content_bytes`,
`event_types`, `non_content_bytes_after_last_content`, and `events_after_last_content`.
The maximum content gap is the longest interval between consecutive real-content events, and
its end time is the later event's timestamp. Both fields are absent with fewer than two content
events. Provider and model come from the run's `run_started`. These fields are informational
and need no reader feature; older readers ignore them.

Content is explicitly classified per wire family: text, reasoning/summary, function-argument,
and refusal deltas. Lifecycle events, completed item snapshots, signatures, usage, pings and
keepalive comments are not content. Unknown event types are non-content and carry `unknown:true`
in the tally. SSE and WebSocket data bytes count UTF-8 payloads (including JSON envelopes), not
SSE framing; SSE keepalives count comment bytes. Bedrock counts event JSON payload bytes.
The tally holds the top sixteen event types by count, ties ordered by name, plus an `other`
bucket for remaining types. Each entry has `event_type`, `count`, measured `bytes`, and optional
`unknown`. WebSocket ping/pong are exposed by the socket pump without their payload lengths:
their counts are observed but their byte count, and any aggregate containing it, stay absent.
Exact counts are retained until the attempt ends so a late frequent event can enter the top sixteen.

No observed content means absent content timestamps, content byte count and suffix counters,
not synthetic zeroes. Once content is observed, the suffix counters reset to measured zero;
subsequent non-content records increment them. A byte class never observed stays absent.
This telemetry and the attempt log's `last_content_ms` do **not** change the existing idle or
content deadlines, retry policy, or rate-floor policy.

Provider-attempt ordinals are scoped to the episode's step. A refusal resend and a
resume of an unfinished step continue after that step's largest durable attempt ordinal;
live `step.attempt` uses the same offset. These ordinals are not stable global IDs.
Accounting event IDs use `lineage_id:wal_seq`, and timestamps are not join keys.

## 15. session.read consistency + pagination

`session.read` cuts each page from one view of the session. The response's optional
`head: {wal_seq, sub_index, incarnation, digest}` is the last durable control unit covered by
that same view. `session.subscribe {from: head}` therefore
starts strictly after the read snapshot without a race, gap, or duplicate.

An explicit `from_ordinal` is one-based (`0` aliases `1`) and returns the forward range
`[from_ordinal, from_ordinal + limit)`, capped at the transcript's end. With no cursor the newest
page is returned. `next_from_ordinal` is present only when more entries follow. To walk backward,
request `from_ordinal = max(1, first_ordinal - limit)`.
There is no direction field, and `session.read` refuses one with `invalid_params`.

**Leniency is per-operation, not a property of the surface.** `run.status` and
`session.read` are STRICT, because absence there changes the question: a misspelled `run_id`
produces `run_id: None` (SESSION status), and a misspelled `from_ordinal` produces the newest
page instead of the range asked for. Either way the caller gets a well-formed answer about the
wrong subject. `session.send` stays lenient for the opposite reason: it accepts additive optional fields, and a newer caller against an older daemon must degrade
rather than fail every call. Lenient is not silent: the module logs each top-level
`session.send` key it does not read, and each unread key inside `model`, at warn (`broca.wire`),
naming the key and the caller's harness, once per key per process. `model.variant` is read, so it
is never reported as unknown. Before making an operation strict, verify that its
callers do not send unsupported fields and check compatibility tests.

Each entry's `ordinal` is its stored position plus one. Mids of older logs stay `m<position>`;
logs whose first `RunStart` has `compaction_plan_version` use `m<lineage-prefix>.<position>`,
where the prefix is the lineage's first eight hex characters. Never derive a mid from an ordinal.
Native sessions on the new storage mint unique message ids, including after a cancelled step
announced an id but placed no message; converted sessions retain their positional ids and may
reuse that abandoned position for the next message.
Every head/read/history page, even empty, carries `history_version` and `incarnation`. A durable
address-keyed high-water row survives log retention and recreation. An absent log reads as
`unwritten`, without creating a log, lease or history row. Destructive same-lineage recovery
fences reads with retryable `history_rewriting` until its writer has durably finished.
`session.resolve`, tag rows and model-view read pages are not served in llm-runner 0.8.
On the new storage, every llm-runner surface serves `history_version` one above the head's
stored version (`store_exposure::EXPOSURE_EPOCH`), because this build serves replies cleaned of
tag notation where their run cleans them (`EntryFacts::clean_reply`), which earlier builds
served with their tags; nothing stored changes, so an older build opens every session and
serves the raw text at the stored version again, which is what that version meant. The
compaction provider's route carries no history version and is unchanged.

**Display facts (opt-in).** Broca declares `read_display_facts`. On transcript reads,
`include_display_facts: true` also exposes the recorded assistant `completed_at` and readable Responses `reasoning_summaries` indexed by stored content part, never opaque or encrypted reasoning; default message bytes and digests are unchanged.
`created_at` is an opt-in host creation fact on any role: send entries use recorded acceptance (including original queue/steer acceptance), and native assistants use the producing request's recorded send time, never row-write, ingest, finish or serve time. Unknown facts stay absent; `written_at` keeps its existing meaning, and host clock steps impose no ordering against completion.
Rows cached before display-fact timestamps were recorded omit those facts; the nullable columns are added without defaults, backfill or an index in the `store.schema` boot step `message_display_fact_times`.
`include_display_facts: true` adds a stored `client_message_id` to its user entry and
`tool_times` to an assistant entry carrying calls, beside `mid`, never inside `message`.
Calls remain in message order. `started_at` is the recorded dispatch time;
`finished_at` is that call's recorded answer time, not the batch's closing time.
`unanswered` names a recorded closure without an answer (`interrupted`, `cancelled`,
`denied` or `not_executed`). Missing facts stay absent. Display-history tool parts
expose the same times and closure reasons. Default transcript reads and raw history do
not request these timing facts. Reads containing tools nevertheless load the sidecar's
exact native outputs to keep envelope notices out of application message content.
Because broca also declares `call_identity`, each call's opt-in timing item copies its
projected `call_key` and `schema_pin` when known. The key identifies the durable call;
the pin encodes the tool name, schema digest and semantics version frozen at dispatch.
Both read envelopes and display rows
use the same stored strings, not a current tool catalog. A prepared call declined before
dispatch keeps its key but has no dispatch schema pin.

For sessions held by `broca-session-store`, an assistant reply routed to the refusal fallback
carries `model_fallback: {requested, answered}` on a `session.read` entry with
`include_display_facts: true` and on its display-history row. The requested id comes from the durable `RunStart.model`; `StepStart.mid` links the
reply to `StepEnd.fallback.route`, with `served_model` taking precedence for a recorded
provider handoff. Both ids are provider-qualified like the row's `model`, and the answered
id must match the reply's stored origin. No route, missing ids or an answer from the requested
model means no fact; a schedule, usage or model names in text never supply one. This changes
no stored or model-bound bytes, content digests, tags, compaction or message identity.
The fact is a bounded, non-encoded session projection populated while existing append,
open-log load/view and recovery folds pass the evidence. Reads never fetch ledger records
to establish it. A sealed transcript or checkpoint alone lacks that evidence, so a fresh
view may omit an older fact, as may cache eviction; absence means no claim.

**Message marks.** Broca declares `message_marks` and `notice_with_speaks_for`. A scoped
session's attested owner can send `speaks_for: {agent_id, name?}`, where `name` is a
display-only 1–64 printable ASCII byte string and never an identity. Any admitted sender can
send `notice: true` for its own automatic message. The owner may combine both marks on one
send, an agent's automatic message. Invalid marks refuse before opening
a writer. Flow- and run-stamped routes are senders, never owners. Marks are durable
metadata beside the entry: opted-in reads, raw history and display rows expose them;
default message bodies, content digests and `history_version` do not change. A send delivered
alongside a tool result is represented by `notices` facts and `notice_delivered` events;
these carry `speaks_for`, never a redundant `notice` flag.
An identical `send_id` retry compares normalized marks as well as the request: changed
marks refuse `send_id_reuse` with `detail.field` naming `speaks_for` or `notice`, while
absent and `notice: false` compare equal and retain the original answer. `speaks_for` is
compared whole, so a changed, added or removed `name` refuses naming `speaks_for`.
A runner without `notice_with_speaks_for` refuses a combined notice naming `notice`, and a
speaker's `name` naming `speaks_for.name`. The reader of names shipped one release before
the writer. Every stored speaker with a name has a must-understand `named_speaker` field
beside it. After named speakers have been written, rolling back requires
a build with that reader; older builds refuse such a record rather than drop the name.

**Kept notices.** Broca declares `notice_keep`. The session's scope owner may add `keep: true`
to a notice it sends, to say the notice carries the person's own words. `keep` without
`notice: true`, `keep` from any other sender, and `keep` on a send whose delivery order lets it
ride a tool result each refuse `invalid_params` naming the field, before any write; `keep:
false` is the same as absent. The mark is stored with the other marks and served the same
way: opted-in reads, raw history and display rows carry `keep: true`, and the notice's
compaction entries carry `notice.keep: true`, which tells the compaction provider never to drop
it on its own. An unkept notice serializes as before, so its entry digest doesn't change. A
`send_id` retry with a different `keep` refuses `send_id_reuse` naming `keep`. Every stored
kept notice has a must-understand `kept_notice` field beside its marks; its reader shipped one
release before the writer, and older builds refuse such a record rather than serve the notice
unkept.

**Compaction entry attribution.** A marked send's user entries carry `speaks_for`
(`agent_id` and, when sent, `name`) in `session.entries` pages and `compaction.notice`
entries. This applies only when the session's compaction provider lists
`compaction-entry-speaks-for/v1` in its catalog entry. Broca reads that declaration from the
catalog at every Setup and keeps it until the next Setup. An actor that has not sent Setup
yet reads the catalog once for the provider the plan names. Every other provider gets
entries without the member. Attribution is outside every checksum, so it never changes entry
digests, `history_version` or views. On today's storage, entries projected before this
release keep no attribution until their projection is rebuilt.

**Media-only references.** Broca declares `media_refs`. `media: "media_refs"` on
`session.read` or raw `session.history` replaces byte-backed media outside tool results with exactly
the `media_ref` metadata served by descriptors, including decoded-byte SHA-256, MIME and
byte size. Large text, reasoning and tool arguments remain inline, without the descriptors
threshold. Existing `blocks`, `descriptors` and default reads keep their previous bytes.
All tool results, including media-bearing ones, keep inline's flattened text byte-for-byte,
including media markers and text above the descriptors threshold. Tool output has no
multi-part read form under the contract; placement of tool-result media references remains
undefined and is not invented by this renderer.

**Rich UI teaching stamp.** Broca declares `rich_ui_stamp`. A teaching provider's fetched
`system_text.rich_ui` value (`{dialect, catalog_digest}`) is frozen at admission, never read
from caller-supplied metadata or fetched again for later runs. Conflicting stamps refuse
admission as `plan_stale`, with `differences` naming the stamped providers in plan order.
Every assistant display row in `session.history` and the live `run_started` event carry
`rich_ui` when the run has a stamp. `session.head.last_run_rich_ui` describes only the
head's last run and is stored with its start. `session.read` includes `rich_ui` beside
each assistant entry's `mid` only with `include_rich_ui: true`; default reads keep their
previous bytes. All these members are omitted, never null, on unstamped runs, including
older manifests that did not freeze teaching metadata. User, system and tool messages
never carry the stamp.

**Declared tools (opt-in).** `include_tools: true` adds `tools` and `tools_run_id` to the
response: the tool list frozen on the durable `RunStart` of the session's LATEST episode
(`name`, `description`, `input_schema`, and `module` when the episode recorded one), in
recorded order, and the run whose `RunStart` it came from. It is copied from the recorded
tools as written, never rebuilt. The list comes from the same view as the page; reading it
costs an extra setup-part read, which is why it is opt-in. A session with no episode omits both fields (never `tools: []`); an episode admitted
with no tools returns `tools: []`. If the run's recorded tools cannot be read, the read fails
rather than omit `tools`, since omission would claim the session never ran. Without the flag the response bytes are exactly what they were before the
flag existed, and the request omits `include_tools` when false. `include_tools` is a known
field only in that spelling: `ReadParams` stays strict, so an older daemon refuses it with
`invalid_params` instead of silently answering without tools.

## 15a. `session.plan_view`: the applied plan

`session.plan_view {session}` is a read on the new session storage, addressed and
admitted like `session.read`. It returns `{"result": ...}` with the plan that
built the latest run's requests, not a provider-rendered prompt:

- `generation` is the applied plan generation (`0` before a refresh applies),
  and optional `applied_at_ms` is the stored start time of the run that applied
  it (the first run for generation zero). When that run recorded no time, the
  member is absent; its age is unknown, not derived from another event.
- `system.text` is the manifest's system text, byte for byte. Optional `items`
  attribute nonempty text ranges to their `provider` and optional `preset`.
  `start` and `end` are UTF-8 byte offsets, inclusive and exclusive respectively;
  separators belong to no item. Empty and unanswered optional providers have no
  range. If this build cannot reproduce the stored join, it omits `items` rather
  than returning unchecked offsets.
- `tools` contains `name`, `description`, `input_schema`, and `provider` in offer
  order. Optional `tokens: {count, exact}` describes a per-definition estimate,
  using the same definition counter as compaction's `tools_tokens` with
  `exact: false`, not an additional model or provider measurement. The estimate
  excludes operator-selected name aliases, like the as-planned tool list.
- Optional `newest_step` identifies the newest request with a recorded send time
  under that generation, not necessarily the request in flight. It uses the
  stored attempt's `sent_at_ms` and, only when it produced an assistant message,
  `reply_mid` matching the message id returned by `session.read`.
  Undated requests do not displace a dated request. If the applied generation
  has no dated request, the member is absent, even if an older generation did.
- Optional `pending: {generation}` identifies an accepted refresh that has not
  yet applied. It does not replace the applied system text or tools and has no
  `notes` member because broca has not told the model about the pending change.
- `as_planned` is always `true`. It excludes the identity line broca prepends on
  a Claude subscription route and tool-name rewrites to a provider's grammar
  during rendering.

A session admitted with system text and tools on its first send, without a plan
manifest, refuses with `no_plan`; it never returns an empty substitute plan. A
reply that would exceed the read frame limit refuses with `too_large`; it is
never truncated.
Sessions kept on the old storage refuse with `not_served_on_old_storage`.

## 15b. Plan-named readers

Broca declares `app_readers` for sessions kept in the new storage. A plan's optional
`readers` array contains distinct principal strings, matched exactly against the route's
attested module identity, never a request parameter. A malformed or duplicate entry refuses
`invalid_params` with `detail.field: "plan.readers"` before writing. A session kept on today's
storage refuses any plan carrying the member, including an empty list, by that same field.
Omitting `readers` preserves existing read admission.

When `readers` is present, `session.head`, `session.read`, `session.plan_view`,
`session.history`, `run.status`, `role.describe` and `session.subscribe` admit only named
readers, the session owner, the effective plan's compaction provider, and direct local
operator connections. Missing, unverified and unknown caller identities do not match a
reader string. Everyone else gets `scope_owner_mismatch` before operation dispatch; a cold
session obtains its stored view through the shared view cache to read authorization metadata,
but never opens an actor or builds a page for a refused caller.

A durably accepted refresh changes reader admission immediately, even during a run or when
tools and system text must wait for a later prefix or history rebuild. Reader-only changes
alter neither tools nor system text. Each open subscription rechecks its own caller and ends
with `scope_owner_mismatch` when admission is
lost; subscribers still admitted remain connected. The implicit compaction grant follows
the effective plan, not a pending refresh: adding the first compaction provider grants it
read access only when the next run applies that plan.

A reader grant authorizes no send, cancel or retract. `session.send_status` and `send.status`
retain their own-send checks: under a reader policy, only the owner and current plan-named
senders are admitted, with a sender limited to its own sends or the predecessor named by
its current `succeeds` grant. `run.result` and `run.tool_calls` keep their existing rules.
The reader member is stored inside the existing opaque plan and its refresh setup parts;
plans omitting it keep their previous stored bytes.

## 16. Subscribe to a terminal run

Terminal state does not special-case attach behavior. `from:"start"` or a historical cursor
replays every strictly eligible durable unit, including the run's terminal or pause, and then
stays quietly attached for a later episode. `from:"live"` resolves at the attach barrier to
strictly after the current durable head, so it replays no terminal or other history and
stays quietly attached. Idle pinning keeps either stream open; a subsequent `session.send`
can append another episode whose units arrive on the same subscription.

## 17. Auth failure mid-run = a resumable pause

A mid-run `needs_reauth`/revoke writes a non-terminal `Pause{reason: auth_required, error}`
record, not a `RunEnd`: the run is paused, not finished. After re-auth, resume continues the
same `run_id` from the last durable boundary; no new episode starts until the pause is resolved
or cancelled. The error detail is recorded for `run.status`.

A routed run may instead move to another compatible account
through `route.account_for` with `mode: "continue"` and `exhausted.limit_window: "signed_out"`.
This requires all three current declarations: `account_continue`, `account_continue_resume`
and `account_continue_signed_out`; unknown support counts as no support. The signal is a vault
`needs_reauth` with no usable held token, or an authentication-class HTTP 401/403 whose
post-rejection vault read says `needs_reauth` or supplied a newer token that was also refused.
When the vault read after a 401 fails, or returns the same token version, the 401 alone does
not prove the sign-in is lost: the run keeps today's auth pause and does not ask routing. An unreachable vault, transient failure or
timeout never triggers the exchange.
A still-unexpired held token remains usable even when its refresh needs sign-in.

An offer uses the existing durable account move before another send: `AuthRefrozen`, together
with `ConfigRefrozen` when the renderer carries account identity. A live move writes no pause;
a move of an already paused run writes the refreezes and `Resumed` in one batch. A hold retains
the auth pause and re-asks with a fresh selection id at `until_ms`, or at the next credential
re-check when no time was supplied. Restoring the original credential first resumes that
account and cancels the re-ask. A refusal, unreachable router or missing declaration retains
the auth pause. The actor remembers the typed outcome: refusal/unreachability is not asked
again for that pause during its life. Discovery that did not send a request, failed admission,
and an offer rejected locally do not consume that exchange; a prior timed hold survives them.
A hold time at or before receipt is clamped to a re-ask floor 30 seconds after receipt.
Opening an actor probes an existing sign-in pause once after dependency routes are ready,
including pauses written before this behavior existed. Only an offer can lift it during that
probe; a now-usable original credential or a permanent admission failure leaves it unchanged.
There is no new storage record
or reply member.

A timed account-limit pause checks for this sign-in loss once its recorded deadline is due.
An offer lifts that pause in the same account-move/resume batch, without an intermediate auth
pause. Without an offer it becomes today's auth pause; a refusal or unreachable router is not
asked again during that actor's life.

Recorded Anthropic fallback settings are frozen on resume, even if the current switches
are off. If the resolved credential method is not OAuth, restoring an episode that froze
either fallback produces a permanent configuration refusal naming
`frozen_fallback_auth_mismatch`, before any provider request or render-config refreeze.
Resume with OAuth credentials rather than silently dropping the frozen opt-in for an API key.

## 17b. The metering lane: direct read-only storage

`usage.export` was removed in release **0.3.156**. Requests to that method receive the
normal `unknown_method` refusal; there is no compatibility flag or replacement RPC.
Consumers read `export_facts` in `run-index.db`, opening SQLite with `mode=ro`.
`export_meta` retains the source identity and high-water mark for incremental readers.
The table spans all sessions in the daemon.

Facts are usage-only — no pricing, no money — because a consumer prices them with its own table and a
price frozen here would be wrong the moment that table changed.

**Reading a fact's timestamps.** `occurred_at_ms` is optional and `occurred_at_precision` does
NOT tell you whether it is present: the precision field describes how a timestamp would be
derived, so it reads `terminal` even on facts that carry none. A consumer deciding whether a
fact can be placed in a time window MUST test `occurred_at_ms` directly. The undateable facts
are a closed historical set (the backfill that re-derived facts for runs predating envelope
timestamps), so the share shrinks as traffic accumulates and cannot grow. `pricing_at_precision`
is the field that does track its timestamp: every fact without `pricing_at_ms` reads `degraded`.

**Cursor identity.** `source_instance_id` binds a cursor to one export database; it is minted
once at database creation and survives every process restart, so a changed value means the
database was replaced, not that the daemon bounced. An incremental reader whose saved identity
no longer matches must reset its position rather than continue from a sequence in the previous
database. `export_seq` is insertion order, NOT event order — a run billed again after a crash
gets fresh high sequence numbers — so ordering a report by `export_seq` orders by when broca
learned a fact, and ordering by `occurred_at_ms` orders by when it happened.

**Delivery.** Facts are insert-once and never rewritten, so a re-read of the same range returns
the same bytes. A terminal run is expected to produce its facts; a run that ends without any is
a defect on this side, not a consumer-side filter to work around.

## 18. Session records

A session's records are defined in `crates/broca-session-store/src/record.rs`; their meaning,
and how a run is written and resumed, are in [the session store](session-store.md). A new
record kind or must-understand field ships in a reader release before any build writes it.

## 19. SubcToolPlane hardening

**Call keepalives.** Dispatch declares `call-keepalive/v1` only when the admitted provider's
manifest listed that promise. The frozen plan carries the promise across resume; an older
manifest without it retains legacy waits and unchanged bind bytes. Negotiated calls are hung
after 90 seconds without a PUSH or terminal frame. Any PUSH resets only that silence timer;
`{"kind":"keepalive"}` bodies are never displayed, stored as tool output, or sent to the model.
A tool without `reply` gets the contract's 24-hour ceiling instead of the runner fallback.
A declared `reply` keeps its existing deadline calculation, never shorter than the runner's
fallback. It is an absolute terminal deadline on every route, negotiated or not, and PUSH
frames cannot extend it. The budget starts only after the request enters the writer queue;
route-open, bind, module-restart and writer-capacity waits do not consume it. Broca does not
stamp a deadline into the tool request body. Only non-negotiated tools without a declared
`reply` keep the previous per-frame wait, normally 120 seconds, including the existing
`aft` provider's shell/watch argument-derived fallback (defined in
[`tool_deadline.rs`](../crates/broca-subc/src/tool_deadline.rs)). Whenever broca abandons an outstanding tool request,
including run cancellation and shutdown, its existing abandonment guard sends a best-effort
CANCEL without waiting for the provider. Late frames find no waiter and cannot produce another
result; an ambiguous call is never re-sent. A provider-held call (described under
[Provider-held calls](#provider-held-calls) below) has already ended its original REQUEST with
an interim held reply. Cancelling its continuing work still uses `tool.withdraw`, not CANCEL.

The manifest promise is read from one admission-local daemon `catalog.list` snapshot, shared
with the existing presetless system-text capability read when present. `tool.catalog` does
not publish manifest `provides`, so otherwise one additional control request is needed.
Optional discovery runs alongside the required provider fetch and is never awaited after
that fetch finishes. A failed lookup, or one still unfinished when the required reply arrives,
freezes no promise instead of refusing the send. A warning is limited to once per minute;
`health.check.metrics.tool_keepalive_discovery` separately counts `lookup_failed` and
`lookup_unfinished`. Its `probes` and wall-clock `wait_us_total`/`wait_us_max` include cache
hits and are not production daemon RTT measurements. Required system-text capability checks
retain their previous fail-closed and retry behavior.

Fence keyed `(session, tool_call_id)` **with eviction** on batch/run terminal; **per-route
window** resolved from the specific provider at route-open (not memoized once); **route
eviction** (TTL/LRU + GOODBYE on idle) against `u16` channel exhaustion; **route
invalidation on GOODBYE/status** (learn a dead route); document the per-module
`run-index.db` `Mutex<Connection>` as a scalability limit (read/write split or pool if it
bites).

**Reverse requests are refused, not dropped.** Broca opens its tool and vault
routes with no reverse-request capabilities, because it fronts no person who could answer
an elicitation or a sampling request. A `Request` a module sends toward broca on a route
channel is answered at once with an `Error` frame carrying code `reverse_request_unhandled`
and "no reverse-request handler is registered for <family>", the same reply the subc Rust
consumer SDK sends. It never reaches a pending call, even one with the same corr, and each
refusal is counted in `health.check` as `reverse_requests_refused`.

**Tool result formats and provider-held calls.** The daemon's dispatch routes declare
`consumer_capabilities: ["tool-result/v1", "held-reply/v1", "call-yield/v1"]` in every build. Typed result
decoding still requires the result-format opt-in in the provider's frozen catalog. Negotiating
`tool-result/v1` does not change the stored text or error payload bytes. Dispatch captures images
and PDFs through the session's
media-capture handle. The frozen model's media profile decides which captures reach a model
request; unsupported kinds remain text markers. Role discovery and management routes do not
negotiate tool results.

### Model-based media input

Media input follows the frozen model's catalog input modalities for every session class,
including heads, workers and sidekicks. No process configuration is required.
`test-attachments` provides attachment test helpers that cannot be built into release binaries;
it is not a deployment prerequisite.

Every fresh episode whose model explicitly supports a non-text input modality
freezes its adapter media profile, even for text-only input: a tool may return an image
after the first model request. Requests without media retain their ordinary rendered bytes.
Resume never adds a profile to an older recorded configuration; those runs keep their media
markers until a fresh episode is admitted. Existing per-family carrier gates remain in force.
The profile freezes the catalog's `limit.images_per_request` when stated; absent or null counts
keep the adapter's existing per-family count (including Anthropic's context-window rule).
Malformed counts exclude the catalog row rather than becoming zero or unlimited. A later
catalog change affects fresh admissions only, never a resumed episode's frozen count.

`role.describe` always declares the llm-runner `attachments` and `media_only_send` groups and its upload
and `session.media_read` operations. A non-template send with committed media may omit `prompt` or send it empty, storing media parts alone without an empty text part; a send with neither text nor media is refused as `invalid_params` naming `template`. Upload staging is available independently of the model;
sending an upload requires the frozen profile to accept its kind and exact MIME type.
The operations follow the pinned llm-runner contract §9.12. In particular,
`session.media_read {sha256, offset, length}` serves at most 2,097,152 decoded bytes as
`{data, size, mime}`. Zero length omits `data`; EOF returns empty data, and an offset
past EOF refuses by name. Only durable captures in the bound session authorize a digest:
another session's objects and unreferenced committed uploads return `unknown_media`.
Read admission is the same as `session.read`.
Capture authorization reads the session's media index, not a replay of the transcript for
every chunk.

Upload appends are strictly contiguous; replaying a chunk refuses
`upload_offset_mismatch` with the current `received` count. Sends preserve the upload refusal
codes (`unknown_upload`, `upload_gone`, `upload_not_committed`) instead of reporting a failed
session open. A send carrying a kind the model cannot accept is refused before admission.

`session.head.input` reports a finished episode's next model's accepted input kinds and exact
MIME types from the in-memory catalog
and adapter MIME filter without changing the recorded config or fetching catalog data.
Known per-image `max_image_bytes` (uploaded file bytes, not base64 bytes) and `max_image_dimension` (longest side for one image) come from that same profile; unknown limits are omitted, never zero. A committed image exceeding a reported limit is refused before writing as non-retryable `media_too_large`, whose detail contains only the exceeded limits.
These facts come from the session's latest frozen run config.

Default reads keep media as text markers. `session.read` with `media: "blocks"` returns
stored references; descriptors and history display rows expose refs rather than payload
bytes. Blocks reads include a digest of each media-bearing message and digests identifying
each of its content parts. Default model-view reads also use not-requested text markers; explicit
blocks mode keeps native media references. Provider rendering uses the frozen family's media profile; unsupported person input
is refused before publication, including ChatGPT audio. Sessions holding media refuse
agent-data moves before any cut or freeze because move transport currently carries WALs
only. The existing Engram catalog already includes `media-durable` as a portable filetree
and excludes temporary, retained and ephemeral media stores; model-based admission needs no
descriptor change.

### Provider-held calls

At graceful stop begin, a running call may be handed back through `tool.yield` on its original
route, with `reason: "runner_restart"`. Admission freezes `yield_tools` in each provider's
declarations only when its catalog snapshot declares `call-yield/v1` and its tool catalog
declares `late_results`; the set contains only canonical tool names tagged `call.yield/v1`
using `tool-result/v1`. Dispatch also requires a call key and a route declaring `call-yield/v1`.
Older manifests without the set never yield. The yield answer does not settle the call: its
original terminal frame does. A result is recorded normally; a held final frame follows the
same durable hold and recovery path below. Refusals and accepted yields without a final frame
within grace keep the existing abort, CANCEL and unknown-outcome seal. An accepted yield
abandoned without its final frame logs the provider, tool and call key.

The daemon installs the provider-held runtime (`broca-module-serve/src/held_runtime.rs`) on
its tool plane: the waiting slots a held call's batch waits in, a late-results intake store
(`late-results.db` under the state root, a dispatch-registration projection kept out of the backup
catalog), one puller per provider module, and boot recovery. Installing that runtime enables
held-reply negotiation without an environment switch, in release and debug builds alike.
`role.describe` advertises `held_calls` beside `run_status`. Tool planes without the held
runtime do not declare held replies or register plain-call keys with the late-results intake;
result-only negotiation remains independent. Role and plan-fetch routes declare neither
capability. A reply carrying `held` on
an undeclared route becomes a malformed-reply error result with reason
`malformed_provider_held_reply`, without pausing or exposing its interim content to the model.

With the capabilities declared, the core batch passes durable intent keys through its keyed
dispatch methods and registers each dispatch group in one synced intake transaction before
sending it. Plain keys (`<lineage_id>:<seq of the call's ToolIntent>`) go only to
providers whose frozen plan manifest declares `late_results`, on a consumer-identity
connection; approved calls keep their deferred key and exact argument bytes. Routing and
decoding read only the provider declarations frozen into the session's plan manifest (on a
fresh run the admitted manifest, on a resume the one its `RunStart` recorded), never a live
catalog. An accepted held reply first syncs a `ToolHeld` record, then waits in its slot; when
every remaining call is held the run pauses `tool_held` (an internal `Pause` record) and sends
nothing to the model. This provider-owned wait remains `active` on `run.status`; it does not
emit `run_paused` on `session.subscribe`.

Each change to the ordered `held_calls` list emits a durable subscribe control frame from
the existing held/terminal records. The frame has `unit: {type: "status", run_id}`, top-level
`state`, a durable `cursor`, and the stream's next `seq`; it includes top-level `held_calls`
only when that list is nonempty. It has no
`event` member: this is a status snapshot, not a live lifecycle or display reset. The list
uses the same fold of the session's records as `run.status`. Clearing the final held call emits another status
frame with `held_calls` omitted; absence means none. Ordinary sessions acquire neither new
frames nor new fields. Live display sequence numbers are not replayed, so a reconnect's
`seq` starts again, while its durable units, cursors and status lists are unchanged.

A late result IS the held call's result: held continuation preserves at-most-once dispatch
of the original arguments. Withdrawal is a separate idempotent operation, not exactly-once
delivery of withdrawal packets.
The puller pulls `late_results` under broca's own principal on one unscoped route per module
(the custodian is the carrier, tool-provider contract §7), every 3 s while the module needs
its feed, backing off to 30 s after errors. Each entry is classed from the intake registrations
and the registered session's own records, never from a projection: an entry for an open held
call
is mapped with the same `content/v1` mapper a synchronous reply uses (`not_started` and
`expired` become error results whose text is pinned by golden), deposited in the call's slot,
and the session's actor resumes a paused run into its original batch, which writes the result
as the call's result at the call's position and syncs it. Only then is the entry acked,
through exactly the cursor the provider returned. An entry whose call already has its terminal
(including an inline answer), whose dispatching run has ended, whose kind is unknown, or which
no registration names is acked as a no-op. A durable intent with no terminal or held record is
pending only while its dispatching run is open; unreadable session storage remains blocked. An answer
for an indeterminate call in a sealed run never changes that call or enters a later run.
A `not_started` entry with `reason: "scope_ended"` is the same never-run error result,
not a request to dispatch the call again.
Entries are deduplicated on
`(provider, custodian, call_key, event_id)`, and `cursor_incarnation_changed` re-reads from the
start. The ack advances only over a settled prefix. An unsettled entry pins the ack: no ack may
cover that entry until it settles. An entry that pins the ack for five minutes logs one WARN
per entry with its module, call key and event id; `health.check`'s `provider_held.overdue_pins`
counts currently pinned entries at least five minutes old, without reading any session.
Batch completion prunes registrations after the terminals and commit are durable. The puller
also retires old inline-terminal registrations as it encounters them. A maintenance sweep
checks at most 64 registrations every 3 s, starting after recovery, even for modules without
a running puller. It lazily retires stale terminal rows without scanning every log at startup;
held calls that still owe a batch commit keep their registrations for recovery.
The intake reads the durable intent, held and terminal records and the operational run state. Delivery uses the same slots and the store actor's `ResumeHeld`
continuation; a cancelling or ended run cannot take an answer, nor can a later run take an
earlier run's key. Deferred entries and retired registrations log their session address,
call key and reason.

At boot tombstoned lineages are pruned from the intake without opening an actor. Every remaining
session with a registration is opened so its open-time recovery runs, then each
module that still needs its feed gets its puller. A missing or unreadable `late-results.db` is
rebuilt off the startup path by reading the operational state and active run records of every
session in the session store's inventory across all backup classes, including intent-only calls whose registration may precede
their held reply. It also reads the runs started within the last seven days, finished or not,
for what they still owe a provider: a call handed back for the person's steer whose notice
isn't in the session yet, and a held call its provider never answered, whose withdrawal may
still be owed and is sent again by withdrawal recovery. Such an obligation of an older run is
lost with the database: its late entry is acked without delivery, and its withdrawal is not
sent again. The rebuild
retains each episode's original scope. Until that scan completes no entry is delivered or
acked. Only roots with no registration history may skip a rebuild scan. An existing unheld
database starts no scan, reads no session and gains no writes; boot adds only a read-only probe
of the rebuild fence. An unreadable session keeps the rebuild fence pinned until a rescan
succeeds.

After a held run's cancel seal is synced, `tool.withdraw` is sent on the original provider's
tool route with its frozen scope, under broca's carrier identity. The cancellation outcome
does not wait for withdrawal. A private SQLite outbox retains registrations independently of
intake pruning and records attempts and replies, so recovery retries the seal-to-send crash
gap without reopening or dispatching the call. Transport failures and transient route refusals
have at most three attempts across process restarts while that outbox is retained; a database
loss/rebuild does not reconstruct prior attempt counts. Final answers and terminal route
refusals do not retry. Provider answers are idempotent under tool-provider 0.13.2 contract §5,
including the ambiguous send-to-record crash gap. Recovery may resend an outstanding withdrawal
for an already-durably-cancelled call. Neither shutdown nor restart creates a withdrawal intent
by itself, and a call that completed before its sibling was cancelled is not withdrawn. The
registry's stop signal suppresses new withdrawal attempts and retries during drain; an earlier
request may already have reached the provider. Health exposes pending/failed withdrawals and
their refusal codes. Route refusals `scope_not_live` and `scope_ended` mean withdrawal is
impossible under the original scope, not that the held call was cancelled or its outcome is
known. They count in `provider_held.withdrawals.scope_gone`, not `failed`, and do not degrade
health; `scope_gone_details` retains at most ten diagnostics with the same `code`, `answer`
and `reason` fields as `failure_details`. All other refusals and errors still count as failures.
After a cancelled call's terminal is durable, a later result is only receipted and acked as a
no-op when the module's on-demand feed next drains, not guaranteed to be delivered to the run.

Late-result pullers start before withdrawal recovery, which runs independently and obeys the
registry's stop signal. Withdrawal recovery snapshots the outbox once, groups rows by session,
and reads each session's records once, in batches of at most four sessions separated by 3 s. As it
reads, it retires inline-terminal watches and their registrations in one transaction; normal
inline batch completion performs the same retirement without reading the session again. Ordinary
intake pruning still preserves a held call's withdrawal watch if broca crashes after the run's
cancellation seal is durable but before `tool.withdraw` is sent.
Each sweep logs one summary with sessions read, rows settled, withdrawals sent and elapsed time.
`provider_held.withdrawals.sweep_running` and `rows_remaining` report whether a sweep is running
and how many snapshot rows remain to examine; `watching` counts retained watches at the last
completed sweep or withdrawal attempt. All these health values are cached, not session reads.

Ack guarantee, narrowed: broca puts no bound of its own on a hold. A provider keeps at most
1,000 unacked results per session and drops the oldest first; a held call whose entry was
dropped that way never receives one and its run stays paused until caller cancellation;
planned daemon shutdown preserves the hold. A finished session that retention
deletes while its ack is still owed keeps its feed row, and entries for its lineage are then
no-ops.

For a retention session, held calls last only as long as the session. Before retention or an
operator deletes a session, broca sends `tool.withdraw` for every call a provider still holds
in it: a held call with no completed terminal, one its run cancelled included. Nobody can
collect a result once the session is gone, so the provider is asked to stop work that would
only be thrown away. Each withdrawal carries the identity read from the session's plan while
its ledger still says whose call it is. A call already withdrawn, or never registered for
withdrawal, is not asked again. Deletion doesn't wait for the answers. A withdrawal the
provider asks to repeat is retried after the deletion with that same identity, and an attempted
withdrawal of a deleted session that can't be repeated any more counts in
`provider_held.withdrawals.scope_gone`, not `failed`.

Deletion leaves the session's registrations and receipts in place. The intake retires a
registration as `session_deleted` the next time it reads it and finds no session at its triple,
or finds a session recreated there whose call keys name another lineage. Late entries for the
deleted calls are then acked as no-ops, and their receipts go with the acks that cover them, as
for any other entry. The shared module feed row, cursor and owed ack survive so other sessions
can still receive their results.

The intake store doesn't hold deletion back: an intake registration, a missing or unreadable
`late-results.db` and the rebuild fence below all leave it to proceed. What keeps a session
with a held call is the session's own state. An operator's deletion refuses a session whose run
is active or paused, a run paused on a held call included, and a frozen retention policy isn't
due while a run is active or paused. Before the first registration, `late-results-registered`
is synced at the state root so losing the rebuildable database cannot erase that history.
Recreating a missing projection with held history leaves `late-results-rebuild-required` set.
That fence also resumes a rebuild when a partial database exists after restart. It is removed
and its directory synced only after the scan over the session store's inventory has succeeded
and all rebuilt rows are committed with SQLite synchronous FULL. An unreadable session keeps
the fence. Calls created after the scan register durably in this same database before
dispatch. A crash after commit and before removal repeats the scan safely.

## 20. Persistence + storage

```
~/.local/share/cortexkit/broca/
  session-store/, session-store-durable/, session-store-ephemeral/
                            ← the session store, one root per storage class: heads, logs,
                              objects and setup parts, under each session's address
  run-index.db              ← SQLite: session-ref index, scope reservations, bus outboxes,
                              usage export facts
  leases/                   ← single-writer lease files
```
A session's records are its authority; everything in `run-index.db` is either its own
authority (the export facts) or operational state. The full layout, with every file, is
maintained in [data-model.md §1](data-model.md#1-the-state-root); that list wins where the two
differ.

The state root shown above is the default. It resolves to `BROCA_STATE_ROOT` when set and
non-empty, else `$XDG_DATA_HOME/cortexkit/broca`, else `~/.local/share/cortexkit/broca`, and
broca refuses to start when none is available (`crates/broca-module-serve/src/state_root.rs`).
Astrocyte reads `run-index.db` at that data-home path, so the location is a contract. Older
builds defaulted to `broca-state` beside the subc connection file; when only that old
directory holds state and `BROCA_STATE_ROOT` is unset, broca refuses to start and names both
paths.

## 21. Core and store invariants

The rules a run keeps across a crash, a restart or a stop are in the
[durability contract](durability-contract.md). The ones callers see on this surface:
1. **A session is a sequence of runs.** Each admitted send opens a run with `RunStart`; it ends
   with `RunEnd` or stops at a `Pause`. Resume continues the last open run with its own frozen
   config, run id and counters, never the first run's.
2. **Durable cursor.** Control units are keyed by `(wal_seq, sub_index)` plus the log's
   incarnation, so a recreated log refuses a stale cursor with `cursor_expired`. A cursor
   advances only past records that are durable, and it does not reset between runs.
3. **Durable queue and identity.** Queued sends, retractions and steers are records, so a
   queued send survives a restart and is consumed exactly once.
4. **Auth required is a pause.** A run that loses its sign-in pauses `auth_required` and
   resumes under the same run id.
5. **One writer per session**, held by the session lease and the head's version check.

## 22. Session CLI

`broca-session` sends turns to the supervised module; it does not own a separate
in-process run loop. A single-turn session is a lineage with one episode, using
the same admission, durability and coordination contracts as other consumers.

## 23. Thin-core fit (subc)

**Zero subc-CORE changes**. All `session.*`/`run.*` are opaque
management-surface RPCs; fan-out, replay, backpressure and the session actors are
module-side. The §21 rules live in broca's own crates, not in subc-core.

## 24. Conformance coverage

- Multi-episode crash-cut resume, including a cut within a later episode, must
  preserve prior episodes and never let an episode terminal seal the lineage.
- Queue retract/consume races must remain unambiguous across crashes; durable
  cursors must remain continuous across reopening; authentication continuation
  retains the run_id; cancellation seals Cancelled.
- Module tests cover actor admission, fan-out backpressure, the attach barrier, run-id
  lookup across restart, and tool-route lifecycle.
- End-to-end tests cover multiple clients, reopening, multiple turns, terminal
  subscriptions, status, and session-read pagination.

## 25. Explicit deferrals

Fork UX (cross-lineage prefix read) · interrupt-steering (v1 = queue + retract) · auto
pause→refresh→continue (v1 = manual resume of a paused run) · router-driven model
selection · cross-machine portability (distributed lease) · consumer-provided custom
tools.

## Graceful restart policy

`session.send` accepts `on_restart: "resume" | "interrupt"`. Omission means
`interrupt`; there is no daemon-level default knob. Invalid values are refused as
`invalid_params` naming `on_restart` before anything is written. The policy belongs to
one send: durable queued submissions retain it in their request, and `RunStart`
freezes it for the episode. It is not inherited from an earlier send and is part
of keyed-send identity.

On the daemon's channel-0 `module.draining` push, broca stops new admission and
queued dispatch immediately, without starting its final seal timer. Requests refused
because broca is stopping answer code `transient` with
`detail: {class: "transient", cause: "module_stopping"}` (see
[module lifecycle and shutdown](module-lifecycle.md)). A resume-enabled
run finishes its current work and writes a `Pause{reason: restart}` record at the next clean
step-entry boundary. Interrupt-enabled runs seal `Interrupted` at the same boundary. GOODBYE (or
connection loss) starts the final 10 s grace and 20 s total shutdown budget: a tool
gets the daemon's drain window **plus** the grace to reach that boundary. A tool
still in flight after grace is sealed `Interrupted` with `outcome_unknown` even
when it opted into resume. Crashes keep their terminal interruption behavior.
The manifest declares `runs_in_flight`, `opening`, `closing`, and
`system_one_batches_in_flight` as busy health gauges; paused actors are idle, so a restart pause does not hold the drain open.
See [module lifecycle and shutdown](module-lifecycle.md) for timings and the
daemon's irreversible drain contract.

After the serving connection registers, boot recovery resumes restart-paused
sessions through the existing admission checks: the recorded model must still
resolve, its recorded account must match the served account, tool routes must be
available, and stamped scopes must pass synchronization and access checks. It
continues the same run and send IDs, records a `Resumed` record, and reconstructs
provider request bytes from the session's records. A readable session refused by admission is
sealed `Interrupted` with the refusal reason. An unreadable session is only logged at WARN: no
bytes or metadata are changed, and other sessions still resume.

A restart pause is continued only within one hour of the
time it records. A restart takes seconds to minutes, so an older pause was left by a long
outage and its caller has stopped waiting. Such a run, or one whose pause records no time,
is not continued: when its session is next opened (at startup for every session with an
unfinished run) the run is sealed `Interrupted` with `restart_resume_expired`, no provider
request is sent for it, and the session's queue drains. The caller decides whether to send
again. A paused run with a cancel pending still ends `Cancelled`.

New episodes freeze image-only labeling for multipart image-notice text; older episodes keep
their literal text projection on resume, while a new episode adopting the rule may rebuild its
prompt cache once, like a renderer-version bump.

Restart and authentication pauses diverge in the actor's `on_send`, `send_plain`,
and `steer_submission` paths. Restart pauses answer keyed retries `paused`, queue
plain sends behind the unfinished episode, and persist steers for its next step.
Auth pauses still refuse unrelated plain sends and steers as `run_paused`; their
own keyed retry remains an explicit re-auth resume. Both pauses support cancel.
`run.status` reports restart pauses with `state: "paused"`, `reason: "restart"`
and `pause_reason: "restart"`. Authentication-pause `run.status` responses retain
their existing format (`reason: "auth_required"`, without `pause_reason`).

### Session class and backup placement

`session.send.session_class` is an optional top-level opaque label beside `send_id`
and `retention`. Its grammar is `[a-z0-9_-]{1,32}`; explicit null, non-string values,
uppercase, whitespace and all other characters are refused as `invalid_params`,
naming `session_class`. The first send freezes the label. Later sends may omit it
or repeat the same label, but cannot change it. An unlabelled session adopts
the first later run start's label only if that label would place it in the folder
already holding the session; otherwise the label is ignored. Adoption never moves
files or changes the class labels recorded for earlier runs. A queued send adopts
when its run starts.
The label is recorded on the run's `RunStart`, not sent to model providers.

The daemon reads `backup_classes` from `broca.jsonc`, defaulting to `["head"]`.
`role.describe.capabilities` includes `session_class`, and its
`session_class.backup_classes` lists the configured labels. The session store keeps one
complete store per class: `session-store-durable/`, `session-store/` (ordinary) and
`session-store-ephemeral/`. A first send whose class is listed creates the session in
`session-store-durable/`; unclassed sessions and other labels use `session-store/`. A send with
`retention` always uses `session-store-ephemeral/` and is never backed up, even if its class is
listed: deletion takes precedence. A send with a storage policy (`retention_policy`, below) is
placed by its policy instead. Changing configuration never moves an existing session. The
root holding a session's head is authoritative afterwards, and a head found in two roots is
refused `session_in_two_classes`.

Engram captures only `session-store-durable/`. An agent head is backed up only when it is
created with `session_class: "head"` (or another label listed in `backup_classes`). A file
import (`session.import` with a file source, which brings an OpenCode 1, OpenCode 2 or Pi agent
head onto broca) carries no label and is placed durable: ephemeral if it carries a retention
policy, else `session-store-durable/`, whatever `backup_classes` lists. Its optional
`retention` (seconds, with a send's limits; a larger value is `invalid_params`) is that policy:
the seeded run records it, and the session is deleted once that long passes after its last
activity with no run active or paused. The import's own time counts as activity. `retention`
is part of the import's payload, so a repeat must carry the same value. Session addressing and
leases remain keyed by the original session triple.

### Storage policies and release

`role.describe` declares llm-runner's `session_retention` with `max_days: 3650`,
`max_unreleased_hold_ms: 86400000` and `required: false`, and lists `session.release`.

`session.send.retention_policy` is `{keep, archive_after_days, backup, request_dumps}`, where
`keep` is `"none"`, `{"days": N}` (1 to 3650) or `"forever"`. A value that fails the contract's
check refuses `invalid_params` naming the member's path, and a send carrying both
`retention` and `retention_policy` refuses `invalid_params` naming `retention_policy`; nothing
is written for a refused send. The first send carrying a policy freezes it in a `Retention`
record, written in the same batch as the send's own record. A later send carrying the same
policy writes nothing; a different one refuses `retention_conflict` with `detail.frozen`.

A new session is placed by its policy: `backup: true` in `session-store-durable/`, else
`keep: "forever"` in `session-store/`, else `session-store-ephemeral/`. A session whose
starter sent none keeps a policy broca assigns (`retention_state.source: "runner"`) from what
already decides its storage: `keep: {days}` from its `retention` seconds, rounded up to whole
days, else `forever`, backed up in the durable root. Its owner (the principal of its first
send) may replace an assigned policy once; another sender's different policy refuses
`retention_conflict`, and a replacement that changes `backup` refuses `backup_unsupported`
(`reason: "backup_fixed_at_creation"`), because a session never moves between roots.
`archive_after_days` is checked and changes nothing: broca has one storage tier.

A session is deleted at its deadline: last activity plus `days`, or last run end plus
`max_unreleased_hold_ms` for `keep: "none"`, or the release time once released. Nothing is
due while a run is active or paused or a send is queued. A session outside the ephemeral root
that can come due is marked (`retained/<address>` in its root), so retention's pass reads it
without reading every head.

`session.release {}` answers `{state}`: `deleted` when the session was deleted before the
answer, `pending` while a run, a pause or a queued send holds it, and `absent` when no session
is at the name. Only the principal of the session's first send may release it; another
principal, a parent scope's owner included, refuses `scope_owner_mismatch`. A release stops
idle cache warming at once. Until deletion, `session.head` carries `released: {at_ms,
delete_after_ms}`, reads serve as before, and every send refuses `session_released` with
`detail.delete_after_ms`.

`session.head` carries `retention_policy` (the frozen or assigned policy, exactly as accepted)
and `retention_state: {source, request_dumps_unsupported?}`. broca saves no request dumps
(`session.request_dump` rebuilds requests from records), so `request_dumps: true` sets
`request_dumps_unsupported`. A session a policy or a release deleted is gone: its head and reads
refuse `unknown_session`, `session.media_read` refuses `unknown_media`, and the next send starts
a new incarnation. Its expiry marker stays: it holds no content, only the next free tag and
notice numbers, so a session started again at the name never reuses a number.

A file import records its attested owner and scope but applies no plan. Its owner's first
`session.send` must carry `plan` and passes ordinary plan admission, including compaction
enrollment and model selection; an omitted plan is `invalid_params` naming `plan`. Another
principal is refused `scope_owner_mismatch`. The imported history precedes that send's prompt.
`session.install_view` may install the moved head's view before this send, but is refused once
the session has run. No `session.adopt` is needed or served for this transition. OpenCode 1
sources are single-session SQLite extracts made by `ck-import opencode1-extract`, not the live
host database; the source is digest-checked before reading and deleted only after durability.
Both import list shapes carry `newest: {ordinal, mid, digest}`, all required: the exact last
entry the import placed, including the digest `session.entries` serves. It is not the current
session head. The receipt records the imported message count and move ids; retries reproject
that original immutable entry and its facts through the same stored-entry code as
`session.entries`, checking its id against the receipt. Source deletion, views and later owner
runs do not change it. No extra receipt field is needed; a missing/replaced original entry
refuses recovery instead of answering with another entry.

A long head's move list does not fit one reply frame (4 MiB, about 60,000 rows). The import's
optional `moves_from` asks for the list in pages: the reply's `moves` holds the rows from that
zero-based index, up to 2 MiB of rows, with `total` (the rows in the whole list) and, while rows
remain, `next_from` (the `moves_from` of the next page): `moves: {shape, rows, total,
next_from?}`. Every other reply member is the same on every page, and the pages concatenated are
the list an unpaged reply carries. `moves_from` is not part of the payload digest, so a repeat
asking for any page matches the import's receipt and is answered from it without importing
again. A caller sends `moves_from: 0` with the import and repeats with each `next_from` until it
is absent. Without `moves_from` the reply carries the whole list, and one too large for the frame
is refused `response_too_large`, though the import itself is durable. A `moves_from` past the
list's end is `invalid_params`. Like the import, a page is refused `session_busy` while the
session runs a run.

OpenCode 1 source rows cover every session message, ordered by `time_created`, then id, even
when a row projects nothing and has `entry: null`. The extract preserves both ordering columns.
`finished_summary` is true when summary is true/numeric 1 and finish is `stop`, regardless of
role or error, exactly `COALESCE(json_extract(data,'$.summary'),0) = 1 AND
COALESCE(json_extract(data,'$.finish'),'') = 'stop'`. A summary object or another finish is false.
Move rows have no `source_entry` member, not even null.
An older OpenCode 1 message's `pending` or `running` tool part is a historical interruption:
it keeps its recorded input (or `{}`) and the host's error result
`[Tool execution was interrupted]`. Only such a part in the last source message row refuses
`import_open_call`, because that row can still be live. Source-row order is `time_created`, then
message id; a later row that projects no message still makes an earlier unfinished call historical.

Before importing, an operator can run `ck-import opencode1-check --extract FILE --session ID`.
It uses the import reader and the same media byte/type admission without needing a store. It
prints one counts-only JSON object: source/imported roles, calls, exclusive result kinds,
distinct media byte objects and occurrences, dropped systems, rows projecting nothing (including
systems), and finished summaries (including errored summaries). A refusal prints only its code
and one-based source message ordinal, or null when no source row is responsible, and exits 1; it prints
neither source diagnostics nor message ids/content. The check does not exercise import-root,
digest or idempotency checks, or store-write/encryption/durability admission. Without
`--moves-out`, it also does not check whether imported part numbers fit above the caller's
highest already-shown number (`tag_floor`). A successful check is not a promise that a later
durable import cannot fail.

With `--moves-out FILE` and optional `--tag-floor N` (default 0), the checker also runs the real
import seeder without a daemon or store. It creates a new owner-only file with wrapper
`{offline: true, unminted_prefix: "unminted:", moves, newest}`. The `moves` member remains
wire-decodable: source rows are exact; every added row keeps its exact ordinal and `added: true`
but its mid is `unminted:<n>` counting added rows from 1. The real store must mint those ids
from its message-id prefix and next record sequence. `newest.ordinal` is exact; its mid/digest
are exact for a source newest, or both `unminted:` markers for an added newest. A single stderr
line reports the count and reason. The wrapper prevents this partial rehearsal list from
being mistaken for a real import reply, and contains no message content. `--tag-floor` requires
`--moves-out`; seeding also validates that imported part numbers fit above that floor.

### Restoring the new storage from a backup

Engram copies `session-store-durable/` by walking its live files, without a snapshot. It
lists every file first, then reads them, and may read a file again later. So a restored
session can hold a head newer than the logs and objects beside it, for example one a seal
wrote after the listing, and an open log holding records that name a medium, a setup part,
an import's id map or an enrollment's overlay written after the listing. A log read while a
batch was being appended is stored cut in the middle of that batch, which loading treats as a
torn tail. Normal loading never falls back: a session whose head names a file the store
lacks is refused by name.

To make such a session whole again, every head version is also kept as its own file,
`heads/<addr>/v/<version>`, beside `heads/<addr>/head`, which is the only head loading
reads. A version is deleted once the version after it has existed for 24 hours, and the
newest two are always kept. After restoring the files and starting broca, the operator runs
`session.restore_from_backup` (operator only):

```sh
broca-session --subc <connection file> --project-root <root> --harness <harness> \
  --session <session> restore-from-backup          # that session
broca-session --subc <connection file> --project-root <root> --harness <harness> \
  --session <session> restore-from-backup --all    # every session in the new storage
```

With `--all`, the session named only attaches the command to the daemon. The restore runs
inside the daemon, since checking an encrypted session needs the deployment key. For each
session it:

- keeps a session whose head and every file it names (open log and checkpoint, directory
  pages, sealed segments, id index pages, and the media, setup parts, import objects and
  enrollment overlay that records name) are there and pass their checks. Nothing is written,
  and the session isn't held;
- otherwise holds the session as a move does, takes the newest head version, `head` first,
  then `v/*` newest first, whose change files are all there, and writes it as a new version
  above every kept one. That rolls back the changes the backup didn't hold whole: a seal
  moving records out of the open log into sealed segments, a revert, or a recovery;
- cuts that version's open log back to where the batch holding the first record that names a
  file the backup lacks starts. Everything the remaining records name was written before
  them, so what is kept is complete. The cut bytes are kept beside the log, as
  `logs/<addr>/<log>.restore-cut-<ms>`.

Restore deletes nothing, and refuses a session that fails to load for any other reason: a
head or open log that fails its checks, or a recovery in progress. A session an actor still
serves is reported busy and left alone; run the command again once it has been idle. The next
load treats a restored session as after a crash: a seal the restored version hadn't
committed, whose intent record ends its log, is undone, a torn tail is cut, and an unfinished run ends interrupted, with calls whose results are
missing left indeterminate and never sent again. A message id is never reused, since each
load starts a new writer generation, and a subscriber's cursor past the restored end, or on a
record that is now another, is refused `cursor_expired`.

The command prints one line per session (`--json-events`: one JSON object, the line in its
`report` field) and exits 0 when every session was kept or restored, 1 otherwise:

- `kept`: the head version, and that everything it names is there;
- `rolled_back`: the version it had, the version restored, and the version that was written;
- `log_cut`: the log, the first record cut, the record naming the file the backup lacks and
  that file, and the sidecar keeping the cut bytes (a rollback can cut too);
- `unrecoverable`: what each kept version lacks, for example a sealed segment no kept version
  can do without; nothing changed;
- `refused`, `busy`, `key_unavailable` or `failed`: why it wasn't restored; nothing changed.

### Request bodies of one run

`session.request_dump` (operator only, like `session.tag_map`, and not one of the runner role's
operations) rebuilds the request each step of one run sent, for a session kept in the new
storage. That storage keeps each attempt's request digest, not its bytes. Each step is rebuilt
from the session's records the way a warm-up rebuilds a recorded request, rendered with the
run's frozen render config, and written only when its SHA-256 equals the digest the step's last
attempt recorded. A written file is therefore the body the provider received. Another caller is
refused `operator_required` before anything reads the session.

Request: `{"run_id": "<run id>"}`. A run the session holds no attempt of is refused
`unknown_run`; an unknown field is refused `invalid_params`. The bodies go to
`request-dumps/<session address>/<run id>/s<step>.json` under the state root, in a folder and
files only the owner can read. Dumps are deleted a day after they are written. The reply names the
folder and each step, never any content:

```json
{"run_id": "r1", "folder": "<state root>/request-dumps/<address>/r1",
 "steps": [{"step": 1, "recorded_sha256": "<hex>", "outcome": "written", "bytes": 1834},
           {"step": 2, "recorded_sha256": "<hex>", "outcome": "digest_mismatch",
            "detail": "rebuilt to <hex>"}]}
```

`outcome` is `written`, `digest_mismatch` (rebuilt to other bytes; nothing written) or
`not_rebuilt` (with `detail` saying why; for example, a request carrying media isn't rendered,
since that needs the media delivery a running run's actor holds). A step outside the session's
current window, behind a later compaction seal, may not rebuild. `broca-session request-dump
--run-id <id>` calls it with a ten-minute reply budget.

### Deleting a session

`session.delete` (operator only, and not one of the runner role's operations) deletes one
session kept in the new storage, in either storage class, with or without a retention policy:
for a session nobody uses any more, such as an import that will be redone. It is deleted as
retention deletes an expired session: its expiry marker first, so reads of its runs answer
expired afterwards and a session created later at the same address continues its counters, then
what a move from today's storage left, then the session's head, logs and objects.

Request: `{}`; the route names the session, and an unknown field is refused `invalid_params`.
Reply: `{"deleted": true}`, or `{"deleted": false}` when no session is stored there. A session
holding work a deletion would lose (a run active or paused, a queued send, or spend not yet
exported) is refused `session_busy`, naming which. One an actor holds open, or a move is
finishing, is refused `transient`: it can be deleted once its actor closes idle. Another caller
is refused `operator_required` before anything reads the session; a session today's storage
keeps is refused `not_served_on_old_storage`. `broca-session delete` calls it.

### Part number map

`session.tag_map` (operator only: only a direct operator connection may call it, and it is
not one of the runner role's operations) names, for a session
kept in the new storage, which message and part each part number belongs to: the number broca
renders in front of a numbered part, as the model is shown it. No caller-facing read carries
these rows. Another caller is refused `operator_required` before anything reads the session.
A session today's storage keeps is refused `not_served_on_old_storage`, by name; an unknown
session answers as `session.read` reads it, an empty transcript that numbers nothing.

Request: `{"after_ordinal": <u64, optional>, "limit": <u64, optional>}`. `limit` counts
messages, not rows, so a message's parts are never split across pages; it defaults to 500 and
is held between 1 and 2000. An unknown field is refused `invalid_params`. Reply:

```json
{"numbered": true,
 "rows": [{"tag": 12, "mid": "<message id>", "ordinal": 7, "part": 0, "role": "user"}],
 "next_after_ordinal": 7}
```

One row per numbered part, in transcript order, sealed history included. `mid` and `ordinal`
are the message's id and ordinal (from 1) as `session.read` and `session.entries` serve them,
`part` is the zero-based index of the part's block within its message (its id in
`session.entries`), and `role` is the message's role. `next_after_ordinal` is present only
when more messages follow. `numbered` is true when the session's latest run shows the model
numbers, or when the page holds a part that an earlier run numbered; when it is false the page
has no rows. Ids and numbers only: the reply carries no content.

On the current compaction contract a part takes the number `session.entries` serves it with;
off it, the number its run's own tag rules gave it. Both are read from what was recorded when
each message was placed, so a page costs work for its own messages only. The read uses a
read-only view of the session as stored: it never takes the session's lease or occupies its
actor, and with an actor open it sees what the actor has made durable.

```sh
broca-session --subc <connection file> --project-root <root> --harness <harness> \
  --session <session> tag-map --out map.json   # {"session": "<session>", "rows": [...]}
```

## External session and call identities

New external session references use the llm-runner v2 derivation over
`{runner: "broca", harness, session}`. Folder spelling is not part of that reference.
The session's storage address, lease and store identity remain the full
`(project_root, harness, session)` triple. At startup, broca maps v2 references to
their sessions and continues to accept the older folder-based v1 references. A v2
reference shared by multiple roots is ambiguous and does not resolve to an arbitrary
session. Queued events keep the reference from their body; their publication headers
carry that same reference. Provider conversation IDs remain scoped
to the full triple and are not the external session references described above.

With `call_identity`, display-history tool-call parts and live `tool_started` and
`tool_finished` frames carry the durable call's `call_key` and its recorded
`schema_pin`. Ordinary keys are `<lineage id>:<ToolIntent sequence>`; approved calls preserve
the preparation key recorded on their `ToolIntent`. Plan sessions freeze actual provider
catalog pins at admission. A later catalog cannot change a recorded pin.
Legacy non-plan tools and older manifests have no recorded provider semantics, so
their pins remain absent rather than being invented from the argument schema.
Default `session.read` and `run.status` gain no call-identity members.
For approved calls only, existing `session.read` attribution and tool-result delivery
keys now preserve that recorded preparation key instead of incorrectly naming the later
dispatch intent. `run.tool_calls` already preserved it. Ordinary keys and all `run.status`
bytes remain unchanged.

An error `session.head` can carry `last_run_error: {class, retry_after_secs?,
resets_at_ms?, provider_code?}`. Its values come from the error `run.status` reports
for the same named run, with the same caller-facing class mapping. Fractional retry
delays are preserved; a missing delay remains absent. `provider_code` copies that
run error's `provider_code` string verbatim, including `billing_exhausted` and
`account_limit_exhausted`; an error without a code leaves the member absent.
Paused, interrupted, successful and never-written sessions carry no error summary.

Compaction Setup continues to omit `ride_wait_max_ms`, `view_apply` and child-run
`session_retention`: broca does not promise provider-chosen ride waits, marked
next-step view application, or storage policies for a compaction provider's child runs.

## Mode-3 compaction

A log whose first `RunStart` carries `compaction_plan_version` uses mode-3 semantics.
New compaction sessions use one session-scoped `compaction-provider/v2` route. Broca declares
`compaction-runner/v1`; a route for that role is authorized only for the provider in the
session's effective plan, not a pending refresh. Provider, preset and params remain fixed for
the session's lifetime.

Setup is content-free. A mode-3 compaction send must resolve a real context limit before
admission; otherwise it refuses `invalid_params`, field `context_limit`, without writing.
Frozen legacy sessions do not acquire that prerequisite. An invalid frozen mode-3 run that
reaches execution without a limit ends `compaction_unavailable` before model I/O, naming the
missing limit. Setup reports the frozen model, window, output reserve, library rules,
all supported ride causes, applied view and newest entry. A refusal, unknown answer or timeout
without an applied view ends the run `error` with `provider_code: compaction_unavailable`
before any model call; Setup is retried before the next send. With an applied view, that view
remains usable.

The provider reads `session.entries`, pushes `session.apply_view` and reads
`session.applied_view`. Acceptance means the full validated view is durable, not yet applied.
Application occurs only at a step boundary and is recorded separately, including ride answers
used for that same request. Notices, applied-view reads and `generated_under` name the applied
view. Notices are serialized, coalesced in memory, and flushed completely before every ride;
they create neither records nor bus outbox work. broca sends a request without sizing it
first: a provider's context-length refusal asks for a fold (a ride with cause `over_limit`)
and resends once, and a second refusal ends the run `context_ceiling`.

When the provider catalog that decides an entry's attribution cannot be read,
`session.entries` refuses with `projection_not_ready` and `detail.class: "transient"`, for
ordinal, message-id and tag selections alike. The caller may retry; this refusal does not mean
an empty transcript or missing ids/tags.

Numbered parts follow the latest host marker's checkpoint. A marker naming another harness or
session refuses the log on load. Tool-result deliveries are a separate `delivered` list, with
sender and template attributed by submission id; the render plan edits the stored output before
appending kept deliveries. Every first-sent result kind can carry a queued tool-result send.

Each run freezes the view library's plan version. An unfinished run cannot resume under a
different version. A new run after completed older-version history freezes the current version
and renders history under the new plan, with one prompt-cache rewrite rather than a permanent
session freeze.

Legacy logs never enroll in v2. They render their last applied v1 view, or raw history if they
never applied one, and drop an outstanding v1 `view_due`. They make no v1 provider calls or new
notice/outbox work, and decide an over-window request themselves without waiting.

### ChatGPT chain resets

Before rendering a ChatGPT step, a known missing response chain can request a bounded
`compaction.ride` RPC with cause `chain_reset`. Age retirement uses the transport's 40-minute
decision, and run-start acquisition supplies its existing idle/absent-socket decision. The
transport records why the socket has no usable completion receipt in the attempt's
`full_input_reason` (for example, `new_connection_age`). The pre-render request and the
attempt use that same retirement decision, not an elapsed-gap guess. A retry or refusal
discovered during sending does not re-enter this boundary and never announces a new
chain-reset request. Cache warms neither produce these signals nor replace the previous model
request's [cache-evidence fingerprint](cache-evidence.md).

## Retired ride-placement trial

New runs always render a ride (text a send delivered with a tool result) inside its carrying
result. The 12 November project-folder trial has ended; the state-root `experiments.json` file
is no longer read and cannot change admissions.

Runs admitted while the trial switch was on still carry `ride_placement: "after_results"` in
their frozen render config. Decoding and rendering that value remain supported so resumes and
rebuilt requests reproduce their recorded bytes: each ride is its own text block after all of
the turn's tool results, which hold only the tools' own output. This compatibility can be
removed once those stored runs no longer need rendering. An older build that drops the member
refuses such a resume (`frozen_render_config_drift`) rather than using the other placement.
