# broca's on-disk data model

This page describes what broca writes to disk, for modules that read broca's state directly
rather than through its RPC surface. Everything here was read from source. Where this page and
the code disagree, the code is right, and this page is the bug.

Section 6 says which items are stable contracts and which are internal. Section 7 lists the
rules every external reader must follow. Read both before building on anything else here.

---

## 1. The state root

All state lives under one directory, the **state root**:

- `$BROCA_STATE_ROOT` when that variable is set and non-empty, used exactly as given;
- otherwise `<data home>/cortexkit/broca`, where the data home is `XDG_DATA_HOME`, else (on
  Windows) `APPDATA` or `USERPROFILE\AppData\Roaming`, else `HOME/.local/share`;
- otherwise broca refuses to start.

Older builds defaulted to `broca-state` beside the subc connection file. When only that
directory holds state and `BROCA_STATE_ROOT` is unset, broca refuses to start and names both
paths rather than begin an empty history (`crates/broca-module-serve/src/state_root.rs`).

```text
<state root>/
  session-store/            sessions, one folder set per storage class      (section 2)
  session-store-durable/    sessions selected for backup at creation
  session-store-ephemeral/  sessions with a retention policy, never backed up
  session-store-moves/      sessions part-way through a move: journals, staged copies, fences
  run-index.db              SQLite: session-ref index, scope reservations,   (section 3)
                            bus outboxes, usage export facts
  system-one.db             SQLite: System One decision batches and their facts
  grants.db                 SQLite: consent grants and held tool calls
  late-results.db           SQLite: late-result intake registrations
  media-uploads/            uploads waiting to be attached to a send
  agent-data/               state of agent moves to other computers (cuts and their fences)
  leases/                   per-session writer leases
  catalog/fusiform-lkg.json last-known-good model catalog
  import-sources/           where session.import reads other harnesses' logs by default
  request-dumps/            request bodies written by the operator's request dump
  logs/                     daily log files
  state-format              format marker, when present; naming the removed broca-state/2 refuses startup
  feature-floor             storage formats this root's sessions use; placement checks it
  broca.lock                held by the one ck-broca serving this root   (below)
```

A root that served a build before 0.4.112 may still hold folders named `wal*`, `media`,
`media-durable`, `media-ephemeral` or `conversion-*`, a `conversion-ledger.db`, a `caps.db` and a
`wal-feature-floor`. broca reads none of them; `scripts/migrate-feature-floor.py` moves the floor
and removes the rest.

Only one `ck-broca` process may serve a state root. At boot, before it touches anything else
in the root, it takes an exclusive OS advisory lock on `broca.lock` (`flock` on Unix,
`LockFileEx` on Windows) and holds it until it exits; the kernel releases it however the
process dies. A second process finding the lock held logs one ERROR naming the root and exits
non-zero at once, without writing to the root. The file holds no data and never marks a root
as holding state. `broca-session` does not take it
(`crates/broca-module-serve/src/state_root_lock.rs`).

Retention publishes an expiry marker at
`session-store-ephemeral/expired/<addr>/<session_id>` before deleting a session's head, logs
and objects, under the same session lease. The marker remains after the address is reused:
reads of deleted runs stay `expired`, while a send can create a fresh session that carries the
deleted sessions' tag floors. `session-store-ephemeral/retention-deadlines.json` keeps the
deadline retention found for each session, with the position it read the session at, so the
first pass after a restart reads only the sessions written to since. It holds addresses,
positions and times, never content, and it is a cache: a missing or unreadable file means
every session is read again. Both are internal storage, not an external-reader contract.

broca creates its folders and files owner-only (`0700` directories, `0600` files), and restricts
`run-index.db` and its `-wal`/`-shm` sidecars to `0600` when it opens them. A reader must run as
the same user.

---

## 2. Sessions

A session is identified by its **bind triple** `(project_root, harness, session)`. Its folders
are named by `addr`: **FNV-1a 64-bit** over the UTF-8 bytes of

```text
project_root + "\x1f" + harness + "\x1f" + session
```

(the separator is U+001F, UNIT SEPARATOR, one byte), with offset basis `0xcbf29ce484222325`
and prime `0x100000001b3`, rendered as **16 lowercase hex digits, zero-padded**. Pinned
example: `("/tmp/p", "runner", "s1")` → `31a7840439280f40`.

```python
def addr(project_root, harness, session):
    h = 0xcbf29ce484222325
    for b in f"{project_root}\x1f{harness}\x1f{session}".encode():
        h = ((h ^ b) * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
    return f"{h:016x}"
```

`project_root` is rendered as broca renders a path for display. A field containing U+001F is
rejected at bind, so the encoding is unambiguous. Changing any component of the triple
addresses a different session.

In each `session-store*` root, a session has a head (`heads/<addr>/head`, with every earlier
version kept as `heads/<addr>/v/<version>` for a restore from a backup), append-only logs under
`logs/<addr>/`, and immutable parts under `objects/`. A session's storage class is chosen when
it is created. [The session store](session-store.md) describes the layout, the records and how
a run is written and resumed.

**Session contents are not an external-reader contract.** An encrypted session's records are
sealed with its own key, so only broca can read them. Read a session through broca's RPC
surface instead: `session.read` for the transcript, `session.entries` for entries by id,
`session.head` and `run.status` for state.

---

## 3. SQLite stores

broca writes the SQLite database `run-index.db` described in this section. It uses
`journal_mode=WAL`, so recent commits sit in the `-wal` sidecar until SQLite checkpoints them
(see section 7). broca caps that sidecar at 64 MiB after a checkpoint resets it.

`run-index.db` is written by one connection per daemon. Its schema is created when the daemon
opens it (`CREATE TABLE IF NOT EXISTS`).

| Tables | What they hold |
|---|---|
| `agent_data_session_ref`, `agent_data_session_ref_v2`, `agent_data_session_ref_v2_rule`, `agent_data_census*` | The session-ref index: which stored session each agent-data ref names, so an agent move finds sessions without reading every head, and the census that fills it from the session store. No session content. |
| `session_scope_reservation` | One row per address a scoped bind reserved, with its owner and scope ref. |
| `bus_event_outbox`, `send_state_outbox` | Bus publications waiting to be sent, recorded once the records they describe are durable. |
| `route_fact_outbox` | The `run_finished` report each run routing admitted owes routing, stored with the run's end. Settled when the run's own report reaches routing; otherwise published as a `route_fact.v1` event after two minutes. |
| `export_facts`, `export_meta`, `export_imported_runs` | The usage export (below). |

None of these holds a session's records. Where a table is derived from them, the records win.

### 3.1 `export_facts`

Usage facts for metering.

| Column | Type | Notes |
|---|---|---|
| `export_seq` | INTEGER PRIMARY KEY AUTOINCREMENT | Insertion order. Increasing but **not dense**. |
| `fact_id` | TEXT NOT NULL UNIQUE | The fact's identity (below). Independent of usage and time. |
| `run_id` | TEXT NOT NULL | Indexed (`export_facts_run_id`). Empty for a flow's System One fact. |
| `segment_json` | TEXT NOT NULL | The fact. |

`segment_json` holds one of three kinds of fact. A reader dispatches on its `kind` member:

**Segment facts** (no `kind`): one billing-homogeneous slice of a finished run. Fields:
`run_id`, `session` (identity object), `segment_ordinal`, `fact_id`, `provider`, `model`,
`auth_selection` (credential id or null), `account_ref`, `resolved_credential_id`,
`vault_request`, `work_class` (these four absent when unset), `charge_basis` (`metered_api`,
`subscription_included`, `unknown`), `charge_basis_mapping_version`, `charge_basis_overrides`
(`"none"` or a 64-hex digest), `usage_policy_version` (absent unless the run used the
separate-reasoning normalizer), `usage` (section 3.3), `usage_by_service_tier` (absent when no
step reported a tier; buckets need not sum to `usage`), `retries_used`, `pricing_at_ms`,
`pricing_at_precision`, `occurred_at_ms`, `occurred_at_precision`, `terminal_reason` (final
slice only), optional `agent_id` and `agent_run_id` for a daemon-stamped agent run, optional
`warm_outcome` on a keep-warm fact, and `usage_in_attempts` (below). `fact_id` is SHA-256 (hex)
over the length-prefixed source instance id, triple, run id and segment ordinal.

A slice ends where the run's frozen config or credential is frozen again, and where a model
call is answered by another model than the slice's: a step routed to the refusal-fallback
model or served by an Anthropic server-side fallback. The next slice's `model` is the model
that answered. For a session whose frozen storage root is exactly `broca-unscoped-v1`, the
`session` object omits `project_root`: the namespace is not a project. Fact ids still hash the
full storage triple.

**Attempt facts** (`kind: "model_attempt"`): one per provider attempt of a run, failed and
cut-off attempts included. Fields: `fact_id`, `run_id`, optional `agent_id` and `agent_run_id`,
`session`, `step_id`, `attempt`, optional `attempt_id`, `outcome`, optional `http_status`,
`provider`, `model`, optional `usage_by_model` (each entry: `provider`, `model`, `usage`,
optional `served_service_tier`, and the `segment_fact_id` of the segment that also counts it),
`reasoning_included_in_output`, optional `usage_policy_version`, `requested_service_tier`,
optional `served_speed`, `content_received`, `response_bytes`, optional `auth_selection`,
`resolved_credential_id`, `account_ref`, `served_account_ref`, `charge_basis`,
`charge_basis_mapping_version`, `charge_basis_overrides`, optional `sent_at_ms`,
`recorded_at_ms`, and optional `input_transformations`. `fact_id` is SHA-256 (hex) over the
length-prefixed source instance id, triple, run id and the literal `model_attempt`, then the
attempt record's sequence number as a little-endian `u64`.

A run exported with attempt facts marks its segments `usage_in_attempts: true`. A reader prices
the attempts and records such a segment without pricing it, so no usage is counted twice. A run
without attempt facts has no marker, and its segments price as before. Don't infer the rule
from finding attempts: a reader may meet them before or after their segment.

**System One facts** (`kind: "system_one"`): one per decision item, with no session. Fields:
`fact_id`, `batch_id`, `item_index`, `provider`, `model`, optional `auth_selection` and
`account_ref`, exactly one of `run_id` or `flow_id`, `charge_basis`,
`charge_basis_mapping_version`, `charge_basis_overrides`, `usage` (only reported `input_tokens`
and `output_tokens`), and `occurred_at_ms`. Their ids use domain-separated, length-prefixed
SHA-256 over `system_one/v1`, source instance id, role, batch id, first-admission time and item
index. `system-one.db` commits the exact fact bytes to an outbox in the same transaction as the
paid outcome; publishing inserts once into `export_facts`, then acknowledges the outbox.

**Writers.** After a run's end is durable, the session actor derives the run's segment and
attempt facts and inserts them in one transaction, then records the run billed in the session.
A run stays owed in the session's records until that billed record lands, so a crash before it
bills the run again at the session's next open, and the boot sweep opens every session that
still owes billing. An existing `fact_id` is **never overwritten**: inserts are `INSERT OR
IGNORE`, and a re-derivation that differs is logged while the stored row stays.

**Source of truth?** The stored rows are the authoritative record of spend: consumers have read
and cursored past them, and deriving an old fact again today can produce different bytes (a
newer mapping version, added fields) and a different `export_seq`.

### 3.2 `export_meta`

A single row (`id = 1`) naming this export stream.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PRIMARY KEY CHECK (id = 1) | |
| `source_epoch` | INTEGER NOT NULL DEFAULT 1 | Written as 1 and never changed. |
| `source_instance_id` | TEXT NOT NULL | Minted once when the database is created (UUID-shaped). Part of every `fact_id`. |
| `high_water` | INTEGER NOT NULL DEFAULT 0 | `MAX(export_seq)` after the latest fact batch. |

Consumers read `export_facts` with SQLite `mode=ro`. Incremental readers retain
`(source_instance_id, source_epoch, export_seq)`. A new `source_instance_id` means a different
export database, and cursors from the old one must be reset. `sqlite_sequence` also exists;
SQLite maintains it because of AUTOINCREMENT.

### 3.3 Usage

`Usage` is an object with seven optional `u64` keys:

| Key | Meaning |
|---|---|
| `input_tokens` | Fresh, **non-cached** input tokens. Disjoint from `cached_input_tokens`; do not subtract. |
| `cached_input_tokens` | Cache-read input tokens. |
| `cache_write_tokens` | Cache-creation tokens. |
| `cache_write_5m_tokens` | Provider-reported 5-minute cache writes, a subset of `cache_write_tokens`. |
| `cache_write_1h_tokens` | Provider-reported 1-hour cache writes, a subset of `cache_write_tokens`. |
| `output_tokens` | Output tokens, **including** reasoning. |
| `reasoning_tokens` | A **subset** of `output_tokens`; never add the two. |

**Absent versus zero.** A key is absent when the provider did not report that quantity. A key
with value `0` is a reported zero. `{}` means nothing was reported. When usages are summed (a
segment), a key stays absent only if no step reported it; if some steps reported it, the sum
covers those steps only, a lower bound.

**Cache-write lifetimes.** A fact carries both lifetime fields or neither; when present, their
sum equals `cache_write_tokens`. An absent lifetime is unknown and is never inferred from the
total or the requested cache lifetime. A response whose split is incomplete, overflows or
disagrees with its total has neither field.

**Historical boundaries.** Facts with `export_seq ≤ 122061` have all five original keys always
present, with an unreported quantity written as `0`; facts above it omit unreported keys. From
the first release after v0.3.141, OpenAI Responses and ChatGPT/Codex steps carry
`cache_write_tokens`, and their `input_tokens` excludes those written tokens; on earlier facts
the key is absent because broca did not map it, and `input_tokens` still includes them.

---

## 4. Timestamps

Every time value broca stores is **wall-clock Unix milliseconds** from the system clock of the
writing machine. None is monotonic, and none is used for ordering or identity. Order in the
export comes from `export_seq`.

- `occurred_at_ms`: the run's end time. Earlier slices of the same run get the same time.
  Absent on a closed, historical set of facts in the backfill band `export_seq` 1..5473;
  `occurred_at_precision` says `terminal` even then, so test `occurred_at_ms` for presence.
- `pricing_at_ms`: when the slice's config was frozen (the run's start, or the moment it was
  frozen again), falling back to the end time. `pricing_at_precision` is `exact` for the first
  case and `degraded` for the fallback.
- `sent_at_ms` and `recorded_at_ms` on an attempt fact: when the request was sent, and when
  broca recorded the attempt's end.
- None of these is the time the row was inserted. A run billed again after a crash inserts its
  facts late, so `export_seq` order is not time order.

---

## 5. Where a given fact lives

| Question | Read |
|---|---|
| Spend, per run or per period | `export_facts.segment_json` and its attempt facts (a stable contract). |
| Per-attempt outcome, timing, account | Attempt facts. |
| Run state by `run_id` | `run.status` over RPC. |
| When a run ended | The fact's `occurred_at_ms`. |
| Which folders are a session's | FNV-1a of the triple (section 2). |
| The transcript | `session.read` over RPC. |

---

## 6. Stability

**Stable** means external readers may rely on it: changes are additive (new optional fields),
or come with a new version marker. **Internal** means it may change without notice.

| Item | Status | Why |
|---|---|---|
| State-root path and the `run-index.db` path | Stable | The export lane's documented surface names the database path. |
| Session address: FNV-1a of the U+001F-joined triple, 16 hex digits | Stable | It names every existing session's folders; a change would orphan every session. Pinned by a test against an independent implementation. |
| Session store layout and record encoding | Internal | Encrypted sessions can't be read outside broca; read through the RPC surface. |
| `export_facts` table and `segment_json`, all three kinds | Stable | The documented metering contract. Facts are immutable; new fields are optional and change neither existing bytes nor `fact_id`. |
| `export_facts.export_seq` as an increasing cursor | Stable | Consumers cursor on it. It is not dense. |
| `export_meta` | Stable | Readers use `source_instance_id`, `source_epoch` and `high_water` to detect a replaced database and track incremental reads. |
| `Usage` keys and the absent-versus-zero rule | Stable | Shared with the export lane contract. |
| Session-ref index, scope reservations, outboxes | Internal | Operational state. |
| `system-one.db`, `grants.db`, `late-results.db` | Internal | Each is its own authority, read only by broca. |
| `leases/`, `catalog/`, `logs/`, `request-dumps/`, expiry markers, retention deadlines | Internal | Process state and diagnostics. |

---

## 7. Rules for external readers

1. **Never write.** Open files read-only, and never delete or rename anything in the state
   root.
2. **Open SQLite read-only with `mode=ro`**, for example `file:/…/run-index.db?mode=ro`, or
   `SQLITE_OPEN_READ_ONLY`. **Never `immutable=1`** on the live database. It tells SQLite to
   ignore the `-wal` sidecar, which silently hides every committed row not yet checkpointed,
   and the data just looks old. (`immutable=1` is only correct on a static copy that was
   checkpointed before it was copied.) The `-wal` and `-shm` sidecars persist across broca
   restarts, so a read-only connection can open while broca is down.
3. **Keep read transactions short.** A long-open read transaction stops SQLite from
   checkpointing, and the `-wal` sidecar grows while it is open.
4. **Read spend from `export_facts`.** It already applies slicing, charge basis, credential
   labels and the absent-versus-zero rule. Price attempts and skip segments marked
   `usage_in_attempts`, as section 3.1 says.
5. **Decode usage keys as optional.** Absent is not zero (section 3.3).
6. **Skip what you don't know.** Unknown JSON fields, unknown `kind`s and unknown enum strings
   (`outcome`, `charge_basis`, `vault_request.op`) are expected as broca grows.
7. **Read sessions through broca.** Don't parse the session store's files.
