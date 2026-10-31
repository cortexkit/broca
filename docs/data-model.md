# broca's on-disk data model

This page describes everything broca writes to disk, for modules that read broca's state
directly rather than through its RPC surface. Everything here was read from source. Where
this page and the code disagree, the code is right, and this page is the bug.

Section 7 says which items are stable contracts and which are internal. Section 8 lists the
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
  wal/<addr>.wal            one append-only WAL per session              (section 2)
  wal-durable/<addr>.wal    live WALs selected for backup at first send
  wal-ephemeral/<addr>.wal  retained WALs, never backed up or folded
  wal-archive/              sealed archive containers of cold WALs       (section 3)
    fold-<unix-ms>.ark
    fold-<unix-ms>.journal
  wal-archive-durable/      sealed containers of durable WALs only
  run-index.db              SQLite: run index, materialized transcript,   (section 4)
                            admission checkpoints, usage export facts
  caps.db                   left over from removed spend caps; not opened (section 4.6)
  wal-manifest              diagnostic address -> identity sidecar         (section 2.7)
  leases/                   per-session lease files
  catalog/fusiform-lkg.json last-known-good model catalog
  logs/                     daily log files
  request-dumps/            debug request dumps
  broca.lock                held by the one ck-broca serving this root   (below)
```

Only one `ck-broca` process may serve a state root. At boot, before it touches anything else
in the root, it takes an exclusive OS advisory lock on `broca.lock` (`flock` on Unix,
`LockFileEx` on Windows) and holds it until it exits; the kernel releases it however the
process dies. A second process finding the lock held logs one ERROR naming the root and exits
non-zero at once, without writing to the root. The file holds no data and never marks a root
as holding state. The command-line tools (store-maintenance, wal-fold, broca-session) do not
take it (`crates/broca-module-serve/src/state_root_lock.rs`).

The WAL is the source of truth. Everything in `run-index.db` is derived from it, although
not all of it can be regenerated (section 4 says which parts cannot).


broca creates the WAL directory, WAL files and archive containers owner-only (`0700`
directories, `0600` files), and restricts `run-index.db` and its `-wal`/`-shm` sidecars to
`0600` when it opens them. A reader must run as the same user.

---

## 2. The WAL

### 2.1 Location and naming

Each session has one WAL file:

```text
<state root>/wal/<addr>.wal
```

Durable sessions use the sibling `wal-durable/` tree, and sessions with retention
use `wal-ephemeral/` regardless of class. The first send freezes `session_class`;
the daemon's `backup_classes` setting (default `["head"]`) chooses whether that
new session is durable. Existing live/archive placement is authoritative after
configuration changes. Readers resolve all three live trees before consulting
`wal-archive/` and `wal-archive-durable/`; duplicate live copies are an error,
not a priority rule. Only the durable trees are captured by Engram, the backup service.

A session is identified by its **bind triple** `(project_root, harness, session)`. `addr`
is **FNV-1a 64-bit** over the UTF-8 bytes of

```text
project_root + "\x1f" + harness + "\x1f" + session
```

(the separator is U+001F, UNIT SEPARATOR, one byte), with offset basis
`0xcbf29ce484222325` and prime `0x100000001b3`, rendered as **16 lowercase hex digits,
zero-padded**. Pinned example: `("/tmp/p", "runner", "s1")` → `31a7840439280f40`.

```python
def addr(project_root, harness, session):
    h = 0xcbf29ce484222325
    for b in f"{project_root}\x1f{harness}\x1f{session}".encode():
        h = ((h ^ b) * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
    return f"{h:016x}"
```

`project_root` is rendered as broca renders a path for display. A field containing U+001F
is rejected at bind, so the encoding is unambiguous. Changing any component of the triple
addresses a different file.

The filename is the only address. broca never checks the triple embedded in the WAL's
`run_started` records against the filename. A WAL copied or renamed to another address is
read as that address's session. So the embedded triple records where a session was born,
not where it lives now.

A WAL can be **zero bytes long**: a lineage opened before lineage frames existed and never
run, for example. A present, empty file is a session with
no records. It is not an error, and it counts as "live" for archive resolution (section
3.3).

Only files named `<16 lowercase hex>.wal` are WALs. While restoring an archived session,
broca writes a temporary `.extract-<pid>-<n>.tmp` file in its original live tree and renames it into
place; ignore anything else in the directory.

### 2.2 Frame layout

A WAL file is a sequence of frames with no file header. All integers are
**little-endian**. Every frame has a fixed 53-byte header followed by its payload:

| Offset | Width | Field | Meaning |
|---:|---:|---|---|
| 0 | 4 | `len` | `u32`, payload length in bytes. Never more than 64 MiB (67,108,864). |
| 4 | 1 | `ver` | `u8`, frame kind (below). |
| 5 | 8 | `seq` | `u64`, record sequence number. |
| 13 | 8 | `fence` | `u64`, lease-tenure stamp of the writer. |
| 21 | 32 | `digest` | SHA-256 over `ver ‖ seq(8 bytes LE) ‖ fence(8 bytes LE) ‖ payload`. |
| 53 | `len` | `payload` | See `ver`. |

`len` is not part of the digest. The frame's total size is `53 + len`.

**`ver` values in use today:**

| `ver` | Frame | `seq` | `fence` | Payload |
|---:|---|---|---|---|
| 1 | record | ≥ 1 | writer's lease tenure | One `WalRecord` as UTF-8 JSON (section 2.4). |
| 2 | lineage | always 0 | always 0 | Exactly 16 opaque bytes: the lineage id (section 2.3). `len` is always 16, so the frame is always 69 bytes. |
| 3 | gated record | ≥ 1 | writer's lease tenure | `{"requires": [feature name, ...], "record": WalRecord}` as UTF-8 JSON. |

**Gated records (`ver = 3`).** A record an older reader must not skip or misread is written
in a `ver = 3` frame. `requires` lists the features it uses, as names like
`hook-output/v1` (`[a-z0-9-]` then `/v` and a number), sorted. A reader checks every name
against its known-name table before it interprets `record`; if any is missing, it refuses
the whole WAL as **NewerWriter** (a newer build wrote it), which is neither corruption nor
a skip: nothing repairs, truncates or folds such a WAL. broca's table is printed by
`ck-broca --wal-known-features`. It holds `plan-manifest/v1` (the frozen plan manifest on
`run_started`), `scope/v1` (the frozen scope on `run_started`), `steer-queue/v1` (the
`steered` record) and `dispatch-module/v1` (`module_id` and `schema_pin` on
`tool_dispatch_intent`). A record with nothing to require is always written as `ver = 1`.
The encoder adds a record's own requirements itself, so a `run_started` with a `scope` or a
`manifest`, a `tool_dispatch_intent` with a `module_id` or a `schema_pin`, and every
`steered` record, can only be written as `ver = 3`.
Builds before the gate refuse `ver = 3` as "unsupported WAL framing version 3".

**The feature floor.** Before the first frame that requires a feature, the writer adds the
feature to `wal-feature-floor` in the state root (next to `wal/`): one name per line, sorted,
written to a temporary file and renamed over the old one, and only ever grown. If the floor
cannot be raised the append is refused and nothing is written, so no gated frame is ever on
disk without the floor naming it. `scripts/place-release.sh` refuses to place a build whose
known-name table does not cover the floor.

Any other `ver` is corruption, and so is a `ver = 2` frame with `len ≠ 16`, `seq ≠ 0` or
`fence ≠ 0`. `ver` is the framing version, not the record schema version. The record
schema version (currently 2) is not written anywhere in the file.

**Sequence numbers.** Record frames are numbered from 1, contiguous and gapless within a
file. Lineage frames do not consume a number. broca refuses a file whose record
sequence does not start at 1, has a gap, or goes backwards.

**`fence`** is the lease epoch of the process that wrote the record. It exists only for
forensics. Nothing compares it or rejects an append because of it.

**Tail and corruption rules** (how broca decodes; readers should do the same):

- Fewer than 53 bytes left, or a header whose payload runs past end-of-file, is a **torn
  tail**: an append that was not finished (it crashed, or it is still being written). Stop
  there. It is not an error.
- A complete frame whose digest does not verify, whose JSON does not decode, or whose `ver`
  is unknown is **corruption**. broca fails loudly on it.
- Only the writer, when it reopens the file, truncates a torn tail. Readers never do.

### 2.3 Lineage id

The lineage id is 16 random bytes, minted from the OS entropy source. Bits are set so it
looks like a version-4 UUID. Treat it as opaque. broca displays it in UUID form
(`8-4-4-4-12` lowercase hex), as it appears in `meta.lineage_id` and in `session.read`.

- **When it is minted:** when a WAL file is **created**. The first frame of every file
  created by v0.3.64 or later is a lineage frame. A file created earlier gets its id at its
  first append after upgrade, when a lineage frame is written just before that record, so
  in such a file the first lineage frame is in the middle.
- **Where it lives:** in `ver = 2` frames. Every append writes the record frame **followed
  by a copy of the lineage frame**, in one write. So a normal file alternates record and
  lineage frames, and its **last complete frame is usually a lineage frame, not a record**.
- **What it means:** the same id means "same log, continued". A different id at the same
  address means the file was deleted and re-created. A file carrying two different ids is
  refused as spliced.

### 2.4 Records

The payload of a `ver = 1` frame is one JSON object, internally tagged by `"type"` (snake
case). Field rules in the tables below:

- **absent** means the key is left out when there is no value (`None`/`false`/empty), and
  reads back as absent/`false`/empty when missing. Absent never means zero.
- **defaulted** means the key is always written, and older records that lack it read as
  the stated default.
- **required** means the key is always present.

Identifier types are JSON strings (`run_id`, `submission_id`, `batch_id`, `tool_call_id`)
except `step_id`, which is a JSON number. `session` is the bind triple as an object:
`{"project_root": str, "harness": str, "session": str}`.

#### Run lifecycle

**`run_started`**: opens an episode (one run). Written atomically with its whole config
and input, and synced before the first provider call. A resumed run (after a crash or a
pause) does **not** write a new `run_started`; it continues under the same `run_id`.

| Field | Type | Rule | Meaning |
|---|---|---|---|
| `run_id` | string | required | |
| `session` | triple object | required | The bind triple at admission. |
| `project_id` | string | absent | Client-supplied, recorded verbatim, not validated. |
| `ts_ms` | u64 | absent | Wall-clock Unix ms at admission. Absent on records from before metering. |
| `config` | `RunConfig` object | required | The request config frozen for this run (below). |
| `input` | array of `Message` | required | The user input for this run. |
| `origin` | object tagged by `kind` | defaulted `{"kind":"fresh"}` | `fresh`, or `seeded_from` (import/fork) with `origin_kind` (`import`/`fork`), `source_ref`, optional `origin_device_pseudonym` (16 bytes), optional `base_seq`. |
| `submission_id` | string | absent | Set when the run consumed a queued submission. |
| `title` | string | absent | The human-readable title the `session.send` that opened this run carried (for a drained queued send, the title its `queued` record carried): 1-200 Unicode scalar values, not blank, no control characters. Informational only and ungated: never rendered into a provider request and not part of routing, cache keys or the request identity digest. Absent when the send carried none and on records written before it existed. |
| `scope` | `{owner, ref, scope_epoch}` | absent | **Must-understand, feature `scope/v1`.** The scope the session was admitted under, as the subc daemon stamped it on the bind: `owner` is a principal object (`{"kind":"reserved","module_id":…}`), `ref` a string, `scope_epoch` a u64. Frozen at the session's first scoped episode and repeated on every later one. A record carrying it is written only in a `ver = 3` frame requiring `scope/v1`; an unscoped record stays `ver = 1`. Written by admission after its owner check (the scope a send freezes or adopts, or the one the session already froze); the broca release before the one that first writes it already reads it, so rolling back one release still reads the session. |
| `manifest` | object | absent | **Must-understand, feature `plan-manifest/v1`.** What a session's first send fetched from its tool providers, frozen so every later episode and resume renders from it without fetching again: `plan` (the first `session.send`'s `plan` member exactly as received, opaque), `plan_identity` (SHA-256 of the plan's RFC 8785 (JCS) bytes, 64 lowercase hex), `composition_digest` (64 lowercase hex, the digest the plan stated and the writer verified), `fetches` (array, one per provider fetched, sorted by provider id bytewise; each is flat: an answered fetch is `{provider, catalog_digest, tools, text}` with `tools` exactly as the provider answered (empty for a provider asked only for system text) and `text` the system text it answered, omitted when no system-text item was asked of it and `""` when present and empty; an absent fetch is `{provider, absent}`, an optional text-only item that did not answer, with `absent` an open reason string such as `timeout`, `unknown_module` or `refused:<code>`; a fetch with both `catalog_digest` and `absent` does not decode, and one with neither decodes as a state from a later build and is never written), `tools` (array of `Tool`, as in `RunConfig.tools`, in the order offered to the model), `system_text` (the joined system text), `system_digest`, `join_version` (u32) and `role_versions` (object, role name to version string). The plan's own layout is never WAL schema, so the plan can grow without a WAL change. `plan` is opaque JSON stored as a JSON **string** holding the JSON text, so its key order, number spellings and escapes survive a decode exactly (an embedded value would not: `run_started` is decoded through serde's internally tagged buffer, which re-sorts keys and re-spells numbers). A record carrying it is written only in a `ver = 3` frame requiring `plan-manifest/v1`. Written on the `run_started` of every episode of a session whose first send carried a fetch plan (the first episode freezes it; later ones repeat it, so episodes left after a fold still carry it); the WAL raises its feature floor to `plan-manifest/v1` before the first such frame. The system texts are joined in plan order with `"\n\n"` under `join_version` 1, an empty text adding neither bytes nor a separator. |

`RunConfig` fields: `model` (`{provider_module_id, model_id}`), `tools` (array, order
significant), `tool_choice`, `generation`, `response_format`, `context_epoch` (u64),
`renderer_version` (u32), all required; `provider_options`, `cache_tiers`,
`frozen_render_config` (opaque JSON, absent when null); `auth_selection` (absent, below);
`tools_were_omitted` (bool, absent when false); `budget`, `context_limit` (u64),
`work_class` (string), `variant` (string), `service_tier`, `context_management` (all
absent); `stop_when` (array, absent when empty). `frozen_render_config`,
`provider_options` and `cache_tiers` are opaque provider blobs (see section 7). Note that
`frozen_render_config` can contain a `project_id` key of its own (Google Code Assist's
GCP project), which is not `run_started.project_id`.

**`run_finished`**: closes the episode (not the session: a later `run_started` can open
the next one). Synced before the caller sees the terminal.

| Field | Type | Rule | Meaning |
|---|---|---|---|
| `reason` | string | required | `completed`, `max_steps`, `cancelled`, `interrupted`, `error`, `transform_unavailable`. |
| `ts_ms` | u64 | absent | Wall-clock Unix ms when the terminal record was written. |
| `usage` | `Usage` | required | The run's usage summed over its model steps (section 2.5). |
| `error` | `ProviderError` | absent | Present only when `reason` is `error` or the terminal has a typed cause. |
| `retries_used` | u32 | defaulted 0 | Provider retries spent over the run, including retries of a step that then failed. |

`ProviderError`: `class` (`transient`, `permanent`, `auth_required` (older records may say
`auth`), `context_overflow`, `provider_unavailable`), `message`, and absent-when-unset
`status` (u16), `retry_after_secs` (float), `provider_code`, `provider_subcode`, `resets_at_ms`
(u64), `limit_window`, `account_ref`, `credential_id`, `affordable_max_tokens` (u32), and
`provider_details` (object). Google `provider_details` retains bounded `errorInfo` (`reason`,
`domain`, string-valued `metadata`), `quotaFailure.violations` (`subject`, `description`,
`quotaMetric`, `quotaId`, string-valued `quotaDimensions`, `quotaValue`, `futureQuotaValue`), and
`retryInfo.retryDelay` fields from `google.rpc.Status.details`; it omits unrecognized fields and
is absent when no recognized details exist. Most text values are capped at 128 Unicode scalar values;
`subject` and `description` are capped at 256. Metadata has at most 16 keys, quota dimensions at most 8,
and there are at most 8 violations. A record carrying it is MUST-UNDERSTAND (feature
`google-rpc-error-details/v1`) and is written only in a `ver = 3` frame requiring that name.

`provider_code: "request_invalid"` marks a provider's HTTP 400 that rejected the request itself
(`invalid_request_error`, or Google's `INVALID_ARGUMENT`) when no more specific code applies;
class `permanent`, never re-sent. `provider_subcode` carries the provider's own short code
beneath it (for example `invalid_json_schema`), only when it matches `[a-z0-9_]{1,64}`; it is
an open, diagnostic-only vocabulary and is absent on every other error.

`affordable_max_tokens` is set only on a `credit_insufficient` terminal (a permanent refusal of a
request the credential's remaining credit cannot pay for now, such as OpenRouter's HTTP 402 with
`error.metadata.limit_source` `openrouter_credits`), and only when the provider stated the number:
the largest request `max_tokens` the remaining credit covers. `credit_insufficient` is distinct
from `billing_exhausted`, which says the balance itself is gone.

`credential_id` is the full credential id the run froze in `auth_selection.credential_id`
(with shape `<method>:<provider>:<account>`), stamped by the run on exactly the credential-scoped
terminals: `billing_exhausted`, `credit_insufficient`, `account_limit_exhausted`, and a transient rate limit (HTTP 429,
or an in-band `rate_limit_error`/`rate_limit_exceeded`). A rate-limit terminal also carries
`provider_code: "rate_limited"` in place of the provider's own code; WALs written before it
hold the raw code. Absent on every other error, on runs that froze no selection, and on records
written before the field; a reader that ignores it computes the same session.

**`run_paused`**: non-terminal. The run is waiting for re-authentication and resumes
under the same `run_id`. Always the last record of its episode while the pause lasts.

| Field | Type | Rule | Meaning |
|---|---|---|---|
| `run_id` | string | required | |
| `ts_ms` | u64 | absent | Wall-clock Unix ms of the pause. |
| `reason` | string | required | `auth_required` (the only value today). |
| `error` | `ProviderError` | absent | The cause. |

**`run_config_refrozen`**: replaces the run's frozen render config after a pause at start.
Legal only before the first provider call.

| Field | Type | Rule |
|---|---|---|
| `renderer_version` | u32 | required |
| `ts_ms` | u64 | absent. Wall-clock ms when the refreeze was recorded. |
| `frozen_render_config` | opaque JSON | required |

**`run_auth_refrozen`**: replaces the run's frozen credential selection after
re-authentication. May appear more than once, and after provider calls.

| Field | Type | Rule |
|---|---|---|
| `ts_ms` | u64 | absent. Wall-clock ms when the refreeze was recorded. |
| `auth_selection` | `AuthSelection` | required |

`AuthSelection` (also `RunConfig.auth_selection`): `credential_id` (required); absent-when-unset
`account_ref`, `resolved_credential_id`, `vault_request`, `account_decision`, and
`auth_method`. Without `auth_method`, `credential_id` is the `<method>:<provider>[:<account>]`
broca key, and method, provider and account parse out of it. With `auth_method` (a string such
as `oauth`, `chatgpt`, `apikey`, `antigravity`, frozen from the vault's metadata when routing
chose the credential), `credential_id` is the vault's own credential id, an opaque record name:
resume reads it verbatim and takes the method from `auth_method`, and nothing parses the id.
A `run_started` or `run_auth_refrozen` whose selection carries `auth_method` is
**must-understand, feature `auth-selection-method/v1`** (a `ver = 3` frame). `vault_request` is
`{min_ttl_ms: i64|null (always written), force_refresh: bool (absent on get_scoped),
op: "get"|"get_scoped"|<unknown string> (absent on older records, meaning "get")}`.
`billing_vault_facts` is an optional billing-only snapshot on `AuthSelection`. When
present it requires `bill-vault-facts/v1`, including when embedded in an attempt reservation.
Reader support ships before production capture. An `observed` snapshot requires the
vault's exact `credential_id` and `record_version`, and may carry nonempty `provider_ids`,
`plan_tier`, and `plan_tier_source`. Each absent fact remains unknown. Observation states
`unwitnessed`, `unavailable`, `not_listed`, `ambiguous`, `version_mismatch`, and
`invalid_metadata` carry no facts and may retain a witnessed id/version. A missing snapshot
means not captured; future states remain unknown. Ordinary resume never backfills the snapshot.
Only a matching independent send witness makes its observed facts attributable to an attempt.
The attempt-level `provider_ids`, `plan_tier` and `plan_tier_source` on a reservation's
`credential` appear only when the snapshot's state is `observed` and its `credential_id` is the
one actually sent (`credential_ref`); otherwise they are absent.

`account_ref` is empty by construction for `apikey` and `antigravity` credentials, so key
spend on `credential_id`, never on `account_ref`.

#### Steps

**`step_started`**: written when a model step begins. `step_id` (u64, required). Carries
no time.

**`model_step_finished`**: the assembled assistant message of a completed step.

| Field | Type | Rule | Meaning |
|---|---|---|---|
| `step_id` | u64 | required | |
| `assistant_message` | `AssistantMessage` | required | The full assistant output, including tool calls. |
| `usage` | `Usage` | required | This step's usage, summed field by field across serving iterations when present. Fresh input sums each iteration's fresh input; an unreported field stays absent. |
| `finish_reason` | string | absent | `stop`, `length`, `tool_calls`, `content_filter`, `error`, `other`. |
| `retries_used` | u32 | defaulted 0 | Retries within this step. |
| `service_tier_used` | string | absent | Tier the provider **reported** serving, verbatim. Never filled from the requested tier. |
| `speed_used` | string | absent | Anthropic `usage.speed`, verbatim. |
| `served_model` | string | absent | **Must-understand, feature `provider-fallback-block/v1`.** The wire model id an Anthropic server-side fallback served the answer with, present only when it differs from the requested model. |
| `usage_iterations` | array | absent | **Must-understand, feature `provider-fallback-block/v1`.** The response's `usage.iterations`, verbatim: entries with `type` (`message`, `fallback_message`), `model`, token counts and any other members. These entries replace last-iteration-only top-level counters for accounting. |
| `route` | `{model, reason}` | absent | **Must-understand, feature `refusal-fallback/v1`.** Present only when the step was sent to the client-side fallback model `model` (`{provider_module_id, model_id}`); `reason` is `refusal` (the source refused this step) or `schedule` (an earlier refusal's schedule routed it). |
| `refusal_fallback` | object | absent | **Must-understand, feature `refusal-fallback/v1`.** The session's refusal-fallback schedule after this step: `source`, `fallback` (model handles), `phase` (`{kind: "fallback", remaining}` or `{kind: "probe", clean}`), `re_refusals` (u8), `updated_at_ms` (absent). |
| `refusal_after_tool_use` | bool | absent | **Must-understand, feature `refusal-fallback/v1`.** The source model refused after completing a tool call; the step committed as a tool step. |

An assistant message may hold a `provider_fallback` part, `{"type": "provider_fallback",
"from_model": str, "to_model": str, "raw"?: str}`: Anthropic's server-side fallback handoff, at
the position the response placed it. `raw` is the complete wire block as JSON text, preserving
key order, whitespace and unknown fields for verbatim echo on opted-in Anthropic requests.
Parts stored before raw capture omit it and still decode. The previous decoder ignores
unknown members of the `provider_fallback` variant, so builds that already support that variant
can read the new parts (but do not retain raw bytes). Builds predating the variant itself
are refused by the existing `provider-fallback-block/v1` feature requirement.

**`model_step_refused`**: **must-understand, feature `refusal-fallback/v1`.** The source model
refused the step and it is re-sent to the client-side fallback model under the same `step_id`,
with no new `step_started`. Synced before the re-send. The refused content is never stored. At
most one per step.

| Field | Type | Rule | Meaning |
|---|---|---|---|
| `step_id` | u64 | required | The step being re-sent. |
| `usage` | `Usage` | required | The refused response's billed usage. |
| `served_model` | string | absent | As on `model_step_finished`. |
| `discarded_blocks` | u32 | required | How many content blocks the refused response held. |
| `schedule` | object | required | The schedule after the refusal, shaped like `model_step_finished.refusal_fallback`. |
| `ts_ms` | u64 | absent | Wall-clock Unix ms of the refusal. |

**`turn_finished`**: `step_id` (u64). The turn (step plus its tool batch) is committed.
Synced.

**`budget_fired`**: a caller-supplied budget message was injected. `run_id`, `kind`
(`nudge`/`finalize`), `threshold` (u64), `ordinal` (u64), all required; `caller_text`
(absent).

#### Tools

| Record | Fields |
|---|---|
| `tool_batch_started` | `batch_id`, `step_id`, `planned_calls` (array of tool-call ids). Every planned call gets exactly one terminal. |
| `tool_dispatch_intent` | `batch_id`, `tool_call_id`, `tool_name` (absent; for display only), `args` (JSON), `module_id` (absent; the tool provider module the call was dispatched to), `schema_pin` (absent; `{schema_digest, semantics}`, the tool schema the call was dispatched against). Synced **before** the tool is dispatched. `module_id` and `schema_pin` are **must-understand, feature `dispatch-module/v1`**: an intent carrying either is a `ver = 3` frame. Every new intent names `module_id` when the tool plane routes the call to a module; `schema_pin` is not written yet, because a session's frozen tools carry no schema digest. |
| `tool_no_dispatch` | `batch_id`, `tool_call_id`, `reason`: object tagged by `kind`: `denied{reason}`, `validation_failed{detail}`, `unavailable{detail}`, `policy_blocked{detail}`. The call never left the process. |
| `tool_result` | `batch_id`, `result` (`ToolResult`), `outcome`: `completed`, `unknown` (sent, no result: the side effect may have happened), `interrupted`, `max_steps`. Optional `reason` (string, absent by default, **not** must-understand): a typed reason name for a result broca wrote in place of a provider's. Today it is written only when the route a call was to use, opened under the run's scope, closed while the call was in flight. It then names what the daemon reported about the scope when asked with `scope.describe` (`scope_ended`, `scope_not_live`, `scope_parent_ended`, `scope_agent_changed`, `scope_unverifiable`), or the refusal code the daemon returned when broca tried to re-open the route; with `completed` the call was never sent (`route_drained`), with `unknown` it was sent and its result never came back. The outcome carries what replay acts on, so an older reader that ignores `reason` computes the same session. |
| `tool_batch_finished` | `batch_id`. Written only after every planned call has its terminal. |
| `approval_requested` | `tool_call_id`. |
| `approval_resolved` | `tool_call_id`, `decision`: object tagged by `decision`: `allow`, `allow_narrowed{input}`, `deny{reason}`. |

A `tool_dispatch_intent` with no `tool_result` after it is **indeterminate**: the call is never
re-sent. On resume broca writes a `tool_result` for it with outcome `unknown`, `is_error: true`
and text saying the call has no recorded result (and an `interrupted` result for any planned
call of that batch that never left the process), commits the batch, then seals the episode
`interrupted` with no model call. The next send renders the call paired with that result.

#### Queue and submission

| Record | Fields |
|---|---|
| `queued` | `submission_id`, `request` (the caller's full `SubmissionRequest`), `request_identity_digest` (absent), `principal` (absent), `send_id` (absent), `title` (absent; the send's title, carried onto the `run_started` that drains it, informational like `run_started.title`), `delivery` (absent; `"interrupt"` for the message an interrupting send queued before cancelling the run it interrupted). A prompt queued while a run was active. |
| `retracted` | `submission_id`, `failure` (absent, `{run_id, error: ProviderError}` when the queued head could never be admitted). |
| `steered` | `send_id` (absent), `request_identity_digest` (absent), `delivery` (absent; `"steer"`), `title` (absent; the steering send's title, informational like `run_started.title`), `input` (the user message(s), after any pre-user transformations), `hooks` (absent; optional pre-user hook metadata, written alongside the prepared `input`). **Must-understand, feature `steer-queue/v1`**: always a `ver = 3` frame. A message a caller added to the running turn. |
| `delivery_ordered` | `submission_id`, `order` (`{modes, dedupe_key?, expires_at_ms?}`). **Must-understand, feature `delivery-order/v1`**. The ordered `delivery` list of the `queued` record for the same submission that follows it in the same barrier (a running turn's own records may lie between them). Written only for a send using the `delivery_order` group. |
| `send_ended` | `submission_id`, `reason` (`{kind: "superseded", by}` or `{kind: "expired"}`). **Must-understand, feature `delivery-order/v1`**. A queued ordered send that will never be delivered; removes it from the pending queue like `retracted`. |
| `send_delivered` | `submission_id`, `run_id`, `mode` (`"steer"` or `"tool_result"`), `input` (with `steer`: placed like a `steered` record's input), `tool_call_id` and `appended_text` (with `tool_result`: rendered after that result's stored output, which is never changed; written between the batch's `tool_batch_finished` and `turn_finished`). **Must-understand, feature `delivery-order/v1`**. Consumes the queued send; an `idle_start` delivery is the `run_started` naming its `submission_id`. |

The pending queue is `queued` minus `retracted` (and `send_ended`) minus those consumed by a
`run_started` carrying their `submission_id` (or a `send_delivered`). A queued send whose order
has no `idle_start` never drains into a run. It drains every pending `delivery: "interrupt"` entry first,
in WAL order, then the plain entries in WAL order. `queued.delivery` is deliberately NOT gated:
a build that predates it drains everything in plain WAL order, so after a rollback an interrupt
runs behind the plain sends queued before it. Each message is still run exactly once; only the
order degrades.

A `steered` message is placed in the conversation at the first step boundary after its record:
the episode's next `step_started` (that step renders it, as a user message appended after
everything before it), or the episode's `run_finished` when no further step comes. It is never
placed between a tool call and its result. `session.read` shows it at that position, and not
before it is placed. A missing `hooks` field decodes as absent. Replay renders `input`
exactly as written and never re-runs hooks.

#### Informational

**`model_attempt_finished`**: one per provider HTTP attempt of a step (failed and cut
attempts included), written before that step's `model_step_finished`, or before the run's
terminal if the step failed. Written without its own fsync. Present from v0.3.122. Every
broca reader skips it: removing these records from a WAL gives the same session.

| Field | Type | Rule | Meaning |
|---|---|---|---|
| `step_id` | u64 | required | Same id as the step's `model_step_finished`. |
| `attempt` | u32 | required | 0-based, gapless within the step. |
| `request_body_len` | u64 | required | |
| `request_body_sha256` | string | required | Lowercase hex. |
| `http_status` | u16 | absent | Absent when no status line arrived. |
| `content_type` | string | absent | |
| `response_body_len` | u64 | required | Raw bytes received (for a cut stream, the prefix). |
| `response_body_sha256` | string | required | Lowercase hex over those bytes. |
| `response_body_cap` | u64 | absent | Present only when an error body was cut at a cap. |
| `outcome` | string | required | `complete`, `eof_before_completion`, `chunk_error`, `idle_timeout`, `content_timeout`, `byte_limit`, `decode_error`, `rate_floor`, `cancelled`, `sealed_in_flight`, `http_error`, `provider_error`, `connect_failed`, `header_timeout`, `no_status`. A later build may add values; decode as an open string. |
| `ts_ms` | u64 | required | Wall-clock Unix ms at attempt end. Informational only, never a join key. |
| `input_transformations` | array | absent | Provider's report of request changes, entries `{type?, path?, reason?}`. |
| `credential_refresh` | object | absent | `{vault_op, min_ttl_ms?, force_refresh, record_version?}` on an attempt re-sent after a 401 refresh. |

It does **not** carry usage.

**`resume_started`**: one per resume of an episode that has no durable `run_finished`,
written before anything else the resume writes. Every broca reader skips it and it never enters
a rendered request: removing these records from a WAL gives the same session. Builds that
predate it skip it as an unknown type.

| Field | Type | Rule | Meaning |
|---|---|---|---|
| `episode` | string | required | Run id of the episode being resumed. |
| `resumed_from_seq` | u64 | required | `seq` of the last durable record the resume read. |
| `replayed` | object | required | Counts rebuilt without calling anyone: `messages` (the whole reconstructed conversation), `model_steps` (durable steps of this episode, not requested again), `tool_results` (durable terminals of the open tool batch, reused as recorded). Absent keys read as 0; a later build may add keys. |
| `redone` | array | required | What the resume does again, entries tagged by `kind`: `model_step{step_id}` (no output of that step was durable), `tool_dispatch{tool_call_id}` (a planned call with no durable intent). A later build may add kinds. |
| `indeterminate` | array | required | Tool-call ids with an intent and no result, or an `unknown` outcome. Each gets an `unknown` `tool_result` if it has none, and is never re-sent. |

#### Other record types

The records below are not given field tables here. Their fields are defined, with the rule for
each, on the `WalRecord` variant of the same name in `crates/broca-wal/src/record.rs`, which is
authoritative. "Must-understand" means broca writes the record only in a `ver = 3` frame that
requires the named feature (section 2.2), so a reader that does not know the feature refuses
the whole WAL instead of skipping it.

| Record | Gate | Meaning |
|---|---|---|
| `input_token_cap_frozen` | must-understand, `input-token-cap/v1` | The session's input-token policy frozen at admission, even when the session was previously unlimited. |
| `input_token_budget_exhausted` | must-understand, `input-token-cap/v1` | The input-token budget ran out; written in the terminal barrier with any never-dispatched tool results. |
| `context_ceiling_wait` | must-understand, `context-ceiling/v1` | A step's bounded wait at the context ceiling. |
| `context_ceiling_decided` | must-understand, `context-ceiling/v1` | The durable decision of a ceiling check, before any model I/O: permit the step, or seal the run without sending. |
| `output_reservation_clamped` | must-understand, `output-reservation/v1` | A step's output-token cap, lowered before any model I/O so the estimated input plus the cap stays inside the run's context limit. Written only for a lowered step; a resume and every rebuild of the step's request (cache warm, cache dump) render the recorded cap. |
| `structured_output_downgraded` | must-understand, `structured-output-downgrade/v1` | The provider refused the strict schema's grammar; from this barrier on the run renders non-strict, including after a crash. |
| `structured_output_result` | must-understand, `structured-output/v1` | The run's structured output and its validation, written in the same commit as `run_finished` and never recomputed on read. |
| `run_cancel_requested` | must-understand, `run-cancel-at/v1` | An accepted cancellation, synced before it is acknowledged. It does not seal the episode; recovery still repairs the open tool batch. |
| `cache_warmed` | none; informational | A keep-warm re-send of a run's request. Never a transcript message. Billing exports it as its own `warm` fact; an older build skips it and exports nothing for it. |
| `compaction_request`, `compaction_message`, `compaction_answer` | must-understand, `compaction/v1` | The issued high-water before a compaction step call, the complete applied view, and every other step outcome with the cursor it moves. |
| `compaction_wait` | must-understand, `compaction-wait/v1` | A durable exit from a compaction wait. |
| `compaction_view_applied`, `compaction_notice` | must-understand, `compaction-view/v1` | A compaction view every later request of the lineage renders until a newer one applies, and the bus notice published for it. |
| `tool_call_deferred` | must-understand, `deferred-call/v1` | A tool call an approve hook held for a person's answer, synced before the question is filed. |
| `approval_recorded` | must-understand, `deferred-call/v1` | The answer to a held call. When the answer is `expired` or `unavailable` it carries the call's closing result and is the call's terminal. |
| `awaiting_approval` | must-understand, `deferred-call/v1` | Which held calls the run is waiting on, with each call's latest `elicitation_id`; the calls' own records decide membership. |
| `late_approval_recorded` | must-understand, `deferred-call/v1` | An answer read after its call was closed as expired; exactly one per expired call. Never enters a prompt. |
| `late_dispatch_intent`, `late_result` | must-understand, `deferred-call/v1` | The before half and the terminal of a late approved call's dispatch; an intent with no result resumes as an `unknown` result and is never sent again. Never enter a prompt. |
| `question_withdrawal_acknowledged` | none; informational | How the withdrawal of a held call's question ended. An older build skips it and computes the same session. |

Several records documented above also become must-understand when they carry certain fields
(for example `run_started` with a scope, a plan manifest or retention, `run_started` and
`run_auth_refrozen` whose `auth_selection` carries `auth_method`, and `run_paused` with
reason `restart`). `WalRecord::required_features` in the same file lists every case.

#### Unknown record types

Since **v0.3.120**, a record in an ordinary (`ver = 1`) frame whose `type` the running build
does not know is decoded as a placeholder and **skipped everywhere**: replay, projection, the
control stream, billing and export. broca writes a new record type in such a frame only when an
older build can skip it and still compute the same session (such as `cache_warmed` and
`question_withdrawal_acknowledged` above). A record that changes what a session means is
written in a `ver = 3` frame requiring a feature instead, and a reader that does not know that
feature must refuse the whole WAL as written by a newer build (section 2.6): skipping it would
compute the wrong session. A reader should do the same: skip an unknown `type` in a `ver = 1`
frame and never treat it as corruption, but never skip a `ver = 3` frame whose features it does
not know. broca never writes the placeholder back.

### 2.5 Usage

`Usage` is an object with five optional `u64` keys:

| Key | Meaning |
|---|---|
| `input_tokens` | Fresh, **non-cached** input tokens. Disjoint from `cached_input_tokens`; do not subtract. |
| `cached_input_tokens` | Cache-read input tokens. |
| `cache_write_tokens` | Cache-creation tokens. |
| `output_tokens` | Output tokens, **including** reasoning. |
| `reasoning_tokens` | A **subset** of `output_tokens`; never add the two. |

**Absent versus zero.** A key is absent when the provider did not report that quantity. A
key with value `0` is a reported zero. `{}` means nothing was reported. When usages are
summed (a run total), a key stays absent only if no step reported it; if some steps
reported it, the sum covers those steps only, a lower bound.

**Which records carry usage:**

- `model_step_finished.usage`: one step, one provider call.
- `model_step_refused.usage`: one refused provider call; it counts toward the run total.
- `run_finished.usage`: the run's accumulated usage.
- `model_attempt_finished` carries **none**. Nothing else in the WAL does.

**Historical boundary.** Builds before v0.3.103 wrote all five keys
on every usage, with an unreported quantity written as `0`. The WAL record has no marker of
the build that wrote it, so a zero in a WAL record written before then may mean "not
reported". In the export lane the boundary is exact: facts with `export_seq ≤ 122061`
have all five keys always present; facts above it omit unreported keys.

**OpenAI Responses cache writes.** From the first release after v0.3.141, OpenAI Responses and ChatGPT/Codex steps carry `cache_write_tokens`, and their `input_tokens` excludes those written tokens (OpenAI counts writes inside its `input_tokens`). On earlier OpenAI Responses and Codex steps the key is absent because broca did not map it, not because OpenAI did not report it, and their `input_tokens` still includes any tokens written to cache. Chat-completions steps that report the `cache_write_tokens` spelling (metered OpenAI, OpenRouter) already carried the key before that release, but their `input_tokens` also still included the written tokens, so those tokens were counted twice; from the same release they are split out. The `cache_creation_input_tokens` spelling (Alibaba) is not split out.

### 2.6 Reading a WAL safely

- Open read-only and read to end-of-file. Stop at a torn tail (section 2.2); the writer may
  be in the middle of an append.
- Accept `ver = 2` frames anywhere, including before the first record. Check that every
  lineage frame in the file carries the same 16 bytes.
- Skip unknown record `type`s in `ver = 1` frames.
- Refuse a `ver = 3` frame whose `requires` names a feature you do not know, and refuse the
  whole file, not just that record. Report it as a newer writer, never as corruption. The
  scripts under `scripts/` that read WAL frames (`step-timing.py`, `find-finalize-leaks.py`,
  `live-runs.py`) read a `ver = 3` frame through `scripts/lib/wal_gate.py`: they decode its
  record when they know every feature it requires (their table is empty today, so every
  gated frame is unknown to them, including broca's `scope/v1`), and
  otherwise list the file as `newer-writer`, use none of its records, and carry on with the
  rest of the corpus.
- The file can change under you. Its writer truncates a torn tail when it reopens, the
  archive fold deletes cold files, and an archive restore re-creates them. If you cache
  byte offsets, key them on the file's identity (inode) and size and re-read from zero when
  either changes.
- broca refuses to read more than 1 GiB of one WAL in one pass. Production files are far
  smaller.

### 2.7 The manifest sidecar

`<state root>/wal-manifest` is an append-only text file with one line per WAL
**created** by this code path:

```text
<addr>  <lineage id, UUID form>  <project_root>\x1f<harness>\x1f<session>\n
```

(two spaces between fields). It is best effort: written without fsync, appended by every
daemon sharing the tree, never read by broca, and rebuildable (`store-maintenance
wal-manifest`). Use it only to look up which triple an `addr` belonged to. Never treat it
as complete or as authoritative.

---

## 3. The WAL archive

Cold WALs (by default, those whose newest run-level `ts_ms` is more than 14 days old) are
folded into archive containers, either by an operator or by the daemon's opt-in scheduler. Folding copies the
file's exact bytes into a container, seals and syncs it, then deletes the live file. An
append to an archived session restores the live file first.

### 3.1 Files

```text
<state root>/wal-archive/fold-<unix-ms>.ark       the container
<state root>/wal-archive/fold-<unix-ms>.journal   the fold pass's JSON-lines journal
```

`<unix-ms>` is written in canonical decimal (no leading zeros). Newer means a **numerically
larger** stamp: `fold-10` is newer than `fold-9`. A pass that found nothing to fold leaves
only a journal. Journals are internal recovery state; readers ignore them.

### 3.2 Container layout

All integers little-endian.

```text
container := member* index trailer
member    := header(64 bytes) payload(payload_len bytes)
header    := "WALM"(4) format_version:u16 (=1) zero:u16
             addr: 16 ASCII lowercase-hex bytes
             payload_len:u64  payload_sha256:32 bytes
index     := count:u32  entry*count
entry     := addr: 16 ASCII bytes  offset:u64  payload_len:u64    (32 bytes; offset of the member header)
trailer   := index_len:u32 (= 4 + 32*count)  "WALK"(4)
```

- A container is **sealed** only if its last four bytes are `WALK`. An unsealed container
  belongs to an interrupted fold and must not be read.
- A member's payload is the **verbatim bytes of the WAL file** at the time of the fold.
  Decode it with the frame layout in section 2.2.
- Verify `payload_sha256` before using the bytes.
- Each address appears at most once per container. The index is an optimization: members
  are laid out back to back from offset 0, so walking the headers until the first
  non-`WALM` magic finds every member when the index is damaged.
- A sealed container is never modified.

### 3.3 Finding a session that is archived, not live

1. Compute `addr` (section 2.1).
2. If `<addr>.wal` exists in `wal/`, `wal-durable/` or `wal-ephemeral/` (even with zero bytes), **it is authoritative**. Read it and
   stop. A live file always wins over any archive copy, which may be older.
3. Otherwise list `wal-archive/*.ark`, order by numeric stamp, **newest first**, and for
   each container: skip it if it has disappeared since you listed it, skip it if it is
   unsealed, and otherwise look `addr` up. The **first** hit is the session. Two containers
   with the same stamp are an error.
4. No hit means the session does not exist on disk.

Folding and restoring can run while you read. If step 2 found no live file and step 3 found
nothing, check the live path once more before concluding the session is absent.

---

## 4. SQLite stores

broca writes the SQLite database `run-index.db` described in this section. It uses
`journal_mode=WAL`, so recent commits sit in the `-wal` sidecar until SQLite checkpoints them
(see section 8). An older `caps.db` may sit beside it; broca no longer opens it (section 4.6).

`run-index.db` is written by one connection per daemon. Its schema is created and migrated
when the daemon opens it: `CREATE TABLE IF NOT EXISTS`, then `ALTER TABLE ADD COLUMN` for
any column an older file lacks. So on a long-lived file, columns appear in the order they
were added, not in the order below.


### 4.1 `run_index`

One row per run: resolves a `run_id` to its session and coarse state without scanning
WALs.

| Column | Type | Notes |
|---|---|---|
| `run_id` | TEXT PRIMARY KEY | |
| `session` | TEXT NOT NULL | The bind triple as **JSON** (`{"project_root":…,"harness":…,"session":…}`). Not the same encoding as `meta.session`. |
| `submission_id` | TEXT | Set when the run consumed a queued submission. |
| `request_identity_digest` | TEXT | Identity of a caller-supplied send id; NULL for server-minted ids and older rows. |
| `state` | TEXT NOT NULL | `active`, `paused`, `completed`, `max_steps`, `cancelled`, `interrupted`, `error`, `transform_unavailable`. |
| `usage_json` | TEXT | The run total as a `Usage` JSON object. Written at the terminal and only if the run completed at least one step. NULL while `active`/`paused`, for zero-step runs, and on older rows. |
| `final_step_finish_reason_json` | TEXT | JSON string of the last step's `finish_reason`, if one was observed. |
| `retries_used` | INTEGER NOT NULL DEFAULT 0 | |
| `state_changed_ms` | INTEGER | See section 5. NULL on rows written before the column existed (v0.3.24). |
| `title` | TEXT | The title the run's `run_started` records, written at admission; the first title written for a run is kept. NULL for a send without one, for runs admitted before the column existed, and always for a run of a session with `retention`: its row outlives the deletion of its WAL, and a title is caller content, so the title stays only in the WAL and is deleted with it. When this column is NULL, `run.status` answers from the run's `run_started` in the WAL. Never rebuilt: like every other column here it is maintained only by the actor. |

**Writers.** The session actor, not the projector:

- at **admission**, after the run's `run_started` is durable (`INSERT … ON CONFLICT(run_id)
  DO UPDATE … WHERE state IN ('active','paused')`): state `active`, usage NULL. A
  terminal row is never overwritten.
- at the **terminal** or **pause**, after the WAL record is durable (`UPDATE … WHERE
  run_id`): the terminal state or `paused`, usage, finish reason, retries.
- by repairs: the shutdown seal (`interrupted`), the startup crash-orphan pass, and
  `store-maintenance reconcile-active` each move a stale `active` row to the terminal its
  WAL already carries.

Each write is its own statement, awaited by the actor but best effort: a failure is logged
and the run continues. So a row can lag its WAL. A crash between the WAL terminal and the
index write leaves a finished run `active` until a repair pass runs. `run_index` is **not
touched per step**.

**Source of truth?** No. The WAL is. There is no full rebuild of this table: it is
maintained incrementally, and `state_changed_ms` records when broca wrote the row, which
cannot be recovered later.

To join `run_index` to `meta`, rebuild `meta`'s key from the JSON:
`json_extract(session,'$.project_root') || char(31) || json_extract(session,'$.harness')
|| char(31) || json_extract(session,'$.session')`. A plain join on `session` returns zero
rows without an error.

### 4.2 `meta`

One row per session: the materialized projection state.

| Column | Type | Notes |
|---|---|---|
| `session` | TEXT PRIMARY KEY | The triple joined by U+001F (the same string that is hashed into the WAL address). |
| `watermark_seq` | INTEGER NOT NULL | Last WAL `seq` folded into this projection. Advances only at turn boundaries. |
| `lineage_id` | TEXT | Lineage id (UUID form) of the WAL this was projected from. NULL on rows not reprojected since lineage ids existed. |
| `control_head_wal_seq`, `control_head_sub_index` | INTEGER | Control-stream cursor at the watermark. |
| `context_epoch` | INTEGER NOT NULL | Written as 0 and never updated. |
| `completed_steps` | INTEGER NOT NULL DEFAULT 0 | Model steps of the **last episode** through the watermark. |
| `usage_json` | TEXT NOT NULL DEFAULT '{}' | Usage summed over the **last episode's** steps through the watermark. |
| `retries_used` | INTEGER | Last episode, through the watermark. |
| `last_step_usage_json` | TEXT | The last step's `Usage`. |
| `last_step_finish_reason_json` | TEXT | The last step's finish reason. |
| `last_run_id` | TEXT | The last episode's `run_id`. |
| `last_episode_usage_json` | TEXT | The last episode's usage from the admission checkpoint (`null` when none). |
| `wal_anchor_file_id`, `wal_anchor_seq`, `wal_anchor_byte_end` | TEXT, INTEGER, INTEGER | Byte offset in the WAL just after `watermark_seq`, bound to the file's identity, so the next read can skip the prefix. |
| `prefix_len` | INTEGER | Number of `message` rows. |
| `admission_checkpoint_json` | TEXT | The **admission checkpoint** (below). |
| `last_title` | TEXT | The newest caller title (on a `run_started`, `queued` or `steered`, newest by WAL position) up to `watermark_seq`. A projection batch with no title keeps the stored one, so it is rebuilt by projecting the WAL like any other field. NULL when no projected send carried one. `session.head` overlays a title in the WAL past the watermark. |

Indexes: `meta_page_usage_idx ON meta(session, last_episode_usage_json)` and
`meta_last_title_idx ON meta(session, last_title)`, each covering a column that was added after
the checkpoint blob so a read of it does not traverse the blob's overflow pages.

**Admission checkpoints** are not a separate table. They are the
`admission_checkpoint_json` column: a versioned envelope `{"version":1,"checkpoint":{…}}`
holding `at_seq`, the last episode (run id, effective config, input, disposition
`running`/`paused`/`finished`, usage, retries, budget state) and the pending submission
queue. A version the running build does not recognise is treated as missing, and the
checkpoint is rebuilt from the WAL.

The optional `has_model_attempt` checkpoint member summarizes the entire folded
prefix, not just its last episode. A complete fold with no attempts records false;
any `model_attempt_finished` makes it true, and subsequent episodes cannot clear
it. Missing on older checkpoints means unknown. Cache diagnostics use this fact
to avoid an unnecessary prefix read when no predecessor exists; unknown history
is rebuilt lazily for that session, never by a startup scan. The version-one
envelope is unchanged, and older checkpoint decoders ignore this optional member.

### 4.3 `message`

The materialized transcript prefix.

| Column | Type | Notes |
|---|---|---|
| `session` | TEXT NOT NULL | Same encoding as `meta.session`. |
| `ord` | INTEGER NOT NULL | 0-based position in the reconstructed prompt. |
| `json` | TEXT NOT NULL | One serialized `Message`. |

Primary key `(session, ord)`.

**Writer for `meta` and `message`: the projector.** After each committed tool turn the run
loop fsyncs the WAL and hands the durable head to a per-run projector task. The projector
runs **asynchronously**, concurrently with the next model step, and coalesces: if several
turns commit while it is busy, it projects once up to the newest. Each projection is **one
SQLite transaction**: insert the new `message` rows, then upsert the session's `meta` row,
including the whole checkpoint JSON. The run's terminal enqueues a final projection that is
drained before the lease is released. Runs with a tool-call budget skip the per-turn
projections and project only at the terminal. Imports and read paths also project: a read
that finds a different `lineage_id` in the WAL resets and reprojects the session.

A final model step with no tool calls is folded in only once the `run_finished` after it is
durable, and `run_finished` itself stays in the unprojected tail. So `meta` alone never
says whether the last episode has ended.

**Source of truth?** No. These tables are a rebuildable projection of the WAL. Deleting a
session's rows costs only speed.

### 4.4 `export_facts`

Usage facts for metering: one row per billing-homogeneous slice of a finished run.

| Column | Type | Notes |
|---|---|---|
| `export_seq` | INTEGER PRIMARY KEY AUTOINCREMENT | Insertion order. Increasing but **not dense**. |
| `fact_id` | TEXT NOT NULL UNIQUE | SHA-256 (hex) over the length-prefixed source instance id, triple, run id and the segment ordinal. Independent of usage and time. |
| `run_id` | TEXT NOT NULL | Indexed (`export_facts_run_id`). |
| `segment_json` | TEXT NOT NULL | The fact (below). |

Older files had a `session` column and a second `fact_id` index; both are dropped when the
database is opened.

**`segment_json` fields:** `run_id`, `session` (triple object), `segment_ordinal`,
`fact_id`, `provider`, `model`, `auth_selection` (credential id or null), `account_ref`,
`resolved_credential_id`, `vault_request`, `work_class` (these four absent when unset),
`charge_basis` (`metered_api`, `subscription_included`, `unknown`),
`charge_basis_mapping_version`, `charge_basis_overrides` (`"none"` or a 64-hex digest;
absent on older facts), `usage_policy_version` (absent unless the run used the
separate-reasoning normalizer), `usage` (`Usage`, section 2.5), `usage_by_service_tier`
(absent when no step reported a tier; buckets need not sum to `usage`), `retries_used`,
`pricing_at_ms`, `pricing_at_precision`, `occurred_at_ms`, `occurred_at_precision`,
`terminal_reason` (final slice only). New fields are only ever added.

A slice ends at a refreeze, and also where a model call is answered by another model than the
slice's: a step routed to the refusal-fallback model (`route`) or served by an Anthropic
server-side fallback (`served_model`). When `usage_iterations` is present, each iteration's
usage joins the slice for its own serving model, in provider order; consecutive iterations
on the same model coalesce. Top-level last-iteration counters are not added again. A step's
retries belong only to its final iteration. Missing measurements remain absent, including
reasoning when only the top-level response reported it. The next slice's `model` is the model
that answered, so a run can carry a row for a fallback model. A `model_step_refused` call belongs to the slice of
the model that refused it, and a `cache_warmed` re-send of a routed step's request is billed
under that step's model. When every call is answered by the run's own model, only refreezes
open additional slices. The split changes fact cardinality, not schema, and applies only
when producing new facts: previously exported insert-once facts are never rewritten.

Session facts have no `kind` member and retain the shape above. Sessionless classifier
facts dispatch on `kind: "system_one"` before decoding. They contain `fact_id`,
`batch_id`, `item_index`, `provider`, `model`, optional `auth_selection` (the resolved
credential pin) and `account_ref`, exactly one of `run_id` or `flow_id`, `charge_basis`,
`charge_basis_mapping_version`, `charge_basis_overrides`, `usage` (only reported
`input_tokens` and `output_tokens`, each absent when unreported), and `occurred_at_ms`.
There is no session, agent id, estimated usage, or price. Astrocyte prices these
measurements against its catalog. A flow fact leaves the SQL `run_id` index column
empty; the JSON carries its `flow_id` rather than an invented run identity.

System One fact ids use domain-separated, length-prefixed SHA-256 over
`system_one/v1`, source instance id, role, batch id, first-admission timestamp and
item index. Thus a resend keeps its id, but new work admitted after seven-day batch
expiry gets another id. `system-one.db` commits the exact fact bytes to a durable
outbox in the same transaction as the paid outcome and frozen-rate ceiling spend.
Publishing inserts once into `export_facts`, then acknowledges the outbox. A crash
on either side of that acknowledgement only replays the same immutable bytes.

A run gets a new slice at every `run_config_refrozen` and `run_auth_refrozen`. A slice's
`usage` is the sum of its `model_step_finished` usages.

**Writers.** After a run's terminal is durable and its `run_index` row is written, the
actor spawns a detached task that reads the session's WAL and inserts that run's facts with
`INSERT OR IGNORE` in one transaction. The session's next run never waits for that export.
At every daemon start, a catch-up pass exports any terminal
`run_index` run that has no facts. An existing `fact_id` is **never overwritten**: a
re-derivation that differs is logged and the stored row stays.

**Source of truth?** A projection of the WAL: the derivation is a pure function of the WAL
records. But the stored rows are what consumers have read and cursored past, and
re-deriving an old fact today can produce different bytes (a newer mapping version, added
fields) and a different `export_seq`. Treat the stored rows as the authoritative record
of spend.

`export_steps`, a per-step usage table added in v0.3.125, was removed in v0.3.127. Every
open runs `DROP TABLE IF EXISTS export_steps`. Rolling back to v0.3.125 or v0.3.126
recreates it empty. Per-step usage is in the WAL
(`model_step_finished.usage`).

### 4.5 `export_meta`

A single row (`id = 1`) naming this export stream.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PRIMARY KEY CHECK (id = 1) | |
| `source_epoch` | INTEGER NOT NULL DEFAULT 1 | Written as 1 and never changed. |
| `source_instance_id` | TEXT NOT NULL | Minted once when the database is created (UUID-shaped). Part of every `fact_id`. |
| `high_water` | INTEGER NOT NULL DEFAULT 0 | `MAX(export_seq)` after the latest fact batch. |

Consumers read `export_facts` in `run-index.db` with SQLite `mode=ro`; the
`usage.export` RPC was removed in release **0.3.156**. Incremental readers retain
`(source_instance_id, source_epoch, export_seq)` from this database. A new
`source_instance_id` means a different export database, and cursors from the old one must
be reset. `sqlite_sequence` also exists; SQLite maintains it because of AUTOINCREMENT.

### 4.6 `caps.db` (no longer opened)

Older builds kept daemon-scoped spend caps in `caps.db`, table `cap_rows`, written only by
the `cap.install` and `spend.delta` operations. Those operations and the admission check
that read the table were removed, and nothing in the fleet called them. broca no longer
opens, reads or writes `caps.db`. An existing file and its `-wal`/`-shm` sidecars are left on
disk as they were; broca does not delete them, and a later build will not read them. No WAL
record or frozen run config ever carried a cap field, so removing caps changes nothing about
how old session logs decode or resume.

---

## 5. Timestamps

Every time value broca stores is **wall-clock Unix milliseconds** from the system clock of
the writing machine. None is monotonic, and none is used for ordering or identity. Order
comes from `seq` (in a WAL) and `export_seq` (in the export lane).

### 5.1 In the WAL

| Field | Set when | Updated? | Notes |
|---|---|---|---|
| `run_started.ts_ms` | The run is admitted and its `run_started` recorded. | Never. A resume writes no new `run_started`. | Absent on pre-metering records, or if the clock read failed. |
| `run_finished.ts_ms` | The terminal record is written. | Never. | |
| `run_paused.ts_ms` | The pause record is written. | Never. | |
| `run_config_refrozen.ts_ms`, `run_auth_refrozen.ts_ms` | The refreeze record is written. | Never. | |
| `model_attempt_finished.ts_ms` | A provider HTTP attempt ends. | Never. | Required. v0.3.122 and later. Informational: not a join key against provider logs. |
| `ProviderError.resets_at_ms` | Reported by the provider on an account-limit refusal. | n/a | When the account's quota refills. Not the time of any broca event. |

`step_started`, `model_step_finished`, every tool record, `turn_finished`, `budget_fired`,
`queued`, `retracted` and the approval records carry **no time at all**. The WAL's last
record usually has no `ts_ms`. The last timestamped record of a run in progress is usually
its latest `model_attempt_finished` (the end of the latest provider attempt) or, before any
attempt has finished, its `run_started`.

### 5.2 In SQLite

**`run_index.state_changed_ms`** is broca's wall clock at the moment **broca writes the
`run_index` row**:

- at admission (state `active`), which is shortly after `run_started.ts_ms`;
- again if the same `run_id` is admitted again while its row is `active` or `paused`
  (resuming a paused run, for example). This rewrites the value even if the state stays
  `active`;
- at the terminal or pause write, shortly after the WAL's `run_finished`/`run_paused`;
- when a repair (the shutdown seal, the crash-orphan pass, `reconcile-active`) moves a stale
  `active` row to its terminal. The value is then the **time of the repair**, which can be
  long after the run ended.

It is **not updated per step, per turn, or per tool call**. It does not mean "last
activity", and it does not mean "when the run ended": for that, read `run_finished.ts_ms`
(or the export fact's `occurred_at_ms`). NULL means no time was recorded (rows written
before v0.3.24). It never means zero.

**`export_facts.segment_json`:**

- `occurred_at_ms`: the run's terminal time, taken from `run_finished.ts_ms` (or from a
  trailing `run_paused.ts_ms` when the run never finished). Earlier slices of the same run
  are given the same terminal time. Absent when the WAL had no such timestamp (a closed,
  historical set of facts in the backfill band `export_seq` 1..5473). `occurred_at_precision`
  says `terminal` even then, so test `occurred_at_ms` for presence.
- `pricing_at_ms`: the time the slice's config was frozen (`run_started.ts_ms` or the
  refreeze's `ts_ms`), falling back to the terminal time. `pricing_at_precision` is `exact`
  for the first case and `degraded` for the fallback.
- Neither is the time the row was inserted. The catch-up pass can insert an old run's facts
  late, so `export_seq` order is not time order.

`meta`, `message` and `export_meta` hold no timestamps.

### 5.3 The best "last activity" signal for an active run

Use the **WAL file's mtime** (`stat` of `wal/<addr>.wal`), for a run whose `run_index.state`
is `active`.

The session's writer task appends each record to the file as soon as the run produces it,
and fsyncs are batched separately, so the mtime moves at every step of progress: step start, each provider attempt's end, step
finish, every tool intent and result, turn finish, terminal. It costs one `stat`, needs no
parsing, and holds no lock.

The other candidates are weaker:

- **The last record's `ts_ms`.** Most records carry none (section 5.1). The newest
  timestamp in an active run is usually a `model_attempt_finished`, so it misses tool
  activity entirely: a run spending ten minutes in tool calls shows its last provider
  attempt. It also needs a full parse to the end of the file.
- **`session.read`'s `head`.** It is a position `(wal_seq, sub_index)` of the last durable
  control unit, not a time. You can detect that something happened by polling and
  comparing, but not when. It requires the RPC, not a disk read.
- **`run_index.state_changed_ms`.** Not updated during the run (section 5.2).

The mtime's limits: it does not move while a provider response is still streaming or while
one tool call is running, since nothing is written until they finish. It is set to the
restore time when an archived WAL is restored, and to the truncation time when the writer
reopens a file with a torn tail. It measures recorded progress, not liveness. For a
liveness question ("is anything happening right now"), combine it with the caller's own
expectation of how long a step or a tool call may take.

---

## 6. Where a given fact lives

| Question | Read |
|---|---|
| Spend, per run or per period | `export_facts.segment_json` (a stable contract). |
| Per-step usage or service tier | WAL `model_step_finished`. |
| Run state by `run_id` | `run_index.state`. Treat `active`/`paused` as possibly stale and confirm from the WAL if it matters. |
| When a run started / ended | WAL `run_started.ts_ms` / `run_finished.ts_ms`, or the fact's `occurred_at_ms`. |
| Last activity of an active run | WAL file mtime (section 5.3). |
| Which WAL is a session's | FNV-1a of the triple (section 2.1). |
| The transcript | WAL replay, or `session.read`. `message` is an internal cache. |

---

## 7. Stability

New warm facts with `bill-attempts/v1` attempt evidence may include optional `warm_outcome` in `segment_json` to distinguish completed, failed and unknown attempts; legacy facts omit it and retain their original bytes and `fact_id`.

**Stable** means external readers may rely on it: changes are additive (new optional
fields, new skippable record types), or come with a new version marker. **Internal** means
it may change without notice.

| Item | Status | Why |
|---|---|---|
| State-root layout: `wal/`, `run-index.db` paths | Stable | The export lane's documented surface names the database path, and the WAL path is the session key. |
| WAL address: FNV-1a of the U+001F-joined triple, 16 hex digits | Stable | It is the filename of every existing WAL; a change would orphan every session. Pinned by a test against an independent implementation. |
| WAL frame layout (`len`, `ver`, `seq`, `fence`, `digest`) and the `ver` 1/2 meanings | Stable | Every existing WAL must stay readable. The lineage frame's bytes are pinned by a test. A new layout would need a new `ver`. |
| Lineage id: 16 opaque bytes, minted at creation, same id = same log | Stable | Exposed to consumers as `lineage_id`. |
| `seq` semantics (1-based, gapless, per file) | Stable | Part of the framing contract. |
| `fence` values | Internal | Forensic only; nothing gives the numbers a meaning. |
| Record `type` names, and the fields listed in section 2.4 with their meanings | Stable (additive) | Old WALs must replay forever, so an existing field cannot change meaning. New fields are optional and absent by default; new record types are added only if skippable. |
| Unknown record types are skipped | Stable | Required for rollback safety since v0.3.120. |
| Opaque sub-objects: `frozen_render_config`, `provider_options`, `cache_tiers`, the inner shape of `tools`, `generation`, `budget`, `context_management` | Internal | Provider- and loop-private. broca only needs them to round-trip. |
| `Usage` keys and the absent-versus-zero rule | Stable | Shared with the export lane contract. |
| `model_attempt_finished` | Stable (additive) | Informational; absent on WALs written before v0.3.122. |
| `resume_started` | Stable (additive) | Informational; absent on WALs written before it shipped. |
| Archive container format version 1 (section 3.2) and the live-first resolution rule | Stable | Sealed containers are immutable and must stay readable; the header carries a format version for any future layout. |
| `fold-*.journal` contents | Internal | Crash-recovery state of the fold pass. |
| `export_facts` table and `segment_json` | Stable | The documented metering contract. Facts are immutable; new fields are optional and change neither existing bytes nor `fact_id`. |
| `export_facts.export_seq` as an increasing cursor | Stable | Consumers cursor on it. It is not dense. |
| `export_meta` | Metering source identity | Direct readers use `source_instance_id`, `source_epoch`, and `high_water` to detect database replacement and track incremental reads. |
| `run_index` | Internal | An operational projection, not a declared contract. Its changes have so far been additive columns, but nothing promises that. The `session` column's JSON encoding, the state strings and `state_changed_ms` are the parts least likely to move. |
| `meta`, `message`, `admission_checkpoint_json` | Internal | Rebuildable caches. The checkpoint has its own version envelope, which broca may bump. |
| `caps.db` | Internal | Left over from removed spend caps; broca no longer opens it. |
| `wal-manifest`, `leases/`, `catalog/`, `logs/`, `request-dumps/` | Internal | Diagnostics and process state. |

---

## 8. Rules for external readers

1. **Never write.** Do not append to, truncate, rename or delete a WAL, archive container
   or journal. Do not "repair" a torn tail: the writer may be in the middle of an append.
   Open files read-only.
2. **Open SQLite read-only with `mode=ro`**, for example
   `file:/…/run-index.db?mode=ro`, or `SQLITE_OPEN_READ_ONLY`. **Never `immutable=1`** on the
   live database. It tells SQLite to ignore the `-wal` sidecar, which silently hides every
   committed row not yet checkpointed, and the data just looks old. (`immutable=1` is only
   correct on a static copy that was checkpointed before it was copied.)
   Starting with v0.3.147, `run-index.db-wal` and `run-index.db-shm` persist across broca restarts, so read-only database connections (`mode=ro`) can open while broca is down.
3. **Keep read transactions short.** A long-open read transaction stops SQLite from
   checkpointing, and the daemon's `-wal` sidecar grows while it is open.
4. **Prefer `export_facts` over the raw WAL for spend.** It already applies slicing at
   refreezes, charge basis, the credential labels and the absent-versus-zero rule. Read the
   WAL only for what the facts do not carry (per-step detail, attempts).
5. **Decode usage keys as optional.** Absent is not zero (section 2.5).
6. **Skip what you do not know.** Unknown record `type`s, unknown JSON fields, unknown enum
   strings (`outcome`, `reason`, `vault_request.op`) are expected as broca grows.
7. **Resolve sessions live-first** (section 3.3), and tolerate files disappearing or
   reappearing between listing and opening.
8. **Treat `run_index` states other than terminal ones as hints.** An `active` row can
   belong to a finished run until a repair pass runs, so confirm from the WAL when it matters.

### Agent-run scope compatibility

`RunStarted.scope` can also carry optional `agent_id` and `run_id`. The latter
names the daemon-stamped agent run, not the broca episode's top-level `run_id`.
These scope members, including an agent identity alone, require the WAL reader
feature `agent-run-scopes/v1`; a reader without it refuses the gated frame rather
than losing ownership. Flow scopes retain their existing `flow-scopes/v1` gate.
Ordinary and flow scopes with no new members retain their exact record bytes and
feature requirements, pinned before the new fields were introduced.

Replay, admission checkpoints, store projection, `run.status`, `session.read`,
archive readers, and diagnostic scripts use the WAL feature table. Reader-only
builds do not advertise the capability and refuse both fresh agent-run sends and
advancement of frozen agent-run sessions without appending WAL bytes.

The broca writer enables `AGENT_RUN_SCOPES_WRITER_ENABLED` and advertises the
`agent-run-scopes/v1` daemon module capability together, promising run-scope admission.
Only `reserved:basal`, not the scope owner, may drive an
agent-run scope with both identifiers. Tool declarations and planned tool items
are refused before fetch/WAL admission. The frozen sender must remain Basal;
`agent_id` and `run_id` cannot change, and existing history cannot be adopted.
Basal still cannot read or affect sessions whose first sender is not Basal.
Usage-export labels are optional `agent_id` and `agent_run_id`, with ordinary
bytes and `fact_id` unchanged.
