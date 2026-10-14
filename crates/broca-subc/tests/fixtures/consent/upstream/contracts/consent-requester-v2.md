# Consent requester contract, v2

Version 2 preserves every v1 operation and body for existing kinds. It adds AFT
permission asks and the optional generic-consent correlation field described below. Pin the [served corpus](../../test-vectors/tool-permission-v1/README.md)
and its `SHA256SUMS`; the phone projection is specified in
[consent-phone-v2.md](consent-phone-v2.md). These asks deliberately have no expiry,
retention tombstone, execution report, acknowledgment or settlement-feed entry:
AFT uses durable request/get/await/withdraw and core receives notices.

## Generic consent and requester correlation

`reserved:cerebellum` is admitted for `kind: "consent"` only, preserving the
v1 generic-consent admission rule. Its seven card families use that generic body;
it cannot request `flow_install`, `flow_decision`, `package`, `tool_permission`
or `macos_permission`. A disallowed kind refuses `elicitation_requester_refused`.
Requester and subject identity come only from the daemon's route stamp, including
the nonblank carried agent id required for this requester, never from body fields.

Cerebellum may call `consent.request`, `consent.await`, `consent.get`,
`consent.answers`, `consent.ack`, `consent.withdraw` and
`consent.report_execution`. Reads and mutations by id require ownership;
answers and acknowledgments are scoped to cards raised by that principal under
the existing v1 owner rules. This admission grants no phone/operator authority
and no access to the core-only `consent.outcomes` feed.

Generic `consent` params add one optional field:

| Field | Rule |
|---|---|
| `requester_ref` | Opaque string of 1–128 printable ASCII characters (bytes `0x20`–`0x7e`), returned unchanged. If present, null, non-string, empty, overlong, control and non-ASCII values refuse `elicitation_invalid_request` with `requester_ref` in the message. Other kinds cannot carry it. |

The reference correlates an originating call with a later answer; it supplies
neither identity nor display text. It is stored durably and returned as
`requester_ref` in requester `consent.get`, `consent.await`, `consent.answers`
and the core outcome feed, as well as requester mutation readbacks. When absent,
the field is omitted. Phone `consent.list_pending`, `consent.get` and answer
readbacks never include it, in either snake_case or camelCase.

It has no uniqueness or deduplication meaning: two cards may share the same
reference. Only `dedup_key` selects a pending card to refresh. Changing or removing
`requester_ref` on an otherwise matching dedup refresh retains the card id and
updates the stored reference; it does not withdraw and replace the card.

The [generic-consent v2 served vectors](../../test-vectors/consent-requester-v2/README.md)
cover requests with and without a reference, requester and phone records,
answers/outcomes, and refusal of a non-consent request. Their `SHA256SUMS` is
generated from actual handler replies. Existing v1 contracts and corpora remain
byte-for-byte unchanged.

## Admission and stamped identity

Only `reserved:aft` may request `tool_permission` or `macos_permission`.
Other requesters refuse `elicitation_requester_refused`, naming the requester.
AFT cannot request any existing kind. An absent scope or a scope owned by anything
other than `reserved:prefrontal-core` refuses `access_not_alfonso`.
The daemon-stamped scope ref, epoch, kind, agent and route harness are recorded;
no body field supplies identity. Workers do not need an agent id.

A `worker` with a stamped `parent` starts in `pending:head`. A missing or
non-live parent refuses `access_no_head`. A `head` or `ephemeral` starts in
`pending:user`. Worker parent liveness is read from the daemon's `scope.describe`,
not route disappearance or a core lookup. The daemon's `not_live` while its
configured owner has not re-synced is unknown, not proof of death. Unknown or
failed liveness at creation refuses `access_unavailable`; for an already-open
ask it leaves the ask pending. The existing maintenance sweep checks pending
head legs and settles definitive head loss as `access_head_gone`. It never
compares an ask's age or a reply deadline. No answer leg expires.

## Request bodies

For permission asks, `params` is a closed object with `kind` and the following fields, not a nested
body. Unknown keys (including `parent`, `scope`, requester identity and expiry)
refuse `elicitation_invalid_request`. The entire framed request retains v1's
1,310,720-byte limit; no field is truncated.

| `tool_permission` field | Rule |
|---|---|
| `call_key` | Required nonblank string; persistent idempotency key. |
| `tool` | Required nonblank string, at most 64 Unicode characters. |
| `operation` | Required `read`, `write` or `exec`. |
| `paths` | Required array of at most 16 absolute `/`-rooted paths; no NUL. Empty is allowed for pathless commands. |
| `command` | Required for `exec`, forbidden otherwise; nonblank, at most 600 characters. |
| `reason` | Required nonblank single line, at most 160 characters; no CR, LF or NUL. |
| `args_hash`, `pattern` | Required nonblank strings, opaque to Cingulate. AFT owns hash and pattern semantics. |
| `schema_pin` | Optional nonblank string; opaque. |
| `head_policy` | Required on workers only: `allow` or `not_held`. AFT attests whether the head holds a covering permission. |

The fixed options are `allow_once`, `allow_scope` (effect `grant`) and `deny`
(effect `decline`, default). Heads also have `escalate`. A head with `not_held`
is offered only `deny` and `escalate`. Unavailable answers are omitted.

`macos_permission` has required string `folder` (absolute, no NUL), `program`
(nonblank), and `steps` (nonblank instructions for System Settings). It refuses
`access_macos_only` outside macOS. It is informational: empty options, no default,
no core park notice, no awaiting call (`consent.await` refuses `access_informational`).
One open card per `(folder, program)` across AFT sessions; a repeat returns the
open id. AFT withdraws it when the folder becomes readable. After withdrawal a
new request can create a new card.

Success remains exactly `{elicitation_id: string}`, with an `acc_` id. For tool
asks, `(raising principal, call_key)` returns the original ask, even if settled
or request content changed, across AFT and Cingulate restarts. The original
fields and identity never change. A replay must still be an admitted scoped AFT
request; a settled replay does not require its original head to remain live.

## Records, await and withdrawal

Requester get uses `elicitation_id` (permission asks also accept `elicitationId`).
The record is flattened: request fields, `elicitation_id`, `created_at_ms`,
`state`, `requester_label`, stamped `asker: {scope_ref, scope_epoch, agent_id?}`,
worker-only `head` of the same shape, fixed `options`, and `default: "deny"`
on tool asks. It omits all expiry fields. Pending records may have
`block_until_ms` for active waits. Escalated records carry `escalation_note`.
Settled records add `outcome`, `answered_by: "head" | "user"`, `answered_at_ms`
and optional `note`. Refusals have no `answered_by` and have their `access_*`
`outcome` and `code`. Explicit `deny` has outcome `deny`, code `access_declined`.

States are `pending:head`, `pending:user`, `settled`,
`refused:access_head_gone`, or `refused:access_withdrawn`. An answer never changes
the recorded asker scope. A user's `allow_scope` on an escalated ask applies to
the worker, not its head. Cingulate stores no grants and performs no operation.

Get/await require the raising **principal**, not a mutable transport session.
Another principal refuses `elicitation_not_owner`; an unknown id refuses
`elicitation_not_found`. Records and audit survive indefinitely for recovery.
`consent.await` accepts the id plus v1 `timeout_ms` (0–120,000 after capping),
returns the current requester record when settled or at the caller's wait bound,
is read-only and can be repeated. A returned pending state grants nothing.
Cancelling/dropping a wait removes its transient registration and changes no row.

`consent.withdraw` accepts the id only from the raising principal. It moves any
pending state to `refused:access_withdrawn`; a terminal record is returned
unchanged. Withdrawal, answer and head loss are serialized store transactions.

## Core relays and first-answer-wins

Core answers from `reserved:prefrontal-core` using
`{elicitation_id, answer, note?, answered_by: {agent_id, scope_ref, scope_epoch}}`.
`answered_by` is closed and must exactly match the worker's stamped parent ref
and epoch and its daemon-described agent. Only `pending:head` tool asks accept a
relay. `allow_once` and `allow_scope` require `head_policy: "allow"`; `deny` and
`escalate` do not. A refused relay records an attempt but leaves the ask pending.
An optional note is one line, at most 160 characters. `escalate` changes the leg
to `pending:user`, recording the note and newly offered user surface.

The user answers only through the v1 Callosum + `fed-phone:` phone bind rule,
only in `pending:user`. No body field can enable that rule. The immediate store
transaction reads the leg, validates authority/options, records the attempt and
compares/sets the new state. The first settlement wins. A later answer, including
the same answer, returns `{ok:false, code:"conflict", elicitation:Record}` with
the committed outcome. Accepted answers return `{ok:true, alreadyAnswered:false,
elicitation:Record}`. Wrong legs use `access_wrong_leg`, wrong parents use
`access_wrong_head`, and an unheld head grant uses `access_head_not_held` in that
same `ok:false` form. Invalid options use `elicitation_invalid_choice`.

## Notices and audit

A transaction enqueues each notice uniquely on `(elicitation_id, purpose)`.
Cingulate calls core's `session.notice` as `reserved:cingulate`, with the exact
[design wire](../designs/tool-permission-asks.md#the-sessionnotice-wire):
`elicitation_id`, `purpose`, `asker: {scope_ref,scope_epoch}`, worker-only `head`
of the same shape, and typed `fields`. Purposes are `head_ask`, `user_ask`,
`escalated`, `settled`, `withdrawn`. There is no phone push in the head leg.
Settlements withdraw previously offered user surfaces and wake awaiters after
commit. Superseded undelivered opening notices cannot later re-park a settled ask.

Notice delivery is off the state-machine request path. Missing handlers, core
unavailability and transport errors retry from the durable outbox, with capped
exponential backoff (250 ms to 60 s), using the existing maintenance cadence.
This scheduling is not an ask deadline or timed settlement. `unknown_session`
or `session_not_live` on `head_ask` settles `access_head_gone`; on `settled` it
records `unclaimed`. On other purposes it records refusal only. None retry.
`unauthorized_caller` and `invalid_params` are logged defects and never retry.

The durable audit retains original request, requester principal, stamped asker
and head identity, scope kind and harness, creation time, every offered surface
and its answers/time, every answer attempt including losers and malformed
attempts (surface, principal, time, answer, note, relayed identity, result),
escalation note, final state and settlement time. Outbox rows retain delivery
status, attempt count and last refusal. A late answer never overwrites an earlier
outcome or its note. Standing grants remain entirely AFT's responsibility.
