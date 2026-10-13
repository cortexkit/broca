# Elicitation requester v1
Source: prefrontal repository, tag `elicitation-records-v1-c9`, commit
`5951123fd14a2a6cbf3ff7aaed5eaab8f0c6c1cd`, path
`test-vectors/elicitation-requester-v1/`. Update payloads from the source after
regeneration, retain the local documentation trims, and refresh the local
`SHA256SUMS` and its pin in `verify.py` and the parent README.
Exact requester operation payloads from the core module's `ManagerRuntime::dispatch`
(`crates/prefrontal-core-module`).
Phone records are separately pinned in `../elicitation-records-v1/`. These are
operation payloads, not JSON-RPC framing. JSON is UTF-8, pretty-printed by
`serde_json`, with one trailing newline. The test compares **bytes**, not parsed
JSON. `tag` and `method` identify the fixture; `request` is the dispatched body
before transport identity injection; `reply` is the returned success value.
Refusal fixtures instead contain `request.params` and `request.route` (the
injected `__elicitation_route`, or null when missing), plus `error.code` and
`error.message` from the actual dispatch error. Messages are illustrative for
that input; clients should branch on codes.

## Reproduction and placeholders

Run from the repository root:

```sh
UPDATE_ELICITATION_REQUESTER_VECTORS=1 cargo nextest run -p prefrontal-core-module --lib -E 'test(elicitation_requester_vectors_match_live_dispatch)'
cargo nextest run -p prefrontal-core-module --lib -E 'test(elicitation_requester_vectors_match_live_dispatch)'
```

Fixture setup requests a card, reads/awaits it while pending, exercises refusals,
answers it through the operator route, reports execution, reads and acknowledges
the feed, then withdraws a second pending card. Each file is one step of this
scenario, not an independent transaction. The `elicitation_not_found` case reads
the acknowledged first record.

Only minted identifiers and clock values are replaced:

- `@ELICITATION_ID@`: the card ID returned by request (withdraw uses a second card).
- `@HEAD_AGENT_ID@`: the registry head's minted agent ID.
- `@NOW_MS@`: integer creation time in Unix milliseconds.
- `@EXPIRES_AT_MS@`: integer creation time plus `expires_in_ms` for a fresh card.
- `@ANSWERED_AT_MS@`: integer settlement time (expiry uses the expiry deadline).

These strings are fixture notation, **not accepted wire values** for numeric
fields. Cursors remain actual integers from the fresh SQLite database.


## `elicitation.request`

The body and nested typed objects reject unknown fields. Required fields:

| Field | Type | Meaning / validation |
| --- | --- | --- |
| `kind` | string | Exactly `consent`. |
| `title`, `prompt` | strings | Card heading and question; nonblank after trimming. |
| `options` | array of objects | Nonempty; each requires string `id`, `label`, `effect`; optional string `detail`. IDs must be nonblank and unique, labels nonblank. Effects are not enumerated except the default must have `decline`; use `grant` for approval. |
| `default` | string | Must name an option whose effect is `decline`. |
| `urgency` | string | `low`, `normal`, or `high`. |
| `on_expiry` | string | Exactly `deny`; timeout never grants approval. |
| `material_damage` | boolean | Whether the proposed action risks material damage. |
| `late_execution` | string | `execute` or `notify_only`; describes the requested late-answer handling policy, not execution by core itself. |
| `target` | object | Required strings `kind`, `label`, both nonblank; identifies the target of the action. |
| `expires_in_ms` | signed integer | Positive, at most 86,400,000 (24 hours). Relative consent deadline. |

Optional fields (absent or null for optional values):

| Field | Type | Meaning / validation |
| --- | --- | --- |
| `dedup_key` | string | Same owner and key refresh a still-pending card's request, subject, routing and deadline, retaining ID/creation time; settled cards are not reused. |
| `scope` | arbitrary JSON | Proposed authority scope. At least a non-null scope or nonblank `args_digest` is required. |
| `args_digest` | string | Digest describing the proposed arguments; no digest format is enforced. |
| `duration` | arbitrary JSON | Descriptive authority duration. |
| `session_ref`, `parent_session_ref` | strings | Attribution references. |
| `facts` | array of objects | Defaults to `[]` when absent (not null); each requires string `label`, `value`. |
| `preview` | object | Requires string `label`, `text`; action preview. |

There are no explicit string-length, option-count, fact-count or JSON-depth limits
in `ConsentRequest::decode` beyond these rules. Nested detail/fact/preview strings
have no additional nonblank validation. Success: `{ "elicitation_id": string }`.
This acknowledges creation, not consent or execution. A new card triggers delivery
push; updating a pending deduplicated card does not trigger a fresh delivery push.

## Other requester operations

These bodies do not deny unknown fields. Required IDs are nonempty strings (not
trimmed). `get`, `await`, `report_execution`, `withdraw` check ownership first.

| Operation | Request fields | Success reply |
| --- | --- | --- |
| `elicitation.get` | `elicitation_id`: string | Requester record below. Reading a pending card past expiry settles it as expired. |
| `elicitation.await` | `elicitation_id`: string; `timeout_ms`: unsigned integer, required | Requester record; waits until settlement or timeout. Timeout is capped at 120,000 ms, and never runs past card expiry. Zero polls immediately. A timeout before expiry returns **pending**, not approval; already-settled cards return immediately. |
| `elicitation.answers` | `since`: nonnegative signed 64-bit integer, required | `{ "records": array, "cursor": integer }`. Start at zero. Only this owner’s unacknowledged answered/expired outcomes strictly after `since`, ordered by cursor. Cursor is the global answer-log high-water mark, so gaps and an empty batch with an advanced cursor are normal. Withdrawn cards are excluded. |
| `elicitation.ack` | `cursor`: nonnegative signed 64-bit integer, required | `{ "ok": true }`; cumulatively acknowledges this owner’s outcomes at or below cursor, including withdrawals. No upper-bound check. Persist/process the batch before ack; acknowledged records are unavailable through get. |
| `elicitation.report_execution` | `elicitation_id`: string; `execution`: nonempty string | Requester record with `execution`. Accepted values: `executed`, `approved_not_executed`, `outcome_unknown`. Only answered cards may be reported (core does not require that the chosen effect was grant). Reports overwrite prior reports, do not execute actions or create new feed events. |
| `elicitation.withdraw` | `elicitation_id`: string | Requester record. Pending card becomes `withdrawn`, with settlement timestamp and no chosen option; retracts the phone card, wakes existing awaits and schedules notice delivery. A settled card keeps its first outcome; expiry wins when already due. |


### Requester record and answer delivery

Records flatten the original request fields (including defaulted `facts`) and add:

- `elicitation_id`: string; `created_at_ms`, `expires_at_ms`: integer Unix ms.
- `subject`: object with string `head_agent_id`, string `label`, optional string
  `agent_id` (head only).
- `state`: `pending`, `answered`, `expired`, or `withdrawn`.
- `answered_choice_id`: string, present only when answered; choose by ID, not label.
- `answered_at_ms`: integer settlement time, present after settlement.
- `execution`: optional reported execution string.
- `block_until_ms`: optional integer countdown deadline, present on pending records
  while a live await exists (maximum live wait deadline). Await replies clear their
  own wait registration before projecting the record; other waits may remain.

No internal requester owner or resolved head routing session is projected. Top-level
and subject keys are snake_case; arbitrary scope/duration JSON is not renamed.
A full settled record is retained for 24 hours after settlement unless acknowledged.
After retention, the feed can return a tombstone with `elicitation_id`,
`state: "expired"`, and integer `settled_at`, even if the original outcome was
answered. The tombstone survives until ack. Get refuses absent, acknowledged or
no-longer-retained records. Pending requests expire at their deadline: no selected
option is invented and no grant is implied. Use await for a blocking carrier,
or periodically read answers with the last safely processed cursor and ack the
batch. Neither a created ID nor an expired/pending/withdrawn state is consent.

## Refusals

Each named JSON refusal is generated by dispatch with one representative input.
Several validation messages share a code; the listed conditions describe the
complete requester surface, not just that input. Evaluation order matters:
transport extraction first, requester authorization before request decoding;
ID operations validate ID then owner before operation-specific fields.

| Code | When raised |
| --- | --- |
| `elicitation_requester_refused` | Missing/malformed transport identity, absent principal, or principal not `reserved:plexus`. |
| `elicitation_invalid_request` | Request deserialization/type/missing/unknown-field errors; blank title/prompt or empty options; blank/duplicate option IDs, blank labels; default not an option; invalid urgency/late_execution; expiry outside (0, 24h]; neither scope nor nonblank digest; blank target fields. Also missing/nonstring/empty ID or execution, invalid/missing/negative/noninteger cursor or timeout. |
| `elicitation_unknown_kind` | Decoded request kind is not consent. |
| `elicitation_fail_closed_required` | Default effect is not decline or on_expiry is not deny. |
| `elicitation_unavailable` | Attribution head/parent cannot resolve or has no registry name; worker session has no resolvable parent. |
| `elicitation_not_owner` | Stored owner differs from route owner, including unknown IDs with no owner. |
| `elicitation_not_found` | Ownership exists but record is acknowledged or beyond full retention; a record disappears between ownership and reading/settling. |
| `elicitation_invalid_execution` | Nonempty execution string outside the three currently accepted values. |
| `elicitation_store_error` | SQLite/store failures, task lookup failures, or accepted execution reported on a non-answered card. The latter wraps the store's invalid-execution error, rather than returning the direct `elicitation_invalid_execution` code. |
| `elicitation_operator_required` | Requester attempts phone-only list_pending_for_user or answer (not one of the seven requester ops). Included to pin the authority boundary. |

The internal unmatched-method fallback is `unknown_method`, but no valid requester
operation reaches it; it is not an elicitation requester refusal. The phone's
invalid choice is a store error, not a separately raised requester code.

## SHA-256

| File | SHA-256 of committed bytes |
| --- | --- |
<!-- generated sha256 -->
| `request.json` | `5e70f4126929e8d130e945771e083dee15e8132ad45528ca617ff62cdf176a49` |
| `get.json` | `9c8f795cb845c39d64479d1dd4f5d18b058d3ba2257d377c526ad81387c5e48a` |
| `await.json` | `e3fd0d1bf64f61be4e33096fc6112abe9bdadf193ff85343b4b0a3f1b1fd44d4` |
| `elicitation_requester_refused.json` | `d90ee27cb662aab11575e977a20fab451c5060c1d9c02ada2bdb8a0e0f744666` |
| `elicitation_invalid_request.json` | `4ee52473c88d04e685a71d7f6fcaef9328d2d6aef15a37500bd15d7caba62a3e` |
| `elicitation_unknown_kind.json` | `088db23b455d48c69e38e042806b24a9c4f0db54b9fd6ca59d46a4ec417c9369` |
| `elicitation_fail_closed_required.json` | `2818bdafbe85521911fd006097b347fd5a5ba05bdfa34a2598a50e728dd3bc53` |
| `elicitation_unavailable.json` | `f6924b8cc0e6a371765121d7c690f95ba8af0750792672383064c3cd658f1b81` |
| `elicitation_not_owner.json` | `d0d43da0661964c1dc4578bd355abf26d31b88cd194b64cac9e79a30367c8840` |
| `elicitation_invalid_execution.json` | `1240f138b3b1339d8c99e78f065c4669d88cd3122eb7bd8f90a0630f60869d6d` |
| `elicitation_store_error.json` | `3d7fae6f594b5a16d2a10850d0d91afe4033e3b2a5101fc81e3b3a2db3e47559` |
| `elicitation_operator_required.json` | `51c80a3ab123b97d90c70c50d3d2bfc5599b9028aea9d873496c88d5b067c5dc` |
| `report_execution.json` | `d0888b61420fdddac065894a16cf02c03f8af94cd1cf1002896087d156b7480a` |
| `answers.json` | `5a4e7892a2da111a63958e62a9ca03531c0d218bc5b0fa8e4625c74705357ba2` |
| `ack.json` | `2789814b9037489fafce97cd0be9d2e6e40299eac1ecb37c5363458ebda9f5aa` |
| `elicitation_not_found.json` | `550e8eb22261f49a6373843b7a56c6159cd4e19d47bb06170b685438ada763be` |
| `withdraw.json` | `65adbbb9a7b1c07576dc3bedc79125e20fd7c39ab260d73f239049208da5bbee` |
