# Elicitation records v1

Source: prefrontal repository, tag `elicitation-records-v1-c9`, commit
`5951123fd14a2a6cbf3ff7aaed5eaab8f0c6c1cd`, path
`test-vectors/elicitation-records-v1/`. The producer tests in
`crates/prefrontal-core-module/src/elicitation_tests.rs` pin every JSON file by
SHA-256 and compare the vectors to live operation replies (normalizing only
minted IDs and clock timestamps).

Phone operation payloads:

- `elicitation.list_pending_for_user {}` → `{records: [...]}`
- `elicitation.get {elicitationId}` → record
- `elicitation.answer {elicitationId, choiceId}` → `{ok: true, alreadyAnswered, elicitation}` on success; `{ok: false, code: "conflict", elicitation}` on conflict; `{ok: false, code: "not_found"}` for an unknown id

The answer is the provider option's `id`, never its display label or `effect`.
An unknown effect is neutral. Same-choice repeats succeed with `alreadyAnswered: true`; a different answer
after settlement returns `ok: false`, `code: "conflict"` and the stored elicitation.
A pending record includes `blockUntilMs` only while a request is awaiting an
answer; the timestamp is core's absolute clock time. Durations may be absent
when the option labels describe different grant lengths.

Requesters on core's route use snake_case parameters and record keys (including
`elicitation_id`, `expires_at_ms`, `answered_choice_id`, and `subject.head_agent_id`):
`elicitation.request`, `elicitation.get`, `elicitation.answers {since}`,
`elicitation.ack {cursor}`, `elicitation.await {elicitation_id, timeout_ms}`,
`elicitation.report_execution {elicitation_id, execution}`, and
`elicitation.withdraw {elicitation_id}`. The allowlisted requester principal is
`reserved:plexus`; the record owner includes its bound session. Attribution refs
are not authority. A requester never answers its own record.

`await` waits at most 120 seconds per call and never beyond the record expiry.
If it returns pending the provider keeps waiting, without returning the approval
step to the agent to re-call. Answer reads replay while unacked; pass the returned
cursor to continue and acknowledge only after durable intake. At 24 hours the
answer feed returns an expired tombstone `{elicitation_id, state: "expired", settled_at}`
rather than the original card. `settled_at` is core epoch milliseconds.

The push plaintext stays inside the existing sealed envelope. A new card carries
`{elicitationId, kind: "elicitation", eventAtMs}`; its silent retraction carries
`{elicitationId, kind: "retraction", eventAtMs}`. As with asks, `kind` identifies
the event, while the id key distinguishes the record type. Lock-screen copy
remains generic.


## Approval and restart vectors

`c9/` contains additive approval and restart vectors; the original phone vectors
remain unchanged. In the source repository, regenerate with
`python3 test-vectors/elicitation-records-v1/generate.py`.
Copy the payloads, retain these local documentation trims, update source provenance,
and refresh local `SHA256SUMS` and its pin in `verify.py` and the parent README.
The generator reuses `fetch-plan-v1/generate.py`'s restricted-domain RFC 8785
writer: `.json` is pretty UTF-8 plus newline, `.jcs` is canonical envelope bytes
without newline, `.sha256` is lowercase SHA-256 of those envelope bytes without
newline. The implementation's producer tests independently check these bytes.

A held-call filing adds `call_key`, `executor: runner | provider`, `module`,
`tool`, `schema_pin`, and `args_jcs` (a JSON string containing the final argument
bytes). `args_hash` is optional on filing; if present it must equal SHA-256 of
`args_jcs.as_bytes()` **as received**. Core parses those bytes for validity and
renders the short form, but never reserializes them before hashing. Senders must
send JCS; the deliberately noncanonical order probe pins the receiver's exact-byte
rule, not a second canonicalization algorithm. Ordinary scoped grants and the
live Plexus filing (args_digest, preview, session_ref, parent_session_ref) remain
valid. Body attribution is accepted for compatibility but ignored on scoped
routes. `late_execution: execute | notify_only` remains unchanged on the record.

Core computes `approval_digest` as lowercase SHA-256 of RFC 8785 bytes of exactly
`{module, tool, args_hash, schema_pin, scope: {owner, ref, scope_epoch}}`.
`approval-binding` pins the complete preimage. A held-call filing adds `args_hash`
and `approval_digest` to the existing `{elicitation_id}` reply. Filing again with
`(requester principal, call_key)` returns the same record in every state. The
executor is fixed on that record. `dedup_key` alone is requester/session scoped;
it does not replace another held-call record.

`elicitation.verify_approval` takes `{approval_digest, module, tool, args_hash,
schema_pin}`. It returns `{verified: true, elicitation_id}` only for an answered
grant with an exact binding match; missing, denied, expired, withdrawn or
mismatched approvals return `{verified: false}`. Scope identity always comes
from the inbound daemon stamp. Extra payload `scope` is ignored; the stamp-wins
and scope-mismatch vectors pin both directions. Verification is read-only;
consumption belongs in the provider's claim transaction, not in core.

Phone records add `callKey`, `argsHash`, `approvalDigest`, `module`, `tool`,
`executor`, and core-rendered `preview`, never raw args or scope identity. Outcome
fields retain the existing `execution` key and add `reason` for `not_run`:
`scope_ended`, `delegation_withdrawn`, or `scope_unverifiable`. `approved_waiting`
has no reason. These are outcome reports, not permission to execute. The
`subject.headAgentId` identifies the head reached through the verified chain.
Runner-held calls send no answered-card notice to that head.

Answers are versioned by the input: integer `since` preserves the live integer
`cursor` reply byte-for-byte. Null starts an incarnation-aware feed; object
`since` and object `cursor` on ack carry `{provider_incarnation, seq}`. An old
incarnation is refused `cursor_incarnation_changed`; restart from null and dedupe
by elicitation_id. New scoped requesters use this object form. Both forms are
pinned here; legacy integer ack remains `{ok: true}`. Provider incarnation is
new per core process, not durable store identity. Approval records and digests
are durable and remain verifiable after restart.
