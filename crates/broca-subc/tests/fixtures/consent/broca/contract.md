# Broca generic-consent binding contract

This is a broca-authored contract, not a Cingulate contract or a served upstream
reply. It defines how broca uses Cingulate cards to authorize held tool calls,
including local argument binding, recovery and card lifecycle.
Cingulate accepts and returns `binding` as opaque requester data; it does not
validate the binding, authorize a tool invocation, or execute a tool.

**Broca is the sole enforcer of the argument binding for its calls.** No tool
provider currently verifies an approval digest. Broca must stop sending
`approval_digest` on approved tool calls; it must not synthesize a replacement
digest or treat opaque binding data as a daemon-issued authorization token.

## Frozen binding

Each broca card with `kind: consent` carries one canonical JSON object in
`params.binding`:

| Key | Value |
|---|---|
| `args_hash` | Lowercase SHA-256 hex of the exact UTF-8 `args_jcs` string stored with the deferred call. Never parse and reserialize those argument bytes before hashing. |
| `module` | The frozen module string, or JSON `null` when absent. |
| `schema_pin` | The frozen schema pin, or JSON `null` when absent. A non-null pin refuses `approval_schema_pin_unsupported`: opaque binding does not establish daemon schema validation. |
| `tool` | The frozen tool name. |

Use RFC 8785 canonicalization to compare this object. The golden vectors restrict
the canonicalization domain to ASCII strings, nulls, booleans and safe integers;
the fixture verifier is not a general-purpose RFC 8785 implementation. The
argument string itself is opaque: even a different JSON key order changes its
hash. Missing keys, extra keys, different null/absent rendering, or a different
value cannot compare equal to the stored four-key object.

Before dispatching an approving answer, broca canonicalizes its returned binding
and compares those bytes to the stored binding's canonical bytes. A mismatch
refuses `consent_binding_mismatch`; the call does not run. This applies to live,
recovered-held and late answers. An answer recovered while closing out a call
interrupted by a crash never dispatches that closed call.
Only the explicit `approve` choice on an answered card authorizes broca's call,
and only after its ordinary or late approval decision is durable. Deny, silence, timeout,
unavailable replies and conflicting recovered decisions authorize nothing.

`requester_ref` and `dedup_key` both equal the frozen `call_key`. They correlate
the request but confer no identity or authorization. Request bodies carry no
requester, owner, subject, agent, session or worker identity; the daemon stamps
subject/head identity from the route. The reply field stored in the WAL as
`elicitation_id` is `result.elicitation_id`.

## Provenance of local bodies

`held-call-request.json` is the broca-authored golden `{method, params}` request.
`held-call-input.json` supplies its frozen inputs, including intentionally
noncanonical argument key order. `expired-rendering.json` supplies the expected
non-authorizing rendering of the upstream expired record, not a daemon reply.
None of these three broca-authored JSON files is an upstream-served response.
No local reply or answer record is represented as upstream-pinned.

Upstream `scope`/`args_digest` fields describe consent content, not transport
identity. `held-call-request.json` supplies `args_digest` equal to the argument hash,
as well as the opaque binding. Tool and module are not top-level request keys.
The `executor` wire field is dropped: broca executes its own approved calls.
Broca generic cards have exactly approve/grant and deny/decline options. Any
approval plan declaring an extra option refuses `approval_options_unsupported`
at admission, before any WAL write. Never silently drop extras when filing.
Extra options need a defined generic-consent contract before broca supports them.

## Recovery with no recorded card id

1. Read `consent.answers` from `since: 0`, including all continuation pages, and
   match `requester_ref == call_key`. Adopt a settled match instead of filing.
2. If none matches, request with `dedup_key == call_key`. A pending card refreshes
   in place. Persist the returned id before another consent operation.
3. Read the settlement feed again. If an older settled matching card has a
   different id, adopt its answer, withdraw the newly filed card, and persist
   the adoption. Crash close-out adopts terminal information only and never
   executes the closed tool call.
4. Conflicting decisions among multiple settled matching cards refuse
   `consent_conflicting_decisions`. The call does not run. Log only the session,
   `call_key`, and card ids, never arguments, prompts or binding content.

A call is decided by at most one answer and runs at most once. After a crash
that lands while a person answers, a duplicate question can be briefly visible;
it is withdrawn by the requester. This is not an upstream atomic deduplication
guarantee: v1's updating/withdrawal section matches only pending cards and v2's
correlation section gives `requester_ref` no uniqueness meaning.

Downstream recovery tests must inject settlement deterministically between the
first feed read and filing, then assert adoption and withdrawal of the new card.
A separate conflicting-decisions test must refuse. Each runtime test needs its
own mutation row; fixture-only tests cannot prove execution/recovery ordering.

## Acknowledgment, reporting and late intake

The selected acknowledgment branch is **C**: Cingulate's settlement feed belongs
to the bound owner (requester principal plus attested session), not a broca run.
The cursor is a store-global log position; an acknowledgment discards that
owner's settlements through the position. Broca sends no `consent.ack`. It reads from `since: 0` and deduplicates locally by
card id, following continuation pages. An upstream acknowledgment hides settled
cards from requester reads, so a run must not discard another run's answers.
The existing no-ack choice requires a two-run regression and a
`no-ack-on-owner-feed` runtime mutation; branch A/B ack-ordering mutations do not
apply.

Only approving calls get `consent.report_execution`. Report `executed` for a
completed provider invocation (including provider error output),
`approved_not_executed` for a pre-send failure or Interrupted/NotSent/MaxSteps, and `outcome_unknown` for an unknown dispatch result. A synthetic route
failure without a prior fence is not execution; a completed prior fence is
returned unchanged. Report after the terminal result is durable, retry the same
value in-process, and add no report debt across restart. An already-answered
withdrawal returns its terminal record unchanged; recognize that approving
record during crash close-out and best-effort report not executed, with zero
provider requests.

Core's `consent_answered` notice only wakes broca. Broca remains the sole intake
for authorization and execution and checks the binding on every late answer.
The notice carries no requester reference or binding, so it cannot authorize
execution. Preserve the existing LateApprovalRecorded/LateDispatchIntent/
LateResult ordering. `state: expired` always maps to LateDecision::None, never
Approved or a person's decision, even if legacy `answered_choice_id` or
`answered_at_ms` metadata is present. Decode that metadata without error and do
not act on it. The v1 expired vector contains such metadata, contrary to v1's
prose; broca follows the prose's non-authorizing semantics, not field absence.
`expired-rendering.json` is a local expected-output fixture: an upstream expired
record with choice metadata must render None and cause no dispatch. The fixture
verifier checks its declared expectation; runtime tests must still prove that
broca's actual decoder and dispatch path obey it.

The detach requirement comes from the separately pinned late-answer supplement,
not the base v2 tag. RestartPause continues holding the call, so it sends neither
detach nor withdraw. Crash close-out, a non-resume stop and retention expiry owe
withdrawal, not detach. Whenever broca releases a held call while its card stays
visible, it must detach and handle the settled-record readback. Merely stopping
an await does not by itself release the held call.

### Detach inventory

| Release path at the vendoring baseline | Action |
|---|---|
| Local deadline closes a held call, `crates/broca-core/src/run.rs:9900-9911` | Detach when a known card may still be pending/visible, unless withdrawal is owed. |
| Answer intake observes broca's deadline, `crates/broca-core/src/run.rs:10099-10102` | Close Expired by broca's clock; handle the settled readback through late intake, never ordinary dispatch. |
| Filing-binding mismatch with a known id, `crates/broca-core/src/run.rs:10050-10055` | Current release-as-Unavailable must detach if the card remains visible, unless withdrawal is owed. The old comparison of filing reply args_hash to frozen args_hash is superseded by canonical comparison of answered binding to stored binding. |
| Other release-as-Unavailable, `crates/broca-core/src/run.rs:9972-9978,10057-10058`; id-less deadline at `9900-9905` | Detach any known visible card unless withdrawal is owed; an absent id cannot be used in an id operation. |
| Closing decision persistence, `crates/broca-core/src/run.rs:9208-9234` | Expired/Unavailable stays closed. A settled detach readback uses binding-checked late intake only and reports not executed for that closed call; no ordinary dispatch. |
| Non-resume stop, `crates/broca-core/src/run.rs:10008-10024,9236-9259` | Withdraw unanswered open cards as the existing withdrawal debt requires. |
| RestartPause, `crates/broca-core/src/run.rs:10000-10006` | Continue holding: neither detach nor withdraw. |

The late-answer operation's params use `id` as spelled in its contract, not an
invented alias. A settled detach readback must not reopen an Expired or
Unavailable call. Only the existing late path may consider a later explicit
binding-matched approval; an Unavailable call is not an expired late-execution
candidate. Report not executed only for approving readbacks, preserving the rule
that declined or unanswered readbacks owe no execution report.
