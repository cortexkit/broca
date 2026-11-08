# System One classifier routing

The `classifier/v1` route serves sessionless batches of decision-model requests.
Its contract is `cortexkit-role-classifier` 0.6.3, using the shared model-batch
0.3.4 rules; routed operations use `cortexkit-role-routing` 0.7.6.

## Rollout switch

The `broca.jsonc` setting below is **off by default**. Keep it off until routing's
classifier consumer is deployed and a live model/account check succeeds:

```json
{
  "classifier_routing": { "enabled": false }
}
```

Off, describe has no `routed_models` group, callers must name a model, and broca
uses the existing fixed credential for that adapter. Pinned request identities,
provider bodies, replies and immutable measurement facts keep their existing
format. A missing-model refusal uses the classifier contract's wording:
`model is required without routed_models`.

On, describe declares `routed_models: {}` only after a live routing describe lists
consumer `classifier`. An unavailable or unready router gives `routing_unavailable`
(transient), never fixed-credential fallback. An answered routing refusal uses
routing's `refusals::retry_class`: Permanent gives `routing_refused` (permanent),
Transient or RetryAt gives `routing_deferred` (transient). Both preserve routing's
code, reason and the caller's pin. A timed refusal includes `detail.retry_after_ms`,
the retry timestamp minus the current Unix time in milliseconds, saturating at zero.
An untimed refusal omits that delay. A refused selection writes no batch and sends
no report.
OpenAI Decisions also requires its independent serving gate; enabling routing
does not enable that adapter.

Switching routing off restores the base validation order before any batch lookup.
A request without a model therefore refuses even when that id names a routed batch.
An originally pinned, final routed batch can replay with its model and decision id at
that same legacy claim boundary. An unfinished routed batch refuses without provider
dispatch; turning routing back on continues its frozen decision. Adding a model to
an originally unpinned batch still changes its immutable body identity. Reports and
release obligations keep draining while admission is switched off.

## Selection and storage

Each new batch selects once with consumer `classifier` and only broca in `runners`.
The routing intelligence-demand hint `iq` is `0`: classification has explicit
question and context requirements rather than a conversational reasoning tier.
A caller-supplied model becomes `pin`, without `pin_fallback`. Routing
requirements use the spend guard's existing estimates: question count, the largest
single-request context, estimated batch input, the caller's cost cap, and image
modality when needed. Only advertised requirement fields are sent.

The selected model must have an installed broca adapter. The classifier contract
accepts catalog kinds `decision` and `classifier`; another stated kind is refused
as stated, while a missing kind stays missing. Selection is followed by pooled
API-key admission through the shared account-routing client. Only keys whose
vault metadata explicitly names the selected provider are offered; an opaque key
id or a missing provider binding never authorizes cross-provider egress.
The select answer is durable before credential listing. The attempt id and exact
pending account-admission request are durable before the admission RPC. Recovery
replays that identical request and releases the resulting admission without
provider dispatch, rather than adopting a possibly abandoned call. The selected model,
credential id, decision id, select answer and frozen catalog bounds/prices are
durable before the first provider request. No secret is stored.

`system-one.db` adds nullable `routing_choice`, `routing_runs` and
`routing_pending` columns using additive schema changes, without a default,
backfill or new index. Null columns identify legacy pinned batches. Item outcomes,
per-run progress, attempt observations and existing measurement intents commit
together before any observation is flushed; routing delivery failure cannot lose
an already-recorded answer. The immutable spend/export fact format does not
acquire a decision id.

Two unavoidable remote-answer/local-commit boundaries remain. If a provider
answered but broca died before committing the answer, the adapters offer no
idempotency key: a resend asks that unrecorded item again, matching the legacy
classifier path. If a select answer was lost before its first local commit, a
resend selects again; that never-dispatched selection has no spend.
`cortexkit-role-routing` 0.7.6's CONTRACT.md §8 makes reporting optional, so a
selection that never dispatched has no mandatory cleanup report.

## Replay and account failures

Stored answers replay before any catalog or routing lookup. A fully retained reply
replays byte-identically without provider calls. A retryable non-account item error
can be dispatched again only on the frozen model and credential; it never selects
again. Account errors are retained rather than retried under a new account.

One exception is allowed before any answer: out-of-credit or credential-refused
failure may select once more and restart the whole batch. The replacement choice
is stored before dispatch, and a caller's pin still applies. After any answer,
there is no reselection and every unanswered item receives the account error.
Resettable account limits do not trigger reselection. One batch never mixes models.

A committed selection survives a refused first account admission, so a resend reuses
that decision instead of silently selecting again. Answered admission and re-admission
refusals use the same `retry_class` mapping, preserving routing's code and reason; no
new run, provider request or account failure is invented. Stored outcomes stay intact.
Answered `invalid_request` and `admission_conflict` refusals are permanent
`routing_refused`, with the original routing code and a local warning naming its
reason. Invalid locally constructed routing requests and malformed routing answers
remain permanent internal errors. A failure to obtain an answer is transient
`routing_unavailable`. When an account admission got no answer, routing may still have granted
the account, so broca keeps that admission's id and releases the grant when it recovers. A local vault-read failure
is a retryable item error, not evidence that the provider refused the credential.

## Dispatch attempts, leases and reports

Each admitted dispatching call has a random, durable attempt id. It admits the
frozen account, sends `admission_started`, forwards provider observations, and
sends `run_finished` when the call ends. A storage-only replay is not a run and
holds no lease. Active calls renew the same account pin at
`now + remaining - min(30 seconds, remaining / 3)`. A renewal uses
`<attempt id>:renew:<n>` and the stored pin generation; a transport retry reuses
that exact request once. The provider future keeps being polled while renewal waits,
so a ready, paid reply is not lost to an adapter deadline. Failed renewal stops further
dispatch without cancelling that reply. For an answered refusal, every undispatched
item receives the same `routing_refused`/permanent or `routing_deferred`/transient
mapping as admission, with `detail.routing_code` and a top-level `retry_after_ms`
delay when timed. The message does not embed routing's code. Transport and locally
invalid renewal failures remain `provider_error`. The full classified refusal stays
in the batch journal and local report metadata.
A resend replays paid answers and re-admits the frozen
choice only for retryable unanswered work. An unknown renewal outcome owes finishes
for the original admission first and the possibly granted renewal second. Successful renewals
make subsequent observations name the latest admission. No lease is held between calls.

The decision freezes its report capability basis: refreshing routing readiness
on a resend cannot switch between per-run and per-decision accounting.
With routing's `report_usage_run_id`, each admitted run reports its own usage under
its attempt id. A run is completed when every dispatched item answered; a failed
item supplies its runner code. Unknown per-request usage makes that report's
corresponding aggregate absent, not zero or a partial sum. Classifier reply usage
still follows its separate answered-item aggregation rule. Credential refusals
and known reset times use the router's advertised reporting/observation groups.

Without that capability, broca reports a decision only when the batch is final.
A batch left with retryable unanswered items has no final report until completion or
expiry. Every abandoned committed decision is closed with a durable report, using
`launch_failed` for pre-dispatch failure (or `cancelled` without `report_outcomes`).
A transient replacement-selection outage does not spend the single replacement;
an invalid or refused replacement is surfaced rather than swallowed. A replacement
failing model, image or ceiling validation leaves the original choice and outcomes
intact and reports the replacement's launch failure.

Each report body is durable before sending. Recovery retries the same body and identity
after a lost acknowledgement; it never invents a replacement body. Delivery is ordered
and stops at the first unknown outcome, including an invalid acknowledgement. Definitive
routing refusals such as `unknown_decision` and `report_conflict` close the entry; a
conflict is logged as a bug. Cleanup runs at boot and every 30 seconds on every root,
independently of the admission switch. One broken batch does not stop other batches.

On legacy roots, seven-day expiry atomically removes item content, answers, request
identity and diagnostics. Only owed routing bodies, their stable cleanup ids, and
metadata needed to deliver them remain in a content-free tombstone. An unknown first
admission retains its exact `account_for` body: recovery replays its idempotency key,
then releases every requested or answered admission id. An unknown outcome keeps that
debt owed. A held, content-free row is removed atomically with release, so an old hold
cannot free a new same-id incarnation. Settled routing debt never extends content TTL.

Overhaul (`broca-state/2`) roots keep the existing destructive-retention fence: routing cleanup still
drains reports, releases, pending admissions and tombstones, but it does not scrub
content or run the ordinary sweep. Live snapshots and claims refuse expired routed
rows on **every** root, so cleanup timing never exposes content past retention.

`llm.classify/v1` results include the model used, as required by llm-tools 0.3.0.
That tool still requires a pin to identify the provider before checking consent
to send content or spend money. Routing an unpinned tool call would need to bind
that consent to the selected provider; the classifier routing switch does not
enable such tool admission.
