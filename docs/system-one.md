# System One classifier routing

The `classifier/v1` route serves sessionless batches of decision-model requests.
Its contract is `cortexkit-role-classifier` 0.6.1, using the shared model-batch
rules; routed operations use `cortexkit-role-routing` 0.7.4.

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
(transient), never fixed-credential fallback. An answered routing refusal gives
`routing_refused` (permanent), preserving its code, reason and the caller's pin.
OpenAI Decisions also requires its independent serving gate; enabling routing
does not enable that adapter.

Switching routing off later does not reinterpret an existing routed row. Stored
final outcomes still replay with their model and decision id. An unfinished routed
batch instead receives transient `routing_unavailable` without a write or dispatch;
turning the switch back on lets it continue under the frozen decision. Reports owed
for earlier real runs keep being drained while the switch is off.

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
API-key admission through the shared account-routing client. The selected model,
credential id, decision id, select answer and frozen catalog bounds/prices are
durable before the first provider request. No secret is stored.

`system-one.db` adds nullable `routing_choice`, `routing_runs` and
`routing_pending` columns using additive schema changes, without a default,
backfill or new index. Null columns identify legacy pinned batches. Item outcomes,
per-run progress and existing measurement intents commit together; the immutable
spend/export fact format does not acquire a decision id.

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

Fresh account-admission refusal leaves no batch or provider request. Refused
re-admission of an existing batch retains its answers and stores an account error
for unanswered items; the error message retains routing's code. Refused admissions
send no run observations or usage report. Transport failure leaves stored outcomes
unchanged and is `routing_unavailable`.

## Dispatch attempts, leases and reports

Each admitted dispatching call has a random, durable attempt id. It admits the
frozen account, sends `admission_started`, forwards provider observations, and
sends `run_finished` when the call ends. A storage-only replay is not a run and
holds no lease. Active calls renew the same account pin at
`now + remaining - min(30 seconds, remaining / 3)`. A renewal uses
`<attempt id>:renew:<n>` and the stored pin generation; a transport retry reuses
that exact request once. Failed renewal stops further dispatch and fails only
unanswered items. No lease is held between calls.

With routing's `report_usage_run_id`, each admitted run reports its own usage under
its attempt id. A run is completed when every dispatched item answered; a failed
item supplies its runner code. Unknown per-request usage makes that report's
corresponding aggregate absent, not zero or a partial sum. Classifier reply usage
still follows its separate answered-item aggregation rule. Credential refusals
and known reset times use the router's advertised reporting/observation groups.

Without that capability, broca reports a decision only when the batch is final.
A batch left with retryable unanswered items has no report and no finalization
deadline. Each report body is durable before sending. Recovery retries the same
body and identity after a lost acknowledgement; it never invents a replacement
body. `unknown_decision` closes the row. `report_conflict` is logged as a bug and
retained without retry. Pending routing work is not removed by ordinary batch
retention. A reselection keeps both decisions' runs and reports in the journal,
while the reply names the decision that answered.

`llm.classify/v1` results include the model used, as required by llm-tools 0.3.0.
That tool still requires a pin to identify the provider before checking consent
to send content or spend money. Routing an unpinned tool call would need to bind
that consent to the selected provider; the classifier routing switch does not
enable such tool admission.
