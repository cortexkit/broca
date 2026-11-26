# System One classification and extraction

The `classifier/v1` route serves sessionless batches of decision-model requests.
Its contract is `cortexkit-role-classifier` 0.6.3, using the shared model-batch
0.3.4 rules; routed operations use `cortexkit-role-routing` 0.7.6.

## Routing readiness

Routing picks the decision model whenever a new `classify.run` batch names none.
Describe declares `routed_models: {}` only after a live routing describe lists
consumer `broca.classifier`. An unavailable or unready router gives `routing_unavailable`
(transient), never fixed-credential fallback. An answered routing refusal uses
routing's `refusals::retry_class`: Permanent gives `routing_refused` (permanent),
Transient or RetryAt gives `routing_deferred` (transient). Both preserve routing's
code, reason and the caller's pin. A timed refusal includes `detail.retry_after_ms`,
the retry timestamp minus the current Unix time in milliseconds, saturating at zero.
An untimed refusal omits that delay. A refused selection writes no batch and sends
no report.
OpenAI Decisions still requires its independent adapter serving gate.

## Selection and storage

Each new batch selects with only broca in `runners` and consumer
`broca.classifier.<caller module id>` (or `broca.classifier.direct` for a direct
opener). Without a model pin, broca sends `iq: 1`, the minimum decision grade,
so routing can pick the cheapest eligible graded model rather than an ungraded
one. A caller-supplied model becomes `pin`, without `pin_fallback` or a grade
requirement. Routing
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

A paid item's System One fact names the caller from the daemon scope stored at batch admission.
An agent-run scope writes optional `agent_id` beside `run_id`; a flow scope writes `flow_id` and
omits both `agent_id` and `run_id`, so a flow's spend never inherits its owner agent. The optional
members stay absent when their scope has no value for them, and stored facts are not backfilled.

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

An internal provider retry follows only an attempt known not to have billed. An
attempt that may have billed but did not report priceable usage reserves its full
ceiling in the batch's budget, without recording that amount as a measurement.
An uncapped attempt with unknown spend reserves the largest amount the ledger can
represent, so a later resend cannot use a new finite cap to ignore it.

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
conflict is logged as a bug. Cleanup runs at boot and every 30 seconds. One
broken batch does not stop other batches.

On legacy roots, seven-day expiry atomically removes item content, answers, request
identity and diagnostics. Only owed routing bodies, their stable cleanup ids, and
metadata needed to deliver them remain in a content-free tombstone. An unknown first
admission retains its exact `account_for` body: recovery replays its idempotency key,
then releases every requested or answered admission id. An unknown outcome keeps that
debt owed. A held, content-free row is removed atomically with release, so an old hold
cannot free a new same-id incarnation. Settled routing debt never extends content TTL.

The daemon refuses roots marked `broca-state/2`; the removed staging format has no
System One service. Live snapshots and claims on supported roots refuse expired
routed rows, so cleanup timing never exposes content past retention.

`llm_classify` (`llm.classify/v1`) uses a supplied model as a pin, without
substitution. Without a pin, it asks routing under
`broca.classifier.<caller>.<run|flow>`, using the caller module and scope kind
the daemon stamped on the route. It asks for `iq: 1`, the lowest positive
quality bar on routing's 0-100 scale, so routing picks the cheapest model with
a decision grade rather than an ungraded one. It serves only per-token priced decision models. The tool checks consent for
the selected provider before anything is sent to it. An approved hold runs on
the selection it was held with, without selecting again. The result names the
model actually used; repeating a recorded `call_key` returns that outcome
without calling routing or the provider again.

## `extractor/v1`

Open a broca management route with `role_versions: {"extractor":"v1"}` and a
daemon-stamped agent/run or flow scope. The route serves `role.describe` and
`extract.run`, not session or classifier operations. Like the classifier route,
it requires a flow scope for Basal and creates no session, lease, or consent
card. Programs and flows that need a single extraction use `llm_extract` below.

An extraction batch names its model, typed fields, and items. Broca serves only
`cloudflare-workers-ai/@cf/cloudflare/clairic`, and only while the catalog lists
it as `extraction`. There is no routing or model substitution. It uses the same
Workers AI credential and account as Clef. `role.describe` lists the nine field
types in `cortexkit-role-extractor` 0.3.0, up to 64 fields and 16 items, and an
8,192-byte limit per answer. When `llm_tools.image_references` is enabled, both
batch roles declare `image_reference` and resolve handles under the caller's
scope before dispatch. Clairic receives the image bytes, not the handle.

`extract.run` validates the request and its worst-case reply size before dispatch.
The spend guard checks each item against the retained batch ceiling using byte
bounds for state and fields. An item that crosses the ceiling returns
`cost_exceeded`; any other item within it can still run. The ledger and usage
facts price only measured Clairic input tokens, not output tokens, passes or
retry tokens. Missing input measurements do not become zero or estimated spend.
An extraction `model_unknown` refusal names the supported model IDs in its message
and in `detail.accepted_models`. These IDs still require a matching extraction
entry in the catalog.

Extraction uses a separate role key in `system-one.db`, so the same `batch_id`
under `classifier/v1` is independent. A resend in the original scope keeps the
recorded model and price and may tighten, not loosen, the ceiling. Stored answers
and permanent errors replay without provider calls; retryable items can run again.
An oversized field answer is a stored `answer_too_large` item error, but any
measured input cost still counts against the ceiling and is exported. As for
classification, the first admission starts a seven-day retention window, and
the same sweeper removes expired batches with their outcomes.

## `llm.extract/v1`

The model-tools route lists `llm_extract` for codemode and flows whenever broca
serves the extractor. The arguments name the Clairic model, typed `fields`, one
`item`, optional extractor `options`, and the required `max_cost_usd` ceiling.
The tool uses the daemon-stamped run or flow scope for spend attribution. Like
`llm_classify`, it checks for private content before contacting the provider and
holds an eligible run's call while consent is pending. Its input schema accepts
image handles only when broca has the image resolver.

The tool sends that one item through `extract.run` using the envelope's
`call_key` as the batch ID. For seven days, a repeat of a recorded call returns
its result or refusal with the original spend and does not call Clairic again.
A successful result carries `answers`, `usage`, and
the measured `cost_usd`. It carries `truncated` only when reported and
`state_text` only when requested and returned. Item failures are tool errors,
not successful results with missing answers. Refusals identify their spend:
for example, a ceiling refusal before a provider call marks `detail.no_spend`,
while a billed item failure with measured cost carries `detail.cost_usd`.
An unknown extraction model also carries `detail.accepted_models` and
`detail.no_spend: true`.

## `llm.complete/v1`

The same tool route also serves `llm_complete` (`llm.complete/v1`): one text or JSON
completion for a program or a flow. The caller names no model: it states a demand, two
scores from 0 to 100 (`iq`, and optionally `eq`) that routing policy reads, and routing
picks the model. Routing is asked under the consumer `broca.complete.<caller>.<run|flow>`,
where the caller is the module the daemon stamped on the route and the scope kind comes
from its scope stamp, never from the arguments. When routing can place the call on a
subscription account it does; otherwise the call runs on a per-token account. `cost_usd`
is the per-token price of the call's usage either way (on a subscription, what the call
would have cost per token), and `max_cost_usd` caps it before anything is sent. Unlike a classify batch, a completion runs as one run with no tools in a
session of its own (`llm-complete:<call_key>:<admitted_at_ms>:<attempt>`), kept 7 days. Its admission and
answer are recorded in the System One store under the role `llm.complete`, so a repeat of
the same `call_key` within 7 days answers the recorded result and is never billed twice.
A recorded refusal is final too, a transient one included: the repeat answers it with the
same spend mark (`cost_usd`, `cost_unmeasured`, `cost_unknown` or `no_spend`) and runs
nothing. A caller that wants another try sends a new `call_key`.
A repeat after the 7 days is a new call: until the hourly sweep removes the old record it is
refused as a transient `provider_error` that spent nothing, and once removed it runs in new
sessions, since the admission time in the session name differs.

A completion from a run or flow that read the person's private content, on a provider not
approved for that content, waits for the person's consent as a classify call does: broca files
the same consent card and answers the call as held, when the caller reads held replies and
names a `call_key`. Nothing is sent until the person answers, and a repeat of the key answers
as held. Once they allow it, the completion runs on the route and at the prices it was held
with, without asking routing again, and its answer arrives as the held call's late result.
