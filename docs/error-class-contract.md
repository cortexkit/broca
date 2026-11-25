# Error Class Contract (v1)

This document defines the normative error-class contract. The vault and
magic-context mirror it by reference; subconscious preserves it on results.

## Principle

**Classification is PRODUCED by the error source, never INFERRED by the consumer.**
A consumer branches on the produced class tag and structured sibling fields only —
never on status codes, message substrings, or producer-detail codes.

Classification is emitted as a structured field, preserved across every layer
boundary, and consumed by branching on the class tag. Message text and status
codes are not substitutes for a producer's classification.

## The closed class set

Wire strings, snake_case, pinned by a conformance test in every producer:

| Wire string | Consumer decision |
|---|---|
| `transient` | Retry may succeed. With `retry_after_secs` present: respect it as a backoff floor (a longer producer value wins; a shorter one may clamp down). Without: consumer's own schedule/routing. |
| `permanent` | Retrying THIS target with THIS input is futile until out-of-band action. Advance to an alternative immediately; if all alternatives are permanent-dead, abandon and surface — never backoff-retry. |
| `auth_required` | A credential/authorization must be re-established by a human or admin. Pause and surface an actionable auth prompt. Provider-scoped: the consumer marks same-provider alternatives dead from its own routing knowledge. |
| `context_overflow` | Input exceeds THIS producer+model's bound; retrying the same input against the SAME model is futile. Whether a larger-bound alternative helps is the CONSUMER's decision (windows are heterogeneous — a 128k overflow may be precisely fixable by a 1M fallback). Producer-neutral: model context windows, bounded batch reads, any bounded-input producer. |

The canonical copy-pastable set every producer constant and golden test asserts
against:

```
ERROR_CLASS_WIRE_SET = ["transient", "permanent", "auth_required", "context_overflow"]
(+ optional sibling retry_after_secs, seconds)
```

## Quota-429 mapping rule

Not all quota errors are the same class, and they all arrive as HTTP 429:

- **Billing/credits exhaustion** (e.g. OpenAI `insufficient_quota`): `permanent` —
  a human must pay; no amount of waiting fixes it.
- **Window quotas** (e.g. Claude 5-hour windows, daily caps): `transient` +
  `retry_after_secs` when the provider supplies it — they self-recover in
  minutes-to-hours.
- **Safe default**: when the producer cannot distinguish which kind a quota-429
  is, map it to `transient`. The costs are asymmetric: mis-classifying as
  transient burns a few retries; mis-classifying as permanent silently kills a
  healthy model and fires a false chain-dead alarm in the consumer's health
  channel.

The `permanent` arm's definition pins as "cannot recover without human or config
action", which makes the quota split derivable per provider code.

**Ordering rule:** code/body discrimination runs before generic status defaults.
A quota-429 is indistinguishable only after provider-code discrimination.

### Timed account-limit recovery

The root `broca.jsonc` setting `max_same_account_wait` does not add a fifth error class.
It defaults to `15`: non-negative integers cap same-account waits in minutes, `0` disables
same-account waiting, and `"max"` waits only up to cache expiry. Other values refuse boot. The obsolete
`account_limit_pause` key is ignored with one boot warning naming `max_same_account_wait`.
The setting is loaded once at boot; changes take effect at the next restart, like other
`broca.jsonc` settings. The existing frozen consent boolean is `setting != 0`; new pause
decisions use the executor's in-memory cap.
The transport's
positive account-wall classification produces `kind: account_limit, scope: account` beside
its existing transient error. Consenting runs persist that disposition with the failed attempt;
replay never infers it from HTTP status, Retry-After alone, message text or producer detail.
Let `wake` be the latest blocking reset/retry floor plus any scheduling jitter,
`cache_expiry` the send time of the session's last 2xx attempt on that account plus the run's
frozen cache TTL (explicit 5 minutes or 1 hour, otherwise 5 minutes), and `cap = now + setting`
(unbounded for `"max"`). A valid, non-stale floor pauses only when
`wake <= min(cache_expiry, cap)` and consent is enabled. A 2xx stream need not complete to
refresh the cache; a refused attempt cannot refresh it. Without an accepted attempt on that
account there is no same-account pause. When that wait is declined, routing may move a run at a
quota wall or, on `no_route` with `reason: concurrency_full` and a future routing reset, trigger
a separate wait once per step, capped at 30 minutes. Otherwise the run ends with the transient
`account_limit_exhausted` error. Routing's reason and reset, when given for that wait, are
`error.account_continue.reason` and `.until_ms`, not the spent account's own reset or retry
hint. Committed pauses replay their recorded wake, never re-decide these bounds.
Untimed walls, resets beyond the cache/cap bounds,
generic 429s, payment exhaustion, authentication and context overflow keep their earlier paths.
Newly stale floors end the run and increment `health.check.metrics.account_limit_stale_resets`;
a committed floor that expires during downtime is instead revalidated for immediate wake.
The always-present health object `account_limit_pause_declined` counts `disabled`,
`no_accepted_response`, `cache_expires_first` and `beyond_cap`; declines log reset, wake,
cache expiry, cap and reason at info level.


### Producer-side status/code refinements

These mappings refine the producer's classification only; they do not add wire vocabulary or
change the consumer contract:

| Provider response | Class |
|---|---|
| HTTP 401 | `auth_required` |
| HTTP 403 (authenticated but forbidden) | `permanent` |
| Anthropic `authentication_error` | `auth_required` |
| Anthropic `permission_error` | `permanent` |

A producer may use a more specific structured provider code before applying a generic status
default. In particular, a credential-authentication code remains `auth_required`, while an
authorization/entitlement denial remains `permanent`. Only the former reaches vault auth-failure
reporting; a 403 must not invalidate a valid credential.

## Sibling fields and the detail channel

Every served run error includes `class`, `message` and `provider_code`, on both storage
implementations. Historical stored `ProviderError` records may omit the code; the
`SharedProviderError` boundary supplies it from the caller-facing class without rewriting
the record:

| Caller-facing class | Code when the stored error has none |
|---|---|
| `transient` | `transient` |
| `auth_required` | `auth_required` |
| `context_overflow` | `context_overflow` |
| `permanent` or an unrecognized class | `provider_error` |

An existing code, including one a reader does not recognize, stays unchanged. The provider's
finer `provider_subcode` and `provider_details` diagnostics also stay unchanged; they are not
promoted into the required code. New-storage terminal `error` waits use this served error
with llm-runner's `ending_awaits`; legacy storage still omits `awaits`.

- `retry_after_secs` (seconds — the only unit) is an optional structured sibling,
  not arm data. Rate-limitedness is a transport phenomenon, not a consumer
  decision: a 429 with `Retry-After` is `transient` + the field; a 429
  `insufficient_quota` (billing exhaustion) is `permanent`.
- Normalization is the PRODUCER's job: whatever shape the provider sends
  (`Retry-After` seconds or HTTP-date, `retry-after-ms`, rate-limit-reset headers,
  reset hints in body text), the producer folds it into `retry_after_secs` so no
  consumer ever parses headers or text. Which shapes count is per provider family:
  the OpenAI Responses and ChatGPT families read only what Codex reads (whole
  seconds or an HTTP date in `Retry-After`, plus Codex's `try again in` text), not
  `retry-after-ms`. Consumers treat the value as a backoff floor and may cap it —
  capping policy stays consumer-local.
- Finer producer detail (broca `provider_subcode`/`status`, vault `ReadError` name)
  travels alongside for diagnostics, logging, and corroboration ONLY. A consumer
  branching generically on detail is the bug this contract kills.
- `ProviderError.message` is retained verbatim from the provider on caller and status surfaces. It
  may echo request content or credentials; callers must treat it as untrusted text, and this
  contract makes no redaction or secret-removal guarantee. Classification must never be inferred
  from it.

## Guard: arm-admission test

*"Does a consumer branch on it GENERICALLY?"*

- `ProviderUnavailable` (broca): NO — its generic decision is `transient`'s;
  the runner's respawn-and-reissue mechanics are producer business. It maps to
  `transient` at shared boundaries (`ErrorClass::shared_class`) and survives in
  detail.
- `ContextOverflow`: YES — the compaction/reduce trigger is a generic consumer
  decision distinct from both `permanent` and `transient`. A consumer whose
  inputs are budget-sized below every target's bound may treat an overflow as its
  own estimator bug and short-circuit — that mapping is consumer-specific, not
  contract semantics.

## Consumer decoding: OPEN on read, PINNED on write

The set is pinned for PRODUCERS. Consumers should decode `class` as an OPEN string
rather than a closed enum, so an unrecognised value becomes a PASSTHROUGH rather
than a decode failure.

This is not a softening of the contract, and it costs nothing while the set holds.
It decides what happens on the day it does not: with a closed enum, adding a fifth
class turns every existing consumer's decode into an error on a field they were
reading fine — the producer ships a strictly more precise error and the consumer
reports a parse failure, which is the least useful outcome available. With an open
string, an unknown class arrives intact and can be logged, surfaced, or defaulted
by the consumer's own policy.

A consumer that pins the enum makes vocabulary extension a BREAKING change for
everyone at once, which in practice means it never happens and producers smuggle
new meanings into `message` text instead. That is the failure this rule exists to
prevent.

Producers assert the closed set. Widening is a contract amendment; open consumer
reads protect against vocabulary changes without licensing arbitrary new classes.

## Placement and migration

The contract is the WIRE STRING SET, not a Rust type. Each producer keeps its own
enum; a pinned string-set serde test per producer asserts conformance. Each producer mirrors this wire contract independently.

broca specifics:

- The canonical enum is `ErrorClass` in `crates/broca-protocol/src/error.rs`.
- The `Auth` arm's wire spelling is `auth_required`, with `auth` accepted on read
  indefinitely (serde alias) so records written before the rename read clean.
- `ErrorClass::shared_class()` is the boundary mapping for cross-producer
  consumers. Durable provider records keep the closed producer enum; caller error
  wrappers and served refusal class fields decode through open strings.
- Served management refusals with a fixed class are identified by their error code; the
  corresponding code-to-class pairs are:

  | Code | Class | Detail |
  |---|---|---|
  | `variant_unsupported` | `permanent` | `{model, variant, defined, class: "permanent"}` |
  | `tool_alias_invalid` | `permanent` | `{tool, class: "permanent"}` |
  | `agent_run_scopes_unsupported` | `permanent` | `{<session scope>}` |
  | `projection_not_ready` | `detail.class: transient` | `{class}`; the provider catalog that decides compaction entry attribution could not be read, so `session.entries` may succeed on retry |
  | `transient` | `detail.class: transient` | `{class, cause?}`; a dependency the request needs has not answered or loaded yet (for a send: the model catalog, a credential lookup), so the same request may succeed on retry. Callers retry by this code alone. `cause` is present only where broca names the dependency: `cause: "credentials_unavailable"` means broca's vault credential listing has not loaded yet; `cause: "module_stopping"` means broca refused the request because it is stopping, the request did nothing and may be repeated after the restart (it says nothing about whether an earlier request with the same `send_id` was admitted) |

- Principal-specific admission refusals `flow_scope_required` and `sender_not_owner` carry a
  top-level `class: "permanent"` beside `code`, `message`, and `detail: {kind, principal}`.
  Their served bytes are pinned by `crates/broca-wire/tests/golden/flow_scope_required.json`
  and `sender_not_owner.json`; the class is mapped by `ErrorClass::shared_class()` before
  serialization.

  Served admission refusals about account selection carry their class in their detail:

  | Code | Class | Detail |
  |---|---|---|
  | `account_selection_refused` | `detail.class`: `transient` or `auth_required` | `{routing_code, class, retry_after_s?, retry_after_secs?, decision_id?, resets_at_ms?, routing_detail?}` |
  | `account_selection_final` | `detail.class`: always `permanent` | same fields as `account_selection_refused`; no `provider_code` |
  | `account_limit_exhausted` | `detail.class`: `transient` | same fields, plus `provider_code: "account_limit_exhausted"` |
  | `routing_unavailable` | `detail.class`: always `transient` | `{reason, class, budget_ms?}` |

  The fetch-plan admission refusal `scoped_tools_require_plan` is permanent and carries
  `detail: {class: "permanent"}`. A new send declaring non-empty legacy `tools` on a stamped
  route or an already-scoped session needs a fetch plan (carried by the send or already frozen
  in the session's manifest): scoped tool providers require a per-call preset, which only the
  manifest supplies. Nothing is fetched or written for this refusal. Existing scope and plan
  refusals retain precedence, and an already-admitted keyed resend keeps its existing answer.
  The served bytes are pinned by
  `crates/broca-wire/tests/golden/scoped_tools_require_plan.json`.

  `account_selection_refused` means the routing module would not choose a provider account
  for the send; nothing was written. `routing_code` is routing's own refusal code, copied
  verbatim with its original detail in `routing_detail`, `retry_after_s` and `decision_id`. It is `transient` for
  `account_hold`, `account_pool_exhausted`, `account_pool_reserve_limited`,
  `account_quota_unknown`, `no_eligible_account`, `account_selection_unavailable`,
  `pin_generation_conflict`, `store_error` and `internal_error`, with `retry_after_secs` set
  from routing's wait when it gave one; `permanent` for `admission_conflict`, `invalid_request`,
  `unknown_decision`, `unscoped_caller`, `account_not_authorized` and
  `account_selection_unsupported`, and for every routing code broca does not know: until broca
  learns a code it can't tell that waiting would clear it, and holding a send forever on a
  refusal that can't clear is the worse failure (routing's own contract classes every untimed
  refusal as permanent except `account_selection_unavailable`). An authorization refusal
  means operator policy excludes the request's credentials; logging in cannot fix it.
  `account_selection_unsupported` means the account cannot serve the requested provider;
  it is a permanent refusal, not a static-credential fallback.
  A permanent refusal goes out with the outer code `account_selection_final`: routing refused
  the account choice and waiting can't change it, so a caller deciding an account wait by the
  outer code alone never waits on it. Its `detail` is the same as `account_selection_refused`'s.
  A queued send refused this way ends with `provider_code: "account_selection_final"` and class
  `permanent` on its run's `error`. Refusal codes, known or not, stay verbatim in
  `routing_code` and are never `routing_unavailable`. Pool exhaustion, reserve limits, account
  holds and unknown quota use `account_limit_exhausted`
  instead of `account_selection_refused`, so callers wait or reroute rather than hot-retrying.
  Routing's `reset_at_ms` is carried as `resets_at_ms`; when no wait was supplied it also yields a
  `retry_after_secs` hint rounded up to the next second. Holds prefer `until_ms` (Unix
  milliseconds), then legacy `retry_after_s` (seconds). A legacy wait's end is the time broca
  received the routing answer plus that wait. Either form supplies `resets_at_ms`, with
  `retry_after_s` and `retry_after_secs` derived from that end and rounded up to the next
  second. Legacy `reset_at` is retained only in the original detail and is never parsed.
  A hold with neither numeric timing field stays untimed.
  `auth_required` retains the admit-and-pause-for-login path, with its routing code in the
  paused run's `auth.routing_code`. `account_selection_disabled` is the routing operator's
  module-wide off-switch: that answer admits the send on the configured static credential,
  records an account decision with path `disabled`, and logs the literal routing code
  `account_selection_disabled`. It is not an account or provider refusal. The undeclared aliases
  `disabled` and `unsupported` are routing codes broca does not know, so they are permanent
  `account_selection_final` refusals, not fallbacks. A send that carries an account hint while
  routing answers `account_selection_disabled` is refused the same way.
  Callers branch on structured fields,
  never on the message. The bodies are pinned by
  `crates/broca-wire/tests/golden/account_selection_refused.*.json` and
  `crates/broca-wire/tests/golden/account_selection_final.*.json`.

  `routing_unavailable` means broca got no usable account decision from the routing module
  for a send that needs one: nothing was written and broca did not pick an account itself.
  It is broca's statement, not routing's, so it never appears as a `routing_code`.
  `detail.reason` is `unreachable` (the route failed, the connection was lost, the reply could
  not be decoded),
  `no_decision_within_budget` (the admission budget ran out; `budget_ms` names it) or
  `invalid_pick` (routing chose an account broca had not offered). Always `transient`: the
  caller retries the same send. A run already admitted keeps its frozen selection on resume
  and is never routed again. The bodies are pinned by
  `crates/broca-wire/tests/golden/routing_unavailable.*.json`, which the real serve path's
  replies are compared with.

  A log holding a record kind or must-understand field this build does not know was written
  by a newer broca; release placement refuses that downgrade through the state root's feature
  floor, so a served session never reaches it. Every failure to open a session, corruption
  included, is `open_failed`.

  `open_failed` carries optional `detail.class` in the four-class shared vocabulary.
  Admission and run-resolution causes use `RunError::admission_error_class`, never message
  matching. A send refused before its run starts gets the same typed refusal (its own code,
  detail and class) whichever storage keeps the session and whether or not it carries a fetch
  plan; a run task that panicked or was aborted before its start stays unclassified. An open
  that may succeed when repeated (the session store's key still loading, for example) answers
  the retryable code `transient` with `detail.cause`. Unsupported handler operations are
  permanent. Opaque storage, write and lifecycle errors remain unclassified, including
  corruption; no generic storage error establishes retryability. When class is absent, no
  detail is added. Consumers must not invent a default class; they may keep
  their legacy fallback for unclassified errors. All other error bodies are unchanged.
