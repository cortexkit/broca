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
- Producer detail (broca `provider_code`/`status`, vault `ReadError` name)
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
  indefinitely (serde alias) so WAL records written before the rename replay clean.
- `ErrorClass::shared_class()` is the boundary mapping for cross-producer
  consumers. Durable provider records keep the closed producer enum; caller error
  wrappers and served refusal class fields decode through open strings.
- Served management refusals with a fixed class are identified by their error code; the
  corresponding code-to-class pairs are:

  | Code | Class | Detail |
  |---|---|---|
  | `variant_unsupported` | `permanent` | `{model, variant, defined}` |
  | `wal_newer_writer` | `permanent` | `{kind: "newer_writer", missing: [<feature names>]}` |
  | `agent_run_scopes_unsupported` | `permanent` | `{<session scope>}` |

- Principal-specific admission refusals `flow_scope_required` and `sender_not_owner` carry a
  top-level `class: "permanent"` beside `code`, `message`, and `detail: {kind, principal}`.
  Their served bytes are pinned by `crates/broca-wire/tests/golden/flow_scope_required.json`
  and `sender_not_owner.json`; the class is mapped by `ErrorClass::shared_class()` before
  serialization.

  Served admission refusals about account selection carry their class in their detail:

  | Code | Class | Detail |
  |---|---|---|
  | `account_selection_refused` | `detail.class`: `transient`, `permanent` or `auth_required` | `{routing_code, class, retry_after_s?, retry_after_secs?, decision_id?, resets_at_ms?, routing_detail?}` |
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
  `unknown_decision`, `unscoped_caller` and `account_not_authorized`. An authorization refusal
  means operator policy excludes the request's credentials; logging in cannot fix it.
  Unknown refusal codes remain verbatim and transient,
  never `routing_unavailable`. Pool exhaustion, reserve limits, account holds and unknown quota use `account_limit_exhausted`
  instead of `account_selection_refused`, so callers wait or reroute rather than hot-retrying.
  Routing's `reset_at_ms` is carried as `resets_at_ms`; when no wait was supplied it also yields a
  `retry_after_secs` hint rounded up to the next second. Holds prefer `until_ms` (Unix
  milliseconds), then legacy `retry_after_s` (seconds); legacy `reset_at` is retained only
  in the original detail and is never parsed. A hold with neither timing field stays untimed.
  `auth_required` retains the admit-and-pause-for-login path, with its routing code in the
  paused run's `auth.routing_code`. `account_selection_disabled` is the routing operator's
  module-wide off-switch: that answer admits the send on the configured static credential,
  records an account decision with path `disabled`, and logs the literal routing code
  `account_selection_disabled`. It is not an account or provider refusal. The undeclared aliases
  `disabled` and `unsupported` do not enable this fallback.
  Callers branch on structured fields,
  never on the message. The bodies are pinned by
  `crates/broca-wire/tests/golden/account_selection_refused.*.json`.

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

  `wal_newer_writer` means the session's WAL holds a complete frame that requires WAL
  features this build does not know: a newer broca wrote it. The bytes are intact and
  nothing was written, so retrying cannot help; the remedy is to place a build that
  knows every name in `missing`. It is not in any retryable set. When the materialized
  store proves a session has history but neither live nor archived WAL can cover it,
  `session.read` answers `history_unreachable`, class `permanent`, with `session_ref` and
  no filesystem paths. Every other failure to open a session's WAL, corruption included,
  stays `open_failed`. The `history_unreachable` and `wal_newer_writer` bodies are pinned by
  their corresponding `crates/broca-wire/tests/golden/` fixtures.

  `open_failed` carries optional `detail.class` in the four-class shared vocabulary.
  Admission and run-resolution causes use `RunError::admission_error_class`, never message
  matching. Typed temporary WAL I/O (`Interrupted`, `WouldBlock`, `TimedOut`) is transient:
  the same read can succeed on retry. Unsupported handler operations are permanent.
  Opaque storage/write/replay/lifecycle errors remain unclassified, including corruption;
  neither a generic WAL error nor replay failure establishes retryability. A newer writer
  retains its dedicated code and unchanged body. When class is absent, no detail is added
  and legacy bytes are unchanged. Consumers must not invent a default class; they may keep
  their legacy fallback for unclassified errors. All other error bodies are unchanged.

### `open_failed` construction inventory

`broca-module::ServeError::Open` has 14 production construction sites in
`crates/broca-module/src/serve.rs`, pinned by
`open_failed_classes::every_open_failed_site_chooses_a_class`:

| Site | Class | Reason |
| --- | --- | --- |
| `final_message_of`, missing RunStarted | absent | Missing history supplies no retryable I/O cause. |
| `final_message_of`, replay failure | absent | Replay errors are not evidence that retry can succeed. |
| `final_message_of`, missing assistant | absent | Transcript shape alone does not establish a remedy. |
| `From<OpenError>`, `Open` | absent | Lease, write, replay and lifecycle causes were erased. |
| `From<OpenError>`, `Classified` | carried | Typed temporary WAL read I/O can succeed on retry. |
| default `head` | permanent | The handler does not implement the operation. |
| default `warm` | permanent | The handler does not implement the operation. |
| default `run_result` | permanent | The handler does not implement the operation. |
| default `baseline` | permanent | The handler does not implement the operation. |
| `send`, `Error` | absent | Actor storage, replay and lifecycle failures are opaque strings. |
| `send`, `ClassifiedError` | `admission_error_class()` | The original admission/run-resolution `RunError` knows the remedy. |
| `retract`, `Error` | absent | Combined WAL read/write errors have erased causes. |
| `subscribe`, `Error` | absent | Attachment errors combine replay and channel failures. |
| `warm`, `Error` | absent | Combined replay, storage and actor failures have erased causes. |

The read classifier only marks `Interrupted`, `WouldBlock` and `TimedOut` transient;
permission errors and other I/O remain unknown. Corrupt WAL bodies remain byte-identical.
`WalOpenError::Failed` erases its cause in `WalWriteError`, so writes and recovery/proof
failures stay unknown rather than guessing from their message. `NewerWriter` is handled
separately before either conversion and never gains `detail.class` here.
