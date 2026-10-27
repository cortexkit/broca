# ChatGPT Responses WebSocket transport

`OpenAiResponsesChatgpt` prefers Responses WebSockets. Other families and their rendered
bytes are unchanged. Keep-warm requests always use HTTPS.

## Break-glass configuration

Set the following in the module's `broca.jsonc` (`BROCA_CONFIG_PATH` overrides its
location; otherwise it is under the shared broca config home):

```jsonc
{
  "chatgpt_force_https": true
}
```

The default is **false**. Setting it to true forces only this family's model attempts
onto the existing HTTPS/SSE path. The file is validated at boot and read per attempt,
so an outage switch takes effect between attempts of an active run without changing
admitted render configuration. A malformed runtime edit fails safe to HTTPS and logs
a configuration warning. Turning it off allows sockets again unless the run has
already exhausted its socket-failure budget.

## Wire and lifetime

A socket is opened lazily for the first eligible attempt and reused across steps and
retries. For the Codex profile, healthy completed runs also return it to an in-memory
owner keyed by the full session bind (project root, harness, session), on by default.
Each run borrows exclusively; other sessions cannot borrow its socket. Failure, changed
authentication, cancellation, graceful stop and task abort retire it. Before reuse,
all connection-scoped headers are compared exactly with the authenticated headers;
only turn id and start time inside `x-codex-turn-metadata` may vary. Its stable
installation/session/thread/window fields still fence the connection; current turn
metadata is also sent in each request. A refreshed token requires a fresh
handshake and discards old routing state. Run shutdown interrupts both reads and blocked
request/Pong writes; even the final close write is bounded to one second. Frozen-versus-served account mismatches pause
before any socket frame is sent. Failure budgets, forced HTTPS and server turn state
remain run-local. The owner retains at most 64 idle sockets, evicts the oldest idle
entry at capacity. A shared 64 MiB conservative owned-receipt byte budget also evicts
oldest idle entries; a single receipt over the budget is not retained. The charge
includes string/array capacities and room for map storage, not process RSS. The owner
closes idle sockets after 10 minutes without requiring another
run. The 40-minute absolute age starts at establishment, never at checkout. Session
close, deletion, move, observed credential rejection and shutdown discard retained
state. Nothing is persisted; a restart begins with full input. A new
connection has a 15-second connect/upgrade deadline. Both transports share the same
normalizer and semantic idle bounds: 300 seconds without a complete record, 1800
seconds of only keepalives. Ping/pong traffic counts as keepalive, not model progress.

Without a frozen `chatgpt_wire` profile, every `response.create` contains the **full canonical input**. There is no
`previous_response_id`, incremental continuation, cut-response resume or socket warmup.
The JSON object differs from the rendered HTTPS body only by `type: "response.create"`
and, when supplied by the server, `client_metadata["x-codex-turn-state"]`. Request field
order need not equal HTTPS's canonical order; the framed payload has its own digest.

Legacy handshake headers retain the existing backend authorization/account/session/originator/
user-agent and content headers, plus `OpenAI-Beta: responses_websockets=2026-02-06`,
`thread-id` and `x-client-request-id` (broca's session identity serves as its thread
identity). The client negotiates permessage-deflate using a pinned client fork.
Turn state is not attached to the handshake; broca
captures it from upgrade headers or `response.metadata` and attaches it to subsequent
request metadata. Optional beta-feature headers already present in auth headers are
preserved. Attestation, routing hints and timing instrumentation are not
synthesized without their corresponding sources.

New ChatGPT admissions freeze profile `codex-0.163.0` and its sourced identity. It sends
developer instruction fragments, typed text arrays, explicit parallel-tool policy, empty
tools and default choice, encrypted-reasoning include, and ordinary identity metadata.
Unknown model effort/summary/verbosity defaults are not guessed; caller controls remain
authoritative. Its User-Agent and originator are frozen at admission, thread and request
headers use full-conversation identity rather than cache scope, and upgrades omit HTTP-only
SSE entity defaults. Routing fields precede large input only in transport encoding.

For this profile, the canonical full body is still rendered and hashed once. A live socket
can consume its preceding successfully normalized `response.completed` receipt exactly once.
Continuation compares Codex 0.163.0's eleven properties: `model`, raw `tools`, `tool_choice`,
`parallel_tool_calls`, `reasoning`, `store`, `stream`, `include`, `service_tier`,
`prompt_cache_key` and `text`. Current input must extend or equal prior input followed by
completed assistant output, converted to input items by the full-history renderer.
Annotations under `internal_chat_message_metadata_passthrough` may differ, but
`executed_tool_calls` entries carrying `tool_result_metadata` must retain their positions,
tool names, arguments and metadata values. A suffix cannot update those earlier results.
A proven match sends `previous_response_id` plus only the suffix; empty suffixes and
`store:false` are allowed. Broca samples wall-clock milliseconds before frame serialization
for `client_metadata["x-codex-ws-stream-request-start-ms"]`, outside the canonical digest.
Frame hashes can therefore differ between retries; the stamp is not a network acknowledgement.

Receipts never cross socket generations. They cross a healthy run boundary only together
with the same authenticated socket. Reconnect, endpoint/connection-header/auth revision
changes, age, idle or capacity eviction, teardown, HTTPS switching, failed send/stream,
consumer cancellation and incomplete responses require full input. Compaction, history
edits and any continuation-property mismatch also send full input, even on a retained
socket. All incomplete reasons, including interrupted, conservatively avoid seeding a
receipt. A process restart loses every receipt, including for resumed runs. There is no
socket warmup or cut-response continuation. Recorded profile absence remains legacy on resume;
the renderer ABI does not change.

A failed connection/upgrade (including timeout, and an HTTP refusal other than a 401, a 429 or
a refusal carrying `Retry-After`) falls back to HTTPS **before any
response.create was sent**, within that same model attempt. The fallback is not an extra
attempt. Upgrade 401 instead enters the existing token re-read/retry/pause path: an
unavailable or unchanged token pauses, while a fresh same-account token gets one recovery
attempt. No stale-token HTTPS fallback is made on a 401.

A refused upgrade that is a 429, or that carries a `Retry-After`, is the attempt's answer, as in
Codex (`codex-api/src/endpoint/responses_websocket.rs:597-611`). Its status, the first 512 bytes
of its body and its `Retry-After` header are classified like an HTTPS answer, and the retry rule
below decides what follows. It is not sent over HTTPS at once, because that would re-send before
the server's time.

An upgrade 403 is not treated as a token rejection: freshly refreshed tokens have been seen to
get the same 403. It triggers no credential re-read; the same attempt is sent over HTTPS and
classified there like any HTTPS answer (a `usage_limit_reached` 429 is
`account_limit_exhausted`, an entitlement 403 is permanent, a 401 enters the credential path),
and the rest of that run uses HTTPS. When the first 512 bytes of a refused upgrade's body
already classify as a ChatGPT usage limit, that refusal is reported directly without the HTTPS
round trip. Every HTTP upgrade refusal logs one WARN with its status, the next step, and only
the `cf-ray`, `x-request-id`, `request-id`, `server`, `content-type` and `x-codex-*` headers;
the body is never logged or stored. Socket failures accumulate within a run; after six failures
the remainder of that run uses HTTPS. A failure after sending is never immediately re-sent over
HTTPS: it goes through the retry rule below. The threshold is retained across steps because an
upgrade fallback can succeed over HTTPS and otherwise hide repeated socket failure. These
transport choices do not reset the retry count.

## Retries

This family and the API-key `OpenAiResponses` family retry exactly as Codex does (revision
af91f6ef2c), in `crates/broca-provider/src/transport_codex_retry.rs`. Codex runs both families
through one retry path. Every other family keeps broca's schedule (six sends, 2 s doubling
backoff, `Retry-After` capped at 120 s, 240 s waiting budget).

- **Server retry time.** Only the `Retry-After` header (whole seconds or an HTTP date) counts,
  read from an HTTPS answer, a refused upgrade, or the `headers` of a wrapped socket error frame
  (nested `error.headers` first, then the top-level `headers`). `retry-after-ms` is not read. A
  streamed `response.failed` contributes the `Retry-After` in its error's `headers`, or for
  `rate_limit_exceeded` and `slow_down` the "try again in 11.054s" phrase in its message.
- **Terminal or retried.** A 429 is retried only when the server gave a retry time, unless its
  code is one Codex ends at once: `flex_unavailable`, `usage_limit_reached` (broca's
  `account_limit_exhausted`), `usage_not_included`, or a quota or credit code (`insufficient_quota`,
  `credit_balance_exhausted`, broca's `billing_exhausted`, and the organization and project
  spend and usage limits). A `server_is_overloaded` 503 or streamed failure is also retried only
  on a server time. Other retryable failures wait for the server's time when there is one, and
  otherwise for Codex's backoff: 200 ms doubling per retry, times a factor drawn from 0.9..1.1,
  with no cap. broca's own vetoes still end the step first: a permanent, auth or
  context-overflow class, `x-should-retry: false`, an exhausted account or balance, a second
  deterministic decode failure, and the 895-second response deadline.
- **Waiting.** A server time is waited out in full, never capped, and never cut short. There is
  no total waiting budget. If the caller's step deadline comes first, the step ends at once with
  `retry_after_exceeds_deadline`, and the error keeps the server's last retry time.
- **Counts.** An HTTPS send first gets Codex's request-layer retries: a connect failure, header
  timeout, failure before a status, or 5xx answer is re-sent up to 4 times, waiting for the
  server's time or 200 ms doubling (jittered the same way). A 429 skips that layer. After that,
  up to 5 stream retries follow. Each one starts a fresh request layer. A connect failure is
  retried without limit, waiting 5 s and doubling to 60 s with no jitter. It ignores any server
  time and doesn't count against the 5 retries. Only cancel, shutdown or the step deadline
  stop it. Each send writes one content-free attempt record, roughly 285 an hour while an
  endpoint stays unreachable.
- **Fallback.** When the 5 stream retries are spent on the socket, the run switches to HTTPS
  for the rest of the run. It waits out the server's time if one was given, and starts a fresh
  count of 5 retries. A step already on HTTPS is not switched again, so the next exhausted
  count ends the step. Two mid-response cuts in a row do not switch transport.
- **Durability.** Every retry sends the same rendered bytes. A wait observes cancel (the run
  ends Cancelled), a closed receiver, and the shutdown seal (the run ends Interrupted), and it
  holds no lock or attempt slot. A restart during a wait resumes the step the same way a restart between attempts does, with a fresh count.

A close/error before completion is `chunk_error`. Code 1012 means server restart.
At 895 seconds or later that cut follows the existing non-reissuable `response_deadline`
rule. Responses error-event wrappers with HTTP statuses use the existing HTTP classifier;
ordinary Responses events, including tool calls, use the existing Responses normalizer.
The named `previous_response_not_found` code is recognized before generic status mapping,
including missing status and wrapped 400. Broca retires the failed socket and retries with full
input through the normal accounted attempt loop, not a same-attempt resend. Retry advice is read from
nested error headers first, with top-level fallback when nested advice is unusable.

A move's busy probe retires only idle retained sockets, never an active run's
transport. Invalidating an active entry cancels its socket without resetting that
run's failure budget, forced-HTTPS choice or turn state. A server
`websocket_connection_limit_reached` error closes that credential's idle retained
sockets before the ordinary accounted retry, leaving other credentials and active
runs alone. A reused socket that the peer closes after checkout is logged and still
uses the existing accounted recovery path.

## Durable evidence

New `model_attempt_finished` records contain optional `transport` (`websocket` or `https`).
Old records decode with the field absent and re-encode byte-identically. The existing
`request_body_len` and `request_body_sha256` still cover the canonical rendered body,
so cache-dump and prefix/cache identity are unchanged. `websocket_request` separately
records the length and SHA-256 of the exact UTF-8 request payload before compression.
Codex-profile frames additionally record continuation, suffix item count (zero on full input),
connection generation, previous-ID presence, confirmed physical send count and full-input
reason. These nested fields, and run starts/refreezes carrying the profile, require
`chatgpt-codex-wire/v1`; the feature floor is published before the gated frame.
`websocket_close_code` records a received server close code. Response digests cover
HTTP body bytes or socket text payloads before adaptation, not WebSocket framing or
ping/pong payloads. The mid-stream WARN identifies transport and numeric close code;
it never logs server close reasons or credentials. Socket errors omit backend body text,
and unknown backend error-code strings are omitted from diagnostics while their class
and retry decisions are retained. Header values are marked sensitive; library IO runs
with payload/handshake tracing disabled, including at trace level. The shared Responses
normalizer has a diagnostics-only privacy switch for sockets; HTTPS keeps its existing
behavior and all model-output decoding is unchanged.

Each websocket attempt also emits one log-only summary on the
`provider.websocket.attempt` target: INFO for failed attempts (including refused
upgrades) or attempts longer than 120 seconds, DEBUG for shorter successes. It
separates enqueued `response.create` frames from successful sends, other outgoing
Text and Pongs, and incoming Text/Ping/Pong/Close frames. It includes socket
generation, reuse, age at send, numeric close code, and only the allowlisted
`websocket_connection_limit_reached` wrapped error code (`other` for unknown codes).
No message, payload, header, token, or raw identifier is included.

Raw Text observations before normalization count sequence numbers, their range,
and numbers at or below the previous high-water mark (repeats or backwards
movement). They also count distinct response/item IDs and record the maximum
output index, argument-delta count, total decoded UTF-8 delta bytes, counts of
nonempty whitespace-only and empty deltas (`empty_deltas`), and distinct-delta
count. A minimal borrowed envelope skips unrelated
fields without building a JSON value tree; unescaped strings borrow the frame,
while JSON-escaped strings are decoded before measuring bytes or hashing.
Distinct counts use per-attempt randomized hashes capped at 4,096 entries each;
an overflow flag means the count is a lower bound. Hashes themselves are not
logged, and sets are freed at attempt end, including cancellation. Frame counts
come from the socket pump and can include Text read ahead of the normalizer;
sequence and delta counts cover Text consumed by the attempt. These diagnostics
do not change request bytes, durable attempt records, retries or deadlines.

## Tests

Offline socket tests are in `crates/broca-provider/src/transport_websocket_tests.rs`.
They exercise a complete tool turn through the real core/WAL, reuse, restart/reconnect,
close diagnostics/deadline, refused-upgrade fallback, sticky fallback, cancellation,
shutdown-seal handover, identical canonical body hashes, exact handshake headers,
turn-state metadata, token rotation, upgrade auth recovery, durable auth/account pauses,
and shared virtual-time idle bounds. Existing WAL pinned-byte tests
retain every old record unchanged and include present/absent transport evidence.


Targeted regression tests cover:

- Connection reuse → `transport::websocket_tests::websocket_reuses_one_handshake_across_steps`.
- Pre-send HTTPS fallback → `transport::websocket_tests::websocket_refused_upgrade_falls_back_without_double_sending`.
- Upgrade 429 waits exactly its `Retry-After`, then falls back to HTTPS with a fresh count → `transport::websocket_tests::codex_upgrade_429_waits_out_retry_after_then_falls_back_to_https_with_a_fresh_count`.
- Upgrade 429 without a retry time ends the step → `transport::websocket_tests::codex_upgrade_429_without_retry_after_ends_the_step`.
- Server time past the step deadline → `transport::websocket_tests::codex_upgrade_retry_after_past_the_step_deadline_ends_the_step`.
- Cancel during a long wait ends the run Cancelled → `transport::websocket_tests::codex_cancel_during_a_long_retry_after_wait_ends_the_run_cancelled`.
- One fallback with a fresh count → `transport::websocket_tests::websocket_exhaustion_falls_back_to_https_once_with_a_fresh_codex_count`.
- Codex retry decision and backoffs → `transport::codex_retry::tests`.
- Durable transport evidence → `transport::websocket_tests::websocket_attempt_durably_records_transport_and_preserves_body_digest`.
- Completion before close → `transport::websocket_tests::websocket_socket_stays_open_until_response_completed`.
- Token refresh → `transport::websocket_tests::websocket_token_refresh_between_steps_reauthenticates_before_sending`.
- Socket-pump cancellation → `transport::responses_websocket::pump_tests::cancel_interrupts_a_blocked_socket_write`.

The catalog also proves a WebSocket step followed by a reconstructed HTTPS warm across
caller scopes, clamped scopes, session-derived scopes and the legacy fallback. The
`authorization`, `session-id`, `chatgpt-account-id`, `originator` and `user-agent` values
match across pipes, as does the body's `prompt_cache_key` (absent on both for the legacy
fallback). Socket-only `openai-beta`, `thread-id`, `x-client-request-id`, WebSocket upgrade
and compression headers are intentionally absent from the HTTPS warm; they are not cache
affinity differences. See `live::tests::websocket_step_and_recorded_https_warm_share_cache_affinity`
in `crates/broca-catalog/src/live.rs`.
