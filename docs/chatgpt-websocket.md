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

A socket is opened lazily for the first eligible attempt, reused across steps and
retries, and discarded on failure, changed authentication, cancellation, graceful
stop, task abort or run end. Before reuse, all current handshake headers are compared
with the connection's authenticated headers; a refreshed token requires a fresh
handshake and discards old routing state. Run shutdown interrupts both reads and blocked
request/Pong writes; even the final close write is bounded to one second. Frozen-versus-served account mismatches pause
before any socket frame is sent. No connection or fallback state survives a run. A new
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

Receipts never cross socket generations or runs. Reconnect, endpoint/header/auth revision
changes, age, teardown, HTTPS switching, failed send/stream, consumer cancellation, incomplete
responses and a resumed run's first request all require full input. All incomplete reasons,
including interrupted, conservatively avoid seeding a receipt. There is no cross-run cache,
socket warmup or cut-response continuation. Recorded profile absence remains legacy on resume;
the renderer ABI does not change.

A failed connection/upgrade (including timeout and HTTP refusal other than 401)
falls back to HTTPS **before any response.create was sent**, within that same model attempt.
The fallback is not an extra attempt. Upgrade 401 instead enters the existing token
re-read/retry/pause path: an unavailable or unchanged token pauses, while a fresh same-account
token gets one recovery attempt. No stale-token HTTPS fallback is made on a 401.

An upgrade 403 is not treated as a token rejection: freshly refreshed tokens have been seen to
get the same 403. It triggers no credential re-read; the same attempt is sent over HTTPS and
classified there like any HTTPS answer (a `usage_limit_reached` 429 is
`account_limit_exhausted`, an entitlement 403 is permanent, a 401 enters the credential path),
and the rest of that run uses HTTPS. When the first 512 bytes of a refused upgrade's body
already classify as a ChatGPT usage limit, that refusal is reported directly without the HTTPS
round trip. Every HTTP upgrade refusal logs one WARN with its status, the next step, and only
the `cf-ray`, `x-request-id`, `request-id`, `server`, `content-type` and `x-codex-*` headers;
the body is never logged or stored. Socket
failures accumulate within a run; after six failures the remainder of that run uses HTTPS. A failure after sending
is never immediately re-sent over HTTPS: it goes through the existing attempt retry
schedule and wall budget. The threshold is retained across steps because an upgrade
fallback can succeed over HTTPS and otherwise hide repeated socket failure. Existing
retry limits are not extended by a transport change.

A close/error before completion is `chunk_error`. Code 1012 means server restart.
At 895 seconds or later that cut follows the existing non-reissuable `response_deadline`
rule. Responses error-event wrappers with HTTP statuses use the existing HTTP classifier;
ordinary Responses events, including tool calls, use the existing Responses normalizer.
The named `previous_response_not_found` code is recognized before generic status mapping,
including missing status and wrapped 400. Broca retires the failed socket and retries with full
input through the normal accounted attempt loop, not a same-attempt resend. Retry advice is read from
nested error headers first, with top-level fallback when nested advice is unusable.

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


Mutation controls to exercise with targeted tests:

- Disable connection reuse → `transport::websocket_tests::websocket_reuses_one_handshake_across_steps`.
- Skip pre-send HTTPS fallback → `transport::websocket_tests::websocket_refused_upgrade_falls_back_without_double_sending`.
- Drop transport evidence → `transport::websocket_tests::websocket_attempt_durably_records_transport_and_preserves_body_digest`.
- Close before completion → `transport::websocket_tests::websocket_socket_stays_open_until_response_completed`.
- Reuse a socket authenticated with an old token → `transport::websocket_tests::websocket_token_refresh_between_steps_reauthenticates_before_sending`.
- Leave a socket pump stuck in a write after cancel → `transport::responses_websocket::pump_tests::cancel_interrupts_a_blocked_socket_write`.

The catalog also proves a WebSocket step followed by a reconstructed HTTPS warm across
caller scopes, clamped scopes, session-derived scopes and the legacy fallback. The
`authorization`, `session-id`, `chatgpt-account-id`, `originator` and `user-agent` values
match across pipes, as does the body's `prompt_cache_key` (absent on both for the legacy
fallback). Socket-only `openai-beta`, `thread-id`, `x-client-request-id`, WebSocket upgrade
and compression headers are intentionally absent from the HTTPS warm; they are not cache
affinity differences. See `live::tests::websocket_step_and_recorded_https_warm_share_cache_affinity`
in `crates/broca-catalog/src/live.rs`. Additional mutation candidates are the diagnostic
privacy switch and the reconstructed warm's session-affinity header.
