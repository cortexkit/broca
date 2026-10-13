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

Every `response.create` contains the **full canonical input**. There is no
`previous_response_id`, incremental continuation, cut-response resume or socket warmup.
The JSON object differs from the rendered HTTPS body only by `type: "response.create"`
and, when supplied by the server, `client_metadata["x-codex-turn-state"]`. Request field
order need not equal HTTPS's canonical order; the framed payload has its own digest.

Handshake headers retain the existing backend authorization/account/session/originator/
user-agent and content headers, plus `OpenAI-Beta: responses_websockets=2026-02-06`,
`thread-id` and `x-client-request-id` (broca's session identity serves as its thread
identity). The client negotiates permessage-deflate using a pinned client fork.
Turn state is not attached to the handshake; broca
captures it from upgrade headers or `response.metadata` and attaches it to subsequent
request metadata. Optional beta-feature headers already present in auth headers are
preserved. Attestation, routing hints and timing instrumentation are not
synthesized without their corresponding sources.

A failed connection/upgrade (including timeout and HTTP refusal other than 401/403)
falls back to HTTPS **before any response.create was sent**, within that same model attempt.
Upgrade 401/403 instead enters the existing token re-read/retry/pause path: an unavailable
or unchanged token pauses, while a fresh same-account token gets one recovery attempt.
No stale-token HTTPS fallback is made on an auth refusal. Socket
failures accumulate within a run; after six failures the remainder of that run uses HTTPS. A failure after sending
is never immediately re-sent over HTTPS: it goes through the existing attempt retry
schedule and wall budget. The threshold is retained across steps because an upgrade
fallback can succeed over HTTPS and otherwise hide repeated socket failure. Existing
retry limits are not extended by a transport change.

A close/error before completion is `chunk_error`. Code 1012 means server restart.
At 895 seconds or later that cut follows the existing non-reissuable `response_deadline`
rule. Responses error-event wrappers with HTTP statuses use the existing HTTP classifier;
ordinary Responses events, including tool calls, use the existing Responses normalizer.

## Durable evidence

New `model_attempt_finished` records contain optional `transport` (`websocket` or `https`).
Old records decode with the field absent and re-encode byte-identically. The existing
`request_body_len` and `request_body_sha256` still cover the canonical rendered body,
so cache-dump and prefix/cache identity are unchanged. `websocket_request` separately
records the length and SHA-256 of the exact UTF-8 request payload before compression;
`websocket_close_code` records a received server close code. Response digests cover
HTTP body bytes or socket text payloads before adaptation, not WebSocket framing or
ping/pong payloads. The mid-stream WARN identifies transport and numeric close code;
it never logs server close reasons or credentials. Socket errors omit backend body text,
and unknown backend error-code strings are omitted from diagnostics while their class
and retry decisions are retained. Header values are marked sensitive; library IO runs
with payload/handshake tracing disabled, including at trace level. The shared Responses
normalizer has a diagnostics-only privacy switch for sockets; HTTPS keeps its existing
behavior and all model-output decoding is unchanged.

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
