# Response-envelope fixture provenance

The response-evidence tests reuse existing fixtures rather than claiming a new live capture.
Test names state their origin:

| Fixture or test input | Origin |
| --- | --- |
| `../openai_chat_cache_write_response.sse`, `../openai_chat_cache_read_response.sse` | **Real metered capture**, as documented by the OpenAI Chat conformance tests. Response bodies only; no status line or headers were retained. |
| `../../golden/usage_normalization.json` and the streams assembled from it | **Synthetic** protocol vectors for Anthropic, Chat (including DeepSeek, Kimi and xAI accounting), Responses, ChatGPT/Codex, Gemini, Code Assist and Bedrock. Bedrock's raw cache semantics remain unverified. |
| TTL splits, partial/disconnected responses, model iterations, HTTP error bodies, request-ID headers and allocation-marker inputs in the response-evidence tests | **Synthetic**. They exercise preservation and absence semantics, not observed vendor behavior. |
| `../content_filter/*.sse` (not used as response-evidence goldens) | **Neutralized** captures: identifiers and numeric values were replaced, as their README states. |
| `../../live/captures/*` (not used as response-evidence goldens) | **Synthetic**, as their README states. |

No synthetic header is evidence that a vendor sends that header. A body `id`, `responseId`,
or client-generated Code Assist `requestId` is never substituted for a response-header request ID.
Missing headers and counters remain absent, not zero or a requested/default value.

## Live-capture gaps

To close the recorded-response acceptance gap, each capture pack must retain the HTTP status
line, response headers and complete usage-bearing body together. Never include request auth
headers or credentials. Retain a refusal/error response as well as a successful response for
each family. In particular:

| Family | Successful responses and usage needed | Response headers to retain | Refusal/error needed |
| --- | --- | --- | --- |
| Anthropic Messages | Complete `message_start` / terminal `message_delta` / `message_stop`, including input/output, cache read, cache creation and both `cache_creation.ephemeral_5m_input_tokens` and `ephemeral_1h_input_tokens`; a cache-write response and a subsequent cache hit. Preserve `iterations`, tier and speed if reported. | Status line, `content-type`, `request-id` (and any proxy `x-request-id`), all reported tier/speed headers. | Non-2xx status, the same headers, full JSON error body and any reported usage. |
| OpenAI Chat / compatible providers | The existing real write/read pair needs its matching status lines and headers. Additional actual DeepSeek and xAI usage frames are needed to validate their different cache/reasoning policies, not merely the generic Chat grammar. Preserve final `choices: []` usage and `[DONE]`. | Status line, `content-type`, `x-request-id`, any reported tier headers. | Non-2xx status, headers and full error body, including usage if present. |
| OpenAI Responses | Actual terminal `response.completed` / `response.incomplete` with native `usage` and all token details; separate cache-write and cache-hit replies, reported model/tier/speed and reasoning if present. | Status line, `content-type`, `x-request-id`, any reported tier/speed headers. | Non-2xx JSON error or `response.failed`, with status/headers and usage if present. |
| ChatGPT/Codex Responses | Actual terminal Responses events on the ChatGPT route, cache write/read where reported; preserve the websocket upgrade status/headers when applicable, plus the terminal usage frame. | HTTP/upgrade status line, `content-type` where present, `x-request-id` if sent, all reported tier headers. | HTTP refusal or socket error event, original status/headers when available, any usage-bearing frame. |
| Gemini | Actual complete native response stream with `usageMetadata`, cached prompt and candidate/thought counts; a cache-populating request and subsequent cache hit (cache-write counters only if reported). | Status line, `content-type`, `x-goog-request-id` or `x-request-id` only if actually present, `x-gemini-service-tier` when reported. | Non-2xx status, headers, full Google error body and usage metadata if reported. |
| Google Code Assist | Actual wrapped `response.usageMetadata`, cache-populating and cache-hit responses, model version, thought counts, tier when reported. Client-generated body request IDs are not vendor request IDs. | Status line, `content-type`, any actual response request-ID header, `x-gemini-service-tier` if present. | Non-2xx status, headers and full wrapped/error response, any reported usage. |
| Bedrock ConverseStream | Actual complete event stream with `metadata.usage`, separate cache-populating and cache-hit responses and all native counters. Preserve `serviceTier` if reported. Do not infer model from the request when the response does not name it. | Status line, `content-type`, `x-amzn-requestid`, and `x-amzn-errortype` on errors. | Non-2xx response or event-stream exception, headers and complete payload, any usage metadata. |

An absent response-header request ID is a valid unknown result, not a reason to manufacture one.

Socket attempts retain a request ID only when an individual wrapped error supplies a recognized
response header; normal socket responses have no request ID here, and a connection-level upgrade
ID is never copied to requests on a reused socket. The wrapped-header tests are synthetic.

The in-band error tests cover a root-level `usage` beside `error` on Anthropic/Chat and
`usageMetadata` beside `error` on Gemini/Code Assist's decoded response, not a vendor promise
that native errors contain usage.
Responses lifecycle metadata is retained before completion; a created-event tier is provisional,
and a later reported terminal tier takes precedence. Envelope finish reasons require a reported
wire reason/status, not a locally synthesized canonical completion.

HTTP error evidence requires a complete readable JSON body; accumulated prefixes discarded by
an error/cap are not decoded. Evidence from cache-warming responses likewise requires the
complete body read to succeed.
No claim is made that every partial HTTP-error/warm body, nested error shape, or warm handoff
without tagged iterations has recoverable usage. The allocation guard measures five decoder
`push` paths, not wrapper/transport construction, `finish`, allocation bytes or CPU time.
