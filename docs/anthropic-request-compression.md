# Anthropic request and response compression

broca compresses request bodies to Anthropic's first-party API the way Claude Code 2.1.302
does, and decodes the compressed responses that API sends back. Both live in the shared HTTP
transport (`crates/broca-provider/src/transport.rs` and `transport_request_gzip.rs`).

## Request gzip

A request body is gzip-encoded only when all of these hold:

- the request URL's host is exactly `api.anthropic.com`. A `providers.<id>.base_url` override,
  a relay, a proxy or a local mock has another host, and its requests are never compressed;
- the rendered body is at least 4,096 UTF-16 code units long (Claude Code counts the JSON
  string's `.length`, so non-ASCII text counts fewer units than UTF-8 bytes);
- `BROCA_ANTHROPIC_REQUEST_GZIP` is not `0`;
- no earlier compressed request in this process was refused (see the fallback below).

API-key and OAuth requests to that host both qualify, as in Claude Code.

Each send pads the body with `\n` and 0 to 256 random spaces and tabs, gzips it once at
level 6 and sets `content-encoding: gzip`. `content-length` is the compressed size. Every
attempt, retries included, pads and compresses again. Cache warms to the same host get the
same treatment.

The encoding exists only on the wire. Rendering, request dumps, `request_body_len` and
`request_body_sha256` in attempt records, the WAL and resume all see the uncompressed
rendered bytes.

### Fallback after a refusal

A response to a compressed request is treated as a refusal of the compression when its status
is 400, 403 or 415 and either:

- it has neither a `request-id` header starting with `req_` nor a `cf-ray` header, so
  something in front of Anthropic answered; or
- it is a 400 whose first 8 KiB say the request was not valid JSON: an Anthropic error message
  starting with `The request body is not valid JSON` or
  `Failed to parse request: could not parse request body as JSON`, a
  `{"reason":"bad_json","error":"bad json: …"}` object, or plain text starting with
  `bad json: invalid character ` or reading `bad json: unexpected EOF`.

An ordinary API 400 that carries a `req_` request id passes through unchanged.

On a refusal the transport:

1. turns request gzip off for `api.anthropic.com` for the rest of the process, and logs that
   once at WARN with the status and the reason (`non_anthropic_rejection` or
   `invalid_json_at_origin`);
2. drops the refused response, which leaves no trace in the attempt record;
3. sends the same request once more, immediately, with the unpadded identity body and the same
   headers minus `content-encoding`. That re-send belongs to the same attempt: it does not use
   one of the step's retry attempts. A network error on the compressed send is an ordinary
   attempt failure and does not turn gzip off.

### Kill switch

```sh
BROCA_ANTHROPIC_REQUEST_GZIP=0
```

Read when the provider HTTP client is built, which for the module is at startup. Only the value
`0` disables request gzip; response decoding stays on.

## Response decoding

Requests to `api.anthropic.com` send Claude Code's `accept-encoding: gzip, deflate, br, zstd`,
and Anthropic then compresses its SSE stream. The provider HTTP client holds two reqwest
clients: the one for `api.anthropic.com` decodes those four encodings as the stream arrives;
the one for every other host decodes nothing, so its requests carry no `accept-encoding`, as
before.

The stream normalizer, the stall deadlines and the display lane all read the decoded bytes, so
they behave as they do for an uncompressed answer. For `api.anthropic.com`,
`response_body_len` and `response_body_sha256` in attempt records, and the response size
limits, now cover the decoded body; the compressed size is not recorded. For every other host
they are still the bytes on the wire, which are also the decoded bytes there.
