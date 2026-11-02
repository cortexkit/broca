# Raw quota observation fixtures

The four `broca-codex-*-request.json` / `*-reply.json` files are unchanged copies
of insula's `usage-observe-v1` vectors from commit
`5c728929ae7f4e0c73e8a9563ce63644323f6937`. Insula generated them with its real
usage.observe decoder and encoder. Identities are synthetic. Replies include the
management transport's `result` envelope. Non-default header families are forwarded
verbatim; only insula interprets quota values.

`codex-quota-capture-2026-11-02.json` is the openai-auth project's real scrubbed capture of HTTP
429, HTTP 200 and websocket quota readings. It contains no bearer or turn-state
token. Tests inject a synthetic turn-state token to prove it cannot be forwarded.
