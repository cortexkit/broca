# Thalamus call facts exports

Synthetic OpenCode 2 and Pi exports with scrubbed paths, supplied by Thalamus.
Each has two calls, one pinned and one unpinned. The four original
files were checked against the publisher's SHA256SUMS before copying.

All four files are copied unchanged from the regenerated source folder
`.cortexkit/internal-docs/export-calls-1110-v2/`. Its `SHA256SUMS` file's SHA-256 is
`ef2669f71fd91cacc6111e4f78d00c175f61ab9ae73dc91d424fc4fbfbb060c1`.
Keys remain exactly as published, including the Pi session prefix; the importer
treats them as opaque.

`export.calls.json` is an object containing `calls`, an array in history order.
Every keyed export call has exactly one row, located by the source message's id
(`mid`) and its original `tool_call_id`. The row records its opaque `call_key`,
optional `call_carrier` (default `thalamus-harness`), optional `schema_pin`, and
optional `started_at`, `finished_at`, and `unanswered` facts. These carriers are
per call, not per host: a history may contain calls from before a runner move.
Without a calls file the import keeps its existing host-derived identities.
