# Runaway tool-argument whitespace

`tool_args_trailing_garbage` means broca stopped a provider attempt after a complete JSON
argument value was followed by more than 4096 bytes of JSON whitespace. The step is complete
and validated; this is not a provider failure or a transport retry. The malformed call keeps
its parsed input, receives an existing `ToolResult` error, and is never dispatched. Other
incomplete calls are omitted and their count is included in that error. The next model step
can re-issue the call with properly escaped arguments.

The shared guard lives beside `decode_terminal_tool_arguments` in `broca-provider/src/wire.rs`.
Each delta updates its buffer's suffix count using only the new bytes; it does not rescan or
reparse the growing buffer. A strict prefix parse happens only at the first threshold crossing
of a whitespace run. An unterminated string cannot pass that parse. Authoritative final
buffers also pass the terminal backstop. Anthropic Messages, OpenAI Chat, OpenAI Responses
(including ChatGPT on HTTPS and WebSocket), and Bedrock Converse use this guard. Gemini and
Google Code Assist receive structured argument values, not JSON argument-string buffers.

No WAL record type or field was added. `ModelAttemptFinished.outcome` gained the open-string
value `tool_args_trailing_garbage`. The existing step, batch and result records carry the
transcript. Step/batch/refusal records are enqueued together and occupy one shutdown-hook
fallback slot. A crash during the write can preserve the step record but lose the later
batch/error records. Current-build resume
reads the existing attempt outcome separately from replay, and rejects every call on a cut
step that lacks both an intent and a result. Its byte counts are reported as unavailable;
already-recorded results keep exact counts. Calls with an intent but no result keep the
existing unknown-outcome/Interrupted rule and are never re-dispatched. Replay state and
released-reader behavior are unchanged. A WARN contains session, step, tool name, code and
byte counts only.
Usage not yet reported by the provider remains unknown rather than being estimated.

## Attempt-outcome readers

The following inventory includes semantic readers, whole-record consumers and generic WAL
scanners. No existing reader needed a change to accept the new outcome spelling:

| Reader | Handling |
| --- | --- |
| `broca-protocol/src/provider.rs` | Open-string decoding preserves unrecognized spellings as `Unknown`; current readers recognize the new variant. |
| `broca-core/src/run.rs` | Attempt records are recorded independently of steps. Every non-`Complete` outcome is synced immediately, including a local cut. Resume separately reads cut-attempt evidence to refuse unresolved no-intent calls, without changing replay. |
| `broca-core/src/replay.rs` | Skips attempt evidence entirely; step and tool terminals drive replay/resume. |
| `broca-core/src/projection.rs` | Skips attempts in control/store projection, including the state used by `run.status`. Tool-dispatch outcomes are a different enum. |
| `broca-core/src/context.rs` | Reads attempt timestamps only. `session.read` transcript reconstruction uses replay. |
| `broca-core/src/billing.rs` | Skips attempts. Billing/export facts use the committed step's usage, not attempt outcomes. |
| `broca-core/src/admission.rs` | Ignores attempt evidence while folding admission state. |
| `broca-wal/src/record.rs` | Classifies attempts as informational; fields still use open-string outcome decoding. |
| `broca-module/src/actor.rs` | Counts attempts since the latest step boundary for shutdown evidence, independent of outcome. |
| `broca-module-serve/examples/reasoning-audit.rs` | Selects attempt records by step; does not inspect their outcomes. |
| `broca-module-serve/examples/sse-idle-capture.rs` | Copies/serializes the outcome in diagnostic capture data; unrecognized spellings remain valid. |
| `broca-core/src/import.rs` | Ignores attempt evidence when repairing imported conversation records. |
| `broca-core/src/compaction.rs` | Reads attempt timestamp and request length only. |
| `broca-core/src/cache_warm.rs` | A current reader accepts the new variant when HTTP status proves cache reach. A released reader excludes `Unknown`, conservatively declining to count that attempt as a request that populated the cache; it does not affect replay/resume or steps. |
| `broca-module-serve/src/cache_dump.rs` | Prefers `Complete` response attempts, otherwise uses the last response attempt for a step with usage. A local cut is handled by that fallback, like an unknown outcome. |
| `broca-provider/src/transport.rs`, `transport_websocket.rs` | Produces the new outcome only with a validated cut completion. Stops body polling and closes a generating WebSocket instead of retrying or reusing it. Retry/fallback decisions branch on actual failures only. |
| `broca-core/src/shutdown_seal.rs` | Writes its own `SealedInFlight` evidence, not an interpretation of another attempt's outcome. |
| `scripts/live-runs.py` | Scans run/step transition tokens, not attempt outcomes. |
| `scripts/lib/archive_read.py` | Generic JSON/frame decoding; no outcome enum validation. |
| `scripts/wal-scan.py` | Searches caller-supplied byte patterns; no outcome semantics. |
| `scripts/step-timing.py` | Reads model-step records and derives run outcomes from run-finished/run-paused transitions, not attempt outcomes. |
| `scripts/audit-export-lane.py` | Audits run-index/export facts, not attempt outcomes. |

## Released-reader rollback checks

Rollback checks consume a staged released binary rather than building an old release from a tag in
this repository. This keeps compatibility checks usable after the public history snapshot drops prior tags.

