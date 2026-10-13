# Framed two-lane wire contract

The body schema is defined in `crates/broca-protocol` (`BODY_SCHEMA_VERSION = 1`).
It nests inside the subc-protocol envelope.

## 1. Transport

ONE socket per runner↔provider pair: a direct, point-to-point,
HMAC-authenticated loopback-TCP connection via `subc-transport`
(`authenticate_*` handshake + `connection_file` discovery + `read_frame` /
`write_frame`). NOT relayed through subc-core. A priority-scheduled writer drains
control first and DROPS lossy display under backpressure (emitting a
`display_gap`), so the reliable lane never starves — control backpressures
upstream, display is shed.

## 2. Envelope reuse

The subc 17-byte envelope is reused as the framing codec. On this direct socket
(no subc routing), two routing fields are repurposed:

| envelope field | broca meaning |
|---|---|
| `flags` PRIORITY | **Lane** — control = `Interactive`, display = `Background` |
| `flags` LAST | final frame of a message/stream |
| `flags` BINARY | always 0 (body is JSON) |
| `channel` (u16) | **`stream_gen`** — dead-generation detectable w/o body parse |
| `corr` (u64) | step / approval round-trip correlation |
| `type` | always `StreamData`; finality is the LAST flag |

No subc-protocol change is needed — priority/channel/corr/LAST already exist and
`peek()` reads all of this WITHOUT deserializing the body.

## 3. Body

`BodyEnvelope<T>` = a fixed app-header + the JSON payload:

```json
{ "version": 1, "run_id": "...", "step_id": 0, "seq": 0, "payload": { "type": "...",... } }
```

- `seq` — per-lane monotonic frame counter (display-gap detection).
- `payload` — a lane- and direction-specific enum, internally tagged (`"type"`),
  so the wire is self-describing and easy for a weaker agent to parse.
- Display frames are COALESCED chunks (not one-token-one-frame), so JSON on the
  display lane is not a per-token cost.

## 4. The two payload schemas

**Display lane** (`DisplayEvent`, identical both hops, passthrough):
`text_delta` · `reasoning_delta` · `tool_input_delta` · `display_gap{dropped}` ·
`display_reset`. Each delta carries the `message_id`/`tool_call_id` linking it to
its authoritative control unit.

**Control lane — provider → runner** (`ProviderControl`, the ASSEMBLED units): `stream_start` · `step_started` · `assistant_message` (assembled;
tool-calls are its content blocks — no separate tool_call unit) · `step_finished`
· `finished` · `error`.

**Control lane — runner → caller** (`ControlUnit`, the OutputStream surface): `run_started` · `step_started` · `assistant_message` · `tool_call` ·
`tool_result` · `step_finished` · `approval_request` · `display_reset` · `error`
· `run_finished{reason}`.

**Approval back-channel** (caller → runner, control lane): `ApprovalResponse` =
`allow` | `allow_narrowed{input}` | `deny{reason}`, correlated by `tool_call_id`
(+ envelope `corr`). Approval may NARROW, never widen.

## 5. Supporting canonical types (frozen alongside)

- `content`: `Message`/`Role`, `ContentBlock` (`text` | `reasoning{signature?}` |
  `tool_call` | `tool_result`), `ToolCall`, `ToolResult`/`ToolOutput`
  (`text`|`json`|`denied`|`error`), `AssistantMessage`, `FinishReason`.
- `error`: `ErrorClass` (`transient`|`permanent`|`auth`|`context_overflow`|
  `provider_unavailable`) + `ProviderError{status, retry_after_secs,...}`.
- `usage`: `Usage` with `fill_tokens() = input + cached_input + cache_write` (
  excludes output/reasoning).
- `ids`: `RunId`, `StepId`, `ToolCallId`, `MessageId`, `StreamGen`.


## 7. Executable spec

`crates/broca-protocol/src/frame.rs` tests pin: lane↔priority round-trip,
encode→peek→decode fidelity, the envelope-field mapping, and the on-wire JSON
shape — so the contract cannot silently drift.
