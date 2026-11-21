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

No record type or field was added. An attempt's outcome gained the open-string value
`tool_args_trailing_garbage`. The existing step, batch and result records carry the
transcript. Calls with an intent but no result keep the unknown-outcome/Interrupted rule and
are never re-dispatched. A WARN contains session, step, tool name, code and byte counts only.
Usage not yet reported by the provider remains unknown rather than being estimated.

