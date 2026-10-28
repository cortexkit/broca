# Synthetic Anthropic refusal and fallback streams

These fixtures preserve the event sequence and JSON key order of three captured
Anthropic server-sent event (SSE) streams, including block types, usage counters,
iteration entries, and the complete
fallback handoff block. Conversation text, thinking, signatures, tool argument string and numeric
values, refusal explanations, and message/tool identifiers are neutral placeholders.
Each replaced string has the original character length; tool JSON chunk boundaries
are preserved. The original conversation and regeneration script are private and are
not distributed with these fixtures.

| Fixture | Events | Blocks | Finish |
| --- | ---: | --- | --- |
| `l2.sse` | 221 | thinking | refusal |
| `l3.sse` | 5261 | thinking, fallback, thinking, tool_use | tool_use |
| `l4.sse` | 2178 | thinking, tool_use | tool_use |

`cargo test --locked -p broca-module-serve --test content_filter_capture` normalizes
all three fixtures through the Anthropic family. L2 drives refusal resend through
the real run loop; L3 exercises write-ahead-log persistence and the `session.read`
and `session.history` methods; L4 checks next-request handoff rendering and reply
normalization.
The tests explicitly characterize two limitations rather than asserting they are
supported contracts: billing selects one model for the top-level last-iteration
usage despite retaining both iteration entries, and the stored handoff omits the
trigger so rendering cannot echo the complete original block.
