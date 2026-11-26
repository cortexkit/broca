# Runaway tool arguments

Broca ends a provider step with finish `length` if one tool call's arguments pass 256,000
bytes while still streaming, before they form a complete JSON value, or end in a run of at
least 4,096 whitespace characters. The guard checks each delta as it arrives and stops reading
the attempt; it is not a transport failure or an attempt retry.
The warning log includes the first and last 200 characters of the offending argument. Treat
those excerpts as untrusted model output that may contain request text.

The shared guard lives in `broca-provider/src/wire.rs`. Suffix counting inspects only the new
delta, not the growing argument buffer. An argument that is already complete is never cut for
size, whatever its length: one delivered whole in a single delta, a provider's final argument
event, and the structured arguments Gemini and Google Code Assist send in a single part. The
size cut exists to stop a model that keeps writing, and a complete value has stopped. Final
argument strings still pass the whitespace check.

An unfinished call gets a normal not-run result with reason `arguments_incomplete`; its
partial arguments aren't saved or sent to the model. Complete sibling calls can still run.
The same rule applies to unfinished calls when the provider itself reports the output ceiling.
The third consecutive length cut ends the run with a permanent `output_limit` error instead
of dispatching any of that step's calls. See the [module surface](module-surface.md).

The existing step, batch and result records carry these outcomes. Usage not yet reported by
the provider remains unknown. Calls with an intent but no result keep the unknown-outcome
rule and are never re-dispatched.
