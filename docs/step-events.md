# Live model-step hints

`step_events_enabled` in `broca.jsonc` is a boolean, **false by default**, read at
daemon startup. When enabled, `role.describe` advertises the llm-runner `STEP_EVENTS`
capability with the `per_attempt` / `step_ref` policy. Leave it off until the spend
ledger consuming these hints can decode the emitted bodies.

The subject is `ck.{account}.live.broca.step.finished.v1`. The account comes from
broca's daemon-attested ck-bus credential, never the caller. Hints share broca's
broker connection and credential renewal with run events, but use plain NATS
publication: no JetStream acknowledgement, outbox, replay or retry.

Both storage paths use the same content-free `StepFinished` builder. `runner` is
`broca`; `harness` and `session` come from the bind; `attempt_id` names that
storage's durable attempt. `harness_step_ref` would name the caller's own step;
the bind carries no such reference, so that optional field stays absent. Each storage keeps
its own log incarnation and sequence. The new-storage writer fills its existing
optional attempt-id field when recording a model attempt; automatic keep-warms
record an id and observed completion metadata in the existing warm record.

Today's WAL observer is installed alongside, not instead of, transcript fanout.
The new-storage observer buffers only attempt metadata until the append's
durability barrier. A completed model attempt receives the usage its durable
step-end records; an interrupted attempt carries no usage. Warm request sizes
are included only when measured by the sender. Unknown measurements stay absent.
New-storage model attempts do not record physical request size, so that optional
field is omitted rather than filled with canonical uncompressed bytes. An attempt
without a recorded send time cannot satisfy the event schema's required start
time and produces no hint; its durable record remains available to billing.

Publication is best-effort. Queues are bounded and never wait on a session writer.
Account discovery and publication have a two-second deadline on the independent
publisher task; a failure drops the hint and logs at most once per minute.
Neither an outage nor a slow consumer changes a run's result or durable billing.
Opening a session does not replay predecessor attempts to repair missed live
hints. Durable billing remains authoritative, including after a crash.
