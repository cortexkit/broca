# Module lifecycle and shutdown

Broca's run lifetime is separate from the `session.send` RPC: the reply acknowledges
durable admission, while the lineage actor owns execution and queued submissions.
Shutdown must therefore count actors' work, not just the daemon's forwarded RPCs.

## In-place daemon upgrades

On macOS and Linux, broca opts in to `live-upgrade/v1`. A `module.upgrading`
notice sets a monotonic reconnect deadline without draining broca. When the
old module connection closes, broca reconnects on the daemon's local socket
using the newest resume token from HELLO_ACK. The token stays in memory.
The new connection republishes broca's catalog and reviews the daemon's
settings through the usual settings gate. If the daemon changes whether it
manages settings, broca restarts so its normal startup can choose the source.
Consumer connections bind again using claims from the new registration.

Session actors, providers' model streams and stores stay up. Routes and
subscriptions on the old connection end. A subscriber can attach again at its
last durable cursor; a sender can repeat the same `send_id` to retrieve the
original admission. Consumer tools reopen routes on use, catalog watches
resubscribe, broca resumes pulling late results from providers, and bus credential
requests open new routes as needed.

The daemon's `module_reloading` refusal with `detail.reason: "daemon_upgrade"`
proves that a request did not reach its provider, so broca can reopen its route
and retry within the reconnect deadline and the request's own budget. In
particular, a pre-tool hook whose declared budget expires during the switch
still fails closed. A tool call already dispatched when its connection was cut
is not sent again. The run continues with `outcome_unknown` (the tool may have
run); a late result cannot replace the terminal already recorded for the call.
A closure during the switch is not evidence that the run's scope has ended.

A local stop or terminal resume refusal stops new work and seals any remaining
runs before exit. If the reconnect deadline passes instead, the daemon kills
broca. Broca stops starting work and attempts to seal, but the kill can arrive
before the seal finishes or is logged. The replacement recovers the durable
session records as it would after any crash, without repeating a dispatched
call whose outcome is unknown.

Losing the connection without a preceding notice still stops broca. Broca
advertises the capability in its first HELLO; if the upgrade interrupts cold
start before broca is ready, broca logs the interruption and lets the supervisor
start it again. There are no live runs to preserve yet. Windows does not opt in.

## Accepting restart drains

Broca declares `accepting-drain/v1`. A daemon that also advertises it in
`HELLO_ACK.subc_capabilities` can restart or reload broca in two phases. It first
sends `module.draining` with `accepting: true` and an absolute Unix-millisecond
`deadline_ms`. It keeps admitting requests and does not consult the busy gauges.

During this phase, reads, subscriptions and steers are served normally. A send
that would start a run is durably queued before its `pending` reply, even on an
idle session, and runs in the replacement process. At the start of the drain,
broca asks already-dispatched yield-capable calls to hand back on their own routes
(`tool.yield`, reason `runner_restart`). Broca records held replies and pauses those
runs `tool_held`; the replacement resumes the same runs when the providers' late
results arrive. Streaming model steps and other dispatched tool calls still finish
and record their results within the drain budget. Broca starts no new model step
or tool batch. At the next boundary, an unfinished `on_restart: "resume"`
run records `Pause{reason: restart}`; other unfinished runs end `interrupted`.
A natural ending or a caller's cancellation still wins. An unanswered tool turn
in a restart pause dispatches after restart, not during the drain. A run parked
on a provider-held call stays parked. Sessionless model calls continue to be
served and broca waits for those in flight.

When the work has finished, broca sends one channel-0 `module.drain_ready`
request and waits for its echo. After the echo, refusal or timeout, or if the
drain deadline arrives first, broca stops admission. The daemon closes admission
on readiness or expiry, allows up to 5 s for already-forwarded requests, then
sends GOODBYEs. There is no second drain push. Broca then uses the same final
seal budget as for an ordinary drain. The daemon controls the accepting window;
setting a 600 s drain budget gives a restart up to ten minutes before teardown.

## Ordinary drains and final seal

Older daemons, module stop, daemon shutdown and `restart --now` use the ordinary
path. A daemon shutdown can also take over an accepting drain this way.

1. The daemon closes route admission and sends a channel-0 `PUSH` with
   `{"op":"module.draining","reason":"restart","deadline_ms":...}` over the
   module's registered connection. Broca calls `LineageRegistry::begin_stop`:
   new runs are refused, durable queued work stays queued, and active runs finish
   their current step. Every request refused because broca is stopping (a send, or any
   operation that would open a session) is answered
   `{"code":"transient","message":…,"detail":{"class":"transient","cause":"module_stopping"}}`:
   the request did nothing, so repeating it after the restart may be answered. The cause says
   nothing about an earlier request with the same `send_id`; a keyed resend is answered from the
   record once broca serves again. It keeps serving health probes and receiving tool results.
   Yieldable tool calls are asked immediately to hand back through `tool.yield` on their
   own routes. A held final frame parks its run `tool_held`; shutdown leaves that obligation
   open for the late result after restart, rather than writing an Interrupted terminal.
2. At the next clean step-entry boundary, `on_restart: "resume"` runs durably
   write a `Pause{reason: restart}` record; other runs end `interrupted`. No
   cancellation or final seal is triggered by the drain notice. The wall-clock
   `deadline_ms` is logged, not used to start a timer: it is the daemon's drain
   ceiling, not broca's post-GOODBYE budget.
3. The daemon probes `health.check`. Broca's manifest `self_signals` entry named
   `runs_in_flight` has `kind: "busy"` and
   `anchored_to: {"health_gauges":{"gauges":["runs_in_flight","opening","closing","system_one_batches_in_flight"]}}`.
   These are existing registry counters in `metrics`, not estimates read from records.
   Opening actors may still be admitting work; closing actors may still be
   syncing/joining their writers. Idle open actors and paused runs are **not**
   busy. A restart-paused actor stays open but clears `runs_in_flight`.
   `system_one_batches_in_flight` counts running `classify.run` calls, which hold no
   session; the post-GOODBYE grace waits for them as it waits for runs, and never
   longer, so a batch never extends the shutdown budget.
4. Once forwarded requests and all declared gauges are zero, or the drain ceiling
   expires, the daemon sends channel-0 GOODBYE. The serve loop returns and
   `stop_and_seal` starts its unchanged **10 s grace / 20 s total** budget. Actors
   still driving work after grace are sealed concurrently; unresolved tool calls
   record `outcome_unknown` and `Interrupted`, never a resumable restart pause.
5. A replacement resumes durable restart pauses through ordinary admission checks
   with the same run/send IDs and reconstructed provider request bytes. Queued
   work is recovered after the unfinished episode. See
   [graceful restart policy](module-surface.md#graceful-restart-policy).

The default drain offers up to **30 s drain + 10 s grace** to reach a
boundary. The drain can end early when idle; `--drain-ms` and `--now` can shorten
it. SIGTERM or connection loss without a drain notice offers only the 10 s grace.
The 20 s final budget includes grace and teardown, not 20 s in addition to grace.
Repeated drain notices are idempotent and do not escalate to a seal.

## A drain is not cancellable

The current daemon has no module drain-abort command or admission-resume notice.
Once it sends `module.draining`, it proceeds to teardown, not back to serving.
Broca does not reopen admission once it stops, even if GOODBYE is delayed or
never arrives. A transport failure runs the same final seal path; an
operator must replace a module whose daemon cannot complete its drain.

Daemon source references (paths relative to the subconscious checkout):

- `crates/subc-protocol/src/session.rs`: defines the ordinary and accepting
  `module.draining` pushes and `module.drain_ready`, but no abort/resume command.
- `crates/subc-daemon/src/supervise.rs:8235-8281`: serializes that command as
  `FrameType::Push`, channel/epoch/corr 0, and sends through `target.sink` (the
  registered module connection).
- `crates/subc-daemon/src/supervise.rs:8547-8584,8605-8614,8664-8686`: closes
  admission, sends draining **before** waiting for quiescence, then sends GOODBYE;
  there is no cancellation branch. A restart failing after initiation is marked
  `Failed`, not reverted to `Running` (`supervise.rs:6420-6439`).
- `crates/subc-protocol/src/manifest.rs:357-404,472-484`: declares a busy signal in
  `self_signals` with `kind: busy` and `SignalAnchor::HealthGauges { gauges }`.
- `crates/subc-daemon/src/supervise.rs:8061-8087`: reads gauge names from the
  registered manifest's busy self-signals. `supervise.rs:8015-8033` reads each
  non-negative integer directly from the `health.check` response's `metrics` and
  sums them; omitted/invalid values remain busy. `supervise.rs:8107-8147` probes
  during drain and returns early when the sum and forwarded request count are zero.
- `crates/subc-daemon/src/supervise.rs:79`: default drain ceiling is 30 s.

These references describe the inspected daemon implementation, not a new broca
wire protocol or a promise to reverse a drain locally.
