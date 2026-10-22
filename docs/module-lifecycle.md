# Module lifecycle and shutdown

Broca's run lifetime is separate from the `session.send` RPC: the reply acknowledges
durable admission, while the lineage actor owns execution and queued submissions.
Shutdown must therefore count actors' work, not just the daemon's forwarded RPCs.

## Drain first, final seal second

1. The daemon closes route admission and sends a channel-0 `PUSH` with
   `{"op":"module.draining","reason":"restart","deadline_ms":...}` over the
   module's registered connection. Broca calls `LineageRegistry::begin_stop`:
   new runs are refused, durable queued work stays queued, and active runs finish
   their current step. It keeps serving health probes and receiving tool results.
2. At the next clean step-entry boundary, `on_restart: "resume"` runs durably
   write `RunPaused{Restart}`; other runs write `RunFinished{Interrupted}`. No
   cancellation or final seal is triggered by the drain notice. The wall-clock
   `deadline_ms` is logged, not used to start a timer: it is the daemon's drain
   ceiling, not broca's post-GOODBYE budget.
3. The daemon probes `health.check`. Broca's manifest `self_signals` entry named
   `runs_in_flight` has `kind: "busy"` and
   `anchored_to: {"health_gauges":{"gauges":["runs_in_flight","opening","closing","system_one_batches_in_flight"]}}`.
   These are existing registry counters in `metrics`, not WAL-derived estimates.
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

The default module restart offers up to **30 s drain + 10 s grace** to reach a
boundary. The drain can end early when idle; `--drain-ms` and `--now` can shorten
it. SIGTERM or connection loss without a drain notice offers only the 10 s grace.
The 20 s final budget includes grace and teardown, not 20 s in addition to grace.
Repeated drain notices are idempotent and do not escalate to a seal.

## A drain is not cancellable

The current daemon has no module drain-abort command or admission-resume notice.
Once it sends `module.draining`, it proceeds to teardown, not back to serving.
Broca intentionally leaves admission stopped even if GOODBYE is delayed or never
arrives: reopening on a local deadline would admit work onto routes the daemon
still marks draining. A transport failure runs the same final seal path; an
operator must replace a module whose daemon cannot complete its drain.

Daemon source references (paths relative to the subconscious checkout):

- `crates/subc-protocol/src/session.rs:115-139`: the one-way command enum contains
  only `module.draining { reason, deadline_ms }`, no abort/resume command.
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
