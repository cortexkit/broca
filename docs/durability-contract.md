# Durability contract

What broca guarantees about a run across a crash, a restart or a stop, and how each guarantee is
kept. The session store's layout, record kinds, encryption and sealed history are in
[the session store](session-store.md); this page states the rules the run loop
(`broca-session-loop`) follows on top of it.

## 1. The invariant: a tool call is sent at most once

A side-effecting tool can have started (its edit, command or paid API call applied) without its
result reaching disk. Re-running such a call after a crash would repeat the side effect, so broca
never does: a call whose intent is durable and whose outcome is not is **indeterminate**, and an
indeterminate call is never dispatched again.

While broca is running, a call with no definitive answer gets a durable `outcome_unknown`
result. The model sees why broca stopped waiting and that the call may have run, so it can
check before trying again. Broca does not retry the call; only the model can request a new
call under a new key.

A model step writes nothing that a later request reads until its answer has ended, so sending an
unfinished step again is safe. Only the tool leg needs a barrier.

## 2. Barriers

A barrier is a sync of the session's open log before broca acts outside the process:

1. The run's start (`RunStart`, holding everything admission froze) is synced before the first
   model call.
2. A tool batch's start and a `ToolIntent` for every call that may leave the process are synced, in
   one barrier, before any call is dispatched.
3. Each call's outcome is synced as it is recorded, while the batch's other calls may still run.
4. The batch's end, after one outcome for every planned call, is synced before the next model
   request.
5. A step that calls no tool is synced at its end.
6. A run that stops with a batch still open writes the missing outcomes, the batch's end and the
   run's end in one append.

## 3. Dispatch outcomes

Before dispatch, each call is planned as dispatchable or refused (denied, invalid, unavailable). A
refused call gets an error result and is never dispatched. A dispatched call ends as one of:

| Outcome | Recorded result | Sent again | Run |
|---|---|---|---|
| Completed | the tool's result | no | continues |
| Not sent: the call never left the process | an error result naming why | no | continues |
| Sent, outcome unknown (including deadline or provider silence) | an `outcome_unknown` result naming why and saying the call may have run | never | continues |
| Run's scope already ended when its batch opened | every unsent call gets a `route_drained` result saying it was not run | no | ends `Interrupted` after settling the batch |
| Scoped route closed before the call was sent | a `route_drained` error result saying it was not run | no | continues unless the run's scope ended |
| Scoped route closed after the call was sent | an `outcome_unknown` result saying the route closed and the call may have run | never | continues unless the run's scope ended |

A scoped route is one the daemon opened under a scope (an agent's identity and epoch); it closes
when that scope ends. A call whose route closed before it was sent was definitely not run, so, as
llm-runner's contract has it, it is refused like a denied call: `run.tool_calls` lists it as
`refused`. Losing a tool route does not prove the run's scope ended. If the daemon confirms the
run's scope or its parent ended, broca settles the batch, records the scope reason on the
`Interrupted` terminal, and makes no further model request. Caller cancellation, run stop
conditions, model request errors and runner or host shutdown also end a run independently of
tool results.

An unknown outcome settles only its call. Other calls can finish, dispatch in order, or remain
held until their providers answer.

A **provider-held** call is one whose provider confirmed it holds the call and will answer later
through a late-results feed. It is not indeterminate. The run pauses `tool_held` until the late
result arrives. In a sequential batch, the calls after a held call are not sent until its result is
recorded, and that result is synced before the next call is sent.

## 4. Resume

A run cut short by a crash, or stopped by a shutdown, resumes from its records; it is never
restarted from its input.

- **Frozen inputs.** The next request is rebuilt from the conversation the records replay to and
  the values admission froze: model and render configuration, tools, setup parts, cache policy and
  stop conditions. Resume does not resolve the model against today's catalog, so a changed or
  withdrawn catalog row cannot move an admitted run. Before calling the provider again, broca
  checks that the provider still renders with the recorded renderer version and configuration; if
  either drifted, the run ends rather than sending a request the original run would not have sent.
- **A finished answer.** If the last step ended with an answer that calls no tool, the run had
  ended on its own: resume writes its end without calling the model again.
- **An unfinished step** is sent again from the same request.
- **Indeterminate calls left by a stop or crash.** When no result was recorded, recovery seals
  the run without making a model call. It closes the open batch: an
  `outcome_unknown` error result for each call that was sent with no recorded result, a not-run
  result for each planned call that was never sent, then the batch's end and `RunEnd`
  `Interrupted`, naming the calls in `indeterminate_tool_calls`. A crash during the close-out
  resumes into the same close-out, which writes no second result for a call that has one. When
  a run resumes after a live `outcome_unknown` result was recorded, it reuses the result and
  never re-dispatches the call. [Startup recovery](module-surface.md#14-runstatus-states-and-busy-vs-stale)
  still seals runs left in flight by the previous process.
- **Stopping with work pending.** A stop takes effect at a step boundary. When it closes an open
  batch, a provider-held call is closed and its provider is asked to withdraw it once the run's end
  is durable. A step limit reached before a batch is dispatched closes every call of that batch
  with a not-run result.
- **Finding the runs to resume.** `recovery-candidates/` lists the sessions that may need resuming
  at start. A session is added before its run can write its start and removed only after its end
  and its billing facts are durable, so a missing entry never hides a session that needs recovery.

## 5. One writer per session

Two mechanisms keep a session to one writer:

- **The lease.** Opening a session for writing takes an OS advisory lock under `leases/`, keyed by
  the session's `(project_root, harness, session)` triple, held for the session actor's lifetime and released by the OS if the
  process dies. A second process that tries to open the session is refused `LeaseHeld`.
- **The head.** A head write succeeds only if the stored head version still equals the version
  the writer read, and every log append names the committed head version and the log end it
  expects. A stale writer is refused (`Fenced`) and changes nothing.

## 6. What readers see

Readers and subscribers see a call's result as soon as it is recorded, before the batch ends, and
it is already synced then. So a result a reader saw is never lost to a crash, and never turns into
an unknown outcome on resume.

## 7. Tests that hold these rules

The session loop's cut tests (`crates/broca-session-loop/tests/differential.rs`) save the log at
every durable append, resume each prefix, and check that an intent in the prefix is never
dispatched again, that an unknown outcome is never retried, and that every resumed request equals
the uninterrupted run's.
