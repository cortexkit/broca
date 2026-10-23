# Stall witness

`ck-broca` opens `<state root>/logs/stall-YYYY-MM-DD.jsonl` once on daemon boot
(0600 on Unix), using the UTC startup date even after midnight. A named OS thread
(`broca-stall-watchdog`) samples about once per second. No tracing calls or
application locks are on its observation/output path; JSON goes through `write_all`
on that independent file handle. Filesystem stalls can still stall that witness;
write failures increment the `stall_watchdog.output_errors` health gauge. It does
not rotate or prune these small diagnostic files.

A 250 ms Tokio heartbeat stamps a monotonic clock and increments its counter.
Heartbeat age over 5 s, reader phase age over 5 s (excluding idle/awaiting frame),
or an active egress write/flush over 5 s emits a snapshot. All simultaneous triggers
are listed; the primary kind prioritizes heartbeat, reader, then egress. Repeats
are at most once per 10 s while stalled; recovery emits once immediately. Health
metrics retain both the last trigger time/kind and latest snapshot time/kind,
including recovery. Times used for ages are monotonic; health trigger times are
Unix milliseconds. The marker clock has a 36-bit millisecond horizon (over two
years of uptime).

The reader has an atomic phase, method code, route channel and entry time. There
are no payloads, session IDs, method strings or correlation IDs in markers.
Method codes: 0 unknown/not decoded, 1 route.bind, 2 health.check, 3 send, 4 read,
5 subscribe, 6 status, 7 cancel, 8 retract, 9 role.describe, 10 vault.audit,
11 import, 12 head, 13 baseline, 16 run.result, 17 warm, 18 resolve,
19 run.tool_calls, 255 other. Codes 14 and 15 named the removed `cap.install` and
`spend.delta` operations and are not reused. Phase `dispatch` also covers decoding and synchronous work
between the narrower await phases. Spawned request tasks cannot overwrite the
reader marker. A phase includes its synchronous logs: logger contention is
explicitly **not instrumented**, so a frozen phase is not proof its awaited
dependency is slow. No claims about DNS or logger mutex occupancy are made.

Egress `sent` counts admitted frames, `written` counts completed frame writes
(which may still be in `BufWriter`), and `flushed_frames` counts frames covered
by completed flushes. Occupancy is sent minus written, including an active frame
but excluding reservations waiting for queue space. Module-originated
`scope.describe` uses the same counters. Failed writes leave their admitted frames
outstanding; cancellation/error clears the active write timer. Runtime worker
busy durations and park counts are cumulative and Tokio may publish them only
after a worker leaves its poll; they are not live stack samples. Stable metrics
include workers, alive tasks and global queue depth. Blocking-pool metrics require
`tokio_unstable` and are explicitly unavailable rather than zero.

`broca_jobs` observes only broca-owned `spawn_blocking` calls, not the shared
blocking pool or dependency-internal jobs such as DNS. Counters are by operation
kind, with oldest queued/running ages sampled from 4096 fixed atomic slots.
When all 4096 slots are occupied, `age_tracking_overflow` increments; reported
ages then cover only jobs assigned a slot.
Completion includes panic unwinds; queued cancellations are counted separately.

## Hot-path cost

No added mutexes, payload copying or frame allocations. Each reader phase
transition uses one monotonic clock read, one packed atomic store, and an atomic
restore on leaving the phase (plus two Arc refcount operations). Each inbound
frame has a dispatch store and an awaiting-frame store; request frames add
authorization and permit transitions; control binds add ownership when stamped.
Each reader egress send adds a phase transition. Each outbound admitted frame
adds one sent-counter RMW. Each completed write adds one counter RMW and one
operation timestamp store; each batch flush adds start/end stores and a
flushed-counter load/store. The sender/receiver share one Arc for the connection,
not an allocation per frame. Blocking-job submission uses counter/index RMWs and
normally one slot CAS; worst-case overflow scans 4096 slots. Start/completion each
add one counter RMW and one slot store. Watchdog sampling allocates only when
emitting snapshots.
