# Cache-warm admission timing investigation

## Cause and change

The intermittent extra wait was **physical WAL `fsync`**, not a provider retry,
keep-warm backoff, or a slow fake HTTP server. No production delay was removed:
these durability barriers are required in production.

The source path after releasing the held tool is:

- `broca-core/src/run.rs::dispatch_batch`: record tool results and await
  `writer.sync()` before the next model request;
- `finish_run`: record the terminal and await another `writer.sync()`;
- `broca-core/src/wal_writer.rs::writer_loop`: acknowledge the barrier only after
  `sync_result` succeeds;
- `broca-wal/src/file_wal.rs`: `fsync` calls `file.sync_all()`.

All paths above are under `crates/`. The actor cannot become idle before run
completion. An idle runner can still have variable storage flush latency.

Temporary `std::time::Instant` stamps covered test/send/execute boundaries, tool
start/release, warm start/return, and server accept/body-read/reply-write. On an
instrumented slow run, tool release was at 0.962 s, the next request at 2.407 s,
and executor return at 3.140 s. HTTP accept/read/reply were within milliseconds.

`strace -f -ttt -T -y -e trace=fsync,fdatasync,clock_nanosleep,nanosleep`
then directly attributed the gaps (separate stamped runs):

| Boundary | Stamps (s since test start) | WAL fsync duration |
| --- | --- | --- |
| Tool release to next request | 1.271 → 2.299 | 1.025327 s |
| Admission send to executor start | 0.592 → 1.923 | 1.326869 s |
| Final response to executor return | 2.333 → 2.594 | 0.259352 s |

Each syscall named the fixture's `codex-admission-…/wal/….wal`. The automatic
scheduler's fixture interval is 100 ms; the old 650 ms sleeps were observation
windows, not the intermittent multi-second wait.

`cache_warm_admission.rs` now wraps the file WAL in a **test-only non-fsync
adapter**. It retains actual encoding, append ordering, lineage identity, replay,
and complete range reads; it does not claim to test crash durability. The core
writer and WAL suites retain that responsibility. No production file changed.

The two 650 ms observation sleeps were removed. The partial-prefix test waits
for the scheduler's actual `warm_missed` stop event, scoped to its executor's
future. Positive cadence assertions wait for completed warm ticks: the executor's
pricing callback runs after the automatic driver's WAL barrier, immediately
before returning the tick. Cancellation also waits for two completed warms before
installing its connection gate. The common idle helper yields to actor completion
instead of sleeping between status requests. **The four-second guard is unchanged**;
it is a hang guard, not an assertion about time spent idle or a quiet observation
window. EOF, unsupported-family, explicit-warm and cancellation tests continue to
exercise real loopback TCP.

After the change, tracing **all nine tests** with
`strace -f -ttt -T -y -e trace=fsync,fdatasync` found **zero** such calls; all nine
passed in 0.84 s under strace.

## Telemetry comparison

The requested uninstrumented 20-run comparison at `8b0a28d8` (before merge
`9dd8dff8`) is below. A further 20 syscall-traced runs at that revision also
showed slow WAL fsyncs: 0.927932 s, 0.690757 s, 1.152084 s, and a run with both
0.655262 s and 1.046469 s flushes. Thus this storage-latency mechanism predates
the telemetry merge; the small samples do not establish identical latency
distributions. The merge did not change this test, `keep_warm_wait.rs`, or the
WAL fsync implementation.

For the extra archived-source comparison, an initial attempt reused a shared
Cargo target directory and failed with a stale cross-revision `content_progress`
type mismatch. Rebuilding the complete archive with an isolated
`CARGO_TARGET_DIR` succeeded. The 20 original uninstrumented comparison runs used
the worktree switched cleanly to `8b0a28d8`, not the failed archive build.

## Measurements

Linux `ck-motor`, `runon: linux,4c`, `CARGO_BUILD_JOBS=4`;
`rustc 1.99.0 (b940084d7 2026-09-28)`,
`cargo 1.99.0 (5f94df478 2026-08-27)`.

Each sample launches the already-built integration-test executable in a fresh
process, selecting only
`codex_partial_head_miss_stops_auto_warms_but_explicit_warm_remains_available`
with `--exact --nocapture`. Values are process wall time measured by Python
`time.monotonic()`, excluding compilation/Cargo startup. Every sample passed.
No measurements from stamped or syscall-traced runs are mixed into these tables.

| Revision | Runs | Minimum | Median | Maximum | Sum |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before, `8eb9487` | 30 | 0.975 s | 0.985 s | 3.365 s | 34.045 s |
| Before telemetry, `8b0a28d8` | 20 | 0.983 s | 1.027 s | 1.718 s | 22.541 s |
| After test-fixture fix | 30 | 0.446 s | 0.469 s | 0.520 s | 14.092 s |

Raw seconds, in run order:

```text
8eb9487 (30):
0.984 0.979 0.986 2.271 0.989 1.575 1.014 3.365 0.982 0.985
0.978 0.975 1.205 0.984 0.982 0.982 0.987 0.989 0.985 0.985
0.984 0.985 0.986 0.985 0.983 0.986 0.980 0.987 0.989 0.996

8b0a28d8 (20):
1.607 1.014 1.002 0.992 0.994 0.993 0.998 1.110 1.232 1.040
1.172 0.991 1.228 1.144 0.983 1.260 0.983 1.094 0.986 1.718

After (30):
0.487 0.463 0.446 0.455 0.520 0.476 0.479 0.463 0.467 0.481
0.478 0.478 0.467 0.467 0.473 0.465 0.477 0.470 0.472 0.476
0.472 0.469 0.476 0.467 0.467 0.451 0.454 0.463 0.468 0.446
```

To repeat a measurement at the checked-out revision (set `N=20` for the older
comparison), run on the same 4-vCPU Linux runner:

```sh
CARGO_BUILD_JOBS=4 N=30 python3 - <<'PY'
import json, os, statistics, subprocess, time
cmd = ['cargo', 'test', '--locked', '-p', 'broca-module-serve', '--features',
       'test-clock', '--test', 'cache_warm_admission', '--no-run',
       '--message-format=json']
build = subprocess.run(cmd, capture_output=True, text=True, check=True)
artifacts = [json.loads(line) for line in build.stdout.splitlines()
             if line.startswith('{')]
exe = next(a['executable'] for a in artifacts if a.get('executable')
           and a.get('target', {}).get('name') == 'cache_warm_admission')
name = 'codex_partial_head_miss_stops_auto_warms_but_explicit_warm_remains_available'
runs = []
for i in range(int(os.environ['N'])):
    start = time.monotonic()
    subprocess.run([exe, name, '--exact', '--nocapture'], check=True)
    runs.append(time.monotonic() - start)
    print(i + 1, f'{runs[-1]:.3f}s', flush=True)
print(len(runs), min(runs), statistics.median(runs), max(runs), sum(runs))
PY
```

## Non-vacuity

With the live test staged, the partial-miss predicate in
`crates/broca-core/src/keep_warm_wait.rs` was temporarily replaced by `false`
(marked `NON-VACUITY BREAK`). Running the whole `cache_warm_admission` target
produced **eight passes and exactly one failure**:
`codex_partial_head_miss_stops_auto_warms_but_explicit_warm_remains_available`,
with `partial prefix miss must stop the automatic scheduler: Elapsed(())`.
The other eight tests, including prefix-hit cadence and cancellation, stayed
green. The core file was restored from the staged live state and touched;
`git diff --stat` was nonempty during the mutation and empty after restoration.
