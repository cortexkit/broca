# `diagnostics.findings` v1

Broca serves this read-only management query for head routes and authorized flow
routes. The operation catalog declares it a query; the manifest's
`flow_operations` contains exactly `diagnostics.findings`. A flow's identity comes
from the daemon's route stamp, never from request parameters.

Head routes must be stamped as `head`, have no agent-run or flow id, and be
opened by their stamped owner. This includes `direct` only when `direct` owns
the stamped head. Worker routes, unscoped callers, foreign-owner direct routes,
and flows without an owner-authorized stamp are refused before evidence reads.
Flow callers must have an attested reserved-module principal; direct and
unverified principals cannot borrow flow admission. An authorized flow stamp
may also carry owner agent/run attributes; its flow id still selects flow
admission rather than owner-agent identity.
Existing channel/epoch checks reject stale route generations.
On a flow-scoped management route every other operation, including
`health.check`, `session.send`, classifier and permissions management operations,
returns `flow_scope_not_supported` naming the operation before any session or
diagnostic evidence read. Flow management binds therefore skip session-history
adoption checks. Actual tool-provider routes retain their existing scoped model
tool admission; a management role declaration cannot bypass the management gate.

```json
{"method":"diagnostics.findings","params":{"window_ms":21600000}}
```

Omitted or null params, and omitted or null `window_ms`, select 21,600,000 ms.
The inclusive range is 1–604,800,000 ms. Other types, out-of-range values and
unknown params are errors; windows are never clamped.

Success is a frame body with `result.generated_at_ms`, `result.window_ms`, and
`result.findings`. Each finding has `check`, stable `key`, `severity` (`warn` or
`error`), positive `count`, `first_seen_ms`, `last_seen_ms`, a sentence `summary`,
up to five object `samples`, and an investigation `hint`. Samples contain only
identifiers, codes and times, never prompts, arguments, tool output or provider
message text. Counts do not shrink when samples or findings are trimmed.

The entire compact response body, including `result`, stays strictly below
65,536 bytes. Findings sort by severity, descending count, check and key. Whole
findings are retained until the next would reach the cap; the remaining tail is
omitted. Summaries are limited to 400 Unicode characters. A bounded reply is not
a complete inventory or a pagination cursor.

## Checks and coverage

| Check | Source and rule |
| --- | --- |
| `paused_runs` | Current indexed authentication/account pauses, independent of the window, grouped by provider and frozen credential. Warn; error after 24 hours. Legacy rows without pause attribution remain visible as `unattributed`, not assumed to be authentication pauses. |
| `deletion_fenced` | Process map of retention refusals/deferments, keyed by session address and fence kind. Includes held intake, incomplete relocation and overhaul deferral. Warn; cleared only after deletion completion. Rebuilds as retention sweeps observe sessions after boot. |
| `late_results_rebuild_required` | State-root `late-results-rebuild-required` marker. Error; observation never clears or repairs it. |
| `catalog_stale` | Cached catalog age beyond the 30-minute poll cadence, or timestamped refused/failed polls in the window. Age warns after 30 minutes and errors after six hours; historical poll refusals warn separately with their observed count. |
| `credential_metadata_missing` | Account-selection health's current `unmapped_credentials` count. Warn. These are listed model credentials that cannot be offered to routing; this does not claim they were actually used by a routed run. |
| `send_refusals` | Timestamped routing-refused send admissions grouped by routing code. Warn. Unreachable routing is `routing_unavailable`. Invalid/non-token codes are `unattributed`. |
| `run_errors` | Indexed error terminals grouped by provider, shared error class and safe provider code. Floors below. Unattributed legacy/synthetic rows are retained, even below a floor. |
| `tool_outages` | Newly durable attributed unavailable tool results/no-dispatch records, grouped by module. Warn. Counts calls/results, not outage episodes. |
| `interrupted_runs` | Indexed interrupted terminals other than explicit `runner_shutdown`/`runner_crash` seals, plus `transform_unavailable`. Warn. Unknown causes are `unattributed`; restart membership is never inferred from a timestamp. |

`result.coverage` is additive. For each ring-backed check it gives `since_ms`
(process start, or the oldest retained event after eviction) and `truncated`.
Each ring-backed finding's first sample also carries `coverage_since_ms`.
Empty rings after boot do not create coverage findings or wake an operator.
Rings retain at most 20,000 events each and at most seven days of history.
They are process-local, not a durable incident ledger.

Run-error coverage reports the oldest attributed row in the bounded window
scan, or the report time when none was observed. Interruption coverage likewise
describes the rows examined. Terminal rows without observation times generate
an explicit `unknown_observation_time` finding instead of being silently placed
inside or outside the requested window. Samples count `rows_without_attribution`;
NULL metadata cannot distinguish an old row from a newer synthetic writer.

The six-hour error floors, calibrated from the operator's 2026-10-28 seven-day
production aggregate (1,359 error runs), are:

* Credential/billing failures (`auth_required`, billing/credit exhausted or
  insufficient, organization/project spend limits, excluded entitlements,
  `ENTITLEMENT_ERROR`, and rejected/invalid API keys): count 1, error.
* Other permanent errors: count 10, error (`request_invalid` p90 was 9, max 93).
* Context overflow: count 25, warn (observed maximum was 10).
* Transient/other errors: count 50, warn (the `inception` provider module's
  transient rate-limited errors had median 60 and p90 322 per six-hour window).

For other windows the non-credential floors scale linearly, round up, and never
fall below three. Credential/billing floors remain one.

## Read limits and failures

The handler opens an existing `run-index.db` read-only. It never opens actors,
takes session leases, reads session logs, refreshes the catalog or calls the
vault. Each of five indexed queries reads at most 20,001 scalar rows: 20,000
observations plus a limit sentinel. The standing live-row query uses the existing
`bus_event_live_runs` partial index; two new partial timestamp indexes cover
error and interrupted/transform-unavailable terminals. Separate NULL-time
queries use those same indexes. Timestamp ties use descending rowid (already
present in each index), without a separate sort. No query joins a transcript table.

Index samples cap run/provider ids at 256 characters, credential ids at 512,
classes at 64, codes and interruption reasons at 128. Ring module ids over 256
bytes and codes over 128 bytes are replaced by stable SHA-256 identifiers rather
than retained unbounded. Store failure or inability
to inspect the persistent rebuild fence returns an error frame. An unreadable
catalog, event ring or retention map returns a check-specific error-severity
`evidence_unavailable` finding. A truncated standing live-row scan likewise
reports that it could not finish the pause check. The retention map is capped
at 20,000 conditions; overflow is explicit unavailability, not a false clean read.

## Verification

Serving-generated request/reply fixtures live under
`crates/broca-module-serve/tests/vectors/diagnostics-findings-v1/`. Regenerate with
`UPDATE_BROCA_DIAGNOSTICS_FINDINGS_VECTORS=1 cargo test --locked -p broca-module-serve --lib diagnostics_findings_v1_vectors_match_serving_replies`.
The handler uses a seeded scratch SQLite store and fixed clocks; ordinary runs
byte-compare pretty JSON plus a trailing newline.

Native real-daemon lane (requires the staged `ck-subc` and `fake-aft-stub`):

```sh
SUBC_CORE_BIN="$PWD/.test-rigs/ck-subc" \
FAKE_AFT_STUB_BIN="$PWD/.test-rigs/fake-aft-stub" \
BROCA_REQUIRE_SUBC=1 cargo test --locked -p broca-module-serve --test it \
  approval_gate::diagnostics_findings_uses_authenticated_head_owner_route -- --exact --nocapture
```

The portable selection already excludes the entire `approval_gate::` group;
this test is therefore reserved for native daemon verification, not a passing
portable skip.
