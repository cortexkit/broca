# `diagnostics.findings` v1

Broca serves this read-only management query for head routes and authorized flow
routes. The operation catalog declares it a query; the manifest's
`flow_operations` includes `diagnostics.findings` alongside the existing
flow-callable management operations. A flow's identity comes
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
Existing management flow callers retain their original per-operation and
session/resource authorization, including Basal's flow-stamp requirement on
sends and the classifier management route. Adding diagnosis does not widen or
narrow another operation. Actual tool-provider routes also retain their existing
scoped model-tool admission. `flow_operations` is derived from the existing
namespace scope/caller gates over the declared catalog, not a second hand-kept
list. Stateful role, session and owner checks remain in their original positions;
discovery is not a resource grant. For example, `permissions.list` and
`permissions.revoke` still admit only Direct or reserved `cingulate`, not Basal.

```json
{"method":"diagnostics.findings","params":{"window_ms":21600000}}
```

Omitted or null params, and omitted or null `window_ms`, select 21,600,000 ms.
The inclusive range is 1–604,800,000 ms. Other types, out-of-range values and
unknown params are errors; windows are never clamped.

Success is a frame body with `result.generated_at_ms`, `result.window_ms`, and
`result.findings`. Each finding has `check`, stable `key`, `severity` (`warn` or
`error` or `info`), positive `count`, `first_seen_ms`, `last_seen_ms`, a sentence `summary`,
up to five object `samples`, and an investigation `hint`. Samples contain only
identifiers, codes and times, never prompts, arguments, tool output or provider
message text. Counts do not shrink when samples or findings are trimmed.

The entire compact response body, including `result`, stays strictly below
65,536 bytes. Findings sort by severity, descending count, check and key. Whole
findings are retained until the next would reach the cap; the remaining tail is
omitted, and `result.findings_omitted` gives how many findings were left out. It is
absent when the reply holds every finding. Summaries are limited to 400 Unicode
characters. A bounded reply is not a complete inventory or a pagination cursor.

## Checks and coverage

| Check | Source and rule |
| --- | --- |
| `paused_runs` | Runs whose newest outcome is a pause for sign-in (`auth_required`) or an account limit, independent of the window, grouped by provider and credential. Warn; error 24 hours after the pause. A pause from before broca started is reported once its session opens: the sign-in sweep at startup opens every session paused for sign-in. |
| `deletion_fenced` | Process map of retention refusals/deferments, keyed by session address and fence kind. Includes held intake, incomplete relocation and overhaul deferral. Warn; cleared only after deletion completion. Rebuilds as retention sweeps observe sessions after boot. |
| `late_results_rebuild_required` | State-root `late-results-rebuild-required` marker. Error; observation never clears or repairs it. |
| `catalog_stale` | Cached catalog age beyond the 30-minute poll cadence, or timestamped refused/failed polls in the window. Age warns after 30 minutes and errors after six hours; historical poll refusals warn separately with their observed count. |
| `credential_metadata_missing` | One warn finding naming each currently unmapped credential in the account chooser's scope. These credentials cannot be offered to routing; this does not claim they were actually used by a routed run. |
| `send_refusals` | Timestamped routing-refused send admissions grouped by routing code. Warn. Unreachable routing is `routing_unavailable`. Invalid/non-token codes are `unattributed`. |
| `session_address_unavailable` | Timestamped session-bind refusals grouped by inventory reason, including `wait_expired`, `migration_failed`, `gap_marker` and store errors. Error from the first refusal; no burst floor. Counts refused binds, not affected sessions or outage episodes. |
| `session_inventory_repairs` | Timestamped repair attempts grouped by `started`, `completed` and `failed`. Progress/completion is informational; failure is an error. Counts attempts, not sessions or census rows. |
| `run_errors` | Runs that ended `error` in the window, grouped by provider, shared error class and provider code. Floors below. An end that recorded no provider error groups with a null class and code. |
| `tool_outages` | Newly durable attributed unavailable tool results/no-dispatch records, grouped by module. Warn. Counts calls/results, not outage episodes. |
| `tool_result_json_dump` | Tool-result intake observations, grouped by provider module and tool name. Warn when model-visible text dumps raw JSON; the tool provider should shape its output. The summary names the tool, its module, and how many results and sessions it covers, and the hint names who to report it to, since digests print those two lines and not the key. The exact rule and bounded coverage are described below. |
| `interrupted_runs` | Runs that ended `interrupted` in the window, other than the `runner_shutdown`/`runner_crash` seals a restart writes, grouped by interruption reason (`unattributed` when the end recorded none). Warn; a run is not classified as restart-related based only on its timestamp. |

`result.coverage` is additive. For each ring-backed check it gives `since_ms`
(process start, or the oldest retained event after eviction) and `truncated`.
Each ring-backed finding's first sample also carries `coverage_since_ms`.
Empty rings after boot do not create coverage findings or wake an operator.
Rings retain at most 20,000 events each and at most seven days of history.
They are process-local, not a durable incident ledger.

`tool_result_json_dump` checks each newly received final tool result at intake,
after normalization, without changing its model-visible output or stored bytes.
Late (provider-held) results aren't counted yet.
A tool result is a raw JSON dump when all three hold:

1. Its tool isn't one of `bash`, `read`, `read_json`, `codemode`, `todowrite`, which
   return the caller's own data or JSON on purpose.
2. Its model-visible text (the text parts joined, images excluded), trimmed of
   surrounding whitespace, is at least 200 bytes.
3. The whole trimmed text parses as a JSON object or array, or as a JSON string
   whose content is one (JSON encoded twice). JSON quoted inside other text, such
   as an escaped literal in a code excerpt, doesn't count: search, grep and
   outline results quote source files.

The tool allowlist and byte floor are checked before attempting a JSON parse.
Each finding's key is a compact JSON pair `[module_id,tool_name]`; samples name
the provider module, tool, session, dispatch call key (the durable call identity,
null when unavailable), and trimmed text byte length (`text_bytes`), never the
text. Evidence retains at most 20,000
observations and seven days, groups counts and first/last times within the requested
window, and keeps up to five samples per tool. Sample trimming does not reduce
counts. Coverage reports eviction just like the other process-local rings. All
retained identifiers longer than 256 bytes are replaced by stable SHA-256 identifiers.

`health.check.metrics.session_address_refusals` exposes lifetime counters for
`incomplete_inventory`, `gap_marker`, `store_error`, `ambiguous` and `invalid_identity`,
including zeros. Only the three unavailable-inventory reasons create this error finding.
They also produce a one-line `session_address` WARN, at most once per reason per 60
seconds; warning throttling never drops counters or diagnostic events. Counters reset
on restart and findings age with the requested window, so neither is an assertion
that the inventory remains unavailable now. A failed diagnostics evidence read still
returns its existing explicit error rather than inventing healthy evidence.

The three run checks read run outcomes, one more process-local ring. Each
session reports its runs' outcomes there from its records once those records are
durable: an error or interrupted end, a pause for sign-in or an account
limit, and the resume or other end that clears such a pause. A completed run
records nothing, so the ring's 20,000 outcomes span far more runs. An outcome
names the run, its provider and its credential (the one an account move chose,
once there is one), the error's shared class and provider code, and the
interruption reason. `run_errors` and `interrupted_runs` coverage is the ring's.

Credential metadata scope reuses `account_routing::selects_accounts`: OAuth
(Claude-subscription) and ChatGPT methods, not API keys or Antigravity. Missing
or unknown methods remain in the metadata-check scope. The known non-model `cursor` refresh adapter is
excluded because its vault record is used only for quota, not a broca model
route. Missing, unknown and every other adapter remain in scope. Each finding
names the credential and missing fields, with a vault-side remedy: set provider
ids and a recognized method, or use a non-model type. The deploy guard mirrors
this scope; a serving-generated row fixture checks Rust/Python agreement.

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

The handler reads process memory and the state root's rebuild fence. It never
opens a database or an actor, takes session leases, reads session logs, refreshes
the catalog or calls the vault.

Ring module ids over 256 bytes and codes over 128 bytes, and run outcomes' run
and provider ids over 256 bytes, credential ids over 512, classes over 64, and
codes and interruption reasons over 128, are replaced by stable SHA-256
identifiers rather than retained unbounded. Inability to inspect the persistent
rebuild fence returns an error frame. An unreadable catalog, event ring or
retention map returns a check-specific error-severity `evidence_unavailable`
finding. The retention map is capped at 20,000 conditions; overflow is explicit
unavailability, not a false clean read.

## Verification

Serving-generated request/reply fixtures live under
`crates/broca-module-serve/tests/vectors/diagnostics-findings-v1/`. Regenerate with
`UPDATE_BROCA_DIAGNOSTICS_FINDINGS_VECTORS=1 cargo test --locked -p broca-module-serve --lib diagnostics_findings_v1_vectors_match_serving_replies`.
The handler uses seeded run outcomes and fixed clocks; ordinary runs
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
