# Pinned consent requester fixtures

The base contracts and requester corpora are vendored unchanged from Cingulate
at tag `consent-requester-v2`, commit
`a87da8c86f526f2d489adc433517246e1399e604`. The separate late-answer supplement
and notice corpus are from tag `consent-late-answer-v1`, commit
`880a065865ae4aaecc0ccbef0e5fa73ae13d014a`. Never treat these two sources as one
revision or replace the base v2 text with the later text under the base pin.

The operator resolved both tags in the Cingulate checkout and supplied frozen
files from those commits. The additional v1 and late-answer corpora were supplied
using `git archive` at their respective commits. Vendoring reads only those
frozen copies, not the source checkout. The base v2 text ends at line 181; the
later text preserves it and adds the late-answer section at lines 183-257.
The latter introduces detach/notices, not a replacement base requester corpus.

Consent provider id: `cingulate`. Its stamped principal is `reserved:cingulate`
(v2 §Notices and audit, lines 158-161). Broca's requester principal is
`reserved:broca`; allowing that principal to file generic consent requests needs
an upstream Cingulate admission change. The Cerebellum vectors do not already
prove that broca is admitted.

## Source pins

These are independent SHA-256 pins for the exact bytes, also recorded in
`verify.py`. Source paths are `docs/contracts/consent-requester-v1.md`,
`docs/contracts/consent-requester-v2.md`, and the corresponding
`test-vectors/consent-requester-v1/`, `test-vectors/consent-requester-v2/`, and
`test-vectors/consent-late-answer-v1/` directories. The late contract copy's
upstream path is also `docs/contracts/consent-requester-v2.md` at the late commit;
its local name distinguishes the supplement. Upstream relative links remain
unchanged and may refer to documents outside this vendored subset.

| Local pinned path | Tag | Source commit | SHA-256 |
|---|---|---|---|
| `upstream/consent-requester-v1/SHA256SUMS` | `consent-requester-v2` | `a87da8c86f526f2d489adc433517246e1399e604` | `e4eb488651dbd1e3c3164644a4e4bc0ba3d39f81cb4f3e1f09cc7913564a3248` |
| `upstream/consent-requester-v2/SHA256SUMS` | `consent-requester-v2` | `a87da8c86f526f2d489adc433517246e1399e604` | `d276eaed5330b95923166d006b2f1b5d83a9a95e050cfe99446d685b22c2e568` |
| `upstream/consent-late-answer-v1/SHA256SUMS` | `consent-late-answer-v1` | `880a065865ae4aaecc0ccbef0e5fa73ae13d014a` | `ab436b9d31fa27553aa9a5c7e5af2a3723a108246c222badcb85bc3de4d1f2d6` |
| `upstream/contracts/consent-requester-v1.md` | `consent-requester-v2` | `a87da8c86f526f2d489adc433517246e1399e604` | `74adafc24e98e7e4958306c03c6db83fe1969763a56002adc0abc867ec3c4042` |
| `upstream/contracts/consent-requester-v2.md` | `consent-requester-v2` | `a87da8c86f526f2d489adc433517246e1399e604` | `068f61d4ee572f0fbe336dad1d1c26f2e418c63456c2d112d9c1b42e1b12a4ae` |
| `upstream/contracts/consent-late-answer-v1.md` | `consent-late-answer-v1` | `880a065865ae4aaecc0ccbef0e5fa73ae13d014a` | `5644837fbc391ba443e55ff1a7d888c444afdbce7ad201a632935c48285be5e9` |

## Integrity and body inventory

The root `SHA256SUMS` covers **every local file except itself**, including this
README, `verify.py`, all contracts and all upstream manifests. There is no
root-manifest digest embedded in a file it hashes: that would be circular.
`verify.py` first verifies complete root coverage and raw file bytes, then checks
each upstream manifest against its independently recorded README/script pin,
checks upstream file coverage/hashes and the contract pins, and checks the local
mapping and fail-closed golden expectations. No normalization is used for hashes.

Run from the repository root:

```sh
python3 -B crates/broca-subc/tests/fixtures/consent/verify.py
```

A failed check prints a named unittest failure. A ledger disposition other than
Settled stops downstream readiness. Upstream files must not be regenerated here.
After intentional changes to broca-authored files or the wrapper, refresh only
the root manifest; never repin upstream merely to make a check pass.

| Directory or file | Authorship and coverage |
|---|---|
| `upstream/consent-requester-v1/` | Upstream: 44 JSON bodies, its README and original manifest; request/get/await/answers, pending dedup refresh/replacement, withdraw, ack, execution report and post-ack/withdraw feeds. Flow-decision requests are Basal examples and the execution report is a package example, not broca generic requests. |
| `upstream/consent-requester-v2/` | Upstream: 26 JSON bodies, its README and original manifest; Cerebellum generic requests, requester readbacks/answers/outcomes, phone projections and kind refusal, with and without requester_ref. |
| `upstream/consent-late-answer-v1/` | Upstream-authored target bytes: three answered/expired/withdrawn notices, README and manifest. Its README says these were written before implementation, not captured served responses. These are core session.notice bodies, not requester detach replies. |
| `upstream/contracts/` | Three unchanged upstream text copies, each independently pinned above. |
| `broca/held-call-request.json` | Broca-authored golden consent.request envelope derived from the 18-row mapping; no upstream held-call request reproduces the required broca binding. |
| `broca/held-call-input.json` | Broca-authored frozen HeldCallRequest inputs, including noncanonical argument key order and the maximum-length call_key. |
| `broca/expired-rendering.json` | Broca-authored expected decoder/dispatch result for the legacy expired vector; not a daemon reply. |
| `broca/contract.md` | Broca-authored argument-binding enforcement, double-lookup recovery, execution reporting and card lifecycle rules; see the ledger for their contract sources and decisions. |

All vendored replies and answer records are upstream-pinned. No broca-authored
reply masquerades as upstream. Generic detach request/readback served vectors
are not present in these corpora; the operation is specified by the late text.
A downstream real-daemon test must verify that operation; absence of a placed
Cingulate build skips by test name, and requiring a build must fail if it is
missing. These fixture checks do not prove requester admission or runtime I/O.

## HeldCallRequest mapping

The source inventory is `crates/broca-subc/src/elicitation.rs:29-50,398-482`
at broca baseline `000babd15951f95f46d9b0d5e1e13e4d37c66d09`. Each of its
18 fields appears exactly once below; the verifier checks the exact multiset.
The mapping describes the new generic body, not the old C9 envelope. Plan
admission means validating the incoming execution plan before writing any
records to the write-ahead log (WAL).

| HeldCallRequest field | Consent v2 field | Consequence |
|---|---|---|
| `kind` | `params.kind` | Exactly consent. |
| `title` | `params.title` | Preserve Run plus the tool name. |
| `prompt` | `params.prompt` | Preserve the prepared prompt. |
| `options` | `params.options` | Exactly approve/grant and deny/decline. Extra options refuse approval_options_unsupported at plan admission before any WAL write; never silently drop them. Old extra-decline emission is superseded. |
| `default` | `params.default` | Exactly deny; silence is not a person's answer. |
| `urgency` | `params.urgency` | Preserve normal. |
| `on_expiry` | `params.on_expiry` | Exactly deny; expired state renders None even with legacy choice metadata. |
| `material_damage` | `params.material_damage` | Preserve the prepared boolean. |
| `late_execution` | `params.late_execution` | Preserve execute or notify_only; Cingulate executes nothing. |
| `target` | `params.target` | Preserve module kind and module label, falling back to broca when absent; display content, not identity. |
| `expires_in_ms` | `params.expires_in_ms` | Remaining lifetime; zero still refuses expired. Broca's absolute deadline wins. |
| `call_key` | `params.requester_ref` and `params.dedup_key` | Both equal call_key; requester_ref is correlation only, dedup matches pending cards only. |
| `module` | `params.binding.module` | Frozen module string or explicit null; not a top-level field. |
| `tool` | `params.binding.tool` | Frozen tool name; not a top-level field. |
| `schema_pin` | `params.binding.schema_pin` | Explicit null for supported filings; Some retains approval_schema_pin_unsupported. Opaque binding is not daemon schema validation. |
| `executor` | dropped | Broca executes its own calls; the generic card does not accept the runner field. |
| `args_jcs` | dropped as raw content; SHA-256 feeds params.binding.args_hash | Hash exact frozen UTF-8 bytes, never parsed/reserialized bytes. Keep the original args_jcs locally for dispatch. |
| `args_hash` | `params.binding.args_hash` and `params.args_digest` | Lowercase SHA-256 hex of exact args_jcs; binding equality is enforced solely by broca. |

Stored elicitation_id: `result.elicitation_id`.
This is the result object in the successful consent.request response envelope.
It is persisted before await or the second recovery lookup. There is no filing
args_hash or issued approval_digest to consume. **Broca is the sole enforcer of
the argument binding for its calls** and stops sending approval_digest on
approved tool requests. Canonical binding has exactly args_hash, module,
schema_pin and tool, with explicit null for absent module/schema pin.

## Assumptions ledger

Every ID is settled by a cited contract section or an explicit operator/SUBC
ruling below. The reviewed spec omitted its promised ID table; the subjects here
follow its explicit L1-L15 references. Any new contradiction must be escalated,
not silently inferred. Rulings supersede the original issued-digest, no-detach,
via_worker, exactly-one-card and extra-option assumptions where stated.

| ID | Disposition | Settling section | Decision |
|---|---|---|---|
| L1 | Settled | Operator tag resolution; v2 Generic consent and requester correlation lines 44-48; late supplement lines 183-257 | Base tag resolves to a87da8c86f526f2d489adc433517246e1399e604. Late tag resolves to 880a065865ae4aaecc0ccbef0e5fa73ae13d014a; keep its pin separate. |
| L2 | Settled | v1 Identity and admission lines 17-43; v2 Generic consent lines 12-24; v2 corpus cerebellum-ref-get-reply.json line 1; operator ruling | No requester-set via_worker or other identity fields. Daemon-stamped subject.agent_id/head_agent_id names the subject/head; that spec clause is superseded. |
| L3 | Settled | v1 Compatibility lines 140-144; v2 Request bodies line 85 is AFT-only | Generic schema_pin has no validated top-level field. Preserve Some refusal approval_schema_pin_unsupported; supported binding carries schema_pin:null. |
| L4 | Settled | v1 Three decision bodies lines 100-101 and Compatibility lines 152-154; v1 corpus grant-lost-expired-answers-reply.json line 1; operator ruling | The vector contradicts prose by including answered_choice_id/answered_at_ms on expired. Preserve bytes; decode without error. Broca follows prose: expired is None, never approval or a person's decision, regardless of metadata. |
| L5 | Settled | v1 consent.request line 69 and Compatibility lines 140-144; SUBC binding ruling; broca/contract.md Frozen binding | Generic binding is opaque. Carry canonical args_hash/tool/module/schema_pin, hash exact args_jcs, compare answered binding canonical bytes to stored bytes; refuse consent_binding_mismatch. Broca alone enforces it. |
| L6 | Settled | v1 request success lines 78-81 and requester records lines 158-165; SUBC digest ruling; broca/contract.md Frozen binding | Supersede the issued-digest assumption. No provider verifies one today; stop sending approval_digest on approved calls, with no computed substitute. Durable approval plus matched binding gates dispatch. |
| L7 | Settled | v1 Updating or withdrawing lines 234-252 and answers lines 177-193; v2 correlation lines 39-42; operator recovery ruling | Lookup before refile, persist returned id, then look up again. Adopt an older settled answer, withdraw a new duplicate and persist adoption. Conflicting settled decisions refuse consent_conflicting_decisions with identity-only logging. At most one answer decides a call and it runs at most once; a duplicate can be briefly visible. |
| L8 | Settled | v1 consent.await lines 206-216 | One id, timeout capped at 120000 ms; pending grants nothing. Dropping/cancelling removes registration. Close the route when core drops the wait; retry only while held. |
| L9 | Settled | v1 consent.ack lines 195-204 | Ack is cursor-cumulative for the owner, not per card; repeating a processed cursor cannot undo an answer. Branch C sends no ack. |
| L10 | Settled | v1 Identity lines 17-21 and answers/ack lines 172-204; v2 Generic consent lines 19-24 | Feed is bound-owner scoped, not run scoped. Select C; read since zero, follow continuation and deduplicate locally by card id. |
| L11 | Settled | v1 get lines 167-170 and ack lines 197-204 | Acked settlements become unavailable to get/await/answers. No run ack means late intake retains its read; retention tombstones are not decisions. |
| L12 | Settled | v1 report_execution lines 218-230 and Updating or withdrawing lines 254-258 | executed, approved_not_executed, outcome_unknown; answered state required. Report approved outcomes after result durability, retry same value in-process, no restart debt. Terminal withdrawal returns a record unchanged, not answered refusal; approving close-out readback reports not executed and never dispatches. |
| L13 | Settled | Late supplement consent.detach lines 185-212; operator lifecycle ruling; broca/contract.md Detach inventory | Contract supersedes no-detach default: detach on releasing a visible card unless withdrawal is owed. Local expiry/known-id Unavailable are listed below. RestartPause still holds. Expired/Unavailable stays closed; settled detach readback uses binding-checked late intake only, never ordinary dispatch. |
| L14 | Settled | Late supplement consent_answered lines 216-257; pinned late notices; operator coordination ruling | Core notice only wakes broca; broca is sole authorization/execution intake, checking every late binding. The notice itself grants nothing and cannot duplicate execution. |
| L15 | Settled | v2 Notices and audit lines 158-161; v1 Identity lines 17-21 | Consent provider id is cingulate. Known retention rows withdraw using frozen owner scope and that id; Unknown rows lapse with no consent op. |

## Lifecycle inventory

Selected ack branch: **C**. Broca sends no `consent.ack`; no branch A/B
ack-after-sync or ack-never-gates-dispatch rows are required. The owner-feed
no-ack test and two-run isolation test replace them.

The cited broca lines are at commit
`000babd15951f95f46d9b0d5e1e13e4d37c66d09`; if later edits move them, locate the
approval-gate closing decision and batch wait loop in run.rs. Every release with
a known visible card must use this inventory, not merely cancel await.

| Path | Source | Consent action |
|---|---|---|
| Local deadline closes held call | crates/broca-core/src/run.rs:9900-9911 | Detach a known still-visible card unless withdrawal is owed. |
| Deadline observed during answer intake | crates/broca-core/src/run.rs:10099-10102 | Broca's clock closes Expired; terminal readback goes to binding-checked late intake only. |
| Filing-binding mismatch releases Unavailable | crates/broca-core/src/run.rs:10050-10055 | Detach known visible card unless withdrawal is owed; old filing-hash path is superseded by the binding ruling. |
| Other Unavailable releases | crates/broca-core/src/run.rs:9972-9978,10057-10058; id-less deadline at 9900-9905 | Every known visible card detaches unless withdrawal is owed. With no id, no id operation can be sent. |
| Closing decision persistence | crates/broca-core/src/run.rs:9208-9234 | Expired/Unavailable never reopens for ordinary dispatch. A settled approving detach readback is reported not executed for that closed call. |
| Non-resume stop/cancel | crates/broca-core/src/run.rs:10008-10024,9236-9259 | Withdraw unanswered open cards through the existing debt. |
| Graceful RestartPause | crates/broca-core/src/run.rs:10000-10006 | Still held: no detach and no withdraw. |
| Crash close-out and retention expiry | Reviewed lifecycle/retention contract; broca/contract.md | Withdraw, not detach; a recovered answer never dispatches the crash-closed call. |

`consent.detach` uses `id` as its contract spells it. A card already settled
returns unchanged; an Expired or Unavailable call stays closed. Readback enters
only the binding-checked late intake, not ordinary dispatch. Report not executed
for that closed approving call. An Unavailable call does not become an expired
late-execution candidate. Declined or unanswered readbacks get no report.

## Downstream runtime test ownership

The fixture verifier proves provenance, coverage and local contract vectors,
not production admission, WAL ordering, cancellation or dispatch. The following
named runtime tests belong to the implementations that change those paths.
A fixture-only test can check a
contract example but cannot establish actual dispatch or durability behavior.

- Plan admission: `approval_plan_extra_options_refused_before_wal`; refuse
  approval_options_unsupported before any WAL write. This supersedes extra
  decline-option emission in elicitation.rs:431-455 until generic consent defines
  extras. Do not drop extras at filing.
- Core recovery: `consent_refile_race_adopts_answer_and_withdraws_duplicate` and
  `consent_recovery_conflicting_decisions_refused`.
- Consent adapter: `consent_request_body_has_no_identity_keys`, comparing actual
  sent body to the golden; never infer identity from body metadata.
- Core/adapter binding: live, recovered-held and late mismatches refuse by name;
  use the exact args_jcs bytes and send no approval_digest to a provider.
- Core expiry: `consent_expired_choice_metadata_is_non_authorizing`, applying
  broca/expired-rendering.json to the actual decoder and asserting no dispatch.
- Core owner feed: `consent_no_ack_on_owner_feed` and a two-run isolation test;
  cancellation before ToolDispatchIntent keeps zero provider requests.
- Consent lifecycle: local expiry/Unavailable detach, settled-readback late-only
  intake, stop/crash/retention withdrawal, RestartPause no detach/withdraw, and
  cancellation closing an outstanding await route within one second.

Release-owner checks remain separate: provider migration is unnecessary under
the current SUBC no-verifier ruling, and deferred-call production/archive scans
still determine rollback safety. This vendoring slice does not assert those
operational checks have run.
