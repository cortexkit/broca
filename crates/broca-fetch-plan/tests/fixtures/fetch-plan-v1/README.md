# fetch-plan-v1: the fetch plan, the composition and relayed-call correlation

Vendored unchanged from prefrontal main `f5a27673b`, `test-vectors/fetch-plan-v1/`.
Broca keeps the compaction-bearing two-phase vector unchanged to verify its identity;
Broca does not accept that plan because it contains `composition.compaction`.

Status: **draft for review with BROCA.** Nothing here is built on either side
yet. Core (prefrontal-core) produces these bytes; Broca's B8 writer consumes
them. Once `cortexkit-role-llm-runner` exists in commons (BROCA owns the
crate), the types and these vectors move there, and this folder becomes a copy
or is removed.

Sources, cited below as:

| Tag | Document |
|---|---|
| D | `magic-context/.cortexkit/alfonso/plans/ck-extensibility-design-r7.3.md` |
| E | `ck-extensibility-r7.3-errata.md`, same folder. Overrides D where they differ. |
| TP | `commons/crates/cortexkit-role-tool-provider/CONTRACT.md` (`tool-provider/v1`) |
| P | prefrontal's stage-2 core plan, `docs/designs/extensibility-stage2-core-plan.md`, slice C1 and "Decisions needed from ALF" |

The shape settlements below are confirmed by BROCA, room #881, unless another
owner is cited. This remains a wire draft, not an implemented runner.

## Rules already settled

- The plan and composition types live in `cortexkit-role-llm-runner`. Until
  that crate exists, this folder is the source Broca builds against (E "Stage-2
  plan wire", first bullet).
- `composition_digest` is SHA-256 over the RFC 8785 (JCS) bytes of the
  composition object, as 64 lowercase hex characters, the same rule as
  `schema_digest` (E "Stage-2 plan wire"; TP §3). Nothing is stripped first.
- The composition lists every plan provider. Text-only and hook-only providers
  appear with an empty tool list (E "Stage-2 plan wire", "What the composition
  lists").
- Purity. A provider chooses its text and tool variant only from provider ids,
  tool names, capability tags, preset, params and config; scope, agent and
  session identity never enter a fetched byte. (E "Stage-2 plan wire",
  "Purity rule". D §4.5 "Prefrontal's text" makes head persona text the one
  exception: it is the stamped agent's text. The bounds are recorded under Persona text exception (MC, room #877).)
- Core sends a plan only when the runner's `role.describe` declares the
  `llm-runner/v1` major and the `plans` group in its top-level `capabilities`,
  naming the plan ops in that major's `ops` (BROCA B8 review, room #964;
  ALF #966). Discovery uses the same role-description shape as core's steer
  support: `llm_runner_consumer/send_delivery.rs::describe_advertises_steer`
  reads the major and the top-level `steer` group, not a manifest claim.
- Core's tool item on every Broca session carries `exclude: ["web_search"]`,
  so the session's only `web_search` is Plexus's (D §4.5 "`web_search` goes to
  Plexus directly on Broca"; D §4.1 `exclude`; P decision 5).
- A relayed call's onward `call_key` is `pf:<relay record id>`, and the
  originating `(carrier, call_key)` is specified top-level beside it (pending subc-protocol field) (E "Stage-2
  plan wire", "Relayed call keys"; D §9.3 "`call_key`").

## Files

Every digested vector is three files with one stem:

| File | Content |
|---|---|
| `<name>.json` | pretty JSON, keys in a readable order |
| `<name>.jcs` | the exact RFC 8785 bytes: UTF-8, no trailing newline |
| `<name>.sha256` | SHA-256 of the `.jcs` bytes: exactly 64 lowercase hex characters, no trailing newline |

`.gitattributes` marks `.jcs` and `.sha256` as binary so no checkout rewrites
them.

| Folder | What |
|---|---|
| `compositions/` | one triple per composition |
| `plans/` | one triple per plan. The `.sha256` is the plan identity (below). |
| `sends/` | `session.send` first-send bodies carrying a plan |
| `calls/` | future relayed tool calls with `origin` (pending subc-protocol 0.28.0), and `pf-call-keys.json` (the key bound) |
| `subscriptions.json` | complete hook and phase vocabulary, including stage-3 `approve` |
| `catalog-requests.json` | tool preset/params and text-only fetch request shapes |
| `admission/` | admission outcomes: what Broca must do with a plan, including refusals |
| `generate.py` | lays the files out from hand-written values and computes bytes and digests |

`browser_use` and `computer_use` go to Cerebellum; `web_search` goes to
Plexus. Broca sessions reach Plexus's `web_search` directly; core's relay
serves only OpenCode and Pi heads. Those plugin calls have no originating key,
so core mints a `pf:` key and sends no `origin`.
`calls/relayed-web-search-from-opencode-plugin.json` pins the relay's
arguments: `web_search_relay::search_frame` in `prefrontal-core-module` copies the model's
`query`, `max_results`, `include_domains`, `recency` and inserts the minted
`agent_assertion`. The token here is illustrative, not a valid signature.
The correlation fields show the future shape, not today's emitted frame.

The vectors hold only objects, arrays, null, ASCII strings, booleans and small
integers; no floats. Within that domain every RFC 8785 implementation agrees.
`crates/prefrontal-core/tests/it/fetch_plan_v1_vectors.rs` recomputes every file
with a full RFC 8785 implementation (UTF-16 key order, ECMAScript number form
including exponents), independent of the generator, requires
prefrontal-core's shared `prefrontal_core::rfc8785` to produce the same bytes
wherever it accepts a value, and checks every `pf:` key with subc-protocol's
`validate_call_key`.

`commons/tool-provider-v1-composition-digest.json` is a byte-for-byte copy of
the tool-provider contract's composition-digest vectors (commons `master` at
commit `25e29afe80dea2ec159473f545246707b82746b5`,
`test-vectors/tool-provider-v1/composition-digest.json`; SHA-256
`714af3c3a1504231e86cd5a4fe9c066214ef65d2d43a3aa8894df6555977ac1e`). The
test pins the copy by that digest and reproduces every case in it, including
the non-ASCII, float and key-order cases, so prefrontal's canonicalisation and
commons' `catalog::composition_digest` agree.

Provider ids are module ids (`prefrontal-core`, `aft`, `magic-context`,
`plexus`). Every provider's preset is the session role: `head`, `worker` or
`reader`. Tool names, param values and capability tags are illustrative; the
shapes, ordering rules and bytes are pinned. Core's catalog also accepts the
old input aliases `forwarding`, `persona` and `role`, but no plan emits them.

## Shapes

### `session.send` with a plan

The plan rides on the session's first `session.send` as a top-level `plan`
member beside today's fields (D §4.5 "The fetch plan"; Broca build plan B8
"Wire").

| Field | Type | Required | Producer → consumer | Source |
|---|---|---|---|---|
| `prompt` | string | yes | core → Broca | today's `session.send` |
| `send_id` | string | yes | core → Broca | D §12.1 |
| `model` | object `{provider, model, variant?}` | yes | core → Broca | today's `session.send` |
| `plan` | Plan | first send, or identical later repeat, when the runner's `role.describe` declares the `plans` group naming the plan ops | core → Broca | D §4.5; BROCA #964; ALF #966 |
| `tools`, `system`, `context_management` | — | absent when `plan` is present; presence refuses `invalid_params {field}` naming the offending send member | | BROCA, room #881 |

The plan names no session: the send is on a route under the session's scope,
and the scope comes from the stamp (D §4.5, first paragraph; D §4.7.2). The
resend after an auth pause repeats the first send byte for byte, plan
included, with the same `send_id` (D §4.5 "The freeze covers the whole
session"). Vectors: `sends/broca-head-first-send.json`,
`sends/broca-worker-first-send.json`.

### Plan

| Field | Type | Required | Producer → consumer | Source |
|---|---|---|---|---|
| `tool_items` | array of Item | yes, may be empty | core → Broca | D §4.5 "The plan's items" |
| `system_text_items` | array of Item | yes | core → Broca | D §4.5 "The plan's items" |
| `compaction_item` | Item | no: omitted when the session has none or the runner does not declare the `compaction` group in `role.describe` | core → Broca | D §4.5 "The plan also names" |
| `step_transform_items` | array of StepItem | yes, may be empty | core → Broca | D §4.5; D §6.2 |
| `composition` | Composition | yes | core → Broca → every provider, verbatim | D §4.5 "The composition"; E "tool-provider/v1 settlements" |
| `composition_digest` | string, 64 lowercase hex | yes (BROCA, room #881) | core → Broca | E "Stage-2 plan wire"; BROCA, room #881 |

The `llm-runner/v1` contract refuses a plan carrying `compaction_item` when
its runner does not declare the `compaction` group in `role.describe`. Broca
accepts `compaction_item` when that group is declared (since 0.3.192), but
acceptance of `composition.compaction`, which the contract requires alongside
the item, is still pending in Broca. No production Broca plan or Broca-named
vector carries a compaction item.

Broca does not declare hook support either. Until its `role.describe`
declaration includes hooks, it refuses every non-empty `step_transform_items`
list with `invalid_params {field: "plan.step_transform_items"}`. Core's plan
builder refuses to build steps for a runner whose declaration lacks hooks. A
Magic Context step-transform item also requires a Magic Context compaction
item in the same plan.

Magic Context's preset is always the session role. The old `primary`,
`subagent`, `tools-only` and placeholder `default` names are retired from
plans. Compaction is represented only by the compaction item and
`composition.compaction: {"provider": "<module id>"}`: the composition member
exists if and only if the plan has a compaction item, names the same provider,
and is otherwise omitted entirely, never `null` (fleet #1658). There is no
per-item `compacts` flag.

Heads receive Magic Context tools and text whether or not they compact. A
non-compacting head receives exactly `ctx_memory`, `ctx_note`, `ctx_search`,
sorted by name. This list was read from the sibling checkout's
`magic-context/docs/designs/mc-tool-catalog-v1/tools-only.answer.json:201–204`
and `magic-context/crates/mc-module/src/tool_catalog.rs:640–641`. A worker or
reader receives no Magic Context tool or text item unless Magic Context is the
plan's compaction provider; when it is, the role's reduced subagent catalog is
used. A compacting head instead receives the full Magic Context catalog. No
current production runner compacts, so no production worker or reader plan
includes Magic Context. Reduction hooks require tags and `ctx_reduce`; the
four hook plans are synthetic fixtures for a runner that declares both hooks
and compaction. They carry a Magic Context compaction item so the hooks have a
compacting session, and each has a paired admission case for a runner without
hooks. Neither set is a production Broca plan.

| Plan vector | When core sends it | MC presets |
|---|---|---|
| `plans/broca-head` | Broca declares `plans`; `composition.compaction` support is pending | `head` for tools/text; non-compacting tools, empty step transforms, no compaction item |
| `plans/broca-head-no-compaction` | same head, retained for existing vector consumers | `head` for tools/text; identical to `broca-head` |
| `plans/broca-worker` | Broca worker; `composition.compaction` support is pending | no Magic Context items |
| `plans/broca-worker-unknown-provider` | worker with a required unavailable provider | no Magic Context items |
| `plans/broca-reader` | Broca reader; `composition.compaction` support is pending | no Magic Context items |

The Broca worker and reader vectors have no Magic Context tool, text, or
compaction items. The worker vectors are `plans/broca-worker` and
`plans/broca-worker-unknown-provider`; `plans/broca-reader` pins the reader
case. The separate synthetic hook vectors do not represent production worker
or reader plans.

Rules:

- **Join.** System texts are joined with `"\n\n"` (join version 1); an empty
  text contributes nothing. Each text's bytes are joined verbatim, with no
  trimming or newline normalisation.
- **Order.** `system_text_items` order is precedence, and `prefrontal-core`
  is first (D §4.5 "The plan's items"). `step_transform_items` order is the
  order the runner applies operations (D §4.5; D §6.2 "Stacking"). `tool_items`
  order is the model-facing order, provider by provider, each in the provider's
  catalog order, frozen at admission (BROCA, room #881).
- **One list per role, two-phase fetches.** A provider with both a tool
  item and a text item (AFT, Magic Context) appears once in each list. The
  runner first fetches tools without `system_text`. After admitting the tool
  providers, it fetches text with `params: {}`, the text item as `system_text`,
  and the admitted composition (after optional tool providers are removed).
  The four collections (`tool_items`, `system_text_items`, `compaction_item`,
  `step_transform_items`) separate providers by role. A provider appears at most
  once in each list; a duplicate is refused as `invalid_params {field}`
  naming that plan list (BROCA, room #881).
- **Text-only fetch.** A provider with a text item and no tool item is fetched
  with `params: {}` and the text item's preset and params in `system_text`,
  and answers an empty tool list (BROCA, room #881; D §4.5).
  A tool item's `preset` rides as a top-level optional `preset` on
  `CatalogRequest`; Broca sends exactly `item.preset` and `item.params`
  (BROCA, room #881; AFT adding it to TP §3, rooms #878 and #880).
- **Optional.** Tool and system-text items may carry `optional: true`;
  producers omit the member otherwise, and a missing member means required.
  Core marks only direct search tool providers optional; repository (AFT),
  Magic Context and core items stay required. Broca refuses literal `false`
  on any item, and any `optional` member on compaction or step-transform items,
  as `invalid_params {field}`. A head's persona item is never optional.
  **Runner advertisement:** optional tool items require the exact token
  `fetch-plan-optional-tools/v1` in the runner's `role.describe.capabilities`
  array, in addition to its existing `llm-runner/v1` plans declaration.
  Core omits nonessential search providers entirely when that token is absent
  or discovery fails; it never falls back to required search items. Required
  repository, core and Magic Context items are unchanged. The optional-tool
  admission vectors record the token in `runner_declaration.capabilities`.
  The token is not a plan member and does not change any plan or composition digest.
  This replaces the former system-text-only rule: `refuse-optional-non-text`
  now tests compaction, not tools; `refuse-optional-false-tool` pins literal false.
  Admission fetches tools first. A refused, timed-out, unknown or stale optional
  tool provider goes to `absent_items` with `kind: "tool"` and reason
  `refused | timeout | unknown_module | plan_stale`. Its text item, if any, also
  goes absent (`kind: "system_text"`) without being fetched, even if required.
  Before fetching any text, remove that provider's **whole entry** from
  `composition.providers`; preserve surviving order, step-transform items,
  compaction and unknown members unchanged. Do not rebuild or normalize them.
  Freeze `composition_digest` over this admitted composition's JCS bytes.
  Every second-phase text `tool.catalog` request carries the admitted
  composition, and its reply must match the admitted digest.
  `admit-optional-tool-two-phase` pins both phases, the admitted composition's
  JSON/JCS/SHA-256 triple and `catalog-requests/optional-tool-two-phase-text`
  request triple, including opaque members and ordered frozen steps.
- **Every provider is composed.** In the planned composition, every provider
  named in any list appears exactly once in `composition.providers`, and no other provider does
  (E "What the composition lists").
- **Digest.** `composition_digest` equals SHA-256 of the JCS bytes of
  `composition`. Broca recomputes it over the composition as received and
  refuses a mismatch as `invalid_params {field: "plan.composition_digest"}`,
  using commons' `composition_digest()` (BROCA, room #881). Tool fetch replies
  are checked against the planned digest; text fetch replies against the
  admitted digest after optional tool providers have been removed.
- **No deadline in the plan.** Broca's one 10 s admission deadline applies.
  Each optional tool fetch has its own 5 s budget capped by that shared deadline.

#### Item

| Field | Type | Required | Producer → consumer | Source |
|---|---|---|---|---|
| `provider` | string, module id | yes | core → Broca (route target) | D §4.5 "Each plan item names its provider, preset and params" |
| `preset` | string | yes | core → Broca → provider, verbatim | D §4.5; D §9.1 |
| `params` | object | yes, may be `{}` | core → Broca → provider, verbatim | D §4.1; D §4.5 "Passed verbatim"; TP §3 |
| `optional` | `true` | no, tool and system-text items only | core → Broca | Two-phase optional tool admission |

`params` is opaque to Broca and carried verbatim. Its shared vocabulary is
`behavior`, `scope`, `tool_descs` and `exclude`, plus each provider's own axes
(D §4.1; TP §3).

#### StepItem

An Item plus:

| Field | Type | Required | Producer → consumer | Source |
|---|---|---|---|---|
| `subscriptions` | array of `{hook, tools?, ops, phase?, on_unavailable, budget_ms}` | yes | core → Broca | Extensibility room, 2026-10-01 #912–#916; MC draft Q4/Q5 |

`hook` is one of `pre_user`, `post_assistant`, `pre_tool`, `post_tool`.
`tools` limits the subscription to named tools and is omitted for all tools.
`ops` lists `prepend`, `append`, `replace` on text hooks and is empty on
`pre_tool`: `mutate` answers pass or rewritten input, `validate` answers pass
or deny, and `approve` answers pass or a human question (room #912–#916, MC Q5).
A plan with `append` on `pre_tool` is refused as `invalid_params`, with
`detail {field: "plan.step_transform_items", item, subscription,
problem: "ops_on_pre_tool"}` (MC draft §4, Q11 and `SubscriptionProblem`).
`phase` is `mutate | validate |
approve` and appears on `pre_tool` only. A provider wanting two phases declares
two subscriptions. `approve` is the runner-side approve gate, whose decline
reason is `pre_tool_declined` (D §6.4–6.5); it is stage 3, so no stage-2 plan
uses it. The enum is complete from the first vector (MC, room #883, owner of
step-transform-provider/v1; BROCA, room #881).

Core copies `on_unavailable` (`pass | refuse`) and `budget_ms` (positive
integer milliseconds) from the provider's per-preset declaration into every
subscription while composing. An omitted declaration policy means `refuse`,
except on `post_assistant`, which always passes; declaring `refuse` there is
malformed (MC draft `DeclaredSubscription`, Q4). These values are frozen in
the plan and inside its composition digest, and the runner caps each call's
budget with its own engine-wide limit.

Admission compares against the current declaration, not for equality. Equal
or stricter subscriptions admit and remain frozen: `refuse` is stricter than
`pass`, and a budget no greater than the declared budget is within bounds.
A looser policy or budget, or planned `tools` or `ops` no longer covered by
the declaration, refuses `plan_stale` with
`detail.differences: [{kind: "subscription_loosened", provider, hook, phase, field}]`.
Core derives plans from declarations, so wider planned tools or ops mean the
current declaration narrowed after the plan froze; rebuilding the plan is the
remedy. Omitted `tools` means all tools on either side; listed tools and ops
must be subsets of the declared bounds.
A missing declared subscription refuses with
`{kind: "subscription_missing", provider, hook, phase}`. Every subscription-scoped
difference requires `phase`, set to `null` for hooks without phases, so two
`pre_tool` phases remain distinguishable. A gone preset produces exactly one
`{kind: "preset_missing", provider, preset}` per item, regardless of its
subscription count.
Admission formerly froze step-transform items verbatim without checking their
declaration (BROCA #881); it now checks these bounds before freezing:
see extensibility room 2026-10-01 #912–#916, especially BROCA #915, and MC's
`step-transform-provider/v1` draft at commons `112e295`, Appendix A.2 and Q4/Q5.
The cross-check rulings below supersede that draft's open Q11 choice and its
older difference encodings.
Vectors: `plans/pre-tool-{declared,tightened,two-phase}` and the corresponding
`admission/admit-pre-tool-*`, `refuse-subscription-loosened-*`,
`refuse-declared-{hook,preset}-gone`, `refuse-declared-preset-gone-two-subscriptions`,
and `refuse-pre-tool-append`. The two-phase admission refusal changes only
`validate`, not `mutate`. The hook vectors carry Magic Context compaction and
have admitted outcomes with runner declarations `{hooks: true, compaction:
true}`; `refuse-runner-without-hooks-*` twins pin the same plans against
`{hooks: false, compaction: true}`. Every shared admission outcome records
both runner declarations explicitly.

### Composition

```json
{"providers": [{"provider": "<module id>", "tools": [{"name": "<tool>", "capabilities": ["<tag>", ...]}, ...]}, ...], "step_transform_items": [{"provider": "<module id>", "preset": "worker", "params": {}, "subscriptions": [{"hook": "pre_tool", "phase": "validate", "ops": [], "on_unavailable": "pass", "budget_ms": 1000}]}]}
```

| Field | Type | Required | Producer → consumer | Source |
|---|---|---|---|---|
| `providers` | array | yes | core → Broca → every provider, verbatim | D §4.5 "The composition" |
| `compaction` | object `{provider: "<module id>"}` | only when the plan has a compaction item; omitted otherwise, never `null` | core → runner → every provider, verbatim | fleet #1658 |
| `step_transform_items` | array of StepItem, identical to the plan's ordered list | core always sends it, `[]` when empty; consumers also accept it omitted | core → Broca → every provider, verbatim | Extensibility room 2026-10-01 #912–#916; frozen declaration values |
| `providers[].provider` | string, module id | yes | | D §4.5 "its module id" |
| `providers[].tools` | array, empty for text-only and hook-only providers | yes | | D §4.5; E "What the composition lists" |
| `providers[].tools[].name` | string, the final model-facing name after removals | yes | | D §4.5 "after the user's removals"; D §9.2 |
| `providers[].tools[].capabilities` | array of capability tags | yes, may be empty | | D §3.9; D §4.5 "each tool's capability tags" |

A provider chooses its text and tool variant only from provider ids, tool names, capability tags, preset, params and config; scope, agent and session identity never enter a fetched byte.

#### Persona text exception

Head persona text is the one named exception to the sentence above (D §4.5; MC, room #877):

1. **Who.** Only prefrontal-core's head persona item. No other provider reads scope, agent or session identity to choose bytes, and the composition never carries them.
2. **What selects it.** The verified stamp's `agent_id` only. Params, preset and composition cannot name an agent, and a request naming one is refused.
3. **Frozen.** The persona's digest is frozen in the manifest at admission like every other item, so a rename or rebind mid-session never changes the bytes. Change policy decides when a refreshed persona applies.
4. **Workers.** Worker role text (`persona.material`, `kind=role`) is gated on its scope ref, not on an agent.

Rules:

- **Canonical order.** Providers sort by module id, tools by name, and each
  tool's capabilities, all bytewise (ASCII), with no repeats. So the
  provider list depends only on the session's final tool set. The ordered
  step-transform list is also hashed, so its order affects the digest, but
  text-item order does not (vector `plans/broca-head-text-reordered`: same composition
  digest as `plans/broca-head`, different plan identity). Broca refuses
  non-canonical order or repeats as `invalid_params {field: "plan.composition"}`
  (BROCA, room #881).
- **Frozen step transforms.** `composition.step_transform_items` mirrors
  the plan's items, including provider, preset, params and subscriptions with
  frozen policy and budget. Core always sends it, as `[]` when there are no step transforms; a consumer also accepts it omitted.
  No scope or identity enters the composition. This extends the former
  tool-set shape (D §4.5) per room #912–#916.
- **Compactor.** `composition.compaction` names the compaction item's provider
  and participates in both digests. It is omitted without a compaction item.
- **Who reads it.** Providers resolve their own text against it and never
  interpret it otherwise (TP §3; E "tool-provider/v1 settlements"). Broca
  carries it verbatim, hashes it, and compares each tool provider's fetched
  tool names and capability tags against its entry (D §4.5 "Staleness check").

The commons vector `tool-provider-v1/composition-digest.json` uses a
different, flatter composition to illustrate the digest rule only. This
folder pins the shape.

### Plan identity

The plan's `.sha256` file is SHA-256 over the JCS bytes of the whole plan as
received. A later send repeating the identical plan is a no-op accept. A
different plan is refused as `plan_drift {frozen, sent}`, with these identities.
A plan on a session whose first episode had none is refused as `plan_drift`
with `frozen` absent (BROCA, room #881; D §4.5).

### Relayed tool call

A tool call core forwards (browser, computer, the `web_search` relay) is an
ordinary `ToolCallRequest` (subc-protocol; TP §4) with one added top-level
member:

```json
{"name": "browser_use", "arguments": {...}, "call_key": "pf:<relay record id>",
 "origin": {"carrier": {"kind": "reserved", "module_id": "broca"}, "call_key": "<the carrier's key>"}}
```

| Field | Type | Required | Producer → consumer | Source |
|---|---|---|---|---|
| `call_key` | string, `pf:` + relay record id | yes on every call core relays | core → Cerebellum, Plexus | D §9.3 "`call_key`"; E "Relayed call keys" |
| `origin` | `Option<CallOrigin>`, pending subc-protocol 0.28.0 | future: when the originating call carried a `call_key`; core does NOT send it today | core → Cerebellum, Plexus | CEREB, room #879; E "Relayed call keys" |
| `origin.carrier` | typed `Principal` identifying the original caller, copied verbatim from subc's route stamp; e.g. `{"kind":"reserved","module_id":"broca"}` | yes | | SUBC, room #885; CEREB, room #879 |
| `origin.call_key` | string, that carrier's key, verbatim | yes | | D §9.3 (Broca `<lineage_id>:<wal_seq>`, Claude Code `cc:<tool_use id>`) |

Rules:

- **Never inside `arguments`.** Both providers refuse undeclared argument
  fields (E "Relayed call keys").
- **Caller-stated.** Providers record `origin` for attribution only, and
  never key, dedupe, authorize or withdraw on it. Every shared record keys on
  `(stamped principal, call_key)`, here `(reserved:prefrontal-core, pf:…)`
  (D §9.3; E "Relayed call keys").
- **Absent origin.** A call relayed from an OpenCode or Pi plugin has no
  carrier key (the plugin's host id is display-only, D §9.3), so it carries no
  `origin` (`calls/relayed-from-opencode-plugin.json`).
- **The key.** The relay record id is durable, printable ASCII (0x21–0x7E,
  no space), 1 to 253 bytes, so `pf:` plus the id passes subc-protocol's
  `validate_call_key` (1 to 256 bytes). A resend of the same relayed call
  reuses its record and key (E "Relayed call keys"). `calls/pf-call-keys.json`
  pins the bound: a 253-byte id is accepted, 254 is refused by the validator,
  and an empty id (`pf:`) passes the validator but is refused by core's own
  record-id rule.
- **Carriers, not module ids.** The stamped principal is a typed object, never a bare module id or the display
  string `reserved:broca` (SUBC, room #885, correcting CEREB, room #879).
  Confirmed from subc-protocol 0.27.0 `src/lib.rs`: `#[serde(tag = "kind",
  rename_all = "snake_case")]`, `Reserved { module_id: String }`, `Direct`,
  `Unverified`; wire kind values are `reserved`, `direct`, `unverified`.
- **Pending subc-protocol 0.28.0; core doesn't send it until then.**
  `origin: Option<CallOrigin>` is top-level beside `call_key` and `schema_pin`,
  omitted when None. `CallOrigin { carrier: Principal, call_key: String }` is
  validated by `validate_call_origin`, which checks the key through
  `validate_call_key` and names the field `origin.call_key` (SUBC, room #885).
  The call vectors preserve the future shape; providers drop unknown members
  today. A late-result entry's `invocation_id` equals the `pf:` key
  (CEREB, room #879).

## Admission outcomes

Each `admission/*.json` names a plan (or inline plan), simulated fetches where
needed, and Broca's answer. Refusals write nothing to the session WAL, so a
retried first send starts clean (D §4.5; BROCA, room #881).

The frozen baseline carries the admitted `composition_digest` and, when
nonempty, `absent_items: [{provider, kind, reason, provider_code?}]`.
`session.send` returns these inside `result.baseline`; `session.baseline`
returns the same baseline. With no absences, omit `absent_items` and retain
the plan's digest, preserving existing baseline bytes. Older optional-text
outcome vectors show `absent_items` directly under `expected` as a shorthand
for that baseline member; the optional-tool vectors pin the baseline nesting.
Core accepts a changed digest only when its own subtraction of the absent
optional tool providers produces exactly that digest. An absent required
tool provider or any other digest change refuses admission locally.

- `fetch_unavailable {provider, reason}` is class `transient` for required
  items, including unknown modules. `reason` is `timeout`, `unknown_module`,
  or `refused` (with the provider's code).
- Optional text-phase fetch failures admit with `absent_items:
  [{provider, kind: "system_text", reason}]`, with the same reason values.
  Text-only absence leaves the composition and digest unchanged. Optional tool
  failures additionally allow `plan_stale`, remove the provider
  before text fetches, and report the admitted `composition_digest`.
- `plan_stale {differences: [...]}` always uses internally tagged objects:
  - `{kind: "composition_digest", provider, expected, got}`
  - `{kind: "tools", provider, missing, unexpected}`
  - `{kind: "capabilities", provider, tool, expected, got}`
  - `{kind: "text_tool_names", provider, missing, unexpected}`
  - `{kind: "subscription_loosened", provider, hook, phase, field}`
  - `{kind: "subscription_missing", provider, hook, phase}`
  - `{kind: "preset_missing", provider, preset}`
  `field` names only a subscription field (`tools`, `ops`, `on_unavailable`,
  or `budget_ms`). Subscription differences always include `phase` (`null`
  without phases); preset differences occur once per item.
- `tool_name_collision {name, providers}` names the colliding model-facing
  name and its providers.
- `invalid_params {field}` covers duplicate providers in a role list,
  forbidden send members, invalid optional encoding, digest mismatch and
  non-canonical composition. Malformed subscriptions use
  `invalid_params {field: "plan.step_transform_items", item, subscription, problem}`.
  The runner refuses admission, so malformed requests use its `invalid_params`
  code consistently (`llm-runner/v1`, commons `112e295`,
  `crates/cortexkit-role-llm-runner/CONTRACT.md:612`).
  Plan drift follows [Plan identity](#plan-identity).

These admission shapes are settled by BROCA, room #881, except the encoding
of a refused provider's code, settled below. The admission vectors pin
both the input defect and the answer; the Rust test checks each claim.

## Settled

**Refused provider code encoding (BROCA, room #904).** `reason`
is always a flat string (`timeout | unknown_module | refused`). A sibling
`provider_code` is present only when `reason` is `refused`, and absent otherwise,
in both `fetch_unavailable` detail and `absent_items` entries. This keeps the
provider's code distinct from the admission refusal's own top-level `code`.

There are no open wire-shape questions. The persona exception remains recorded above (MC,
room #877). Subscription phases are confirmed by their owner (MC, room #883);
the relayed-call `origin` field remains pending subc-protocol 0.28.0 (SUBC, room #885),
not an unsettled wire-shape decision.

## Digest migration (2026-10-01)

Frozen subscription policy and budget now participate in the composition digest
(room #912–#916). BROCA consumers pinning the previous bytes must update these
values. Every omitted existing digest is unchanged.

| Vector digest file | Previous SHA-256 | New SHA-256 |
|---|---|---|
| `test-vectors/fetch-plan-v1/compositions/broca-head-optional-text.sha256` | `ff78791a4a98baa4958d56adde3a7a9029a78de55610cec8ff59474dbc35a178` | `87581db15a872ce717663efa4de3bbdf0dc45f214ea7937ab0240cfa2c411001` |
| `test-vectors/fetch-plan-v1/compositions/broca-head-plexus-direct.sha256` | `eecbf607cac5e8ab41dc7fbef1c2ff4dc7167cede4017e6318ec6b2b3feea146` | `6745ef6d7d93f1f90db1ad59e1e69591aa964c61f36c8ee1336ec8bc8b14b40e` |
| `test-vectors/fetch-plan-v1/compositions/broca-head-plexus-no-exclude.sha256` | `f01a962042f28d35ff2c8646baddde3f7460b22704b27d9da62739c4aa065a53` | `c37b21128d37d00ae862445c641017489a05063c974a7e50f1d990fc201c9269` |
| `test-vectors/fetch-plan-v1/compositions/broca-head.sha256` | `e22b20ae7cff32aca9b8d7462629400ce3e4547571e449e1adca44a3e90718c7` | `ffae38ca80a6cd9072f5f96c1ab883db9f196ea1335a539c8b1b51b6985d219e` |
| `test-vectors/fetch-plan-v1/compositions/pre-tool-declared.sha256` | `(new)` | `117494b7488a70e1b738a7a924fd16b61b2bf56df4af45952115cd39dfcfe758` |
| `test-vectors/fetch-plan-v1/compositions/pre-tool-tightened.sha256` | `(new)` | `351e37968f6ac0646b0dbe076ade3f29a8658cf1d5077f1b9539af8b23f43fd6` |
| `test-vectors/fetch-plan-v1/compositions/text-only-and-hook-only.sha256` | `624e79d93afe2aa8ac5f52c4c9ca0122a18d9b6854a6ff3e4b9ad68c254489fa` | `16640b1d73054fd72ac1a71908445bcf1b9ac68969f0a35d2596b30d09ef6eba` |
| `test-vectors/fetch-plan-v1/plans/broca-head-optional-text.sha256` | `13b5bead1e7c1448ef1d8d8d7b80787ff43680c96287eee51196ff6a7748ed31` | `7ea16362dc2aef97bd44cb2f6307ae31a4558dc3743977f4a2e871f628ac0745` |
| `test-vectors/fetch-plan-v1/plans/broca-head-plexus-direct.sha256` | `d5ad89b9cf6b629a7111623204bf090a22d7de6782942166b27bd1381b1db87a` | `02a615ecee0d33bf7092c6464e5774fb32d4d1dd836ec9aeaffb294ff4be940e` |
| `test-vectors/fetch-plan-v1/plans/broca-head-plexus-no-exclude.sha256` | `4f90892251b3e623bd0d2a9e35c8a0f20113fdf70414e0260c992f93fa6a20fe` | `0684b818b71f05316650174fbd14e705a9880f2303f56fc797baed2883b1858b` |
| `test-vectors/fetch-plan-v1/plans/broca-head-text-reordered.sha256` | `925ccf19894264ad2b29dc7aa3cdc805c56a78dcc2702e48c845b8357d84d10e` | `96f706f1fe4ce25187a5bee9b5e4eb98a0ccd1b21b294906f397b4dfd8f78473` |
| `test-vectors/fetch-plan-v1/plans/broca-head.sha256` | `3b8de915902a5bb763e2e25695cd510ec2e4089b42128662802511f520587548` | `a9804632f2d9f05b59f2598b15e7e3fab9a415d4d474b20d3cd3c5e90742d399` |
| `test-vectors/fetch-plan-v1/plans/pre-tool-declared.sha256` | `(new)` | `bc48b4f2c4983340b29a07623125b01e4403f68a9b30791a82851e3a9a770233` |
| `test-vectors/fetch-plan-v1/plans/pre-tool-tightened.sha256` | `(new)` | `ea6ef342b64e14b976bb6b415727f6fd13d363c1292ec634b6fb104cee33845c` |
| `test-vectors/fetch-plan-v1/plans/text-only-and-hook-only.sha256` | `e5fbf58a48c082a5592aecf5b2c5a9baed6f6af505b334024c388a85df2f137a` | `5dc0739a884b5949c7f718861ddb2714eadf78240df90f8e85a2b609723cc379` |

## Digest migration (2026-10-02)

The admission differences changed shape, not plan bytes: all existing digests
remain unchanged (zero replacements). The two-phase plan adds two digests,
listed below with `(new)` because no previous SHA-256 exists. Admission JSON
is not digested. Regenerating the JSON, JCS and SHA-256 files confirms that
all existing composition and plan identities remain unchanged.

| Vector digest file | Previous SHA-256 | New SHA-256 |
|---|---|---|
| `test-vectors/fetch-plan-v1/compositions/pre-tool-two-phase.sha256` | `(new)` | `6d3ccc75c05a28ff8946ea79350e73adfde0a709bf573466ece97a22a6fef72f` |
| `test-vectors/fetch-plan-v1/plans/pre-tool-two-phase.sha256` | `(new)` | `0f3e8cf742d953197cfd2ce16a0e3a90a065b8fe22e248248384dd8d1ee8c5cf` |

## Digest migration (2026-10-02, tags)

Capability tags now follow the `cortexkit-role-tool-provider` rules at CortexKit
commons commit `42949fc331d8c318225d8ffa0faa024584237c57`: unprefixed tags
must be defined by that contract; other tags use `<namespace>:<name>/v<N>`.
Browser and desktop tools advertise both generic activities and Cerebellum
contracts. Magic Context advertises its catalog tags, including
`magic-context:archive.search/v1` for `ctx_search`; Plexus tags carry their owner
namespace. `bash_status` advertises `shell.exec/v1`. The admission mismatch remains a
mismatch using the valid namespaced tag `aft:code.read/v2`. Every omitted
existing digest is unchanged.

| Vector digest file | Previous SHA-256 | New SHA-256 |
|---|---|---|
| `test-vectors/fetch-plan-v1/compositions/broca-head-optional-text.sha256` | `87581db15a872ce717663efa4de3bbdf0dc45f214ea7937ab0240cfa2c411001` | `89fce32ac7c504941fae8067e804b8451f039f1f99c29c922a8c39ee1b374393` |
| `test-vectors/fetch-plan-v1/compositions/broca-head-plexus-direct.sha256` | `6745ef6d7d93f1f90db1ad59e1e69591aa964c61f36c8ee1336ec8bc8b14b40e` | `d2864bee49e91df3df8e4586c42de43c50f730fb72242ea8d234b7936e188e20` |
| `test-vectors/fetch-plan-v1/compositions/broca-head-plexus-no-exclude.sha256` | `c37b21128d37d00ae862445c641017489a05063c974a7e50f1d990fc201c9269` | `0ae6693756a6d3e5f659dd2efd289e1cd09f5c82fec4460ee4548c79b76805d7` |
| `test-vectors/fetch-plan-v1/compositions/broca-head.sha256` | `ffae38ca80a6cd9072f5f96c1ab883db9f196ea1335a539c8b1b51b6985d219e` | `850e48beb333c30913da7013d0bdd6b462cca7801717d00f29d7062716b4728d` |
| `test-vectors/fetch-plan-v1/plans/broca-head-optional-text.sha256` | `7ea16362dc2aef97bd44cb2f6307ae31a4558dc3743977f4a2e871f628ac0745` | `1390ef5982fc88ee6feed76befb68caabddf4ff58f39d26f4e899f2afd9bc9c5` |
| `test-vectors/fetch-plan-v1/plans/broca-head-plexus-direct.sha256` | `02a615ecee0d33bf7092c6464e5774fb32d4d1dd836ec9aeaffb294ff4be940e` | `b31a28b6c87e8d21297859420aaedc600f3dc63d541866411e6bb9ab686c077d` |
| `test-vectors/fetch-plan-v1/plans/broca-head-plexus-no-exclude.sha256` | `0684b818b71f05316650174fbd14e705a9880f2303f56fc797baed2883b1858b` | `9f9a090134809e207b977dd8637c51f2aa170aca193f5d502e64a7bf02de5d71` |
| `test-vectors/fetch-plan-v1/plans/broca-head-text-reordered.sha256` | `96f706f1fe4ce25187a5bee9b5e4eb98a0ccd1b21b294906f397b4dfd8f78473` | `f6e3694c3b2c0ea5368d38d653aa85c10fe017e5c74abe1fa00cbedb5a3e5206` |
| `test-vectors/fetch-plan-v1/plans/broca-head.sha256` | `a9804632f2d9f05b59f2598b15e7e3fab9a415d4d474b20d3cd3c5e90742d399` | `edf948ae9b6f4d56e9196060fb1be01922066e176ecce6b2df9f79edbbf14879` |

## Digest migration (2026-10-02, compaction variant)

The no-compaction vector's two digests were revised before publication from
main (MC #969). The previous values below came from its unpublished draft;
all other `.sha256` files remain unchanged. The corrected `tools-only` head
has exactly `ctx_memory`, `ctx_note`, `ctx_search`, no `ctx_reduce`, and empty
step-transform arrays in both plan and composition. These changes account
for both revised identities.

| Vector digest file | Previous SHA-256 | New SHA-256 |
|---|---|---|
| `test-vectors/fetch-plan-v1/compositions/broca-head-no-compaction.sha256` | `452675f00037839dda1ecd2eba40ab59b23f2e12693271e98fefc396849abbf0` | `70cf3fd571f311da66099550a142dac57676a64efca2a4278e624966b10bb4db` |
| `test-vectors/fetch-plan-v1/plans/broca-head-no-compaction.sha256` | `c8101447e328760143c31e8a64febf54c825858fdf5ef8f6c3796740b1e4e144` | `4c10baef822a0f7e495b2c54a1856aa33520d0781c61ac53664935aad2c0165e` |

## Digest migration (2026-10-02, archive.search)

`ctx_search` now carries Magic Context's catalog capability tag,
`magic-context:archive.search/v1`, replacing the incorrect
`magic-context:memory.search/v1`. The changed composition and plan digest files
are:

| Vector digest file | Previous SHA-256 | New SHA-256 |
|---|---|---|
| `test-vectors/fetch-plan-v1/compositions/broca-head-no-compaction.sha256` | `70cf3fd571f311da66099550a142dac57676a64efca2a4278e624966b10bb4db` | `e5d6c646c78f48fe16b1e3f3d1ee8a656f7bbd5447ba6b8b02a4837d44c4d3a7` |
| `test-vectors/fetch-plan-v1/compositions/broca-head-optional-text.sha256` | `89fce32ac7c504941fae8067e804b8451f039f1f99c29c922a8c39ee1b374393` | `16c40a6e1d7821d2ab77d529e205e9e8b6482139f08dccd2cf8e458f83088db3` |
| `test-vectors/fetch-plan-v1/compositions/broca-head-plexus-direct.sha256` | `d2864bee49e91df3df8e4586c42de43c50f730fb72242ea8d234b7936e188e20` | `10e4b33778a49a3b3f1edf528882924df56ed9d487264116b9ef2f1152d7293c` |
| `test-vectors/fetch-plan-v1/compositions/broca-head-plexus-no-exclude.sha256` | `0ae6693756a6d3e5f659dd2efd289e1cd09f5c82fec4460ee4548c79b76805d7` | `10e7dba4c5e83a07797c792a11544666730144f69782aef22206e29fd06e2e3f` |
| `test-vectors/fetch-plan-v1/compositions/broca-head.sha256` | `850e48beb333c30913da7013d0bdd6b462cca7801717d00f29d7062716b4728d` | `75ce8a5d27a17acde533684ad5343fc709369645d32190f352ca55f68446ab1d` |
| `test-vectors/fetch-plan-v1/plans/broca-head-no-compaction.sha256` | `4c10baef822a0f7e495b2c54a1856aa33520d0781c61ac53664935aad2c0165e` | `0bb0b96ca0bd0b52b1e9de16015d594a13e5054a2796ca6b6c6accffbb603822` |
| `test-vectors/fetch-plan-v1/plans/broca-head-optional-text.sha256` | `1390ef5982fc88ee6feed76befb68caabddf4ff58f39d26f4e899f2afd9bc9c5` | `2734b17785b9edb9fd6866504d3f9d4b0cf8de2a4851cc0406d97729719a18a1` |
| `test-vectors/fetch-plan-v1/plans/broca-head-plexus-direct.sha256` | `b31a28b6c87e8d21297859420aaedc600f3dc63d541866411e6bb9ab686c077d` | `a20ef5a2caf8487ae4442a7d35668907073e9f9b7cead7f4aaa4b3ea352ee749` |
| `test-vectors/fetch-plan-v1/plans/broca-head-plexus-no-exclude.sha256` | `9f9a090134809e207b977dd8637c51f2aa170aca193f5d502e64a7bf02de5d71` | `96432db5e81654d1e7a51a6fe21ee4edf806e79f7bfd94b83e22ff132d82ab0c` |
| `test-vectors/fetch-plan-v1/plans/broca-head-text-reordered.sha256` | `f6e3694c3b2c0ea5368d38d653aa85c10fe017e5c74abe1fa00cbedb5a3e5206` | `629f4780e0f7cc2bc4349959158819def7dbbea425b416aea96474cc0bd630f6` |
| `test-vectors/fetch-plan-v1/plans/broca-head.sha256` | `edf948ae9b6f4d56e9196060fb1be01922066e176ecce6b2df9f79edbbf14879` | `2fe40913b71be24cea5d6c2ece7754e66d795c124545404d2837bb9e8492625e` |

## Digest migration (2026-10-03, persona model)

Core's head persona item now names the session's model in its params,
`{"model": {"provider", "id", "variant"?}}`: a head persona varies with the
model, so the model is frozen in the plan with the item, and core refuses a
persona fetch without it as `system_text_model_required` (ALF ruling for
slice C4 of `docs/designs/extensibility-stage2-core-plan.md`; the same ruling
as Magic Context's optional model parameter). The vectors use the first
send's model, `anthropic` / `claude-opus-5` / `max`. Text items are not part
of the composition, so every composition digest is unchanged; only the plans
carrying the persona item move. The worker role item (`{"role": "mason"}`)
is unchanged.

| Vector digest file | Previous SHA-256 | New SHA-256 |
|---|---|---|
| `test-vectors/fetch-plan-v1/plans/broca-head-no-compaction.sha256` | `0bb0b96ca0bd0b52b1e9de16015d594a13e5054a2796ca6b6c6accffbb603822` | `db0a2521cae13136ee638e72a94f056a7d808c9d33b42f95be7065221fb61cb8` |
| `test-vectors/fetch-plan-v1/plans/broca-head-optional-text.sha256` | `2734b17785b9edb9fd6866504d3f9d4b0cf8de2a4851cc0406d97729719a18a1` | `e6ec77e5b0870286dffbd7cd92a45c8e9089a3d0597f6359c41c405fa34bec7e` |
| `test-vectors/fetch-plan-v1/plans/broca-head-plexus-direct.sha256` | `a20ef5a2caf8487ae4442a7d35668907073e9f9b7cead7f4aaa4b3ea352ee749` | `39a24c7921953dc34926a0e1de692e0aecfe95a1a26127210e691c763d2a5323` |
| `test-vectors/fetch-plan-v1/plans/broca-head-plexus-no-exclude.sha256` | `96432db5e81654d1e7a51a6fe21ee4edf806e79f7bfd94b83e22ff132d82ab0c` | `777a99404e14f753d86081e8076ede09903bf0a8286137822addb03c63cd01a0` |
| `test-vectors/fetch-plan-v1/plans/broca-head-text-reordered.sha256` | `629f4780e0f7cc2bc4349959158819def7dbbea425b416aea96474cc0bd630f6` | `31f9d1915244c649419135f1cd5e5bd7d2f8a099f35487006ea6e24d5003cf28` |
| `test-vectors/fetch-plan-v1/plans/broca-head.sha256` | `2fe40913b71be24cea5d6c2ece7754e66d795c124545404d2837bb9e8492625e` | `25c37a418e35db77a403316d5beb0c1a8d1b962a3f0dce3f3970ad8d7bd983b9` |

## Digest migration (2026-10-03, empty step transforms)

Core, as producer, always serializes the composition's `step_transform_items`,
as `[]` when the session has no step transforms, the same as the plan's own
list (ALF ruling for slice C5 of `docs/designs/extensibility-stage2-core-plan.md`;
consumers accept both forms). Previously `generate.py` added the member only
when non-empty, except for the no-compaction head, so the worker vectors
omitted it. Only the two worker compositions and their plans change; every
other digest is unchanged. `core-forwarding-catalog-v1/system-text.json`
(`worker-role`) carries the new worker composition digest.

| Vector digest file | Previous SHA-256 | New SHA-256 |
|---|---|---|
| `test-vectors/fetch-plan-v1/compositions/broca-worker.sha256` | `bcd1760ccd1acaebdfe45dcf9a2a7d470d96b984b669d9762854bbfc2a37ecba` | `804d43d4845feac793b051772414475648cf3abc413294b5b9b7d89fe4020b82` |
| `test-vectors/fetch-plan-v1/compositions/broca-worker-unknown-provider.sha256` | `3088f56b6e0b6b7c6697dfebde92fd5e39bcf7628bc3ea535902a2c2987985e0` | `4fc17e9e8c3e16dc8f5409ab59c3b001ee8fccfdef58e2436a62b0d4d21b3000` |
| `test-vectors/fetch-plan-v1/plans/broca-worker.sha256` | `ba9699748f435df822cd2e3d4002810d94fd168ad5c4e6e55639dc2c4c9b4746` | `c59b80a71ef2b119e3cb404ef7d32c879e4972c2d4b030af9ebe709db33ed3a9` |
| `test-vectors/fetch-plan-v1/plans/broca-worker-unknown-provider.sha256` | `170d472c9edff3597326fb5fb75d51aaa9f8697c69f9fbe48be4949ce66aebc2` | `a109bfd04344970dc97d77805099ba8c47cfa7fa9226cf26054c393bbd91958e` |

## Digest migration (2026-10-03, AFT head preset)

Broca head plans now name AFT's `head` preset for both tool and system-text
items, matching AFT's catalog. The catalog-request vector uses the same preset.
Composition digests are unchanged; the six plan identities below change, and
the generator updates their embedded copies in the send and admission vectors.

| Vector digest file | Previous SHA-256 | New SHA-256 |
|---|---|---|
| `test-vectors/fetch-plan-v1/plans/broca-head-no-compaction.sha256` | `0bb0b96ca0bd0b52b1e9de16015d594a13e5054a2796ca6b6c6accffbb603822` | `16ebc836e5973231e43c76cd0ee252dab1164f1f3f10f9cef8c68a3e8585074e` |
| `test-vectors/fetch-plan-v1/plans/broca-head-optional-text.sha256` | `2734b17785b9edb9fd6866504d3f9d4b0cf8de2a4851cc0406d97729719a18a1` | `c82f2e7196fc854bd00424882f92074c32897872027aadd733e5a0b48ddd8de5` |
| `test-vectors/fetch-plan-v1/plans/broca-head-plexus-direct.sha256` | `a20ef5a2caf8487ae4442a7d35668907073e9f9b7cead7f4aaa4b3ea352ee749` | `05b5b8fcbf89f510e6e9d2505f5e5b8d2f0e48daa445fefa77ea94afd26c69a1` |
| `test-vectors/fetch-plan-v1/plans/broca-head-plexus-no-exclude.sha256` | `96432db5e81654d1e7a51a6fe21ee4edf806e79f7bfd94b83e22ff132d82ab0c` | `97108243a6f994bb2507b3611e29c7cfcb81b5b8567483a769af2ff654b66186` |
| `test-vectors/fetch-plan-v1/plans/broca-head-text-reordered.sha256` | `629f4780e0f7cc2bc4349959158819def7dbbea425b416aea96474cc0bd630f6` | `069e00e2ed18d0dd0043edddfffb9de61252e1e858f8b1426afbe0b9e36208ca` |
| `test-vectors/fetch-plan-v1/plans/broca-head.sha256` | `edf948ae9b6f4d56e9196060fb1be01922066e176ecce6b2df9f79edbbf14879` | `36a7e4ea518f97145bb6bb8bf1a28f8ab7c67dc58ef7bfed0fc2c8e516f1ce30` |

## Digests before remote-exec policy (role presets and compaction)

Generated by `generate.py`. All Broca vectors omit compaction; every item
uses a role preset. Earlier migration tables are historical, not current pins.

| Plan vector | Plan identity SHA-256 | Composition SHA-256 |
|---|---|---|
| `broca-head` | `8f3ac238ef62c3ea5018135445beb0db6b568cdeba8674a2edadb5de03490022` | `e5d6c646c78f48fe16b1e3f3d1ee8a656f7bbd5447ba6b8b02a4837d44c4d3a7` |
| `broca-head-no-compaction` | `8f3ac238ef62c3ea5018135445beb0db6b568cdeba8674a2edadb5de03490022` | `e5d6c646c78f48fe16b1e3f3d1ee8a656f7bbd5447ba6b8b02a4837d44c4d3a7` |
| `broca-head-text-reordered` | `14531e80b0af6cc5340b03c5722a6499873cfe1fd31688ac39731c29986c8b92` | `e5d6c646c78f48fe16b1e3f3d1ee8a656f7bbd5447ba6b8b02a4837d44c4d3a7` |
| `broca-worker` | `2117ce222ad28720fcc7a4bc800fec97276c06eb3e97b0b4adea42c30ea812a7` | `804d43d4845feac793b051772414475648cf3abc413294b5b9b7d89fe4020b82` |
| `broca-worker-policy-absent` | `b205711df4fd1d968ec90c56eeca081d4f0ed55c5c46cae94bb94c2457c700c8` | `804d43d4845feac793b051772414475648cf3abc413294b5b9b7d89fe4020b82` |
| `broca-worker-policy-disabled` | `f9475fc9a4b84f93e23bc009840891e8adaf0a758c2577aa718859b6f9e6a526` | `804d43d4845feac793b051772414475648cf3abc413294b5b9b7d89fe4020b82` |
| `broca-reader` | `db88ab09d8fa1b168c25b44993adafa2e3f22b62014c337956ce0a9fafb000d2` | `437daa8dd11309877f49113a646ac30242d36975fd0aa444b218b3fb198c9f3e` |
| `text-only-and-hook-only` | `651fe89a4805ce0a88a7952cce4213b43311d6ad8981a1a4111c2bdd6885855b` | `4fbb7040e057600736798e3def78aa85af81305cb1b1d217638f7bad8457b623` |
| `broca-head-plexus-direct` | `05766eb539d5c21cada0f403049c705639be4749915803d85952485713b86785` | `9ce573eb40d0f5915ad1b1dd25a3e69c5c07713cefbdeac92f1135192367afd1` |
| `broca-head-plexus-no-exclude` | `b8445bb61d5aecf28a5c874e7c11edc0127a9db176a4906fb9160fb7860cb9f3` | `9c3f75fe0ab282e77cbb3f2da80041e425818be953ad286f7edbd65c0d5854c5` |
| `broca-head-optional-text` | `2364a41294deba1c7a283d4be2847b1e267badeac526eb584ef336e235b102b2` | `c5128fb525aa4af8f05b3a7ee015b506e83ea26820aa04d3ab1ab2b437e56723` |
| `broca-worker-unknown-provider` | `de4c8baf1e63fd761278132cffca58e398e39bf016c389854c5335fd718d5888` | `4fc17e9e8c3e16dc8f5409ab59c3b001ee8fccfdef58e2436a62b0d4d21b3000` |
| `pre-tool-declared` | `93cb52994ad60bdee3feac440532f51f2584861643b8732613f9feee6b87af1a` | `0482cc96fe3b2a69a8a81a10d43a23a18151ed81450d39c3d3d24fa82cb2189c` |
| `pre-tool-tightened` | `4bc9fbe2e74b02ac92ceac4ff7315f2fabe60ac85adf41856442bac1d5c0ae96` | `12107bb71f007a0eb61bc98ebdee4bd0c6299967ccd576fe3807a8935f350dd9` |
| `pre-tool-two-phase` | `b9f8da8516ca579ac8a026df2e319fb976aaa99390edb80f40cfb37cf926fc42` | `269eea6f25a2e32d586de78e255fa49aa6debe8ce3d5545a6be2052298e59745` |

## Digest migration (2026-10-05, remote-exec worker policy)

At that migration the worker tool item's params carried
`remote_exec: {enabled, commands}` (superseded by the explicit `bash runon`
cutover below) and `siblings: [canonical absolute path, ...]`. Core resolves them before
preflight, then freezes them on the worker's first send. AFT reads those
params, not a config file. An absent project policy adds neither field;
an explicitly disabled policy carries `enabled: false` and remains distinct.

These params change the **plan identity**, not `composition_digest`: the
composition contract covers tool names/tags, steps and compaction, not tool
item params. Every composition digest is unchanged from the table above.
Only these existing plan identities changed; all embedded send and admission
copies are regenerated by `python3 test-vectors/fetch-plan-v1/generate.py`.

| Vector digest file | Previous SHA-256 | New SHA-256 |
|---|---|---|
| `plans/broca-worker.sha256` | `b205711df4fd1d968ec90c56eeca081d4f0ed55c5c46cae94bb94c2457c700c8` | `2117ce222ad28720fcc7a4bc800fec97276c06eb3e97b0b4adea42c30ea812a7` |
| `plans/broca-worker-unknown-provider.sha256` | `aa4f2874f35f4778ee8195723b5e2a168812b2dc40610e8a96e443542f9851c8` | `de4c8baf1e63fd761278132cffca58e398e39bf016c389854c5335fd718d5888` |
| `plans/text-only-and-hook-only.sha256` | `3b4fa9d7d378b9650adfb0637100df6267bc49b10d09001b14cbb8a789985482` | `651fe89a4805ce0a88a7952cce4213b43311d6ad8981a1a4111c2bdd6885855b` |

New policy-state pins (same `broca-worker` composition):

| Plan vector | Plan identity SHA-256 |
|---|---|
| `broca-worker-policy-absent` | `b205711df4fd1d968ec90c56eeca081d4f0ed55c5c46cae94bb94c2457c700c8` |
| `broca-worker-policy-disabled` | `f9475fc9a4b84f93e23bc009840891e8adaf0a758c2577aa718859b6f9e6a526` |

## Explicit `bash runon`

Worker AFT params carry only `remote_exec: {enabled, default_demand?}` and
`siblings: [canonical absolute path, ...]`. Heads and workers use explicit
`runon: "linux"` for Linux-safe builds and tests, following AFT's bash guidance.
Command-prefix matching is retired. Core resolves
trusted authorization before preflight and freezes these params on the worker's
first send. AFT routes explicit `runon` calls. It does not read project config to
authorize a remote run. The `runon` parameter is exposed only when `enabled` is true.

Regenerated with `python3 test-vectors/fetch-plan-v1/generate.py`; add `--check`
to verify generated bytes without writing files. BROCA and MC
must re-pin the current digest table below. The worker composition includes
core's read-only `sidekick` research tool as well as the existing worker helpers.
The corresponding `.json`/`.jcs` files and `sends/broca-worker-first-send.json`
are generated together; no digest is hand-merged.

## Current digests (role presets and compaction)

Generated by `generate.py`. All Broca vectors omit compaction; every item
uses a role preset. Earlier migration tables are historical, not current pins.

| Plan vector | Plan identity SHA-256 | Composition SHA-256 |
|---|---|---|
| `broca-head` | `8f3ac238ef62c3ea5018135445beb0db6b568cdeba8674a2edadb5de03490022` | `e5d6c646c78f48fe16b1e3f3d1ee8a656f7bbd5447ba6b8b02a4837d44c4d3a7` |
| `broca-head-no-compaction` | `8f3ac238ef62c3ea5018135445beb0db6b568cdeba8674a2edadb5de03490022` | `e5d6c646c78f48fe16b1e3f3d1ee8a656f7bbd5447ba6b8b02a4837d44c4d3a7` |
| `broca-head-text-reordered` | `14531e80b0af6cc5340b03c5722a6499873cfe1fd31688ac39731c29986c8b92` | `e5d6c646c78f48fe16b1e3f3d1ee8a656f7bbd5447ba6b8b02a4837d44c4d3a7` |
| `broca-worker` | `479ae0f6a1fc32bff6bf61fa703812ca0f8065a9f185daee3a8f500721bc176a` | `ae5f5d0a9238f443ea4676e31000c9233381a4c07ee962bd941c3d51fe933fb4` |
| `broca-worker-policy-absent` | `27ca6fc3b94dd5339a1838f38e60b4ebaa695762e883f4ce4634e1b93e915de8` | `ae5f5d0a9238f443ea4676e31000c9233381a4c07ee962bd941c3d51fe933fb4` |
| `broca-worker-policy-disabled` | `a2c73f3cffcd89490d7436ca5a07c59324ac6ea2ee1e1e7d9dfb8c5c6d8ded70` | `ae5f5d0a9238f443ea4676e31000c9233381a4c07ee962bd941c3d51fe933fb4` |
| `broca-reader` | `db88ab09d8fa1b168c25b44993adafa2e3f22b62014c337956ce0a9fafb000d2` | `437daa8dd11309877f49113a646ac30242d36975fd0aa444b218b3fb198c9f3e` |
| `text-only-and-hook-only` | `c46f61d81594b3dd7fbd5161a3ed6f5c2ea97c015bee3d6c530cee7d2b89d60b` | `4fbb7040e057600736798e3def78aa85af81305cb1b1d217638f7bad8457b623` |
| `broca-head-plexus-direct` | `05766eb539d5c21cada0f403049c705639be4749915803d85952485713b86785` | `9ce573eb40d0f5915ad1b1dd25a3e69c5c07713cefbdeac92f1135192367afd1` |
| `broca-head-plexus-no-exclude` | `b8445bb61d5aecf28a5c874e7c11edc0127a9db176a4906fb9160fb7860cb9f3` | `9c3f75fe0ab282e77cbb3f2da80041e425818be953ad286f7edbd65c0d5854c5` |
| `broca-head-optional-text` | `2364a41294deba1c7a283d4be2847b1e267badeac526eb584ef336e235b102b2` | `c5128fb525aa4af8f05b3a7ee015b506e83ea26820aa04d3ab1ab2b437e56723` |
| `broca-worker-unknown-provider` | `618d80714127e85fd6181a90d4ac4a07578c8d358c2479533cbdbb226a9b0f9c` | `9255606990e8b29239d1920645b91078a9cb71d65076ede9f4b724b448cae376` |
| `pre-tool-declared` | `93cb52994ad60bdee3feac440532f51f2584861643b8732613f9feee6b87af1a` | `0482cc96fe3b2a69a8a81a10d43a23a18151ed81450d39c3d3d24fa82cb2189c` |
| `pre-tool-tightened` | `4bc9fbe2e74b02ac92ceac4ff7315f2fabe60ac85adf41856442bac1d5c0ae96` | `12107bb71f007a0eb61bc98ebdee4bd0c6299967ccd576fe3807a8935f350dd9` |
| `pre-tool-two-phase` | `b9f8da8516ca579ac8a026df2e319fb976aaa99390edb80f40cfb37cf926fc42` | `269eea6f25a2e32d586de78e255fa49aa6debe8ce3d5545a6be2052298e59745` |
| `broca-worker-optional-tool` | `2b450f884acd8844a2e564aa926f576d26321be306acf2c7400762c9ef0ea005` | `cdf0ce9ad9c935be2dcc7cf59952388bf8622d94462b5d11826f140ab221c8a3` |
| `broca-worker-optional-tool-with-text` | `7746b56b1973e0dddba047878465c37f5783043c9197fa51955ec071056afebc` | `cdf0ce9ad9c935be2dcc7cf59952388bf8622d94462b5d11826f140ab221c8a3` |
| `optional-tool-two-phase` | `9b5a011374f6b289dfd81af87a95649de1ab27c5b22a33a8874f42e772c1b4f0` | `600786dfbeacf1e5587437d75b38ab6c82a18674f9faf72e6827688c3649b8fe` |

Admitted compositions remove whole provider entries only. The two-phase text
requests are also pinned as JSON, JCS and SHA-256.

| Admitted composition / request | SHA-256 |
|---|---|
| `compositions/broca-worker-optional-tool-admitted` | `ae5f5d0a9238f443ea4676e31000c9233381a4c07ee962bd941c3d51fe933fb4` |
| `compositions/broca-worker-optional-tool-with-text-admitted` | `ae5f5d0a9238f443ea4676e31000c9233381a4c07ee962bd941c3d51fe933fb4` |
| `compositions/optional-tool-two-phase-admitted` | `0856d31b85000103ca214234dfedc2a935733d1e5b887b4757f618f88c492ab4` |
| `compositions/broca-worker-optional-tool-two-phase-admitted` | `ae5f5d0a9238f443ea4676e31000c9233381a4c07ee962bd941c3d51fe933fb4` |
| `catalog-requests/optional-tool-two-phase-text` | `2d56610d57bed3401dc053dfab6a3ee71d541b83b3c54a5efc4fa4ae3112930d` |
| `catalog-requests/optional-tool-two-phase-broca-text` | `b8a86db83398bcd744fa3b96bc3cc33e210bbc6ccdc92d73c6ac52addca2c795` |
