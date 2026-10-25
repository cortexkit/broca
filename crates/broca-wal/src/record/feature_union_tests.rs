//! `required_features` adds the agent-run, session-class and input-cap features through
//! precomputed tables (`FeatureUnionTable`). These tests pin that every feature a record can
//! bring to one of those tables is listed in it, and that no combination of a `RunStarted`'s
//! optional fields can make `required_features` panic or list a feature twice or out of order.
//!
//! A `RunStarted` whose session had a session class and whose plan froze a provider's
//! declarations once panicked the WAL writer: the declarations feature reached the
//! session-class table, which did not list it.

use super::*;
use crate::framing::{
    AGENT_RUN_SCOPE_FEATURE, CLIENT_MESSAGE_ID_FEATURE, DEFERRED_CALL_FEATURE, FLOW_SCOPE_FEATURE,
    INPUT_TOKEN_CAP_FEATURE, MEDIA_CONTENT_FEATURE, MESSAGE_TIMES_FEATURE, PLAN_MANIFEST_FEATURE,
    PROVIDER_DECLARATIONS_FEATURE, RETENTION_FEATURE, SCOPE_FEATURE, SESSION_CLASS_FEATURE,
    STEP_TRANSFORMS_FEATURE, TOOL_TIMES_FEATURE, TURN_DIRECTIVES_FEATURE,
};
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

type Seen = BTreeMap<&'static str, BTreeSet<&'static str>>;

thread_local! {
    /// While a test observes, the names each table was asked about, keyed by the name the
    /// table adds.
    static SEEN: RefCell<Option<Seen>> = const { RefCell::new(None) };
}

/// Called by `FeatureUnionTable::lookup` in test builds with every list it is asked about.
pub(super) fn note_lookup(table: &'static str, base: &'static [&'static str]) {
    SEEN.with(|seen| {
        if let Some(seen) = seen.borrow_mut().as_mut() {
            seen.entry(table).or_default().extend(base.iter().copied());
        }
    });
}

/// The names each table was asked about while `run` ran on this thread.
fn observe(run: impl FnOnce()) -> Seen {
    SEEN.with(|seen| *seen.borrow_mut() = Some(Seen::new()));
    run();
    SEEN.with(|seen| seen.borrow_mut().take().unwrap())
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ScopeShape {
    None,
    Plain,
    Flow,
    /// A sender without a flow id or agent-run identity, which also reads as a flow scope.
    Sender,
    AgentRun,
    /// Malformed, but a reader may meet it: both a flow id and an agent-run identity.
    FlowAgentRun,
    SenderAgentRun,
}

const SCOPE_SHAPES: [ScopeShape; 7] = [
    ScopeShape::None,
    ScopeShape::Plain,
    ScopeShape::Flow,
    ScopeShape::Sender,
    ScopeShape::AgentRun,
    ScopeShape::FlowAgentRun,
    ScopeShape::SenderAgentRun,
];

impl ScopeShape {
    fn json(self) -> Option<Value> {
        let mut scope = json!({
            "owner": {"kind": "reserved", "module_id": "prefrontal-core"},
            "ref": "head-1",
            "scope_epoch": 3,
        });
        match self {
            ScopeShape::None => return None,
            ScopeShape::Plain => {}
            ScopeShape::Flow => scope["flow_id"] = json!("flow-1"),
            ScopeShape::Sender => {
                scope["sender"] = json!({"kind": "reserved", "module_id": "flow-runner"})
            }
            ScopeShape::AgentRun => {
                scope["agent_id"] = json!("agent-1");
                scope["run_id"] = json!("agent-run-1");
            }
            ScopeShape::FlowAgentRun => {
                scope["flow_id"] = json!("flow-1");
                scope["agent_id"] = json!("agent-1");
                scope["run_id"] = json!("agent-run-1");
            }
            ScopeShape::SenderAgentRun => {
                scope["sender"] = json!({"kind": "reserved", "module_id": "flow-runner"});
                scope["agent_id"] = json!("agent-1");
                scope["run_id"] = json!("agent-run-1");
            }
        }
        Some(scope)
    }

    fn agent_run(self) -> bool {
        matches!(
            self,
            ScopeShape::AgentRun | ScopeShape::FlowAgentRun | ScopeShape::SenderAgentRun
        )
    }

    fn flow(self) -> bool {
        matches!(
            self,
            ScopeShape::Flow | ScopeShape::Sender | ScopeShape::FlowAgentRun
        )
    }
}

/// One combination of the `RunStarted` fields that decide its required features.
#[derive(Clone, Copy, Debug)]
struct RunStartedShape {
    scope: ScopeShape,
    session_class: bool,
    input_cap: bool,
    retention: bool,
    manifest: bool,
    /// Only takes effect with `manifest`: the declarations live on the manifest's fetches.
    provider_declarations: bool,
    step_transforms: bool,
    client_message_id: bool,
    turn_directives: bool,
    media: bool,
    written_at: bool,
}

const RUN_STARTED_TOGGLES: u32 = 10;

fn run_started_shapes() -> impl Iterator<Item = RunStartedShape> {
    SCOPE_SHAPES.into_iter().flat_map(|scope| {
        (0..1u32 << RUN_STARTED_TOGGLES).map(move |bits| {
            let on = |bit: u32| bits & (1 << bit) != 0;
            RunStartedShape {
                scope,
                session_class: on(0),
                input_cap: on(1),
                retention: on(2),
                manifest: on(3),
                provider_declarations: on(4),
                step_transforms: on(5),
                client_message_id: on(6),
                turn_directives: on(7),
                media: on(8),
                written_at: on(9),
            }
        })
    })
}

fn media_message() -> Value {
    json!({"role": "user", "content": [{"type": "media", "kind": "image", "mime": "image/png", "byte_size": 42, "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "width": 3, "height": 2}]})
}

/// An identity digest carrying the send's `client_message_id`, as broca-core writes it.
const DIGEST_WITH_CLIENT_MESSAGE_ID: &str = r#"{"version":1,"model":"m","input":"i","tools":"t","policy_allowed":"p","tool_choice":"c","generation":"g","cache":"k","budget":"b","stop_when":"s","client_message_id":"msg-1"}"#;

fn plan_manifest(declared: bool) -> Value {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/plan_manifest_run_started.json"
    ))
    .unwrap();
    let mut manifest = fixture["manifest"].clone();
    if declared {
        manifest["fetches"][0]["declarations_frozen"] =
            json!({"result_formats": {"ask": "content/v1"}, "late_results": true});
    }
    manifest
}

fn run_started(shape: RunStartedShape, manifest: &Value, declared_manifest: &Value) -> WalRecord {
    let mut raw: Value = serde_json::from_str(
        include_str!("../../tests/fixtures/before_message_times.jsonl")
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    if let Some(scope) = shape.scope.json() {
        raw["scope"] = scope;
    }
    if shape.session_class {
        raw["session_class"] = json!("mason");
    }
    if shape.input_cap {
        raw["config"]["budget"] = json!({"max_input_tokens": 5});
    }
    if shape.retention {
        raw["retention"] = json!(3600);
    }
    if shape.manifest {
        raw["manifest"] = if shape.provider_declarations {
            declared_manifest.clone()
        } else {
            manifest.clone()
        };
    }
    if shape.step_transforms {
        raw["hooks"] = json!({"ops": []});
    }
    if shape.client_message_id {
        raw["request_identity_digest"] = json!(DIGEST_WITH_CLIENT_MESSAGE_ID);
    }
    if shape.turn_directives {
        raw["config"]["turn_directives_v1"] = json!(["wake"]);
    }
    if shape.media {
        raw["input"] = json!([media_message()]);
    }
    if shape.written_at {
        raw["written_at_ms"] = json!(42);
    }
    serde_json::from_value(raw).unwrap_or_else(|error| panic!("{shape:?}: {error}"))
}

/// The features a `RunStarted` of this shape requires, computed field by field without the
/// tables.
fn naive_run_started_features(shape: RunStartedShape) -> Vec<&'static str> {
    let mut names = BTreeSet::new();
    if shape.scope != ScopeShape::None {
        names.insert(SCOPE_FEATURE);
    }
    if shape.scope.flow() {
        names.insert(FLOW_SCOPE_FEATURE);
    }
    if shape.scope.agent_run() {
        names.insert(AGENT_RUN_SCOPE_FEATURE);
    }
    let toggles = [
        (shape.session_class, SESSION_CLASS_FEATURE),
        (shape.input_cap, INPUT_TOKEN_CAP_FEATURE),
        (shape.retention, RETENTION_FEATURE),
        (shape.manifest, PLAN_MANIFEST_FEATURE),
        (
            shape.manifest && shape.provider_declarations,
            PROVIDER_DECLARATIONS_FEATURE,
        ),
        (shape.step_transforms, STEP_TRANSFORMS_FEATURE),
        (shape.client_message_id, CLIENT_MESSAGE_ID_FEATURE),
        (shape.turn_directives, TURN_DIRECTIVES_FEATURE),
        (shape.media, MEDIA_CONTENT_FEATURE),
        (shape.written_at, MESSAGE_TIMES_FEATURE),
    ];
    for (on, feature) in toggles {
        if on {
            names.insert(feature);
        }
    }
    names.into_iter().collect()
}

fn tool_call_deferred(scope: ScopeShape, deferred_at: bool) -> WalRecord {
    let mut raw = json!({"type":"tool_call_deferred","batch_id":"r-b1","call_id":"t1","executor":"broca","tool":"edit","args_jcs":"{\"f\":\"a\"}","args_hash":"910b42ae2f4a973d3af5f056c18c62742efb4f7c4af344444e47e071411d6e57","ask":{"prompt":"Edit a?","expires_at_ms":1790000723000_u64,"on_expiry":"deny","material_damage":false,"late_execution":"notify_only"}});
    raw["scope"] = scope
        .json()
        .expect("a deferred call always carries a scope");
    if deferred_at {
        raw["deferred_at_ms"] = json!(42);
    }
    serde_json::from_value(raw).unwrap()
}

/// Every record shape that reaches at least one table, each optional field that adds a
/// feature combined with the others. `RunStarted` reaches all three tables; the others reach
/// the agent-run table (`ToolCallDeferred`) or the input-cap table.
fn table_reaching_records() -> Vec<WalRecord> {
    let manifest = plan_manifest(false);
    let declared = plan_manifest(true);
    let mut records: Vec<WalRecord> = run_started_shapes()
        .map(|shape| run_started(shape, &manifest, &declared))
        .collect();
    for scope in SCOPE_SHAPES {
        if scope != ScopeShape::None {
            records.push(tool_call_deferred(scope, false));
            records.push(tool_call_deferred(scope, true));
        }
    }
    for bits in 0..1u32 << 6 {
        let on = |bit: u32| bits & (1 << bit) != 0;
        let mut raw = json!({"type":"queued","submission_id":"sub-run-8","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[],"budget":{"max_input_tokens":5}}});
        if on(0) {
            raw["hooks"] = json!({"ops": []});
        }
        if on(1) {
            raw["retention"] = json!(3600);
        }
        if on(2) {
            raw["request"]["directives"] = json!(["wake"]);
        }
        if on(3) {
            raw["request"]["input"] = json!([media_message()]);
        }
        if on(4) {
            raw["request_identity_digest"] = json!(DIGEST_WITH_CLIENT_MESSAGE_ID);
        }
        if on(5) {
            raw["written_at_ms"] = json!(42);
        }
        records.push(serde_json::from_value(raw).unwrap());
    }
    for bits in 0..1u32 << 4 {
        let on = |bit: u32| bits & (1 << bit) != 0;
        let mut raw = json!({"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"text","text":"done"},"is_error":false},"outcome":"completed","reason":"budget_exhausted"});
        if on(0) {
            raw["hooks"] = json!({"ops": []});
        }
        if on(1) {
            raw["provider_held"] = json!({"call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:9","module":"plexus","elicitation_id":"el-2"});
        }
        if on(2) {
            let mut media = media_message()["content"][0].clone();
            media.as_object_mut().unwrap().remove("type");
            raw["result"]["output"] =
                json!({"kind": "parts", "parts": [{"kind": "media", "media": media}]});
        }
        if on(3) {
            raw["finished_at_ms"] = json!(42);
        }
        records.push(serde_json::from_value(raw).unwrap());
    }
    for interrupt_reason in [None, Some("runner_shutdown")] {
        let mut raw =
            json!({"type":"run_finished","reason":"budget_exhausted","usage":{},"retries_used":0});
        if let Some(reason) = interrupt_reason {
            raw["interrupt_reason"] = json!(reason);
        }
        records.push(serde_json::from_value(raw).unwrap());
    }
    for raw in [
        r#"{"type":"input_token_cap_frozen","limit":150}"#,
        r#"{"type":"input_token_budget_exhausted","detail":{"limit":150,"spent":150,"cause":"limit_reached"}}"#,
    ] {
        records.push(serde_json::from_str(raw).unwrap());
    }
    records
}

/// Every name a record brings to a table is one that table lists, so no record takes the
/// slower fallback. Pins `required_features`' rule that only `base_required_features` and the
/// earlier tables feed a table: a feature added before a table, like the provider
/// declarations that once reached the session-class table, shows up here by name.
#[test]
fn every_feature_a_record_brings_to_a_union_table_is_listed_in_it() {
    let records = table_reaching_records();
    let seen = observe(|| {
        for record in &records {
            record.required_features();
        }
    });
    let tables: [(&str, &[&str]); 3] = [
        (AGENT_RUN_SCOPE_FEATURE, &UNION_TABLE_BASE_FEATURES),
        (SESSION_CLASS_FEATURE, &CLASS_BASE_FEATURES),
        (INPUT_TOKEN_CAP_FEATURE, &INPUT_CAP_BASE_FEATURES),
    ];
    for (table, listed) in tables {
        let seen = seen
            .get(table)
            .unwrap_or_else(|| panic!("no record reached the {table} table"));
        let unlisted: Vec<_> = seen.iter().filter(|name| !listed.contains(name)).collect();
        assert!(
            unlisted.is_empty(),
            "records bring {unlisted:?} to the {table} table, which does not list them: add the \
             feature after the tables in `required_features`, or list it in \
             `UNION_TABLE_BASE_FEATURES`"
        );
    }
    // The records reach every name each table lists, so the check above saw each table's
    // whole input. A `RunStarted` never carries `deferred-call/v1`, the one listed name the
    // session-class table never meets.
    let set = |names: &[&'static str]| names.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(
        seen[AGENT_RUN_SCOPE_FEATURE],
        set(&UNION_TABLE_BASE_FEATURES)
    );
    let mut class = set(&CLASS_BASE_FEATURES);
    class.remove(DEFERRED_CALL_FEATURE);
    assert_eq!(seen[SESSION_CLASS_FEATURE], class);
    assert_eq!(seen[INPUT_TOKEN_CAP_FEATURE], set(&INPUT_CAP_BASE_FEATURES));
}

/// No combination of session class, agent-run scope, input cap, scope, flow scope, retention,
/// plan manifest, provider declarations, step transforms, client message id, turn directives,
/// media and message times can panic `required_features`, and each yields a sorted,
/// duplicate-free list equal to the one computed field by field. Deferred calls, the other
/// record that reaches the agent-run table, are covered alongside.
#[test]
fn every_run_started_shape_requires_a_sorted_duplicate_free_naive_union() {
    let manifest = plan_manifest(false);
    let declared = plan_manifest(true);
    let mut checked = 0;
    for shape in run_started_shapes() {
        let record = run_started(shape, &manifest, &declared);
        let features =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| record.required_features()))
                .unwrap_or_else(|_| panic!("required_features panicked for {shape:?}"));
        assert!(
            features.windows(2).all(|pair| pair[0] < pair[1]),
            "{shape:?}: {features:?} is not sorted and duplicate-free"
        );
        assert_eq!(features, naive_run_started_features(shape), "{shape:?}");
        checked += 1;
    }
    assert_eq!(checked, SCOPE_SHAPES.len() << RUN_STARTED_TOGGLES);

    for scope in SCOPE_SHAPES {
        if scope == ScopeShape::None {
            continue;
        }
        for deferred_at in [false, true] {
            let mut expected = vec![DEFERRED_CALL_FEATURE];
            if scope.agent_run() {
                expected.insert(0, AGENT_RUN_SCOPE_FEATURE);
            }
            if deferred_at {
                expected.push(TOOL_TIMES_FEATURE);
            }
            assert_eq!(
                tool_call_deferred(scope, deferred_at).required_features(),
                expected,
                "{scope:?} deferred_at={deferred_at}"
            );
        }
    }
}

/// Each table answers any sorted list, including names it does not list (a feature added
/// after it was written, or a name only a later table adds), with the sorted union instead of
/// panicking.
#[test]
fn every_table_answers_any_sorted_list_with_the_sorted_union() {
    let mut universe: Vec<&'static str> = INPUT_CAP_BASE_FEATURES.to_vec();
    universe.extend([
        INPUT_TOKEN_CAP_FEATURE,
        PROVIDER_DECLARATIONS_FEATURE,
        CLIENT_MESSAGE_ID_FEATURE,
        MESSAGE_TIMES_FEATURE,
        // A feature that does not exist yet.
        "plan-reference-slots/v1",
    ]);
    universe.sort_unstable();
    type Lookup = fn(&'static [&'static str]) -> &'static [&'static str];
    let tables: [(&str, Lookup); 3] = [
        (AGENT_RUN_SCOPE_FEATURE, agent_run_features),
        (SESSION_CLASS_FEATURE, session_class_features),
        (INPUT_TOKEN_CAP_FEATURE, input_cap_features),
    ];
    for mask in 0..1u32 << universe.len() {
        let base: Vec<&'static str> = universe
            .iter()
            .enumerate()
            .filter(|(bit, _)| mask & (1 << bit) != 0)
            .map(|(_, name)| *name)
            .collect();
        let base: &'static [&'static str] = Box::leak(base.into_boxed_slice());
        for (added, lookup) in tables {
            let mut expected: BTreeSet<&str> = base.iter().copied().collect();
            expected.insert(added);
            let expected: Vec<&str> = expected.into_iter().collect();
            assert_eq!(lookup(base), expected, "{added} with {base:?}");
        }
    }
}

/// The generated tables list exactly the shared list plus the names the earlier tables add,
/// in sorted order, as the tables they replaced did.
#[test]
fn every_table_is_generated_from_the_one_shared_list() {
    assert_eq!(
        UNION_TABLE_BASE_FEATURES,
        [
            DEFERRED_CALL_FEATURE,
            FLOW_SCOPE_FEATURE,
            PLAN_MANIFEST_FEATURE,
            RETENTION_FEATURE,
            SCOPE_FEATURE,
            STEP_TRANSFORMS_FEATURE,
        ]
    );
    assert_eq!(
        CLASS_BASE_FEATURES,
        [
            AGENT_RUN_SCOPE_FEATURE,
            DEFERRED_CALL_FEATURE,
            FLOW_SCOPE_FEATURE,
            PLAN_MANIFEST_FEATURE,
            RETENTION_FEATURE,
            SCOPE_FEATURE,
            STEP_TRANSFORMS_FEATURE,
        ]
    );
    assert_eq!(
        INPUT_CAP_BASE_FEATURES,
        [
            AGENT_RUN_SCOPE_FEATURE,
            DEFERRED_CALL_FEATURE,
            FLOW_SCOPE_FEATURE,
            PLAN_MANIFEST_FEATURE,
            RETENTION_FEATURE,
            SCOPE_FEATURE,
            SESSION_CLASS_FEATURE,
            STEP_TRANSFORMS_FEATURE,
        ]
    );
    assert_eq!(
        input_cap_features(&[AGENT_RUN_SCOPE_FEATURE, SESSION_CLASS_FEATURE]),
        [
            AGENT_RUN_SCOPE_FEATURE,
            INPUT_TOKEN_CAP_FEATURE,
            SESSION_CLASS_FEATURE
        ]
    );
}
