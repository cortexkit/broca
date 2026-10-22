//! Sessions written under the stored-ledger tag rules (`runner-tags/v1`) keep their exact bytes.
//!
//! The fixture is the WAL of a tag-rendering run on Anthropic Messages (the one family whose
//! assistant text showed its tag under those rules): a tool loop, a steer delivered while a tool
//! ran, and a model-written `§99§` at the start of its text. It was written by a build that
//! assigned and stored a number for every part. Later builds compute numbers instead, but a
//! session they find in this state must not change under a reader:
//!
//! - every request a resume rebuilds from the WAL is byte for byte the request the run sent;
//! - the transcript a read projects carries the stored numbers, part for part;
//! - a run cut mid-way and resumed by a later build finishes under the rules it was frozen with:
//!   its first request repeats the old bytes and its new records are stamped like the old ones.
//!
//! The fixture is frozen. Regenerating it with a later build would write the later rules and
//! prove nothing; `BROCA_PIN_V1_GENERATE=1` exists only to show how it was made.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use broca_core::{
    canonical_request_bytes, run_loop_on_writer_admitted_with_prior_snapshot, AllowAll,
    FileWalStore, Provider, RunControl, RunRequest, StartMode, SteerInbox, StubReply,
    StubToolPlane, WalHandle, WalStore,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput},
    provider::{NormalizedEvent, ProviderControl},
    request::Tool,
    BindIdentity, CallOptions, FinishReason, MessageId, RunId, StepId, SubmissionId, ToolCallId,
    Usage,
};
use broca_wal::{FramedRecord, WalRecord};
use serde_json::{json, Value};
use tokio::sync::mpsc;

const FAMILY: &str = broca_protocol::tags::ANTHROPIC_MESSAGES_FAMILY;
const SESSION: &str = "v1-pin";

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/runner_tags_v1")
        .join(name)
}

fn generating() -> bool {
    std::env::var_os("BROCA_PIN_V1_GENERATE").is_some()
}

/// Answers by the number of assistant turns the request already holds, so a resumed run asks
/// for, and gets, what the uninterrupted run did at that point.
struct TurnModel {
    turns: Vec<Vec<ContentBlock>>,
    calls: Mutex<Vec<CallOptions>>,
}

#[async_trait::async_trait]
impl Provider for TurnModel {
    fn render_config_blob(&self) -> Value {
        json!({"render_config": "stateless", "family_id": FAMILY})
    }

    async fn step(&self, _step: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.calls.lock().unwrap().push(opts.clone());
        let answered = opts
            .prompt
            .iter()
            .filter(|message| message.role == Role::Assistant)
            .count();
        let content = self.turns[answered.min(self.turns.len() - 1)].clone();
        let reason = if content
            .iter()
            .any(|block| matches!(block, ContentBlock::ToolCall(_)))
        {
            FinishReason::ToolCalls
        } else {
            FinishReason::Stop
        };
        let events = vec![
            NormalizedEvent::Control(ProviderControl::StreamStart { warnings: vec![] }),
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("m".into()),
                    content,
                },
            }),
            NormalizedEvent::Control(ProviderControl::StepFinished {
                step_id: StepId(0),
                usage: Usage::default(),
                finish_reason: reason,
                service_tier_used: None,
                speed_used: None,
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: reason,
                retries_used: 0,
            }),
        ];
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            for event in events {
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });
        rx
    }
}

fn text(text: &str) -> ContentBlock {
    ContentBlock::Text { text: text.into() }
}

fn call(id: &str) -> ContentBlock {
    ContentBlock::ToolCall(ToolCall {
        tool_call_id: ToolCallId(id.into()),
        tool_name: "lookup".into(),
        input: json!({"q": "tags"}),
        thought_signature: None,
    })
}

fn turns() -> Vec<Vec<ContentBlock>> {
    vec![
        vec![text("looking"), call("call-1")],
        vec![text("§99§ checking again"), call("call-2")],
        vec![text("done")],
    ]
}

fn lookup() -> Tool {
    Tool {
        name: "lookup".into(),
        description: "look something up".into(),
        input_schema: json!({"type":"object"}),
        module: None,
        execution_mode: Default::default(),
        reply: None,
    }
}

fn rendering_manifest() -> broca_wal::PlanManifest {
    broca_wal::PlanManifest {
        plan: broca_wal::OpaqueJson::from_json_text(
            json!({"step_transform_items":[],"render_tags":true}).to_string(),
        )
        .unwrap(),
        plan_identity: String::new(),
        composition_digest: String::new(),
        fetches: Vec::new(),
        tools: vec![lookup()].into(),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: Default::default(),
        declarations: Vec::new(),
    }
}

fn request() -> RunRequest {
    let submission = serde_json::from_value(json!({
        "model":{"provider_module_id":"stub","model_id":"m"},
        "input":[{"role":"user","content":[{"type":"text","text":"find the tags"}]}],
        "tools":[lookup()],
        "policy_allowed":["lookup"],
        "stop_when":[{"type":"step_count","n":8}],
    }))
    .unwrap();
    let mut request = RunRequest::from_submission(
        submission,
        RunId(format!("run-{SESSION}")),
        BindIdentity::new("/tmp/tags-v1-pin", "runner", SESSION),
        SubmissionId(format!("send-{SESSION}")),
    );
    request.submission_id = None;
    request.policy_allowed = HashSet::from(["lookup".to_owned()]);
    request
}

fn scratch(name: &str) -> PathBuf {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-tags-v1-pin-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Adds one steer to the running turn the way the session's actor does, the first time a tool
/// runs.
#[derive(Clone)]
struct SteerOnce {
    inbox: Arc<SteerInbox>,
    writer: Arc<Mutex<Option<Arc<WalHandle>>>>,
    sent: Arc<std::sync::atomic::AtomicBool>,
}

impl SteerOnce {
    fn deliver(&self) {
        if self.sent.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let writer = self.writer.lock().unwrap().clone().expect("writer set");
        let input = vec![Message::new(Role::User, vec![text("also check the docs")])];
        let record = WalRecord::Steered {
            tags: None,
            retention: None,
            ts_ms: None,
            title: None,
            send_id: None,
            request_identity_digest: None,
            delivery: Some(broca_protocol::Delivery::Steer),
            input: Arc::new(input.clone()),
            hooks: None,
        };
        assert!(self.inbox.deliver(input, || writer.record_tagged(record).1));
    }
}

struct Run {
    calls: Vec<CallOptions>,
    records: Vec<FramedRecord>,
}

async fn run(store: &FileWalStore, steer: bool) -> Run {
    run_with(store, steer, None).await
}

/// [`run`], with `ordered` (a send with an ordered `delivery` list) already queued for the turn
/// when it starts, so its first step boundary decides it.
async fn run_with(
    store: &FileWalStore,
    steer: bool,
    ordered: Option<broca_core::delivery_order::OrderedSend>,
) -> Run {
    let request = request();
    let session = request.session.clone();
    let writer = Arc::new(WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap());
    let steering = SteerOnce {
        inbox: SteerInbox::new(),
        writer: Arc::new(Mutex::new(Some(writer.clone()))),
        sent: Arc::new(std::sync::atomic::AtomicBool::new(!steer)),
    };
    if let Some(send) = ordered {
        steering.inbox.seed_ordered(vec![send]);
    }
    let on_dispatch = steering.clone();
    let plane = StubToolPlane::new().with_tool("lookup", move |_| {
        on_dispatch.deliver();
        StubReply::Output(ToolOutput::Text {
            text: "two hits".into(),
        })
    });
    let model = TurnModel {
        turns: turns(),
        calls: Mutex::new(Vec::new()),
    };
    let control = RunControl {
        manifest: Some(Arc::new(rendering_manifest())),
        steer: Some(steering.inbox.clone()),
        ..RunControl::default()
    };
    let (tx, mut rx) = mpsc::channel(4096);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop_on_writer_admitted_with_prior_snapshot(
        request,
        &writer,
        StartMode::FreshOrResume,
        &model,
        &plane,
        &AllowAll,
        store,
        None,
        control,
        &tx,
        None,
        None,
    )
    .await
    .expect("the run ends");
    drop(tx);
    drain.await.unwrap();
    assert_eq!(summary.reason, RunFinishReason::Completed);
    steering.writer.lock().unwrap().take();
    Arc::into_inner(writer)
        .expect("the test holds the last writer handle")
        .shutdown()
        .await
        .unwrap();
    let calls = model.calls.lock().unwrap().clone();
    Run {
        calls,
        records: store.read_records(&session).unwrap(),
    }
}

/// The request the production rebuild (the one a resume and cache warming use) gives for the
/// step `records[step_started]` opened.
fn rebuilt(records: &[FramedRecord], step_started: usize) -> CallOptions {
    let WalRecord::StepStarted { step_id } = records[step_started].record else {
        panic!("not a step boundary");
    };
    let mut prefix: Vec<WalRecord> = records[..=step_started]
        .iter()
        .map(|frame| frame.record.clone())
        .collect();
    prefix.push(
        serde_json::from_value(json!({
            "type":"model_attempt_finished","step_id":step_id.0,"attempt":0,
            "request_body_len":1,"request_body_sha256":"aa","response_body_len":0,
            "response_body_sha256":"bb","outcome":"complete","ts_ms":1
        }))
        .unwrap(),
    );
    broca_core::cache_warm::last_recorded_request(&prefix)
        .expect("the WAL rebuilds the step's request")
        .opts
}

fn step_boundaries(records: &[FramedRecord]) -> Vec<usize> {
    records
        .iter()
        .enumerate()
        .filter(|(_, frame)| matches!(frame.record, WalRecord::StepStarted { .. }))
        .map(|(index, _)| index)
        .collect()
}

fn request_text(opts: &CallOptions) -> String {
    String::from_utf8(canonical_request_bytes(opts)).unwrap()
}

/// What a read projects for every transcript message: its stored JSON and its part numbers.
fn transcript(records: &[FramedRecord]) -> Value {
    let records: Vec<WalRecord> = records.iter().map(|frame| frame.record.clone()).collect();
    let state = broca_core::replay(&records).unwrap();
    Value::Array(
        state
            .prompt
            .iter()
            .map(|message| {
                json!({
                    "message": serde_json::to_value(message).unwrap(),
                    "tags": message.part_tags().map(|tags| tags.parts.clone()),
                })
            })
            .collect(),
    )
}

fn wal_tags(records: &[FramedRecord]) -> Vec<u64> {
    records
        .iter()
        .flat_map(|frame| frame.record.runner_tags().to_vec())
        .flat_map(|tags| tags.parts)
        .collect()
}

/// The record a resume test cuts the fixture after: the first tool result, so the cut run has
/// a committed tool loop ahead of it and the steer still pending.
fn cut_at(records: &[FramedRecord]) -> usize {
    records
        .iter()
        .position(|frame| matches!(frame.record, WalRecord::ToolBatchFinished { .. }))
        .expect("the fixture holds a finished tool batch")
        + 1
}

fn store_with(records: &[FramedRecord], name: &str) -> FileWalStore {
    let store = FileWalStore::new(scratch(name));
    let session = request().session;
    let mut wal = store.open(&session).unwrap();
    for frame in records {
        wal.append(frame.fence, &frame.record, true).unwrap();
    }
    store
}

/// The frozen fixture's records, read through a scratch copy named for `name`: each test copies
/// into its own directory, because tests run in parallel and `scratch` clears the directory first.
fn load_fixture_records(name: &str) -> Vec<FramedRecord> {
    let store = FileWalStore::new(scratch(&format!("load-{name}")));
    let session = request().session;
    let path = store.wal_path(&session);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::copy(fixture("steer_anthropic.wal"), &path).expect("the frozen v1 WAL fixture");
    store.read_records(&session).unwrap()
}

#[tokio::test]
async fn a_session_written_under_the_stored_ledger_keeps_its_exact_bytes() {
    if generating() {
        let store = FileWalStore::new(scratch("generate"));
        let reference = run(&store, true).await;
        let dir = fixture("");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::copy(
            store.wal_path(&request().session),
            fixture("steer_anthropic.wal"),
        )
        .unwrap();
        let sent: Vec<String> = reference.calls.iter().map(request_text).collect();
        let records = &reference.records;
        let cut = cut_at(records);
        let resumed = run(&store_with(&records[..cut], "generate-resume"), false).await;
        let expected = json!({
            "sent": sent,
            "transcript": transcript(records),
            "wal_tags": wal_tags(records),
            "resumed_sent": resumed.calls.iter().map(request_text).collect::<Vec<_>>(),
            "resumed_wal_tags": wal_tags(&resumed.records),
        });
        std::fs::write(
            fixture("expected.json"),
            serde_json::to_string_pretty(&expected).unwrap() + "\n",
        )
        .unwrap();
    }
    let expected: Value =
        serde_json::from_str(&std::fs::read_to_string(fixture("expected.json")).unwrap()).unwrap();
    let records = load_fixture_records("exact-bytes");
    assert_eq!(
        json!(wal_tags(&records)),
        expected["wal_tags"],
        "the fixture's stored numbers"
    );

    // Every request a rebuild from the WAL gives is the request the run sent.
    let rebuilt: Vec<String> = step_boundaries(&records)
        .into_iter()
        .map(|at| request_text(&rebuilt(&records, at)))
        .collect();
    assert_eq!(json!(rebuilt), expected["sent"], "rebuilt requests");

    // A read projects the stored numbers on every part.
    assert_eq!(transcript(&records), expected["transcript"], "transcript");

    // A run cut mid-way resumes under the rules it was frozen with.
    let cut = cut_at(&records);
    let resumed = run(&store_with(&records[..cut], "resume"), false).await;
    assert_eq!(
        json!(resumed.calls.iter().map(request_text).collect::<Vec<_>>()),
        expected["resumed_sent"],
        "requests of the resumed run"
    );
    assert_eq!(
        json!(wal_tags(&resumed.records)),
        expected["resumed_wal_tags"],
        "numbers stamped by the resumed run"
    );
    assert!(
        resumed.records[cut..]
            .iter()
            .all(|frame| !frame.record.required_features().contains(&"runner-tags/v2")),
        "the resumed run writes nothing under the computed rules"
    );
}

/// Every request `resumed` sent is byte for byte the request a rebuild from its WAL gives for the
/// same step. `cut` is the index of the resumed run's first own record.
fn assert_resumed_matches_rebuilt(label: &str, resumed: &Run, cut: usize) {
    let steps: Vec<usize> = step_boundaries(&resumed.records)
        .into_iter()
        .filter(|at| *at >= cut)
        .collect();
    assert_eq!(
        steps.len(),
        resumed.calls.len(),
        "{label}: one step per request"
    );
    for (n, (live, at)) in resumed.calls.iter().zip(steps).enumerate() {
        assert_eq!(
            request_text(&rebuilt(&resumed.records, at)),
            request_text(live),
            "{label}: request {n} differs from its rebuild"
        );
    }
}

/// The first number the resumed run stamped on a record that `holds` matches.
fn stamped(records: &[FramedRecord], holds: impl Fn(&WalRecord) -> bool) -> u64 {
    records
        .iter()
        .filter(|frame| holds(&frame.record))
        .find_map(|frame| frame.record.runner_tags().first()?.parts.first().copied())
        .expect("the resumed run stamps the record under the stored-ledger rules")
}

/// A run frozen under the stored-ledger rules and resumed by a later build still numbers a steer
/// that joins it by its record: the queued message carries the number its `Steered` record was
/// stamped with, so every request the resumed run sends equals its rebuild and shows that number.
#[tokio::test]
async fn a_resumed_stored_ledger_run_renders_a_steer_live_as_its_rebuild() {
    let records = load_fixture_records("steer");
    let cut = cut_at(&records);
    let resumed = run(&store_with(&records[..cut], "resume-steer"), true).await;
    assert_resumed_matches_rebuilt("steer", &resumed, cut);
    let tag = stamped(&resumed.records[cut..], |record| {
        matches!(record, WalRecord::Steered { .. })
    });
    let last = request_text(
        resumed
            .calls
            .last()
            .expect("the resumed run sent a request"),
    );
    assert!(
        last.contains(&format!("§{tag}§ also check the docs")),
        "the steer shows its stamped number: {last}"
    );
}

/// The same for a queued send whose ordered `delivery` list lets it join the resumed run as a
/// steer: the delivered message carries the number its `SendDelivered` record was stamped with.
#[tokio::test]
async fn a_resumed_stored_ledger_run_renders_a_steer_delivery_live_as_its_rebuild() {
    let records = load_fixture_records("steer-delivery");
    let cut = cut_at(&records);
    let send = broca_core::delivery_order::OrderedSend {
        submission_id: SubmissionId("send-in-order".into()),
        order: broca_protocol::DeliveryOrder {
            modes: vec![broca_protocol::DeliveryMode::Steer],
            dedupe_key: None,
            expires_at_ms: None,
            template: None,
        },
        principal: None,
        input: vec![Message::new(Role::User, vec![text("a note sent in order")])],
    };
    let resumed = run_with(
        &store_with(&records[..cut], "resume-delivery"),
        false,
        Some(send),
    )
    .await;
    assert_resumed_matches_rebuilt("steer delivery", &resumed, cut);
    let tag = stamped(&resumed.records[cut..], |record| {
        matches!(record, WalRecord::SendDelivered { .. })
    });
    let first = request_text(
        resumed
            .calls
            .first()
            .expect("the resumed run sent a request"),
    );
    assert!(
        first.contains(&format!("§{tag}§ a note sent in order")),
        "the delivered send shows its stamped number: {first}"
    );
}

/// The first run on such a session after the switch runs under the computed rules
/// (`runner-tags/v2`) without renumbering anything a reader has seen: every stored number stays
/// on its part, so the transcript's earlier reads hold, and the new run's counted parts are
/// numbered from the stored ledger's high-water. Its requests show the earlier parts exactly as
/// before and the new ones by the computed rules.
#[tokio::test]
async fn the_first_run_after_the_switch_counts_on_from_the_stored_high_water() {
    let records = load_fixture_records("switch");
    let high_water = *wal_tags(&records).iter().max().expect("stored numbers");
    let before = transcript(&records);
    let store = store_with(&records, "switch");
    let leases = broca_core::FileLeaseStore::new(scratch("switch-leases"));
    let mut request = request();
    request.run_id = RunId("run-after-switch".into());
    request.input = vec![Message::new(Role::User, vec![text("next question")])];
    let model = TurnModel {
        turns: vec![
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![text("§2§ again, as §5§ said")],
        ],
        calls: Mutex::new(Vec::new()),
    };
    let plane = StubToolPlane::new();
    let (tx, mut rx) = mpsc::channel(4096);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = broca_core::run_loop_append_episode(
        request.clone(),
        &model,
        &plane,
        &leases,
        &AllowAll,
        &store,
        None,
        &tx,
    )
    .await
    .expect("the appended run ends");
    drop(tx);
    drain.await.unwrap();
    assert_eq!(summary.reason, RunFinishReason::Completed);
    let after = store.read_records(&request.session).unwrap();

    // The run froze the computed rules with the ledger's high-water as its checkpoint, and
    // stored no number.
    let started = after[records.len()..]
        .iter()
        .find_map(|frame| match &frame.record {
            WalRecord::RunStarted { config, .. } => Some(config.computed_tags),
            _ => None,
        })
        .expect("the appended run started");
    assert_eq!(
        started,
        Some(broca_wal::ComputedTagRules {
            checkpoint: high_water
        })
    );
    assert!(after[records.len()..]
        .iter()
        .all(|frame| frame.record.runner_tags().is_empty()));

    // Every earlier part keeps its stored number; the new ones count on above them.
    let now = transcript(&after);
    let earlier = before.as_array().unwrap();
    for (index, entry) in earlier.iter().enumerate() {
        assert_eq!(now[index]["tags"], entry["tags"], "message {index}");
    }
    let new: Vec<Value> = now.as_array().unwrap()[earlier.len()..]
        .iter()
        .map(|entry| entry["tags"].clone())
        .collect();
    assert_eq!(
        new,
        vec![json!([high_water + 1]), json!([high_water + 2])],
        "the prompt and the model's answer"
    );

    // The request shows the earlier parts by the stored-ledger rules and the new prompt by the
    // computed ones; the model's own notation in its answer is cleaned where it is shown.
    let sent = request_text(&model.calls.lock().unwrap()[0]);
    assert!(sent.contains("§1§ find the tags"), "{sent}");
    assert!(sent.contains("§2§ looking"), "{sent}");
    assert!(
        sent.contains(&format!("§{}§ next question", high_water + 1)),
        "{sent}"
    );
    let answer = &now.as_array().unwrap().last().unwrap()["message"];
    assert_eq!(answer["content"][0]["text"], "§2§ again, as §5§ said");
}
