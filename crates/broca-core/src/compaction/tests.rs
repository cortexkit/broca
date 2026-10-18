use super::*;
use crate::compaction_provider::{
    setup::{SetupAnswer, SetupRequest},
    status::{Estimate, MessageRef},
};
use broca_protocol::{BindIdentity, CallOptions, FinishReason, ModelHandle, RunFinishReason};
use broca_wal::{FramedRecord, Wal, WalError};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

fn text(role: Role, text: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: text.into() }])
}
fn raw() -> Vec<Message> {
    let mut raw = vec![
        text(Role::System, "system"),
        text(Role::User, "user"),
        text(Role::Assistant, "answer"),
        text(Role::User, "next"),
    ];
    broca_protocol::content::stamp_ordinals(&mut raw);
    raw
}
fn compaction(replacement: Vec<Value>) -> CompactionMessage {
    CompactionMessage::new("c", 2, Range::new(0, 3), replacement)
}
fn fence() -> FenceState {
    FenceState {
        newest_request_id: "r".into(),
        last_applied_version: Some(1),
        newest_ordinal: Some(3),
        newest_deadline_ms: Some(100),
    }
}
fn answer(message: CompactionMessage) -> StepAnswer {
    StepAnswer::CompactionMessage {
        request_id: "r".into(),
        compaction: message,
        coverage: None,
        channel2_directive: None,
        bootstrap_complete: None,
    }
}
fn rejected(message: CompactionMessage) {
    assert_eq!(
        validate_answer(&fence(), &answer(message), 99, &raw()),
        Err(CompactionDisposition::Structural)
    );
}
#[test]
fn structural_rule_cache_prefix_is_refused() {
    rejected(compaction(vec![
        json!({"role":"user","cache_prefix_blocks":1,"content":[{"type":"text","text":"head"}]} ),
    ]));
}
#[test]
fn structural_rule_origin_fidelity_is_refused() {
    let mut original = text(Role::Assistant, "signed model answer");
    original.origin = Some(ModelHandle::new("p", "m"));
    let mut raw = vec![original.clone(), text(Role::User, "tail")];
    broca_protocol::content::stamp_ordinals(&mut raw);
    let mut carried = original.clone();
    carried.origin = None;
    let message = CompactionMessage::new("c", 2, Range::new(0, 1), vec![encoded(&carried)]);
    assert_eq!(
        validate_answer(&fence(), &answer(message), 99, &raw),
        Err(CompactionDisposition::Structural)
    );
    let message = CompactionMessage::new("c", 2, Range::new(0, 1), vec![encoded(&original)]);
    assert!(validate_answer(&fence(), &answer(message), 99, &raw).is_ok());
}
#[test]
fn structural_rule_authored_signed_reasoning_is_refused() {
    for block in [
        json!({"type":"reasoning","text":"thinking","signature":"signed"}),
        json!({"type":"redacted_reasoning","data":"opaque"}),
    ] {
        rejected(compaction(vec![
            json!({"role":"assistant","content":[block]}),
        ]));
    }
}

#[test]
fn text_provider_state_can_only_be_carried_as_an_unchanged_original() {
    let original: Message = serde_json::from_value(json!({"role":"assistant","origin":{"provider_module_id":"p","model_id":"m"},"content":[{"type":"text_with_state","text":"answer","provider_state":{"format":"gemini","data":"opaque"}}]})).unwrap();
    let raw = vec![original.clone(), text(Role::User, "tail")];
    let carried = CompactionMessage::new("c", 2, Range::new(0, 1), vec![encoded(&original)]);
    assert!(validate_answer(&fence(), &answer(carried), 99, &raw).is_ok());
    let mut forged = original.clone();
    forged.origin = None;
    if let ContentBlock::TextWithState { text, .. } = &mut forged.content[0] {
        text.push_str(" rewritten");
    }
    let changed = CompactionMessage::new("c", 2, Range::new(0, 1), vec![encoded(&forged)]);
    assert_eq!(
        validate_answer(&fence(), &answer(changed), 99, &raw),
        Err(CompactionDisposition::Structural)
    );
}
#[test]
fn structural_rule_system_is_refused() {
    rejected(compaction(vec![encoded(&text(Role::System, "head"))]));
}
fn call(id: &str) -> ContentBlock {
    ContentBlock::ToolCall(broca_protocol::ToolCall {
        tool_call_id: broca_protocol::ToolCallId(id.into()),
        tool_name: "t".into(),
        input: json!({}),
        thought_signature: None,
    })
}
fn result(id: &str) -> ContentBlock {
    ContentBlock::ToolResult(broca_protocol::ToolResult {
        tool_call_id: broca_protocol::ToolCallId(id.into()),
        output: broca_protocol::ToolOutput::Text {
            text: "done".into(),
        },
        is_error: false,
    })
}
#[test]
fn structural_rule_tool_pair_and_range_integrity_is_refused() {
    let mut raw = vec![
        Message::new(Role::Assistant, vec![call("id")]),
        Message::new(Role::Tool, vec![result("id")]),
        text(Role::User, "tail"),
    ];
    broca_protocol::content::stamp_ordinals(&mut raw);
    for message in [
        CompactionMessage::new("c", 2, Range::new(0, 1), vec![]),
        CompactionMessage::new(
            "c",
            2,
            Range::new(0, 2),
            vec![encoded(&Message::new(Role::Tool, vec![result("id")]))],
        ),
        CompactionMessage::new(
            "c",
            2,
            Range::new(0, 2),
            vec![encoded(&Message::new(Role::Assistant, vec![call("id")]))],
        ),
        CompactionMessage::new(
            "c",
            2,
            Range::new(0, 0),
            vec![
                encoded(&Message::new(Role::Assistant, vec![call("id")])),
                encoded(&Message::new(Role::Tool, vec![result("id")])),
            ],
        ),
    ] {
        assert_eq!(
            validate_answer(&fence(), &answer(message), 99, &raw),
            Err(CompactionDisposition::Structural)
        );
    }
    let valid = CompactionMessage::new(
        "c",
        2,
        Range::new(0, 2),
        vec![encoded(&raw[0]), encoded(&raw[1])],
    );
    assert!(validate_answer(&fence(), &answer(valid), 99, &raw).is_ok());
}
#[test]
fn structural_rule_unknown_native_fields_are_refused() {
    for value in [
        json!({"role":"user","content":[],"mystery":1}),
        json!({"role":"user","content":[{"type":"image","data":"x"}]}),
        json!({"role":"user","content":[{"type":"text","text":"x","mystery":1}]}),
    ] {
        rejected(compaction(vec![value]));
    }
}
#[test]
fn fence_order_size_deadline_version_and_range_are_pinned() {
    let mut oversized = compaction(vec![encoded(&text(
        Role::User,
        &"x".repeat(MAX_REPLACEMENT_BYTES),
    ))]);
    oversized.range = Range::new(10, 0);
    let mut stale = answer(oversized.clone());
    if let StepAnswer::CompactionMessage { request_id, .. } = &mut stale {
        *request_id = "old".into();
    }
    assert_eq!(
        validate_answer(&fence(), &stale, 100, &raw()),
        Err(CompactionDisposition::SupersededRequest)
    );
    assert_eq!(
        validate_answer(&fence(), &answer(oversized.clone()), 100, &raw()),
        Err(CompactionDisposition::Late)
    );
    assert_eq!(
        validate_answer(&fence(), &answer(oversized), 99, &raw()),
        Err(CompactionDisposition::Structural)
    );
    let mut message = compaction(vec![]);
    message.range = Range::new(9, 8);
    message.version = 1;
    assert_eq!(
        validate_answer(&fence(), &answer(message.clone()), 99, &raw()),
        Err(CompactionDisposition::RangeInverted)
    );
    message.range = Range::new(0, 8);
    assert_eq!(
        validate_answer(&fence(), &answer(message.clone()), 99, &raw()),
        Err(CompactionDisposition::StaleVersion)
    );
    message.version = 2;
    assert_eq!(
        validate_answer(&fence(), &answer(message), 99, &raw()),
        Err(CompactionDisposition::RangeBeyondNewest)
    );
}

#[test]
fn replacement_ceiling_is_eight_mib_independent_of_configuration() {
    // The fixture must not derive its boundary from the implementation's constant:
    // increasing that constant must make the real validator accept this oversized input.
    let message = compaction(vec![encoded(&text(
        Role::User,
        &"x".repeat(8 * 1024 * 1024),
    ))]);
    assert_eq!(
        validate_answer(&fence(), &answer(message), 99, &raw()),
        Err(CompactionDisposition::Structural)
    );
}

#[test]
fn inverted_range_precedes_invalid_native_content() {
    let mut message = compaction(vec![
        json!({"role":"user","content":[{"type":"text","text":"summary"}],"future":true}),
    ]);
    message.range = Range::new(3, 2);
    assert_eq!(
        validate_answer(&fence(), &answer(message), 99, &raw()),
        Err(CompactionDisposition::RangeInverted)
    );
}
#[test]
fn compaction_margin_is_strict_and_unknown_window_fails_closed() {
    assert!(estimate_fits(Some(899), Some(1000)));
    assert!(!estimate_fits(Some(900), Some(1000)));
    assert!(!estimate_fits(Some(950), Some(1000)));
    assert!(!estimate_fits(Some(100), None));
    assert!(!estimate_fits(None, Some(1000)));
    assert_eq!(estimate_tokens(101, None), 51);
    assert_eq!(estimate_tokens(120, Some((100, 100))), 110);
}
#[test]
fn scheduling_uses_fill_and_shared_model_overrides() {
    let mut state = CompactionState {
        call_when: Some(
            OpaqueJson::from_json_text(r#"{"default":0.8,"models":{"m*":0.7,"model":0.6}}"#)
                .unwrap(),
        ),
        ..Default::default()
    };
    let usage = Usage {
        input_tokens: Some(100),
        cached_input_tokens: Some(400),
        cache_write_tokens: Some(100),
        ..Usage::default()
    };
    assert!(state.should_call("model", Some(usage), Some(1000), false, false));
    assert!(!state.should_call("model", Some(usage), Some(1001), false, false));
    assert!(!state.should_call("other", Some(usage), Some(1000), false, false));
    assert!(state.should_call("other", None, None, true, false));
    assert!(state.should_call("other", None, None, false, true));
    state.call_when = Some(OpaqueJson::from_json_text(r#"{"default":0.8,"future":1}"#).unwrap());
    assert!(state.should_call("other", None, None, false, false));
}
#[derive(Clone, Default)]
struct Shared {
    records: Arc<Mutex<Vec<WalRecord>>>,
    durable: Arc<AtomicUsize>,
    lineage: Arc<Mutex<Option<broca_wal::WalLineageId>>>,
    omit_lineage: bool,
}

fn fixture_lineage_id() -> broca_wal::WalLineageId {
    static LINEAGE: std::sync::OnceLock<broca_wal::WalLineageId> = std::sync::OnceLock::new();
    *LINEAGE.get_or_init(|| {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target")
            .join(format!("compaction-lineage-{}.wal", std::process::id()));
        let wal = broca_wal::FileWal::open(&path).unwrap();
        let lineage = wal.lineage_id().unwrap();
        drop(wal);
        std::fs::remove_file(path).unwrap();
        lineage
    })
}

impl Shared {
    fn frames(&self) -> Vec<FramedRecord> {
        self.records
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, record)| FramedRecord {
                seq: i as u64 + 1,
                fence: 1,
                record,
            })
            .collect()
    }
    fn writer(&self) -> crate::WalHandle {
        crate::WalHandle::spawn(Box::new(self.clone()), 1).unwrap()
    }
}
impl Wal for Shared {
    fn append(&mut self, _: u64, record: &WalRecord, _: bool) -> Result<u64, WalError> {
        let mut records = self.records.lock().unwrap();
        records.push(record.clone());
        if !self.omit_lineage {
            // Mirror FileWal's legacy upgrade: identity appears only after an append.
            self.lineage
                .lock()
                .unwrap()
                .get_or_insert_with(fixture_lineage_id);
        }
        Ok(records.len() as u64)
    }
    fn sync(&self) -> Result<(), WalError> {
        self.durable
            .store(self.records.lock().unwrap().len(), Ordering::SeqCst);
        Ok(())
    }
    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        Ok(self.frames())
    }
    fn tail_after_seq(&self, after: u64) -> Result<Vec<FramedRecord>, WalError> {
        Ok(self
            .frames()
            .into_iter()
            .filter(|frame| frame.seq > after)
            .collect())
    }
    fn last_seq(&self) -> u64 {
        self.records.lock().unwrap().len() as u64
    }
    fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        *self.lineage.lock().unwrap()
    }
}
impl crate::WalStore for Shared {
    fn open(&self, _: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        Ok(Box::new(self.clone()))
    }
    fn read_records(&self, _: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        Ok(self.frames())
    }
}
struct Scripted {
    setup: Value,
    steps: Mutex<std::collections::VecDeque<Value>>,
    setups: AtomicUsize,
    setup_requests: Mutex<Vec<SetupRequest>>,
    statuses: Mutex<Vec<StepStatus>>,
    shared: Shared,
}
impl Scripted {
    fn new(shared: &Shared, setup: Value, steps: Vec<Value>) -> Arc<Self> {
        Arc::new(Self {
            setup,
            steps: Mutex::new(steps.into()),
            setups: AtomicUsize::new(0),
            setup_requests: Mutex::new(Vec::new()),
            statuses: Mutex::new(Vec::new()),
            shared: shared.clone(),
        })
    }
}
#[async_trait::async_trait]
impl CompactionPlane for Scripted {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        request: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        self.setups.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.harness, "caller");
        self.setup_requests.lock().unwrap().push(request.clone());
        let mut value = self.setup.clone();
        value["request_id"] = request.request_id.into();
        if value["answer"] == "never" {
            return std::future::pending().await;
        }
        serde_json::from_value(value).map_err(|_| CompactionUnavailable)
    }
    async fn step(
        &self,
        _: &str,
        _: &BindIdentity,
        status: StepStatus,
    ) -> Result<StepAnswer, CompactionUnavailable> {
        {
            let records = self.shared.records.lock().unwrap();
            assert_eq!(
                self.shared.durable.load(Ordering::SeqCst),
                records.len(),
                "issued request is durable before route call"
            );
            assert!(
                matches!(records.last(), Some(WalRecord::CompactionRequest { request_id, .. }) if request_id == &status.request_id)
            );
        }
        self.statuses.lock().unwrap().push(status.clone());
        let mut value = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| json!({"answer":"noop"}));
        if value.get("request_id").is_none() {
            value["request_id"] = status.request_id.into();
        }
        if value["answer"] == "never" {
            return std::future::pending().await;
        }
        serde_json::from_value(value).map_err(|_| CompactionUnavailable)
    }
}
fn ready() -> Value {
    json!({"answer":"ready","initial":{"compaction_id":"c","version":1,"range":{"from":0,"to":0},"replacement":[{"role":"user","content":[{"type":"text","text":"head"}]}]},"stability":[{"index":0,"rank":7}]})
}
fn bind() -> BindIdentity {
    BindIdentity::new(std::path::PathBuf::from("/project"), "caller", "session")
}
fn status() -> StepStatus {
    StepStatus::new(
        "session",
        "caller",
        "",
        "lineage",
        "1",
        "user_turn",
        "model",
        Estimate::new(1),
        1,
    )
    .with_model_details(None, Some(1000), None)
}

#[test]
fn broca_step_status_keeps_its_pre_lane_wire_bytes() {
    let mut status = status();
    status.request_id = "lineage:compaction:4".into();
    let mut messages = vec![text(Role::User, "hello")];
    broca_protocol::content::stamp_ordinals(&mut messages);
    CompactionState::default().fill_status(&mut status, &messages);

    // Broca does not meet both Claude Code admission conditions: attested `reserved:thalamus`
    // identity and a plan-declared capability. Its status must serialize to the exact bytes
    // asserted below, without the new optional members.
    // Keep `geometry`, `output_reserve`, `mid_turn`, `fill`, `channel2_delivered_id`,
    // `channel2_receipt`, and `raw_message` absent.
    assert_eq!(
        serde_json::to_vec(&status).unwrap(),
        br#"{"session":"session","harness":"caller","request_id":"lineage:compaction:4","lineage_id":"lineage","step_id":"1","step_kind":"user_turn","model":"model","context_window":1000,"estimate":{"request_tokens":1},"newest":{"ordinal":0,"mid":"m0"},"messages":[{"ordinal":0,"mid":"m0","message":{"content":[{"text":"hello","type":"text"}],"role":"user"}}],"now":1}"#
    );
}

#[tokio::test]
async fn claude_code_answer_fields_decode_but_do_not_change_broca_apply_or_wal() {
    let replacement = json!([{"role":"user","content":[{"type":"text","text":"summary"}]}]);
    let plain = json!({"answer":"compaction_message","compaction":{
        "compaction_id":"c","version":2,"range":{"from":0,"to":0},
        "replacement":replacement
    }});
    let digest = "a".repeat(64);
    let view = json!({
        "view_id":"view-2","version":2,"byte_length":17,"sha256":digest,
    });
    let mut with_lane_fields = plain.clone();
    with_lane_fields["coverage"] = json!({
        "end_mid":"m0","ordinal":0,
        "endpoint":{"message_id":"m0","part_index":0}
    });
    with_lane_fields["compaction"]["raw_view"] = json!({
        "view_id":"view-2","version":2,"byte_length":17,"sha256":digest,
        "profile":"profile","profile_generation":"profile-generation",
        "provider_generation":"provider-generation","lineage_id":"lineage",
        "endpoint":{"message_id":"m0","part_index":0},
        "payload":{"kind":"reference","view":view}
    });
    with_lane_fields["channel2_directive"] = json!({
        "directive_id":"notice-1","text":"notice","armed_at_ms":10
    });
    with_lane_fields["request_id"] = json!("r");

    match serde_json::from_value::<StepAnswer>(with_lane_fields.clone()).unwrap() {
        StepAnswer::CompactionMessage {
            compaction,
            coverage: Some(coverage),
            channel2_directive: Some(directive),
            ..
        } => {
            assert_eq!(coverage.end_mid, "m0");
            let endpoint = coverage.endpoint.as_ref().expect("typed coverage endpoint");
            assert_eq!(
                (endpoint.message_id.as_str(), endpoint.part_index),
                ("m0", 0)
            );
            assert_eq!(
                compaction
                    .raw_view
                    .as_ref()
                    .expect("frozen view reference")
                    .view
                    .sha256,
                digest
            );
            assert_eq!(directive.directive_id, "notice-1");
        }
        answer => panic!("unexpected answer: {answer:?}"),
    }

    async fn apply(mut answer: Value) -> Vec<WalRecord> {
        answer.as_object_mut().unwrap().remove("request_id");
        let shared = Shared::default();
        let writer = shared.writer();
        let control = crate::RunControl::default();
        let bind = bind();
        let scripted = Scripted::new(&shared, ready(), vec![answer]);
        let plane: Arc<dyn CompactionPlane> = scripted;
        let replies = runtime::CompactionReplies::new();
        let runtime = runtime::Runtime {
            provider: "provider",
            bind: &bind,
            plane: Some(&plane),
            inbox: None,
            writer: &writer,
            control: &control,
            replies: &replies,
        };
        let mut state = runtime.setup(setup(), &raw()).await.unwrap();
        assert!(matches!(
            runtime
                .step(&mut state, status(), &raw(), Some(10), None)
                .await
                .unwrap(),
            runtime::StepResult::Continue
        ));
        let records = shared.records.lock().unwrap().clone();
        let _ = writer.shutdown().await;
        records
    }

    let plain_records = apply(plain).await;
    let lane_records = apply(with_lane_fields).await;
    let applied_replacement = |records: &[WalRecord]| {
        records
            .iter()
            .find_map(|record| match record {
                WalRecord::CompactionMessage {
                    kind: CompactionMessageKind::Step,
                    replacement,
                    ..
                } => Some((**replacement).clone()),
                _ => None,
            })
            .expect("the step answer was applied")
    };
    assert_eq!(
        applied_replacement(&plain_records),
        applied_replacement(&lane_records)
    );
    assert_eq!(plain_records, lane_records);
}

fn setup() -> SetupRequest {
    SetupRequest::new("session", "caller", "setup", Map::new(), "model", 1)
}
fn applied_record(kind: CompactionMessageKind, request: Option<&str>) -> WalRecord {
    let replacement = vec![text(Role::User, "head")];
    WalRecord::CompactionMessage {
        provider: "provider".into(),
        kind,
        compaction_id: "c".into(),
        version: if kind == CompactionMessageKind::Setup {
            1
        } else {
            2
        },
        range: CompactionRange { from: 0, to: 0 },
        sha256: broca_wal::compaction_replacement_sha256(&replacement).unwrap(),
        replacement: Arc::new(replacement),
        request_id: request.map(str::to_owned),
        cursor: request.map(|_| CompactionCursor {
            lineage_id: "lineage".into(),
            after_ordinal: Some(3),
        }),
        call_when: None,
        stability: Some(vec![CompactionStabilityRank { index: 0, rank: 7 }]),
    }
}
#[tokio::test]
async fn setup_failures_timeout_refuse_and_oversize_write_no_setup() {
    let mut oversized = ready();
    oversized["initial"]["replacement"][0]["content"][0]["text"] =
        "x".repeat(MAX_REPLACEMENT_BYTES).into();
    for (reply, code, provider_code) in [
        (
            json!({"answer":"unknown"}),
            None,
            Some("compaction_unavailable"),
        ),
        (
            json!({"answer":"never"}),
            None,
            Some("compaction_unavailable"),
        ),
        (oversized, None, Some("compaction_unavailable")),
        (
            json!({"answer":"refuse","code":"window_too_small","reason":"small","provider_code":"finer"}),
            Some("window_too_small"),
            Some("finer"),
        ),
        (
            json!({"answer":"refuse","code":"future_code","reason":"future"}),
            Some("future_code"),
            None,
        ),
    ] {
        let shared = Shared::default();
        let writer = shared.writer();
        let control = crate::RunControl::default();
        let bind = bind();
        let scripted = Scripted::new(&shared, reply, vec![]);
        let plane: Arc<dyn CompactionPlane> = scripted;
        let runtime = runtime::Runtime {
            provider: "provider",
            bind: &bind,
            plane: Some(&plane),
            inbox: None,
            writer: &writer,
            control: &control,
            replies: &runtime::CompactionReplies::new(),
        };
        let error = runtime.setup(setup(), &raw()).await.unwrap_err();
        assert_eq!(error.provider_code.as_deref(), code.or(provider_code));
        assert_eq!(
            error
                .refusal
                .as_ref()
                .and_then(|refusal| refusal.provider_detail_code.as_deref()),
            code.and(provider_code)
        );
        assert!(shared.frames().is_empty());
        let _ = writer.shutdown().await;
    }
}
#[tokio::test]
async fn every_step_outcome_records_atomic_cursor_and_rejection() {
    let shared = Shared::default();
    let writer = shared.writer();
    let control = crate::RunControl::default();
    let bind = bind();
    let invalid = compaction(vec![encoded(&text(Role::System, "bad"))]);
    let scripted = Scripted::new(
        &shared,
        ready(),
        vec![
            json!({"answer":"noop"}),
            json!({"answer":"unknown"}),
            json!({"answer":"never"}),
            json!({"answer":"compaction_message","compaction":invalid}),
            json!({"answer":"noop","request_id":"superseded"}),
        ],
    );
    let plane: Arc<dyn CompactionPlane> = scripted.clone();
    let runtime = runtime::Runtime {
        provider: "provider",
        bind: &bind,
        plane: Some(&plane),
        inbox: None,
        writer: &writer,
        control: &control,
        replies: &runtime::CompactionReplies::new(),
    };
    let mut state = runtime.setup(setup(), &raw()).await.unwrap();
    for expected in [
        CompactionDisposition::Noop,
        CompactionDisposition::FailedCall,
        CompactionDisposition::Timeout,
        CompactionDisposition::Structural,
        CompactionDisposition::SupersededRequest,
    ] {
        runtime
            .step(&mut state, status(), &raw(), Some(10), None)
            .await
            .unwrap();
        let records = shared.records.lock().unwrap();
        assert!(
            matches!(records.last(), Some(WalRecord::CompactionAnswer { disposition, cursor, .. }) if *disposition == expected && cursor.after_ordinal == Some(3))
        );
        assert_eq!(shared.durable.load(Ordering::SeqCst), records.len());
    }
    let mut next = status();
    state.fill_status(&mut next, &raw());
    assert_eq!(next.last_not_applied.unwrap().reason, "structural");
    assert_eq!(next.after_ordinal, Some(3));
    assert!(next.messages.is_empty());
    let _ = writer.shutdown().await;
}
#[test]
fn checkpoint_has_three_states_and_absent_never_folds_from_tail() {
    let mut checkpoint = crate::AdmissionCheckpointV1::empty();
    assert_eq!(checkpoint.compaction, Some(CompactionCheckpoint::None));
    checkpoint.compaction = None;
    checkpoint
        .fold_record(1, &applied_record(CompactionMessageKind::Setup, None))
        .unwrap();
    assert!(checkpoint.compaction.is_none());
    let bytes = crate::encode_admission_checkpoint(&checkpoint).unwrap();
    assert!(!bytes.contains("\"compaction\":"));
    let mut captured = crate::AdmissionCheckpointV1::empty();
    captured
        .fold_record(1, &applied_record(CompactionMessageKind::Setup, None))
        .unwrap();
    captured
        .fold_record(2, &applied_record(CompactionMessageKind::Step, Some("r")))
        .unwrap();
    let decoded =
        crate::decode_admission_checkpoint(&crate::encode_admission_checkpoint(&captured).unwrap())
            .unwrap();
    assert_eq!(decoded, captured);
    let state = decoded.compaction.as_ref().unwrap().state().unwrap();
    assert_eq!(state.applied.as_ref().unwrap().version, 2);
    assert_eq!(state.cursor.as_ref().unwrap().after_ordinal, Some(3));
}
#[test]
fn render_slices_stamped_tail_and_declares_frozen_ranks_without_decorating_replacement() {
    let mut state = CompactionState::default();
    state.fold(&applied_record(CompactionMessageKind::Setup, None));
    state.applied.as_mut().unwrap().range = CompactionRange { from: 0, to: 3 };
    state.stability.push(CompactionStabilityRank {
        index: 99,
        rank: 10,
    });
    let options = CallOptions::new(raw());
    let view = model_options(&options, &state);
    assert_eq!(view.prompt.len(), 3);
    assert_eq!(view.prompt[0], options.prompt[0]);
    assert_eq!(view.prompt[1].ordinal, None);
    assert_eq!(view.prompt[2].ordinal, Some(3));
    assert_eq!(
        view.prompt[1].content,
        vec![ContentBlock::Text {
            text: "head".into()
        }]
    );
    assert_eq!(
        view.declared_markers,
        vec![broca_protocol::request::DeclaredMessageMarker {
            message_index: 1,
            rank: 7
        }]
    );
    let before = crate::canonical_request_bytes(&view);
    let mut without = view.clone();
    without.declared_markers.clear();
    assert_eq!(before, crate::canonical_request_bytes(&without));
}

#[tokio::test]
async fn wait_bound_and_ready_recall_with_fresh_status_and_close_wait() {
    for ready_hint in [false, true] {
        let shared = Shared::default();
        let writer = shared.writer();
        let bind = bind();
        let inbox = Arc::new(CompactionInbox::default());
        let control = crate::RunControl::default();
        let scripted = Scripted::new(
            &shared,
            ready(),
            vec![
                json!({"answer":"wait","reason":"work","bound_ms":10}),
                json!({"answer":"noop"}),
            ],
        );
        let plane: Arc<dyn CompactionPlane> = scripted.clone();
        let runtime = runtime::Runtime {
            provider: "provider",
            bind: &bind,
            plane: Some(&plane),
            inbox: Some(&inbox),
            writer: &writer,
            control: &control,
            replies: &runtime::CompactionReplies::new(),
        };
        let mut state = runtime.setup(setup(), &raw()).await.unwrap();
        let wake = async {
            if ready_hint {
                // Observe the durable wait, not a proxy "waiting" flag.
                loop {
                    if let Some(WalRecord::CompactionAnswer {
                        request_id,
                        disposition: CompactionDisposition::Wait,
                        ..
                    }) = shared.records.lock().unwrap().last().cloned()
                    {
                        let request = crate::compaction_provider::ready::CompactionReady::new(
                            "session", request_id,
                        );
                        assert_eq!(
                            inbox.ready("stranger", Some("provider"), &request),
                            Err("not_session_compaction_provider")
                        );
                        inbox.ready("provider", Some("provider"), &request).unwrap();
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }
        };
        let raw = raw();
        let (result, _) = tokio::join!(
            runtime.step(&mut state, status(), &raw, Some(10), None),
            wake
        );
        assert!(matches!(result.unwrap(), runtime::StepResult::Continue));
        let statuses = scripted.statuses.lock().unwrap().clone();
        assert_eq!(statuses.len(), 2);
        assert_ne!(statuses[0].request_id, statuses[1].request_id);
        assert_eq!(statuses[1].after_ordinal, Some(3));
        let exit = if ready_hint {
            CompactionWaitExit::Ready
        } else {
            CompactionWaitExit::Bound
        };
        assert!(shared.frames().iter().any(|frame| matches!(frame.record, WalRecord::CompactionWait { exited, .. } if exited == exit)));
        assert!(state.open_wait.is_none());
        let _ = writer.shutdown().await;
    }
}
#[tokio::test]
async fn wait_cap_sends_only_below_margin_and_fails_closed_otherwise() {
    for (estimate, window, expected) in [
        (Some(899), Some(1000), CompactionWaitExit::Cap),
        (Some(900), Some(1000), CompactionWaitExit::Exceeded),
        (Some(950), Some(1000), CompactionWaitExit::Exceeded),
        (None, Some(1000), CompactionWaitExit::Exceeded),
        (Some(10), None, CompactionWaitExit::Exceeded),
    ] {
        let shared = Shared::default();
        let writer = shared.writer();
        let control = crate::RunControl::default();
        let bind = bind();
        let scripted = Scripted::new(
            &shared,
            ready(),
            vec![json!({"answer":"wait","reason":"work","bound_ms":60000})],
        );
        let plane: Arc<dyn CompactionPlane> = scripted;
        let runtime = runtime::Runtime {
            provider: "provider",
            bind: &bind,
            plane: Some(&plane),
            inbox: None,
            writer: &writer,
            control: &control,
            replies: &runtime::CompactionReplies::new(),
        };
        let mut state = runtime.setup(setup(), &raw()).await.unwrap();
        let mut status = status();
        status.context_window = window;
        let result = runtime
            .step(
                &mut state,
                status,
                &raw(),
                estimate,
                Some(crate::CacheClockReading {
                    last_sent_at_ms: now(),
                    ttl_ms: Some(1),
                    prefix_tokens: None,
                }),
            )
            .await
            .unwrap();
        match result {
            runtime::StepResult::Continue => assert_eq!(expected, CompactionWaitExit::Cap),
            runtime::StepResult::Terminal(reason, error) => {
                assert_eq!(expected, CompactionWaitExit::Exceeded);
                assert_eq!(reason, RunFinishReason::Error);
                assert_eq!(
                    error.unwrap().provider_code.as_deref(),
                    Some("compaction_wait_exceeded")
                );
            }
        }
        assert!(
            matches!(shared.records.lock().unwrap().last(), Some(WalRecord::CompactionWait { exited, .. }) if *exited == expected)
        );
        let _ = writer.shutdown().await;
    }
}
#[test]
fn inbox_accepts_only_frozen_provider_and_current_request() {
    let inbox = CompactionInbox::default();
    inbox.issue("new");
    let old = crate::compaction_provider::ready::CompactionReady::new("s", "old");
    assert_eq!(
        inbox.ready("foreign", Some("provider"), &old),
        Err("not_session_compaction_provider")
    );
    inbox.ready("provider", Some("provider"), &old).unwrap();
    let current = crate::compaction_provider::ready::CompactionReady::new("s", "new");
    assert_eq!(
        inbox.ready("provider", None, &current),
        Err("not_session_compaction_provider")
    );
}
#[tokio::test]
async fn terminal_wait_intents_recover_without_provider_call_and_survive_second_cut() {
    for exited in [
        None,
        Some(CompactionWaitExit::Cancel),
        Some(CompactionWaitExit::Stopped),
        Some(CompactionWaitExit::Interrupted),
        Some(CompactionWaitExit::Exceeded),
    ] {
        let shared = Shared::default();
        let writer = shared.writer();
        let bind = bind();
        let control = crate::RunControl::default();
        let scripted = Scripted::new(&shared, ready(), vec![]);
        let plane: Arc<dyn CompactionPlane> = scripted.clone();
        let runtime = runtime::Runtime {
            provider: "provider",
            bind: &bind,
            plane: Some(&plane),
            inbox: None,
            writer: &writer,
            control: &control,
            replies: &runtime::CompactionReplies::new(),
        };
        let mut state = CompactionState::default();
        state.fold(&applied_record(CompactionMessageKind::Setup, None));
        state.fold(&WalRecord::CompactionAnswer {
            provider: "provider".into(),
            request_id: "r".into(),
            cursor: CompactionCursor {
                lineage_id: "l".into(),
                after_ordinal: None,
            },
            disposition: CompactionDisposition::Wait,
            rejected: None,
            wait: Some(CompactionWait {
                reason: "work".into(),
                bound_ms: 10,
            }),
        });
        if let Some(exited) = exited {
            state.fold(&WalRecord::CompactionWait {
                exited,
                reason: "work".into(),
            });
        }
        let expected = match exited {
            Some(CompactionWaitExit::Cancel) => RunFinishReason::Cancelled,
            Some(CompactionWaitExit::Exceeded) => RunFinishReason::Error,
            _ => RunFinishReason::Interrupted,
        };
        for _ in 0..2 {
            assert!(
                matches!(runtime.recover_wait(&mut state).await.unwrap(), Some(runtime::StepResult::Terminal(reason, _)) if reason == expected)
            );
        }
        assert_eq!(scripted.setups.load(Ordering::SeqCst), 0);
        assert!(scripted.statuses.lock().unwrap().is_empty());
        assert_eq!(shared.frames().len(), usize::from(exited.is_none()));
        state.fold(&WalRecord::RunFinished {
            ts_ms: None,
            reason: expected,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        });
        assert!(state.open_wait.is_none());
        let _ = writer.shutdown().await;
    }
}
#[test]
fn model_page_folds_snapshot_and_mid_turn_tail_and_refuses_after_mid() {
    let mut captured = CompactionCheckpoint::None;
    captured.fold(&applied_record(CompactionMessageKind::Setup, None));
    let raw = raw();
    let snapshot = crate::StorePageSnapshot {
        prefix_len: raw.len() as u64,
        rows: raw
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, message)| crate::StorePageRow {
                run: None,
                ordinal: i as u64,
                message,
            })
            .collect(),
        compaction: Some(captured),
        lineage_id: Some("lineage".into()),
        ..crate::StorePageSnapshot::empty()
    };
    let mut update = applied_record(CompactionMessageKind::Step, Some("r"));
    if let WalRecord::CompactionMessage {
        range,
        replacement,
        sha256,
        ..
    } = &mut update
    {
        *range = CompactionRange { from: 0, to: 3 };
        *replacement = Arc::new(vec![text(Role::User, "new view")]);
        *sha256 = broca_wal::compaction_replacement_sha256(replacement).unwrap();
    }
    let tail = vec![FramedRecord {
        seq: 2,
        fence: 1,
        record: update,
    }];
    let request = crate::ModelPageRequest {
        from_ordinal: Some(0),
        ..Default::default()
    };
    let page = crate::read_session_model_page(&snapshot, &tail, &request, || {
        panic!("captured state and all native rows suffice")
    })
    .unwrap();
    assert_eq!(page.version, Some(2));
    assert_eq!(page.messages.len(), 2);
    assert_eq!(
        page.messages[0].message.content,
        vec![ContentBlock::Text {
            text: "new view".into()
        }]
    );
    assert_eq!(page.messages[0].source["kind"], "replacement");
    assert_eq!(page.messages[1].source["ordinal"], 3);
    let request = crate::ModelPageRequest {
        after_mid: Some("m0".into()),
        ..Default::default()
    };
    assert_eq!(
        crate::read_session_model_page(&snapshot, &tail, &request, || panic!(
            "invalid cursor must refuse before any read"
        )),
        Err(crate::ModelPageError::InvalidParams { field: "view" })
    );
}
fn manifest() -> Arc<broca_wal::PlanManifest> {
    Arc::new(broca_wal::PlanManifest { plan: OpaqueJson::from_json_text(json!({"compaction_item":{"provider":"provider","preset":"head","params":{}},"composition":{}}).to_string()).unwrap(),
        plan_identity: "id".into(), composition_digest: "digest".into(), tools: Arc::new(vec![]), system_text: String::new(), system_digest: String::new(), join_version: 1, role_versions: Default::default(), fetches: vec![], declarations: vec![] })
}
fn request() -> crate::RunRequest {
    let submission: broca_protocol::request::SubmissionRequest = serde_json::from_value(json!({"model":{"provider_module_id":"stub","model_id":"model"},"input":[{"role":"user","content":[{"type":"text","text":"user"}]}]})).unwrap();
    crate::RunRequest::from_submission(
        submission,
        broca_protocol::RunId("run".into()),
        bind(),
        broca_protocol::SubmissionId("sub".into()),
    )
}
fn model_events() -> Vec<broca_protocol::NormalizedEvent> {
    use broca_protocol::{NormalizedEvent, ProviderControl};
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: broca_protocol::AssistantMessage {
                message_id: broca_protocol::MessageId("a".into()),
                content: vec![ContentBlock::Text {
                    text: "done".into(),
                }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}
#[tokio::test]
async fn engine_setup_once_and_durable_apply_cut_resume_make_no_compaction_calls() {
    let shared = Shared::default();
    let writer = shared.writer();
    assert_eq!(
        writer.lineage_id(),
        None,
        "legacy identity has not been minted yet"
    );
    let applied = json!({"answer":"compaction_message","compaction":{"compaction_id":"c","version":2,"range":{"from":0,"to":0},"replacement":[{"role":"user","content":[{"type":"text","text":"applied head"}]}]}});
    let scripted = Scripted::new(&shared, ready(), vec![applied]);
    let model = crate::StubProvider::new(vec![model_events()]);
    let (out, _events) = tokio::sync::mpsc::channel(128);
    let control = crate::RunControl {
        manifest: Some(manifest()),
        compaction_plane: Some(scripted.clone()),
        ..Default::default()
    };
    let summary = crate::run_loop_on_writer(
        request(),
        &writer,
        crate::StartMode::FreshOrResume,
        &model,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        &shared,
        None,
        control,
        &out,
    )
    .await
    .unwrap();
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(scripted.setups.load(Ordering::SeqCst), 1);
    let lineage_id = writer.lineage_id().unwrap().to_string();
    let setup_request = scripted.setup_requests.lock().unwrap()[0].clone();
    assert_eq!(
        setup_request.lineage_id.as_deref(),
        Some(lineage_id.as_str())
    );
    assert!(setup_request
        .request_id
        .starts_with(&format!("{lineage_id}:setup:")));
    assert_eq!(scripted.statuses.lock().unwrap()[0].lineage_id, lineage_id);
    assert_eq!(
        model.calls()[0].render_episodes.compaction_prefix_len,
        Some(1)
    );
    assert_eq!(
        model.calls()[0].prompt[0].content,
        vec![ContentBlock::Text {
            text: "applied head".into()
        }]
    );
    let records = shared.records.lock().unwrap().clone();
    let _ = writer.shutdown().await;
    let cut = records
        .iter()
        .position(|record| {
            matches!(
                record,
                WalRecord::CompactionMessage {
                    kind: CompactionMessageKind::Step,
                    ..
                }
            )
        })
        .unwrap()
        + 1;
    assert!(
        matches!(&records[cut - 1], WalRecord::CompactionMessage { cursor: Some(cursor), version: 2, .. } if cursor.after_ordinal == Some(0))
    );
    let resumed = Shared {
        records: Arc::new(Mutex::new(records[..cut].to_vec())),
        ..Default::default()
    };
    let writer = resumed.writer();
    let scripted = Scripted::new(&resumed, ready(), vec![]);
    let model2 = crate::StubProvider::new(vec![model_events()]);
    let control = crate::RunControl {
        compaction_plane: Some(scripted.clone()),
        ..Default::default()
    };
    crate::run_loop_on_writer(
        request(),
        &writer,
        crate::StartMode::FreshOrResume,
        &model2,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        &resumed,
        None,
        control,
        &out,
    )
    .await
    .unwrap();
    assert_eq!(scripted.setups.load(Ordering::SeqCst), 0);
    assert!(scripted.statuses.lock().unwrap().is_empty());
    assert_eq!(
        model2.calls()[0].render_episodes.compaction_prefix_len,
        Some(1)
    );
    assert_eq!(
        crate::canonical_request_bytes(&model.calls()[0]),
        crate::canonical_request_bytes(&model2.calls()[0])
    );
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn engine_missing_lineage_refuses_before_compaction_or_model_calls() {
    let shared = Shared {
        omit_lineage: true,
        ..Default::default()
    };
    let writer = shared.writer();
    let scripted = Scripted::new(&shared, ready(), vec![]);
    let model = crate::StubProvider::new(vec![model_events()]);
    let (out, _events) = tokio::sync::mpsc::channel(128);
    let control = crate::RunControl {
        manifest: Some(manifest()),
        compaction_plane: Some(scripted.clone()),
        ..Default::default()
    };
    let error = crate::run_loop_on_writer(
        request(),
        &writer,
        crate::StartMode::FreshOrResume,
        &model,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        &shared,
        None,
        control,
        &out,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        crate::RunError::Wal(ref reason)
            if reason == "compaction requires a WAL lineage id after the durable StepStarted barrier"
    ));
    assert_eq!(writer.lineage_id(), None);
    assert_eq!(scripted.setups.load(Ordering::SeqCst), 0);
    assert!(scripted.statuses.lock().unwrap().is_empty());
    assert!(model.calls().is_empty());
    assert!(
        !shared.records.lock().unwrap().iter().any(|record| matches!(
            record,
            WalRecord::CompactionRequest { .. }
                | WalRecord::CompactionMessage { .. }
                | WalRecord::CompactionAnswer { .. }
        ))
    );
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn engine_legacy_auth_resume_mints_lineage_before_setup_and_step() {
    let shared = Shared::default();
    let writer = shared.writer();
    let model = crate::StubProvider::new(vec![model_events()]);
    let (out, _events) = tokio::sync::mpsc::channel(128);
    let mut pause = super::runtime::error(None, "needs reauthentication", None, false);
    pause.class = broca_protocol::ErrorClass::Auth;
    let summary = crate::run_loop_on_writer(
        request(),
        &writer,
        crate::StartMode::FreshOrResume,
        &model,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        &shared,
        None,
        crate::RunControl {
            manifest: Some(manifest()),
            pause_for_auth_at_start: Some(pause),
            ..Default::default()
        },
        &out,
    )
    .await
    .unwrap();
    assert_eq!(
        summary.paused,
        Some(broca_protocol::PauseReason::AuthRequired)
    );
    assert!(model.calls().is_empty());
    let _ = writer.shutdown().await;

    // A legacy file retains the paused records but has no lineage header. Resolving auth
    // without refreezing config writes no resume record, so StepStarted must mint the id.
    let legacy = Shared {
        records: Arc::new(Mutex::new(shared.records.lock().unwrap().clone())),
        ..Default::default()
    };
    let writer = legacy.writer();
    assert_eq!(writer.lineage_id(), None);
    let scripted = Scripted::new(&legacy, ready(), vec![]);
    let summary = crate::run_loop_on_writer(
        request(),
        &writer,
        crate::StartMode::ResumeAfterAuth,
        &model,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        &legacy,
        None,
        crate::RunControl {
            compaction_plane: Some(scripted.clone()),
            ..Default::default()
        },
        &out,
    )
    .await
    .unwrap();
    assert_eq!(summary.reason, RunFinishReason::Completed);
    let lineage_id = writer.lineage_id().unwrap().to_string();
    let setup_request = scripted.setup_requests.lock().unwrap()[0].clone();
    assert_eq!(
        setup_request.lineage_id.as_deref(),
        Some(lineage_id.as_str())
    );
    assert!(setup_request
        .request_id
        .starts_with(&format!("{lineage_id}:setup:")));
    assert_eq!(scripted.statuses.lock().unwrap()[0].lineage_id, lineage_id);
    assert_eq!(model.calls().len(), 1);
    let _ = writer.shutdown().await;
}

#[test]
fn status_uses_native_values_and_caps_before_newest_without_skipping_cursor() {
    let mut raw = vec![
        text(Role::User, &"x".repeat(3 * 1024 * 1024)),
        text(Role::Assistant, &"y".repeat(3 * 1024 * 1024)),
    ];
    broca_protocol::content::stamp_ordinals(&mut raw);
    let state = CompactionState::default();
    let mut status = status();
    state.fill_status(&mut status, &raw);
    assert_eq!(status.messages.len(), 1);
    assert!(status.more);
    assert_eq!(status.next_cursor(), Some(0));
    assert_eq!(
        status.newest,
        Some(MessageRef {
            ordinal: 1,
            mid: "m1".into()
        })
    );
    assert_eq!(status.messages[0].message, encoded(&raw[0]));
    let mut state = state;
    state.cursor = Some(CompactionCursor {
        lineage_id: "lineage".into(),
        after_ordinal: status.next_cursor(),
    });
    state.fill_status(&mut status, &raw);
    assert_eq!(status.messages.len(), 1);
    assert_eq!(status.next_cursor(), Some(1));
    assert!(!status.more);
    status.lineage_id = "new lineage".into();
    state.fill_status(&mut status, &raw);
    assert_eq!(status.after_ordinal, None);
}
#[test]
fn guidance_fold_precedes_slice_and_carried_replacements_remain_undecorated() {
    let mut raw = vec![text(Role::System, "system"), text(Role::User, "user")];
    for i in 0..12 {
        raw.push(Message::new(Role::Assistant, vec![call(&format!("id{i}"))]));
        raw.push(Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(broca_protocol::ToolResult {
                tool_call_id: broca_protocol::ToolCallId(format!("id{i}")),
                output: broca_protocol::ToolOutput::Error {
                    message: "failed".into(),
                },
                is_error: true,
            })],
        ));
    }
    broca_protocol::content::stamp_ordinals(&mut raw);
    let mut folded = raw.clone();
    crate::run::restore_repetition_guidance(&mut folded[2..]);
    assert_ne!(
        folded[13], raw[13],
        "the carried-over result actually has guidance in the full fold"
    );
    let mut state = CompactionState::default();
    state.fold(&applied_record(CompactionMessageKind::Setup, None));
    let applied = state.applied.as_mut().unwrap();
    applied.range = CompactionRange { from: 0, to: 14 };
    applied.replacement = Arc::new(vec![
        text(Role::User, "head"),
        raw[12].clone(),
        raw[13].clone(),
    ]);
    let options = CallOptions::new(folded.clone());
    let view = model_options(&options, &state);
    assert_eq!(
        view.prompt[3].content, raw[13].content,
        "carried replacement is not decorated"
    );
    assert_eq!(
        &view.prompt[4..],
        &folded[14..],
        "tail keeps counters from the replaced messages"
    );
    assert_eq!(
        options.prompt.len(),
        raw.len(),
        "the transcript is not rewritten"
    );
}
#[tokio::test]
async fn engine_setup_refusals_make_no_model_call_and_preserve_role_code() {
    for reply in [
        json!({"answer":"unknown"}),
        json!({"answer":"refuse","code":"window_too_small","reason":"small","provider_code":"own"}),
    ] {
        let shared = Shared::default();
        let writer = shared.writer();
        let scripted = Scripted::new(&shared, reply.clone(), vec![]);
        let model = crate::StubProvider::new(vec![model_events()]);
        let (out, _events) = tokio::sync::mpsc::channel(128);
        let control = crate::RunControl {
            manifest: Some(manifest()),
            compaction_plane: Some(scripted),
            ..Default::default()
        };
        let summary = crate::run_loop_on_writer(
            request(),
            &writer,
            crate::StartMode::FreshOrResume,
            &model,
            &crate::StubToolPlane::new(),
            &crate::AllowAll,
            &shared,
            None,
            control,
            &out,
        )
        .await
        .unwrap();
        assert_eq!(summary.reason, RunFinishReason::Error);
        assert!(model.calls().is_empty());
        let records = shared.records.lock().unwrap().clone();
        assert!(!records.iter().any(|record| matches!(
            record,
            WalRecord::CompactionMessage {
                kind: CompactionMessageKind::Setup,
                ..
            }
        )));
        let Some(WalRecord::RunFinished {
            error: Some(error), ..
        }) = records.last()
        else {
            panic!("typed terminal error");
        };
        if reply["answer"] == "refuse" {
            assert_eq!(error.provider_code.as_deref(), Some("window_too_small"));
            assert_eq!(
                error
                    .refusal
                    .as_ref()
                    .unwrap()
                    .provider_detail_code
                    .as_deref(),
                Some("own")
            );
        } else {
            assert_eq!(
                error.provider_code.as_deref(),
                Some("compaction_unavailable")
            );
        }
        drop(records);
        let _ = writer.shutdown().await;
    }
}
#[tokio::test]
async fn admission_and_model_page_absent_compaction_summary_force_complete_reads() {
    let shared = Shared::default();
    let writer = shared.writer();
    let scripted = Scripted::new(&shared, ready(), vec![]);
    let model = crate::StubProvider::new(vec![model_events()]);
    let (out, _events) = tokio::sync::mpsc::channel(128);
    let control = crate::RunControl {
        manifest: Some(manifest()),
        compaction_plane: Some(scripted),
        ..Default::default()
    };
    crate::run_loop_on_writer(
        request(),
        &writer,
        crate::StartMode::FreshOrResume,
        &model,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        &shared,
        None,
        control,
        &out,
    )
    .await
    .unwrap();
    let frames = shared.frames();
    let head = frames.last().unwrap().seq;
    let mut checkpoint = crate::AdmissionCheckpointV1::from_frames(&frames).unwrap();
    checkpoint.compaction = None;
    struct Store(crate::AdmissionStoreSnapshot);
    impl crate::MaterializedStore for Store {
        fn read_prefix_with_watermark(
            &self,
            _: &BindIdentity,
        ) -> Result<crate::StorePrefix, String> {
            Ok(crate::StorePrefix::empty())
        }
        fn read_admission_snapshot(
            &self,
            _: &BindIdentity,
        ) -> Result<Option<crate::AdmissionStoreSnapshot>, String> {
            Ok(Some(self.0.clone()))
        }
    }
    struct Reads<'a>(&'a Shared, AtomicUsize);
    impl crate::WalStore for Reads<'_> {
        fn open(&self, _: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
            unreachable!()
        }
        fn read_records(&self, _: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
            self.1.fetch_add(1, Ordering::SeqCst);
            Ok(self.0.frames())
        }
        fn read_records_range(
            &self,
            _: &BindIdentity,
            _: u64,
            _: Option<u64>,
            _: Option<&broca_wal::WalByteAnchor>,
        ) -> Result<broca_wal::WalRangeRead, WalError> {
            Ok(broca_wal::WalRangeRead::without_anchors(vec![]))
        }
    }
    let store = Store(crate::AdmissionStoreSnapshot {
        watermark_seq: head,
        lineage_id: None,
        anchor: None,
        checkpoint: Some(checkpoint),
    });
    let reads = Reads(&shared, AtomicUsize::new(0));
    let snapshot = crate::load_prior_wal_snapshot(&reads, &bind(), head, Some(&store)).unwrap();
    assert_eq!(reads.1.load(Ordering::SeqCst), 1);
    assert_eq!(snapshot.base_watermark, 0);
    assert_eq!(
        snapshot
            .checkpoint_at_head
            .compaction
            .as_ref()
            .unwrap()
            .state()
            .unwrap()
            .applied
            .as_ref()
            .unwrap()
            .version,
        1
    );
    let snapshot = crate::StorePageSnapshot {
        watermark_seq: head,
        lineage_id: Some("lineage".into()),
        ..crate::StorePageSnapshot::empty()
    };
    let reads = AtomicUsize::new(0);
    let page =
        crate::read_session_model_page(&snapshot, &[], &crate::ModelPageRequest::default(), || {
            reads.fetch_add(1, Ordering::SeqCst);
            Ok(frames)
        })
        .unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert_eq!(page.compaction_id.as_deref(), Some("c"));
    let _ = writer.shutdown().await;
}

#[test]
fn structural_tool_pairing_tracks_reused_ids_by_occurrence() {
    let mut raw = vec![text(Role::User, "user")];
    for _ in 0..2 {
        raw.push(Message::new(Role::Assistant, vec![call("reused")]));
        raw.push(Message::new(Role::Tool, vec![result("reused")]));
    }
    broca_protocol::content::stamp_ordinals(&mut raw);
    let initial = CompactionMessage::new(
        "c",
        2,
        Range::new(0, 0),
        vec![encoded(&text(Role::User, "head"))],
    );
    assert!(validate_answer(
        &FenceState {
            newest_ordinal: Some(4),
            ..fence()
        },
        &answer(initial),
        99,
        &raw
    )
    .is_ok());
    let invalid = CompactionMessage::new(
        "c",
        2,
        Range::new(0, 5),
        vec![
            encoded(&Message::new(
                Role::Assistant,
                vec![call("new"), call("new")],
            )),
            encoded(&Message::new(Role::Tool, vec![result("new")])),
        ],
    );
    assert_eq!(
        validate_answer(
            &FenceState {
                newest_ordinal: Some(4),
                ..fence()
            },
            &answer(invalid),
            99,
            &raw
        ),
        Err(CompactionDisposition::Structural)
    );
}

#[tokio::test]
async fn q28_cached_fill_is_inside_margin_and_unreported_input_fails_closed() {
    let anthropic = Usage {
        input_tokens: Some(1),
        cached_input_tokens: Some(945),
        cache_write_tokens: Some(4),
        output_tokens: Some(3),
        ..Usage::default()
    };
    let codex = Usage {
        input_tokens: Some(1),
        cached_input_tokens: Some(850),
        cache_write_tokens: None,
        output_tokens: Some(3),
        ..Usage::default()
    };
    for (usage, expected_fill, has_wire_usage) in [
        (anthropic, Some(950), true),
        (codex, Some(851), false),
        (
            Usage {
                input_tokens: None,
                ..anthropic
            },
            None,
            false,
        ),
    ] {
        let mut history = CompactionHistory {
            previous_native_usage: Some(usage),
            previous_request_bytes: Some(20),
            ..Default::default()
        };
        history.previous_usage = wire_usage(usage, 1, "stop".into());
        assert_eq!(
            history.usage().and_then(|usage| usage.fill_tokens()),
            expected_fill
        );
        assert_eq!(history.previous_usage.is_some(), has_wire_usage);
        let (display, reliable) = request_estimate(20, &history);
        assert_eq!(reliable, expected_fill);
        if let Some(fill) = expected_fill {
            assert_eq!(display, fill);
        }
        let encoded = serde_json::to_string(&history).unwrap();
        let decoded: CompactionHistory = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.previous_native_usage, Some(usage));
        assert_eq!(request_estimate(20, &decoded), (display, reliable));
        if has_wire_usage {
            let status = history.previous_usage.as_ref().unwrap();
            assert_eq!(
                (status.input, status.cache_read, status.cache_write),
                (1, 945, 4)
            );
        }
        let shared = Shared::default();
        let writer = shared.writer();
        let control = crate::RunControl::default();
        let bind = bind();
        let scripted = Scripted::new(
            &shared,
            ready(),
            vec![json!({"answer":"wait","reason":"work","bound_ms":60000})],
        );
        let plane: Arc<dyn CompactionPlane> = scripted;
        let runtime = runtime::Runtime {
            provider: "provider",
            bind: &bind,
            plane: Some(&plane),
            inbox: None,
            writer: &writer,
            control: &control,
            replies: &runtime::CompactionReplies::new(),
        };
        let mut state = runtime.setup(setup(), &raw()).await.unwrap();
        let outcome = runtime
            .step(
                &mut state,
                status(),
                &raw(),
                reliable,
                Some(crate::CacheClockReading {
                    last_sent_at_ms: now(),
                    ttl_ms: Some(1),
                    prefix_tokens: None,
                }),
            )
            .await
            .unwrap();
        assert!(
            matches!(
                outcome,
                runtime::StepResult::Terminal(RunFinishReason::Error, _)
            ) != (usage == codex)
        );
        let _ = writer.shutdown().await;
    }
    assert_eq!(
        request_estimate(101, &CompactionHistory::default()),
        (51, Some(51))
    );
}

#[tokio::test]
async fn issued_request_cut_is_filed_without_advancing_cursor_and_forces_fresh_call() {
    let shared = Shared::default();
    let writer = shared.writer();
    let bind = bind();
    let control = crate::RunControl::default();
    let mut setup_answer = ready();
    setup_answer["call_when"] = json!({"default":1.0});
    let scripted = Scripted::new(&shared, setup_answer, vec![]);
    let plane: Arc<dyn CompactionPlane> = scripted.clone();
    let runtime = runtime::Runtime {
        provider: "provider",
        bind: &bind,
        plane: Some(&plane),
        inbox: None,
        writer: &writer,
        control: &control,
        replies: &runtime::CompactionReplies::new(),
    };
    let mut state = runtime.setup(setup(), &raw()).await.unwrap();
    assert!(!state.should_call("model", None, Some(1000), false, false));
    let lost = WalRecord::CompactionRequest {
        provider: "provider".into(),
        request_id: "lost".into(),
    };
    writer.record(lost.clone());
    writer.sync().await.unwrap();
    state.fold(&lost);
    assert!(state.should_call("model", None, Some(1000), false, false));
    runtime
        .step(&mut state, status(), &raw(), Some(10), None)
        .await
        .unwrap();
    let statuses = scripted.statuses.lock().unwrap().clone();
    assert_eq!(statuses.len(), 1);
    assert_ne!(statuses[0].request_id, "lost");
    assert_eq!(statuses[0].after_ordinal, None);
    assert_eq!(statuses[0].messages.len(), 4);
    let records = shared.records.lock().unwrap().clone();
    assert!(
        matches!(&records[2], WalRecord::CompactionAnswer { request_id, cursor, disposition: CompactionDisposition::FailedCall, .. } if request_id == "lost" && cursor.after_ordinal.is_none())
    );
    assert!(!state.pending_request);
    let _ = writer.shutdown().await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn frozen_clock_drives_configured_compaction_timeout_without_wall_time() {
    let root = broca_test_support::scratch_root().join("compaction-frozen-clock");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("test-clock");
    std::fs::write(&path, r#"{"now_ms":1000}"#).unwrap();
    let clock = crate::Clock::for_state_root(&root).unwrap();
    assert_eq!(clock.now_ms(), 1000);
    let shared = Shared::default();
    let writer = shared.writer();
    let control = crate::RunControl {
        clock: Some(clock.clone()),
        compaction_config: runtime::CompactionConfig {
            call_budget_ms: 17,
            step_wait_cap_ms: 31,
        },
        ..Default::default()
    };
    let bind = bind();
    let scripted = Scripted::new(&shared, ready(), vec![json!({"answer":"never"})]);
    let plane: Arc<dyn CompactionPlane> = scripted.clone();
    let runtime = runtime::Runtime {
        provider: "provider",
        bind: &bind,
        plane: Some(&plane),
        inbox: None,
        writer: &writer,
        control: &control,
        replies: &runtime::CompactionReplies::new(),
    };
    let mut state = runtime.setup(setup(), &raw()).await.unwrap();
    assert_eq!(scripted.setup_requests.lock().unwrap()[0].now, 1000);
    let step = async {
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            runtime.step(&mut state, status(), &raw(), Some(10), None),
        )
        .await
        .expect("configured clock deadline did not release the step")
        .unwrap();
    };
    let advance = async {
        while scripted.statuses.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        assert_eq!(scripted.statuses.lock().unwrap()[0].now, 1000);
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert_eq!(
            clock.now_ms(),
            1000,
            "wall time must not advance a frozen domain"
        );
        assert!(matches!(
            shared.records.lock().unwrap().last(),
            Some(WalRecord::CompactionRequest { .. })
        ));
        std::fs::write(&path, r#"{"now_ms":1016}"#).unwrap();
        clock.refresh().unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert!(matches!(
            shared.records.lock().unwrap().last(),
            Some(WalRecord::CompactionRequest { .. })
        ));
        std::fs::write(&path, r#"{"now_ms":1017}"#).unwrap();
        clock.refresh().unwrap();
    };
    tokio::join!(step, advance);
    assert!(matches!(
        shared.records.lock().unwrap().last(),
        Some(WalRecord::CompactionAnswer {
            disposition: CompactionDisposition::Timeout,
            ..
        })
    ));
    let _ = writer.shutdown().await;
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn discarded_receipts_preserve_pending_requests_and_genuine_pending_answers() {
    for disposition in [
        CompactionDisposition::Late,
        CompactionDisposition::SupersededRequest,
    ] {
        for pending in [false, true] {
            let mut state = CompactionState::default();
            state.fold(&applied_record(CompactionMessageKind::Setup, None));
            state.fold(&WalRecord::CompactionRequest {
                provider: "provider".into(),
                request_id: "new".into(),
            });
            if pending {
                state.fold(&applied_record(CompactionMessageKind::Step, Some("new")));
            }
            let before = (
                state.pending_answer,
                state.pending_request,
                state.cursor.clone(),
            );
            state.fold(&WalRecord::CompactionAnswer {
                provider: "provider".into(),
                request_id: "old".into(),
                cursor: CompactionCursor {
                    lineage_id: "old-lineage".into(),
                    after_ordinal: None,
                },
                disposition,
                rejected: None,
                wait: None,
            });
            assert_eq!(
                (
                    state.pending_answer,
                    state.pending_request,
                    state.cursor.clone()
                ),
                before
            );
            assert_eq!(
                state.applied.as_ref().unwrap().version,
                if pending { 2 } else { 1 }
            );
        }
        let mut idle = CompactionState::default();
        idle.fold(&WalRecord::CompactionAnswer {
            provider: "provider".into(),
            request_id: "old".into(),
            cursor: CompactionCursor {
                lineage_id: "old-lineage".into(),
                after_ordinal: None,
            },
            disposition,
            rejected: None,
            wait: None,
        });
        assert!(!idle.pending_answer);
        assert!(!idle.pending_request);
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn concurrent_clock_refresh_cannot_move_wait_cap_into_real_time() {
    let root = broca_test_support::scratch_root().join("compaction-refresh-order");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("test-clock");
    let clock = crate::Clock::for_state_root(&root).unwrap();
    let (read_tx, read_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let poll_clock = clock.clone();
    let poll = std::thread::spawn(move || {
        poll_clock.refresh_with_read_probe(|serialized| {
            // Suspend a real NotFound read, as when the poller runs before installation.
            read_tx.send(serialized).unwrap();
            release_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("release the held clock read");
        })
    });
    let serialized = read_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("poller read the absent clock file");
    std::fs::write(&path, r#"{"now_ms":0}"#).unwrap();
    if serialized {
        // The admission refresh cannot overtake a read that owns the clock lock.
        release_tx.send(()).unwrap();
        poll.join().unwrap().unwrap();
        clock.refresh().unwrap();
    } else {
        // Force the unsafe ordering: admission installs zero, then the old poll
        // applies its missing-file snapshot and puts the clock back in real time.
        clock.refresh().unwrap();
        release_tx.send(()).unwrap();
        poll.join().unwrap().unwrap();
    }

    let shared = Shared::default();
    let writer = shared.writer();
    let control = crate::RunControl {
        clock: Some(clock.clone()),
        compaction_config: runtime::CompactionConfig {
            call_budget_ms: 100,
            step_wait_cap_ms: 100,
        },
        ..Default::default()
    };
    let bind = bind();
    let scripted = Scripted::new(
        &shared,
        ready(),
        vec![json!({"answer":"wait","reason":"work","bound_ms":200})],
    );
    let plane: Arc<dyn CompactionPlane> = scripted.clone();
    let runtime = runtime::Runtime {
        provider: "provider",
        bind: &bind,
        plane: Some(&plane),
        inbox: None,
        writer: &writer,
        control: &control,
        replies: &runtime::CompactionReplies::new(),
    };
    let raw = raw();
    let mut state = runtime.setup(setup(), &raw).await.unwrap();
    let mut status = status();
    status.context_window = Some(1000);
    let mut step = Box::pin(runtime.step(&mut state, status, &raw, Some(950), None));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            assert!(futures::poll!(&mut step).is_pending());
            if matches!(
                shared.records.lock().unwrap().last(),
                Some(WalRecord::CompactionAnswer {
                    disposition: CompactionDisposition::Wait,
                    ..
                })
            ) && shared.durable.load(Ordering::SeqCst) == shared.frames().len()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("WAIT must become durable before advancing the fixed clock");
    if !serialized {
        eprintln!(
            "stale refresh: step issued at {} ms; fixed clock will advance only to 100 ms",
            scripted.statuses.lock().unwrap()[0].now
        );
    }

    // Only the injected clock advances; the wall-clock timeout is a failure fence,
    // not the mechanism that expires the compaction wait.
    std::fs::write(&path, r#"{"now_ms":99}"#).unwrap();
    clock.refresh().unwrap();
    assert!(
        futures::poll!(&mut step).is_pending(),
        "WAIT ended before its cap"
    );
    std::fs::write(&path, r#"{"now_ms":100}"#).unwrap();
    clock.refresh().unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), step).await;
    let _ = writer.shutdown().await;
    let result = result
        .expect("fixed-clock compaction cap did not fire at 100 ms after the held refresh")
        .unwrap();
    let runtime::StepResult::Terminal(reason, error) = result else {
        panic!("over-margin WAIT must fail closed at its cap");
    };
    assert_eq!(reason, RunFinishReason::Error);
    assert_eq!(
        error.unwrap().provider_code.as_deref(),
        Some("compaction_wait_exceeded")
    );
    assert_eq!(scripted.statuses.lock().unwrap()[0].now, 0);
    assert!(matches!(
        shared.records.lock().unwrap().last(),
        Some(WalRecord::CompactionWait {
            exited: CompactionWaitExit::Exceeded,
            ..
        })
    ));
}

/// The provider's bound (200 ms) is longer than the step's cap (100 ms), so the wait must
/// end with the cap's decision at 100 ms. The clock reaches 100 between two clock reads
/// made in one poll of the wait, as when the test-clock poller refreshes on another thread
/// while the run task is being polled. The decision must not depend on which read saw it.
#[cfg(feature = "test-clock")]
#[tokio::test]
async fn clock_reaching_the_cap_mid_poll_takes_the_cap_exit_not_the_bound() {
    let root = broca_test_support::scratch_root().join("compaction-cap-mid-poll");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("test-clock");
    std::fs::write(&path, r#"{"now_ms":0}"#).unwrap();
    let clock = crate::Clock::for_state_root(&root).unwrap();
    let shared = Shared::default();
    let writer = shared.writer();
    let control = crate::RunControl {
        clock: Some(clock.clone()),
        compaction_config: runtime::CompactionConfig {
            call_budget_ms: 25,
            step_wait_cap_ms: 100,
        },
        ..Default::default()
    };
    let bind = bind();
    // After the WAIT the script answers noop, as the conformance provider does once its
    // script runs out, so asking again would let the step go on to the model.
    let scripted = Scripted::new(
        &shared,
        ready(),
        vec![json!({"answer":"wait","reason":"work","bound_ms":200})],
    );
    let plane: Arc<dyn CompactionPlane> = scripted.clone();
    let runtime = runtime::Runtime {
        provider: "provider",
        bind: &bind,
        plane: Some(&plane),
        inbox: None,
        writer: &writer,
        control: &control,
        replies: &runtime::CompactionReplies::new(),
    };
    let raw = raw();
    let mut state = runtime.setup(setup(), &raw).await.unwrap();
    let mut status = status();
    status.context_window = Some(1000);
    let mut step = Box::pin(runtime.step(&mut state, status, &raw, Some(950), None));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            assert!(futures::poll!(&mut step).is_pending());
            if matches!(
                shared.records.lock().unwrap().last(),
                Some(WalRecord::CompactionAnswer {
                    disposition: CompactionDisposition::Wait,
                    ..
                })
            ) && shared.durable.load(Ordering::SeqCst) == shared.frames().len()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("WAIT must become durable before advancing the fixed clock");

    std::fs::write(&path, r#"{"now_ms":99}"#).unwrap();
    clock.refresh().unwrap();
    assert!(
        futures::poll!(&mut step).is_pending(),
        "WAIT ended before its cap"
    );
    // Wake every clock timer with the time still at 99: an offset change notifies
    // subscribers but does not move a fixed clock. The file already says 100, and the
    // probe applies it just after the first timer has read 99 and subscribed.
    std::fs::write(&path, r#"{"now_ms":99,"offset_ms":1}"#).unwrap();
    clock.refresh().unwrap();
    std::fs::write(&path, r#"{"now_ms":100,"offset_ms":1}"#).unwrap();
    clock.refresh_on_read_after(99);
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), step).await;
    let _ = writer.shutdown().await;
    let result = result
        .expect("fixed-clock compaction cap did not fire at 100 ms")
        .unwrap();
    assert_eq!(clock.now_ms(), 100, "the probe moved the clock to the cap");
    let exits: Vec<_> = shared
        .records
        .lock()
        .unwrap()
        .iter()
        .filter_map(|record| match record {
            WalRecord::CompactionWait { exited, .. } => Some(*exited),
            _ => None,
        })
        .collect();
    assert_eq!(
        exits,
        [CompactionWaitExit::Exceeded],
        "the wait reached its cap and must take the cap's decision"
    );
    assert_eq!(
        scripted.statuses.lock().unwrap().len(),
        1,
        "the provider is not asked again once the cap is reached"
    );
    let runtime::StepResult::Terminal(reason, error) = result else {
        panic!("over-margin WAIT must fail closed at its cap, not continue to the model");
    };
    assert_eq!(reason, RunFinishReason::Error);
    assert_eq!(
        error.unwrap().provider_code.as_deref(),
        Some("compaction_wait_exceeded")
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "test-clock")]
struct HeldCompaction {
    script: Arc<Scripted>,
    reply: Mutex<Option<tokio::sync::oneshot::Sender<StepAnswer>>>,
}
#[async_trait::async_trait]
#[cfg(feature = "test-clock")]
impl CompactionPlane for HeldCompaction {
    async fn setup(
        &self,
        provider: &str,
        bind: &BindIdentity,
        request: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        self.script.setup(provider, bind, request).await
    }
    async fn step(
        &self,
        _: &str,
        _: &BindIdentity,
        status: StepStatus,
    ) -> Result<StepAnswer, CompactionUnavailable> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.script.statuses.lock().unwrap().push(status);
        *self.reply.lock().unwrap() = Some(tx);
        rx.await.map_err(|_| CompactionUnavailable)
    }
}
#[cfg(feature = "test-clock")]
struct HeldModel {
    calls: Mutex<Vec<CallOptions>>,
    release: tokio::sync::Notify,
}
#[async_trait::async_trait]
#[cfg(feature = "test-clock")]
impl crate::Provider for HeldModel {
    async fn step(
        &self,
        _: broca_protocol::StepId,
        opts: &CallOptions,
    ) -> tokio::sync::mpsc::Receiver<broca_protocol::NormalizedEvent> {
        self.calls.lock().unwrap().push(opts.clone());
        self.release.notified().await;
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        for event in model_events() {
            tx.send(event).await.unwrap();
        }
        rx
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn late_receipt_during_model_wait_is_durable_without_resending_and_crash_resume_uses_last_view(
) {
    let root = broca_test_support::scratch_root().join("late-model-clock");
    std::fs::create_dir_all(&root).unwrap();
    let clock_path = root.join("test-clock");
    std::fs::write(&clock_path, r#"{"now_ms":1000}"#).unwrap();
    let clock = crate::Clock::for_state_root(&root).unwrap();
    let shared = Shared::default();
    let writer = shared.writer();
    let script = Scripted::new(&shared, ready(), vec![]);
    let plane = Arc::new(HeldCompaction {
        script: script.clone(),
        reply: Mutex::new(None),
    });
    let model = HeldModel {
        calls: Mutex::new(vec![]),
        release: tokio::sync::Notify::new(),
    };
    let control = crate::RunControl {
        manifest: Some(manifest()),
        compaction_plane: Some(plane.clone()),
        clock: Some(clock.clone()),
        compaction_config: runtime::CompactionConfig {
            call_budget_ms: 17,
            step_wait_cap_ms: 100,
        },
        ..Default::default()
    };
    let (out, _events) = tokio::sync::mpsc::channel(128);
    let tools = crate::StubToolPlane::new();
    let run = crate::run_loop_on_writer(
        request(),
        &writer,
        crate::StartMode::FreshOrResume,
        &model,
        &tools,
        &crate::AllowAll,
        &shared,
        None,
        control,
        &out,
    );
    let observe = async {
        while plane.reply.lock().unwrap().is_none() {
            tokio::task::yield_now().await;
        }
        std::fs::write(&clock_path, r#"{"now_ms":1017}"#).unwrap();
        clock.refresh().unwrap();
        while model.calls.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        let request_id = script.statuses.lock().unwrap()[0].request_id.clone();
        plane
            .reply
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(StepAnswer::CompactionMessage {
                request_id,
                compaction: CompactionMessage::new(
                    "c",
                    99,
                    Range::new(0, 0),
                    vec![
                        json!({"role":"user","content":[{"type":"text","text":"must not apply"}]}),
                    ],
                ),
                coverage: None,
                channel2_directive: None,
                bootstrap_complete: None,
            })
            .unwrap();
        loop {
            let frames = shared.frames();
            if matches!(frames.last().map(|f| &f.record), Some(WalRecord::CompactionAnswer {
                disposition: CompactionDisposition::Late, rejected: Some(rejected), .. }) if rejected.version == 99)
                && shared.durable.load(Ordering::SeqCst) == frames.len()
            {
                assert_eq!(
                    model.calls.lock().unwrap().len(),
                    1,
                    "late reply must not resend model request"
                );
                model.release.notify_one();
                return frames.into_iter().map(|f| f.record).collect::<Vec<_>>();
            }
            tokio::task::yield_now().await;
        }
    };
    let (summary, cut) = tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        tokio::join!(run, observe)
    })
    .await
    .expect("late receipt must be filed while the model is held");
    assert_eq!(summary.unwrap().reason, RunFinishReason::Completed);
    let _ = writer.shutdown().await;
    // The persisted prefix used for resume ends after the timeout and discarded receipt;
    // it contains neither a model answer nor a run terminal.
    let resumed = Shared {
        records: Arc::new(Mutex::new(cut)),
        ..Default::default()
    };
    let writer = resumed.writer();
    let scripted = Scripted::new(&resumed, ready(), vec![]);
    let model2 = crate::StubProvider::new(vec![model_events()]);
    crate::run_loop_on_writer(
        request(),
        &writer,
        crate::StartMode::FreshOrResume,
        &model2,
        &tools,
        &crate::AllowAll,
        &resumed,
        None,
        crate::RunControl {
            compaction_plane: Some(scripted.clone()),
            ..Default::default()
        },
        &out,
    )
    .await
    .unwrap();
    assert_eq!(scripted.setups.load(Ordering::SeqCst), 0);
    assert!(scripted.statuses.lock().unwrap().is_empty());
    assert_eq!(
        crate::canonical_request_bytes(&model.calls.lock().unwrap()[0]),
        crate::canonical_request_bytes(&model2.calls()[0])
    );
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn boundary_cancel_during_compaction_wait_exits_cancelled_without_recall() {
    let shared = Shared::default();
    let writer = shared.writer();
    let bind = bind();
    let inbox = Arc::new(CompactionInbox::default());
    let boundary = crate::SteerInbox::new();
    let control = crate::RunControl {
        steer: Some(boundary.clone()),
        ..Default::default()
    };
    let scripted = Scripted::new(
        &shared,
        ready(),
        vec![json!({"answer":"wait","reason":"work","bound_ms":60000})],
    );
    let plane: Arc<dyn CompactionPlane> = scripted.clone();
    let replies = runtime::CompactionReplies::new();
    let runtime = runtime::Runtime {
        provider: "provider",
        bind: &bind,
        plane: Some(&plane),
        inbox: Some(&inbox),
        writer: &writer,
        control: &control,
        replies: &replies,
    };
    let mut state = runtime.setup(setup(), &raw()).await.unwrap();
    let driver = async {
        loop {
            if shared.frames().iter().any(|frame| {
                matches!(
                    frame.record,
                    WalRecord::CompactionAnswer {
                        disposition: CompactionDisposition::Wait,
                        ..
                    }
                )
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
        boundary
            .request_boundary_cancel(&writer, &broca_protocol::RunId("r".into()))
            .await
            .unwrap();
    };
    let raw = raw();
    let (result, _) = tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        tokio::join!(
            runtime.step(&mut state, status(), &raw, Some(10), None),
            driver
        )
    })
    .await
    .expect("boundary cancellation must not wait out compaction's bound");
    assert!(matches!(
        result.unwrap(),
        runtime::StepResult::Terminal(RunFinishReason::Cancelled, None)
    ));
    assert_eq!(
        scripted.statuses.lock().unwrap().len(),
        1,
        "no compaction recall after cancellation"
    );
    assert!(shared.frames().iter().any(|frame| matches!(
        frame.record,
        WalRecord::CompactionWait {
            exited: CompactionWaitExit::Cancel,
            ..
        }
    )));
    writer.shutdown().await.unwrap();
}
