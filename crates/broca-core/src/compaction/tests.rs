//! Setup, the replay of what the removed synchronous client recorded, and the in-memory WAL the
//! asynchronous view tests share.

use super::*;
use crate::compaction_provider::setup::{SetupAnswer, SetupRequest};
use broca_protocol::{BindIdentity, CallOptions, FinishReason, ModelHandle, RunFinishReason};
use broca_wal::{CompactionCursor, CompactionDisposition, FramedRecord, Wal, WalError};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

pub(super) fn text(role: Role, text: &str) -> Message {
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
fn initial(range: Range, replacement: Vec<Value>) -> CompactionMessage {
    CompactionMessage::new("c", 1, range, replacement)
}
fn refused(message: CompactionMessage, raw: &[Message]) {
    assert_eq!(validate_initial(&message, raw), None, "{message:?}");
}
pub(super) fn call(id: &str) -> ContentBlock {
    ContentBlock::ToolCall(broca_protocol::ToolCall {
        tool_call_id: broca_protocol::ToolCallId(id.into()),
        tool_name: "t".into(),
        input: json!({}),
        thought_signature: None,
    })
}
pub(super) fn result(id: &str) -> ContentBlock {
    ContentBlock::ToolResult(broca_protocol::ToolResult {
        tool_call_id: broca_protocol::ToolCallId(id.into()),
        output: broca_protocol::ToolOutput::Text {
            text: "done".into(),
        },
        is_error: false,
    })
}

/// Setup's initial message is rendered as written, so it may not use what only the model or
/// the runner may write: a cache marker, a model origin or signed reasoning it did not copy
/// unchanged, a system message, or a split tool call.
#[test]
fn setup_initial_is_refused_when_broca_cannot_render_it_faithfully() {
    let head = |value: Value| initial(Range::new(0, 0), vec![value]);
    refused(
        head(
            json!({"role":"user","cache_prefix_blocks":1,"content":[{"type":"text","text":"head"}]}),
        ),
        &raw(),
    );
    for block in [
        json!({"type":"reasoning","text":"thinking","signature":"signed"}),
        json!({"type":"redacted_reasoning","data":"opaque"}),
    ] {
        refused(head(json!({"role":"assistant","content":[block]})), &raw());
    }
    refused(head(encoded(&text(Role::System, "head"))), &raw());
    for value in [
        json!({"role":"user","content":[],"mystery":1}),
        json!({"role":"user","content":[{"type":"image","data":"x"}]}),
        json!({"role":"user","content":[{"type":"text","text":"x","mystery":1}]}),
    ] {
        refused(head(value), &raw());
    }
    // A model origin survives only on an unchanged copy of the message it stands for.
    let mut original = text(Role::Assistant, "signed model answer");
    original.origin = Some(ModelHandle::new("p", "m"));
    let mut transcript = vec![original.clone(), text(Role::User, "tail")];
    broca_protocol::content::stamp_ordinals(&mut transcript);
    let mut carried = original.clone();
    carried.origin = None;
    refused(
        initial(Range::new(0, 1), vec![encoded(&carried)]),
        &transcript,
    );
    assert!(validate_initial(
        &initial(Range::new(0, 1), vec![encoded(&original)]),
        &transcript
    )
    .is_some());
    // Tool calls and results stay paired across the range.
    let mut transcript = vec![
        Message::new(Role::Assistant, vec![call("id")]),
        Message::new(Role::Tool, vec![result("id")]),
        text(Role::User, "tail"),
    ];
    broca_protocol::content::stamp_ordinals(&mut transcript);
    for message in [
        initial(Range::new(0, 1), vec![]),
        initial(
            Range::new(0, 2),
            vec![encoded(&Message::new(Role::Tool, vec![result("id")]))],
        ),
        initial(
            Range::new(0, 0),
            vec![
                encoded(&Message::new(Role::Assistant, vec![call("id")])),
                encoded(&Message::new(Role::Tool, vec![result("id")])),
            ],
        ),
        // Inverted, and past the transcript.
        initial(Range::new(2, 1), vec![]),
        initial(Range::new(0, 9), vec![]),
    ] {
        refused(message, &transcript);
    }
    assert!(validate_initial(
        &initial(
            Range::new(0, 2),
            vec![encoded(&transcript[0]), encoded(&transcript[1])]
        ),
        &transcript
    )
    .is_some());
}

pub(super) fn setup_record(ceiling: Option<&str>) -> WalRecord {
    let replacement = vec![text(Role::User, "head")];
    WalRecord::CompactionMessage {
        provider: "provider".into(),
        kind: CompactionMessageKind::Setup,
        compaction_id: "c".into(),
        version: 1,
        range: CompactionRange { from: 0, to: 0 },
        sha256: broca_wal::compaction_replacement_sha256(&replacement).unwrap(),
        replacement: Arc::new(replacement),
        request_id: None,
        cursor: None,
        call_when: None,
        stability: None,
        ceiling: ceiling.map(|json| OpaqueJson::from_json_text(json).unwrap()),
        tag_checkpoint: None,
        ride_wait_ms: None,
    }
}

/// Records the removed synchronous client wrote: Setup with frozen ranks, a step replacement
/// with its request id and cursor, and the answer and wait records around it.
fn synchronous_records() -> Vec<WalRecord> {
    let head = vec![text(Role::User, "head")];
    let summary = vec![text(Role::User, "summary")];
    vec![
        WalRecord::CompactionMessage {
            provider: "provider".into(),
            kind: CompactionMessageKind::Setup,
            compaction_id: "c".into(),
            version: 1,
            range: CompactionRange { from: 0, to: 0 },
            sha256: broca_wal::compaction_replacement_sha256(&head).unwrap(),
            replacement: Arc::new(head),
            request_id: None,
            cursor: None,
            call_when: Some(OpaqueJson::from_json_text(r#"{"default":0.8}"#).unwrap()),
            // A rank past the replacement's end is never declared.
            stability: Some(vec![
                CompactionStabilityRank { index: 0, rank: 7 },
                CompactionStabilityRank {
                    index: 99,
                    rank: 10,
                },
            ]),
            ceiling: None,
            tag_checkpoint: None,
            ride_wait_ms: None,
        },
        WalRecord::CompactionRequest {
            provider: "provider".into(),
            request_id: "r".into(),
        },
        WalRecord::CompactionMessage {
            provider: "provider".into(),
            kind: CompactionMessageKind::Step,
            compaction_id: "c".into(),
            version: 2,
            range: CompactionRange { from: 0, to: 3 },
            sha256: broca_wal::compaction_replacement_sha256(&summary).unwrap(),
            replacement: Arc::new(summary),
            request_id: Some("r".into()),
            cursor: Some(CompactionCursor {
                lineage_id: "lineage".into(),
                after_ordinal: Some(3),
            }),
            call_when: None,
            stability: None,
            ceiling: None,
            tag_checkpoint: None,
            ride_wait_ms: None,
        },
        WalRecord::CompactionAnswer {
            provider: "provider".into(),
            request_id: "late".into(),
            cursor: CompactionCursor {
                lineage_id: "lineage".into(),
                after_ordinal: Some(3),
            },
            disposition: CompactionDisposition::Late,
            rejected: None,
            wait: None,
        },
    ]
}

/// A session the synchronous client compacted still renders what it rendered: the newest
/// applied replacement in place of its range, the frozen ranks declared on it, and the raw tail
/// after it. Folding its records through an admission checkpoint keeps every one of them.
#[test]
fn recorded_synchronous_compaction_still_renders_and_survives_a_checkpoint() {
    let mut captured = crate::AdmissionCheckpointV1::empty();
    for (seq, record) in synchronous_records().iter().enumerate() {
        captured.fold_record(seq as u64 + 1, record).unwrap();
    }
    let decoded =
        crate::decode_admission_checkpoint(&crate::encode_admission_checkpoint(&captured).unwrap())
            .unwrap();
    assert_eq!(decoded, captured);
    let state = decoded.compaction.as_ref().unwrap().state().unwrap();
    assert_eq!(state.applied.as_ref().unwrap().version, 2);
    assert!(state.call_when.is_some());
    assert!(state.view.is_none());
    let options = CallOptions::new(raw());
    let view = model_options(&options, state).unwrap();
    assert_eq!(view.prompt.len(), 3);
    assert_eq!(view.prompt[0], options.prompt[0]);
    assert_eq!(view.prompt[1].ordinal, None);
    assert_eq!(view.prompt[1].content, text(Role::User, "summary").content);
    assert_eq!(view.prompt[2].ordinal, Some(3));
    assert_eq!(
        view.declared_markers,
        vec![broca_protocol::request::DeclaredMessageMarker {
            message_index: 1,
            rank: 7
        }]
    );
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

#[derive(Clone, Default)]
pub(super) struct Shared {
    records: Arc<Mutex<Vec<WalRecord>>>,
    durable: Arc<AtomicUsize>,
    lineage: Arc<Mutex<Option<broca_wal::WalLineageId>>>,
}

impl Shared {
    /// A WAL already holding `records`, with the identity the first append minted.
    pub(super) fn with_records(records: Vec<WalRecord>) -> Self {
        let shared = Self::default();
        *shared.records.lock().unwrap() = records;
        *shared.lineage.lock().unwrap() = Some(fixture_lineage_id());
        shared
    }
    pub(super) fn records(&self) -> Vec<WalRecord> {
        self.records.lock().unwrap().clone()
    }
    /// How many records were durable at the last sync.
    pub(super) fn durable(&self) -> usize {
        self.durable.load(Ordering::SeqCst)
    }
    fn frames(&self) -> Vec<FramedRecord> {
        self.records()
            .into_iter()
            .enumerate()
            .map(|(i, record)| FramedRecord {
                seq: i as u64 + 1,
                fence: 1,
                record,
            })
            .collect()
    }
    pub(super) fn writer(&self) -> crate::WalHandle {
        crate::WalHandle::spawn(Box::new(self.clone()), 1).unwrap()
    }
}
impl Wal for Shared {
    fn append(&mut self, _: u64, record: &WalRecord, _: bool) -> Result<u64, WalError> {
        let mut records = self.records.lock().unwrap();
        records.push(record.clone());
        // Like FileWal's legacy upgrade, the identity appears only after an append.
        self.lineage
            .lock()
            .unwrap()
            .get_or_insert_with(fixture_lineage_id);
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
    answer: Value,
    setups: AtomicUsize,
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
        let mut value = self.answer.clone();
        value["request_id"] = request.request_id.into();
        if value["answer"] == "never" {
            return std::future::pending().await;
        }
        serde_json::from_value(value).map_err(|_| CompactionUnavailable)
    }
}
fn ready() -> Value {
    json!({"answer":"ready","initial":{"compaction_id":"c","version":1,"range":{"from":0,"to":0},
        "replacement":[{"role":"user","content":[{"type":"text","text":"head"}]}]}})
}
pub(super) fn bind() -> BindIdentity {
    BindIdentity::new(std::path::PathBuf::from("/project"), "caller", "session")
}
fn setup_request() -> SetupRequest {
    SetupRequest::new("session", "caller", "setup", Map::new(), "model", 1)
}

async fn run_setup(
    answer: Value,
) -> (
    Result<CompactionState, Box<broca_protocol::ProviderError>>,
    Vec<WalRecord>,
) {
    run_setup_for(setup_request(), answer).await
}

async fn run_setup_for(
    request: SetupRequest,
    answer: Value,
) -> (
    Result<CompactionState, Box<broca_protocol::ProviderError>>,
    Vec<WalRecord>,
) {
    let shared = Shared::default();
    let writer = shared.writer();
    let control = crate::RunControl::default();
    let bind = bind();
    let plane: Arc<dyn CompactionPlane> = Arc::new(Scripted {
        answer,
        setups: AtomicUsize::new(0),
    });
    let runtime = runtime::Runtime {
        provider: "provider",
        bind: &bind,
        plane: Some(&plane),
        writer: &writer,
        control: &control,
    };
    let outcome = runtime.setup(request, &raw()).await;
    let _ = writer.shutdown().await;
    (outcome, shared.records())
}

/// A failed, timed-out or unrenderable Setup is `compaction_unavailable`; a refusal carries the
/// provider's role code and, beside it, its finer code. Neither records anything, so the next
/// send calls Setup again.
#[tokio::test]
async fn setup_failures_and_refusals_record_nothing() {
    let mut oversized = ready();
    oversized["initial"]["replacement"][0]["content"][0]["text"] =
        "x".repeat(MAX_REPLACEMENT_BYTES).into();
    let mut bad_ceiling = ready();
    bad_ceiling["ceiling"] = json!({"at_share": 1.5, "wait_ms": 1000});
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
        (bad_ceiling, None, Some("compaction_unavailable")),
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
        let (outcome, records) = run_setup(reply).await;
        let error = outcome.unwrap_err();
        assert_eq!(error.provider_code.as_deref(), code.or(provider_code));
        assert_eq!(
            error
                .refusal
                .as_ref()
                .and_then(|refusal| refusal.provider_detail_code.as_deref()),
            code.and(provider_code)
        );
        assert!(records.is_empty());
    }
}

/// A ready Setup records its initial message and its ceiling verbatim, durable before Setup
/// returns, behind the asynchronous compaction feature when a ceiling is present.
#[tokio::test]
async fn setup_records_its_initial_message_and_ceiling() {
    let mut answer = ready();
    answer["ceiling"] = json!({"at_share": 0.8, "on_overflow": true, "wait_ms": 1000});
    let (outcome, records) = run_setup(answer).await;
    let state = outcome.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].required_features(),
        &[
            broca_wal::framing::COMPACTION_VIEW_FEATURE,
            broca_wal::framing::COMPACTION_FEATURE
        ]
    );
    let policy = view::ceiling_policy(&state).unwrap();
    assert_eq!(policy.wait_ms, 1000);
    assert!(policy.on_overflow);
    let mut refolded = CompactionCheckpoint::None;
    refolded.fold(&records[0]);
    assert_eq!(refolded.state().unwrap().ceiling, state.ceiling);
    // Without a ceiling the record is the one the synchronous client wrote.
    let (_, records) = run_setup(ready()).await;
    assert_eq!(
        records[0].required_features(),
        &[broca_wal::framing::COMPACTION_FEATURE]
    );
}

/// compaction-provider 0.5.1 §4: a `ready` answer's `ride_wait_ms` within the cap broca sent
/// is frozen with the session, on the Setup record (which then needs
/// `compaction-ride-wait/v1`) and in the folded state. One that is 0, above the cap, or sent to
/// a request without a cap fails Setup as `compaction_unavailable` and records nothing; broca
/// never clamps it.
#[tokio::test]
async fn setup_freezes_a_ride_wait_within_the_cap_and_fails_any_other() {
    let capped = || setup_request().with_ride_wait_max_ms(runtime::RIDE_WAIT_MAX_MS);
    let mut answer = ready();
    answer["ride_wait_ms"] = json!(4000);
    let (outcome, records) = run_setup_for(capped(), answer).await;
    assert_eq!(outcome.unwrap().ride_wait_ms, Some(4000));
    assert_eq!(records.len(), 1);
    assert!(records[0]
        .required_features()
        .contains(&broca_wal::framing::COMPACTION_RIDE_WAIT_FEATURE));
    let mut refolded = CompactionCheckpoint::None;
    refolded.fold(&records[0]);
    assert_eq!(refolded.state().unwrap().ride_wait_ms, Some(4000));
    for (request, wait) in [
        (capped(), 0),
        (capped(), runtime::RIDE_WAIT_MAX_MS + 1),
        (setup_request(), 4000),
    ] {
        let mut answer = ready();
        answer["ride_wait_ms"] = json!(wait);
        let (outcome, records) = run_setup_for(request, answer).await;
        assert_eq!(
            outcome.unwrap_err().provider_code.as_deref(),
            Some("compaction_unavailable"),
            "ride_wait_ms {wait}"
        );
        assert!(records.is_empty());
    }
    // Without the member the record keeps the bytes and requirements it always had.
    let (outcome, records) = run_setup_for(capped(), ready()).await;
    assert_eq!(outcome.unwrap().ride_wait_ms, None);
    assert_eq!(
        records[0].required_features(),
        &[broca_wal::framing::COMPACTION_FEATURE]
    );
}

/// A ride that answers after `delay`, recording each request's deadline.
struct SlowRide {
    delay: std::time::Duration,
    deadlines: Mutex<Vec<u64>>,
}
#[async_trait::async_trait]
impl CompactionPlane for SlowRide {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        Err(CompactionUnavailable)
    }
    async fn ride(
        &self,
        _: &str,
        _: &BindIdentity,
        request: crate::compaction_provider::ride::RideRequest,
    ) -> Result<crate::compaction_provider::ride::RideAnswer, CompactionUnavailable> {
        self.deadlines.lock().unwrap().push(request.deadline_ms);
        tokio::time::sleep(self.delay).await;
        Ok(crate::compaction_provider::ride::RideAnswer::nothing())
    }
}

/// Ride once under `state` with a provider answering after `delay`: the deadline the request
/// carried and the records the ride wrote.
async fn ride_after(
    delay: std::time::Duration,
    state: &mut CompactionState,
) -> (Vec<u64>, Vec<WalRecord>) {
    let shared = Shared::default();
    let writer = shared.writer();
    let control = crate::RunControl::default();
    let bind = bind();
    let slow = Arc::new(SlowRide {
        delay,
        deadlines: Mutex::new(Vec::new()),
    });
    let plane: Arc<dyn CompactionPlane> = slow.clone();
    let runtime = runtime::Runtime {
        provider: "provider",
        bind: &bind,
        plane: Some(&plane),
        writer: &writer,
        control: &control,
    };
    let view = runtime
        .ride(
            state,
            broca_protocol::StepId(2),
            "0123456789abcdef0123456789abcdef",
            crate::compaction_provider::ride::RideCause::TtlExpired,
        )
        .await
        .unwrap();
    assert!(view.is_none());
    let _ = writer.shutdown().await;
    let deadlines = slow.deadlines.lock().unwrap().clone();
    (deadlines, shared.records())
}

/// llm-runner 0.5.2 §6, compaction-provider 0.5.1 §8: a rewrite ride waits the session's
/// frozen `ride_wait_ms`, sent as its deadline. One still unanswered after
/// `COMPACTION_WAIT_REPORT_AFTER_MS` is recorded as a runner-owned wait with its absolute
/// deadline, ended once the ride resolves, and the control lane reports it as
/// `compaction_wait`. A ride answered sooner records nothing, and a session whose Setup gave no
/// `ride_wait_ms` keeps broca's own short ride deadline.
#[tokio::test]
async fn a_slow_ride_is_reported_as_a_compaction_wait_and_a_fast_one_is_not() {
    let mut state = CompactionState {
        ride_wait_ms: Some(4000),
        ..Default::default()
    };
    let before = crate::compaction::now();
    let (deadlines, records) = ride_after(std::time::Duration::from_millis(1300), &mut state).await;
    assert_eq!(deadlines, vec![4000]);
    let [WalRecord::CompactionRideWait {
        step_id,
        deadline_ms,
    }, WalRecord::CompactionRideWaitEnded { step_id: ended }] = records.as_slice()
    else {
        panic!("a wait and its end: {records:?}");
    };
    assert_eq!(
        (*step_id, *ended),
        (broca_protocol::StepId(2), broca_protocol::StepId(2))
    );
    assert!(
        (before + 4000..=crate::compaction::now() + 4000).contains(deadline_ms),
        "the deadline is the ride's absolute end: {deadline_ms}"
    );
    assert_eq!(state.ride_wait, None, "the wait ended with the ride");

    let mut state = CompactionState::default();
    let (deadlines, records) = ride_after(std::time::Duration::from_millis(10), &mut state).await;
    assert!(
        records.is_empty(),
        "a quick answer is never reported: {records:?}"
    );
    assert_eq!(
        deadlines,
        vec![COMPACTION_CALL_BUDGET_MS.clamp(
            1,
            crate::compaction_provider::ride::MAX_REWRITE_RIDE_DEADLINE_MS
        )]
    );
}

/// A wait the synchronous client left open when the process stopped is sealed without calling
/// any provider: a recorded terminal intent ends the run with it, and an open wait with none
/// ends `interrupted`, recorded once.
#[tokio::test]
async fn recorded_waits_are_sealed_without_a_provider() {
    for exited in [
        None,
        Some(CompactionWaitExit::Cancel),
        Some(CompactionWaitExit::Stopped),
        Some(CompactionWaitExit::Exceeded),
    ] {
        let shared = Shared::default();
        let writer = shared.writer();
        let mut state = CompactionState::default();
        state.fold(&setup_record(None));
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
            let sealed = runtime::seal_recorded_wait(&writer, &mut state)
                .await
                .unwrap();
            assert!(matches!(sealed, Some(runtime::Terminal(reason, _)) if reason == expected));
        }
        assert_eq!(shared.records().len(), usize::from(exited.is_none()));
        state.fold(&WalRecord::RunFinished {
            interrupt_reason: None,
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

/// The replacement ceiling is broca's own, independent of configuration: an initial message
/// of 8 MiB is refused.
#[test]
fn replacement_ceiling_is_eight_mib_independent_of_configuration() {
    // The fixture must not derive its boundary from the implementation's constant:
    // increasing that constant must make the real validator accept this oversized input.
    let message = initial(
        Range::new(0, 0),
        vec![encoded(&text(Role::User, &"x".repeat(8 * 1024 * 1024)))],
    );
    refused(message, &raw());
}

/// A model page folds the captured compaction state with the WAL tail after it, renders the
/// newest replacement (here one the synchronous client recorded) in place of its range, and
/// refuses an `after_mid` cursor before reading anything.
#[test]
fn model_page_folds_snapshot_and_mid_turn_tail_and_refuses_after_mid() {
    let mut captured = CompactionCheckpoint::None;
    captured.fold(&setup_record(None));
    let raw = raw();
    let snapshot = crate::StorePageSnapshot {
        model_history: Some(crate::context::ModelHistory {
            spans: vec![],
            open_span: None,
            episodes: None,
            row_ranges: std::iter::once(0..raw.len() as u64).collect(),
            prefix_len: raw.len() as u64,
        }),
        prefix_len: raw.len() as u64,
        rows: raw
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, message)| crate::StorePageRow {
                written_at: None,
                run: None,
                ordinal: i as u64,
                message,
            })
            .collect(),
        compaction: Some(captured),
        lineage_id: Some("lineage".into()),
        ..crate::StorePageSnapshot::empty()
    };
    let mut update = synchronous_records().remove(2);
    if let WalRecord::CompactionMessage {
        replacement,
        sha256,
        ..
    } = &mut update
    {
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
        text(Role::User, "new view").content
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
    Arc::new(broca_wal::PlanManifest {
        plan: OpaqueJson::from_json_text(
            json!({"compaction_item":{"provider":"provider","preset":"head","params":{}},"composition":{}})
                .to_string(),
        )
        .unwrap(),
        plan_identity: "id".into(),
        composition_digest: "digest".into(),
        tools: Arc::new(vec![]),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    })
}
fn request() -> crate::RunRequest {
    let submission: broca_protocol::request::SubmissionRequest = serde_json::from_value(json!({
        "model":{"provider_module_id":"stub","model_id":"model"},
        "input":[{"role":"user","content":[{"type":"text","text":"user"}]}]
    }))
    .unwrap();
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

/// A real run with a compaction plan calls Setup once, records it and then a notice before the
/// model call. An admission or model page whose materialized summary lacks the compaction state
/// reads the complete WAL instead of rendering without Setup's message.
#[tokio::test]
async fn admission_and_model_page_absent_compaction_summary_force_complete_reads() {
    let shared = Shared::default();
    let writer = shared.writer();
    let scripted = Arc::new(Scripted {
        answer: ready(),
        setups: AtomicUsize::new(0),
    });
    let model = crate::StubProvider::new(vec![model_events()]);
    let (out, _events) = tokio::sync::mpsc::channel(128);
    let control = crate::RunControl {
        manifest: Some(manifest()),
        compaction_plane: Some(scripted.clone()),
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
    assert_eq!(scripted.setups.load(Ordering::SeqCst), 1);
    let frames = shared.frames();
    let position = |matches: fn(&WalRecord) -> bool| {
        frames
            .iter()
            .position(|frame| matches(&frame.record))
            .unwrap()
    };
    let setup = position(|r| matches!(r, WalRecord::CompactionMessage { .. }));
    let notice = position(|r| matches!(r, WalRecord::CompactionNotice { .. }));
    let model_call = position(|r| matches!(r, WalRecord::ModelStepFinished { .. }));
    assert!(setup < notice && notice < model_call);
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
    // The stored snapshot and the WAL agree on lineage, so the identity check passes and the
    // missing compaction summary is the only thing that can force the complete read.
    let lineage = crate::projection_plan_tests::incoming_lineage();
    struct Reads<'a>(&'a Shared, AtomicUsize, broca_wal::WalLineageId);
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
            let mut range = broca_wal::WalRangeRead::without_anchors(vec![]);
            range.lineage_id = Some(self.2);
            Ok(range)
        }
    }
    let store = Store(crate::AdmissionStoreSnapshot {
        watermark_seq: head,
        lineage_id: Some(lineage.to_string()),
        anchor: None,
        checkpoint: Some(checkpoint),
    });
    let reads = Reads(&shared, AtomicUsize::new(0), lineage);
    let snapshot = crate::load_prior_wal_snapshot(&reads, &bind(), head, Some(&store)).unwrap();
    assert_eq!(reads.1.load(Ordering::SeqCst), 1);
    assert_eq!(snapshot.base_watermark, 0);
    let state = snapshot
        .checkpoint_at_head
        .compaction
        .as_ref()
        .unwrap()
        .state()
        .unwrap()
        .clone();
    assert_eq!(state.applied.as_ref().unwrap().version, 1);
    // One notice at the step boundary, and one announcing the final answer before the end.
    assert_eq!(state.notice_seq, 2);
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

/// A model provider that binds signed reasoning to its prefix, as Anthropic's thinking does,
/// around the stub provider that records every request it is sent.
struct PrefixBound(crate::StubProvider);
#[async_trait::async_trait]
impl crate::Provider for PrefixBound {
    async fn step(
        &self,
        step_id: broca_protocol::StepId,
        opts: &CallOptions,
    ) -> tokio::sync::mpsc::Receiver<broca_protocol::NormalizedEvent> {
        self.0.step(step_id, opts).await
    }
    fn reasoning_is_prefix_bound(&self) -> bool {
        true
    }
}

/// The provider's one view of the session, its first message folded into a summary, announced
/// by a `view_ready` event and served by `view.get`. It also serves as the session's plane, so
/// the fetch goes through the run loop's real call.
#[derive(Default)]
struct OneView {
    /// `view.get` calls that answered the view.
    offered: AtomicUsize,
    /// Replace the first question with a copy of it from the raw transcript, which lacks the
    /// runner tag the request shows on its text, instead of a summary.
    copy_raw_question: bool,
    /// Serve a view whose bytes differ from the ones the `view_ready` event announced.
    tampered: bool,
    unavailable: bool,
    /// Answer `view.get` with this refusal code instead of the view.
    refuse: Option<&'static str>,
    /// Serve a view of another lineage, which is never a prefix of the run's transcript.
    foreign_lineage: bool,
    /// The lineage the run asked for, which the view names.
    lineage: Mutex<Option<String>>,
    /// For each `view.get` call: the version it asked for, and whether it came at a step
    /// boundary (the step's `StepStarted` durable, none of its model output yet).
    fetches: Mutex<Vec<(u64, bool)>>,
    /// The WAL the run writes, to see where each fetch falls.
    wal: Mutex<Option<Shared>>,
    /// Every `compaction.ride` request, and the view a ride answers with when set.
    rides: Mutex<Vec<crate::compaction_provider::ride::RideRequest>>,
    ride_answers: bool,
}
impl OneView {
    fn view(
        &self,
        session_ref: &str,
        text_of_fold: &str,
    ) -> crate::compaction_provider::view::CompactionView {
        use crate::compaction_provider::{lineage::MessageRef, view::*};
        let lineage = if self.foreign_lineage {
            "another-lineage".to_string()
        } else {
            self.lineage.lock().unwrap().clone().expect("asked first")
        };
        let fold = if self.copy_raw_question {
            vec![text(Role::User, "first question")]
        } else {
            vec![text(Role::User, text_of_fold)]
        };
        let covers = 0;
        CompactionView::new(
            session_ref,
            2,
            lineage,
            MessageRef {
                ordinal: covers,
                mid: format!("m{covers}"),
            },
            vec![ViewRange::replace(
                0,
                covers + 1,
                serde_json::to_string(&fold).unwrap(),
            )],
        )
    }
}
impl CompactionViews for OneView {
    fn newest_ready(
        &self,
        session_ref: &str,
        lineage_id: &str,
        after: Option<u64>,
    ) -> Option<crate::compaction_provider::notice::ViewReady> {
        if after.is_some_and(|applied| applied >= 2) {
            return None;
        }
        *self.lineage.lock().unwrap() = Some(lineage_id.into());
        Some(crate::compaction_provider::notice::ViewReady::for_view(
            &self.view(session_ref, "summary"),
        ))
    }
}
#[async_trait::async_trait]
impl CompactionPlane for OneView {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        request: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        let mut answer = ready();
        if self.copy_raw_question {
            // Setup inserts nothing, so the view's copy is the request's only change.
            answer["initial"]["replacement"] = json!([]);
        }
        answer["request_id"] = request.request_id.into();
        Ok(serde_json::from_value(answer).unwrap())
    }
    async fn view_get(
        &self,
        _: &str,
        _: &BindIdentity,
        request: crate::compaction_provider::view::ViewGetRequest,
    ) -> Result<crate::compaction_provider::view::ViewGetAnswer, CompactionUnavailable> {
        use crate::compaction_provider::view::ViewGetAnswer;
        // At a step boundary: the step's `StepStarted` is durable and nothing of the step's
        // model output is (Setup's own records may follow it on the first step).
        let at_boundary = self.wal.lock().unwrap().as_ref().is_some_and(|wal| {
            let records = wal.records();
            let last = |is: fn(&WalRecord) -> bool| records.iter().rposition(is);
            let started = last(|record| matches!(record, WalRecord::StepStarted { .. }));
            let finished = last(|record| matches!(record, WalRecord::ModelStepFinished { .. }));
            started.is_some() && started > finished
        });
        self.fetches
            .lock()
            .unwrap()
            .push((request.view_version, at_boundary));
        if self.unavailable {
            return Err(CompactionUnavailable);
        }
        if let Some(code) = self.refuse {
            return Ok(ViewGetAnswer::refuse(code, "not held"));
        }
        self.offered.fetch_add(1, Ordering::SeqCst);
        let fold = if self.tampered { "tampered" } else { "summary" };
        Ok(ViewGetAnswer::view(self.view(&request.session_ref, fold)))
    }
    async fn ride(
        &self,
        _: &str,
        _: &BindIdentity,
        request: crate::compaction_provider::ride::RideRequest,
    ) -> Result<crate::compaction_provider::ride::RideAnswer, CompactionUnavailable> {
        use crate::compaction_provider::ride::RideAnswer;
        let session_ref = request.session_ref.clone();
        self.rides.lock().unwrap().push(request);
        Ok(if self.ride_answers {
            RideAnswer::view(self.view(&session_ref, "ridden summary"))
        } else {
            RideAnswer::nothing()
        })
    }
}

/// A send carrying history with signed reasoning, on a plan that renders runner tags.
fn history_request() -> (crate::RunRequest, crate::RunControl, Arc<OneView>) {
    let submission: broca_protocol::request::SubmissionRequest = serde_json::from_value(json!({
        "model":{"provider_module_id":"stub","model_id":"model"},
        "input":[
            {"role":"user","content":[{"type":"text","text":"first question"}]},
            {"role":"assistant","origin":{"provider_module_id":"stub","model_id":"model"},
             "content":[{"type":"reasoning","text":"old thinking","signature":"old-signature"},
                        {"type":"text","text":"first answer"}]},
            {"role":"user","content":[{"type":"text","text":"second question"}]}
        ]
    }))
    .unwrap();
    let request = crate::RunRequest::from_submission(
        submission,
        broca_protocol::RunId("run".into()),
        bind(),
        broca_protocol::SubmissionId("sub".into()),
    );
    let mut plan = manifest().as_ref().clone();
    plan.plan = OpaqueJson::from_json_text(
        json!({"compaction_item":{"provider":"provider","preset":"head","params":{}},
            "composition":{},"render_tags":true})
        .to_string(),
    )
    .unwrap();
    let views = Arc::new(OneView::default());
    let control = crate::RunControl {
        manifest: Some(Arc::new(plan)),
        compaction_plane: Some(Arc::new(Scripted {
            answer: ready(),
            setups: AtomicUsize::new(0),
        })),
        compaction_views: Some(views.clone()),
        ..Default::default()
    };
    (request, control, views)
}

#[tokio::test]
async fn presetless_compaction_setup_omits_the_member_on_the_real_call() {
    struct DefaultSetup(AtomicUsize);
    #[async_trait::async_trait]
    impl CompactionPlane for DefaultSetup {
        async fn setup(
            &self,
            _: &str,
            _: &BindIdentity,
            request: SetupRequest,
        ) -> Result<SetupAnswer, CompactionUnavailable> {
            assert!(serde_json::to_value(&request)
                .unwrap()
                .get("preset")
                .is_none());
            self.0.fetch_add(1, Ordering::SeqCst);
            let mut answer = ready();
            answer["request_id"] = json!(request.request_id);
            Ok(serde_json::from_value(answer).unwrap())
        }
    }
    let shared = Shared::default();
    let writer = shared.writer();
    let (request, mut control, _) = history_request();
    let mut manifest = control.manifest.take().unwrap().as_ref().clone();
    manifest.plan = OpaqueJson::from_json_text(
        json!({"compaction_item":{"provider":"provider","params":{}},"composition":{}}).to_string(),
    )
    .unwrap();
    let plane = Arc::new(DefaultSetup(AtomicUsize::new(0)));
    control.manifest = Some(Arc::new(manifest));
    control.compaction_plane = Some(plane.clone());
    control.compaction_views = None;
    let model = PrefixBound(crate::StubProvider::new(vec![model_events()]));
    let (out, _events) = tokio::sync::mpsc::channel(256);
    crate::run_loop_on_writer(
        request,
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
    assert_eq!(plane.0.load(Ordering::SeqCst), 1);
    writer.shutdown().await.unwrap();
}

async fn run_history(shared: &Shared) -> (PrefixBound, Arc<OneView>) {
    run_history_with(shared, OneView::default()).await
}

async fn run_history_with(shared: &Shared, views: OneView) -> (PrefixBound, Arc<OneView>) {
    run_history_steps(shared, views, false).await
}

/// [`run_history_with`], or, with `two_steps`, a run whose first step calls a tool, so the
/// run passes two step boundaries.
async fn run_history_steps(
    shared: &Shared,
    views: OneView,
    two_steps: bool,
) -> (PrefixBound, Arc<OneView>) {
    let writer = shared.writer();
    let (mut request, mut control, _) = history_request();
    *views.wal.lock().unwrap() = Some(shared.clone());
    let views = Arc::new(views);
    control.compaction_plane = Some(views.clone());
    control.compaction_views = Some(views.clone());
    let scripts = if two_steps {
        request.tools = vec![serde_json::from_value(
            json!({"name":"edit","description":"edit","input_schema":{"type":"object"}}),
        )
        .unwrap()];
        request.policy_allowed.insert("edit".into());
        let mut answer = model_events();
        if let broca_protocol::NormalizedEvent::Control(
            broca_protocol::ProviderControl::AssistantMessage { message },
        ) = &mut answer[0]
        {
            message.message_id = broca_protocol::MessageId("b".into());
        }
        vec![tool_call_events("edit", 1), answer]
    } else {
        vec![model_events()]
    };
    let model = PrefixBound(crate::StubProvider::new(scripts));
    let tools = crate::StubToolPlane::new().with_tool("edit", |_| {
        crate::StubReply::Output(broca_protocol::content::ToolOutput::Text {
            text: "edited".into(),
        })
    });
    let (out, _events) = tokio::sync::mpsc::channel(256);
    crate::run_loop_on_writer(
        request,
        &writer,
        crate::StartMode::FreshOrResume,
        &model,
        &tools,
        &crate::AllowAll,
        shared,
        None,
        control,
        &out,
    )
    .await
    .unwrap();
    let _ = writer.shutdown().await;
    (model, views)
}

fn notice_seqs(records: &[WalRecord]) -> Vec<u64> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::CompactionNotice { seq, .. } => Some(*seq),
            _ => None,
        })
        .collect()
}

/// The run loop applies a provider's view at a step boundary with the companion strip of the
/// signed reasoning it invalidates, over the tagged prompt the model is sent. A crash after
/// the view is recorded, before or after its notice, resumes through replay and the run loop
/// and sends the same request bytes without offering the view again; the notices stay one
/// gapless sequence, and the run's final answer is announced before the run ends.
#[tokio::test]
async fn an_applied_view_resumes_through_replay_and_the_run_loop_byte_identically() {
    let shared = Shared::default();
    let (model, views) = run_history(&shared).await;
    assert_eq!(views.offered.load(Ordering::SeqCst), 1);
    let calls = model.0.calls();
    assert_eq!(calls.len(), 1);
    let live = crate::canonical_request_bytes(&calls[0]);
    let sent = String::from_utf8_lossy(&live).into_owned();
    assert!(sent.contains("summary"), "{sent}");
    assert!(!sent.contains("first question"), "the view replaced it");
    assert!(sent.contains('§'), "runner tags are rendered: {sent}");
    assert!(
        calls[0]
            .prompt
            .iter()
            .all(|message| !message.content.iter().any(view::is_prefix_bound)),
        "the invalidated signed reasoning is stripped"
    );
    let records = shared.records();
    let applied = records
        .iter()
        .position(|record| {
            matches!(
                record,
                WalRecord::CompactionViewApplied {
                    choice: broca_wal::CompactionViewChoice::Companion { .. },
                    ..
                }
            )
        })
        .expect("view applied with the companion strip");
    let finished = records
        .iter()
        .position(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
        .unwrap();
    let seqs = notice_seqs(&records);
    assert_eq!(seqs, vec![1, 2], "a boundary notice and the final answer's");
    let last_notice = records
        .iter()
        .rposition(|record| matches!(record, WalRecord::CompactionNotice { .. }))
        .unwrap();
    assert!(
        last_notice > finished,
        "the final answer is announced before the run ends"
    );
    for cut in applied + 1..finished {
        let resumed = Shared::with_records(records[..cut].to_vec());
        let (model, views) = run_history(&resumed).await;
        assert_eq!(
            views.offered.load(Ordering::SeqCst),
            0,
            "cut {cut}: a durable view was offered again"
        );
        let calls = model.0.calls();
        assert_eq!(calls.len(), 1, "cut {cut}");
        assert_eq!(
            crate::canonical_request_bytes(&calls[0]),
            live,
            "cut {cut}: resumed request bytes differ"
        );
        let after = resumed.records();
        assert_eq!(
            after
                .iter()
                .filter(|record| matches!(record, WalRecord::CompactionViewApplied { .. }))
                .count(),
            1,
            "cut {cut}"
        );
        let seqs = notice_seqs(&after);
        assert_eq!(
            seqs,
            (1..=seqs.len() as u64).collect::<Vec<_>>(),
            "cut {cut}"
        );
    }
}

/// A new lineage of a session whose earlier lineage retention deleted starts its notice numbers
/// at the number that lineage's tombstone recorded. The bus and the provider key notices on the
/// session, so they never take the new lineage's notices for repeats of the deleted lineage's.
/// The deleted lineage's recorded Setup answer did not survive it, so the new lineage calls
/// Setup again before its first model call (llm-runner 0.4.3 §11.1, compaction-provider §4).
#[tokio::test]
async fn a_new_lineage_numbers_its_notices_past_the_deleted_lineages() {
    let root = broca_test_support::scratch_root().join(format!(
        "broca-compaction-notice-floor-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let store = crate::FileWalStore::new(&root);
    let stone = store.tombstone_path(&bind(), "deleted-lineage");
    std::fs::create_dir_all(stone.parent().unwrap()).unwrap();
    std::fs::write(
        &stone,
        json!({"lineage_id":"deleted-lineage","expired_at_ms":1,"seq":1,"next_notice_seq":7})
            .to_string(),
    )
    .unwrap();
    let writer =
        crate::WalHandle::spawn(crate::WalStore::open(&store, &bind()).unwrap(), 1).unwrap();
    let model = crate::StubProvider::new(vec![model_events()]);
    let plane = Arc::new(Scripted {
        answer: ready(),
        setups: AtomicUsize::new(0),
    });
    let (out, _events) = tokio::sync::mpsc::channel(128);
    crate::run_loop_on_writer(
        request(),
        &writer,
        crate::StartMode::FreshOrResume,
        &model,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        &store,
        None,
        crate::RunControl {
            manifest: Some(manifest()),
            compaction_plane: Some(plane.clone()),
            ..Default::default()
        },
        &out,
    )
    .await
    .unwrap();
    writer.shutdown().await.unwrap();
    let records: Vec<WalRecord> = crate::WalStore::read_records(&store, &bind())
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    assert_eq!(notice_seqs(&records), vec![7, 8]);
    assert_eq!(
        plane.setups.load(Ordering::SeqCst),
        1,
        "Setup ran for the new lineage"
    );
    assert!(records.iter().any(|record| matches!(
        record,
        WalRecord::CompactionMessage {
            kind: broca_wal::CompactionMessageKind::Setup,
            ..
        }
    )));
    let _ = std::fs::remove_dir_all(&root);
}

/// The run loop decides a view over the prompt the model is sent, tags included. A view that
/// puts back the raw text of the first question changes the request there (the request showed
/// it with its runner tag), so the signed answer after it is invalidated: the view applies
/// with the companion strip and the old signature never goes out over the changed prefix.
#[tokio::test]
async fn the_run_loop_decides_a_view_over_the_tagged_prompt_it_sends() {
    let shared = Shared::default();
    let (model, views) = run_history_with(
        &shared,
        OneView {
            copy_raw_question: true,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(views.offered.load(Ordering::SeqCst), 1);
    assert!(shared.records().iter().any(|record| matches!(
        record,
        WalRecord::CompactionViewApplied {
            choice: broca_wal::CompactionViewChoice::Companion { .. },
            ..
        }
    )));
    let call = &model.0.calls()[0];
    assert_eq!(
        call.prompt[0].content,
        text(Role::User, "first question").content
    );
    assert!(
        call.prompt
            .iter()
            .all(|message| !message.content.iter().any(view::is_prefix_bound)),
        "the old signature is not sent over the changed first message"
    );
}

fn views_applied(records: &[WalRecord]) -> Vec<u64> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::CompactionViewApplied { view_version, .. } => Some(*view_version),
            _ => None,
        })
        .collect()
}

/// A `view_ready` event only announces a view: the run loop fetches it with `view.get` at the
/// step boundary, after the step's durable `StepStarted` and before its request, asking for
/// exactly the announced version, and applies it there.
#[tokio::test]
async fn an_announced_view_is_fetched_at_the_step_boundary_and_applied() {
    let shared = Shared::default();
    let ((model, views), logs) = broca_test_support::capture_logs(run_history(&shared)).await;
    assert_eq!(*views.fetches.lock().unwrap(), vec![(2, true)]);
    assert_eq!(views_applied(&shared.records()), vec![2]);
    let sent =
        String::from_utf8_lossy(&crate::canonical_request_bytes(&model.0.calls()[0])).into_owned();
    assert!(sent.contains("summary"), "{sent}");
    assert_step_decisions(&logs, &[(2, "applied", "")]);
}

fn assert_step_decisions(logs: &[broca_test_support::LogEvent], expected: &[(u64, &str, &str)]) {
    let session_ref = crate::agent_session_ref(&bind());
    let decisions: Vec<_> = logs
        .iter()
        .filter(|event| event.message() == "compaction view decided at a step boundary")
        .map(|event| {
            assert_eq!(event.target, "compaction");
            assert_eq!(event.level, tracing::Level::INFO);
            assert_eq!(event.fields.get("session_ref"), Some(&session_ref));
            (
                event.fields["view_version"].parse::<u64>().unwrap(),
                event.fields["choice"].as_str(),
                event.fields.get("superseded").map_or("", String::as_str),
            )
        })
        .collect();
    assert_eq!(decisions, expected, "one correlated INFO decision per view");
}

/// A fetched view whose bytes are not the ones its `view_ready` announced (its digest differs
/// from the event's) is rejected: never recorded, never rendered, and not fetched again at
/// the run's later boundary.
#[tokio::test]
async fn a_fetched_view_that_differs_from_its_announcement_is_never_applied() {
    let shared = Shared::default();
    let (model, views) = run_history_steps(
        &shared,
        OneView {
            tampered: true,
            ..Default::default()
        },
        true,
    )
    .await;
    assert_eq!(*views.fetches.lock().unwrap(), vec![(2, true)]);
    assert!(views_applied(&shared.records()).is_empty());
    assert_eq!(model.0.calls().len(), 2);
    for call in model.0.calls() {
        let sent = String::from_utf8_lossy(&crate::canonical_request_bytes(&call)).into_owned();
        assert!(!sent.contains("tampered"), "{sent}");
        assert!(
            sent.contains("head"),
            "Setup's head is still rendered: {sent}"
        );
    }
}

/// A provider that refuses `view.get` (it does not hold the announced version) leaves the run
/// on its current view; nothing is recorded for the view, and the run does not ask for that
/// version again at its later boundary.
#[tokio::test]
async fn a_refused_fetch_keeps_the_current_view() {
    let shared = Shared::default();
    let (model, views) = run_history_steps(
        &shared,
        OneView {
            refuse: Some("unknown_version"),
            ..Default::default()
        },
        true,
    )
    .await;
    assert_eq!(*views.fetches.lock().unwrap(), vec![(2, true)]);
    assert!(views_applied(&shared.records()).is_empty());
    assert_eq!(model.0.calls().len(), 2);
    for call in model.0.calls() {
        let sent = String::from_utf8_lossy(&crate::canonical_request_bytes(&call)).into_owned();
        assert!(sent.contains("head") && !sent.contains("summary"), "{sent}");
    }
}

/// A fetched view that is not a prefix of the transcript (here: of another lineage) is not
/// applied, and the run keeps it for its later boundaries instead of fetching it again.
#[tokio::test]
async fn a_view_that_is_not_a_prefix_is_kept_not_fetched_again() {
    let shared = Shared::default();
    let (model, views) = run_history_steps(
        &shared,
        OneView {
            foreign_lineage: true,
            ..Default::default()
        },
        true,
    )
    .await;
    assert_eq!(*views.fetches.lock().unwrap(), vec![(2, true)]);
    assert!(views_applied(&shared.records()).is_empty());
    assert_eq!(model.0.calls().len(), 2);
}

/// A later send of the session under another model rewrites the request's prefix anyway, so
/// the run rides the provider for a view (`compaction.ride`, cause `model_change`, within the
/// short rewrite deadline) and applies the view it answers at that boundary. The first send,
/// with no model announced before it, rides nothing.
#[tokio::test]
async fn a_model_change_rides_for_a_view_and_applies_it() {
    let shared = Shared::default();
    let views = Arc::new(OneView {
        ride_answers: true,
        ..Default::default()
    });
    let control = || crate::RunControl {
        manifest: Some(manifest()),
        compaction_plane: Some(views.clone()),
        // A call budget above the rewrite deadline: the ride still waits no longer than the
        // contract's short rewrite deadline.
        compaction_config: crate::compaction::CompactionConfig {
            call_budget_ms: 60_000,
        },
        ..Default::default()
    };
    for (index, model_id) in ["model", "other"].into_iter().enumerate() {
        let submission: broca_protocol::request::SubmissionRequest =
            serde_json::from_value(json!({
                "model":{"provider_module_id":"stub","model_id":model_id},
                "input":[{"role":"user","content":[{"type":"text","text":"first question"}]}]
            }))
            .unwrap();
        let request = crate::RunRequest::from_submission(
            submission,
            broca_protocol::RunId(format!("run-{index}")),
            bind(),
            broca_protocol::SubmissionId(format!("sub-{index}")),
        );
        let writer = shared.writer();
        if index == 0 {
            // The ride's view names the lineage the transcript counts in.
            let model = crate::StubProvider::new(vec![model_events()]);
            let (out, _events) = tokio::sync::mpsc::channel(256);
            crate::run_loop_on_writer(
                request,
                &writer,
                crate::StartMode::FreshOrResume,
                &model,
                &crate::StubToolPlane::new(),
                &crate::AllowAll,
                &shared,
                None,
                control(),
                &out,
            )
            .await
            .unwrap();
            assert!(
                views.rides.lock().unwrap().is_empty(),
                "no model to change from"
            );
            *views.lineage.lock().unwrap() =
                Some(writer.lineage_id().expect("lineage").to_string());
        } else {
            let model = crate::StubProvider::new(vec![model_events()]);
            let (out, _events) = tokio::sync::mpsc::channel(256);
            crate::run_loop_on_writer(
                request,
                &writer,
                crate::StartMode::AppendEpisode,
                &model,
                &crate::StubToolPlane::new(),
                &crate::AllowAll,
                &shared,
                None,
                control(),
                &out,
            )
            .await
            .unwrap();
            let sent = String::from_utf8_lossy(&crate::canonical_request_bytes(&model.calls()[0]))
                .into_owned();
            assert!(sent.contains("ridden summary"), "{sent}");
        }
        let _ = writer.shutdown().await;
    }
    let rides = views.rides.lock().unwrap().clone();
    assert_eq!(rides.len(), 1);
    assert_eq!(
        rides[0].cause,
        crate::compaction_provider::ride::RideCause::ModelChange
    );
    assert!(rides[0].check().is_ok(), "{:?}", rides[0]);
    assert_eq!(
        rides[0].deadline_ms,
        crate::compaction_provider::ride::MAX_REWRITE_RIDE_DEADLINE_MS
    );
    let records = shared.records();
    assert_eq!(views_applied(&records), vec![2]);
    let last_notice = records
        .iter()
        .rev()
        .find_map(|record| match record {
            WalRecord::CompactionNotice { headers, .. } => Some(headers.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(last_notice["applied_view_version"], "2");
    assert_eq!(last_notice["model"], "other");
}

/// Serves views 1 and 2 of one session. The `view.get` for version 1 signals `fetching_first`
/// and waits for `release` before it answers, so a test can announce version 2 while the
/// decision for version 1 is in flight.
struct RacingViews {
    fetching_first: tokio::sync::Notify,
    release: tokio::sync::Notify,
    fetches: Mutex<Vec<u64>>,
}
impl RacingViews {
    fn view(session_ref: &str, version: u64) -> crate::compaction_provider::view::CompactionView {
        use crate::compaction_provider::{lineage::MessageRef, view::*};
        let fold = vec![text(Role::User, &format!("summary {version}"))];
        CompactionView::new(
            session_ref,
            version,
            fixture_lineage_id().to_string(),
            MessageRef {
                ordinal: 0,
                mid: "m0".into(),
            },
            vec![ViewRange::replace(
                0,
                1,
                serde_json::to_string(&fold).unwrap(),
            )],
        )
    }
}
#[async_trait::async_trait]
impl CompactionPlane for RacingViews {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        Err(CompactionUnavailable)
    }
    async fn view_get(
        &self,
        _: &str,
        _: &BindIdentity,
        request: crate::compaction_provider::view::ViewGetRequest,
    ) -> Result<crate::compaction_provider::view::ViewGetAnswer, CompactionUnavailable> {
        self.fetches.lock().unwrap().push(request.view_version);
        if request.view_version == 1 {
            self.fetching_first.notify_one();
            self.release.notified().await;
        }
        Ok(crate::compaction_provider::view::ViewGetAnswer::view(
            Self::view(&request.session_ref, request.view_version),
        ))
    }
}

/// Two views announced close together are applied once, as the newest. The idle boundary
/// starts deciding version 1; version 2 is receipted and announced while version 1 is being
/// fetched; the boundary then discards version 1 as superseded and applies version 2 alone, so
/// the session's prefix is rewritten once rather than twice.
#[tokio::test]
async fn idle_boundary_applies_only_the_newest_view_announced_during_its_fetch() {
    use crate::compaction_provider::notice::ViewReady;
    let mut raw = vec![
        text(Role::User, &"large history ".repeat(2000)),
        text(Role::Assistant, "answer"),
        text(Role::User, "next"),
    ];
    broca_protocol::content::stamp_ordinals(&mut raw);
    let options = CallOptions::new(raw.clone());
    let shared = Shared::with_records(vec![setup_record(Some("{}"))]);
    let writer = shared.writer();
    writer.sync().await.unwrap();
    let session_ref = crate::agent_session_ref(&bind());
    let first = ViewReady::for_view(&RacingViews::view(&session_ref, 1));
    let second = ViewReady::for_view(&RacingViews::view(&session_ref, 2));
    let inbox = Arc::new(crate::ViewReadyInbox::default());
    inbox.announce(first.clone());
    let racing = Arc::new(RacingViews {
        fetching_first: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        fetches: Mutex::new(Vec::new()),
    });
    let plane: Arc<dyn CompactionPlane> = racing.clone();
    let mut state = CompactionState::default();
    for record in shared.records() {
        state.fold(&record);
    }
    let views: Arc<dyn CompactionViews> = inbox.clone();
    let session = bind();
    let (applied, ()) = tokio::join!(
        runtime::apply_idle_view(
            &session,
            &writer,
            &plane,
            Some(views),
            &first,
            &mut state,
            &raw,
            &raw,
            false,
            "model",
            Some(100_000),
            |state| Ok(
                serde_json::to_vec(&model_options(&options, state).unwrap().prompt)
                    .unwrap()
                    .len()
            ),
        ),
        async {
            // Hold the decision for version 1 at its fetch and announce version 2 to the inbox,
            // as the bus intake does once version 2's receipt is durable.
            racing.fetching_first.notified().await;
            assert!(inbox.announce(second.clone()));
            racing.release.notify_one();
        }
    );
    assert!(applied.unwrap(), "the newest view was not applied");
    assert_eq!(
        views_applied(&shared.records()),
        vec![2],
        "exactly one applied view, the newest"
    );
    assert_eq!(*racing.fetches.lock().unwrap(), vec![1, 2]);
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn step_boundary_logs_applied_and_superseded_views_with_session_ref() {
    use crate::compaction_provider::notice::ViewReady;
    let mut raw = vec![
        text(Role::User, &"large history ".repeat(2000)),
        text(Role::Assistant, "answer"),
        text(Role::User, "next"),
    ];
    broca_protocol::content::stamp_ordinals(&mut raw);
    let shared = Shared::with_records(vec![setup_record(Some("{}"))]);
    let writer = shared.writer();
    let session = bind();
    let session_ref = crate::agent_session_ref(&session);
    let inbox = Arc::new(crate::ViewReadyInbox::default());
    inbox.announce(ViewReady::for_view(&RacingViews::view(&session_ref, 1)));
    let racing = Arc::new(RacingViews {
        fetching_first: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        fetches: Mutex::new(Vec::new()),
    });
    let plane: Arc<dyn CompactionPlane> = racing.clone();
    let control = crate::RunControl {
        compaction_views: Some(inbox.clone()),
        ..Default::default()
    };
    let runtime = runtime::Runtime {
        provider: "provider",
        bind: &session,
        plane: Some(&plane),
        writer: &writer,
        control: &control,
    };
    let mut state = CompactionState::default();
    for record in shared.records() {
        state.fold(&record);
    }
    let mut boundary = runtime::BoundaryViews::default();
    let lineage = fixture_lineage_id().to_string();
    let ((), logs) = broca_test_support::capture_logs(async {
        broca_test_support::wait_for(
            "step boundary deciding the newest view",
            async {
                let (result, ()) = tokio::join!(
                    runtime.views_at_boundary(
                        &mut boundary,
                        &mut state,
                        Some(inbox.as_ref()),
                        &session_ref,
                        &lineage,
                        "model",
                        &raw,
                        super::StoredTranscript::plain(&raw),
                        false,
                        true,
                        runtime::StepAt {
                            run_id: "run",
                            step: 1,
                            superseded: &[]
                        }
                    ),
                    async {
                        racing.fetching_first.notified().await;
                        inbox.announce(ViewReady::for_view(&RacingViews::view(&session_ref, 2)));
                        racing.release.notify_one();
                    }
                );
                result.unwrap();
            },
            || format!("fetches={:?}", racing.fetches.lock().unwrap()),
        )
        .await;
    })
    .await;
    assert_eq!(views_applied(&shared.records()), vec![2]);
    assert_eq!(*racing.fetches.lock().unwrap(), vec![1, 2]);
    assert_step_decisions(&logs, &[(1, "superseded", ""), (2, "applied", "1")]);
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn idle_view_records_lower_estimate_and_repairs_every_cut_before_wake() {
    let mut raw = vec![
        text(Role::User, &"large history ".repeat(2000)),
        text(Role::Assistant, "answer"),
        text(Role::User, "next"),
    ];
    broca_protocol::content::stamp_ordinals(&mut raw);
    let options = CallOptions::new(raw.clone());
    let shared = Shared::with_records(vec![setup_record(Some("{}"))]);
    let writer = shared.writer();
    writer.sync().await.unwrap();
    let plane = Arc::new(OneView::default());
    let ready = plane
        .newest_ready(
            &crate::agent_session_ref(&bind()),
            &fixture_lineage_id().to_string(),
            None,
        )
        .unwrap();
    let size = |state: &CompactionState| -> Result<usize, String> {
        Ok(
            serde_json::to_vec(&model_options(&options, state).unwrap().prompt)
                .unwrap()
                .len(),
        )
    };
    let apply = |shared: Shared,
                 writer: crate::WalHandle,
                 plane: Arc<OneView>,
                 ready: crate::compaction_provider::notice::ViewReady,
                 raw: Vec<Message>,
                 options: CallOptions| async move {
        let mut state = CompactionState::default();
        for record in shared.records() {
            state.fold(&record);
        }
        let plane: Arc<dyn CompactionPlane> = plane;
        assert!(runtime::apply_idle_view(
            &bind(),
            &writer,
            &plane,
            None,
            &ready,
            &mut state,
            &raw,
            &raw,
            false,
            "model",
            Some(100_000),
            |state| Ok(
                serde_json::to_vec(&model_options(&options, state).unwrap().prompt)
                    .unwrap()
                    .len()
            )
        )
        .await
        .unwrap());
        assert_eq!(shared.durable(), shared.records().len());
        state
    };
    let mut initial = CompactionState::default();
    initial.fold(&setup_record(Some("{}")));
    let before = size(&initial).unwrap() as u64 / 2;
    let state = apply(
        shared.clone(),
        writer,
        plane.clone(),
        ready.clone(),
        raw.clone(),
        options.clone(),
    )
    .await;
    assert!(state.announced.as_ref().unwrap().est_tokens.unwrap() < before / 2);
    let baseline = model_options(&options, &state).unwrap();
    let records = shared.records();
    assert!(matches!(
        records[1],
        WalRecord::CompactionViewApplied { .. }
    ));
    assert!(matches!(records[2], WalRecord::CompactionNotice { .. }));
    for cut in 1..=records.len() {
        let shared = Shared::with_records(records[..cut].to_vec());
        let writer = shared.writer();
        let calls = plane.fetches.lock().unwrap().len();
        let state = apply(
            shared.clone(),
            writer,
            plane.clone(),
            ready.clone(),
            raw.clone(),
            options.clone(),
        )
        .await;
        assert_eq!(
            model_options(&options, &state).unwrap(),
            baseline,
            "cut {cut}"
        );
        assert_eq!(
            state.announced.as_ref().unwrap().applied_view_version,
            Some(2)
        );
        if cut >= 2 {
            assert_eq!(
                plane.fetches.lock().unwrap().len(),
                calls,
                "recorded view fetched again at cut {cut}"
            );
        }
    }
}

/// A compaction session never sends a request broca counts over the model's context window:
/// the run ends before the model call, and the boundary's notice, durable first, tells the
/// provider how large the request is. A session without compaction sends the same request.
#[tokio::test]
async fn a_request_over_the_window_is_refused_before_it_is_sent() {
    for compaction in [true, false] {
        let shared = Shared::default();
        let writer = shared.writer();
        let model = crate::StubProvider::new(vec![model_events()]);
        let (out, _events) = tokio::sync::mpsc::channel(256);
        let mut control = crate::RunControl {
            admitted_context_limit: Some(4),
            ..Default::default()
        };
        if compaction {
            control.manifest = Some(manifest());
            control.compaction_plane = Some(Arc::new(Scripted {
                answer: ready(),
                setups: AtomicUsize::new(0),
            }));
        }
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
        let _ = writer.shutdown().await;
        let records = shared.records();
        if !compaction {
            assert_eq!(model.calls().len(), 1, "no compaction: sent as before");
            assert_eq!(summary.reason, RunFinishReason::Completed);
            continue;
        }
        assert!(model.calls().is_empty(), "never sent over the window");
        assert_eq!(summary.reason, RunFinishReason::ContextCeiling);
        let state = crate::reconcile_run_state(records.iter(), "run").unwrap();
        assert!(
            state.error.is_none(),
            "a runner ceiling is not a provider error"
        );
        let detail = state.context_ceiling.expect("the run's ceiling detail");
        assert_eq!(detail.cause, "over_window");
        assert_eq!(detail.window, 4);
        assert!(detail.size > detail.window);
        let WalRecord::CompactionNotice { headers, .. } = records
            .iter()
            .find(|record| matches!(record, WalRecord::CompactionNotice { .. }))
            .expect("the boundary notice")
        else {
            unreachable!()
        };
        assert_eq!(headers["window"], "4");
        assert!(headers["est_tokens"].parse::<u64>().unwrap() > 4);
    }
}

/// The inbox keeps the newest `view_ready` per session: an older or repeated event (the bus
/// may redeliver) changes nothing, and a boundary is answered only an event above the view it
/// renders.
#[test]
fn the_view_ready_inbox_keeps_the_newest_event_per_session() {
    use crate::compaction_provider::notice::ViewReady;
    let inbox = runtime::ViewReadyInbox::default();
    let session = "0123456789abcdef0123456789abcdef";
    let ready = |version: u64| ViewReady::new(session, version, 3, "ab".repeat(32));
    assert!(inbox.announce(ready(2)));
    assert!(!inbox.announce(ready(1)));
    assert!(!inbox.announce(ready(2)));
    assert_eq!(inbox.newest_ready(session, "l", None), Some(ready(2)));
    assert_eq!(inbox.newest_ready(session, "l", Some(1)), Some(ready(2)));
    assert_eq!(inbox.newest_ready(session, "l", Some(2)), None);
    assert_eq!(inbox.newest_ready("another", "l", None), None);
    assert!(inbox.announce(ready(3)));
    assert_eq!(inbox.newest_ready(session, "l", Some(2)), Some(ready(3)));
    inbox.forget_ready(&ready(2));
    assert_eq!(inbox.newest_ready(session, "l", None), Some(ready(3)));
    inbox.forget_ready(&ready(3));
    assert_eq!(inbox.newest_ready(session, "l", None), None);
    assert!(inbox.announce(ready(3)));
    inbox.forget(session);
    assert_eq!(inbox.newest_ready(session, "l", None), None);
}

#[test]
fn ready_inbox_is_bounded_and_cleanup_cannot_drop_a_newer_delivery() {
    use crate::compaction_provider::notice::ViewReady;
    let inbox = runtime::ViewReadyInbox::default();
    let ready = |session: u64, version| {
        ViewReady::new(format!("{session:032x}"), version, 0, "a".repeat(64))
    };
    for session in 0..1000 {
        assert!(inbox.announce(ready(session, 1)));
    }
    assert!(!inbox.announce(ready(1000, 1)));
    assert!(inbox.announce(ready(0, 2)));
    inbox.forget_ready(&ready(0, 1));
    assert_eq!(
        inbox.newest_ready(&ready(0, 1).session_ref, "ignored", None),
        Some(ready(0, 2))
    );
    inbox.forget_ready(&ready(0, 2));
    assert!(inbox.announce(ready(1000, 1)));
}

/// A WAL store that counts how often admission asks it for the notice floor.
struct CountingFloor {
    inner: Shared,
    floor_reads: AtomicUsize,
}
impl crate::WalStore for CountingFloor {
    fn open(&self, session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        crate::WalStore::open(&self.inner, session)
    }
    fn read_records(&self, session: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        crate::WalStore::read_records(&self.inner, session)
    }
    fn retired_notice_floor(&self, _: &BindIdentity) -> Result<u64, WalError> {
        self.floor_reads.fetch_add(1, Ordering::SeqCst);
        Ok(1)
    }
}

async fn floor_reads(control: crate::RunControl) -> usize {
    let store = CountingFloor {
        inner: Shared::default(),
        floor_reads: AtomicUsize::new(0),
    };
    let writer = store.inner.writer();
    let model = crate::StubProvider::new(vec![model_events()]);
    let (out, _events) = tokio::sync::mpsc::channel(128);
    crate::run_loop_on_writer(
        request(),
        &writer,
        crate::StartMode::FreshOrResume,
        &model,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        &store,
        None,
        control,
        &out,
    )
    .await
    .unwrap();
    let _ = writer.shutdown().await;
    store.floor_reads.load(Ordering::SeqCst)
}

/// Admission reads the retention tombstones for the notice floor only for a session whose plan
/// names a compaction provider: a session without a plan, or with a plan that names none, does
/// exactly the I/O it did before compaction notices existed.
#[tokio::test]
async fn only_a_compaction_session_reads_the_notice_floor() {
    assert_eq!(floor_reads(crate::RunControl::default()).await, 0);
    // A session with a plan that names no compaction provider.
    let mut plain = manifest().as_ref().clone();
    plain.plan = OpaqueJson::from_json_text(json!({"composition":{}}).to_string()).unwrap();
    assert_eq!(
        floor_reads(crate::RunControl {
            manifest: Some(Arc::new(plain)),
            ..Default::default()
        })
        .await,
        0
    );
    assert_eq!(
        floor_reads(crate::RunControl {
            manifest: Some(manifest()),
            compaction_plane: Some(Arc::new(Scripted {
                answer: ready(),
                setups: AtomicUsize::new(0),
            })),
            ..Default::default()
        })
        .await,
        1
    );
}

/// The stub provider, answering with a structured-output run frozen in its render config
/// when `submit` is set, as a provider admitted with `submit_tool` does.
struct EndingModel {
    stub: crate::StubProvider,
    submit: bool,
}
#[async_trait::async_trait]
impl crate::Provider for EndingModel {
    async fn step(
        &self,
        step_id: broca_protocol::StepId,
        opts: &CallOptions,
    ) -> tokio::sync::mpsc::Receiver<broca_protocol::NormalizedEvent> {
        self.stub.step(step_id, opts).await
    }
    fn render_config_blob(&self) -> Value {
        if self.submit {
            json!({"render_config":"stateless","structured_output":{
                "mode":"submit_tool","name":"result","schema_jcs":"{}",
                "enforcement":"best_effort","delivery":"unconstrained"}})
        } else {
            json!({"render_config":"stateless"})
        }
    }
}

/// One model step that calls `tool` with `input_tokens` of reported usage.
fn tool_call_events(tool: &str, input_tokens: u64) -> Vec<broca_protocol::NormalizedEvent> {
    use broca_protocol::{NormalizedEvent, ProviderControl};
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: broca_protocol::AssistantMessage {
                message_id: broca_protocol::MessageId("a".into()),
                content: vec![ContentBlock::ToolCall(broca_protocol::ToolCall {
                    tool_call_id: broca_protocol::ToolCallId("call-1".into()),
                    tool_name: tool.into(),
                    input: json!({"answer": 42}),
                    thought_signature: None,
                })],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(input_tokens),
                output_tokens: Some(1),
                ..Usage::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

/// Run a compaction session whose one model step calls `tool`, with the request changed by
/// `adjust`, and return its WAL and how the run ended.
async fn ending_run(
    tool: &str,
    input_tokens: u64,
    submit: bool,
    adjust: impl FnOnce(&mut Value),
) -> (Vec<WalRecord>, RunFinishReason) {
    let mut submission = json!({
        "model":{"provider_module_id":"stub","model_id":"model"},
        "input":[{"role":"user","content":[{"type":"text","text":"user"}]}],
        "tools":[{"name":"edit","description":"edit","input_schema":{"type":"object"}}]
    });
    adjust(&mut submission);
    let mut request = crate::RunRequest::from_submission(
        serde_json::from_value(submission).unwrap(),
        broca_protocol::RunId("run".into()),
        bind(),
        broca_protocol::SubmissionId("sub".into()),
    );
    request.policy_allowed.insert("edit".into());
    let shared = Shared::default();
    let writer = shared.writer();
    let model = EndingModel {
        stub: crate::StubProvider::new(vec![tool_call_events(tool, input_tokens)]),
        submit,
    };
    let (out, _events) = tokio::sync::mpsc::channel(128);
    let summary = crate::run_loop_on_writer(
        request,
        &writer,
        crate::StartMode::FreshOrResume,
        &model,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        &shared,
        None,
        crate::RunControl {
            manifest: Some(manifest()),
            compaction_plane: Some(Arc::new(Scripted {
                answer: ready(),
                setups: AtomicUsize::new(0),
            })),
            ..Default::default()
        },
        &out,
    )
    .await
    .unwrap();
    let _ = writer.shutdown().await;
    (shared.records(), summary.reason)
}

/// A run that ends at a hard stop announces the step it ended on (the model's tool call and the
/// synthetic results the stop writes) before its terminal, in the stop's own barrier.
fn assert_closing_notice(records: &[WalRecord], kind: &str) {
    let finished = records
        .iter()
        .rposition(|record| matches!(record, WalRecord::RunFinished { .. }))
        .unwrap_or_else(|| panic!("{kind}: no terminal"));
    let last_result = records
        .iter()
        .rposition(|record| matches!(record, WalRecord::ToolResult { .. }))
        .unwrap_or_else(|| panic!("{kind}: no synthetic result"));
    let notice = records
        .iter()
        .rposition(|record| matches!(record, WalRecord::CompactionNotice { .. }))
        .unwrap_or_else(|| panic!("{kind}: no notice"));
    assert!(
        last_result < notice && notice < finished,
        "{kind}: the closing notice follows the step's results and precedes the terminal"
    );
    let WalRecord::CompactionNotice { headers, .. } = &records[notice] else {
        unreachable!()
    };
    // The user message, the tool call and its result: ordinals 0, 1, 2.
    assert_eq!(headers["through_ordinal"], "2", "{kind}");
    let seqs = notice_seqs(records);
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>(), "{kind}");
}

#[tokio::test]
async fn a_max_steps_stop_with_pending_calls_announces_its_step() {
    let (records, reason) = ending_run("edit", 1, false, |request| {
        request["stop_when"] = json!([{"type":"step_count","n":1}]);
    })
    .await;
    assert_eq!(reason, RunFinishReason::MaxSteps);
    assert_closing_notice(&records, "max steps");
}

#[tokio::test]
async fn a_budget_stop_with_pending_calls_announces_its_step() {
    let (records, reason) = ending_run("edit", 1_000, false, |request| {
        request["budget"] = json!({"max_input_tokens": 10});
    })
    .await;
    assert_eq!(reason, RunFinishReason::BudgetExhausted);
    assert_closing_notice(&records, "budget");
}

#[tokio::test]
async fn a_structured_output_submit_announces_its_step() {
    let (records, reason) = ending_run(
        broca_protocol::structured_output::SUBMIT_RESULT_TOOL,
        1,
        true,
        |_| {},
    )
    .await;
    assert_eq!(reason, RunFinishReason::Completed);
    assert!(records
        .iter()
        .any(|record| matches!(record, WalRecord::StructuredOutputResult { .. })));
    assert_closing_notice(&records, "structured output submit");
}

#[path = "ceiling_tests.rs"]
mod ceiling_tests;
