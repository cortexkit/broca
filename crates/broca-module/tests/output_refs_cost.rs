//! What `session.output_refs` and `session.output_ref_read` cost on a long session, and that a
//! reference a compaction view had folded out of the writing request stays unknown.
//!
//! Output references keep no copy of anything: every read derives a message's records from the
//! session log through the `session.read` path. These tests seed a tagged session of tool
//! episodes straight into the WAL, materialize it into the store, and ask the registry for the
//! final reply's records and one record's content. The final reply names its own episode's tool
//! output (in its request) and the first episode's (folded out of its request when the session
//! has a compaction view, in it when it has none).
//!
//! The measurement runs only when asked for:
//! `CARGO_PROFILE_DEV_OPT_LEVEL=3 OUTPUT_REFS_COST_MESSAGES=20000 cargo test -p broca-module --test output_refs_cost -- --ignored --nocapture`
//! (optimized, but with debug assertions: a release build refuses broca-wal's test-only feature).
//! It prints wall time and the heap's high-water mark above its level at the call's start.

// A counting allocator needs `unsafe`: `GlobalAlloc` is an unsafe trait. It is confined to this
// measurement binary, which is never shipped.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use broca_core::{
    FileLeaseStore, FileWalStore, OutputEvent, Persister, RunError, RunSummary, WalStore,
};
use broca_module::executor::{RunExecutor, RunJob};
use broca_module::{DefaultOpener, LineageRegistry, ServeHandler};
use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{
    AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult,
};
use broca_protocol::request::{ExecutionMode, ModelHandle, Tool, ToolChoice};
use broca_protocol::{BatchId, BindIdentity, FinishReason, MessageId, RunId, StepId, ToolCallId};
use broca_wal::{DispatchOutcome, OpaqueJson, RunConfig, WalRecord};
use cortexkit_role_llm_runner::output_refs::{ListRequest, ReadRequest};
use serde_json::json;
use tokio::sync::mpsc;

/// The heap in use and its high-water mark, counted around the system allocator.
struct Counting;

static IN_USE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            let now = IN_USE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        IN_USE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Run `work`, and report its wall time and the heap's peak above the level it started at.
async fn measured<T>(label: &str, work: impl std::future::Future<Output = T>) -> T {
    let base = IN_USE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let start = Instant::now();
    let value = work.await;
    let elapsed = start.elapsed();
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(base);
    println!(
        "OUTPUT_REFS_COST {label}: ms={:.3} peak_heap_bytes={peak}",
        elapsed.as_secs_f64() * 1000.0
    );
    value
}

struct NoopExec;

#[async_trait]
impl RunExecutor for NoopExec {
    async fn execute(
        &self,
        _job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        panic!("output references must not start a run")
    }
}

struct Rig {
    dir: std::path::PathBuf,
    registry: LineageRegistry,
    wal_store: Arc<FileWalStore>,
    store: Arc<broca_store::SqliteStore>,
}

impl Rig {
    fn new(tag: &str) -> Self {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-output-refs-cost-{}-{tag}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        let opener = Arc::new(
            DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                wal_store.clone(),
                Arc::new(NoopExec),
            )
            .with_idle_ttl(Duration::from_secs(3600))
            .with_run_index(store.clone())
            .with_materialized_store(store.clone()),
        );
        let wal_reader: Arc<dyn WalStore> = wal_store.clone();
        let store_reader: Arc<dyn broca_core::MaterializedStore> = store.clone();
        let registry = LineageRegistry::new_with_reader(opener, wal_reader, Some(store_reader));
        Self {
            dir,
            registry,
            wal_store,
            store,
        }
    }

    fn key(&self) -> BindIdentity {
        BindIdentity::new(self.dir.join("proj"), "runner", "long")
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A run frozen under the computed tag rules that shows the model its numbers.
fn tagged_config() -> RunConfig {
    RunConfig {
        turn_directives_v1: None,
        render_tags: true,
        computed_tags: Some(broca_wal::ComputedTagRules { checkpoint: 0 }),
        model: ModelHandle::new("stub", "stub-model"),
        tools: vec![Tool {
            name: "lookup".into(),
            description: "test tool".into(),
            input_schema: json!({"type": "object"}),
            module: Some("tools".into()),
            execution_mode: ExecutionMode::Parallel,
            reply: None,
        }]
        .into(),
        tool_choice: ToolChoice::Auto,
        generation: Default::default(),
        response_format: Default::default(),
        provider_options: serde_json::Value::Null,
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: 1,
        frozen_render_config: serde_json::Value::Null,
        auth_selection: None,
        tools_were_omitted: false,
        budget: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: Vec::new(),
        context_management: None,
    }
}

/// A frozen plan that names a compaction provider, which is all output references read of it.
fn compacting_manifest() -> broca_wal::PlanManifest {
    serde_json::from_value(json!({
        "plan": json!({"compaction_item": {"provider": "compactor", "params": {}}}).to_string(),
        "plan_identity": "0".repeat(64),
        "composition_digest": "0".repeat(64),
        "fetches": [],
        "tools": [],
        "system_text": "",
        "system_digest": "0".repeat(64),
        "join_version": 1,
        "role_versions": {"compaction-provider": "v1"},
    }))
    .unwrap()
}

fn step(step: u64, content: Vec<ContentBlock>, finish: FinishReason) -> WalRecord {
    WalRecord::ModelStepFinished {
        written_at_ms: None,
        tags: None,
        hooks: None,
        step_id: StepId(step),
        assistant_message: (AssistantMessage {
            message_id: MessageId(format!("m-{step}")),
            content,
        })
        .into(),
        usage: Default::default(),
        finish_reason: Some(finish),
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
    }
}

/// A view that replaces every message through `covers_through` with nothing.
fn folding_view(version: u64, covers_through: u64) -> WalRecord {
    WalRecord::CompactionViewApplied {
        provider: "compactor".into(),
        view_version: version,
        view: OpaqueJson::from_json_text(
            json!({
                "session_ref": "long", "view_version": version, "lineage_id": "lineage",
                "covers_through": {"ordinal": covers_through, "mid": format!("m{covers_through}")},
                "ranges": [{"kind": "replace", "from": 0, "to": covers_through + 1, "bytes": "[]"}],
                "digest": "0".repeat(64),
            })
            .to_string(),
        )
        .unwrap(),
        choice: broca_wal::CompactionViewChoice::Applied,
    }
}

/// Seed `episodes` tool episodes of four messages each: the user's prompt (numbered 3e+1), a
/// `lookup` call, its result `output e` (3e+2), and a reply (3e+3) naming its own episode's
/// result and the first episode's (§2§). With `compacts`, the plan names a compaction provider
/// and a view folding everything before the episode is applied before every hundredth
/// episode's reply. Everything is materialized into the store.
fn seed(rig: &Rig, episodes: u64, compacts: bool) {
    let key = rig.key();
    let mut wal = rig.wal_store.open(&key).unwrap();
    let mut views = 0;
    for episode in 0..episodes {
        let call = ToolCallId(format!("call-{episode}"));
        let batch = BatchId(format!("batch-{episode}"));
        let first = 4 * episode;
        let mut records = vec![
            WalRecord::RunStarted {
                written_at_ms: None,
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: compacts.then(|| Box::new(compacting_manifest())),
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                ts_ms: None,
                run_id: RunId(format!("run-{episode}")),
                session: key.clone(),
                config: Box::new(tagged_config()),
                input: vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: format!("prompt {episode}"),
                    }],
                )]
                .into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            step(
                0,
                vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: call.clone(),
                    tool_name: "lookup".into(),
                    input: json!({}),
                })],
                FinishReason::ToolCalls,
            ),
            WalRecord::ToolBatchStarted {
                batch_id: batch.clone(),
                step_id: StepId(0),
                planned_calls: vec![call.clone()],
            },
            WalRecord::ToolDispatchIntent {
                dispatched_at_ms: None,
                hooks: None,
                batch_id: batch.clone(),
                tool_call_id: call.clone(),
                tool_name: Some("lookup".into()),
                args: json!({}).into(),
                module_id: Some("tools".into()),
                schema_pin: None,
                approval_digest: None,
                call_key: None,
            },
            WalRecord::ToolResult {
                finished_at_ms: None,
                batch_id: batch.clone(),
                result: Arc::new(ToolResult {
                    tool_call_id: call,
                    output: ToolOutput::Text {
                        text: format!("output {episode}"),
                    },
                    is_error: false,
                }),
                outcome: DispatchOutcome::Completed,
                hooks: None,
                reason: None,
                withdraw_elicitation: None,
                provider_held: None,
                tags: None,
            },
            WalRecord::ToolBatchFinished { batch_id: batch },
        ];
        if compacts && episode > 0 && episode % 100 == 0 {
            views += 1;
            records.push(folding_view(views, first - 1));
        }
        records.extend([
            step(
                1,
                vec![ContentBlock::Text {
                    text: format!("see $$tc({}) and $$tc(2)", 3 * episode + 2),
                }],
                FinishReason::Stop,
            ),
            WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: Default::default(),
                error: None,
                retries_used: 0,
            },
        ]);
        for record in &records {
            wal.append(1, record, false).unwrap();
        }
    }
    wal.sync().unwrap();
    drop(wal);
    let range = rig
        .wal_store
        .read_records_range(&key, 0, None, None)
        .unwrap();
    rig.store.project_range(&key, &range).unwrap();
}

/// The final reply's records and its first record's content, measured.
async fn final_reply(rig: &Rig, episodes: u64, label: &str) -> Vec<serde_json::Value> {
    let key = rig.key();
    let last = 4 * episodes - 1;
    let lineage = rig
        .registry
        .read(broca_module::serve::SessionReadRequest::new(
            key.clone(),
            Some(last),
            Some(1),
        ))
        .await
        .unwrap()
        .lineage_id
        .unwrap();
    let set = format!("{lineage}/m{last}");
    // The frozen plan comes from the session's ownership fold, which a route bind computes and
    // caches before any request on it. Measured apart, so the operations below show their own
    // cost on a bound route.
    measured(
        &format!("{label} frozen plan, first read of the session"),
        async { rig.registry.frozen_manifest(&key).await.unwrap() },
    )
    .await;
    measured(&format!("{label} session.read of the reply"), async {
        rig.registry
            .read(broca_module::serve::SessionReadRequest::new(
                key.clone(),
                Some(last),
                Some(1),
            ))
            .await
            .unwrap()
    })
    .await;
    // A read of a tool-call message attributes its calls from the session's dispatch cache,
    // which the first such read builds from the whole log. Output references read the call of
    // each tool output they resolve through the same path, so this is measured apart too.
    measured(
        &format!("{label} session.read of the tool-call message, first one"),
        async {
            rig.registry
                .read(broca_module::serve::SessionReadRequest::new(
                    key.clone(),
                    Some(last - 2),
                    Some(1),
                ))
                .await
                .unwrap()
        },
    )
    .await;
    let page = measured(&format!("{label} session.output_refs"), async {
        rig.registry
            .output_refs(&key, &ListRequest::new(&set))
            .await
            .unwrap()
            .unwrap()
    })
    .await;
    let read = measured(&format!("{label} session.output_ref_read"), async {
        rig.registry
            .output_ref_read(&key, &ReadRequest::new(format!("{set}/r0"), 0, 131_072))
            .await
            .unwrap()
            .unwrap()
    })
    .await;
    let output = format!("output {}", episodes - 1);
    assert_eq!(read.size, output.len() as u64);
    use base64::Engine;
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(read.data)
            .unwrap(),
        output.as_bytes()
    );
    page.records
        .iter()
        .map(|record| serde_json::to_value(record).unwrap())
        .collect()
}

/// Without a compaction view the writing request held every earlier numbered part, so both
/// references resolve; with one, the first episode's result was folded out of the request and
/// stays unknown, while the reply's own episode's result resolves.
#[tokio::test]
async fn a_folded_part_was_not_in_the_writing_request() {
    for compacts in [false, true] {
        let rig = Rig::new(if compacts { "folded" } else { "plain" });
        let episodes = 250;
        seed(&rig, episodes, compacts);
        let records = final_reply(&rig, episodes, "small").await;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["tag"], (3 * (episodes - 1) + 2).to_string());
        assert_eq!(records[0]["state"], "resolved");
        assert_eq!(records[0]["provider"], "tools");
        assert_eq!(records[1]["tag"], "2");
        if compacts {
            assert_eq!(records[1]["state"], "unresolved", "{records:?}");
            assert_eq!(records[1]["reason"], "unknown_tag");
        } else {
            assert_eq!(records[1]["state"], "resolved", "{records:?}");
            assert_eq!(records[1]["part"], "m2#0");
        }
    }
}

struct CountedReads<'a> {
    registry: &'a LineageRegistry,
    reads: AtomicUsize,
    tags: AtomicUsize,
    manifests: AtomicUsize,
}

#[async_trait]
impl ServeHandler for CountedReads<'_> {
    async fn read(
        &self,
        request: broca_module::serve::SessionReadRequest,
    ) -> Result<broca_wire::SessionReadResponse, broca_module::ServeError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.registry.read(request).await
    }
    async fn tag_location(
        &self,
        session: &BindIdentity,
        number: u64,
    ) -> Result<Option<broca_core::TagLocation>, broca_module::ServeError> {
        self.tags.fetch_add(1, Ordering::Relaxed);
        self.registry.tag_location(session, number).await
    }
    async fn frozen_manifest(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<Arc<broca_wal::PlanManifest>>, broca_module::ServeError> {
        self.manifests.fetch_add(1, Ordering::Relaxed);
        self.registry.frozen_manifest(session).await
    }
    async fn send(
        &self,
        _: broca_module::serve::SendRequest,
    ) -> Result<broca_module::serve::SendResponse, broca_module::ServeError> {
        unreachable!()
    }
    async fn retract(
        &self,
        _: &BindIdentity,
        _: broca_protocol::SubmissionId,
    ) -> Result<broca_module::serve::RetractResponse, broca_module::ServeError> {
        unreachable!()
    }
    async fn cancel(
        &self,
        _: &BindIdentity,
        _: RunId,
    ) -> Result<broca_module::serve::CancelResponse, broca_module::ServeError> {
        unreachable!()
    }
    async fn status(
        &self,
        _: &BindIdentity,
    ) -> Result<broca_module::serve::StatusResponse, broca_module::ServeError> {
        unreachable!()
    }
    async fn run_status(
        &self,
        _: &BindIdentity,
        _: RunId,
    ) -> Result<broca_wire::RunStatusResponse, broca_module::ServeError> {
        unreachable!()
    }
    async fn subscribe(
        &self,
        _: &BindIdentity,
        _: broca_module::serve::SubscribeFrom,
        _: mpsc::Sender<broca_module::Delivery>,
    ) -> Result<(), broca_module::ServeError> {
        unreachable!()
    }
}

#[tokio::test]
async fn a_display_page_reads_each_shared_target_and_manifest_once() {
    let rig = Rig::new("page-batch");
    seed(&rig, 3, false);
    let page = rig
        .registry
        .read(broca_module::serve::SessionReadRequest::new(
            rig.key(),
            Some(0),
            Some(12),
        ))
        .await
        .unwrap();
    let entries: Vec<_> = page
        .messages
        .into_iter()
        .filter(|entry| entry.ordinal % 4 == 3)
        .collect();
    let counted = CountedReads {
        registry: &rig.registry,
        reads: AtomicUsize::new(0),
        tags: AtomicUsize::new(0),
        manifests: AtomicUsize::new(0),
    };
    let sets = broca_module::output_refs::page_output_refs(
        &counted,
        &rig.key(),
        page.lineage_id.as_deref().unwrap(),
        &entries,
        None,
    )
    .await
    .unwrap();
    assert_eq!(sets.len(), 3);
    assert!(sets.values().all(|refs| refs.reply.count == 2));
    assert_eq!(
        counted.reads.load(Ordering::Relaxed),
        6,
        "three distinct outputs and their containing calls, not per-row rereads"
    );
    assert_eq!(
        counted.tags.load(Ordering::Relaxed),
        3,
        "one lookup per distinct number"
    );
    assert_eq!(counted.manifests.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn history_never_invents_creation_times_for_legacy_records() {
    let rig = Rig::new("history-untimed");
    seed(&rig, 1, false);
    let reply = broca_module::history::read(
        &rig.registry,
        &rig.key(),
        &cortexkit_role_llm_runner::history::HistoryRequest::newest(),
    )
    .await;
    assert!(
        matches!(
            reply,
            Err(broca_module::history::HistoryError::Unrepresentable {
                ordinal: 0,
                field: "written_at"
            })
        ),
        "{reply:?}"
    );
}

#[tokio::test]
async fn pre_message_times_history_uses_legacy_clocks_and_reports_remaining_gaps() {
    let rig = Rig::new("history-before-message-times");
    let mut start: WalRecord = serde_json::from_str(
        include_str!("../../broca-wal/tests/fixtures/before_message_times.jsonl")
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    if let WalRecord::RunStarted { session, ts_ms, .. } = &mut start {
        *session = rig.key();
        *ts_ms = Some(10);
    }
    // These pre-message-times records retain the clocks older writers actually recorded.
    // The last assistant has no completed-attempt clock; no other event can supply its time.
    let values = [
        json!({"type":"step_started","step_id":1}),
        json!({"type":"model_attempt_finished","step_id":1,"attempt":0,"outcome":"complete","ts_ms":12,
            "request_body_len":0,"request_body_sha256":"a","response_body_len":0,"response_body_sha256":"b"}),
        json!({"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"call","content":[
            {"type":"tool_call","tool_call_id":"call","tool_name":"work","input":{}}
        ]},"usage":{},"retries_used":0}),
        json!({"type":"tool_batch_started","batch_id":"b","step_id":1,"planned_calls":["call"]}),
        json!({"type":"steered","ts_ms":13,"input":[{"role":"user","content":[{"type":"text","text":"steer"}]}]}),
        json!({"type":"tool_result","finished_at_ms":14,"batch_id":"b","result":{
            "tool_call_id":"call","output":{"kind":"text","text":"done"},"is_error":false},"outcome":"completed"}),
        json!({"type":"tool_batch_finished","batch_id":"b"}),
        json!({"type":"turn_finished","step_id":1}),
        json!({"type":"step_started","step_id":2}),
        json!({"type":"model_attempt_finished","step_id":2,"attempt":0,"outcome":"complete","ts_ms":15,
            "request_body_len":0,"request_body_sha256":"c","response_body_len":0,"response_body_sha256":"d"}),
        json!({"type":"model_step_finished","step_id":2,"assistant_message":{"message_id":"answer","content":[{"type":"text","text":"answer"}]},"usage":{},"retries_used":0}),
        json!({"type":"run_finished","reason":"completed","usage":{},"retries_used":0}),
    ];
    let mut records = vec![start.clone()];
    records.extend(
        values
            .into_iter()
            .map(|value| serde_json::from_value::<WalRecord>(value).unwrap()),
    );
    if let WalRecord::RunStarted { run_id, ts_ms, .. } = &mut start {
        *run_id = RunId("legacy-later".into());
        *ts_ms = Some(20);
    }
    records.push(start);
    for value in [
        json!({"type":"step_started","step_id":3}),
        json!({"type":"model_step_finished","step_id":3,"assistant_message":{"message_id":"untimed","content":[{"type":"text","text":"untimed answer"}]},"usage":{},"retries_used":0}),
        json!({"type":"run_finished","reason":"completed","usage":{},"retries_used":0}),
    ] {
        records.push(serde_json::from_value(value).unwrap());
    }
    assert!(records
        .iter()
        .all(|record| !record.required_features().contains(&"message-times/v1")));
    let key = rig.key();
    let mut wal = rig.wal_store.open(&key).unwrap();
    for record in &records {
        wal.append(1, record, false).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);
    let range = rig
        .wal_store
        .read_records_range(&key, 0, None, None)
        .unwrap();
    rig.store.project_range(&key, &range).unwrap();
    let raw = rig
        .registry
        .read(broca_module::serve::SessionReadRequest::new(
            key.clone(),
            Some(0),
            Some(100),
        ))
        .await
        .unwrap();
    assert_eq!(
        raw.messages
            .iter()
            .map(|entry| entry.written_at)
            .collect::<Vec<_>>(),
        [
            Some(10),
            Some(12),
            Some(14),
            Some(13),
            Some(15),
            Some(20),
            None
        ]
    );
    let missing_models = raw
        .messages
        .iter()
        .filter(|entry| entry.message.role == Role::Assistant && entry.message.origin.is_none())
        .count();
    assert_eq!(missing_models, 0);
    println!("HISTORY_REPRESENTABILITY pre_0_3_255: rows=7 missing_written_at=1 missing_model=0 display_refusals=1");
    let request = cortexkit_role_llm_runner::history::HistoryRequest::newest()
        .with_form(cortexkit_role_llm_runner::history::HistoryForm::Display);
    let reply = broca_module::history::read(&rig.registry, &key, &request).await;
    assert!(
        matches!(
            reply,
            Err(broca_module::history::HistoryError::Unrepresentable {
                ordinal: 6,
                field: "written_at"
            })
        ),
        "{reply:?}"
    );
    let representable = cortexkit_role_llm_runner::history::HistoryRequest::from_ordinal(0)
        .with_limit(6)
        .with_form(cortexkit_role_llm_runner::history::HistoryForm::Display);
    let page = broca_module::history::read(&rig.registry, &key, &representable)
        .await
        .unwrap();
    assert_eq!(page["messages"].as_array().unwrap().len(), 6);
}

/// The cost of both operations on a session of `OUTPUT_REFS_COST_MESSAGES` messages (20,000
/// by default), with and without a compaction view.
#[tokio::test]
#[ignore = "a measurement: run it alone, optimized, with --ignored --nocapture"]
async fn measure_output_refs_on_a_long_session() {
    let messages: u64 = std::env::var("OUTPUT_REFS_COST_MESSAGES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20_000);
    let episodes = messages / 4;
    for compacts in [false, true] {
        let label = if compacts { "compaction" } else { "plain" };
        let rig = Rig::new(label);
        let start = Instant::now();
        seed(&rig, episodes, compacts);
        let wal_bytes = std::fs::metadata(rig.wal_store.wal_path(&rig.key()))
            .map(|meta| meta.len())
            .unwrap_or(0);
        println!(
            "OUTPUT_REFS_COST {label} fixture: messages={} wal_bytes={wal_bytes} seed_ms={}",
            4 * episodes,
            start.elapsed().as_millis()
        );
        final_reply(&rig, episodes, label).await;
    }
}
