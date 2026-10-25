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
