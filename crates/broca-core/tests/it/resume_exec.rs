//! The RESUME-EXECUTION exit gate.
//!
//! The replay tests (replay.rs) prove the WAL RECONSTRUCTS correctly; this gate goes
//! further and RE-RUNS `run_loop` from a truncated WAL to completion, asserting:
//!  1. EXACTLY-ONCE: every tool call ran exactly once across the whole crash/resume
//!     sequence (0 if reconstructed from the WAL, 1 if actually dispatched) — never 2.
//!  2. CACHE STABILITY: the resumed run's provider-SENT bytes == the no-crash
//!     reference run's sent bytes at the corresponding step. Reconstructing an equal
//!     prompt is necessary but NOT sufficient — the resumed RUN must actually
//!     re-render identical bytes, or the provider cache is busted.
//!  3. CLEAN FINISH matching the reference reason.
//!
//! The cases here each cut at one chosen point. The every-cut-point scenarios (every record
//! boundary, torn appends, a resume killed inside itself, and real process kills) run on the
//! shared crash harness in `crates/broca-crash-harness`, which checks every resume invariant
//! at each cut.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::{
    provider::Provider, replay, run_loop, run_loop_with_control, AllowAll, FileLeaseStore,
    FileWalStore, OutputEvent, ReplayResolution, RunControl, RunRequest, StubReply, StubToolPlane,
};
use broca_protocol::{
    caller::{ControlUnit, RunFinishReason},
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput},
    provider::{NormalizedEvent, ProviderControl},
    request::{
        BudgetConfig, CallOptions, ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice,
    },
    FinishReason, GenerationConfig, MessageId, RunId, StepId, ToolCallId, Usage,
};
use broca_wal::{FileWal, Wal};
use serde_json::json;
use tokio::sync::mpsc;

/// A provider that:
///  - serves a scripted step keyed by `completed_steps` (so a resumed run that
///    already has N durable steps gets step N+1, not a FIFO-popped wrong one);
///  - RECORDS the exact request bytes it is asked to render, keyed by step index.
#[derive(Clone)]
struct RecordingProvider {
    scripts: Arc<Vec<Vec<NormalizedEvent>>>,
    /// step index (0-based) → the canonical bytes render_request produced.
    sent: Arc<Mutex<HashMap<usize, Vec<u8>>>>,
    renderer_version: u32,
}

impl RecordingProvider {
    fn new(scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            scripts: Arc::new(scripts),
            sent: Arc::new(Mutex::new(HashMap::new())),
            renderer_version: 1,
        }
    }

    fn with_renderer_version(mut self, renderer_version: u32) -> Self {
        self.renderer_version = renderer_version;
        self
    }
}

#[async_trait]
impl Provider for RecordingProvider {
    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        // Record the bytes this step was sent (the C7 hash domain).
        let bytes = self.render_request(opts);
        let idx = (step_id.0 - 1) as usize;
        self.sent.lock().unwrap().insert(idx, bytes);

        let (tx, rx) = mpsc::channel(64);
        let script = self.scripts.get(idx).cloned().unwrap_or_default();
        tokio::spawn(async move {
            for ev in script {
                let _ = tx.send(ev).await;
            }
        });
        rx
    }

    fn renderer_version(&self) -> u32 {
        self.renderer_version
    }
}

/// Script: step1 emits TWO tool calls, step2 is the text reply.
fn scripts() -> Vec<Vec<NormalizedEvent>> {
    vec![
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("m1".into()),
                    content: vec![
                        ContentBlock::ToolCall(ToolCall {
                            thought_signature: None,
                            tool_call_id: ToolCallId("t1".into()),
                            tool_name: "edit".into(),
                            input: json!({"f": "a"}),
                        }),
                        ContentBlock::ToolCall(ToolCall {
                            thought_signature: None,
                            tool_call_id: ToolCallId("t2".into()),
                            tool_name: "edit".into(),
                            input: json!({"f": "b"}),
                        }),
                    ],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage {
                    input_tokens: Some(100),
                    output_tokens: Some(10),
                    ..Default::default()
                },
                finish_reason: FinishReason::ToolCalls,
                retries_used: 0,
            }),
        ],
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("m2".into()),
                    content: vec![ContentBlock::Text {
                        text: "done".into(),
                    }],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage {
                    input_tokens: Some(50),
                    output_tokens: Some(4),
                    ..Default::default()
                },
                finish_reason: FinishReason::Stop,
                retries_used: 0,
            }),
        ],
    ]
}

fn request(run: &str, dir: &std::path::Path) -> RunRequest {
    RunRequest {
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId(run.into()),
        session: broca_protocol::BindIdentity::new(dir, "m9", "s1"),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "edit a and b".into(),
            }],
        }],
        tools: vec![Tool {
            name: "edit".into(),
            description: "edit a file".into(),
            input_schema: json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
        }],
        policy_allowed: ["edit".to_string()].into_iter().collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 5 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
    }
}

fn plane() -> StubToolPlane {
    StubToolPlane::new().with_tool("edit", |c| {
        StubReply::Output(ToolOutput::Text {
            text: format!("edited-{}", c.tool_call_id.0),
        })
    })
}

async fn run_once(
    dir: &std::path::Path,
    provider: &RecordingProvider,
    plane: &StubToolPlane,
) -> RunFinishReason {
    run_once_req(dir, request("r", dir), provider, plane).await
}

async fn run_once_inherited(
    dir: &std::path::Path,
    provider: &RecordingProvider,
    plane: &StubToolPlane,
) -> RunFinishReason {
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop_with_control(
        request("r", dir),
        provider,
        plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        RunControl {
            tools_were_omitted: true,
            ..Default::default()
        },
        &tx,
    )
    .await
    .expect("inherited run ok");
    drop(tx);
    collector.await.unwrap();
    summary.reason
}

async fn run_once_req(
    dir: &std::path::Path,
    req: RunRequest,
    provider: &RecordingProvider,
    plane: &StubToolPlane,
) -> RunFinishReason {
    run_once_req_with_events(dir, req, provider, plane).await.0
}

async fn run_once_req_with_events(
    dir: &std::path::Path,
    req: RunRequest,
    provider: &RecordingProvider,
    plane: &StubToolPlane,
) -> (RunFinishReason, Vec<OutputEvent>) {
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    });
    let summary = run_loop(req, provider, plane, &lease, &AllowAll, &wal, &tx)
        .await
        .expect("run ok");
    drop(tx);
    let events = collector.await.unwrap();
    (summary.reason, events)
}

/// Truncate the on-disk WAL to its first `keep` records (simulating a crash that
/// committed only those). We re-encode the kept records into a fresh file.
fn truncate_wal(dir: &std::path::Path, keep: usize) {
    let wal_store = FileWalStore::new(dir.join("wal"));
    let path = wal_store.wal_path(&request("r", dir).session);
    let framed = FileWal::open(&path).unwrap().replay().unwrap();
    let _ = std::fs::remove_file(&path);
    let mut wal = FileWal::open(&path).unwrap();
    for f in framed.into_iter().take(keep) {
        wal.append(f.fence, &f.record, true).unwrap();
    }
}

#[tokio::test]
async fn interrupted_recovery_closes_the_batch_seals_once_and_next_episode_pairs_the_call() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-interrupted-seal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Produce a real tool turn, then retain only the durable prefix through its first dispatch
    // intent. The request may have run, but no result or episode terminal survived the crash.
    let seed_provider = RecordingProvider::new(scripts());
    let seed_plane = plane();
    assert_eq!(
        run_once_inherited(&dir, &seed_provider, &seed_plane).await,
        RunFinishReason::Completed
    );
    let intent_cut = read_wal(&dir)
        .iter()
        .position(|record| matches!(record, broca_wal::WalRecord::ToolDispatchIntent { .. }))
        .expect("the tool run writes a dispatch intent")
        + 1;
    truncate_wal(&dir, intent_cut);
    assert!(matches!(
        replay(&read_wal(&dir))
            .expect("crash cut replays")
            .resolution,
        ReplayResolution::Interrupted { .. }
    ));

    // Recovery must not call the provider or re-run either tool. It closes the open batch
    // (an outcome_unknown result for the call whose intent is durable, an interrupted one for
    // the call that never left the process), then seals the episode Interrupted.
    let recovery_provider = RecordingProvider::new(scripts());
    let recovery_plane = plane();
    assert_eq!(
        run_once_inherited(&dir, &recovery_provider, &recovery_plane).await,
        RunFinishReason::Interrupted
    );
    assert!(recovery_provider.sent.lock().unwrap().is_empty());
    for id in ["t1", "t2"] {
        assert_eq!(
            recovery_plane.exec_count(&ToolCallId(id.into())),
            0,
            "sealing must not re-dispatch {id}"
        );
    }

    let sealed = read_wal(&dir);
    assert_eq!(
        sealed
            .iter()
            .filter(|record| matches!(record, broca_wal::WalRecord::RunFinished { .. }))
            .count(),
        1
    );
    let sealed_state = replay(&sealed).expect("sealed WAL replays");
    assert!(matches!(
        sealed_state.resolution,
        ReplayResolution::LineageIdle {
            last_reason: RunFinishReason::Interrupted
        }
    ));
    let closed: Vec<(String, broca_wal::DispatchOutcome)> = sealed[intent_cut..]
        .iter()
        .filter_map(|record| match record {
            broca_wal::WalRecord::ToolResult {
                result, outcome, ..
            } => Some((result.tool_call_id.0.clone(), *outcome)),
            _ => None,
        })
        .collect();
    assert_eq!(
        closed,
        vec![
            ("t1".to_string(), broca_wal::DispatchOutcome::Unknown),
            ("t2".to_string(), broca_wal::DispatchOutcome::Interrupted),
        ],
        "the close-out pairs the dispatched call with outcome_unknown and the unsent one \
         with an interrupted result"
    );
    // The conversation now holds the tool turn, each call paired with its result.
    assert_eq!(sealed_state.prompt.len(), 3);
    let results: Vec<&broca_protocol::content::ToolResult> = sealed_state.prompt[2]
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolResult(result) => Some(result),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0],
        &broca_core::outcome_unknown_result(&ToolCallId("t1".into()))
    );

    // A same-id re-drive observes the durable terminal and appends nothing.
    let record_count_after_seal = sealed.len();
    let idempotent_provider = RecordingProvider::new(scripts());
    let idempotent_plane = plane();
    assert_eq!(
        run_once_inherited(&dir, &idempotent_provider, &idempotent_plane).await,
        RunFinishReason::Interrupted
    );
    assert!(idempotent_provider.sent.lock().unwrap().is_empty());
    assert_eq!(read_wal(&dir).len(), record_count_after_seal);
    for id in ["t1", "t2"] {
        assert_eq!(
            idempotent_plane.exec_count(&ToolCallId(id.into())),
            0,
            "idempotent re-drive must not dispatch {id}"
        );
    }

    // A fresh id now opens a new episode. Its provider prompt carries the tool turn with each
    // call paired with the result the close-out wrote, then the new input.
    let mut next_request = request("r2", &dir);
    next_request.input[0] = Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "continue after review".into(),
        }],
    );
    let next_provider = RecordingProvider::new(vec![vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m-next".into()),
                content: vec![ContentBlock::Text {
                    text: "continued".into(),
                }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]]);
    assert_eq!(
        run_once_req(&dir, next_request, &next_provider, &recovery_plane).await,
        RunFinishReason::Completed
    );
    let sent = next_provider.sent.lock().unwrap();
    let opts: CallOptions =
        serde_json::from_slice(sent.get(&0).expect("new episode calls provider"))
            .expect("default provider bytes decode as CallOptions");
    assert_eq!(
        opts.prompt.iter().map(|m| m.role).collect::<Vec<_>>(),
        vec![Role::User, Role::Assistant, Role::Tool, Role::User]
    );
    let prompt_text: Vec<&str> = opts
        .prompt
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(prompt_text, vec!["edit a and b", "continue after review"]);
    drop(sent);

    let final_records = read_wal(&dir);
    assert_eq!(
        final_records
            .iter()
            .filter(|record| matches!(record, broca_wal::WalRecord::RunStarted { .. }))
            .count(),
        2,
        "the fresh send opens exactly one later episode"
    );
    let final_state = replay(&final_records).expect("two-episode WAL replays");
    let calls = final_state
        .prompt
        .iter()
        .flat_map(|message| message.content.iter())
        .filter(|block| matches!(block, ContentBlock::ToolCall(_)))
        .count();
    let results = final_state
        .prompt
        .iter()
        .flat_map(|message| message.content.iter())
        .filter(|block| matches!(block, ContentBlock::ToolResult(_)))
        .count();
    assert_eq!(
        (calls, results),
        (2, 2),
        "every call stays paired with one result"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn renderer_drift_resume_seals_and_next_invocation_opens_a_new_episode() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-render-drift-seal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Retain the real, synced RunStarted but cut every later record. This is the durable state of
    // a process that exited before its first provider call could commit a terminal.
    let seed_provider = RecordingProvider::new(scripts());
    let plane = plane();
    assert_eq!(
        run_once(&dir, &seed_provider, &plane).await,
        RunFinishReason::Completed
    );
    truncate_wal(&dir, 1);
    let unsealed = read_wal(&dir);
    assert!(matches!(
        unsealed.as_slice(),
        [broca_wal::WalRecord::RunStarted { .. }]
    ));
    assert!(matches!(
        replay(&unsealed).expect("unsealed run replays").resolution,
        ReplayResolution::Resume { .. }
    ));

    // The current renderer differs from the frozen identity. Recovery must make no provider call,
    // preserve the visible Error -> Interrupted sequence, and durably seal the same terminal.
    let drifted_provider = RecordingProvider::new(scripts()).with_renderer_version(2);
    let (reason, events) =
        run_once_req_with_events(&dir, request("r", &dir), &drifted_provider, &plane).await;
    assert_eq!(reason, RunFinishReason::Interrupted);
    assert!(drifted_provider.sent.lock().unwrap().is_empty());
    let controls: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            OutputEvent::Control(control) => Some(control),
            OutputEvent::Display(_) => None,
        })
        .collect();
    assert!(matches!(
        controls.as_slice(),
        [
            ControlUnit::Error { error },
            ControlUnit::RunFinished {
                reason: RunFinishReason::Interrupted,
                ..
            }
        ] if error.provider_code.as_deref() == Some("renderer_version_drift")
    ));

    let sealed = read_wal(&dir);
    assert_eq!(
        sealed
            .iter()
            .filter(|record| matches!(record, broca_wal::WalRecord::RunFinished { .. }))
            .count(),
        1
    );
    assert!(sealed.iter().any(|record| matches!(
        record,
        broca_wal::WalRecord::RunFinished {
            reason: RunFinishReason::Interrupted,
            usage,
            error: Some(error),
            retries_used: 0,
            ..
        } if *usage == Usage::default()
            && error.provider_code.as_deref() == Some("renderer_version_drift")
    )));
    assert!(matches!(
        replay(&sealed)
            .expect("sealed drift terminal replays")
            .resolution,
        ReplayResolution::LineageIdle {
            last_reason: RunFinishReason::Interrupted
        }
    ));

    // A later invocation must append a fresh episode. Without the durable seal this call would
    // resume the original episode, hit the same drift guard, and make no provider call forever.
    let mut next_request = request("r-next", &dir);
    next_request.input[0] = Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "continue after renderer update".into(),
        }],
    );
    let next_script = scripts().into_iter().nth(1).unwrap();
    let next_provider = RecordingProvider::new(vec![next_script]).with_renderer_version(2);
    assert_eq!(
        run_once_req(&dir, next_request, &next_provider, &plane).await,
        RunFinishReason::Completed
    );
    assert_eq!(next_provider.sent.lock().unwrap().len(), 1);

    let final_records = read_wal(&dir);
    let started_ids: Vec<_> = final_records
        .iter()
        .filter_map(|record| match record {
            broca_wal::WalRecord::RunStarted { run_id, .. } => Some(run_id.0.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(started_ids, vec!["r", "r-next"]);
    assert!(matches!(
        replay(&final_records)
            .expect("new episode after drift seal replays")
            .resolution,
        ReplayResolution::LineageIdle {
            last_reason: RunFinishReason::Completed
        }
    ));

    let _ = std::fs::remove_dir_all(&dir);
}

/// The full WAL records (current on-disk state).
fn read_wal(dir: &std::path::Path) -> Vec<broca_wal::WalRecord> {
    let ws = FileWalStore::new(dir.join("wal"));
    FileWal::open(ws.wal_path(&request("r", dir).session))
        .unwrap()
        .replay()
        .unwrap()
        .into_iter()
        .map(|f| f.record)
        .collect()
}

/// BUILD-TASK #0 (C6 authority): a resume that supplies a DIFFERENT req.tools +
/// req.generation than the original must STILL render byte-identical bytes — proof
/// that run_config (frozen at run start) is authoritative on resume and req is
/// IGNORED for the byte-affecting fields.
#[tokio::test]
async fn resume_ignores_divergent_req_uses_frozen_run_config() {
    let dir =
        broca_test_support::scratch_root().join(format!("broca-m9-c6-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 1. REFERENCE: a clean run with the ORIGINAL req. Capture step-0's sent bytes.
    let ref_provider = RecordingProvider::new(scripts());
    run_once(&dir, &ref_provider, &plane()).await;
    let ref_bytes = ref_provider
        .sent
        .lock()
        .unwrap()
        .get(&0)
        .cloned()
        .expect("step 0 sent");

    // 2. Re-seed a fresh run, truncate to a mid-run crash (after step 1's turn so
    //    step 2 re-renders on resume), then RESUME with a DIVERGENT req:
    //    extra tool + a different temperature/max_tokens. The C6 fix must ignore
    //    all of it and render from the frozen run_config.
    let dir2 = dir.join("resume");
    std::fs::create_dir_all(&dir2).unwrap();
    let seed = RecordingProvider::new(scripts());
    run_once(&dir2, &seed, &plane()).await;
    // Truncate to just after the first TurnFinished so step 2 must re-render.
    let cut = {
        let ws = FileWalStore::new(dir2.join("wal"));
        let framed = FileWal::open(ws.wal_path(&request("r", &dir2).session))
            .unwrap()
            .replay()
            .unwrap();
        framed
            .iter()
            .position(|f| matches!(f.record, broca_wal::WalRecord::TurnFinished { .. }))
            .unwrap()
            + 1
    };
    truncate_wal(&dir2, cut);

    // The DIVERGENT resume req: an extra "delete" tool + changed generation.
    let mut divergent = request("r", &dir2);
    divergent.tools.push(Tool {
        name: "delete".into(),
        description: "DIFFERENT tool not in the frozen config".into(),
        input_schema: json!({"type": "object"}),
        module: None,
        execution_mode: ExecutionMode::Parallel,
    });
    divergent.policy_allowed.insert("delete".into());
    divergent.generation.temperature = Some(0.99);
    divergent.generation.max_output_tokens = Some(4242);

    let resume_provider = RecordingProvider::new(scripts());
    run_once_req(&dir2, divergent, &resume_provider, &plane()).await;

    // The resumed run RE-RENDERED a step (step 2 at minimum). EVERY re-rendered
    // step's bytes must match the reference — the divergent req.tools/generation
    // had ZERO effect (run_config is authoritative).
    let resumed = resume_provider.sent.lock().unwrap().clone();
    assert!(
        !resumed.is_empty(),
        "the resume re-rendered at least one step"
    );
    // Step 0 is durable (not re-sent); compare any re-sent step against the
    // reference by re-deriving the reference for that step. The strongest single
    // assertion: the resumed step-2 bytes contain NO "delete" tool + match the
    // frozen tools/generation. We assert it equals a same-config reference render.
    let ref2 = RecordingProvider::new(scripts());
    let dir3 = dir.join("ref2");
    std::fs::create_dir_all(&dir3).unwrap();
    run_once(&dir3, &ref2, &plane()).await;
    let ref2_sent = ref2.sent.lock().unwrap().clone();
    for (idx, bytes) in &resumed {
        assert_eq!(
            bytes,
            ref2_sent.get(idx).expect("reference rendered this step"),
            "resumed step {idx} ignored the divergent req → byte-identical to the frozen-config reference (C6)"
        );
        // Belt-and-suspenders: the divergent "delete" tool never appears.
        assert!(
            !String::from_utf8_lossy(bytes).contains("delete"),
            "resumed step {idx} must NOT carry the divergent req's `delete` tool"
        );
    }
    // Sanity: ref_bytes (original step 0) also has no "delete".
    assert!(!String::from_utf8_lossy(&ref_bytes).contains("delete"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn finalize_answer_is_committed_to_durable_history() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-finalize-history-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut request = request("r", &dir);
    request.budget = Some(BudgetConfig {
        nudges: Vec::new(),
        finalize_at_tool_calls: 2,
        finalize_text: "Give the final answer now.".into(),
    });
    let provider = RecordingProvider::new(scripts());
    let reason = run_once_req(&dir, request, &provider, &plane()).await;
    assert_eq!(reason, RunFinishReason::Completed);

    let state = broca_core::replay(&read_wal(&dir)).expect("finalize WAL replays");
    let trailing: Vec<(Role, Vec<ContentBlock>)> = state
        .prompt
        .iter()
        .rev()
        .take(2)
        .map(|message| (message.role, message.content.clone()))
        .collect();
    assert!(matches!(
        trailing.as_slice(),
        [
            (Role::Assistant, answer),
            (Role::User, instruction)
        ] if matches!(answer.as_slice(), [ContentBlock::Text { text }] if text == "done")
            && matches!(instruction.as_slice(), [ContentBlock::Text { text }] if text == "Give the final answer now.")
    ));

    let _ = std::fs::remove_dir_all(dir);
}

async fn check_tool_result_ceiling(tag: &str, output: ToolOutput, original: String) {
    check_named_tool_result_ceiling(tag, "edit", "edit", output, original).await;
}

async fn check_named_tool_result_ceiling(
    tag: &str,
    tool_name: &str,
    marker_name: &str,
    output: ToolOutput,
    original: String,
) {
    const LIMIT: usize = 1_048_576;
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-tool-ceiling-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut steps = scripts();
    if let NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) =
        &mut steps[0][0]
    {
        message.content.truncate(1);
        if let ContentBlock::ToolCall(call) = &mut message.content[0] {
            call.tool_name = tool_name.into();
        }
    }
    let is_error = matches!(&output, ToolOutput::Error { .. });
    let tool_plane = StubToolPlane::new().with_tool(tool_name, move |_| match &output {
        ToolOutput::Error { message } => StubReply::ExecError(message.clone()),
        _ => StubReply::Output(output.clone()),
    });
    let mut req = request("r", &dir);
    req.tools[0].name = tool_name.into();
    req.policy_allowed = [tool_name.to_string()].into_iter().collect();
    let provider = RecordingProvider::new(steps.clone());
    assert_eq!(
        run_once_req(&dir, req.clone(), &provider, &tool_plane).await,
        RunFinishReason::Completed
    );
    let records = read_wal(&dir);
    let (index, stored) = records
        .iter()
        .enumerate()
        .find_map(|(index, record)| match record {
            broca_wal::WalRecord::ToolResult { result, .. } => Some((index, result)),
            _ => None,
        })
        .expect("durable dispatched result");
    let ToolOutput::Text { text } = &stored.output else {
        panic!("expected text result")
    };
    assert!(
        stored.is_error == is_error,
        "truncation does not change the tool outcome"
    );
    assert!(
        text.len() <= LIMIT,
        "stored result exceeds byte ceiling: {}",
        text.len()
    );
    assert!(std::str::from_utf8(text.as_bytes()).is_ok());
    if original.len() <= LIMIT {
        assert_eq!(text, &original);
    } else {
        let (head, marker) = text.rsplit_once('\n').expect("truncation marker line");
        assert!(original.starts_with(head));
        assert!(!head.is_empty());
        assert_eq!(marker, format!("[broca: tool result truncated: {marker_name} returned {} bytes, over the {LIMIT} byte limit; the first {} bytes are shown. Narrow the request (a smaller range, a filter, or a more specific query).]", original.len(), head.len()));
        // The retained head must be the longest complete UTF-8 prefix that fits with its marker.
        let next = original[head.len()..].chars().next().unwrap().len_utf8();
        let next_count = head.len() + next;
        let next_marker = format!("\n[broca: tool result truncated: {marker_name} returned {} bytes, over the {LIMIT} byte limit; the first {next_count} bytes are shown. Narrow the request (a smaller range, a filter, or a more specific query).]", original.len());
        assert!(next_count + next_marker.len() > LIMIT);
    }
    let sent = provider
        .sent
        .lock()
        .unwrap()
        .get(&1)
        .cloned()
        .expect("next model step");
    let rendered: serde_json::Value = serde_json::from_slice(&sent).unwrap();
    assert!(
        rendered
            .to_string()
            .contains(&serde_json::to_string(text).unwrap()),
        "next request carries stored text"
    );
    // Retain the tool-result WAL entry but exclude the later batch and turn completion entries,
    // simulating a crash after the result was stored and before the turn was marked complete.
    truncate_wal(&dir, index + 1);
    let resumed = RecordingProvider::new(steps);
    let resume_plane = plane();
    assert_eq!(
        run_once_req(&dir, req, &resumed, &resume_plane).await,
        RunFinishReason::Completed
    );
    assert_eq!(resume_plane.exec_count(&ToolCallId("t1".into())), 0);
    assert_eq!(
        resumed.sent.lock().unwrap().get(&1).unwrap(),
        &sent,
        "resume renders byte-identical capped output"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn tool_result_ceiling_caps_over_limit_in_wal_and_resume() {
    let text = "a".repeat(1_048_577);
    check_tool_result_ceiling("over", ToolOutput::Text { text: text.clone() }, text).await;
}

#[tokio::test]
async fn tool_result_ceiling_preserves_multibyte_boundary() {
    // A one-byte prefix offsets the two-byte characters so the raw byte cut splits one.
    let text = format!("a{}a", "é".repeat((11 * 1_048_576 - 2) / 2));
    check_tool_result_ceiling("utf8", ToolOutput::Text { text: text.clone() }, text).await;
}

#[tokio::test]
async fn tool_result_ceiling_exact_limit_is_unchanged() {
    let text = "a".repeat(1_048_576);
    check_tool_result_ceiling("exact", ToolOutput::Text { text: text.clone() }, text).await;
}

#[tokio::test]
async fn tool_result_ceiling_serializes_large_json_as_text() {
    let value = json!({"payload": "é".repeat(1_048_576)});
    let original = value.to_string();
    check_tool_result_ceiling("json", ToolOutput::Json { value }, original).await;
}

#[tokio::test]
async fn tool_result_ceiling_caps_error_and_preserves_is_error() {
    let message = "failure".repeat(160_000);
    check_tool_result_ceiling(
        "error",
        ToolOutput::Error {
            message: message.clone(),
        },
        message,
    )
    .await;
}

#[tokio::test]
async fn tool_result_ceiling_caps_denied_including_provider_prefix() {
    let reason = "denial".repeat(180_000);
    let original = format!("denied: {reason}");
    check_tool_result_ceiling("denied", ToolOutput::Denied { reason }, original).await;
}

#[tokio::test]
async fn tool_result_ceiling_shortens_unbounded_utf8_tool_name() {
    let tool_name = format!("{}{}", "x".repeat(255), "é".repeat(600_000));
    let marker_name = format!("{}…", "x".repeat(255));
    let text = "a".repeat(1_048_577);
    check_named_tool_result_ceiling(
        "name",
        &tool_name,
        &marker_name,
        ToolOutput::Text { text: text.clone() },
        text,
    )
    .await;
}
