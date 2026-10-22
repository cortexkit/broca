//! A tool declared directly on a send (no fetch plan) that carries its own `reply.max_ms`, over a
//! real daemon, with the tool's call held behind an explicit gate.

use super::preset_real_daemon::{call, Rig};
use super::reply_deadline_real_daemon::{HeldAppend, HeldModel, HeldStore};
use async_trait::async_trait;
use broca_core::{
    lease_key_for, run_loop_on_writer, run_loop_with_control, AllowAll, FileLeaseStore,
    FileWalStore, LeaseStore, RunControl, RunRequest, StartMode, StubProvider, WalHandle, WalStore,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role},
    provider::{NormalizedEvent, ProviderControl},
    request::{ModelHandle, StopCondition, Tool, ToolChoice, ToolReply},
    BindIdentity, FinishReason, GenerationConfig, MessageId, RunId, Usage,
};
use broca_subc::SubcToolPlane;
use broca_wal::WalRecord;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Notify};

// The production default is 120 seconds. Only this plane's debug-only test seam shortens it, and
// the shortened wait is a floor a declared reply is never cut below.
const FALLBACK_MS: u64 = 1_200;
const HELD_MS: u64 = 1_250;
// Longer than the hold and than the fallback, and short enough that a wrongly long wait would
// still let a broken test finish.
const DECLARED_MS: u64 = 60_000;
const MODULE: &str = "prefrontal-core";
const TOOL: &str = "research";

#[derive(Default)]
struct Gate {
    entered: Notify,
    release: Notify,
}

struct HeldResearch(Arc<Gate>);
#[async_trait]
impl subc_client_rs::ModuleHandler for HeldResearch {
    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        bytes: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(request["name"], TOOL, "{request}");
        assert!(
            request.get("reply").is_none() && request["arguments"].get("reply").is_none(),
            "the declared deadline is dispatch metadata, never sent to the tool: {request}"
        );
        self.0.entered.notify_one();
        self.0.release.notified().await;
        let answer = json!({"content":[{"type":"text", "text":"research done"}], "isError":false});
        subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&answer).unwrap())
    }
}

async fn register_held(rig: &Rig, gate: Arc<Gate>) -> tokio::task::JoinHandle<()> {
    use subc_protocol::manifest::{
        Concurrency, ExecutionMode, IdentityScope, ModuleManifest, ProviderRole, Tool,
    };
    let manifest = ModuleManifest::builder(MODULE, "1")
        .provides(vec![ProviderRole::ToolProvider {
            tools: vec![Tool {
                name: TOOL.into(),
                description: None,
                execution_mode: ExecutionMode::Pure,
                schema: json!({"type":"object"}),
            }],
            identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
            concurrency: Concurrency::ModuleManaged,
            emits_push: false,
            sub_supervises: false,
        }])
        .build();
    let (_, serve) =
        subc_client_rs::serve_with_handle(&rig.connection_file, manifest, HeldResearch(gate))
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = serve.await;
    })
}

fn research(reply: Option<u64>) -> Tool {
    let mut tool = json!({
        "name": TOOL,
        "description": "Ask sidekick a question.",
        "input_schema": {"type": "object"},
        "module": MODULE,
    });
    if let Some(max_ms) = reply {
        tool["reply"] = json!({"max_ms": max_ms});
    }
    serde_json::from_value(tool).unwrap()
}

fn request(session: BindIdentity, tools: Vec<Tool>) -> RunRequest {
    RunRequest {
        directives: None,
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId("send-reply-gate".into()),
        session,
        model: ModelHandle::new("test", "test"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "research".into(),
            }],
        }],
        policy_allowed: tools.iter().map(|tool| tool.name.clone()).collect(),
        tools,
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 4 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    }
}

fn scripts() -> StubProvider {
    StubProvider::new(vec![
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("tool-step".into()),
                    content: vec![ContentBlock::ToolCall(call(
                        TOOL,
                        json!({"request": "how is a run resumed"}),
                        "held-research",
                    ))],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: FinishReason::ToolCalls,
                retries_used: 0,
            }),
        ],
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("text-step".into()),
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
        ],
    ])
}

/// Run one session whose `research` tool is held by the gate past the (shortened) default wait.
/// `declared` gives the tool `reply: {max_ms}` on the send. With `resume`, a first run is cut
/// after its `RunStarted` is durable and before any tool intent, and the run is then resumed with
/// a request whose tool declares no deadline: the deadline frozen in the WAL must still apply.
async fn held_run(declared: bool, resume: bool) {
    let mut rig = Rig::start(None).await;
    let gate = Arc::new(Gate::default());
    let held = register_held(&rig, gate.clone()).await;
    let conn = rig.connect().await;
    let session = rig.session("send-reply-gate");
    let plane = SubcToolPlane::new(Arc::try_unwrap(conn).ok().unwrap(), "aft")
        .with_reply_fallback_for_test(FALLBACK_MS);
    let hold = HeldAppend::new(resume);
    let wal = HeldStore {
        inner: FileWalStore::new(rig.dir.join("wal")),
        hold: hold.clone(),
    };
    let lease = FileLeaseStore::new(rig.dir.join("leases"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let sent = vec![research(declared.then_some(DECLARED_MS))];
    let mut later = sent.clone();
    if resume {
        // Like the session actor, the test owns the first run's lease and WAL writer: dropping
        // the run's future does not stop the writer thread, which still holds the queued
        // StepStarted append.
        let model = HeldModel(Notify::new());
        let seed_lease = lease.acquire(&lease_key_for(&session)).unwrap();
        let seed_writer =
            WalHandle::spawn(wal.open(&session).unwrap(), seed_lease.epoch()).unwrap();
        {
            let run = run_loop_on_writer(
                request(session.clone(), sent.clone()),
                &seed_writer,
                StartMode::FreshOrResume,
                &model,
                &plane,
                &AllowAll,
                &wal,
                None,
                RunControl::default(),
                &tx,
            );
            tokio::pin!(run);
            tokio::select! {
                _ = model.0.notified() => {},
                result = &mut run => panic!("seed run unexpectedly ended: {result:?}"),
                _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("seed run never reached the model"),
            }
            tokio::time::timeout(Duration::from_secs(10), hold.parked.notified())
                .await
                .expect("seed writer never reached its StepStarted append");
        }
        hold.release();
        let _ = seed_writer.shutdown().await;
        drop(seed_lease);
        // A later request whose tool declares no deadline disagrees with the WAL; the WAL wins.
        later = vec![research(None)];
    }
    {
        let provider = scripts();
        let started = tokio::time::Instant::now();
        let run = run_loop_with_control(
            request(session.clone(), later),
            &provider,
            &plane,
            &lease,
            &AllowAll,
            &wal,
            None,
            RunControl::default(),
            &tx,
        );
        tokio::pin!(run);
        tokio::select! {
            _ = gate.entered.notified() => {},
            result = &mut run => panic!("run ended before the module held the call: {result:?}"),
            _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("module never received the call"),
        }
        let summary = if declared {
            // The module does no timed work: the test owns the gate and observes a full hold.
            assert!(
                tokio::time::timeout(Duration::from_millis(HELD_MS), &mut run)
                    .await
                    .is_err(),
                "the declared reply was cut at the default wait while the gate was held"
            );
            gate.release.notify_one();
            tokio::time::timeout(Duration::from_secs(10), &mut run)
                .await
                .unwrap()
                .unwrap()
        } else {
            let summary = tokio::time::timeout(Duration::from_secs(10), &mut run)
                .await
                .unwrap()
                .unwrap();
            assert!(started.elapsed() >= Duration::from_millis(FALLBACK_MS));
            gate.release.notify_one();
            summary
        };
        assert_eq!(
            summary.reason,
            if declared {
                RunFinishReason::Completed
            } else {
                RunFinishReason::Interrupted
            }
        );
        let records = wal.read_records(&session).unwrap();
        let outcomes: Vec<_> = records
            .iter()
            .filter_map(|frame| match &frame.record {
                WalRecord::ToolResult {
                    outcome, result, ..
                } => Some((*outcome, result.is_error)),
                _ => None,
            })
            .collect();
        assert_eq!(
            outcomes,
            if declared {
                vec![(broca_wal::DispatchOutcome::Completed, false)]
            } else {
                vec![(broca_wal::DispatchOutcome::Unknown, true)]
            }
        );
        let starts: Vec<_> = records
            .iter()
            .filter_map(|frame| match &frame.record {
                WalRecord::RunStarted { config, .. } => Some(config.tools.as_ref().clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            starts,
            vec![sent],
            "one run start, frozen with the sent tools"
        );
        assert_eq!(
            starts[0][0].reply,
            declared.then_some(ToolReply {
                max_ms: DECLARED_MS
            })
        );
    }
    plane.close_session_routes(&session).await;
    drop(tx);
    collector.await.unwrap();
    held.abort();
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_declared_reply_outlasts_default_over_real_daemon() {
    held_run(true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_declared_tool_without_reply_interrupts_at_default_over_real_daemon() {
    held_run(false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumed_run_keeps_send_declared_reply_frozen_in_wal_over_real_daemon() {
    held_run(true, true).await;
}
