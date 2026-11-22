//! Setup parts are decoded once per process and shared by every session and run that uses them.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason};
use broca_protocol::request::StopCondition;
use broca_protocol::{MessageId, NormalizedEvent, ProviderControl, Usage};
use broca_session_loop::parts::{MemoryParts, Parts, SharedParts};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::record::{PartKind, PartRef};
use broca_session_store::session::Session;
use tokio::sync::mpsc;

use super::*;

/// A parts store in memory that counts every read of a part's value, which parses its bytes.
struct Counting {
    inner: MemoryParts,
    reads: Arc<AtomicUsize>,
}

impl Parts for Counting {
    fn put(&mut self, kind: PartKind, value: &serde_json::Value) -> Result<PartRef, String> {
        self.inner.put(kind, value)
    }

    fn get(&self, part: &PartRef) -> Result<serde_json::Value, String> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(part)
    }
}

/// A step that calls the `read` tool once, as call `call`.
fn read_call(id: &str, call: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::ToolCall(broca_protocol::content::ToolCall {
                    thought_signature: None,
                    provider_state: None,
                    tool_call_id: broca_protocol::ToolCallId(call.into()),
                    tool_name: "read".into(),
                    input: serde_json::json!({}).into(),
                })],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(100),
                output_tokens: Some(20),
                ..Usage::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

/// A planned send that may take up to 20 steps.
fn long(text: &str) -> broca_protocol::SubmissionRequest {
    let mut request = untooled(text);
    request.stop_when = vec![StopCondition::StepCount { n: 20 }];
    request
}

/// How many part reads one worker session's life makes: its planned first run through ten steps
/// (nine tool calls and an answer), then a second run.
#[tokio::test]
async fn a_worker_sessions_part_reads_are_measured() {
    let reads = Arc::new(AtomicUsize::new(0));
    let parts = SharedParts::new(Box::new(Counting {
        inner: MemoryParts::default(),
        reads: reads.clone(),
    }));
    let mut steps: Vec<_> = (0..9)
        .map(|step| read_call(&format!("a{step}"), &format!("c{step}")))
        .collect();
    steps.push(text_step("a9", "done"));
    steps.push(text_step("b0", "again"));
    let executor = Arc::new(executor(steps, None));
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let actor = StoreActor::new(bind(), session, executor.clone(), Box::new(parts.clone()));
    let (commands, receiver) = mpsc::unbounded_channel();
    let commands = Commands::over(commands, backend);
    let served = tokio::spawn(actor.serve(receiver));
    let manifest = plan_manifest("plan-1", serde_json::json!({}));
    let decodes = broca_session_loop::parts::decodes_on_this_thread();
    assert!(matches!(
        send_planned(&commands, "r1", long("first"), "plan-1", Some(manifest)).await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    let first = reads.load(Ordering::SeqCst);
    let first_decodes = broca_session_loop::parts::decodes_on_this_thread() - decodes;
    assert!(matches!(
        send_planned(&commands, "r2", long("second"), "plan-1", None).await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    let second = reads.load(Ordering::SeqCst) - first;
    let second_decodes =
        broca_session_loop::parts::decodes_on_this_thread() - decodes - first_decodes;
    drop(commands);
    served.await.unwrap();
    assert_eq!(executor.provider.inner.calls().len(), 11);
    // Every part read decodes the part's bytes, on this thread.
    assert_eq!(
        (first, second),
        (first_decodes as usize, second_decodes as usize)
    );
    eprintln!("part reads: first run (admission and ten steps) {first}, second run {second}");
}

/// A store actor over a new session in memory on the setup parts store `parts`, serving runs
/// through `executor`, which the test keeps to read the requests its provider was sent.
async fn actor_on(
    executor: Arc<LoopExecutor>,
    parts: &SharedParts,
) -> (
    Commands,
    tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
) {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let actor = StoreActor::new(bind(), session, executor, Box::new(parts.clone()))
        .with_readers(Arc::new(move || backend));
    let (commands, receiver) = mpsc::unbounded_channel();
    (
        Commands::over(commands, backend),
        tokio::spawn(actor.serve(receiver)),
    )
}

/// A request's tool list as canonical JSON bytes, the form every provider request renderer
/// writes it in.
fn tool_bytes(call: &broca_protocol::request::CallOptions) -> String {
    let tools = serde_json::to_value(&*call.tools).unwrap();
    String::from_utf8(broca_protocol::canonical::canonical_json_bytes(&tools)).unwrap()
}

/// A tool schema whose numbers its stored part spells differently (`1.0` is stored as `1`, `1e3`
/// as `1000`) renders the same tool bytes in a run's fresh first step, in that run's resumed
/// step, and in the next run, which inherits the tools: every run renders the decoded part.
#[tokio::test]
async fn tool_bytes_match_across_a_fresh_step_its_resume_and_the_next_run() {
    let executor = Arc::new(executor(
        vec![
            auth_failure(),
            text_step("a1", "hello"),
            text_step("a2", "again"),
        ],
        None,
    ));
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let (commands, served) = actor_on(executor.clone(), &parts).await;
    let mut first = request("first");
    first.tools = Some(vec![serde_json::from_value(serde_json::json!({
        "name": "read",
        "description": "reads a file",
        "input_schema": {
            "type": "object",
            "properties": {"lines": {"type": "number", "minimum": 1.0, "maximum": 1e3}},
        },
    }))
    .unwrap()]);
    let keyed = SendKey::SendId("k1".into());
    assert!(matches!(
        send_with(&commands, "r-k1", first.clone(), keyed.clone()).await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    // The run paused for sign-in; its retry resumes it from its start.
    assert!(matches!(
        send_with(&commands, "r-k1", first, keyed).await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    assert!(matches!(
        send(&commands, "r2", "second").await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    drop(commands);
    served.await.unwrap();
    let calls = executor.provider.inner.calls();
    assert_eq!(calls.len(), 3);
    let fresh = tool_bytes(&calls[0]);
    assert_eq!(tool_bytes(&calls[1]), fresh, "the resumed step");
    assert_eq!(tool_bytes(&calls[2]), fresh, "the next run");
    assert!(
        fresh.contains(r#"{"maximum":1000,"minimum":1,"type":"number"}"#),
        "every run renders the stored spelling: {fresh}"
    );
}

/// Runs each store job as [`LoopExecutor`] does, noting the plan manifest the actor gave each
/// and the start of the session's latest run when the job began.
struct Recording {
    inner: LoopExecutor,
    manifests: std::sync::Mutex<Vec<Option<Arc<broca_records::record::PlanManifest>>>>,
    latest: std::sync::Mutex<Vec<Option<broca_session_store::record::RunStart>>>,
}

impl Recording {
    fn new(inner: LoopExecutor) -> Arc<Self> {
        Arc::new(Self {
            inner,
            manifests: Default::default(),
            latest: Default::default(),
        })
    }

    fn manifests(&self) -> Vec<Option<Arc<broca_records::record::PlanManifest>>> {
        self.manifests.lock().unwrap().clone()
    }
}

#[async_trait]
impl RunExecutor for Recording {
    fn hook_plane(&self) -> Option<Arc<dyn broca_core::HookPlane>> {
        self.inner.hook_plane()
    }

    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::CompactionPlane>> {
        self.inner.compaction_plane()
    }

    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: mpsc::Sender<broca_core::OutputEvent>,
    ) -> StoreRunEnd {
        self.manifests
            .lock()
            .unwrap()
            .push(job.control.manifest.clone());
        let latest = job.writer.operations().run.as_ref();
        self.latest
            .lock()
            .unwrap()
            .push(latest.and_then(|run| run.start.clone()));
        self.inner.execute_on_store(job, out).await
    }
}

/// A store actor over a new session in memory on the setup parts store `parts`, at its own
/// backend, serving runs through `executor`. It has no record readers, so its media fold stays
/// off: that fold reads each run start's parts again as JSON values to rebuild the old storage's
/// records, which no session keeps, so those reads are outside what the sharing tests count.
async fn recorded_actor(
    executor: Arc<Recording>,
    parts: &SharedParts,
) -> (
    Commands,
    tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
) {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let actor = StoreActor::new(bind(), session, executor, Box::new(parts.clone()));
    let (commands, receiver) = mpsc::unbounded_channel();
    (
        Commands::over(commands, backend),
        tokio::spawn(actor.serve(receiver)),
    )
}

/// Two sessions on one plan, each over its own store actor and one shared parts store, hold one
/// tool list and one manifest. While the first session's run is in flight it holds every part
/// its start names, so the second session's admission, its next run's admission and a read of
/// its latest admission decode none of them.
#[tokio::test]
async fn sessions_on_one_plan_share_its_decoded_parts() {
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let (reached, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let mut first_executor = executor(
        vec![
            read_call("a1", "c1"),
            text_step("a2", "done"),
            text_step("a3", "again"),
        ],
        None,
    );
    first_executor.planes.tool_gate = Some((reached.clone(), release.clone()));
    let first_executor = Recording::new(first_executor);
    let second_executor = Recording::new(executor(
        vec![text_step("b1", "hello"), text_step("b2", "again")],
        None,
    ));
    let (first, first_served) = recorded_actor(first_executor.clone(), &parts).await;
    let (second, second_served) = recorded_actor(second_executor.clone(), &parts).await;
    let manifest = plan_manifest("plan-1", serde_json::json!({}));

    // The first session's run, held while its tool call is in flight.
    let before = broca_session_loop::parts::decodes_on_this_thread();
    assert!(matches!(
        send_planned(
            &first,
            "r1",
            long("first"),
            "plan-1",
            Some(manifest.clone())
        )
        .await,
        SendOutcome::Active { .. }
    ));
    reached.notified().await;
    let first_admission = broca_session_loop::parts::decodes_on_this_thread() - before;
    assert!(
        first_admission > 0,
        "the first admission decodes the parts it stored, on this thread"
    );

    let before = broca_session_loop::parts::decodes_on_this_thread();
    assert!(matches!(
        send_planned(&second, "s1", long("first"), "plan-1", Some(manifest)).await,
        SendOutcome::Active { .. }
    ));
    idle(&second).await;
    assert!(matches!(
        send_planned(&second, "s2", long("second"), "plan-1", None).await,
        SendOutcome::Active { .. }
    ));
    idle(&second).await;
    assert_eq!(
        broca_session_loop::parts::decodes_on_this_thread() - before,
        0,
        "the second session's two runs decoded a part the first session's run holds"
    );
    // Reading the second session's latest admission again decodes nothing either.
    let latest = second_executor.latest.lock().unwrap()[1].clone().unwrap();
    let before = broca_session_loop::parts::decodes_on_this_thread();
    let admission = broca_session_loop::run::admission_of_start(&latest, &parts).unwrap();
    assert_eq!(broca_session_loop::parts::decodes_on_this_thread(), before);

    release.notify_one();
    idle(&first).await;
    assert!(matches!(
        send_planned(&first, "r2", long("second"), "plan-1", None).await,
        SendOutcome::Active { .. }
    ));
    idle(&first).await;
    drop((first, second));
    first_served.await.unwrap();
    second_served.await.unwrap();

    // Every request of both sessions carries the one tool list.
    let calls: Vec<_> = first_executor
        .inner
        .provider
        .inner
        .calls()
        .into_iter()
        .chain(second_executor.inner.provider.inner.calls())
        .collect();
    assert_eq!(calls.len(), 5);
    for call in &calls[1..] {
        assert!(Arc::ptr_eq(&call.tools, &calls[0].tools));
    }
    // Each session's later run was given the one manifest, whose tool list is the requests'.
    let later: Vec<_> = [&first_executor, &second_executor]
        .iter()
        .map(|executor| executor.manifests()[1].clone().unwrap())
        .collect();
    assert!(Arc::ptr_eq(&later[0], &later[1]));
    assert!(Arc::ptr_eq(&later[0].tools, &calls[0].tools));
    // So does the latest admission read back.
    assert!(Arc::ptr_eq(&admission.tools, &calls[0].tools));
    assert!(Arc::ptr_eq(admission.manifest.as_ref().unwrap(), &later[0]));
}
