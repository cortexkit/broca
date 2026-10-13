//! Cut every writer record boundary, then resume against real, frozen hook routes.
//! The process cut parks the WAL writer after post_tool answers but before its result append.
mod delivery_rig;
mod step_transform_rig;
use async_trait::async_trait;
use broca_core::{
    AllowAll, BatchCtx, DispatchResult, FileWalStore, Provider, RunControl, RunRequest, StartMode,
    ToolManifest, ToolPlane, ToolPlaneError, WalHandle, WalStore,
};
use broca_protocol::{
    content::{AssistantMessage, ContentBlock, Message, ToolCall, ToolOutput, ToolResult},
    provider::{NormalizedEvent, ProviderControl},
    request::CallOptions,
    BindIdentity, FinishReason, MessageId, RunId, StepId, SubmissionId, ToolCallId, Usage,
};
use broca_wal::{FramedRecord, Wal, WalError, WalRecord};
use cortexkit_role_step_transform_provider::{answer::HookAnswer, hook::Subject};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use step_transform_rig::*;
use tokio::sync::mpsc;

#[derive(Default)]
struct Model {
    calls: Mutex<Vec<CallOptions>>,
    siblings: bool,
}
#[async_trait]
impl Provider for Model {
    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.calls.lock().unwrap().push(opts.clone());
        let mut content = if step_id.0 == 1 {
            vec![
                ContentBlock::Text {
                    text: "assistant".into(),
                },
                ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId("c".into()),
                    tool_name: "work".into(),
                    input: json!({"value":"original"}),
                    thought_signature: Some("signed".into()),
                }),
            ]
        } else {
            vec![ContentBlock::Text {
                text: "done".into(),
            }]
        };
        if step_id.0 == 1 && self.siblings {
            content.push(ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("c2".into()),
                tool_name: "work".into(),
                input: json!({"value":"second"}),
                thought_signature: Some("signed-second".into()),
            }));
        }
        let (tx, rx) = mpsc::channel(8);
        tx.send(NormalizedEvent::Control(
            ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId(format!("m{}", step_id.0)),
                    content,
                },
            },
        ))
        .await
        .unwrap();
        tx.send(NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: if step_id.0 == 1 {
                FinishReason::ToolCalls
            } else {
                FinishReason::Stop
            },
            retries_used: 0,
        }))
        .await
        .unwrap();
        rx
    }
}
struct Tools {
    dispatched: Mutex<Vec<String>>,
    root: PathBuf,
}
#[async_trait]
impl ToolPlane for Tools {
    async fn batch_started(
        &self,
        _: &BatchCtx,
        _: &[ToolCall],
        _: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn batch_finished(&self, _: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    fn dispatch_target(&self, _: &BatchCtx, _: &ToolCallId) -> Option<String> {
        Some(MODULE.into())
    }
    async fn dispatch(&self, _: &BatchCtx, call: &ToolCall) -> DispatchResult {
        use std::io::Write;
        self.dispatched
            .lock()
            .unwrap()
            .push(call.tool_call_id.0.clone());
        let mut log = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(self.root.join("dispatches"))
            .unwrap();
        writeln!(log, "{}", call.tool_call_id.0).unwrap();
        log.sync_all().unwrap();
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Text {
                text: "result".into(),
            },
            is_error: false,
        })
    }
}
fn project_root() -> PathBuf {
    std::env::var_os("BROCA_HOOK_PROJECT")
        .map(PathBuf::from)
        .unwrap_or_else(broca_test_support::scratch_root)
}

fn request(manifest: &broca_wal::PlanManifest) -> RunRequest {
    let submission=serde_json::from_value(json!({"model":{"provider_module_id":"fake","model_id":"fake"},"input":[{"role":"user","content":[{"type":"text","text":"user"}]}],"tools":manifest.tools.as_ref(),"policy_allowed":["work"]})).unwrap();
    let mut request = RunRequest::from_submission(
        submission,
        RunId("crash-run".into()),
        BindIdentity::new(project_root(), "crash-caller", "s"),
        SubmissionId("unused".into()),
    );
    request.submission_id = None;
    request
}
struct ParkBeforeResult {
    inner: Box<dyn Wal + Send>,
    marker: PathBuf,
}
impl Wal for ParkBeforeResult {
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        if matches!(record, WalRecord::ToolResult { .. }) {
            std::fs::write(
                &self.marker,
                b"post_tool answered before ToolResultRecorded",
            )
            .unwrap();
            let (_tx, rx) = std::sync::mpsc::channel::<()>();
            let _ = rx.recv();
        }
        self.inner.append(fence, record, sync)
    }
    fn sync(&self) -> Result<(), WalError> {
        self.inner.sync()
    }
    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        self.inner.replay()
    }
    fn tail_after_seq(&self, s: u64) -> Result<Vec<FramedRecord>, WalError> {
        self.inner.tail_after_seq(s)
    }
    fn last_seq(&self) -> u64 {
        self.inner.last_seq()
    }
    fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        self.inner.lineage_id()
    }
}
async fn drive(
    root: &Path,
    connection: &Path,
    manifest: Arc<broca_wal::PlanManifest>,
    park: bool,
    installed: bool,
) -> (
    Result<broca_core::RunSummary, broca_core::RunError>,
    Model,
    Tools,
) {
    std::fs::create_dir_all(root).unwrap();
    let store = FileWalStore::new(root.join("wal"));
    let mut req = request(&manifest);
    let wal = store.open(&req.session).unwrap();
    let wal: Box<dyn Wal + Send> = if park {
        Box::new(ParkBeforeResult {
            inner: wal,
            marker: root.join("reached"),
        })
    } else {
        wal
    };
    let writer = WalHandle::spawn(wal, 1).unwrap();
    if writer.next_record_seq() > 1 {
        // Internal resume carries no new send input. A caller retry is answered by the actor
        // from its recorded pre-hook identity, covered by the serve-path tests.
        req.input.clear();
    }
    let conn = Arc::new(
        broca_subc::SubcConnection::connect(connection)
            .await
            .unwrap(),
    );
    let plane: Arc<dyn broca_core::HookPlane> = Arc::new(broca_subc::SubcHookPlane::new(conn));
    let mut control = RunControl {
        manifest: Some(manifest.clone()),
        hook_plane: installed.then_some(plane.clone()),
        ..Default::default()
    };
    if writer.next_record_seq() == 1 && installed {
        let plan = broca_core::hooks::HookPlan::from_manifest(&manifest).unwrap();
        let runtime = broca_core::hooks::HookRuntime {
            plan: &plan,
            plane: &plane,
            bind: &req.session,
            writer: &writer,
            cancel: None,
            stop: None,
            seal: None,
        };
        let original = serde_json::to_value(&req.input).unwrap();
        let output = runtime
            .chain(
                Subject::PreUser {
                    blocks: vec!["user".into()],
                    mark: None,
                    delivery: None,
                },
                Default::default(),
                |_| true,
            )
            .await;
        let Subject::PreUser { blocks, .. } = output.subject else {
            unreachable!()
        };
        req.input[0].content[0] = ContentBlock::Text {
            text: blocks[0].clone(),
        };
        control.input_hooks =
            output
                .trace
                .metadata(original, serde_json::to_value(&req.input).unwrap(), false);
    }
    let model = Model::default();
    let tools = Tools {
        dispatched: Default::default(),
        root: root.into(),
    };
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = broca_core::run_loop_on_writer(
        req,
        &writer,
        StartMode::FreshOrResume,
        &model,
        &tools,
        &AllowAll,
        &store,
        None,
        control,
        &tx,
    )
    .await;
    writer.shutdown().await;
    drop(tx);
    collector.await.unwrap();
    (summary, model, tools)
}
fn log(root: &Path) -> (PathBuf, Vec<FramedRecord>) {
    let store = FileWalStore::new(root.join("wal"));
    let path = store.wal_path(&BindIdentity::new(project_root(), "crash-caller", "s"));
    let records = broca_wal::replay_file(&path, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
    (path, records)
}
fn first_step_calls(state: &State) -> std::collections::HashMap<String, usize> {
    let mut counts = std::collections::HashMap::new();
    for call in state
        .hooks()
        .into_iter()
        .filter(|c| c.get("step_id").is_none() || c["step_id"] == "1")
    {
        *counts.entry(key(&call)).or_default() += 1;
    }
    counts
}
fn expected(prefix: &[WalRecord]) -> std::collections::HashMap<String, usize> {
    let started = prefix
        .iter()
        .any(|r| matches!(r, WalRecord::RunStarted { .. }));
    let step = prefix
        .iter()
        .any(|r| matches!(r,WalRecord::ModelStepFinished{step_id,..} if step_id.0==1));
    let intent = prefix
        .iter()
        .any(|r| matches!(r, WalRecord::ToolDispatchIntent { .. }));
    let terminal = prefix.iter().any(|r| {
        matches!(
            r,
            WalRecord::ToolResult { .. } | WalRecord::ToolNoDispatch { .. }
        )
    });
    let mut counts = std::collections::HashMap::new();
    if !started {
        counts.insert("pre_user".into(), 1);
    }
    if !step {
        counts.insert("post_assistant".into(), 1);
    }
    if !intent && !terminal {
        counts.insert("pre_tool:mutate".into(), 1);
        counts.insert("pre_tool:validate".into(), 1);
        counts.insert("post_tool".into(), 1);
    }
    counts
}
fn assert_rendered_prefix(
    model: &Model,
    baseline: &CallOptions,
    live_prefix: &[Message],
    prefix: &[Message],
    after: &[Message],
) {
    assert!(after.len() >= prefix.len());
    assert!(live_prefix.len() >= prefix.len());
    let mut live = baseline.clone();
    live.prompt = live_prefix[..prefix.len()].to_vec();
    let mut old = baseline.clone();
    old.prompt = prefix.to_vec();
    // Expected bytes come from the uninterrupted provider request, not another replay of the
    // same WAL. A replay that reapplied attribution ops would otherwise agree with itself.
    assert_eq!(
        model.render_request(&live),
        model.render_request(&old),
        "the durable prefix must match the uncut live request"
    );
    let mut resumed = baseline.clone();
    resumed.prompt = after[..prefix.len()].to_vec();
    assert_eq!(
        model.render_request(&live),
        model.render_request(&resumed),
        "durable prefix must render byte-identically"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_record_cut_preserves_prefix_and_exact_hook_call_table() {
    let root = broca_test_support::scratch_root()
        .join(format!("step-transform-cuts-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let state = State::new(vec![
        subscription("pre_user", None, "pass", 1000),
        subscription("post_assistant", None, "pass", 1000),
        subscription("pre_tool", Some("mutate"), "pass", 1000),
        subscription("pre_tool", Some("validate"), "refuse", 1000),
        subscription("post_tool", None, "pass", 1000),
    ]);
    state.answer_with(|c| match key(c).as_str() {
        "pre_tool:mutate" => serde_json::to_value(HookAnswer::Mutate {
            input: json!({"value":"mutated"}),
            note: None,
        })
        .unwrap(),
        "pre_tool:validate" => serde_json::to_value(HookAnswer::Pass).unwrap(),
        _ => append("-hooked"),
    });
    let daemon = Daemon::start(&root, state.clone()).await;
    let manifest = frozen(&state);
    let reference = root.join("reference");
    let (summary, model, tools) = drive(
        &reference,
        &daemon.connection_file,
        manifest.clone(),
        false,
        true,
    )
    .await;
    summary.unwrap();
    assert_eq!(*tools.dispatched.lock().unwrap(), vec!["c"]);
    assert_eq!(
        first_step_calls(&state),
        expected(&[]),
        "the uncut control must reach every hook"
    );
    let (path, framed) = log(&reference);
    let bytes = std::fs::read(path).unwrap();
    let mut ends = vec![0];
    let mut at = 0;
    while at < bytes.len() {
        match broca_wal::framing::decode_next(&bytes[at..]) {
            broca_wal::framing::DecodeStep::Lineage { consumed, .. } => {
                at += consumed;
                *ends.last_mut().unwrap() = at;
            }
            broca_wal::framing::DecodeStep::Record { consumed, .. } => {
                at += consumed;
                ends.push(at);
            }
            _ => panic!("unexpected reference frame"),
        }
    }
    assert_eq!(ends.len(), framed.len() + 1);
    let baseline = model.calls.lock().unwrap()[0].clone();
    let mut live_prefix = model.calls.lock().unwrap()[1].prompt.clone();
    let final_message = framed
        .iter()
        .rev()
        .find_map(|frame| match &frame.record {
            WalRecord::ModelStepFinished {
                assistant_message, ..
            } => Some(assistant_message),
            _ => None,
        })
        .unwrap();
    live_prefix.push(Message::assistant_from(
        final_message.content.clone(),
        baseline.target_model.clone().unwrap(),
    ));
    let mut named = std::collections::BTreeSet::new();
    for (n, end) in ends.into_iter().enumerate() {
        let cut = root.join(format!("cut-{n}"));
        std::fs::create_dir_all(&cut).unwrap();
        let store = FileWalStore::new(cut.join("wal"));
        let path = store.wal_path(&request(&manifest).session);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &bytes[..end]).unwrap();
        assert_eq!(
            log(&cut).1.len(),
            n,
            "cut {n}: actual bytes must hold only the requested records; end={end}"
        );
        let prefix: Vec<_> = framed[..n].iter().map(|f| f.record.clone()).collect();
        if let Some(r) = prefix.last() {
            let name = match r {
                WalRecord::RunStarted { .. } => Some("SendRecorded"),
                WalRecord::ModelStepFinished { step_id, .. } if step_id.0 == 1 => {
                    Some("StepRecorded")
                }
                WalRecord::ToolDispatchIntent { .. } => Some("DispatchIntent"),
                WalRecord::ToolResult { .. } => Some("ToolResultRecorded"),
                _ => None,
            };
            if let Some(name) = name {
                named.insert(name);
            }
        }
        let rendered = broca_core::replay(&prefix).unwrap().prompt;
        state.clear();
        *state.refuse_declare.lock().unwrap() = true;
        let (summary, resumed, tools) =
            drive(&cut, &daemon.connection_file, manifest.clone(), false, true).await;
        summary.unwrap();
        assert_eq!(state.declares(), 0, "cut {n}: declaration must stay frozen");
        assert_eq!(
            first_step_calls(&state),
            expected(&prefix),
            "cut {n}: exact per-cut hook table; requests={:?}; records={:?}",
            state.requests.lock().unwrap(),
            log(&cut).1
        );
        let intent = prefix
            .iter()
            .any(|r| matches!(r, WalRecord::ToolDispatchIntent { .. }));
        assert_eq!(
            tools.dispatched.lock().unwrap().len(),
            usize::from(!intent),
            "cut {n}: an intent is never dispatched twice"
        );
        let (path, after) = log(&cut);
        assert!(
            std::fs::read(path).unwrap().starts_with(&bytes[..end]),
            "cut {n}: durable bytes changed"
        );
        let after_records: Vec<_> = after.into_iter().map(|f| f.record).collect();
        let transcript = broca_core::replay(&after_records).unwrap().prompt;
        assert_rendered_prefix(&resumed, &baseline, &live_prefix, &rendered, &transcript);
        if intent
            && !prefix
                .iter()
                .any(|r| matches!(r, WalRecord::ToolResult { .. }))
        {
            assert!(after_records.iter().any(|r| matches!(r,WalRecord::ToolResult{outcome:broca_wal::DispatchOutcome::Unknown,result,..} if serde_json::to_string(result).unwrap().contains("outcome_unknown"))));
        }
    }
    assert_eq!(
        named,
        [
            "SendRecorded",
            "StepRecorded",
            "DispatchIntent",
            "ToolResultRecorded"
        ]
        .into_iter()
        .collect()
    );
    // A real process dies with intents durable and post_tool answered, but no result recorded.
    let killed = root.join("process-cut");
    std::fs::create_dir_all(&killed).unwrap();
    std::fs::write(
        killed.join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    state.clear();
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "hook_crash_child", "--nocapture"])
        .env("BROCA_HOOK_CHILD_ROOT", &killed)
        .env("BROCA_HOOK_PROJECT", project_root())
        .env("BROCA_HOOK_CHILD_CONNECTION", &daemon.connection_file)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        while !killed.join("reached").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited before cut"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        first_step_calls(&state),
        [
            ("pre_user".into(), 1),
            ("post_assistant".into(), 1),
            ("pre_tool:mutate".into(), 1),
            ("pre_tool:validate".into(), 1),
            ("post_tool".into(), 1)
        ]
        .into_iter()
        .collect()
    );
    child.kill().await.unwrap();
    let status = child.wait().await.unwrap();
    assert!(!status.success(), "a real process was killed");
    let (_, before) = log(&killed);
    let prefix: Vec<_> = before.into_iter().map(|f| f.record).collect();
    assert!(prefix
        .iter()
        .any(|r| matches!(r, WalRecord::ToolDispatchIntent { .. })));
    assert!(!prefix
        .iter()
        .any(|r| matches!(r, WalRecord::ToolResult { .. })));
    let rendered = broca_core::replay(&prefix).unwrap().prompt;
    state.clear();
    let (summary, resumed, tools) =
        drive(&killed, &daemon.connection_file, manifest, false, true).await;
    summary.unwrap();
    assert!(tools.dispatched.lock().unwrap().is_empty());
    assert_eq!(
        std::fs::read_to_string(killed.join("dispatches")).unwrap(),
        "c\n"
    );
    assert!(state.hooks().is_empty());
    assert_eq!(state.declares(), 0);
    let (_, after) = log(&killed);
    let transcript = broca_core::replay(&after.into_iter().map(|f| f.record).collect::<Vec<_>>())
        .unwrap()
        .prompt;
    assert_rendered_prefix(&resumed, &baseline, &live_prefix, &rendered, &transcript);
    daemon.finish().await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hook_crash_child() {
    let Some(root) = std::env::var_os("BROCA_HOOK_CHILD_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let connection = PathBuf::from(std::env::var_os("BROCA_HOOK_CHILD_CONNECTION").unwrap());
    let manifest =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
    let _ = drive(&root, &connection, manifest, true, true).await;
    panic!("child must be killed before the result append");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_tool_excludes_resumed_already_terminals() {
    let root =
        broca_test_support::scratch_root().join(format!("already-hook-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let state = State::new(vec![subscription("post_tool", None, "pass", 1000)]);
    state.answer_with(|_| append("-hooked"));
    let daemon = Daemon::start(&root, state.clone()).await;
    let manifest = frozen(&state);
    let reference = root.join("reference");
    let (summary, _, _) = drive(
        &reference,
        &daemon.connection_file,
        manifest.clone(),
        false,
        true,
    )
    .await;
    summary.unwrap();
    assert_eq!(state.hooks().len(), 1);
    let (path, _) = log(&reference);
    let bytes = std::fs::read(path).unwrap();
    let mut at = 0;
    let mut end = 0;
    while at < bytes.len() {
        match broca_wal::framing::decode_next(&bytes[at..]) {
            broca_wal::framing::DecodeStep::Record { framed, consumed } => {
                if end > 0 {
                    break;
                }
                at += consumed;
                if matches!(framed.record, WalRecord::ToolResult { .. }) {
                    end = at;
                }
            }
            broca_wal::framing::DecodeStep::Lineage { consumed, .. } => {
                at += consumed;
                if end > 0 {
                    end = at;
                }
            }
            _ => panic!("invalid reference WAL"),
        }
    }
    assert!(end > 0);
    let cut = root.join("cut");
    let path = FileWalStore::new(cut.join("wal")).wal_path(&request(&manifest).session);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, &bytes[..end]).unwrap();
    state.clear();
    *state.refuse_declare.lock().unwrap() = true;
    let (summary, _, tools) = drive(&cut, &daemon.connection_file, manifest, false, true).await;
    summary.unwrap();
    assert!(state.hooks().is_empty());
    assert_eq!(state.declares(), 0);
    assert!(tools.dispatched.lock().unwrap().is_empty());
    let (_, records) = log(&cut);
    assert_eq!(
        records
            .iter()
            .filter(|f| matches!(f.record, WalRecord::ToolResult { .. }))
            .count(),
        1
    );
    assert!(records.iter().any(|f|matches!(&f.record,WalRecord::ToolResult{result,..} if result.output==ToolOutput::Text{text:"result-hooked".into()})));
    daemon.finish().await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_user_answer_lost_before_send_record_retries_once() {
    let root = broca_test_support::scratch_root()
        .join(format!("pre-user-answer-cut-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let state = State::new(vec![subscription("pre_user", None, "pass", 1000)]);
    state.answer_with(|_| append("-hooked"));
    let daemon = Daemon::start(&root, state.clone()).await;
    let manifest = frozen(&state);
    let cut = root.join("cut");
    let store = FileWalStore::new(cut.join("wal"));
    let req = request(&manifest);
    let writer = WalHandle::spawn(store.open(&req.session).unwrap(), 1).unwrap();
    let conn = Arc::new(
        broca_subc::SubcConnection::connect(&daemon.connection_file)
            .await
            .unwrap(),
    );
    let plane: Arc<dyn broca_core::HookPlane> = Arc::new(broca_subc::SubcHookPlane::new(conn));
    let plan = broca_core::hooks::HookPlan::from_manifest(&manifest).unwrap();
    let runtime = broca_core::hooks::HookRuntime {
        plan: &plan,
        plane: &plane,
        bind: &req.session,
        writer: &writer,
        cancel: None,
        stop: None,
        seal: None,
    };
    let answered = runtime
        .chain(
            Subject::PreUser {
                blocks: vec!["user".into()],
                mark: None,
                delivery: None,
            },
            Default::default(),
            |_| true,
        )
        .await;
    assert_eq!(answered.subject.blocks().unwrap(), ["user-hooked"]);
    assert_eq!(state.hooks().len(), 1);
    writer.shutdown().await;
    assert!(
        log(&cut).1.is_empty(),
        "an answered but unacknowledged send has no durable record"
    );
    state.clear();
    let (summary, _, _) = drive(&cut, &daemon.connection_file, manifest, false, true).await;
    summary.unwrap();
    assert_eq!(state.hooks().len(), 1);
    let (_, records) = log(&cut);
    let input = records
        .iter()
        .find_map(|f| match &f.record {
            WalRecord::RunStarted { input, .. } => Some(input),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        input[0].content[0],
        ContentBlock::Text {
            text: "user-hooked".into()
        }
    );
    daemon.finish().await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequential_batch_finishes_post_tool_before_next_dispatch() {
    let root = broca_test_support::scratch_root()
        .join(format!("sequential-post-tool-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let state = State::new(vec![subscription("post_tool", None, "pass", 1000)]);
    let dispatches = root.join("dispatches");
    state.answer_with(move |c| {
        let expected = if c["tool_call_id"] == "c" {
            "c\n"
        } else {
            assert_eq!(c["tool_call_id"], "c2");
            "c\nc2\n"
        };
        assert_eq!(
            std::fs::read_to_string(&dispatches).unwrap(),
            expected,
            "the next dispatch must wait for this hook"
        );
        append("-hooked")
    });
    let daemon = Daemon::start(&root, state.clone()).await;
    let manifest = frozen(&state);
    let req = request(&manifest);
    let store = FileWalStore::new(root.join("wal"));
    let writer = WalHandle::spawn(store.open(&req.session).unwrap(), 1).unwrap();
    let conn = Arc::new(
        broca_subc::SubcConnection::connect(&daemon.connection_file)
            .await
            .unwrap(),
    );
    let plane: Arc<dyn broca_core::HookPlane> = Arc::new(broca_subc::SubcHookPlane::new(conn));
    let model = Model {
        calls: Default::default(),
        siblings: true,
    };
    let tools = Tools {
        dispatched: Default::default(),
        root: root.clone(),
    };
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    broca_core::run_loop_on_writer(
        req,
        &writer,
        StartMode::FreshOrResume,
        &model,
        &tools,
        &AllowAll,
        &store,
        None,
        RunControl {
            manifest: Some(manifest),
            hook_plane: Some(plane),
            ..Default::default()
        },
        &tx,
    )
    .await
    .unwrap();
    writer.shutdown().await;
    drop(tx);
    collector.await.unwrap();
    assert_eq!(state.hooks().len(), 2);
    assert_eq!(*tools.dispatched.lock().unwrap(), vec!["c", "c2"]);
    daemon.finish().await;
    std::fs::remove_dir_all(root).unwrap();
}
