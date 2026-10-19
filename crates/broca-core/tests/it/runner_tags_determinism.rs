//! Request bytes of a run frozen with `render_tags` are the bytes a rebuild from the WAL gives,
//! at every step. That is broca's resume rule: a resumed run renders from the WAL, so a live
//! request that differed from the rebuild would change the prompt the provider has cached.
//!
//! The live loop attaches tags to the messages it adds from what the writer assigned, and a
//! rebuild attaches them from the records. Each case here compares the two, step by step, where
//! the live side could drift: a steer joining the running turn, a budget nudge, a tool call id
//! the model repeats, a budget finalize turn whose calls are dropped, a resumed pending turn,
//! and a crash cut at every record boundary of a tool loop with a steer. The cases run for a
//! family that keeps assistant tags as metadata and for Anthropic Messages, which shows them
//! in assistant text, so every assistant turn's rendering is compared too.

use std::collections::HashSet;
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
    request::{BudgetConfig, BudgetNudge, Tool},
    BindIdentity, CallOptions, FinishReason, MessageId, RunId, StepId, SubmissionId, ToolCallId,
    Usage,
};
use broca_wal::{FramedRecord, WalRecord};
use serde_json::json;
use tokio::sync::mpsc;

/// A model whose answer depends only on how many assistant turns the request already holds,
/// so a resumed run asks for, and gets, exactly what the uninterrupted run did at that point.
///
/// `family` is the wire family its frozen render config names, which decides whether
/// assistant text shows its tag (Anthropic Messages only).
struct TurnModel {
    turns: Vec<Vec<ContentBlock>>,
    family: Option<&'static str>,
    calls: Mutex<Vec<CallOptions>>,
}

impl TurnModel {
    fn new(turns: Vec<Vec<ContentBlock>>, family: Option<&'static str>) -> Self {
        Self {
            turns,
            family,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<CallOptions> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Provider for TurnModel {
    fn render_config_blob(&self) -> serde_json::Value {
        match self.family {
            Some(family) => json!({"render_config": "stateless", "family_id": family}),
            None => json!({"render_config": "stateless"}),
        }
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

fn lookup() -> Tool {
    Tool {
        name: "lookup".into(),
        description: "look something up".into(),
        input_schema: json!({"type":"object"}),
        module: None,
        execution_mode: Default::default(),
    }
}

/// A session's frozen plan manifest with `render_tags` on.
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

fn request(name: &str, budget: Option<BudgetConfig>) -> RunRequest {
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
        RunId(format!("run-{name}")),
        BindIdentity::new("/tmp/tags-determinism", "runner", name),
        SubmissionId(format!("send-{name}")),
    );
    request.submission_id = None;
    request.policy_allowed = HashSet::from(["lookup".to_owned()]);
    request.budget = budget;
    request
}

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-tags-determinism-{}-{name}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// A steer that a tool's execution adds to the running turn, the way the session's actor does:
/// the `Steered` record and the queued message under the inbox's lock.
#[derive(Clone)]
struct SteerOnce {
    inbox: Arc<SteerInbox>,
    writer: Arc<Mutex<Option<Arc<WalHandle>>>>,
    sent: Arc<std::sync::atomic::AtomicBool>,
}

impl SteerOnce {
    fn deliver(&self, text: &str) {
        if self.sent.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let writer = self.writer.lock().unwrap().clone().expect("writer set");
        let input = vec![Message::new(Role::User, vec![self::text(text)])];
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
    reason: RunFinishReason,
}

/// Run `request` to its end over the WAL in `store`, continuing whatever that WAL holds.
async fn run(
    store: &FileWalStore,
    request: RunRequest,
    turns: Vec<Vec<ContentBlock>>,
    steer: Option<&str>,
    family: Option<&'static str>,
) -> Run {
    let session = request.session.clone();
    let writer = Arc::new(WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap());
    let steering = SteerOnce {
        inbox: SteerInbox::new(),
        writer: Arc::new(Mutex::new(Some(writer.clone()))),
        sent: Arc::new(std::sync::atomic::AtomicBool::new(steer.is_none())),
    };
    let steer_text = steer.unwrap_or_default().to_owned();
    let on_dispatch = steering.clone();
    let plane = StubToolPlane::new().with_tool("lookup", move |_| {
        on_dispatch.deliver(&steer_text);
        StubReply::Output(ToolOutput::Text {
            text: "two hits".into(),
        })
    });
    let model = TurnModel::new(turns, family);
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
    steering.writer.lock().unwrap().take();
    Arc::into_inner(writer)
        .expect("the test holds the last writer handle")
        .shutdown()
        .await
        .unwrap();
    Run {
        calls: model.calls(),
        records: store.read_records(&session).unwrap(),
        reason: summary.reason,
    }
}

/// The request a resume rebuilds from the WAL for the step that `records[step_started]`
/// opened: the production rebuild (the one cache warming sends), given an attempt record for
/// that step.
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

/// Every request `run` sent equals, byte for byte, the request rebuilt from its WAL for the
/// same step. `first_new` is the index of the run's first own record (0 for a fresh run).
fn assert_live_matches_rebuilt(label: &str, run: &Run, first_new: usize) {
    let steps: Vec<usize> = run
        .records
        .iter()
        .enumerate()
        .skip(first_new)
        .filter(|(_, frame)| matches!(frame.record, WalRecord::StepStarted { .. }))
        .map(|(index, _)| index)
        .collect();
    assert_eq!(
        steps.len(),
        run.calls.len(),
        "{label}: one step per request"
    );
    for (n, (live, at)) in run.calls.iter().zip(steps).enumerate() {
        let rebuilt = rebuilt(&run.records, at);
        assert_eq!(
            String::from_utf8(canonical_request_bytes(&rebuilt)).unwrap(),
            String::from_utf8(canonical_request_bytes(live)).unwrap(),
            "{label}: request {n} differs from its rebuild"
        );
    }
}

fn wal_tags(records: &[FramedRecord]) -> Vec<u64> {
    records
        .iter()
        .flat_map(|frame| frame.record.runner_tags().to_vec())
        .flat_map(|tags| tags.parts)
        .collect()
}

fn assert_tags_increase(label: &str, records: &[FramedRecord]) {
    let tags = wal_tags(records);
    assert!(!tags.is_empty(), "{label}: the session is tagged");
    assert!(
        tags.windows(2).all(|pair| pair[0] < pair[1]),
        "{label}: tags {tags:?}"
    );
}

/// Every text the request holds, in order, flattened.
fn sent_texts(opts: &CallOptions) -> Vec<String> {
    opts.prompt
        .iter()
        .flat_map(|message| message.content.iter().map(broca_protocol::tags::part_text))
        .collect()
}

/// No family (metadata-only assistant tags, as on every family but one) and Anthropic
/// Messages (assistant text shows its tag).
const FAMILIES: [Option<&str>; 2] = [None, Some(broca_protocol::tags::ANTHROPIC_MESSAGES_FAMILY)];

fn label(case: &str, family: Option<&str>) -> String {
    format!("{case} [{}]", family.unwrap_or("no family"))
}

/// A tool loop whose assistant turns carry text: one plain, one that begins with a tag mark the
/// model wrote itself (a wrong one), so Anthropic renders it with the part's own tag.
fn tool_loop_with_steer() -> Vec<Vec<ContentBlock>> {
    vec![
        vec![text("looking"), call("call-1")],
        vec![text("§99§ checking again"), call("call-2")],
        vec![text("done")],
    ]
}

#[tokio::test]
async fn a_steer_mid_run_renders_live_as_its_rebuild() {
    for family in FAMILIES {
        let label = label("steer", family);
        let store = FileWalStore::new(scratch(&label));
        let run = run(
            &store,
            request("steer", None),
            tool_loop_with_steer(),
            Some("also check the docs"),
            family,
        )
        .await;
        assert_eq!(run.reason, RunFinishReason::Completed);
        assert_tags_increase(&label, &run.records);
        assert_live_matches_rebuilt(&label, &run, 0);
        let second = sent_texts(&run.calls[1]);
        assert!(
            second
                .iter()
                .any(|text| text.starts_with('§') && text.ends_with("§ also check the docs")),
            "{label}: the steer is rendered with its tag: {second:?}"
        );
        // The first assistant text's tag is the one right after the user input's.
        let third = sent_texts(&run.calls[2]);
        let anthropic = family.is_some();
        assert_eq!(
            third.contains(&"§2§ looking".to_owned()),
            anthropic,
            "{label}: {third:?}"
        );
        assert_eq!(third.contains(&"looking".to_owned()), !anthropic, "{label}");
        // The model's own (wrong) mark is replaced by the part's tag on Anthropic, and kept
        // as written elsewhere.
        assert_eq!(
            third.contains(&"§99§ checking again".to_owned()),
            !anthropic,
            "{label}: {third:?}"
        );
        if anthropic {
            assert!(third
                .iter()
                .any(|text| text.ends_with("§ checking again") && !text.starts_with("§99§")));
        }
    }
}

#[tokio::test]
async fn a_repeated_tool_call_id_renders_live_as_its_rebuild() {
    let store = FileWalStore::new(scratch("repeat"));
    let run = run(
        &store,
        request("repeat", None),
        vec![vec![call("same")], vec![call("same")], vec![text("done")]],
        None,
        Some(broca_protocol::tags::ANTHROPIC_MESSAGES_FAMILY),
    )
    .await;
    assert_eq!(run.reason, RunFinishReason::Completed);
    assert_tags_increase("repeat", &run.records);
    assert_live_matches_rebuilt("repeat", &run, 0);
    // Two results for one call id, each with its own tag.
    let rendered: Vec<String> = sent_texts(&run.calls[2])
        .into_iter()
        .filter(|text| text.contains("two hits"))
        .collect();
    assert_eq!(rendered.len(), 2, "{rendered:?}");
    assert_ne!(rendered[0].split(' ').next(), rendered[1].split(' ').next());
}

#[tokio::test]
async fn a_budget_nudge_renders_live_as_its_rebuild() {
    let budget = BudgetConfig {
        nudges: vec![BudgetNudge {
            at_tool_calls: 1,
            text: "one call used".into(),
        }],
        ..BudgetConfig::default()
    };
    let store = FileWalStore::new(scratch("nudge"));
    let run = run(
        &store,
        request("nudge", Some(budget)),
        vec![
            vec![text("first"), call("call-1")],
            vec![call("call-2")],
            vec![text("done")],
        ],
        None,
        Some(broca_protocol::tags::ANTHROPIC_MESSAGES_FAMILY),
    )
    .await;
    assert_eq!(run.reason, RunFinishReason::Completed);
    assert!(run
        .records
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::BudgetFired { .. })));
    assert_tags_increase("nudge", &run.records);
    assert_live_matches_rebuilt("nudge", &run, 0);
    assert!(sent_texts(&run.calls[1])
        .iter()
        .any(|text| text.starts_with('§') && text.ends_with("§ one call used")));
}

/// A budget finalize turn's tool calls are never dispatched: the live loop records the turn
/// without them, and replay drops any such call a record holds. The blocks the turn keeps keep
/// their own tags, and the request renders as its rebuild.
#[tokio::test]
async fn a_finalize_turn_keeps_its_kept_blocks_tags_and_renders_as_its_rebuild() {
    let budget = BudgetConfig {
        finalize_at_tool_calls: 1,
        finalize_text: "wrap up now".into(),
        ..BudgetConfig::default()
    };
    let store = FileWalStore::new(scratch("finalize"));
    let run = run(
        &store,
        request("finalize", Some(budget)),
        vec![
            vec![call("call-1")],
            vec![text("final answer"), call("late")],
            vec![text("unreachable")],
        ],
        None,
        Some(broca_protocol::tags::ANTHROPIC_MESSAGES_FAMILY),
    )
    .await;
    assert_tags_increase("finalize", &run.records);
    assert_live_matches_rebuilt("finalize", &run, 0);
    let (at, finalize_tags) = run
        .records
        .iter()
        .enumerate()
        .rev()
        .find_map(|(at, frame)| match &frame.record {
            WalRecord::ModelStepFinished { tags, .. } => Some((
                at,
                tags.as_deref()
                    .cloned()
                    .expect("the finalize step is tagged"),
            )),
            _ => None,
        })
        .expect("the finalize step");
    let mut records: Vec<WalRecord> = run.records.iter().map(|f| f.record.clone()).collect();
    let transcript_answer = |records: &[WalRecord]| {
        broca_core::replay(records)
            .unwrap()
            .prompt
            .into_iter()
            .rev()
            .find(|message| message.role == Role::Assistant)
            .unwrap()
    };
    let answer = transcript_answer(&records);
    assert_eq!(answer.content, vec![text("final answer")]);
    assert_eq!(
        answer.part_tags().unwrap().parts,
        vec![Some(finalize_tags.parts[0])],
        "the kept text block keeps the tag the writer gave it"
    );

    // A finalize record that still holds a call (one a provider emitted, tagged like every
    // block): replay drops the call, and the text keeps its own tag, not the call's.
    let next = wal_tags(&run.records).last().copied().unwrap() + 1;
    if let WalRecord::ModelStepFinished {
        assistant_message,
        tags,
        ..
    } = &mut records[at]
    {
        let mut message = (**assistant_message).clone();
        message.content.insert(0, call("late"));
        *assistant_message = Arc::new(message);
        *tags = Some(Box::new(broca_protocol::tags::MessageTags::new(vec![
            next,
            finalize_tags.parts[0],
        ])));
    }
    let answer = transcript_answer(&records);
    assert_eq!(answer.content, vec![text("final answer")]);
    assert_eq!(
        answer.part_tags().unwrap().parts,
        vec![Some(finalize_tags.parts[0])]
    );
}

/// Kill at every record boundary of a tagged tool loop with a steer, resume, and check: every
/// request the resumed run sends equals its rebuild from the resumed WAL (a resumed pending
/// turn included), and its first request repeats, byte for byte, the rendered messages that
/// were durable at the cut as the uninterrupted run sent them.
#[tokio::test]
async fn a_crash_cut_at_every_record_boundary_resumes_with_the_same_rendered_prefix() {
    for family in FAMILIES {
        sweep(family).await;
    }
}

async fn sweep(family: Option<&'static str>) {
    let reference_store = FileWalStore::new(scratch(&label("sweep-reference", family)));
    let name = "sweep";
    let reference = run(
        &reference_store,
        request(name, None),
        tool_loop_with_steer(),
        Some("also check the docs"),
        family,
    )
    .await;
    assert_eq!(reference.reason, RunFinishReason::Completed);
    assert_live_matches_rebuilt(&label("reference", family), &reference, 0);
    let by_step: std::collections::HashMap<u64, &CallOptions> = reference
        .calls
        .iter()
        .map(|opts| (opts.step_context.as_ref().unwrap().step_index, opts))
        .collect();

    let mut resumed_with_calls = 0;
    let mut resumed_pending_turns = 0;
    for cut in 1..reference.records.len() {
        let label = label(&format!("cut {cut}"), family);
        let store = FileWalStore::new(scratch(&label));
        let session = request(name, None).session;
        {
            let mut wal = store.open(&session).unwrap();
            for frame in &reference.records[..cut] {
                wal.append(frame.fence, &frame.record, true).unwrap();
            }
        }
        let prefix: Vec<WalRecord> = reference.records[..cut]
            .iter()
            .map(|frame| frame.record.clone())
            .collect();
        let durable = broca_core::replay(&prefix).unwrap();
        if matches!(
            durable.resolution,
            broca_core::ReplayResolution::Resume { pending: Some(_) }
        ) {
            resumed_pending_turns += 1;
        }
        let resumed = run(
            &store,
            request(name, None),
            tool_loop_with_steer(),
            None,
            family,
        )
        .await;
        assert_tags_increase(&label, &resumed.records);
        assert_live_matches_rebuilt(&label, &resumed, cut);
        let Some(first) = resumed.calls.first() else {
            continue;
        };
        resumed_with_calls += 1;
        let step = first.step_context.as_ref().unwrap().step_index;
        let sent = by_step
            .get(&step)
            .unwrap_or_else(|| panic!("{label}: the reference sent step {step}"));
        let committed = durable.prompt.len();
        let rendered = |opts: &CallOptions| -> Vec<String> {
            opts.prompt[..committed]
                .iter()
                .map(|message| serde_json::to_string(message).unwrap())
                .collect()
        };
        assert_eq!(
            rendered(first),
            rendered(sent),
            "{label}: the durable prefix renders as the uninterrupted run sent it"
        );
    }
    assert!(
        resumed_with_calls > 3,
        "the sweep resumed runs that called the model"
    );
    assert!(
        resumed_pending_turns > 0,
        "the sweep resumed a pending tool turn"
    );
}
