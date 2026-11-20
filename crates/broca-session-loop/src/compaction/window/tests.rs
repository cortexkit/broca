use std::sync::Arc;

use async_trait::async_trait;
use broca_core::provider::StubProvider;
use broca_core::stub_plane::StubToolPlane;
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason, ToolCall};
use broca_protocol::{
    BindIdentity, MessageId, NormalizedEvent, ProviderControl, RunId, SubmissionRequest,
    ToolCallId, Usage,
};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::Session;
use serde_json::{json, Value};

use super::{CHECKED, PROJECTED};
use crate::compaction::prompt::TAKEN;
use crate::compaction::RENDERED;
use crate::parts::MemoryParts;
use crate::run::Seams;
use crate::writer::Writer;

fn bind() -> BindIdentity {
    BindIdentity::new("/project", "window", "s1")
}

/// A compaction provider that answers Setup ready, takes every notice and never hands over a
/// view.
struct Quiet;

#[async_trait]
impl broca_core::CompactionPlane for Quiet {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::CompactionUnavailable,
    > {
        panic!("a session on the current compaction contract never calls the first protocol")
    }

    async fn call_v2(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        method: &str,
        _: Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        let answer = match method {
            "compaction.setup" => json!({"answer": "ready"}),
            "compaction.notice" => json!({"answer": "received"}),
            "compaction.ride" => json!({"answer": "nothing"}),
            other => panic!("unexpected compaction method {other}"),
        };
        Ok(serde_json::value::to_raw_value(&answer).unwrap())
    }
}

fn manifest() -> Arc<broca_wal::PlanManifest> {
    Arc::new(broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(
            json!({"compaction_item": {"provider": "compactor", "preset": "head", "params": {}}})
                .to_string(),
        )
        .unwrap(),
        plan_identity: "plan-1".into(),
        composition_digest: "composition".into(),
        tools: Arc::new(vec![serde_json::from_value(json!({
            "name": "read",
            "description": "Reads a file.",
            "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}},
        }))
        .unwrap()]),
        system_text: "You are a careful agent.".into(),
        system_digest: "system".into(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    })
}

fn tool_step(call: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(format!("m-{call}")),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    provider_state: None,
                    tool_call_id: ToolCallId(call.into()),
                    tool_name: "read".into(),
                    input: json!({"path": format!("src/{call}.rs")}).into(),
                })],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(1_000),
                output_tokens: Some(20),
                ..Usage::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

fn answer_step(id: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::Text {
                    text: "Done.".into(),
                }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(1_000),
                output_tokens: Some(10),
                ..Usage::default()
            },
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

/// Runs `run_id` on `session` under a plan naming a compaction provider, the model answering
/// `steps` in turn, and answers the requests the model was sent.
async fn compacted_run(
    session: &mut Session<&MemoryBackend>,
    parts: &mut MemoryParts,
    run_id: &str,
    steps: Vec<Vec<NormalizedEvent>>,
) -> Vec<broca_protocol::request::CallOptions> {
    let provider = StubProvider::new(steps);
    let request: SubmissionRequest = serde_json::from_value(json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [{"role": "user", "content": [{"type": "text", "text": format!("task {run_id}")}]}],
        "stop_when": [{"type": "step_count", "n": 50}],
        "context_limit": 1_000_000,
    }))
    .unwrap();
    let mut request = broca_core::RunRequest::from_submission(
        request,
        RunId(run_id.into()),
        bind(),
        broca_protocol::SubmissionId(String::new()),
    );
    let control = broca_core::RunControl {
        cancel: Some(Default::default()),
        manifest: Some(manifest()),
        admitted_context_limit: Some(1_000_000),
        new_log_compaction_plan_version: Some(broca_core::compaction_v2::PLAN_VERSION),
        ..Default::default()
    };
    let now = || 1_000;
    let admission = crate::admit::admission(
        &mut request,
        &control,
        &provider,
        Writer::operations(&*session),
        Some(now()),
    )
    .unwrap();
    let tools = StubToolPlane::new().with_canned_text("read", "fn main() {}");
    let seams = Seams {
        person_steers: None,
        provider: &provider,
        tools: &tools,
        bind: bind(),
        out: None,
        cancel: Default::default(),
        stop: Default::default(),
        held: None,
        approval: &broca_core::capability::AllowAll,
        hook_plane: None,
        now_ms: &now,
        session_address: None,
        compaction_plane: Some(&Quiet),
        compaction_session: None,
        compaction_changes: true,
        keep_warm: None,
        account_reports: None,
        max_same_account_wait: Default::default(),
        auth_selection: None,
        step_progress: None,
        model_call_ready: None,
        pause_for_auth_at_start: None,
        account_continue: None,
    };
    crate::run::run(session, parts, admission, &seams)
        .await
        .unwrap();
    provider.calls()
}

/// What a measured run's steps did, as the tests count it.
struct Counts {
    /// Window entries projected through the kept transcript.
    projected: usize,
    /// Kept entries the transcript's reads went back over.
    checked: usize,
    /// Window messages copied into a stored prompt.
    taken: usize,
    /// Entries the requests' renders rendered rather than reused.
    rendered: usize,
}

/// What a run of `tool_steps` tool steps and an answer does (see [`Counts`]) on a session
/// holding `history` answered runs.
async fn by_a_run(history: usize, tool_steps: usize) -> Counts {
    let backend = MemoryBackend::default();
    let mut session = Session::create(
        &backend,
        SessionAddr("0123456789abcdef".into()),
        SessionTriple {
            project_root: "/project".into(),
            harness: "window".into(),
            session: "s1".into(),
        },
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    let mut parts = MemoryParts::default();
    for index in 0..history {
        compacted_run(
            &mut session,
            &mut parts,
            &format!("h{index}"),
            vec![answer_step(&format!("a{index}"))],
        )
        .await;
    }
    let mut steps: Vec<_> = (0..tool_steps)
        .map(|n| tool_step(&format!("c{n}")))
        .collect();
    steps.push(answer_step("last"));
    let projected = PROJECTED.with(std::cell::Cell::get);
    let checked = CHECKED.with(std::cell::Cell::get);
    let taken = TAKEN.with(std::cell::Cell::get);
    let rendered = RENDERED.with(std::cell::Cell::get);
    compacted_run(&mut session, &mut parts, "measured", steps).await;
    Counts {
        projected: PROJECTED.with(std::cell::Cell::get) - projected,
        checked: CHECKED.with(std::cell::Cell::get) - checked,
        taken: TAKEN.with(std::cell::Cell::get) - taken,
        rendered: RENDERED.with(std::cell::Cell::get) - rendered,
    }
}

/// A run's transcript reads project each window entry once, then only what each step adds: the
/// assistant message and its results, and the held assistant message while its calls run. A
/// step that projected the whole window at each of its four reads would cost the window's size
/// four times over.
#[tokio::test]
async fn a_step_projects_only_the_entries_it_added() {
    let short = by_a_run(6, 1).await.projected;
    let long = by_a_run(6, 9).await.projected;
    let per_step = (long - short) as f64 / 8.0;
    assert!(
        per_step <= 6.0,
        "each more step projected {per_step} entries ({short} for 2 steps, {long} for 10)"
    );
}

/// A read learns which kept entries changed from the conversation's record of changes, so what
/// a run's reads go back over doesn't grow with the window. Comparing every kept entry with the
/// window's at each read would go over the whole window four times a step.
#[tokio::test]
async fn a_read_goes_back_only_over_what_changed() {
    let small = by_a_run(6, 4).await.checked;
    let large = by_a_run(30, 4).await.checked;
    assert_eq!(
        large, small,
        "a run went back over {small} kept entries behind 6 runs, {large} behind 30"
    );
}

/// A compacted run keeps its stored prompt between steps, so each more step copies in only the
/// messages it added, however long the window: copying the window into every step's request
/// would cost the window's size per step.
#[tokio::test]
async fn a_step_takes_into_its_stored_prompt_only_the_messages_it_added() {
    let extra = |short: Counts, long: Counts| long.taken - short.taken;
    let small = extra(by_a_run(6, 1).await, by_a_run(6, 9).await);
    let large = extra(by_a_run(30, 1).await, by_a_run(30, 9).await);
    assert_eq!(
        large, small,
        "8 more steps copied {small} more messages behind 6 runs, {large} behind 30"
    );
}

/// A compacted run's requests render only the entries that changed since its last request,
/// and reuse what they rendered of the rest, so what each more step renders doesn't grow with
/// the window.
#[tokio::test]
async fn a_request_renders_only_the_entries_that_changed() {
    let extra = |short: Counts, long: Counts| long.rendered - short.rendered;
    let small = extra(by_a_run(6, 1).await, by_a_run(6, 9).await);
    let large = extra(by_a_run(30, 1).await, by_a_run(30, 9).await);
    assert_eq!(
        large, small,
        "8 more steps rendered {small} more entries behind 6 runs, {large} behind 30"
    );
}

/// A compacted run's requests share one episode list rather than each copying the run's.
#[tokio::test]
async fn a_runs_requests_share_one_episode_list() {
    let backend = MemoryBackend::default();
    let mut session = Session::create(
        &backend,
        SessionAddr("0123456789abcdef".into()),
        SessionTriple {
            project_root: "/project".into(),
            harness: "window".into(),
            session: "s1".into(),
        },
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    let mut parts = MemoryParts::default();
    compacted_run(&mut session, &mut parts, "h0", vec![answer_step("a0")]).await;
    let mut steps: Vec<_> = (0..4).map(|n| tool_step(&format!("c{n}"))).collect();
    steps.push(answer_step("last"));
    let calls = compacted_run(&mut session, &mut parts, "measured", steps).await;
    assert_eq!(calls.len(), 5);
    assert!(calls[0].render_episodes.raw_ordinals);
    for (step, call) in calls.iter().enumerate() {
        assert!(
            Arc::ptr_eq(&call.render_episodes, &calls[0].render_episodes),
            "step {} carried a copy of the episode list",
            step + 1
        );
    }
}

/// The memory report's estimate of a run's kept transcript is within 15% of what the allocator
/// measured for it, built over a synthetic session shaped like production ones. Its weak handle
/// keeps nothing: once the run drops the transcript, the handle finds nothing, and the drop
/// freed the entries although the handle still exists.
#[test]
fn a_kept_transcripts_estimate_matches_the_allocator_and_its_handle_keeps_nothing() {
    use broca_session_store::synthetic::{write, Shape};
    use broca_test_support::alloc::{installed, measure};

    assert!(
        installed(),
        "the crate's tests count with the counting allocator"
    );
    let backend = MemoryBackend::default();
    let session = futures::executor::block_on(async {
        let mut session = Session::create(
            &backend,
            SessionAddr("0123456789abcdef".into()),
            SessionTriple {
                project_root: "/project".into(),
                harness: "window".into(),
                session: "s1".into(),
            },
            &DeploymentKey::new(1, &[7; 32]),
            false,
        )
        .await
        .unwrap();
        write(&mut session, &Shape::production(1_000))
            .await
            .unwrap();
        session
    });
    let (window, built) = measure(|| {
        let window = super::WindowTranscript::default();
        window
            .update(session.session_id(), &session.conversation, None, |_| None)
            .unwrap();
        window
    });
    let handle = window.downgrade();
    let estimated = handle.held_bytes().expect("the run holds its transcript");
    let measured = built.held.max(0) as usize;
    eprintln!("a kept transcript: estimated {estimated}, measured {measured}");
    assert!(
        estimated.abs_diff(measured) * 100 <= measured * 15,
        "a kept transcript: estimated {estimated} bytes, measured {measured}, off by more than 15%"
    );
    let ((), dropped) = measure(|| drop(window));
    assert!(
        handle.held_bytes().is_none(),
        "a dropped transcript is found"
    );
    let freed = dropped.held.unsigned_abs();
    assert!(
        freed * 100 >= measured * 95,
        "dropping the transcript freed {freed} of its {measured} bytes while a handle remained"
    );
}
