//! AuthRequired pause → continue conformance (Phase 2, commit-3).
//!
//! A mid-run Auth-class provider error pauses the run (a durable, non-terminal
//! `RunPaused`) instead of finishing it. The run is then:
//!  - resumed WITHOUT the auth-resolved signal → stays paused (the durable RunPaused is
//!    preserved, the run is still resumable);
//!  - resumed WITH the auth-resolved signal → CONTINUES the SAME run_id from the last
//!    durable boundary to a clean terminal, sending byte-identical request bytes to a
//!    no-pause reference run (the pause is transparent to what the model finally sees).

use broca_core::{
    replay, run_loop, run_loop_resume_after_auth, AllowAll, FileLeaseStore, FileWalStore,
    OutputEvent, ReplayResolution, RunRequest, StubProvider, StubReply, StubToolPlane, WalStore,
};
use broca_protocol::{
    caller::{ControlUnit, PauseReason, RunFinishReason},
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput},
    error::{ErrorClass, ProviderError},
    provider::{NormalizedEvent, ProviderControl},
    request::{ExecutionMode, GenerationConfig, ModelHandle, StopCondition, Tool, ToolChoice},
    BindIdentity, FinishReason, MessageId, RunId, StepId, ToolCallId, Usage,
};
use broca_wal::{FileWal, RunConfig, Wal, WalRecord};
use serde_json::json;
use tokio::sync::mpsc;

fn req(session: &BindIdentity, prompt: &str) -> RunRequest {
    RunRequest {
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
        run_id: RunId("r1".into()),
        session: session.clone(),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: prompt.into(),
            }],
        }],
        tools: vec![],
        policy_allowed: Default::default(),
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
        structured_output: None,
    }
}

fn text_reply(id: &str, text: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

fn tool_reply(message_id: &str, tool_call_id: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(message_id.into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId(tool_call_id.into()),
                    tool_name: "edit".into(),
                    input: json!({}),
                })],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

fn edit_tool() -> Tool {
    Tool {
        name: "edit".into(),
        description: "edit".into(),
        input_schema: json!({"type": "object"}),
        module: None,
        execution_mode: ExecutionMode::Parallel,
    }
}

fn tool_req(run_id: &str, session: &BindIdentity, prompt: &str) -> RunRequest {
    RunRequest {
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
        run_id: RunId(run_id.into()),
        session: session.clone(),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: prompt.into(),
            }],
        }],
        tools: vec![edit_tool()],
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
        structured_output: None,
    }
}

fn seed_resume_without_terminal(
    wal: &FileWalStore,
    session: &BindIdentity,
    auth_selection: Option<broca_core::AuthSelection>,
) {
    let mut seeded = FileWal::open(wal.wal_path(session)).expect("open wal");
    let recs = vec![
        WalRecord::RunStarted {
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("r-orig".into()),
            session: session.clone(),
            ts_ms: None,
            config: Box::new(RunConfig {
                model: ModelHandle::new("stub", "m"),
                tools: vec![edit_tool()].into(),
                tool_choice: ToolChoice::Auto,
                generation: GenerationConfig::default(),
                response_format: Default::default(),
                provider_options: serde_json::Value::Null,
                cache_tiers: serde_json::Value::Null,
                context_epoch: 0,
                renderer_version: 1,
                frozen_render_config: json!({"render_config": "stateless"}),
                auth_selection,
                tools_were_omitted: false,
                budget: None,
                context_limit: None,
                work_class: None,
                variant: None,
                service_tier: None,
                stop_when: Vec::new(),
                context_management: None,
            }),
            input: vec![Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: "hello".into(),
                }],
            }]
            .into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
        WalRecord::ModelStepFinished {
            hooks: None,
            step_id: StepId(1),
            assistant_message: AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("t1".into()),
                    tool_name: "edit".into(),
                    input: json!({}),
                })],
            }
            .into(),
            usage: Usage::default(),
            finish_reason: Some(FinishReason::ToolCalls),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        },
    ];
    for rec in recs {
        seeded.append(1, &rec, true).expect("append seeded record");
    }
}

fn auth_error_step() -> Vec<NormalizedEvent> {
    vec![NormalizedEvent::Control(ProviderControl::Error {
        error: ProviderError {
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Auth,
            message: "credential expired".into(),
            status: Some(401),
            retry_after_secs: None,
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        },
        retries_used: 0,
    })]
}

async fn drive_paused(
    req: RunRequest,
    provider: &StubProvider,
    plane: &StubToolPlane,
    lease: &FileLeaseStore,
    wal: &FileWalStore,
) -> broca_core::RunSummary {
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let s = run_loop(req, provider, plane, lease, &AllowAll, wal, &tx)
        .await
        .unwrap();
    drop(tx);
    collector.await.unwrap();
    s
}

async fn drive_with_events(
    req: RunRequest,
    provider: &StubProvider,
    plane: &StubToolPlane,
    lease: &FileLeaseStore,
    wal: &FileWalStore,
) -> (broca_core::RunSummary, Vec<OutputEvent>) {
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(ev) = rx.recv().await {
            events.push(ev);
        }
        events
    });
    let summary = run_loop(req, provider, plane, lease, &AllowAll, wal, &tx)
        .await
        .unwrap();
    drop(tx);
    let events = collector.await.unwrap();
    (summary, events)
}

fn control_units(events: &[OutputEvent]) -> Vec<&ControlUnit> {
    events
        .iter()
        .filter_map(|ev| match ev {
            OutputEvent::Control(unit) => Some(unit),
            _ => None,
        })
        .collect()
}

fn records_of(wal: &FileWalStore, session: &BindIdentity) -> Vec<WalRecord> {
    WalStore::read_records(wal, session)
        .unwrap()
        .into_iter()
        .map(|f| f.record)
        .collect()
}

/// A resumed run keeps the ORIGINAL durable run_id on both its durable pause record and
/// its caller-visible control units, even when the crash-recovery send arrived with a fresh
/// request id.
#[tokio::test]
async fn resumed_auth_pause_keeps_original_run_id_everywhere() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-resume-pause-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let plane = StubToolPlane::new().with_tool("edit", |_| {
        StubReply::Output(ToolOutput::Text { text: "ran".into() })
    });
    seed_resume_without_terminal(&wal, &session, None);

    let provider = StubProvider::new(vec![auth_error_step()]);
    let (summary, events) = drive_with_events(
        tool_req("r-fresh", &session, "hello"),
        &provider,
        &plane,
        &lease,
        &wal,
    )
    .await;
    assert_eq!(
        summary.reason,
        RunFinishReason::Interrupted,
        "a resumed auth failure still surfaces as a resumable pause"
    );

    let controls = control_units(&events);
    assert!(
        controls.iter().any(|unit| matches!(
            unit,
            ControlUnit::RunStarted {
                run_id,
                submission_id: _,
            } if run_id == &RunId("r-orig".into())
        )),
        "the resumed run_started unit keeps the original durable run_id"
    );
    assert!(
        controls.iter().any(|unit| matches!(
            unit,
            ControlUnit::Paused {
                run_id,
                reason: PauseReason::AuthRequired,
                ..
            } if run_id == &RunId("r-orig".into())
        )),
        "the resumed pause unit must keep the original durable run_id"
    );

    let recs = records_of(&wal, &session);
    assert!(
        recs.iter().any(|rec| matches!(
            rec,
            WalRecord::RunPaused {
                run_id,
                reason: PauseReason::AuthRequired,
                ..
            } if run_id == &RunId("r-orig".into())
        )),
        "the durable RunPaused record must keep the original durable run_id"
    );
    let replayed = replay(&recs).expect("replay");
    assert!(
        matches!(
            replayed.resolution,
            ReplayResolution::AuthRequired {
                run_id,
                reason: PauseReason::AuthRequired,
                ..
            } if run_id == RunId("r-orig".into())
        ),
        "replay still resolves the resumed pause under the original run_id"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A mid-run auth error writes a durable RunPaused (NOT RunFinished), and replay
/// classifies the run as AuthRequired (resumable), not finished.
#[tokio::test]
async fn auth_error_pauses_the_run() {
    let dir =
        broca_test_support::scratch_root().join(format!("broca-pause-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));

    // The model step fails with an Auth error → pause.
    let provider = StubProvider::new(vec![auth_error_step()]);
    let plane = StubToolPlane::new();
    let s = drive_paused(req(&session, "hello"), &provider, &plane, &lease, &wal).await;
    assert_eq!(
        s.reason,
        RunFinishReason::Interrupted,
        "a pause surfaces as Interrupted"
    );

    let recs = records_of(&wal, &session);
    assert!(
        recs.iter()
            .any(|r| matches!(r, WalRecord::RunPaused { .. })),
        "a durable RunPaused was written"
    );
    assert!(
        !recs
            .iter()
            .any(|r| matches!(r, WalRecord::RunFinished { .. })),
        "NO RunFinished — a pause is not a terminal"
    );
    let st = replay(&recs).expect("replay");
    assert!(
        matches!(st.resolution, ReplayResolution::AuthRequired { .. }),
        "replay classifies the paused run as AuthRequired (resumable)"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A provider auth failure on the step after a tool call pauses only after the completed tool
/// batch is durable, so the producer never creates a pause with an open dispatch intent.
#[tokio::test]
async fn auth_pause_after_tool_dispatch_closes_the_batch_first() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-pause-after-tool-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let provider = StubProvider::new(vec![tool_reply("m1", "t1"), auth_error_step()]);
    let plane = StubToolPlane::new().with_tool("edit", |_| {
        StubReply::Output(ToolOutput::Text { text: "ran".into() })
    });

    drive_paused(
        tool_req("r1", &session, "hello"),
        &provider,
        &plane,
        &lease,
        &wal,
    )
    .await;

    let recs = records_of(&wal, &session);
    let batch_started = recs
        .iter()
        .position(|record| matches!(record, WalRecord::ToolBatchStarted { .. }))
        .expect("the first model step opens a tool batch");
    let intent = recs
        .iter()
        .position(|record| matches!(record, WalRecord::ToolDispatchIntent { .. }))
        .expect("the tool dispatch records its intent");
    let result = recs
        .iter()
        .position(|record| matches!(record, WalRecord::ToolResult { .. }))
        .expect("the tool dispatch records its result");
    let batch_finished = recs
        .iter()
        .position(|record| matches!(record, WalRecord::ToolBatchFinished { .. }))
        .expect("the tool batch closes");
    let paused = recs
        .iter()
        .position(|record| matches!(record, WalRecord::RunPaused { .. }))
        .expect("the next provider call pauses for auth");
    assert!(
        batch_started < intent
            && intent < result
            && result < batch_finished
            && batch_finished < paused,
        "the producer must durably finish the prior batch before writing RunPaused"
    );
    assert!(
        matches!(
            replay(&recs).expect("replay").resolution,
            ReplayResolution::AuthRequired { .. }
        ),
        "the producer-generated pause has no indeterminate dispatch"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Resuming a paused run WITHOUT the auth-resolved signal keeps it paused (the durable
/// RunPaused is preserved; the run is still resumable).
#[tokio::test]
async fn resume_without_signal_stays_paused() {
    let dir =
        broca_test_support::scratch_root().join(format!("broca-pause-stay-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));

    let p1 = StubProvider::new(vec![auth_error_step()]);
    let plane = StubToolPlane::new();
    drive_paused(req(&session, "hello"), &p1, &plane, &lease, &wal).await;

    // Resume WITHOUT the signal (a plain run_loop) → stays paused (still Interrupted),
    // and the WAL still has no RunFinished.
    let p2 = StubProvider::new(vec![text_reply("m2", "should not run")]);
    let s = drive_paused(req(&session, "hello"), &p2, &plane, &lease, &wal).await;
    assert_eq!(
        s.reason,
        RunFinishReason::Interrupted,
        "still paused without the signal"
    );
    let recs = records_of(&wal, &session);
    assert!(
        !recs
            .iter()
            .any(|r| matches!(r, WalRecord::RunFinished { .. })),
        "still no RunFinished — the run is still paused, not finished"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Resuming WITH the auth-resolved signal continues the SAME run_id to a clean terminal,
/// and the resumed run renders byte-identical request bytes to a no-pause reference run
/// (the pause is transparent to what the model finally sees).
#[tokio::test]
async fn resume_with_signal_continues_same_run_byte_identical() {
    // 1. The reference: a clean run that never paused — its step-2 sent bytes are the
    //    target. The model answers in one step.
    let ref_dir =
        broca_test_support::scratch_root().join(format!("broca-pause-ref-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&ref_dir);
    std::fs::create_dir_all(ref_dir.join("proj")).unwrap();
    let ref_session = BindIdentity::new(ref_dir.join("proj"), "h", "s");
    let ref_lease = FileLeaseStore::new(ref_dir.join("lease"));
    let ref_wal = FileWalStore::new(ref_dir.join("wal"));
    let ref_provider = CapturingProvider::new(vec![text_reply("m1", "the answer")]);
    let plane = StubToolPlane::new();
    drive_capturing(
        req(&ref_session, "hello"),
        &ref_provider,
        &plane,
        &ref_lease,
        &ref_wal,
        false,
    )
    .await;
    let reference_bytes = ref_provider.sent_bytes();

    // 2. The paused run: step 1 fails with auth → pause. Then resume WITH the signal →
    //    the provider answers and the run completes. Capture the resumed run's sent bytes.
    let dir =
        broca_test_support::scratch_root().join(format!("broca-pause-cont-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));

    let p1 = StubProvider::new(vec![auth_error_step()]);
    drive_paused(req(&session, "hello"), &p1, &plane, &lease, &wal).await;

    let resumed = CapturingProvider::new(vec![text_reply("m1", "the answer")]);
    let s = drive_capturing(req(&session, "hello"), &resumed, &plane, &lease, &wal, true).await;
    assert_eq!(
        s.reason,
        RunFinishReason::Completed,
        "the resumed-after-auth run completes cleanly"
    );

    // The run is finished now (the continue wrote RunFinished under the SAME run_id).
    let recs = records_of(&wal, &session);
    let run_ids: Vec<String> = recs
        .iter()
        .filter_map(|r| match r {
            WalRecord::RunStarted { run_id, .. } => Some(run_id.0.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        run_ids,
        vec!["r1"],
        "ONE episode, the SAME run_id (no new RunStarted)"
    );
    assert!(
        recs.iter()
            .any(|r| matches!(r, WalRecord::RunFinished { .. })),
        "the continued run reached a durable terminal"
    );

    // Byte-identical: the resumed run sent the same first request bytes the no-pause
    // reference did (the pause didn't change what the model sees).
    assert_eq!(
        resumed.sent_bytes(),
        reference_bytes,
        "the resumed-after-auth run renders byte-identical to the no-pause reference"
    );
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&ref_dir);
}

/// A fresh run started with `RunControl.auth_selection` set must PERSIST that credential id
/// on the durable `RunStarted.RunConfig.auth_selection` — AND it must be metadata-only (NOT
/// a render input): the request bytes the provider sends are byte-identical to a run WITHOUT
/// the selection set. (auth_selection never enters the rendered bytes; a resume re-reads it
/// from this durable record to re-fetch the same credential.)
#[tokio::test]
async fn auth_selection_is_frozen_on_run_config_and_not_a_render_input() {
    use broca_core::{run_loop_with_control, AuthSelection, RunControl};

    // Reference: a no-selection run; capture its sent bytes.
    let ref_dir =
        broca_test_support::scratch_root().join(format!("broca-as-ref-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&ref_dir);
    std::fs::create_dir_all(ref_dir.join("proj")).unwrap();
    let ref_session = BindIdentity::new(ref_dir.join("proj"), "h", "s");
    let ref_lease = FileLeaseStore::new(ref_dir.join("lease"));
    let ref_wal = FileWalStore::new(ref_dir.join("wal"));
    let ref_provider = CapturingProvider::new(vec![text_reply("m1", "answer")]);
    let plane = StubToolPlane::new();
    drive_capturing(
        req(&ref_session, "hello"),
        &ref_provider,
        &plane,
        &ref_lease,
        &ref_wal,
        false,
    )
    .await;

    // The selection run: same everything, but auth_selection set.
    let dir = broca_test_support::scratch_root().join(format!("broca-as-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let provider = CapturingProvider::new(vec![text_reply("m1", "answer")]);
    let control = RunControl {
        auth_selection: Some(AuthSelection {
            credential_id: "oauth:anthropic".into(),
            account_ref: None,
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        }),
        ..Default::default()
    };
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    run_loop_with_control(
        req(&session, "hello"),
        &provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        control,
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();

    // The durable RunConfig froze the credential id.
    let recs = records_of(&wal, &session);
    let frozen = recs
        .iter()
        .find_map(|r| match r {
            WalRecord::RunStarted { config, .. } => Some(config.auth_selection.clone()),
            _ => None,
        })
        .expect("a RunStarted was written");
    assert_eq!(
        frozen.map(|s| s.credential_id),
        Some("oauth:anthropic".to_string()),
        "auth_selection must be frozen on the durable RunConfig"
    );

    // Metadata-only: the sent bytes are identical with and without the selection set.
    assert_eq!(
        provider.sent_bytes(),
        ref_provider.sent_bytes(),
        "auth_selection must NOT change the rendered request bytes (not a render input)"
    );

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&ref_dir);
}

/// A fresh run started with a vault `account_ref` on `RunControl.auth_selection` must persist it
/// verbatim on the durable `RunStarted.RunConfig.auth_selection` AND stamp it onto the emitted
/// usage segment — end-to-end through the loop's terminal export derivation.
#[tokio::test]
async fn account_ref_freezes_on_run_config_and_stamps_the_export_segment() {
    use broca_core::{
        compute_segments, run_loop_with_control, AuthSelection, ChargeBasis, RunControl,
    };

    let dir =
        broca_test_support::scratch_root().join(format!("broca-acctref-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let provider = CapturingProvider::new(vec![text_reply("m1", "answer")]);
    let plane = StubToolPlane::new();
    let control = RunControl {
        auth_selection: Some(AuthSelection {
            credential_id: "chatgpt:openai".into(),
            account_ref: Some("acct-uuid-777".into()),
            resolved_credential_id: Some("chatgpt:openai:actual-secret".into()),
            vault_request: Some(broca_core::VaultRequest {
                min_ttl_ms: Some(600_000),
                force_refresh: Some(false),
                op: Some(broca_core::VaultReadOp::Get),
            }),
            account_decision: None,
        }),
        ..Default::default()
    };
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    run_loop_with_control(
        req(&session, "hello"),
        &provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        control,
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();

    // The durable RunConfig froze the vault account_id verbatim.
    let framed = WalStore::read_records(&wal, &session).expect("read WAL frames");
    let frozen = framed
        .iter()
        .find_map(|frame| match &frame.record {
            WalRecord::RunStarted { config, .. } => config.auth_selection.clone(),
            _ => None,
        })
        .expect("a RunStarted was written");
    assert_eq!(frozen.credential_id, "chatgpt:openai");
    assert_eq!(
        frozen.resolved_credential_id.as_deref(),
        Some("chatgpt:openai:actual-secret")
    );
    assert_eq!(
        frozen.vault_request,
        Some(broca_core::VaultRequest {
            min_ttl_ms: Some(600_000),
            force_refresh: Some(false),
            op: Some(broca_core::VaultReadOp::Get),
        })
    );
    assert_eq!(
        frozen.account_ref.as_deref(),
        Some("acct-uuid-777"),
        "the vault account_id is frozen verbatim on the durable RunConfig"
    );

    // The emitted usage segment carries the frozen account_ref verbatim.
    let resolver = |_: &str, _: &str| (ChargeBasis::Unknown, 1);
    let segments = compute_segments(&framed, "instance-test", &resolver);
    assert_eq!(segments.len(), 1);
    assert_eq!(
        segments[0].auth_selection.as_deref(),
        Some("chatgpt:openai")
    );
    assert_eq!(
        segments[0].resolved_credential_id.as_deref(),
        Some("chatgpt:openai:actual-secret")
    );
    let exported = serde_json::to_value(&segments[0]).unwrap();
    assert_eq!(exported["auth_selection"], "chatgpt:openai");
    assert_eq!(
        exported["resolved_credential_id"],
        "chatgpt:openai:actual-secret"
    );
    // The export fact carries what the run asked the vault for, a false force_refresh
    // included, so it can be joined to the vault's record of that request.
    assert_eq!(
        exported["vault_request"],
        serde_json::json!({"min_ttl_ms": 600_000, "force_refresh": false, "op": "get"})
    );
    assert_eq!(
        segments[0].account_ref.as_deref(),
        Some("acct-uuid-777"),
        "the export segment is stamped with the frozen vault account_id"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Admit-then-pause: a FRESH run started with `pause_for_auth_at_start` (the caller knew,
/// before any provider call, that the credential needs re-auth — e.g. the vault returned
/// needs_reauth) must durably ADMIT (write RunStarted, so it's a real resumable run) then
/// immediately write RunPaused{AuthRequired} and make NO model call — and then continue
/// under the SAME run_id once re-auth is provided, byte-identical to a no-pause reference.
#[tokio::test]
async fn pause_for_auth_at_start_admits_then_pauses_then_continues() {
    use broca_core::{
        compute_segments, run_loop_resume_after_auth_with_control, run_loop_with_control,
        AuthSelection, ChargeBasis, RunControl,
    };

    // The reference: a clean no-pause run; its first sent bytes are the target.
    let ref_dir =
        broca_test_support::scratch_root().join(format!("broca-pa-ref-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&ref_dir);
    std::fs::create_dir_all(ref_dir.join("proj")).unwrap();
    let ref_session = BindIdentity::new(ref_dir.join("proj"), "h", "s");
    let ref_lease = FileLeaseStore::new(ref_dir.join("lease"));
    let ref_wal = FileWalStore::new(ref_dir.join("wal"));
    let ref_provider = CapturingProvider::new(vec![text_reply("m1", "the answer")]);
    let plane = StubToolPlane::new();
    drive_capturing(
        req(&ref_session, "hello"),
        &ref_provider,
        &plane,
        &ref_lease,
        &ref_wal,
        false,
    )
    .await;
    let reference_bytes = ref_provider.sent_bytes();

    // The pause-at-start run.
    let dir = broca_test_support::scratch_root().join(format!("broca-pa-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));

    // The provider would answer in one step — but it must NEVER be called (we pause first).
    let p1 = CapturingProvider::new(vec![text_reply("m1", "the answer")]);
    let control = RunControl {
        pause_for_auth_at_start: Some(ProviderError {
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Auth,
            message: "vault: refresh token dead".into(),
            status: None,
            retry_after_secs: None,
            provider_code: Some("needs_reauth".into()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        }),
        auth_selection: Some(AuthSelection {
            credential_id: "chatgpt:openai".into(),
            account_ref: None,
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        }),
        ..Default::default()
    };
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let s = run_loop_with_control(
        req(&session, "hello"),
        &p1,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        control,
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();

    assert_eq!(
        s.reason,
        RunFinishReason::Interrupted,
        "a pause surfaces as Interrupted"
    );
    assert!(
        p1.sent_bytes().is_empty(),
        "NO provider call was made — paused before the first step"
    );

    let recs = records_of(&wal, &session);
    assert!(
        recs.iter()
            .any(|r| matches!(r, WalRecord::RunStarted { .. })),
        "the run durably ADMITTED (RunStarted written) — it's a real resumable run"
    );
    assert!(
        recs.iter()
            .any(|r| matches!(r, WalRecord::RunPaused { .. })),
        "a durable RunPaused was written"
    );
    assert!(
        !recs.iter().any(|r| matches!(
            r,
            WalRecord::ModelStepFinished { .. } | WalRecord::RunFinished { .. }
        )),
        "no model step and no terminal — admitted then paused, nothing ran"
    );
    let st = replay(&recs).expect("replay");
    assert!(
        matches!(st.resolution, ReplayResolution::AuthRequired { .. }),
        "replay classifies the run as AuthRequired (resumable)"
    );

    // Continue after re-auth: same run_id, byte-identical to the no-pause reference.
    let resumed = CapturingProvider::new(vec![text_reply("m1", "the answer")]);
    let resolved_selection = AuthSelection {
        credential_id: "chatgpt:openai".into(),
        account_ref: Some("resolved-account".into()),
        resolved_credential_id: Some("chatgpt:openai:reauthenticated".into()),
        vault_request: None,
        account_decision: None,
    };
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let s2 = run_loop_resume_after_auth_with_control(
        req(&session, "hello"),
        &resumed,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        RunControl {
            auth_selection: Some(resolved_selection.clone()),
            ..Default::default()
        },
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();
    assert_eq!(
        s2.reason,
        RunFinishReason::Completed,
        "the resumed-after-auth run completes cleanly"
    );
    let framed = WalStore::read_records(&wal, &session).expect("read resumed WAL");
    let run_ids: Vec<String> = framed
        .iter()
        .filter_map(|frame| match &frame.record {
            WalRecord::RunStarted { run_id, .. } => Some(run_id.0.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(run_ids, vec!["r1"], "ONE episode, the SAME run_id");
    let auth_refreezes: Vec<_> = framed
        .iter()
        .filter_map(|frame| match &frame.record {
            WalRecord::RunAuthRefrozen { auth_selection, .. } => Some(auth_selection),
            _ => None,
        })
        .collect();
    assert_eq!(auth_refreezes, vec![&resolved_selection]);
    let replayed = replay(
        &framed
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
    )
    .expect("resumed WAL replays");
    assert_eq!(
        replayed.run_config.and_then(|config| config.auth_selection),
        Some(resolved_selection.clone())
    );
    let resolver = |_: &str, _: &str| (ChargeBasis::Unknown, 1);
    let segments = compute_segments(&framed, "instance-test", &resolver);
    let post_resume = segments.last().expect("resumed run exports spend");
    assert_eq!(post_resume.account_ref.as_deref(), Some("resolved-account"));
    assert_eq!(
        post_resume.resolved_credential_id.as_deref(),
        Some("chatgpt:openai:reauthenticated")
    );
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0].account_ref, None);
    assert_eq!(post_resume.usage, s2.usage);
    assert_eq!(
        resumed.sent_bytes(),
        reference_bytes,
        "the continued run renders byte-identical to the no-pause reference"
    );
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&ref_dir);
}

#[tokio::test]
async fn auth_resume_with_the_same_selection_is_silent_and_keeps_one_segment() {
    use broca_core::{
        compute_segments, run_loop_resume_after_auth_with_control, run_loop_with_control,
        AuthSelection, ChargeBasis, RunControl,
    };

    let dir =
        broca_test_support::scratch_root().join(format!("broca-auth-same-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let plane = StubToolPlane::new();
    let selection = AuthSelection {
        credential_id: "chatgpt:openai".into(),
        account_ref: Some("same-account".into()),
        resolved_credential_id: None,
        vault_request: None,
        account_decision: None,
    };

    let paused_provider = CapturingProvider::new(vec![text_reply("unused", "unused")]);
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    run_loop_with_control(
        req(&session, "hello"),
        &paused_provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        RunControl {
            pause_for_auth_at_start: Some(ProviderError {
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::Auth,
                message: "credential needs re-authentication".into(),
                status: None,
                retry_after_secs: None,
                provider_code: Some("needs_reauth".into()),
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
            }),
            auth_selection: Some(selection.clone()),
            ..Default::default()
        },
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();

    let resumed_provider = CapturingProvider::new(vec![text_reply("m1", "answer")]);
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop_resume_after_auth_with_control(
        req(&session, "hello"),
        &resumed_provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        RunControl {
            auth_selection: Some(selection.clone()),
            ..Default::default()
        },
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();

    let framed = WalStore::read_records(&wal, &session).expect("read resumed WAL");
    assert!(!framed
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::RunAuthRefrozen { .. })));
    let resolver = |_: &str, _: &str| (ChargeBasis::Unknown, 1);
    let segments = compute_segments(&framed, "instance-test", &resolver);
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].account_ref.as_deref(), Some("same-account"));
    assert_eq!(segments[0].usage, summary.usage);

    let _ = std::fs::remove_dir_all(&dir);
}

/// A pre-auth pause may persist an incomplete credential-bound render config. Once auth
/// supplies the project, resume must replace that config before the first provider call.
#[tokio::test]
async fn pause_before_first_call_refreezes_project_on_auth_resume() {
    use broca_core::{read_prior_run_config, run_loop_with_control, RunControl};

    let dir =
        broca_test_support::scratch_root().join(format!("broca-pa-project-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let plane = StubToolPlane::new();

    let incomplete = ProjectCapturingProvider::new(None, vec![text_reply("unused", "unused")]);
    let control = RunControl {
        pause_for_auth_at_start: Some(ProviderError {
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Auth,
            message: "credential needs re-authentication".into(),
            status: None,
            retry_after_secs: None,
            provider_code: Some("needs_reauth".into()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        }),
        ..Default::default()
    };
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    run_loop_with_control(
        req(&session, "hello"),
        &incomplete,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        control,
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();
    assert!(incomplete.sent_bytes().is_empty());

    let resolved = ProjectCapturingProvider::new(
        Some("encouraging-env-qwp21"),
        vec![text_reply("m1", "the answer")],
    );
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop_resume_after_auth(
        req(&session, "hello"),
        &resolved,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();

    assert_eq!(summary.reason, RunFinishReason::Completed);
    let sent = resolved.sent_bytes();
    assert_eq!(sent.len(), 1, "resume must make the first provider call");
    let body: serde_json::Value = serde_json::from_slice(&sent[0]).unwrap();
    assert_eq!(body["project"], "encouraging-env-qwp21");
    assert!(records_of(&wal, &session)
        .iter()
        .any(|record| matches!(record, WalRecord::RunConfigRefrozen { .. })));
    let frozen = read_prior_run_config(&wal, &session).unwrap().unwrap();
    assert_eq!(
        frozen.frozen_render_config["project_id"],
        "encouraging-env-qwp21"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ───────────────────────── a provider that records its sent bytes ─────────────────

use async_trait::async_trait;
use broca_protocol::CallOptions;
use std::sync::Mutex;

/// Wraps a StubProvider's scripted replies but records the rendered request bytes of
/// each step, so a test can assert byte-identical sends across runs.
struct CapturingProvider {
    inner: StubProvider,
    sent: Mutex<Vec<Vec<u8>>>,
    renderer_version: u32,
}

impl CapturingProvider {
    fn new(scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            inner: StubProvider::new(scripts),
            sent: Mutex::new(Vec::new()),
            renderer_version: 1,
        }
    }

    fn with_renderer_version(mut self, renderer_version: u32) -> Self {
        self.renderer_version = renderer_version;
        self
    }

    fn sent_bytes(&self) -> Vec<Vec<u8>> {
        self.sent.lock().unwrap().clone()
    }
}

#[async_trait]
impl broca_core::provider::Provider for CapturingProvider {
    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.sent.lock().unwrap().push(self.render_request(opts));
        self.inner.step(step_id, opts).await
    }

    fn renderer_version(&self) -> u32 {
        self.renderer_version
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn auth_resume_refreezes_use_run_clock() {
    use broca_core::{
        compute_segments, run_loop_resume_after_auth_with_control, run_loop_with_control,
        AuthSelection, ChargeBasis, Clock, RunControl,
    };

    let dir = broca_test_support::scratch_root()
        .join(format!("broca-refreeze-clock-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("test-clock"), r#"{"now_ms":1234}"#).unwrap();
    let clock = Clock::for_state_root(&dir).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let plane = StubToolPlane::new();
    let initial = ProjectCapturingProvider::new(None, vec![]);
    let (tx, _rx) = mpsc::channel(256);
    let paused = run_loop_with_control(
        req(&session, "hello"),
        &initial,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        RunControl {
            clock: Some(clock.clone()),
            pause_for_auth_at_start: Some(ProviderError {
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::Auth,
                message: "credential unresolved".into(),
                status: None,
                retry_after_secs: None,
                provider_code: None,
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
            }),
            ..Default::default()
        },
        &tx,
    )
    .await
    .unwrap();
    assert_eq!(paused.paused, Some(PauseReason::AuthRequired));
    assert!(initial.sent_bytes().is_empty());

    std::fs::write(dir.join("test-clock"), r#"{"now_ms":5678}"#).unwrap();
    clock.refresh().unwrap();
    let resumed =
        ProjectCapturingProvider::new(Some("resolved-project"), vec![text_reply("m", "done")]);
    let summary = run_loop_resume_after_auth_with_control(
        req(&session, "hello"),
        &resumed,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        RunControl {
            clock: Some(clock),
            auth_selection: Some(AuthSelection {
                credential_id: "provider:key".into(),
                account_ref: Some("account".into()),
                resolved_credential_id: None,
                vault_request: None,
                account_decision: None,
            }),
            ..Default::default()
        },
        &tx,
    )
    .await
    .unwrap();
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(resumed.sent_bytes().len(), 1);
    let frames = WalStore::read_records(&wal, &session).unwrap();
    let config_times: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.record {
            WalRecord::RunConfigRefrozen { ts_ms, .. } => Some(*ts_ms),
            _ => None,
        })
        .collect();
    let auth_times: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.record {
            WalRecord::RunAuthRefrozen { ts_ms, .. } => Some(*ts_ms),
            _ => None,
        })
        .collect();
    assert_eq!(
        config_times,
        vec![Some(5678)],
        "render refreeze uses the run clock"
    );
    assert_eq!(
        auth_times,
        vec![Some(5678)],
        "auth refreeze uses the run clock"
    );
    let segments = compute_segments(&frames, "clock-test", &|_: &str, _: &str| {
        (ChargeBasis::Unknown, 1)
    });
    let freeze_times: Vec<_> = segments
        .iter()
        .map(|segment| segment.pricing_at_ms)
        .collect();
    assert_eq!(freeze_times, vec![Some(1234), Some(5678), Some(5678)]);
    std::fs::remove_dir_all(dir).unwrap();
}

struct ProjectCapturingProvider {
    inner: StubProvider,
    project_id: Option<String>,
    sent: Mutex<Vec<Vec<u8>>>,
}

impl ProjectCapturingProvider {
    fn new(project_id: Option<&str>, scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            inner: StubProvider::new(scripts),
            project_id: project_id.map(str::to_string),
            sent: Mutex::new(Vec::new()),
        }
    }

    fn sent_bytes(&self) -> Vec<Vec<u8>> {
        self.sent.lock().unwrap().clone()
    }
}

#[async_trait]
impl broca_core::provider::Provider for ProjectCapturingProvider {
    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.sent.lock().unwrap().push(self.render_request(opts));
        self.inner.step(step_id, opts).await
    }

    fn render_request(&self, _opts: &CallOptions) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "project": self.project_id,
        }))
        .unwrap()
    }

    fn renderer_version(&self) -> u32 {
        7
    }

    fn render_config_blob(&self) -> serde_json::Value {
        serde_json::json!({ "project_id": self.project_id })
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive_capturing(
    req: RunRequest,
    provider: &CapturingProvider,
    plane: &StubToolPlane,
    lease: &FileLeaseStore,
    wal: &FileWalStore,
    auth_resolved: bool,
) -> broca_core::RunSummary {
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let s = if auth_resolved {
        run_loop_resume_after_auth(req, provider, plane, lease, &AllowAll, wal, None, &tx).await
    } else {
        run_loop(req, provider, plane, lease, &AllowAll, wal, &tx).await
    }
    .unwrap();
    drop(tx);
    collector.await.unwrap();
    s
}

#[tokio::test]
async fn auth_resumed_renderer_drift_after_a_provider_call_is_durably_sealed() {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-auth-resume-render-drift-seal-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let plane = StubToolPlane::new();

    let initial = CapturingProvider::new(vec![auth_error_step()]);
    let paused = drive_capturing(
        req(&session, "hello"),
        &initial,
        &plane,
        &lease,
        &wal,
        false,
    )
    .await;
    assert_eq!(paused.paused, Some(PauseReason::AuthRequired));
    let paused_records = records_of(&wal, &session);
    assert!(paused_records
        .iter()
        .any(|record| matches!(record, WalRecord::StepStarted { .. })));
    assert!(!paused_records
        .iter()
        .any(|record| matches!(record, WalRecord::RunFinished { .. })));

    let drifted =
        CapturingProvider::new(vec![text_reply("must-not-run", "wrong")]).with_renderer_version(2);
    let (tx, mut rx) = mpsc::channel(64);
    let collector = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    });
    let summary = run_loop_resume_after_auth(
        req(&session, "hello"),
        &drifted,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    let events = collector.await.unwrap();

    assert_eq!(summary.reason, RunFinishReason::Interrupted);
    assert_eq!(summary.paused, None);
    assert!(drifted.sent_bytes().is_empty());
    let controls = control_units(&events);
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

    let sealed = records_of(&wal, &session);
    assert_eq!(
        sealed
            .iter()
            .filter(|record| matches!(record, WalRecord::StepStarted { .. }))
            .count(),
        1,
        "the resumed drift guard must run before another provider call"
    );
    assert!(sealed.iter().any(|record| matches!(
        record,
        WalRecord::RunFinished {
            reason: RunFinishReason::Interrupted,
            error: Some(error),
            ..
        } if error.provider_code.as_deref() == Some("renderer_version_drift")
    )));
    assert!(matches!(
        replay(&sealed)
            .expect("sealed auth-resume drift replays")
            .resolution,
        ReplayResolution::LineageIdle {
            last_reason: RunFinishReason::Interrupted
        }
    ));

    let _ = std::fs::remove_dir_all(&dir);
}

struct NullRenderConfigProvider {
    inner: StubProvider,
}

#[async_trait]
impl broca_core::provider::Provider for NullRenderConfigProvider {
    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.inner.step(step_id, opts).await
    }

    fn render_config_blob(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

#[tokio::test]
async fn null_blob_cannot_refreeze_an_at_start_auth_pause() {
    use broca_core::{run_loop_with_control, RunControl};

    let dir =
        broca_test_support::scratch_root().join(format!("broca-pa-null-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let plane = StubToolPlane::new();
    let initial = ProjectCapturingProvider::new(
        Some("durable-project"),
        vec![text_reply("unused", "unused")],
    );
    let pause = RunControl {
        pause_for_auth_at_start: Some(ProviderError {
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Auth,
            message: "reauth required".into(),
            status: None,
            retry_after_secs: None,
            provider_code: Some("needs_reauth".into()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        }),
        ..Default::default()
    };
    let (tx, mut rx) = mpsc::channel(64);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    run_loop_with_control(
        req(&session, "hello"),
        &initial,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        pause,
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    drain.await.unwrap();

    let null_provider = NullRenderConfigProvider {
        inner: StubProvider::new(vec![text_reply("must-not-run", "wrong")]),
    };
    let (tx, mut rx) = mpsc::channel(64);
    let events = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    });
    let summary = run_loop_resume_after_auth(
        req(&session, "hello"),
        &null_provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    let events = events.await.unwrap();
    assert_eq!(summary.reason, RunFinishReason::Interrupted);
    assert!(events.iter().any(|event| matches!(
        event,
        broca_core::OutputEvent::Control(ControlUnit::Error { error })
            if error.provider_code.as_deref() == Some("frozen_render_config_unverifiable")
    )));
    let records = records_of(&wal, &session);
    assert!(!records
        .iter()
        .any(|record| matches!(record, WalRecord::StepStarted { .. })));
    assert!(!records
        .iter()
        .any(|record| matches!(record, WalRecord::RunConfigRefrozen { .. })));
    assert!(records.iter().any(|record| matches!(
        record,
        WalRecord::RunFinished {
            reason: RunFinishReason::Interrupted,
            error: Some(error),
            ..
        } if error.provider_code.as_deref() == Some("frozen_render_config_unverifiable")
    )));
    assert!(matches!(
        replay(&records)
            .expect("sealed unverifiable config replays")
            .resolution,
        ReplayResolution::LineageIdle {
            last_reason: RunFinishReason::Interrupted
        }
    ));

    let recovered =
        ProjectCapturingProvider::new(Some("durable-project"), vec![text_reply("answer", "ok")]);
    let mut next_req = req(&session, "hello after review");
    next_req.run_id = RunId("r2".into());
    let (tx, mut rx) = mpsc::channel(64);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop(next_req, &recovered, &plane, &lease, &AllowAll, &wal, &tx)
        .await
        .unwrap();
    drop(tx);
    drain.await.unwrap();
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(recovered.sent_bytes().len(), 1);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Crash resume retains the recorded credential identity instead of replacing it with a
/// newly resolved identity when the handle-to-credential mapping has changed.
#[tokio::test]
async fn resolved_credential_witness_survives_resume_despite_new_resolution() {
    use broca_core::{
        compute_segments, read_prior_run_config, run_loop_with_control, AuthSelection, ChargeBasis,
        RunControl,
    };
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-witness-resume-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let frozen = AuthSelection {
        credential_id: "apikey:kimi-code-plan-global".into(),
        account_ref: None,
        resolved_credential_id: Some("apikey:kimi-for-coding".into()),
        vault_request: None,
        account_decision: None,
    };
    seed_resume_without_terminal(&wal, &session, Some(frozen.clone()));
    let plane = StubToolPlane::new().with_tool("edit", |_| {
        StubReply::Output(ToolOutput::Text { text: "ran".into() })
    });
    let provider = StubProvider::new(vec![text_reply("m2", "done")]);
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop_with_control(
        tool_req("new-request", &session, "hello"),
        &provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        RunControl {
            auth_selection: Some(AuthSelection {
                resolved_credential_id: Some("apikey:changed-since-admission".into()),
                ..frozen.clone()
            }),
            ..Default::default()
        },
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(
        read_prior_run_config(&wal, &session)
            .unwrap()
            .unwrap()
            .auth_selection,
        Some(frozen)
    );
    let frames = WalStore::read_records(&wal, &session).unwrap();
    assert!(!frames
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::RunAuthRefrozen { .. })));
    let segments = compute_segments(&frames, "witness-resume", &|_: &str, _: &str| {
        (ChargeBasis::Unknown, 1)
    });
    assert_eq!(segments.len(), 1);
    assert_eq!(
        segments[0].auth_selection.as_deref(),
        Some("apikey:kimi-code-plan-global")
    );
    assert_eq!(
        segments[0].resolved_credential_id.as_deref(),
        Some("apikey:kimi-for-coding")
    );
    let _ = std::fs::remove_dir_all(&dir);
}
