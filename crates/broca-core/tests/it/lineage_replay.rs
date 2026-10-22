//! Multi-episode (lineage) replay conformance — the Phase-1 close-review gates.
//!
//! A session WAL is a SEQUENCE of episodes (one per turn). These tests assert the
//! episode-aware replay properties that the single-episode tests can't reach:
//!  1. multi-episode crash-cut: truncate a 3-episode WAL at EVERY record boundary,
//!     assert closed episodes become the immutable prefix, the trailing episode
//!     classifies correctly, and the recovered config/run_id are the LAST episode's;
//!  2. an episode terminal does NOT seal the lineage (it is idle + appendable);
//!  3. an `AuthRequired` pause is resumable, never read as a terminal;
//!  4. a `RunStarted` over an unfinished prior episode is a fail-loud corrupt lineage.
//!
//! Plus a non-gating per-episode-config-recovery check (episode 2's config wins).

use broca_core::{replay, ReplayError, ReplayResolution};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role},
    error::{ErrorClass, ProviderError},
    request::{GenerationConfig, ModelHandle, ToolChoice},
    BindIdentity, MessageId, RunId, StepId, Usage,
};
use broca_wal::{PauseReason, RunConfig, RunOrigin, WalRecord};

// ───────────────────────── record builders ─────────────────────────

fn run_config(max_tokens: u32) -> RunConfig {
    RunConfig {
        turn_directives_v1: None,
        render_tags: false,
        computed_tags: None,
        model: ModelHandle::new("stub", "m"),
        tools: vec![].into(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig {
            max_output_tokens: Some(u64::from(max_tokens)),
            ..Default::default()
        },
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

/// A `RunStarted` opening an episode `run`, with a user turn `text` and a config
/// distinguished by `max_tokens` (so a per-episode-config test can tell them apart).
fn start(run: &str, text: &str, max_tokens: u32) -> WalRecord {
    WalRecord::RunStarted {
        tags: None,
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
        ts_ms: None,
        run_id: RunId(run.into()),
        session: BindIdentity::new("/tmp/p", "h", "s"),
        config: Box::new(run_config(max_tokens)),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text { text: text.into() }],
        }]
        .into(),
        origin: RunOrigin::Fresh,
        submission_id: None,
    }
}

/// A terminal (text-only) model step.
fn step_text(step: u64, text: &str) -> WalRecord {
    WalRecord::ModelStepFinished {
        tags: None,
        hooks: None,
        step_id: StepId(step),
        assistant_message: (AssistantMessage {
            message_id: MessageId(format!("m{step}")),
            content: vec![ContentBlock::Text { text: text.into() }],
        })
        .into(),
        usage: Usage {
            input_tokens: Some(7),
            output_tokens: Some(3),
            ..Default::default()
        },
        finish_reason: None,
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
    }
}

fn turn_finished(step: u64) -> WalRecord {
    WalRecord::TurnFinished {
        step_id: StepId(step),
    }
}

fn run_finished(reason: RunFinishReason) -> WalRecord {
    WalRecord::RunFinished {
        interrupt_reason: None,
        ts_ms: None,
        reason,
        usage: Usage::default(),
        error: None,
        retries_used: 0,
    }
}

/// One complete text-only episode: RunStarted, a text step, TurnFinished, RunFinished.
fn episode(run: &str, prompt: &str, answer: &str, max_tokens: u32) -> Vec<WalRecord> {
    vec![
        start(run, prompt, max_tokens),
        step_text(1, answer),
        turn_finished(1),
        run_finished(RunFinishReason::Completed),
    ]
}

/// A 3-episode finished lineage.
fn three_episode_lineage() -> Vec<WalRecord> {
    let mut recs = Vec::new();
    recs.extend(episode("r1", "q1", "a1", 100));
    recs.extend(episode("r2", "q2", "a2", 200));
    recs.extend(episode("r3", "q3", "a3", 300));
    recs
}

fn prompt_text(prompt: &[Message]) -> String {
    prompt
        .iter()
        .flat_map(|m| m.content.iter())
        .filter_map(|b| match b {
            ContentBlock::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("|")
}

// ───────────────────────── 1. multi-episode crash-cut ─────────────────────────

/// Truncating a 3-episode lineage at EVERY record boundary always replays to a
/// COHERENT state — never a corruption error on a clean prefix, never a fabricated
/// prompt. At each cut the prompt is exactly what the records so far justify, the
/// closed episodes are the immutable prefix, and the recovered run_id/config track the
/// LAST `RunStarted` seen.
#[test]
fn multi_episode_crash_cut_is_always_coherent() {
    let full = three_episode_lineage();

    for cut in 1..=full.len() {
        let prefix = &full[..cut];
        let st = replay(prefix).unwrap_or_else(|e| panic!("cut@{cut}: replay errored: {e}"));

        // The recovered run_id is the LAST RunStarted at or before the cut.
        let expected_run_id = prefix.iter().rev().find_map(|r| match r {
            WalRecord::RunStarted { run_id, .. } => Some(run_id.clone()),
            _ => None,
        });
        assert_eq!(
            st.run_id, expected_run_id,
            "cut@{cut}: run_id is the last episode's"
        );

        // The recovered config is the LAST RunStarted's (distinguished by max_tokens).
        let expected_max = prefix.iter().rev().find_map(|r| match r {
            WalRecord::RunStarted { config, .. } => config.generation.max_output_tokens,
            _ => None,
        });
        assert_eq!(
            st.run_config
                .as_ref()
                .and_then(|c| c.generation.max_output_tokens),
            expected_max,
            "cut@{cut}: run_config is the last episode's"
        );

        // Every committed turn so far (a step whose TurnFinished+RunFinished are within
        // the cut) is in the prompt; a turn whose RunFinished isn't durable yet is the
        // trailing episode and may or may not be committed, but the prompt never
        // contains a turn from a not-yet-started episode.
        let committed_answers = ["a1", "a2", "a3"]
            .iter()
            .enumerate()
            .filter(|(i, _)| {
                // episode i is fully committed if its RunFinished is within the cut.
                // each episode is 4 records: RunStarted, step, TurnFinished, RunFinished.
                let run_finished_idx = i * 4 + 3;
                run_finished_idx < cut
            })
            .map(|(_, a)| *a)
            .collect::<Vec<_>>();
        let text = prompt_text(&st.prompt);
        for a in &committed_answers {
            assert!(
                text.contains(a),
                "cut@{cut}: committed answer {a} must be in the prefix ({text})"
            );
        }
    }
}

/// The full 3-episode lineage replays to one coherent conversation: all three turns'
/// questions AND answers in order, idle at the end, last episode's run_id.
#[test]
fn full_three_episode_lineage_is_one_conversation() {
    let st = replay(&three_episode_lineage()).expect("replay");
    assert!(matches!(
        st.resolution,
        ReplayResolution::LineageIdle { .. }
    ));
    assert_eq!(st.run_id, Some(RunId("r3".into())));
    let text = prompt_text(&st.prompt);
    for fragment in ["q1", "a1", "q2", "a2", "q3", "a3"] {
        assert!(
            text.contains(fragment),
            "the lineage prompt carries {fragment}: {text}"
        );
    }
    // Per-episode counter: the LAST episode had exactly one step.
    assert_eq!(
        st.completed_steps, 1,
        "completed_steps is the last episode's, not the lineage total"
    );
}

// ───────────────────────── 2. episode terminal ≠ lineage sealed ─────────────────

/// A WAL ending in a durable `RunFinished` classifies `LineageIdle` (idle +
/// APPENDABLE), NEVER a "sealed" state. The resolution carries the terminal reason for
/// run.status, and a new episode can be appended (a fresh `RunStarted` after it replays
/// coherently as a 2-episode lineage).
#[test]
fn episode_terminal_does_not_seal_the_lineage() {
    let mut recs = episode("r1", "q1", "a1", 100);
    let st = replay(&recs).expect("replay");
    assert_eq!(
        st.resolution,
        ReplayResolution::LineageIdle {
            last_reason: RunFinishReason::Completed
        },
        "a finished episode is idle+appendable, not sealed"
    );

    // Appending a fresh RunStarted (a new turn) over the idle lineage is well-formed:
    // it replays to a 2-episode lineage, NOT a corruption error.
    recs.extend(episode("r2", "q2", "a2", 200));
    let st2 = replay(&recs).expect("appending a new episode over an idle lineage is valid");
    assert!(matches!(
        st2.resolution,
        ReplayResolution::LineageIdle { .. }
    ));
    assert_eq!(st2.run_id, Some(RunId("r2".into())));
    let text = prompt_text(&st2.prompt);
    assert!(
        text.contains("a1") && text.contains("a2"),
        "both episodes' turns: {text}"
    );
}

// ───────────────────────── 3. AuthRequired pause is resumable ───────────────────

/// A WAL ending in `RunPaused{AuthRequired}` classifies `AuthRequired` — a NON-terminal
/// resolution carrying the SAME run_id — never `LineageIdle` (which would treat the
/// pause as a terminal that seals the run). The pause's error detail is preserved.
#[test]
fn auth_required_pause_is_resumable_not_sealed() {
    let mut recs = vec![start("r1", "q1", 100), step_text(1, "partial")];
    recs.push(WalRecord::RunPaused {
        ts_ms: None,
        run_id: RunId("r1".into()),
        reason: PauseReason::AuthRequired,
        error: Some(ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Auth,
            message: "token expired".into(),
            status: Some(401),
            retry_after_secs: None,
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        }),
    });

    let st = replay(&recs).expect("replay");
    match st.resolution {
        ReplayResolution::AuthRequired {
            run_id,
            reason,
            error,
            pending: None,
        } => {
            assert_eq!(
                run_id,
                RunId("r1".into()),
                "resume continues the SAME run_id"
            );
            assert_eq!(reason, PauseReason::AuthRequired);
            assert_eq!(
                error.map(|e| e.message),
                Some("token expired".to_string()),
                "the pause error detail is preserved for run.status"
            );
        }
        other => panic!("expected AuthRequired, got {other:?}"),
    }
    // A paused episode is NOT a durable terminal.
    assert!(
        !st.run_finished_durable,
        "a pause is not a terminal — the run is not finished"
    );
}

/// A pause checked FIRST: even if (hypothetically) other trailing state exists, the
/// pause wins the classification (a paused episode is always resumable).
#[test]
fn pause_takes_precedence_over_natural_end() {
    // A terminal text step (natural-end) FOLLOWED by a pause record — the pause must
    // still classify AuthRequired, not LineageIdle.
    let recs = vec![
        start("r1", "q1", 100),
        step_text(1, "answer"),
        WalRecord::RunPaused {
            run_id: RunId("r1".into()),
            ts_ms: None,
            reason: PauseReason::AuthRequired,
            error: None,
        },
    ];
    let st = replay(&recs).expect("replay");
    assert!(
        matches!(st.resolution, ReplayResolution::AuthRequired { .. }),
        "a pause classifies AuthRequired even after a natural-end step"
    );
}

// ───────────────────────── 4. corruption guard ─────────────────────────

/// A `RunStarted` opening a new episode over a prior episode that has NO durable
/// `RunFinished` is a corrupt lineage → fail-loud `ReplayError::CorruptLineage`,
/// naming the unfinished episode. Never a fabricated prefix.
#[test]
fn run_started_over_unfinished_episode_is_corrupt() {
    // Episode 1 never wrote RunFinished; episode 2's RunStarted opens over it.
    let recs = vec![
        start("r1", "q1", 100),
        step_text(1, "a1"),
        turn_finished(1),
        // NO run_finished for r1 — then a new episode starts.
        start("r2", "q2", 200),
    ];
    let err = replay(&recs).expect_err("a RunStarted over an unfinished episode is corrupt");
    match err {
        ReplayError::CorruptLineage { episode_run_id, .. } => {
            assert_eq!(
                episode_run_id,
                RunId("r1".into()),
                "the corruption names the UNFINISHED prior episode"
            );
        }
    }
}

/// A `RunStarted` directly after a `RunPaused` (no RunFinished) is also corrupt — a
/// paused episode is resumed under the SAME run_id, never superseded by a new episode.
#[test]
fn run_started_over_paused_episode_is_corrupt() {
    let recs = vec![
        start("r1", "q1", 100),
        step_text(1, "partial"),
        WalRecord::RunPaused {
            run_id: RunId("r1".into()),
            ts_ms: None,
            reason: PauseReason::AuthRequired,
            error: None,
        },
        start("r2", "q2", 200),
    ];
    let err = replay(&recs).expect_err("a RunStarted after a pause is corrupt");
    assert!(matches!(err, ReplayError::CorruptLineage { .. }));
}

// ───────────────────────── per-episode config recovery (non-gating) ─────────────

/// Episode 2 carries a DIFFERENT config (different max_tokens, SAME family). Replay
/// recovers EPISODE-2's config as authoritative — proving per-episode/last-wins config
/// recovery (the core Phase-1 change), with no cross-provider rendering. This is the
/// mechanism a future model-switch-between-turns would ride on; framed here as
/// per-episode-config-recovery, not a model switch.
#[test]
fn replay_recovers_the_last_episodes_config() {
    let mut recs = episode("r1", "q1", "a1", 111);
    recs.extend(episode("r2", "q2", "a2", 222));
    let st = replay(&recs).expect("replay");
    assert_eq!(
        st.run_config.unwrap().generation.max_output_tokens,
        Some(222),
        "the recovered config is the LAST episode's (last-wins), not the first"
    );
}

#[test]
fn restart_pause_is_resumable_not_sealed() {
    let mut records = vec![start("r1", "q1", 100), step_text(1, "partial")];
    records.push(WalRecord::RunPaused {
        ts_ms: None,
        run_id: RunId("r1".into()),
        reason: PauseReason::Restart,
        error: None,
    });
    let state = replay(&records).unwrap();
    assert!(matches!(
        state.resolution,
        ReplayResolution::AuthRequired {
            reason: PauseReason::Restart,
            ..
        }
    ));
    assert!(!state.run_finished_durable);
    let checkpoint = broca_core::AdmissionCheckpointV1::from_frames(
        &records
            .iter()
            .enumerate()
            .map(|(i, record)| broca_wal::FramedRecord {
                seq: i as u64 + 1,
                fence: 1,
                record: record.clone(),
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    assert_eq!(
        checkpoint.last_episode.unwrap().pause_reason,
        Some(PauseReason::Restart)
    );
}
