use super::*;
use crate::compaction::runtime::{CompactionPlane, CompactionUnavailable};
use crate::compaction_provider::{
    setup::{SetupAnswer, SetupRequest},
    view::{ViewGetAnswer, ViewGetRequest},
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

fn negotiated_setup(view: Option<&CompactionView>) -> WalRecord {
    let mut record = setup_record(None);
    let WalRecord::CompactionMessage {
        view_features,
        view_due,
        ..
    } = &mut record
    else {
        unreachable!()
    };
    *view_features = Some(vec!["truncate".into(), "tag_bytes_edits".into()]);
    *view_due = view.map(|view| broca_wal::SetupViewDue {
        version: view.view_version,
        digest: view.digest.clone(),
    });
    record
}

struct DuePlane {
    view: CompactionView,
    refused: AtomicBool,
    requests: Mutex<Vec<ViewGetRequest>>,
}

#[async_trait::async_trait]
impl CompactionPlane for DuePlane {
    async fn setup(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
        _: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        panic!("a recorded Setup must not run again");
    }
    async fn view_get(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
        request: ViewGetRequest,
    ) -> Result<ViewGetAnswer, CompactionUnavailable> {
        self.requests.lock().unwrap().push(request);
        Ok(if self.refused.load(Ordering::SeqCst) {
            ViewGetAnswer::refuse("unknown_version", "not available")
        } else {
            ViewGetAnswer::view(self.view.clone())
        })
    }
}

#[tokio::test]
async fn recorded_due_view_survives_refused_fetch_and_restart_then_applies() {
    let prompt = stamped(vec![text(Role::User, "stored")]);
    let (view, ready) = make_view(
        7,
        0,
        vec![ViewRange::replace(
            0,
            1,
            replace_bytes(&[text(Role::User, "published")]),
        )],
    );
    let setup = negotiated_setup(Some(&view));
    let mut state = CompactionState::default();
    state.fold(&setup);
    assert!(
        rendered_history(&state, &prompt).is_empty(),
        "initial is not renderable while due"
    );
    assert!(render(&CallOptions::new(prompt.clone()), &state, &[])
        .unwrap()
        .is_err());
    let shared = Shared::default();
    let writer = shared.writer();
    let bind = bind();
    let control = crate::RunControl::default();
    let plane = Arc::new(DuePlane {
        view,
        refused: AtomicBool::new(true),
        requests: Mutex::new(Vec::new()),
    });
    let dyn_plane: Arc<dyn CompactionPlane> = plane.clone();
    let runtime = Runtime {
        provider: "provider",
        bind: &bind,
        plane: Some(&dyn_plane),
        writer: &writer,
        control: &control,
    };
    macro_rules! attempt {
        ($state:expr) => {
            runtime.apply_due_view(
                $state,
                crate::compaction::runtime::DueViewAt {
                    session_ref: SESSION_REF,
                    lineage_id: LINEAGE,
                    prompt: &prompt,
                    stored: crate::compaction::StoredTranscript::plain(&prompt),
                    prefix_bound: false,
                    run_start: true,
                    step_start_ms: crate::compaction::now(),
                },
            )
        };
    }
    assert!(!attempt!(&mut state).await.unwrap());
    assert!(state.view_due.is_some());
    assert!(shared.records().is_empty());
    // A notice, including the due version itself, cannot supply Setup's claim check.
    let (decision, _) = runtime
        .offer_view(
            &mut state,
            &plane.view,
            &ready,
            LINEAGE,
            &prompt,
            &prompt,
            false,
            true,
        )
        .await
        .unwrap();
    assert_eq!(decision, Decision::Stale);
    let saved = serde_json::to_vec(&state).unwrap();
    let mut resumed: CompactionState = serde_json::from_slice(&saved).unwrap();
    plane.refused.store(false, Ordering::SeqCst);
    assert!(attempt!(&mut resumed).await.unwrap());
    assert!(resumed.view_due.is_none());
    assert_eq!(plane.requests.lock().unwrap().len(), 2);
    for request in plane.requests.lock().unwrap().iter() {
        assert_eq!(request.view_version, 7);
        assert_eq!(request.session_ref, SESSION_REF);
    }
    let mut replayed = CompactionState::default();
    replayed.fold(&setup);
    for record in shared.records() {
        replayed.fold(&record);
    }
    assert!(replayed.view_due.is_none());
    assert_eq!(
        rendered_history(&replayed, &prompt),
        vec![text(Role::User, "published")]
    );
    writer.shutdown().await.unwrap();
}

#[tokio::test]
async fn due_view_digest_mismatch_and_elapsed_step_never_clear_the_claim() {
    let prompt = stamped(vec![text(Role::User, "stored")]);
    let (view, _) = make_view(7, 0, vec![ViewRange::keep(0, 1)]);
    let mut state = CompactionState::default();
    state.fold(&negotiated_setup(Some(&view)));
    state.view_due.as_mut().unwrap().digest = "0".repeat(64);
    let shared = Shared::default();
    let writer = shared.writer();
    let bind = bind();
    let control = crate::RunControl::default();
    let plane = Arc::new(DuePlane {
        view,
        refused: AtomicBool::new(false),
        requests: Mutex::new(Vec::new()),
    });
    let dyn_plane: Arc<dyn CompactionPlane> = plane.clone();
    let runtime = Runtime {
        provider: "provider",
        bind: &bind,
        plane: Some(&dyn_plane),
        writer: &writer,
        control: &control,
    };
    assert!(!runtime
        .apply_due_view(
            &mut state,
            crate::compaction::runtime::DueViewAt {
                session_ref: SESSION_REF,
                lineage_id: LINEAGE,
                prompt: &prompt,
                stored: crate::compaction::StoredTranscript::plain(&prompt),
                prefix_bound: false,
                run_start: true,
                step_start_ms: crate::compaction::now(),
            }
        )
        .await
        .unwrap());
    assert!(state.view_due.is_some());
    assert!(!runtime
        .apply_due_view(
            &mut state,
            crate::compaction::runtime::DueViewAt {
                session_ref: SESSION_REF,
                lineage_id: LINEAGE,
                prompt: &prompt,
                stored: crate::compaction::StoredTranscript::plain(&prompt),
                prefix_bound: false,
                run_start: true,
                step_start_ms: crate::compaction::now()
                    .saturating_sub(crate::compaction::runtime::VIEW_DUE_MAX_MS),
            }
        )
        .await
        .unwrap());
    assert_eq!(plane.requests.lock().unwrap().len(), 1);
    assert!(shared.records().is_empty());
    writer.shutdown().await.unwrap();
}

fn tagged_prompt() -> (Vec<Message>, Vec<Message>) {
    let mut stored = stamped(vec![Message::new(
        Role::User,
        vec![
            ContentBlock::Text {
                text: "first".into(),
            },
            ContentBlock::Text {
                text: "second".into(),
            },
        ],
    )]);
    broca_protocol::tags::number_message(
        &mut stored[0],
        broca_protocol::tags::ComputedRules {
            render: true,
            clean: true,
        },
        &mut 16,
    )
    .unwrap();
    let mut prompt = stored.clone();
    broca_protocol::tags::render_tags_into(
        &mut prompt,
        broca_protocol::tags::TagRendering::default(),
    );
    (stored, prompt)
}

#[test]
fn assistant_part_edits_preserve_runner_authored_tag_references() {
    use cortexkit_role_llm_runner::tags::TruncateMode;
    let mut stored = stamped(vec![text(Role::Assistant, "model-authored text")]);
    broca_protocol::tags::number_message(
        &mut stored[0],
        broca_protocol::tags::ComputedRules {
            render: true,
            clean: true,
        },
        &mut 1,
    )
    .unwrap();
    let mut prompt = stored.clone();
    broca_protocol::tags::render_tags_into(
        &mut prompt,
        broca_protocol::tags::TagRendering::default(),
    );
    for (edit, expected) in [
        (
            PartEdit::truncate(0, "m0", "m0#0", TruncateMode::Placeholder),
            "§2§ [dropped §2§]",
        ),
        (
            PartEdit::replace(
                0,
                "m0",
                "m0#0",
                r#"{"type":"text","text":"provider reference §99§"}"#,
            ),
            "§2§ provider reference §99§",
        ),
    ] {
        let (view, ready) = make_view(2, 0, vec![ViewRange::keep_with_edits(0, 1, vec![edit])]);
        let mut state = CompactionState::default();
        state.fold(&negotiated_setup(None));
        assert_eq!(
            decide(&view, &ready, &state, LINEAGE, &prompt, &stored, false),
            Decision::Apply(CompactionViewChoice::Applied)
        );
        state.fold(&WalRecord::CompactionViewApplied {
            provider: "provider".into(),
            view_version: 2,
            view: broca_wal::OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap())
                .unwrap(),
            choice: CompactionViewChoice::Applied,
        });
        let rendered = render(&CallOptions::new(prompt.clone()), &state, &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            rendered.prompt[0].content,
            vec![ContentBlock::Text {
                text: expected.into()
            }]
        );
    }
}

#[test]
fn negotiated_part_edits_use_the_parts_own_tags_and_replay_old_bytes_verbatim() {
    use cortexkit_role_llm_runner::tags::TruncateMode;
    let (stored, prompt) = tagged_prompt();
    for (edit, expected) in [
        (
            PartEdit::replace(0, "m0", "m0#1", r#"{"type":"text","text":"changed"}"#),
            "§18§ changed",
        ),
        (
            PartEdit::truncate(0, "m0", "m0#1", TruncateMode::Placeholder),
            "§18§ [dropped §18§]",
        ),
    ] {
        let (view, ready) = make_view(2, 0, vec![ViewRange::keep_with_edits(0, 1, vec![edit])]);
        let mut state = CompactionState::default();
        let setup = negotiated_setup(None);
        state.fold(&setup);
        assert_eq!(
            decide(&view, &ready, &state, LINEAGE, &prompt, &stored, false),
            Decision::Apply(CompactionViewChoice::Applied)
        );
        let record = WalRecord::CompactionViewApplied {
            provider: "provider".into(),
            view_version: 2,
            view: broca_wal::OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap())
                .unwrap(),
            choice: CompactionViewChoice::Applied,
        };
        state.fold(&record);
        let rendered = render(&CallOptions::new(prompt.clone()), &state, &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            rendered.prompt[0].content,
            vec![
                ContentBlock::Text {
                    text: "§17§ first".into()
                },
                ContentBlock::Text {
                    text: expected.into()
                },
            ]
        );
        let mut resumed = CompactionState::default();
        for record in [setup, record] {
            let bytes = serde_json::to_vec(&record).unwrap();
            resumed.fold(&serde_json::from_slice(&bytes).unwrap());
        }
        assert_eq!(
            render(&CallOptions::new(prompt.clone()), &resumed, &[])
                .unwrap()
                .unwrap()
                .prompt,
            rendered.prompt
        );
    }
    // A recorded pre-negotiation view keeps final edited bytes, including after replay.
    let (old, _) = make_view(
        2,
        0,
        vec![ViewRange::keep_with_edits(
            0,
            1,
            vec![PartEdit::replace(
                0,
                "m0",
                "m0#1",
                r#"{"type":"text","text":"old-final"}"#,
            )],
        )],
    );
    let mut state = after_setup();
    let record = WalRecord::CompactionViewApplied {
        provider: "provider".into(),
        view_version: 2,
        view: broca_wal::OpaqueJson::from_json_text(serde_json::to_string(&old).unwrap()).unwrap(),
        choice: CompactionViewChoice::Applied,
    };
    state
        .fold(&serde_json::from_slice::<WalRecord>(&serde_json::to_vec(&record).unwrap()).unwrap());
    let rendered = render(&CallOptions::new(prompt), &state, &[])
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_string(&rendered.prompt[0]).unwrap(),
        r#"{"role":"user","content":[{"type":"text","text":"§17§ first"},{"type":"text","text":"old-final"}]}"#
    );
}

#[test]
fn truncate_checks_stored_container_negotiation_and_shared_renderer_refusals() {
    use cortexkit_role_llm_runner::tags::TruncateMode;
    let (stored, prompt) = tagged_prompt();
    let mut state = CompactionState::default();
    state.fold(&negotiated_setup(None));
    for (edit, expected) in [
        (
            PartEdit::truncate(0, "m0", "m0#1", TruncateMode::Placeholder).with_drop_attachments(),
            Decision::Rejected(ViewProblem::EditAction),
        ),
        (
            PartEdit::truncate(0, "m0", "m0#1", TruncateMode::Skeleton),
            Decision::Refuse("truncate_kind"),
        ),
    ] {
        let (view, ready) = make_view(2, 0, vec![ViewRange::keep_with_edits(0, 1, vec![edit])]);
        assert_eq!(
            decide(&view, &ready, &state, LINEAGE, &prompt, &stored, false),
            expected
        );
    }
    let (view, ready) = make_view(
        2,
        0,
        vec![ViewRange::keep_with_edits(
            0,
            1,
            vec![PartEdit::truncate(
                0,
                "m0",
                "m0#1",
                TruncateMode::Placeholder,
            )],
        )],
    );
    assert_eq!(
        decide(
            &view,
            &ready,
            &after_setup(),
            LINEAGE,
            &prompt,
            &stored,
            false
        ),
        Decision::Refuse("truncate_kind")
    );
    let untagged = stamped(vec![text(Role::User, "original")]);
    let (view, _) = make_view(
        2,
        0,
        vec![ViewRange::keep_with_edits(
            0,
            1,
            vec![PartEdit::truncate(
                0,
                "m0",
                "m0#0",
                TruncateMode::Placeholder,
            )],
        )],
    );
    let rendered = assemble_judged(
        &view,
        &untagged,
        Some(crate::compaction::StoredTranscript::plain(&untagged)),
        state.view_features.as_deref().unwrap(),
    )
    .unwrap();
    assert_eq!(
        rendered[0].content,
        vec![ContentBlock::Text {
            text: "[dropped]".into()
        }]
    );
}

#[test]
fn negotiated_replace_ranges_remain_final_and_never_inherit_part_tags() {
    let (_, prompt) = tagged_prompt();
    let (view, _) = make_view(
        2,
        0,
        vec![ViewRange::replace(
            0,
            1,
            r#"[{"role":"user","content":[{"type":"text","text":"provider-final"}]}]"#,
        )],
    );
    let mut state = CompactionState::default();
    state.fold(&negotiated_setup(None));
    state.fold(&WalRecord::CompactionViewApplied {
        provider: "provider".into(),
        view_version: 2,
        view: broca_wal::OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap()).unwrap(),
        choice: CompactionViewChoice::Applied,
    });
    let rendered = render(&CallOptions::new(prompt), &state, &[])
        .unwrap()
        .unwrap();
    assert_eq!(
        rendered.prompt[0].content,
        vec![ContentBlock::Text {
            text: "provider-final".into()
        }]
    );
}

#[test]
fn runner_truncate_edit_marker_preserves_a_stored_tool_calls_signature() {
    use cortexkit_role_llm_runner::tags::TruncateMode;
    let mut call = call("signed");
    let ContentBlock::ToolCall(tool) = &mut call else {
        unreachable!()
    };
    tool.input = json!({"content":"a".repeat(60),"path":"keep"});
    tool.thought_signature = Some("unchanged".into());
    let prompt = stamped(vec![
        Message::new(Role::Assistant, vec![call]),
        Message::new(Role::Tool, vec![result("signed")]),
    ]);
    let (view, ready) = make_view(
        2,
        1,
        vec![ViewRange::keep_with_edits(
            0,
            2,
            vec![PartEdit::truncate(
                0,
                "m0",
                "m0#0",
                TruncateMode::EditMarker,
            )],
        )],
    );
    let mut state = CompactionState::default();
    state.fold(&negotiated_setup(None));
    assert_eq!(
        decide(&view, &ready, &state, LINEAGE, &prompt, &prompt, false),
        Decision::Apply(CompactionViewChoice::Applied)
    );
    let rendered = assemble_judged(
        &view,
        &prompt,
        Some(crate::compaction::StoredTranscript::plain(&prompt)),
        state.view_features.as_deref().unwrap(),
    )
    .unwrap();
    let ContentBlock::ToolCall(tool) = &rendered[0].content[0] else {
        panic!("tool call retained")
    };
    assert_eq!(
        tool.input,
        json!({"content":format!("{}...[truncated]", "a".repeat(40)),"path":"keep"})
    );
    assert_eq!(tool.thought_signature.as_deref(), Some("unchanged"));
}
