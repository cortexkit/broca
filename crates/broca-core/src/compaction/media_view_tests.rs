use super::*;
use crate::compaction::tests::{altered_media, image};
use broca_protocol::{MediaRef, ToolOutput, ToolOutputPart, ToolResult};

fn media_result(parts: Vec<ToolOutputPart>) -> ContentBlock {
    ContentBlock::ToolResult(ToolResult {
        tool_call_id: broca_protocol::ToolCallId("a".into()),
        output: ToolOutput::Parts { parts },
        is_error: false,
    })
}

fn parts(reference: MediaRef) -> Vec<ToolOutputPart> {
    vec![
        ToolOutputPart::Text {
            text: "long before".into(),
        },
        ToolOutputPart::Media { media: reference },
        ToolOutputPart::Text {
            text: "long after".into(),
        },
    ]
}

fn media_conversation() -> Vec<Message> {
    stamped(vec![
        text(Role::User, "question"),
        Message::new(Role::Assistant, vec![call("a")]),
        Message::new(Role::Tool, vec![media_result(parts(image()))]),
        text(Role::User, "continue"),
    ])
}

fn capture(state: &mut CompactionState, input: &[Message]) {
    state.fold(&serde_json::from_value(json!({"type":"steered", "input": input})).unwrap());
}

fn keep_edit(block: ContentBlock) -> (CompactionView, ViewReady) {
    make_view(
        2,
        2,
        vec![ViewRange::keep_with_edits(
            0,
            3,
            vec![PartEdit::replace(
                2,
                "m2",
                part_key(2, 0),
                serde_json::to_string(&block).unwrap(),
            )],
        )],
    )
}

#[tokio::test]
async fn keep_media_edits_preserve_images_or_replace_them_with_text_without_renumbering() {
    use broca_protocol::tags::{number_message, render_tags_into, ComputedRules, TagRendering};
    for image_only in [false, true] {
        let mut raw = media_conversation();
        if image_only {
            raw[2].content[0] = media_result(vec![ToolOutputPart::Media { media: image() }]);
        }
        let mut high = 0;
        for message in &mut raw {
            number_message(
                message,
                ComputedRules {
                    render: true,
                    clean: false,
                },
                &mut high,
            )
            .unwrap();
        }
        let stored_bytes = serde_json::to_vec(&raw).unwrap();
        let numbers: Vec<_> = raw.iter().map(|m| m.part_tags().cloned()).collect();
        let mut prompt = raw.clone();
        render_tags_into(&mut prompt, TagRendering::for_family(Some("anthropic")));
        for strip in [false, true] {
            let mut edited = if image_only {
                vec![ToolOutputPart::Media { media: image() }]
            } else {
                vec![
                    ToolOutputPart::Text {
                        text: "before".into(),
                    },
                    ToolOutputPart::Media { media: image() },
                    ToolOutputPart::Text {
                        text: "after".into(),
                    },
                ]
            };
            if strip {
                edited[usize::from(!image_only)] = ToolOutputPart::Text {
                    text: "[image stripped]".into(),
                };
            }
            let block = media_result(edited);
            let (view, ready) = keep_edit(block.clone());
            let shared = Shared::default();
            let mut state = after_setup();
            capture(&mut state, &raw);
            assert_eq!(
                apply_over(&shared, &mut state, &view, &ready, &prompt, &raw).await,
                Decision::Apply(CompactionViewChoice::Applied)
            );
            let rendered = model_options(&CallOptions::new(prompt.clone()), &state).unwrap();
            assert_eq!(rendered.prompt[2].content, vec![block]);
            assert_eq!(
                rendered
                    .prompt
                    .iter()
                    .map(|m| m.part_tags().cloned())
                    .collect::<Vec<_>>(),
                numbers
            );
            assert_eq!(serde_json::to_vec(&raw).unwrap(), stored_bytes);
            let record = shared
                .records()
                .into_iter()
                .find(|record| matches!(record, WalRecord::CompactionViewApplied { .. }))
                .unwrap();
            assert!(record
                .required_features()
                .contains(&broca_wal::framing::MEDIA_CONTENT_FEATURE));
        }
    }
}

#[test]
fn unjudged_render_marks_every_uncaptured_or_altered_reference() {
    let raw = media_conversation();
    let mut state = after_setup();
    capture(&mut state, &raw);
    for reference in std::iter::once(image()).chain(altered_media()) {
        let replacement = vec![Message::new(
            Role::User,
            vec![
                ContentBlock::Media(reference.clone()),
                media_result(parts(reference.clone())),
            ],
        )];
        let (view, _) = make_view(
            2,
            3,
            vec![ViewRange::replace(0, 4, replace_bytes(&replacement))],
        );
        state.view = Some(AppliedView {
            view: view.clone(),
            choice: CompactionViewChoice::Applied,
        });
        let opts = model_options(&CallOptions::new(raw.clone()), &state).unwrap();
        assert_eq!(opts.prompt, rendered_history(&state, &raw));
        if reference == image() {
            assert_eq!(opts.prompt, replacement);
        } else {
            assert_eq!(
                crate::compaction::media_refs(&opts.prompt[0].content).count(),
                0
            );
            assert!(serde_json::to_string(&opts.prompt)
                .unwrap()
                .contains("reason=provenance"));
        }
        // With no session capture context, even the exact reference fails closed.
        let isolated = assemble(&view, &raw).unwrap();
        assert_eq!(
            crate::compaction::media_refs(&isolated[0].content).count(),
            0
        );
    }
}

#[test]
fn captured_index_accepts_steers_and_tool_results_but_never_provider_replacements() {
    let mut state = after_setup();
    capture(
        &mut state,
        &[Message::new(Role::User, vec![ContentBlock::Media(image())])],
    );
    assert!(state.captured.contains(&image()));
    for (i, kind) in ["tool_result", "late_result"].into_iter().enumerate() {
        let reference = altered_media().remove(i);
        let ContentBlock::ToolResult(result) = media_result(parts(reference.clone())) else {
            unreachable!()
        };
        let record = serde_json::from_value(json!({
            "type": kind, "batch_id": "b", "call_key": "a", "outcome": "completed", "result": result,
        })).unwrap();
        state.fold(&record);
        assert!(state.captured.contains(&reference));
    }
    let mut setup = setup_record(None);
    let WalRecord::CompactionMessage { replacement, .. } = &mut setup else {
        unreachable!()
    };
    let foreign = altered_media().remove(4);
    *replacement = std::sync::Arc::new(vec![Message::new(
        Role::User,
        vec![ContentBlock::Media(foreign.clone())],
    )]);
    let bytes = replace_bytes(replacement);
    state.fold(&setup);
    assert!(!state.captured.contains(&foreign));
    let (view, _) = make_view(2, 0, vec![ViewRange::replace(0, 1, bytes)]);
    let applied = WalRecord::CompactionViewApplied {
        provider: "provider".into(),
        view_version: 2,
        view: OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap()).unwrap(),
        choice: CompactionViewChoice::Applied,
    };
    state.fold(&applied);
    assert!(!state.captured.contains(&foreign));
}

#[tokio::test]
async fn keep_media_foreign_or_changed_metadata_refuses_the_whole_view() {
    let raw = media_conversation();
    for reference in altered_media() {
        let (view, ready) = keep_edit(media_result(parts(reference)));
        let shared = Shared::default();
        let mut state = after_setup();
        assert_eq!(
            apply_over(&shared, &mut state, &view, &ready, &raw, &raw).await,
            Decision::Refuse(refusal_reasons::PROVIDER_CONSTRAINT)
        );
        assert!(shared.records().is_empty());
        assert!(state.view.is_none());
    }
    // A capture elsewhere in the message does not authorize an edit to this part.
    let mut raw = raw;
    raw[2]
        .content
        .push(ContentBlock::Media(altered_media().remove(0)));
    let (view, ready) = keep_edit(media_result(parts(altered_media().remove(0))));
    assert_eq!(
        decide(&view, &ready, &after_setup(), LINEAGE, &raw, &raw, false),
        Decision::Refuse(refusal_reasons::PROVIDER_CONSTRAINT)
    );
}

#[tokio::test]
async fn replace_media_is_authorized_only_by_its_carried_range() {
    let raw = media_conversation();
    for reference in std::iter::once(image()).chain(altered_media()) {
        for from in [0, 3] {
            let mut ranges = Vec::new();
            if from > 0 {
                ranges.push(ViewRange::keep(0, from));
            }
            ranges.push(ViewRange::replace(
                from,
                4,
                replace_bytes(&[Message::new(
                    Role::User,
                    vec![ContentBlock::Media(reference.clone())],
                )]),
            ));
            let (view, ready) = make_view(2, 3, ranges);
            let shared = Shared::default();
            let mut state = after_setup();
            let decision = apply_over(&shared, &mut state, &view, &ready, &raw, &raw).await;
            if from == 0 && reference == image() {
                assert_eq!(decision, Decision::Apply(CompactionViewChoice::Applied));
                assert!(state.view.is_some());
                assert!(shared.records()[0]
                    .required_features()
                    .contains(&broca_wal::framing::MEDIA_CONTENT_FEATURE));
            } else {
                assert_eq!(
                    decision,
                    Decision::Refuse(refusal_reasons::PROVIDER_CONSTRAINT)
                );
                assert!(shared.records().is_empty());
                assert!(state.view.is_none());
            }
        }
    }
}

#[test]
fn keep_media_block_checks_the_original_part_not_other_session_captures() {
    let raw = stamped(vec![
        Message::new(Role::User, vec![ContentBlock::Media(image())]),
        Message::new(
            Role::User,
            vec![ContentBlock::Media(altered_media().remove(0))],
        ),
    ]);
    for reference in std::iter::once(image()).chain(altered_media()) {
        let (view, ready) = make_view(
            2,
            1,
            vec![ViewRange::keep_with_edits(
                0,
                2,
                vec![PartEdit::replace(
                    0,
                    "m0",
                    part_key(0, 0),
                    serde_json::to_string(&ContentBlock::Media(reference.clone())).unwrap(),
                )],
            )],
        );
        assert_eq!(
            decide(&view, &ready, &after_setup(), LINEAGE, &raw, &raw, false),
            if reference == image() {
                Decision::Apply(CompactionViewChoice::Applied)
            } else {
                Decision::Refuse(refusal_reasons::PROVIDER_CONSTRAINT)
            }
        );
    }
}

#[test]
fn media_replacements_read_their_released_range_before_judging() {
    let raw = media_conversation();
    let sparse = &raw[3..];
    let (view, ready) = make_view(
        2,
        3,
        vec![ViewRange::replace(
            0,
            4,
            replace_bytes(&[Message::new(Role::User, vec![ContentBlock::Media(image())])]),
        )],
    );
    let needs = crate::compaction::released::released_needs(
        &view,
        sparse,
        crate::compaction::StoredTranscript::plain(sparse).with_extent(4),
        4,
    );
    assert!(needs.stored.iter().any(|range| range.contains(&2)));
    assert_eq!(
        decide(&view, &ready, &after_setup(), LINEAGE, &raw, &raw, false),
        Decision::Apply(CompactionViewChoice::Applied)
    );
}

#[tokio::test]
async fn live_captures_authorize_media_only_after_the_wal_barrier() {
    let shared = Shared::default();
    let writer = shared.writer();
    writer
        .record(
            serde_json::from_value(json!({"type":"steered", "input": [
                Message::new(Role::User, vec![ContentBlock::Media(image())]),
            ]}))
            .unwrap(),
        )
        .into_result()
        .unwrap();
    assert!(writer.take_media_captures().is_empty());
    writer.sync().await.unwrap();
    assert!(writer.take_media_captures().contains(&image()));
    assert!(writer.take_media_captures().is_empty());
    writer.shutdown().await.unwrap();
}
