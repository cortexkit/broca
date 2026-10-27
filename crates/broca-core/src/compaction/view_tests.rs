//! Asynchronous views: assembly, the checks before applying, strip-or-defer for signed
//! reasoning, durable rendering on resume, advance notices and the ceiling.

use super::*;
use crate::compaction::{
    model_options,
    runtime::Runtime,
    tests::{bind, call, result, setup_record, text, Shared},
    CompactionCheckpoint,
};
use crate::compaction_provider::{
    lineage::MessageRef,
    notice::ViewReady,
    view::{PartEdit, ViewRange},
};
use broca_protocol::CallOptions;
use broca_wal::WalRecord;
use serde_json::json;

const LINEAGE: &str = "lineage-1";
const SESSION_REF: &str = "0123456789abcdef0123456789abcdef";

#[path = "media_view_tests.rs"]
mod media;

fn stamped(mut prompt: Vec<Message>) -> Vec<Message> {
    broca_protocol::content::stamp_ordinals(&mut prompt);
    prompt
}

fn signed(text: &str) -> ContentBlock {
    ContentBlock::Reasoning {
        text: text.into(),
        signature: Some(format!("sig-{text}")),
    }
}

fn assistant(content: Vec<ContentBlock>) -> Message {
    Message::assistant_from(
        content,
        broca_protocol::ModelHandle::new("anthropic", "claude"),
    )
}

fn replace_bytes(messages: &[Message]) -> String {
    serde_json::to_string(messages).unwrap()
}

fn make_view(version: u64, covers: u64, ranges: Vec<ViewRange>) -> (CompactionView, ViewReady) {
    let view = CompactionView::new(
        SESSION_REF,
        version,
        LINEAGE,
        MessageRef {
            ordinal: covers,
            mid: format!("m{covers}"),
        },
        ranges,
    );
    let ready = ViewReady::for_view(&view);
    (view, ready)
}

/// A state after Setup's empty head, as the run loop holds it at its first boundary.
fn after_setup() -> CompactionState {
    let mut state = CompactionState::default();
    state.fold(&setup_record(None));
    state
}

fn conversation() -> Vec<Message> {
    stamped(vec![
        text(Role::User, "first question"),
        assistant(vec![signed("plan"), call("a")]),
        Message::new(Role::Tool, vec![result("a")]),
        text(Role::User, "second question"),
        assistant(vec![
            signed("answer"),
            ContentBlock::Text {
                text: "the answer".into(),
            },
        ]),
        text(Role::User, "third question"),
    ])
}

#[test]
fn companion_strip_ends_at_extent_not_the_last_held_ordinal() {
    let mut prompt = stamped(vec![
        assistant(vec![signed("copy")]),
        text(Role::User, "tail"),
    ]);
    prompt[1].ordinal = Some(5);
    let (view, ready) = make_view(
        2,
        5,
        vec![ViewRange::replace(
            0,
            6,
            replace_bytes(&[
                text(Role::User, "summary"),
                prompt[0].clone(),
                text(Role::User, "continue"),
            ]),
        )],
    );
    let decision = decide_stored_at(
        &view,
        &ready,
        &after_setup(),
        LINEAGE,
        &prompt,
        crate::compaction::StoredTranscript::plain(&prompt).with_extent(100),
        true,
        false,
    );
    assert_eq!(
        decision,
        Decision::Apply(CompactionViewChoice::Companion {
            edit: companion_edits::STRIP_SIGNED_REASONING.into(),
            strip: CompactionStrip {
                from_ordinal: 100,
                to_ordinal: 100
            },
        })
    );
}

#[test]
fn sparse_render_maps_original_first_users_without_promoting_later_steers() {
    use broca_protocol::request::EpisodeRenderInput;
    let prompt = stamped(vec![
        text(Role::User, "original"),
        text(Role::Assistant, "answer"),
        text(Role::User, "discarded original"),
        text(Role::Assistant, "answer"),
        text(Role::User, "later steer"),
        text(Role::Assistant, "admitted without a user"),
        text(Role::User, "another later steer"),
    ]);
    let mut opts = CallOptions::new(prompt);
    opts.render_episodes = std::sync::Arc::new(
        vec![
            EpisodeRenderInput {
                prompt_start: 0,
                first_user: Some(0),
                frozen_config: json!({"effort":"low"}),
            },
            EpisodeRenderInput {
                prompt_start: 2,
                first_user: Some(2),
                frozen_config: json!({"effort":"medium"}),
            },
            EpisodeRenderInput {
                prompt_start: 5,
                first_user: None,
                frozen_config: json!({"effort":"high"}),
            },
        ]
        .into(),
    );
    let (view, _) = make_view(
        2,
        6,
        vec![
            ViewRange::keep(0, 1),
            ViewRange::replace(1, 4, "[]"),
            ViewRange::keep(4, 7),
        ],
    );
    let mut state = after_setup();
    state.view = Some(AppliedView {
        view,
        choice: CompactionViewChoice::Applied,
    });
    let full = model_options(&opts, &state).unwrap();
    opts.prompt.retain(|message| {
        crate::run::held_prompt::held_by(&state.view.as_ref().unwrap().view, message)
    });
    let sparse = model_options(&opts, &state).unwrap();
    assert_eq!(sparse, full);
    let episodes = &sparse.render_episodes.episodes;
    assert_eq!(
        episodes
            .iter()
            .map(|episode| (episode.prompt_start, episode.first_user))
            .collect::<Vec<_>>(),
        vec![(0, Some(0)), (1, None), (2, None)]
    );
}

#[test]
fn ordinal_ranges_bound_the_work_to_the_requested_window() {
    let mut prompt = stamped(vec![text(Role::User, "old"); 150_000]);
    prompt[0].role = Role::System;
    let window = ordinal_range(&prompt, 149_800, 149_999);
    assert_eq!(window.len(), 199);
    assert_eq!(window[0].ordinal, Some(149_800));
    assert!(ordinal_range(&prompt, 150_000, u64::MAX).is_empty());
    assert_eq!(ordinal_range(&prompt, 10, 10).len(), 0);
}

#[test]
fn view_validation_encodes_only_requested_entries_and_possible_signed_copies() {
    let mut prompt = stamped(vec![text(Role::User, "hidden"); 10_000]);
    prompt[1] = assistant(vec![ContentBlock::Text {
        text: "model output".into(),
    }]);
    prompt[1].ordinal = Some(1);
    let (view, ready) = make_view(
        2,
        9_998,
        vec![ViewRange::replace(
            0,
            9_999,
            replace_bytes(&[
                text(Role::User, "summary"),
                text(Role::Assistant, "short answer"),
            ]),
        )],
    );
    let (decision, encodings) = crate::compaction::carried_encoding_tests::measure(|| {
        decide(
            &view,
            &ready,
            &after_setup(),
            LINEAGE,
            &prompt,
            &prompt,
            false,
        )
    });
    assert!(matches!(decision, Decision::Apply(_)), "{decision:?}");
    assert_eq!(
        encodings, 3,
        "only the coverage anchor and the replacements need encoding"
    );
}

#[test]
fn removing_a_model_origin_does_not_make_a_stored_copy_provider_authored() {
    let raw = stamped(vec![
        text(Role::User, "q"),
        assistant(vec![ContentBlock::Text {
            text: "answer".into(),
        }]),
        text(Role::User, "next"),
    ]);
    let mut stripped = raw[1].clone();
    stripped.origin = None;
    let (view, ready) = make_view(
        2,
        1,
        vec![ViewRange::replace(
            0,
            2,
            replace_bytes(&[text(Role::User, "summary"), stripped]),
        )],
    );
    assert_eq!(
        decide(
            &view,
            &ready,
            &after_setup(),
            LINEAGE,
            &tagged(&raw),
            &raw,
            false
        ),
        Decision::Rejected(ViewProblem::ReplacementNotArray)
    );
    let (view, ready) = make_view(
        2,
        1,
        vec![ViewRange::replace(
            0,
            2,
            replace_bytes(&[text(Role::User, "summary"), raw[1].clone()]),
        )],
    );
    assert!(matches!(
        decide(
            &view,
            &ready,
            &after_setup(),
            LINEAGE,
            &tagged(&raw),
            &raw,
            false
        ),
        Decision::Apply(_)
    ));
}

#[test]
fn sparse_originals_preserve_signed_authorship_and_notice_digests() {
    let raw = conversation();
    let prompt = tagged(&raw);
    let mut originals = crate::compaction::OriginalMessages::default();
    for (i, (original, rendered)) in raw.iter().zip(&prompt).enumerate() {
        originals.retain_change(i, original.clone(), rendered);
    }
    let stored = crate::compaction::StoredTranscript::new(&prompt, Some(&originals));
    let state = after_setup();
    for mut copy in [raw[1].clone(), raw[4].clone()] {
        for changed in [false, true] {
            if changed {
                copy.content[0] = signed("forged");
            }
            let (view, ready) = make_view(
                2,
                4,
                vec![ViewRange::replace(
                    0,
                    5,
                    replace_bytes(&[text(Role::User, "summary"), copy.clone()]),
                )],
            );
            assert_eq!(
                decide_stored_at(&view, &ready, &state, LINEAGE, &prompt, stored, false, false),
                decide_at(&view, &ready, &state, LINEAGE, &prompt, &raw, false, false)
            );
        }
    }
    let facts = || NoticeFacts {
        session_ref: SESSION_REF.into(),
        lineage_id: LINEAGE,
        est_tokens: 123,
        model: "model",
        window: Some(1000),
        usage: None,
        events: vec![],
        seq_floor: 0,
    };
    let expected = next_notice(&state, &raw, facts()).unwrap();
    let actual = notice_through(&state, stored.newest().unwrap(), facts()).unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn indexed_plain_summary_does_not_walk_the_replaced_transcript() {
    let mut prompt = stamped(vec![text(Role::User, "hidden"); 150_000]);
    prompt[1] = assistant(vec![ContentBlock::Text {
        text: "model output".into(),
    }]);
    prompt[1].ordinal = Some(1);
    let mut originals = crate::compaction::OriginalMessages::default();
    originals.index_origins(&prompt);
    let stored = crate::compaction::StoredTranscript::new(&prompt, Some(&originals));
    let (view, ready) = make_view(
        2,
        149_998,
        vec![ViewRange::replace(
            0,
            149_999,
            replace_bytes(&[text(Role::User, "summary")]),
        )],
    );
    let (decision, visits) = crate::compaction::stored::measure_range_visits(|| {
        decide_stored_at(
            &view,
            &ready,
            &after_setup(),
            LINEAGE,
            &prompt,
            stored,
            false,
            false,
        )
    });
    assert!(matches!(decision, Decision::Apply(_)));
    assert_eq!(
        visits, 0,
        "a user summary cannot copy an origin that no user message has"
    );
}

#[test]
fn indexed_origins_still_refuse_a_user_copy_with_its_model_origin_removed() {
    let mut prompt = stamped(vec![
        text(Role::User, "model-owned input"),
        text(Role::User, "tail"),
    ]);
    prompt[0].origin = Some(broca_protocol::ModelHandle::new("provider", "model"));
    let mut originals = crate::compaction::OriginalMessages::default();
    originals.index_origins(&prompt[..1]);
    originals.index_origins(&prompt);
    let stored = crate::compaction::StoredTranscript::new(&prompt, Some(&originals));
    let mut stripped = prompt[0].clone();
    stripped.origin = None;
    let (view, ready) = make_view(
        2,
        0,
        vec![ViewRange::replace(0, 1, replace_bytes(&[stripped]))],
    );
    assert_eq!(
        decide_stored_at(
            &view,
            &ready,
            &after_setup(),
            LINEAGE,
            &prompt,
            stored,
            false,
            false
        ),
        Decision::Rejected(ViewProblem::ReplacementNotArray)
    );
}

#[test]
fn indexed_render_keeps_interior_system_messages_and_episode_coordinates() {
    let prompt = stamped(vec![
        text(Role::User, "hidden"),
        text(Role::System, "system"),
        text(Role::User, "kept"),
        text(Role::Assistant, "answer"),
    ]);
    let mut opts = CallOptions::new(prompt.clone());
    opts.render_episodes = std::sync::Arc::new(broca_protocol::request::RenderHistory {
        compaction_prefix_len: None,
        episodes: (0..4)
            .map(|i| broca_protocol::request::EpisodeRenderInput {
                prompt_start: i,
                first_user: Some(i),
                frozen_config: json!({}),
            })
            .collect(),
    });
    let (view, _) = make_view(
        2,
        1,
        vec![ViewRange::replace(
            0,
            2,
            replace_bytes(&[text(Role::User, "summary")]),
        )],
    );
    let mut state = after_setup();
    state.view = Some(AppliedView {
        view,
        choice: broca_wal::CompactionViewChoice::Applied,
    });
    let rendered = crate::compaction::model_options_indexed(&opts, &state, &[1]).unwrap();
    assert_eq!(rendered.prompt[0], prompt[1]);
    let coords: Vec<_> = rendered
        .render_episodes
        .iter()
        .map(|e| (e.prompt_start, e.first_user))
        .collect();
    assert_eq!(coords, vec![(2, Some(2)), (3, Some(3))]);
}

#[test]
fn boundary_request_reuses_exact_bytes_until_a_view_changes() {
    let opts = CallOptions::new(conversation());
    let mut state = after_setup();
    let mut request = crate::compaction::BoundaryRequest::default();
    let renders = std::cell::Cell::new(0);
    let render = |opts: &CallOptions| {
        renders.set(renders.get() + 1);
        crate::canonical_request_bytes(opts)
    };
    for _ in 0..4 {
        request.refresh(&opts, &state, &[], render).unwrap();
        assert_eq!(
            request.bytes,
            crate::canonical_request_bytes(&model_options(&opts, &state).unwrap()).len()
        );
    }
    assert_eq!(renders.get(), 1);
    let (view, _) = make_view(
        2,
        2,
        vec![ViewRange::replace(
            0,
            3,
            replace_bytes(&[text(Role::User, "short")]),
        )],
    );
    state.view = Some(AppliedView {
        view,
        choice: broca_wal::CompactionViewChoice::Applied,
    });
    request.refresh(&opts, &state, &[], render).unwrap();
    assert_eq!(renders.get(), 2);
    let expected = crate::canonical_request_bytes(&model_options(&opts, &state).unwrap());
    assert_eq!(request.bytes, expected.len());
    assert_eq!(
        crate::canonical_request_bytes(&request.into_options()),
        expected
    );
}

/// A view may replace several ranges and edit single parts of the messages it keeps. The
/// history it gives is the role's reference assembly, with broca's transcript messages keeping
/// their ordinals and the provider's messages carrying none.
#[test]
fn multi_range_view_assembles_like_the_role_reference() {
    let prompt = conversation();
    let summary = [text(Role::User, "summary of the first turn")];
    let note = [text(Role::User, "note before the answer")];
    let (view, _) = make_view(
        2,
        4,
        vec![
            ViewRange::replace(0, 3, replace_bytes(&summary)),
            ViewRange::keep_with_edits(
                3,
                4,
                vec![PartEdit::replace(
                    3,
                    "m3",
                    part_key(3, 0),
                    json!({"type":"text","text":"second, shortened"}).to_string(),
                )],
            ),
            ViewRange::replace(4, 4, replace_bytes(&note)),
            ViewRange::keep_with_edits(4, 5, vec![PartEdit::drop(4, "m4", part_key(4, 1))]),
        ],
    );
    view.check().unwrap();
    let history = assemble(&view, &prompt).unwrap();
    let reference = view
        .assemble(
            &prompt
                .iter()
                .filter_map(transcript_message)
                .collect::<Vec<_>>(),
        )
        .unwrap();
    assert_eq!(
        history
            .iter()
            .map(|m| serde_json::to_value(m).unwrap())
            .collect::<Vec<_>>(),
        reference
    );
    let ordinals: Vec<_> = history.iter().map(|m| m.ordinal).collect();
    assert_eq!(ordinals, vec![None, Some(3), None, Some(4), Some(5)]);
    assert_eq!(
        history[1].content,
        vec![ContentBlock::Text {
            text: "second, shortened".into()
        }]
    );
    assert_eq!(history[3].content, vec![signed("answer")]);
}

/// A view is applied only when it is the view announced, its digest matches, it is newer, its
/// anchors (message ids and part keys) match this transcript and its history keeps every tool
/// call next to its result. Otherwise the session keeps rendering what it rendered, and nothing
/// is recorded.
#[tokio::test]
async fn mismatched_views_are_never_applied() {
    let prompt = conversation();
    let state = after_setup();
    let summary = replace_bytes(&[text(Role::User, "summary")]);
    let (good, ready) = make_view(2, 2, vec![ViewRange::replace(0, 3, summary.clone())]);

    let mut tampered = good.clone();
    tampered.ranges = vec![ViewRange::replace(
        0,
        3,
        replace_bytes(&[text(Role::User, "x")]),
    )];
    let mut announced_other = ready.clone();
    announced_other.digest = make_view(3, 2, vec![ViewRange::replace(0, 3, summary.clone())])
        .0
        .digest;
    announced_other.view_version = 2;
    let (missing_part, missing_ready) = make_view(
        2,
        3,
        vec![ViewRange::keep_with_edits(
            0,
            4,
            vec![PartEdit::drop(1, "m1", "m1#7")],
        )],
    );
    let (other_mid, other_mid_ready) = {
        let mut view = good.clone();
        view.covers_through.mid = "m9".into();
        view.digest = view.compute_digest();
        let ready = ViewReady::for_view(&view);
        (view, ready)
    };
    let (split, split_ready) = make_view(2, 1, vec![ViewRange::replace(0, 2, summary.clone())]);
    let cases: Vec<(&str, CompactionView, ViewReady, Decision)> = vec![
        (
            "a view whose bytes no longer match its digest",
            tampered,
            ready.clone(),
            Decision::Rejected(ViewProblem::DigestMismatch),
        ),
        (
            "a digest other than the one announced",
            good.clone(),
            announced_other,
            Decision::Rejected(ViewProblem::DigestMismatch),
        ),
        (
            "an edit of a part the message does not have",
            missing_part,
            missing_ready,
            Decision::Refuse(refusal_reasons::UNKNOWN_PART),
        ),
        (
            "coverage anchored on another message id",
            other_mid,
            other_mid_ready,
            Decision::Discard,
        ),
        (
            "a fold that separates a tool call from its result",
            split,
            split_ready,
            Decision::Refuse(refusal_reasons::TOOL_PAIR_SPLIT),
        ),
    ];
    for (label, view, ready, expected) in cases {
        let shared = Shared::default();
        let writer = shared.writer();
        let control = crate::RunControl::default();
        let bind = bind();
        let runtime = Runtime {
            provider: "provider",
            bind: &bind,
            plane: None,
            writer: &writer,
            control: &control,
        };
        let mut live = state.clone();
        let (decision, event) = runtime
            .offer_view(
                &mut live, &view, &ready, LINEAGE, &prompt, &prompt, true, false,
            )
            .await
            .unwrap();
        assert_eq!(decision, expected, "{label}");
        assert!(shared.records().is_empty(), "{label}: recorded");
        assert_eq!(live, state, "{label}: state changed");
        assert_eq!(
            event.map(|event| event.token()),
            matches!(expected, Decision::Refuse(_)).then(|| format!(
                "view_refused:2:{}",
                match expected {
                    Decision::Refuse(reason) => reason,
                    _ => unreachable!(),
                }
            )),
            "{label}"
        );
        let _ = writer.shutdown().await;
    }
    // The control: the well-formed view does apply.
    assert_eq!(
        decide(&good, &ready, &state, LINEAGE, &prompt, &prompt, true),
        Decision::Apply(CompactionViewChoice::Companion {
            edit: companion_edits::STRIP_SIGNED_REASONING.into(),
            strip: CompactionStrip {
                from_ordinal: 4,
                to_ordinal: 6
            }
        })
    );
    // A view of another lineage is never applied.
    assert_eq!(
        decide(&good, &ready, &state, "lineage-2", &prompt, &prompt, true),
        Decision::Discard
    );
}

/// On a model provider that binds signed reasoning to its prefix (Anthropic's thinking), a
/// view that changes bytes before kept signed reasoning is applied with the companion strip of
/// that reasoning, unless the reasoning belongs to the open tool turn the next request
/// continues: that view is deferred to a later boundary. A provider without the bind applies
/// it as published, and a view that changes nothing before the reasoning needs neither.
#[test]
fn signed_reasoning_is_stripped_or_the_view_deferred() {
    let summary = replace_bytes(&[text(Role::User, "summary")]);
    // A head insertion changes the bytes before every message.
    let (head, head_ready) = make_view(
        2,
        5,
        vec![
            ViewRange::replace(0, 0, summary.clone()),
            ViewRange::keep(0, 6),
        ],
    );
    let prompt = conversation();
    let state = after_setup();
    let companion = decide(&head, &head_ready, &state, LINEAGE, &prompt, &prompt, true);
    assert_eq!(
        companion,
        Decision::Apply(CompactionViewChoice::Companion {
            edit: companion_edits::STRIP_SIGNED_REASONING.into(),
            strip: CompactionStrip {
                from_ordinal: 1,
                to_ordinal: 6
            }
        })
    );
    assert_eq!(
        decide(&head, &head_ready, &state, LINEAGE, &prompt, &prompt, false),
        Decision::Apply(CompactionViewChoice::Applied)
    );

    // The same view while the next request continues the tool turn of message 4's reasoning.
    let mut open = conversation();
    open.truncate(4);
    open.push(assistant(vec![signed("answer"), call("b")]));
    open.push(Message::new(Role::Tool, vec![result("b")]));
    let open = stamped(open);
    let (head_open, head_open_ready) = make_view(
        2,
        5,
        vec![
            ViewRange::replace(0, 0, summary.clone()),
            ViewRange::keep(0, 6),
        ],
    );
    let deferred = decide(
        &head_open,
        &head_open_ready,
        &state,
        LINEAGE,
        &open,
        &open,
        true,
    );
    assert_eq!(deferred, Decision::Defer(defer_reasons::SIGNED_REASONING));
    assert_eq!(
        deferred.event(2).unwrap().token(),
        "view_deferred:2:signed_reasoning"
    );

    // A view whose first change comes after every signed block needs no companion edit. (Under
    // a Setup head the view's dropping that head would itself be a change before them, so the
    // session here has an empty head.)
    let mut bare = after_setup();
    bare.applied.as_mut().unwrap().replacement = std::sync::Arc::new(Vec::new());
    let (late, late_ready) = make_view(
        2,
        5,
        vec![ViewRange::keep(0, 5), ViewRange::replace(5, 6, summary)],
    );
    assert_eq!(
        decide(&late, &late_ready, &bare, LINEAGE, &prompt, &prompt, true),
        Decision::Apply(CompactionViewChoice::Applied)
    );
}

/// Offer `view` over a prompt that renders as stored (no tags or guidance folded in).
async fn apply(
    shared: &Shared,
    state: &mut CompactionState,
    view: &CompactionView,
    ready: &ViewReady,
    prompt: &[Message],
) -> Decision {
    apply_over(shared, state, view, ready, prompt, prompt).await
}

/// Offer `view` over `prompt`, whose stored form (what `session.read` returns) is `stored`.
async fn apply_over(
    shared: &Shared,
    state: &mut CompactionState,
    view: &CompactionView,
    ready: &ViewReady,
    prompt: &[Message],
    stored: &[Message],
) -> Decision {
    let writer = shared.writer();
    let control = crate::RunControl::default();
    let bind = bind();
    let runtime = Runtime {
        provider: "provider",
        bind: &bind,
        plane: None,
        writer: &writer,
        control: &control,
    };
    let (decision, _) = runtime
        .offer_view(state, view, ready, LINEAGE, prompt, stored, true, false)
        .await
        .unwrap();
    writer.shutdown().await.unwrap();
    decision
}

/// An applied view is durable before the step that first renders it, with how it was applied.
/// A session refolded from its records alone (a restart, a resume) renders the same request
/// bytes without asking the provider again, companion strip included; messages written after
/// the view keep their reasoning.
#[tokio::test]
async fn an_applied_view_renders_byte_identically_after_a_restart() {
    let shared = Shared::default();
    let mut live = after_setup();
    let prompt = conversation();
    let (view, ready) = make_view(
        2,
        5,
        vec![
            ViewRange::replace(0, 0, replace_bytes(&[text(Role::User, "summary")])),
            ViewRange::keep(0, 6),
        ],
    );
    assert!(matches!(
        apply(&shared, &mut live, &view, &ready, &prompt).await,
        Decision::Apply(CompactionViewChoice::Companion { .. })
    ));
    let records = shared.records();
    assert_eq!(shared.durable(), records.len(), "applied before it renders");
    assert!(matches!(
        records.as_slice(),
        [WalRecord::CompactionViewApplied {
            view_version: 2,
            ..
        }]
    ));
    // A later turn, produced against the compacted prefix, keeps its reasoning.
    let mut later = prompt.clone();
    later.push(assistant(vec![
        signed("later"),
        ContentBlock::Text { text: "ok".into() },
    ]));
    later.push(text(Role::User, "fourth question"));
    let later = stamped(later);
    let options = CallOptions::new(later);

    // Rebuild the state the way admission and resume do: replay of the records, which places
    // Setup's thinking boundary from WAL order.
    let replayed = crate::replay::replay(
        &std::iter::once(crate::replay::projection_boundary_tests::run_started(1, "r").record)
            .chain(std::iter::once(setup_record(None)))
            .chain(records)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let refolded = replayed.compaction.clone();
    let resumed = refolded.state().unwrap();
    live.applied.as_mut().unwrap().thinking_prefix_len =
        resumed.applied.as_ref().unwrap().thinking_prefix_len;
    let live_view = model_options(&options, &live).unwrap();
    let resumed_view = model_options(&options, resumed).unwrap();
    assert_eq!(
        crate::canonical_request_bytes(&resumed_view),
        crate::canonical_request_bytes(&live_view)
    );
    assert_eq!(
        live_view.prompt[0].content,
        text(Role::User, "summary").content
    );
    let reasoning: Vec<_> = live_view
        .prompt
        .iter()
        .filter(|m| m.content.iter().any(is_prefix_bound))
        .map(|m| m.ordinal)
        .collect();
    assert_eq!(
        reasoning,
        vec![Some(6)],
        "only the later turn keeps reasoning"
    );
    // Without the view the transcript renders as before: the record is what changes the bytes.
    assert_ne!(
        crate::canonical_request_bytes(&model_options(&options, &after_setup()).unwrap()),
        crate::canonical_request_bytes(&live_view)
    );
    // The view survives an admission checkpoint too.
    let encoded = serde_json::to_string(&refolded).unwrap();
    assert_eq!(
        serde_json::from_str::<CompactionCheckpoint>(&encoded).unwrap(),
        refolded
    );
}

fn facts(lineage_id: &str) -> NoticeFacts<'_> {
    NoticeFacts {
        session_ref: SESSION_REF.into(),
        lineage_id,
        est_tokens: 100,
        model: "claude",
        window: Some(200_000),
        usage: Some(Usage {
            input_tokens: Some(10),
            cached_input_tokens: Some(20),
            cache_write_tokens: Some(30),
            output_tokens: Some(40),
            ..Default::default()
        }),
        events: Vec::new(),
        seq_floor: 1,
    }
}

fn record(state: &mut CompactionState, notice: &AdvanceNotice) {
    state.fold(&WalRecord::CompactionNotice {
        seq: notice.seq,
        headers: notice.to_headers(),
    });
}

/// Notices number from 1, announce the newest message by ordinal and content digest, report
/// the applied view and its companion edit, and say nothing when nothing changed. A session's
/// new lineage starts with no compaction state: its first notice claims no parent lineage and
/// names no applied view, and a view of the old lineage is never named.
#[tokio::test]
async fn notices_follow_the_durable_state() {
    let prompt = conversation();
    let mut state = after_setup();
    let first = next_notice(&state, &prompt, facts(LINEAGE)).unwrap();
    first.check().unwrap();
    let headers = first.to_headers();
    assert_eq!(headers["seq"], "1");
    assert_eq!(headers["through_ordinal"], "5");
    assert_eq!(
        headers["through_digest"],
        content_digest(&serde_json::to_value(&prompt[5]).unwrap())
    );
    assert_eq!(headers["last_usage"], "10:20:30:40");
    assert!(!headers.contains_key("applied_view_version"));
    assert!(!headers.contains_key("descends_from"));
    assert_eq!(
        first.message_id(),
        format!("{SESSION_REF}:advanced:1"),
        "the bus deduplicates a republished notice by this id"
    );
    record(&mut state, &first);
    assert!(next_notice(&state, &prompt, facts(LINEAGE)).is_none());

    let shared = Shared::default();
    let (view, ready) = make_view(
        2,
        5,
        vec![
            ViewRange::replace(0, 0, replace_bytes(&[text(Role::User, "summary")])),
            ViewRange::keep(0, 6),
        ],
    );
    apply(&shared, &mut state, &view, &ready, &prompt).await;
    let second = next_notice(&state, &prompt, facts(LINEAGE)).unwrap();
    let headers = second.to_headers();
    assert_eq!(headers["seq"], "2");
    assert_eq!(headers["applied_view_version"], "2");
    assert_eq!(headers["events"], "view_companion:2:strip_signed_reasoning");
    record(&mut state, &second);
    let mut switched = facts(LINEAGE);
    switched.model = "other-model";
    let third = next_notice(&state, &prompt, switched).unwrap();
    assert_eq!(third.to_headers()["events"], "model_change");
    assert_eq!(third.to_headers()["applied_view_version"], "2");

    // A new lineage: nothing carries over, so nothing is claimed, but its notices number on
    // from the old lineage's: the bus and the provider key notices on the session, and a
    // number at or below one already seen is dropped as a duplicate. The floor is what the
    // old lineage's retention tombstone records, its next sequence number.
    let last_seq = third.seq;
    let fresh = after_setup();
    let mut on_new_lineage = facts("lineage-2");
    on_new_lineage.seq_floor = last_seq + 1;
    let notice = next_notice(&fresh, &prompt, on_new_lineage).unwrap();
    let headers = notice.to_headers();
    assert!(notice.seq > last_seq, "{} after {last_seq}", notice.seq);
    assert_eq!(headers["seq"], (last_seq + 1).to_string());
    assert!(!headers.contains_key("descends_from"));
    assert!(!headers.contains_key("applied_view_version"));
    assert_eq!(
        model_options(&CallOptions::new(prompt.clone()), &fresh)
            .unwrap()
            .prompt[0]
            .content,
        text(Role::User, "head").content,
        "the new lineage renders Setup's initial message"
    );
    // A view recorded for the old lineage is never named on the new one.
    assert_eq!(state.applied_view_version("lineage-2"), None);
}

/// A model provider's refusal states its context limit only in its own words; the limit is
/// read from those words and never guessed.
#[test]
fn overflow_limits_come_only_from_the_providers_text() {
    for (text, limit) in [
        (
            "prompt is too long: 215463 tokens > 200000 maximum",
            Some(200_000),
        ),
        (
            r#"{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 1000 tokens > 999 maximum"}}"#,
            Some(999),
        ),
        (
            "This model's maximum context length is 128000 tokens. However, your messages resulted in 130000 tokens.",
            Some(128_000),
        ),
        ("prompt is too long", None),
        ("prompt is too long: 215463 tokens", None),
        ("maximum context length is exceeded", None),
        ("Input is too long for requested model.", None),
        ("rate limited", None),
    ] {
        assert_eq!(reported_limit_tokens(text), limit, "{text}");
    }
    let ride = ceiling_ride(
        SESSION_REF,
        1000,
        Some("prompt is too long: 9 tokens > 8 maximum"),
    );
    ride.check().unwrap();
    assert_eq!(
        ride.ceiling_overflow()
            .and_then(|o| o.reported_limit_tokens),
        Some(8)
    );
    let unstated = ceiling_ride(SESSION_REF, 1000, Some("request too large"));
    assert!(unstated.ceiling_overflow().is_some());
    assert_eq!(
        unstated.ceiling_overflow().unwrap().reported_limit_tokens,
        None
    );
    assert_eq!(
        serde_json::to_value(&unstated).unwrap()["overflow"],
        json!({})
    );
    let counted = ceiling_ride(SESSION_REF, 1000, None);
    assert!(counted.ceiling_overflow().is_none());
    assert!(serde_json::to_value(&counted)
        .unwrap()
        .get("overflow")
        .is_none());
}

/// A request over the window is never sent, with or without a ceiling policy; a request at the
/// policy's ceiling pauses for a view; the end of the wait follows the policy.
#[test]
fn the_ceiling_refuses_before_sending_past_the_window() {
    let policy: CeilingPolicy =
        serde_json::from_value(json!({"at_share":0.8,"wait_ms":1000})).unwrap();
    let over = before_send(None, 1001, Some(1000));
    match &over {
        BeforeSend::Refuse(detail) => {
            assert_eq!((detail.window, detail.size), (1000, 1001));
            assert_eq!(detail.cause, "over_window");
        }
        other => panic!("sent past the window: {other:?}"),
    }
    assert_eq!(before_send(Some(&policy), 1001, Some(1000)), over);
    assert_eq!(
        before_send(Some(&policy), 800, Some(1000)),
        BeforeSend::Pause
    );
    assert_eq!(
        before_send(Some(&policy), 799, Some(1000)),
        BeforeSend::Send
    );
    assert_eq!(before_send(None, 999, Some(1000)), BeforeSend::Send);
    assert_eq!(before_send(Some(&policy), 10_000, None), BeforeSend::Send);
    assert_eq!(
        after_wait(&policy, 700, 1000, false, WaitEnd::TimedOut),
        Ok(())
    );
    assert_eq!(
        after_wait(&policy, 700, 1000, true, WaitEnd::TimedOut)
            .unwrap_err()
            .cause,
        "view_timeout"
    );
    assert_eq!(
        after_wait(&policy, 700, 1000, true, WaitEnd::ViewApplied),
        Ok(())
    );
    assert_eq!(
        after_wait(&policy, 900, 1000, false, WaitEnd::ProviderUnavailable)
            .unwrap_err()
            .cause,
        "provider_unavailable"
    );
    assert_eq!(
        after_wait(&policy, 1100, 1000, false, WaitEnd::ViewApplied)
            .unwrap_err()
            .cause,
        "over_window"
    );
}

/// A notice is recorded and durable before `record_notice` returns, so nothing can publish it
/// (the publisher projects only durable WAL ranges) before a crash could lose it. A notice the
/// role would refuse is not recorded at all.
#[tokio::test]
async fn a_recorded_notice_is_durable_before_it_returns() {
    let shared = Shared::default();
    let writer = shared.writer();
    let control = crate::RunControl::default();
    let bind = bind();
    let runtime = Runtime {
        provider: "provider",
        bind: &bind,
        plane: None,
        writer: &writer,
        control: &control,
    };
    let mut state = after_setup();
    let notice = next_notice(&state, &conversation(), facts(LINEAGE)).unwrap();
    runtime
        .record_notice(&mut state, notice.clone())
        .await
        .unwrap();
    let records = shared.records();
    assert_eq!(shared.durable(), records.len());
    assert!(matches!(
        records.as_slice(),
        [WalRecord::CompactionNotice { seq: 1, headers }] if *headers == notice.to_headers()
    ));
    assert_eq!(state.notice_seq, 1);
    let mut malformed = notice;
    malformed.seq = 0;
    runtime.record_notice(&mut state, malformed).await.unwrap();
    assert_eq!(shared.records().len(), 1);
    writer.shutdown().await.unwrap();
}

/// `prompt` with runner tags rendered the way the run loop renders them into the model-facing
/// prompt: every text part, and every tool result's text, prefixed with its message's ordinal.
/// The raw transcript (and what `session.read` returns) keeps the unprefixed text.
fn tagged(prompt: &[Message]) -> Vec<Message> {
    let mut tagged = prompt.to_vec();
    for message in &mut tagged {
        let tag = format!("§{}§ ", message.ordinal.unwrap());
        for block in &mut message.content {
            match block {
                ContentBlock::Text { text } => text.insert_str(0, &tag),
                ContentBlock::ToolResult(result) => {
                    if let broca_protocol::ToolOutput::Text { text } = &mut result.output {
                        text.insert_str(0, &tag);
                    }
                }
                _ => {}
            }
        }
    }
    tagged
}

/// A session whose Setup inserted nothing, with Setup's thinking boundary at its first
/// message, as the run loop records Setup at the first boundary of a one-message transcript.
fn after_empty_setup() -> CompactionState {
    let mut state = after_setup();
    let applied = state.applied.as_mut().unwrap();
    applied.replacement = std::sync::Arc::new(Vec::new());
    applied.thinking_prefix_len = 1;
    state
}

/// A view's own bytes are judged against the stored transcript a provider copies from
/// (`session.read`'s untagged text), and sent exactly as given (compaction-provider CONTRACT.md
/// §6: the runner adds nothing inside replaced bytes). So a verbatim copy of a stored, signed
/// assistant message on a tag-rendering session is accepted, not refused; its bytes differ from
/// the tagged message the model was sent, so its reasoning, behind a changed prefix, is left out
/// with the companion strip. A copy whose signed content was edited is the provider's own
/// signed content and is refused.
#[tokio::test]
async fn a_verbatim_copy_of_a_stored_signed_message_is_accepted_and_an_edited_one_refused() {
    let raw = stamped(vec![
        text(Role::User, "q"),
        assistant(vec![
            signed("plan"),
            ContentBlock::Text {
                text: "hello".into(),
            },
        ]),
        text(Role::User, "q2"),
    ]);
    let prompt = tagged(&raw);
    let view_copying = |message: Message| {
        make_view(
            2,
            1,
            vec![ViewRange::replace(
                0,
                2,
                replace_bytes(&[text(Role::User, "summary"), message]),
            )],
        )
    };
    let mut edited = raw[1].clone();
    edited.content[0] = signed("rewritten");
    let (forged, forged_ready) = view_copying(edited);
    let shared = Shared::default();
    let mut state = after_setup();
    assert_eq!(
        apply_over(&shared, &mut state, &forged, &forged_ready, &prompt, &raw).await,
        Decision::Refuse(refusal_reasons::PROVIDER_CONSTRAINT)
    );
    assert!(shared.records().is_empty());
    assert_eq!(state.applied_view_version(LINEAGE), None);

    let (verbatim, verbatim_ready) = view_copying(raw[1].clone());
    assert_eq!(
        apply_over(
            &shared,
            &mut state,
            &verbatim,
            &verbatim_ready,
            &prompt,
            &raw
        )
        .await,
        Decision::Apply(CompactionViewChoice::Companion {
            edit: companion_edits::STRIP_SIGNED_REASONING.into(),
            strip: CompactionStrip {
                from_ordinal: 3,
                to_ordinal: 3
            }
        })
    );
    let rendered = model_options(&CallOptions::new(prompt.clone()), &state).unwrap();
    assert_eq!(
        rendered.prompt[0].content,
        text(Role::User, "summary").content
    );
    // The copy goes out as given: the stored text, without the prompt's tag, and without the
    // reasoning whose prefix changed.
    assert_eq!(
        rendered.prompt[1].content,
        vec![ContentBlock::Text {
            text: "hello".into()
        }]
    );
    assert_eq!(rendered.prompt[1].ordinal, None);
}

/// The first difference is found in the bytes the model is sent. A view replacing a
/// tagged message with its raw text changes the request at that message, so the signed
/// reasoning after it is invalidated and stripped, not sent over a changed prefix.
#[tokio::test]
async fn a_change_only_in_rendered_bytes_still_strips_the_reasoning_after_it() {
    let raw = stamped(vec![
        text(Role::User, "q"),
        assistant(vec![signed("a"), ContentBlock::Text { text: "x".into() }]),
        text(Role::User, "q2"),
        assistant(vec![signed("b"), ContentBlock::Text { text: "y".into() }]),
        text(Role::User, "q3"),
    ]);
    let prompt = tagged(&raw);
    let (view, ready) = make_view(
        2,
        4,
        vec![
            ViewRange::replace(0, 1, replace_bytes(&[raw[0].clone()])),
            ViewRange::keep(1, 5),
        ],
    );
    let shared = Shared::default();
    let mut state = after_empty_setup();
    assert_eq!(
        apply_over(&shared, &mut state, &view, &ready, &prompt, &raw).await,
        Decision::Apply(CompactionViewChoice::Companion {
            edit: companion_edits::STRIP_SIGNED_REASONING.into(),
            strip: CompactionStrip {
                from_ordinal: 1,
                to_ordinal: 5
            }
        })
    );
    let rendered = model_options(&CallOptions::new(prompt.clone()), &state).unwrap();
    assert_eq!(rendered.prompt[0].content, raw[0].content);
    assert!(rendered
        .prompt
        .iter()
        .all(|message| !message.content.iter().any(is_prefix_bound)));
}

/// User text after the tool results (a steer delivered at the boundary) does not close the
/// open tool turn. A view that would invalidate that turn's reasoning is deferred, not applied
/// with a strip of the reasoning the continuation needs: a steer continues the running run, so
/// it does not end a deferral (compaction-provider §7).
#[test]
fn a_steer_after_the_tool_results_keeps_the_turn_open() {
    let prompt = stamped(vec![
        text(Role::User, "q"),
        assistant(vec![signed("plan"), call("a")]),
        Message::new(Role::Tool, vec![result("a")]),
        text(Role::User, "steer"),
    ]);
    let (view, ready) = make_view(
        2,
        3,
        vec![
            ViewRange::replace(0, 0, replace_bytes(&[text(Role::User, "summary")])),
            ViewRange::keep(0, 4),
        ],
    );
    assert_eq!(
        decide(
            &view,
            &ready,
            &after_setup(),
            LINEAGE,
            &prompt,
            &prompt,
            true
        ),
        Decision::Defer(defer_reasons::SIGNED_REASONING)
    );
}

/// broca's Setup lists no view `apply` marks, so a view carrying one (even `next_step`, which
/// this crate defines) is refused `unsupported_apply` (compaction-provider §7), never applied;
/// the same view unmarked applies.
#[test]
fn a_view_with_an_apply_mark_broca_did_not_list_is_refused() {
    let summary = replace_bytes(&[text(Role::User, "summary")]);
    let (view, _) = make_view(
        2,
        5,
        vec![ViewRange::replace(0, 0, summary), ViewRange::keep(0, 6)],
    );
    let prompt = conversation();
    let state = after_setup();
    let marked = view
        .clone()
        .with_apply(crate::compaction_provider::view::apply_marks::NEXT_STEP);
    let marked_ready = ViewReady::for_view(&marked);
    let refused = decide(
        &marked,
        &marked_ready,
        &state,
        LINEAGE,
        &prompt,
        &prompt,
        false,
    );
    assert_eq!(
        refused,
        Decision::Refuse(refusal_reasons::UNSUPPORTED_APPLY)
    );
    assert_eq!(
        refused.event(2).unwrap().token(),
        "view_refused:2:unsupported_apply"
    );
    let ready = ViewReady::for_view(&view);
    assert_eq!(
        decide(&view, &ready, &state, LINEAGE, &prompt, &prompt, false),
        Decision::Apply(CompactionViewChoice::Applied)
    );
}

/// A view with a `cut` would start a new lineage once it applied (compaction-provider 0.5.0 §7,
/// llm-runner 0.5.0 §4.5), which broca does not serve: it refuses the view `cut_unsupported`
/// and keeps its one lineage starting at ordinal 0. The same view without the cut applies.
#[test]
fn a_view_with_a_cut_is_refused_and_the_same_view_without_it_applies() {
    let summary = replace_bytes(&[text(Role::User, "summary")]);
    let (view, _) = make_view(
        2,
        5,
        vec![ViewRange::replace(0, 1, summary), ViewRange::keep(1, 6)],
    );
    let prompt = conversation();
    let state = after_setup();
    let ready = ViewReady::for_view(&view);
    assert_eq!(
        decide(&view, &ready, &state, LINEAGE, &prompt, &prompt, false),
        Decision::Apply(CompactionViewChoice::Applied)
    );
    let cut = view
        .clone()
        .with_cut(crate::compaction_provider::view::ViewCut::new("cut-1", 0));
    assert_eq!(cut.check(), Ok(()), "the cut itself is well formed");
    let cut_ready = ViewReady::for_view(&cut);
    let refused = decide(&cut, &cut_ready, &state, LINEAGE, &prompt, &prompt, false);
    assert_eq!(refused, Decision::Refuse(CUT_UNSUPPORTED));
    assert_eq!(
        refused.event(2).unwrap().token(),
        "view_refused:2:cut_unsupported"
    );
}

/// A view deferred over the open tool turn when a run ends stays deferred at no later step: at
/// the first step of the session's next run it is refused for the provider's constraint
/// (compaction-provider §7), whatever the request ends with, and the same request inside a run
/// still defers it.
#[test]
fn a_deferral_pending_at_a_run_end_ends_at_the_next_runs_first_step() {
    let summary = replace_bytes(&[text(Role::User, "summary")]);
    let (view, ready) = make_view(
        2,
        5,
        vec![ViewRange::replace(0, 0, summary), ViewRange::keep(0, 6)],
    );
    let state = after_setup();
    // The run ended after the open turn's results, its view deferred.
    let mut ended = conversation();
    ended.truncate(4);
    ended.push(assistant(vec![signed("answer"), call("b")]));
    ended.push(Message::new(Role::Tool, vec![result("b")]));
    let ended = stamped(ended);
    assert_eq!(
        decide_at(&view, &ready, &state, LINEAGE, &ended, &ended, true, false),
        Decision::Defer(defer_reasons::SIGNED_REASONING)
    );
    // The next run's prompt joins the transcript; its first step ends the deferral.
    let mut next_run = ended.clone();
    next_run.push(text(Role::User, "next prompt"));
    let next_run = stamped(next_run);
    let refused = decide_at(
        &view, &ready, &state, LINEAGE, &next_run, &next_run, true, true,
    );
    assert_eq!(
        refused,
        Decision::Refuse(refusal_reasons::PROVIDER_CONSTRAINT)
    );
    assert_eq!(
        refused.event(2).unwrap().token(),
        "view_refused:2:provider_constraint"
    );
    // The same request at a later step of a run (a steer, say) keeps deferring.
    assert_eq!(
        decide_at(&view, &ready, &state, LINEAGE, &next_run, &next_run, true, false),
        Decision::Defer(defer_reasons::SIGNED_REASONING)
    );
}

/// The open tool turn copied into a replace range loses its reasoning in the request
/// (the renderer leaves out signed reasoning on every message the provider wrote), so the view
/// is deferred, not applied.
#[test]
fn an_open_turn_copied_into_a_replace_range_is_deferred() {
    let prompt = stamped(vec![
        text(Role::User, "q"),
        assistant(vec![signed("plan"), call("a")]),
        Message::new(Role::Tool, vec![result("a")]),
    ]);
    let (view, ready) = make_view(
        2,
        2,
        vec![
            ViewRange::replace(
                0,
                2,
                replace_bytes(&[text(Role::User, "summary"), prompt[1].clone()]),
            ),
            ViewRange::keep(2, 3),
        ],
    );
    assert_eq!(
        decide(
            &view,
            &ready,
            &after_setup(),
            LINEAGE,
            &prompt,
            &prompt,
            true
        ),
        Decision::Defer(defer_reasons::SIGNED_REASONING)
    );
}

/// A recorded view that does not assemble over the prompt is an error to render, never a
/// silent fallback to Setup's head without its strips. A view that passed its checks always
/// assembles again, so this takes a record no check passed: a replacement outside broca's
/// message schema, as a corrupted or foreign WAL could hold.
#[test]
fn a_recorded_view_that_does_not_assemble_is_an_error_not_a_fallback() {
    let prompt = stamped(vec![text(Role::User, "q"), text(Role::User, "q2")]);
    let (view, _) = make_view(
        2,
        0,
        vec![ViewRange::replace(
            0,
            1,
            json!([{"role":"user","content":[{"type":"image","data":"x"}]}]).to_string(),
        )],
    );
    let mut state = after_empty_setup();
    state.fold(&WalRecord::CompactionViewApplied {
        provider: "provider".into(),
        view_version: 2,
        view: broca_wal::OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap()).unwrap(),
        choice: CompactionViewChoice::Applied,
    });
    let error = model_options(&CallOptions::new(prompt), &state).unwrap_err();
    assert_eq!(error.view_version, 2);
    assert!(matches!(error.problem, AssemblyProblem::Rejected(_)));
}

/// A request rides for a view only when the model differs from the one the newest notice
/// announced: never before the first notice, and never for the same model again.
#[test]
fn only_a_model_change_since_the_last_notice_rides() {
    let mut state = after_setup();
    assert_eq!(
        rewrite_ride_cause(&state, "model"),
        None,
        "nothing announced yet"
    );
    state.announced = Some(Announced {
        model: Some("model".into()),
        ..Default::default()
    });
    assert_eq!(rewrite_ride_cause(&state, "model"), None, "the same model");
    assert_eq!(
        rewrite_ride_cause(&state, "other"),
        Some(crate::compaction_provider::ride::RideCause::ModelChange)
    );
}
