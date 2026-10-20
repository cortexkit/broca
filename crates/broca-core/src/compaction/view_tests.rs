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
            .offer_view(&mut live, &view, &ready, LINEAGE, &prompt, true)
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
        decide(&good, &ready, &state, LINEAGE, &prompt, true),
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
        decide(&good, &ready, &state, "lineage-2", &prompt, true),
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
    let companion = decide(&head, &head_ready, &state, LINEAGE, &prompt, true);
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
        decide(&head, &head_ready, &state, LINEAGE, &prompt, false),
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
    let deferred = decide(&head_open, &head_open_ready, &state, LINEAGE, &open, true);
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
        decide(&late, &late_ready, &bare, LINEAGE, &prompt, true),
        Decision::Apply(CompactionViewChoice::Applied)
    );
}

async fn apply(
    shared: &Shared,
    state: &mut CompactionState,
    view: &CompactionView,
    ready: &ViewReady,
    prompt: &[Message],
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
        .offer_view(state, view, ready, LINEAGE, prompt, true)
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
    let live_view = model_options(&options, &live);

    let mut refolded = CompactionCheckpoint::None;
    for record in std::iter::once(setup_record(None)).chain(records) {
        refolded.fold(&record);
    }
    let resumed = refolded.state().unwrap();
    let resumed_view = model_options(&options, resumed);
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
        crate::canonical_request_bytes(&model_options(&options, &after_setup())),
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

    // A new lineage: nothing carries over, so nothing is claimed.
    let fresh = after_setup();
    let notice = next_notice(&fresh, &prompt, facts("lineage-2")).unwrap();
    let headers = notice.to_headers();
    assert_eq!(headers["seq"], "1");
    assert!(!headers.contains_key("descends_from"));
    assert!(!headers.contains_key("applied_view_version"));
    assert_eq!(
        model_options(&CallOptions::new(prompt.clone()), &fresh).prompt[0].content,
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
