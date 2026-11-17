//! A session's enrollment in the current compaction contract, at the store: the object holding
//! its decisions, the entries compaction reads through it, the fold that keeps it, and what
//! seals, reloads, reverts and damage do to it.

use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult};
use broca_protocol::ids::{MessageId, ToolCallId};
use broca_protocol::request::ModelHandle;
use futures::executor::block_on;

use crate::backend::{MemoryBackend, SessionAddr, SessionBackend};
use crate::fold::conversation::Conversation;
use crate::fold::operations::{FoldError, Operations};
use crate::head::SessionTriple;
use crate::keys::{Cipher, DeploymentKey};
use crate::record::{
    Assistant, CompactionEnrolled, EnrolledEntries, EnrolledEntry, EntryFacts, Input, Notice,
    Placement, Record, ResultAppend, RunEnd, RunStart, ToolReply, TranscriptReverted,
};
use crate::session::{RevertRefusal, Session, SessionError};

cipher_tests!(
    compaction_reads_the_earlier_messages_through_the_enrollment_and_reads_serve_them_as_stored,
    a_seal_through_the_enrollment_and_a_reload_serve_the_same_entries,
    a_damaged_enrollment_object_refuses_by_name,
    rides_joined_before_the_enrollment_keep_their_bytes_and_later_rides_stay_apart,
    a_revert_across_the_enrollment_refuses_and_a_later_one_records_its_high,
    a_revert_off_the_contract_records_no_high,
    a_seal_that_would_change_the_enrollment_refuses_before_it_commits,
);

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/project".into(),
        harness: "broca".into(),
        session: "s".into(),
    }
}

/// The test deployment key, creating sessions in mode `cipher`.
fn deployment(cipher: Cipher) -> DeploymentKey {
    DeploymentKey::new(1, &[5; 32]).creating(cipher)
}

fn addr() -> SessionAddr {
    SessionAddr("0123456789abcdef".into())
}

fn model() -> ModelHandle {
    ModelHandle::new("p", "m")
}

fn start(run: &str, contract: bool) -> Record {
    Record::RunStart(Box::new(RunStart {
        named_speaker: None,
        imported_ids: None,
        message_marks: None,
        run_id: run.into(),
        model: model(),
        setup: Vec::new(),
        renderer_version: 1,
        submission_id: None,
        send_id: Some(format!("send-{run}")),
        request_identity_digest: None,
        ts_ms: None,
        written_at_ms: None,
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: contract.then_some(2),
        auth_selection: None,
        run_choices: None,
        origin: None,
        principal: None,
        scope: None,
        hooks: None,
        keep_warm: None,
        on_restart: None,
        seeded: None,
        refusal_schedule: None,
    }))
}

fn end(run: &str) -> Record {
    Record::RunEnd(RunEnd {
        run_id: run.into(),
        reason: RunFinishReason::Completed,
        usage: None,
        ts_ms: None,
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    })
}

fn input(run: &str, message: Message) -> Record {
    Record::Input(Input {
        media: Vec::new(),
        run_id: run.into(),
        placement: Placement::RunInput,
        message: std::sync::Arc::new(message),
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        pruned: Vec::new(),
    })
}

fn text(role: Role, text: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: text.into() }])
}

fn answer(run: &str, content: Vec<ContentBlock>) -> Record {
    Record::Assistant(Assistant {
        run_id: run.into(),
        step_id: 1,
        provider_message_id: MessageId(format!("m-{run}")),
        message: std::sync::Arc::new(Message::assistant_from(content, model())),
        usage: None,
        written_at_ms: None,
        last_content_at_ms: None,
        budget_finalize: None,
        tags_v1: None,
        pruned: Vec::new(),
    })
}

/// Off-contract facts, as a run outside the contract records them: part numbers from 0, and the
/// message's own time, which `written_at` gives when it has one.
fn facts(ordinal: u64, mid: String, written_at: Option<u64>) -> Record {
    Record::EntryFacts(std::sync::Arc::new(EntryFacts {
        send_id: None,
        named_speaker: None,
        client_message_id: None,
        sender: None,
        marks: None,
        no_display_facts: None,
        reasoning_summaries: Vec::new(),
        rich_ui: None,
        runner_tag_base: None,
        legacy_view_seq: None,
        notices: Vec::new(),
        ordinal,
        written_at_ms: written_at,
        tag_base: 0,
        notice: None,
        usage: None,
        generated_under: None,
        native: true,
        render_tags: false,
        clean_reply: false,
        mid,
        run_id: None,
        created_at_ms: None,
        delivery_mode: None,
        delivery_tool_call_id: None,
    }))
}

/// Appends `records`, each placed message followed by its facts, and returns the ids given.
async fn place<B: SessionBackend>(
    session: &mut Session<B>,
    records: Vec<Record>,
    written_at: Option<u64>,
) -> Vec<String> {
    let mut batch = Vec::new();
    let mut mids = Vec::new();
    let mut ordinal = session.conversation.placed;
    for record in records {
        let places = matches!(record, Record::Input(_) | Record::Assistant(_));
        batch.push(record);
        if places {
            ordinal += 1;
            let mid = session.message_id(batch.len() as u64);
            mids.push(mid.clone());
            batch.push(facts(ordinal, mid, written_at));
        }
    }
    session.append(&batch, true).await.unwrap();
    mids
}

/// Two runs outside the contract: the first's input opens with the session's global system
/// message, then a user message; each run answers. Ordinals 1 (system), 2, 3, then 4, 5.
async fn two_runs<B: SessionBackend>(session: &mut Session<B>) -> Vec<String> {
    let mut mids = place(
        session,
        vec![
            start("r1", false),
            input("r1", text(Role::System, "be brief")),
            input("r1", text(Role::User, "first")),
        ],
        Some(100),
    )
    .await;
    mids.extend(
        place(
            session,
            vec![answer(
                "r1",
                vec![ContentBlock::Text { text: "one".into() }],
            )],
            None,
        )
        .await,
    );
    session.append(&[end("r1")], true).await.unwrap();
    mids.extend(
        place(
            session,
            vec![start("r2", false), input("r2", text(Role::User, "second"))],
            Some(300),
        )
        .await,
    );
    mids.extend(
        place(
            session,
            vec![answer(
                "r2",
                vec![ContentBlock::Text { text: "two".into() }],
            )],
            None,
        )
        .await,
    );
    session.append(&[end("r2")], true).await.unwrap();
    mids
}

/// The decisions an enrollment after [`two_runs`] makes: the system message suppressed, the rest
/// numbered from a floor of 7, each taking the newest earlier time where it has none of its own.
fn decisions() -> EnrolledEntries {
    let entry = |ordinal, tag_base, written_at_ms, suppressed| EnrolledEntry {
        ordinal,
        tag_base,
        written_at_ms,
        suppressed,
        notice: (ordinal == 4).then(|| Notice {
            sender: "occipital".into(),
            template: None,
        }),
    };
    EnrolledEntries {
        entries: vec![
            entry(1, 7, 100, true),
            entry(2, 7, 100, false),
            entry(3, 8, 100, false),
            entry(4, 9, 300, false),
            entry(5, 10, 300, false),
        ],
    }
}

/// Enrolls the session after [`two_runs`]: the enrolling run's start, its input, then the
/// enrollment naming the stored decisions, in one batch, as the run loop writes it.
async fn enroll<B: SessionBackend>(session: &mut Session<B>) -> String {
    let overlay = session.put_enrollment(&decisions()).await.unwrap();
    let mid = session.message_id(2);
    session
        .append(
            &[
                start("r3", true),
                input("r3", text(Role::User, "third")),
                facts(6, mid.clone(), Some(500)),
                Record::CompactionEnrolled(CompactionEnrolled {
                    through_ordinal: 5,
                    tag_floor: 7,
                    tag_high: 11,
                    overlay,
                }),
            ],
            true,
        )
        .await
        .unwrap();
    mid
}

/// `(ordinal, tag_base, written_at, notice sender)` of each entry.
fn served(entries: &[crate::session::StoredEntry]) -> Vec<(u64, u64, Option<u64>, Option<String>)> {
    entries
        .iter()
        .map(|entry| {
            let facts = entry.state.facts.as_ref().unwrap();
            (
                entry.ordinal,
                facts.tag_base,
                facts.written_at_ms,
                facts.notice.as_ref().map(|notice| notice.sender.clone()),
            )
        })
        .collect()
}

fn expected() -> Vec<(u64, u64, Option<u64>, Option<String>)> {
    vec![
        (1, 7, Some(100), None),
        (2, 7, Some(100), None),
        (3, 8, Some(100), None),
        (4, 9, Some(300), Some("occipital".into())),
        (5, 10, Some(300), None),
        (6, 0, Some(500), None),
    ]
}

/// Compaction reads every earlier message through the enrollment's decisions on every read path:
/// a page, a page split anywhere, and by id. The suppressed system message keeps its ordinal and
/// id, served with no content, so the ordinals have no gaps. What `session.read` reads
/// (`Session::entries`) is every message as stored.
fn compaction_reads_the_earlier_messages_through_the_enrollment_and_reads_serve_them_as_stored(
    cipher: Cipher,
) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let mids = two_runs(&mut session).await;
        let raw = session.entries(1, usize::MAX).await.unwrap();
        enroll(&mut session).await;
        let enrolled = session.operations.enrolled.clone().unwrap();
        assert_eq!(
            (
                enrolled.through_ordinal,
                enrolled.tag_floor,
                enrolled.tag_high,
                enrolled.run_id.as_str()
            ),
            (5, 7, 11, "r3")
        );
        assert_eq!(session.operations.tag_high_water, 11);
        assert!(session.operations.on_contract());

        let all = session.compaction_entries(1, usize::MAX).await.unwrap();
        assert_eq!(served(&all), expected());
        // The suppressed system message: a system message with nothing in it, under its own id.
        assert_eq!(all[0].message.role, Role::System);
        assert!(!all[0].message.turn_system);
        assert!(all[0].message.content.is_empty());
        assert_eq!(all[0].state.facts.as_ref().unwrap().mid, mids[0]);
        // Pages count every message, the suppressed one included, wherever they split.
        for limit in 1..=all.len() {
            let mut paged = Vec::new();
            let mut from = 1;
            loop {
                let page = session.compaction_entries(from, limit).await.unwrap();
                let Some(last) = page.last() else {
                    break;
                };
                from = last.ordinal + 1;
                paged.extend(page);
            }
            assert_eq!(paged, all, "pages of {limit}");
        }
        let first = session.compaction_entries(1, 1).await.unwrap();
        assert_eq!(served(&first), expected()[..1]);
        let next = session.compaction_entries(3, 2).await.unwrap();
        assert_eq!(served(&next), expected()[2..4]);
        // By id: the suppressed message is found as it's served, the others too.
        assert_eq!(
            session.entry_by_mid(&mids[0]).await.unwrap().as_ref(),
            Some(&all[0])
        );
        let by_id = session.entry_by_mid(&mids[3]).await.unwrap().unwrap();
        assert_eq!(served(&[by_id]), expected()[3..4]);
        let named = session
            .entries_by_mid(&[mids[0].clone(), mids[1].clone(), mids[4].clone()])
            .await
            .unwrap();
        assert_eq!(
            served(&named),
            vec![
                expected()[0].clone(),
                expected()[1].clone(),
                expected()[4].clone()
            ]
        );

        // What reads serve is untouched: every earlier message, with its own facts.
        let after = session.entries(1, raw.len()).await.unwrap();
        assert_eq!(after, raw);
    });
}

/// A seal through the enrollment point and a reload read the same entries back: the sealed
/// history is read a block at a time and overlaid alike, and the decisions are read from their
/// object, since only the fold names it once the enrollment record is sealed.
fn a_seal_through_the_enrollment_and_a_reload_serve_the_same_entries(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        two_runs(&mut session).await;
        enroll(&mut session).await;
        let before = session.compaction_entries(1, usize::MAX).await.unwrap();
        let cut = session
            .open_log_records()
            .await
            .unwrap()
            .into_iter()
            .find(|(_, record)| matches!(record, Record::Input(input) if input.run_id == "r3"))
            .map(|(seq, _)| seq)
            .unwrap();
        session.seal(Some(cut), 1_000).await.unwrap();
        assert_eq!(session.conversation.window_start(), 6);
        assert_eq!(
            session.compaction_entries(1, usize::MAX).await.unwrap(),
            before
        );
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(loaded.operations.enrolled, session.operations.enrolled);
        assert_eq!(loaded.operations.tag_high_water, 11);
        assert_eq!(
            loaded.compaction_entries(1, usize::MAX).await.unwrap(),
            before
        );
        assert_eq!(served(&before), expected());
    });
}

/// The decisions' object is damage when it's missing or unreadable: a read refuses by name,
/// never serves the earlier messages without their decisions.
fn a_damaged_enrollment_object_refuses_by_name(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        two_runs(&mut session).await;
        enroll(&mut session).await;
        let name = backend
            .object_names(&addr())
            .into_iter()
            .find(|name| name.ends_with("-compaction-enrollment"))
            .unwrap();
        backend.set_object(&addr(), &name, b"not an enrollment".to_vec());
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert!(matches!(
            loaded.compaction_entries(1, usize::MAX).await,
            Err(SessionError::EnrollmentDamaged(reason)) if reason.contains(&name)
        ));
        // What reads serve doesn't depend on it.
        assert_eq!(loaded.entries(1, usize::MAX).await.unwrap().len(), 6);
    });
}

/// A seal whose new log would hold a different enrollment than the session refuses before it
/// commits, and leaves the session as it was. Here the seal carries an enrollment record into
/// the new log, which would enroll the session there without the run that enrolls it.
fn a_seal_that_would_change_the_enrollment_refuses_before_it_commits(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        two_runs(&mut session).await;
        let overlay = session.put_enrollment(&decisions()).await.unwrap();
        let before = session.entries(1, usize::MAX).await.unwrap();
        let carried = [Record::CompactionEnrolled(CompactionEnrolled {
            through_ordinal: 5,
            tag_floor: 7,
            tag_high: 11,
            overlay,
        })];
        let carry = crate::session::Carry {
            records: &carried,
            ..Default::default()
        };
        assert!(matches!(
            session.seal_carrying(None, &carry, 1_000).await,
            Err(SessionError::EnrollmentNotCarried)
        ));
        assert!(session.operations.enrolled.is_none());
        assert!(
            session.operations.open_change.is_none(),
            "the seal was undone"
        );
        let mut loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert!(loaded.operations.enrolled.is_none());
        assert_eq!(loaded.head.open_log, session.head.open_log);
        assert_eq!(loaded.entries(1, usize::MAX).await.unwrap(), before);
        // A seal that keeps the enrollment as it is goes ahead. Loading fenced the first copy
        // out of the session's log, so the loaded copy seals.
        loaded.seal(None, 2_000).await.unwrap();
        assert!(loaded.operations.enrolled.is_none());
    });
}

/// The fold keeps one enrollment, made in a run's start batch, and the session's part-number
/// high-water, which only grows: an enrollment sets it, a revert's recorded high raises it.
#[test]
fn the_fold_keeps_one_enrollment_and_a_high_water_that_never_falls() {
    let mut operations = Operations::default();
    let enrolled = |tag_high| {
        Record::CompactionEnrolled(CompactionEnrolled {
            through_ordinal: 5,
            tag_floor: 7,
            tag_high,
            overlay: crate::record::EnrollmentOverlayRef {
                log: crate::codec::Id128([1; 16]),
                generation: crate::codec::Id128([2; 16]),
                seq: 9,
            },
        })
    };
    assert_eq!(
        operations.apply(&enrolled(11)),
        Err(FoldError::Enrollment("no run start precedes it"))
    );
    operations.apply(&start("r3", true)).unwrap();
    operations.apply(&enrolled(11)).unwrap();
    assert_eq!(operations.tag_high_water, 11);
    assert_eq!(
        operations.apply(&enrolled(12)),
        Err(FoldError::Enrollment("the session already enrolled"))
    );
    let reverted = |tag_high| {
        Record::TranscriptReverted(TranscriptReverted {
            change: crate::codec::Id128([3; 16]),
            from_seq: 20,
            through_seq: 30,
            ordinal: 8,
            history_version: 1,
            tag_high,
        })
    };
    operations.apply(&reverted(Some(40))).unwrap();
    assert_eq!(operations.tag_high_water, 40);
    operations.apply(&reverted(Some(20))).unwrap();
    operations.apply(&reverted(None)).unwrap();
    assert_eq!(operations.tag_high_water, 40);
}

fn call(id: &str) -> ContentBlock {
    ContentBlock::ToolCall(ToolCall {
        tool_call_id: ToolCallId(id.into()),
        tool_name: "read".into(),
        input: serde_json::json!({}).into(),
        thought_signature: None,
    })
}

fn reply(run: &str, id: &str) -> Record {
    Record::ToolReply(ToolReply {
        run_id: run.into(),
        batch_id: Some(format!("b-{id}")),
        result: ToolResult {
            tool_call_id: ToolCallId(id.into()),
            output: ToolOutput::Text {
                text: "contents".into(),
            },
            is_error: false,
        },
        finished_at_ms: None,
        tags_v1: None,
        media: Vec::new(),
    })
}

fn ride(run: &str, id: &str, text: &str) -> Record {
    Record::ResultAppend(ResultAppend {
        run_id: run.into(),
        tool_call_id: ToolCallId(id.into()),
        text: text.into(),
        notice: None,
        written_at_ms: None,
        send_id: None,
        ride: None,
    })
}

/// The text of the tool result `id` in `conversation`, and the text delivered beside it, which
/// the entry of the message that made the call keeps.
fn result_and_rides(conversation: &Conversation, id: &str) -> (String, Vec<String>) {
    let messages = conversation.held_messages().unwrap();
    let result = messages
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|block| match block {
            ContentBlock::ToolResult(result) if result.tool_call_id.0 == id => Some(result),
            _ => None,
        })
        .unwrap();
    let ToolOutput::Text { text } = &result.output else {
        panic!("a text result");
    };
    let rides = conversation
        .held_messages()
        .unwrap()
        .iter()
        .zip(&conversation.entries)
        .find(|(message, _)| {
            message.content.iter().any(
                |block| matches!(block, ContentBlock::ToolCall(call) if call.tool_call_id.0 == id),
            )
        })
        .map(|(_, entry)| {
            entry
                .delivered
                .iter()
                .map(|ride| ride.text.clone())
                .collect()
        })
        .unwrap();
    (text.to_string(), rides)
}

/// A session that joined delivered text into its results keeps the text joined into the results
/// it placed before it enrolled, and keeps text delivered with a later result apart from it, as
/// the contract does. A sealed block is folded the same way.
fn rides_joined_before_the_enrollment_keep_their_bytes_and_later_rides_stay_apart(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), true)
            .await
            .unwrap();
        place(
            &mut session,
            vec![start("r1", false), input("r1", text(Role::User, "look"))],
            Some(1),
        )
        .await;
        place(&mut session, vec![answer("r1", vec![call("c1")])], None).await;
        session.append(&[reply("r1", "c1")], true).await.unwrap();
        session
            .append(&[ride("r1", "c1", "\n\nearly")], true)
            .await
            .unwrap();
        place(
            &mut session,
            vec![answer(
                "r1",
                vec![ContentBlock::Text {
                    text: "seen".into(),
                }],
            )],
            None,
        )
        .await;
        session.append(&[end("r1")], true).await.unwrap();
        // The input, the call, its result, the answer.
        assert_eq!(session.conversation.placed, 4);

        let decided = EnrolledEntries {
            entries: (1..=4)
                .map(|ordinal| EnrolledEntry {
                    ordinal,
                    tag_base: 0,
                    written_at_ms: 1,
                    suppressed: false,
                    notice: None,
                })
                .collect(),
        };
        let overlay = session.put_enrollment(&decided).await.unwrap();
        place(
            &mut session,
            vec![start("r2", true), input("r2", text(Role::User, "again"))],
            Some(2),
        )
        .await;
        session
            .append(
                &[Record::CompactionEnrolled(CompactionEnrolled {
                    through_ordinal: 4,
                    tag_floor: 0,
                    tag_high: 3,
                    overlay,
                })],
                true,
            )
            .await
            .unwrap();
        assert_eq!(session.conversation.joined_through, Some(4));
        place(&mut session, vec![answer("r2", vec![call("c2")])], None).await;
        session.append(&[reply("r2", "c2")], true).await.unwrap();
        session
            .append(&[ride("r2", "c2", "\n\nlate")], true)
            .await
            .unwrap();

        assert_eq!(
            result_and_rides(&session.conversation, "c1"),
            ("contents\n\nearly".to_owned(), Vec::new())
        );
        assert_eq!(
            result_and_rides(&session.conversation, "c2"),
            ("contents".to_owned(), vec!["\n\nlate".to_owned()])
        );

        // Sealed, each block is folded from an empty conversation that joins alike.
        let before = session.compaction_entries(1, usize::MAX).await.unwrap();
        session.seal(None, 1_000).await.unwrap();
        let cut = session
            .open_log_records()
            .await
            .unwrap()
            .into_iter()
            .find(|(_, record)| matches!(record, Record::Assistant(assistant) if assistant.run_id == "r2"))
            .map(|(seq, _)| seq)
            .unwrap();
        session.seal(Some(cut), 2_000).await.unwrap();
        assert_eq!(
            session.compaction_entries(1, usize::MAX).await.unwrap(),
            before
        );
    });
}

/// A revert to a message placed before the enrollment is refused: its ordinal would be taken
/// again by a message numbered on the contract. A revert after it records the highest part
/// number before it, which the fold keeps; a revert must say it on the contract, and only there.
fn a_revert_across_the_enrollment_refuses_and_a_later_one_records_its_high(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let mids = two_runs(&mut session).await;
        let enrolled_input = enroll(&mut session).await;
        place(
            &mut session,
            vec![answer(
                "r3",
                vec![ContentBlock::Text {
                    text: "three".into(),
                }],
            )],
            None,
        )
        .await;
        session.append(&[end("r3")], true).await.unwrap();
        let resend = |text: &str| {
            vec![
                start("r9", true),
                input("r9", self::text(Role::User, text)),
                facts(0, String::new(), Some(900)),
            ]
        };
        let refused = |result: Result<_, SessionError>| match result {
            Err(SessionError::RevertRefused(refusal)) => refusal,
            other => panic!("expected a refusal, got {other:?}"),
        };
        assert_eq!(
            refused(session.revert(&mids[3], resend("x"), Some(20), 1).await),
            RevertRefusal::BeforeEnrollment
        );
        assert_eq!(
            refused(session.revert(&enrolled_input, resend("x"), None, 1).await),
            RevertRefusal::TagHigh
        );
        session
            .revert(&enrolled_input, resend("edited"), Some(20), 1_000)
            .await
            .unwrap();
        assert_eq!(session.operations.tag_high_water, 20);
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(loaded.operations.tag_high_water, 20);
        assert_eq!(loaded.operations.enrolled, session.operations.enrolled);
    });
}

/// Off the contract a revert records no high: there the part numbers aren't served.
fn a_revert_off_the_contract_records_no_high(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let mids = two_runs(&mut session).await;
        let resend = vec![
            start("r9", false),
            input("r9", text(Role::User, "edited")),
            facts(0, String::new(), Some(900)),
        ];
        let refused = session
            .revert(&mids[3], resend.clone(), Some(5), 1)
            .await
            .unwrap_err();
        assert!(matches!(
            refused,
            SessionError::RevertRefused(RevertRefusal::TagHigh)
        ));
        session.revert(&mids[3], resend, None, 1_000).await.unwrap();
        assert_eq!(session.operations.history_version, 1);
        assert_eq!(session.operations.tag_high_water, 0);
    });
}
