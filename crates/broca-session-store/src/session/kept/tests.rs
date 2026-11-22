//! The records a session keeps in memory, reads from memory only, syncing an unsynced tail, and
//! views loaded at a position.

use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::ids::{MessageId, ToolCallId};
use broca_protocol::request::ModelHandle;
use futures::executor::block_on;

use crate::backend::{MemoryBackend, SessionAddr, SessionBackend};
use crate::codec::{Digest, Message as _};
use crate::head::SessionTriple;
use crate::idindex::{self, IdKind};
use crate::keys::{Cipher, DeploymentKey};
use crate::record::{
    Assistant, BatchEnd, BatchStart, CallOutcome, EntryFacts, Event, Input, Placement, QueuedSend,
    Record, RunEnd, RunStart, StepEnd, StepStart, ToolIntent, ToolOutcome, ToolReply,
};
use crate::session::{Session, SessionError};

cipher_tests!(
    a_writer_keeps_its_latest_runs_records,
    a_view_keeps_every_record_it_read,
    a_sealed_views_open_log_reads_back_only_its_checkpoint_frame,
    a_seal_drops_the_kept_records_and_memory_only_refuses_sealed_history,
    a_sync_makes_an_unsynced_tail_durable_once,
    a_failed_sync_leaves_the_tail_unsynced,
    a_view_at_a_position_reads_only_that_far,
    the_kept_buffer_is_sized_for_what_it_keeps,
    an_id_lookup_reads_the_log_only_outside_the_latest_run,
    a_queued_sends_records_stay_in_memory_after_its_run_starts,
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
    serde_json::from_value(serde_json::json!({"provider_module_id": "p", "model_id": "m"})).unwrap()
}

fn start(run: &str) -> Record {
    Record::RunStart(Box::new(RunStart {
        named_speaker: None,
        kept_notice: None,
        imported_ids: None,
        message_marks: None,
        run_id: run.into(),
        model: model(),
        setup: Vec::new(),
        renderer_version: 1,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        ts_ms: None,
        written_at_ms: None,
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: None,
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

fn input(run: &str, text: &str) -> Record {
    Record::Input(Input {
        media: Vec::new(),
        run_id: run.into(),
        placement: Placement::RunInput,
        message: std::sync::Arc::new(Message::new(
            Role::User,
            vec![ContentBlock::Text { text: text.into() }],
        )),
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        pruned: Vec::new(),
    })
}

fn event(name: &str) -> Record {
    Record::Event(Event {
        name: name.into(),
        run_id: None,
        body: None,
    })
}

/// Two finished runs, then a third still running. Returns the session and the seq of each run's
/// start; the last run's input, where a cut can fall, follows its start.
async fn history<B: SessionBackend>(backend: &B, cipher: Cipher) -> (Session<&B>, Vec<u64>) {
    let mut session = Session::create(backend, addr(), triple(), &deployment(cipher), false)
        .await
        .unwrap();
    let mut starts = Vec::new();
    for run in ["a", "b", "c"] {
        starts.push(session.append(&[start(run)], true).await.unwrap());
        session
            .append(&[input(run, &format!("ask {run}")), event(run)], true)
            .await
            .unwrap();
        if run != "c" {
            session.append(&[end(run)], true).await.unwrap();
        }
    }
    (session, starts)
}

async fn view<B: SessionBackend>(backend: B, cipher: Cipher) -> crate::session::SessionView<B> {
    Session::view(backend, addr(), &triple(), &deployment(cipher))
        .await
        .unwrap()
}

/// A writer keeps the records of its latest run, from the run's start on: reading them from
/// memory only gives exactly what a fresh view reads from storage, and a run before it reads
/// from storage, which memory only refuses.
fn a_writer_keeps_its_latest_runs_records(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (session, starts) = history(&backend, cipher).await;
        assert_eq!(session.kept_run_start("c"), Some(starts[2]));
        assert_eq!(session.kept_run_start("b"), None);
        let fresh = view(&backend, cipher).await;
        let from_storage = fresh.records_from(starts[2]).await.unwrap();
        let in_memory = {
            let _memory = session.in_memory();
            session.records_from(starts[2]).await.unwrap()
        };
        assert_eq!(in_memory, from_storage);
        assert_eq!(in_memory.len(), 3);
        {
            let _memory = session.in_memory();
            assert!(matches!(
                session.records_between(starts[1], starts[2]).await,
                Err(SessionError::NotInMemory)
            ));
        }
        // Outside it, the writer reads the log as before.
        assert_eq!(
            session
                .records_between(starts[1], starts[2] - 1)
                .await
                .unwrap(),
            fresh
                .records_between(starts[1], starts[2] - 1)
                .await
                .unwrap()
        );
    });
}

/// A view keeps every record it read, so even reading from memory only it serves any run's
/// records, and never reads the log again. Serving its open log whole reads back only the log's
/// checkpoint frame, which the view doesn't hold, so reading from memory only refuses that.
fn a_view_keeps_every_record_it_read(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (session, starts) = history(&backend, cipher).await;
        let fresh = view(&backend, cipher).await;
        assert_eq!(fresh.kept_run_start("c"), Some(starts[2]));
        let expected = fresh.records_from(1).await.unwrap();
        // A record appended after the view was loaded is not part of it.
        let mut session = session;
        session.append(&[event("later")], true).await.unwrap();
        {
            let _memory = fresh.in_memory();
            assert_eq!(fresh.records_from(1).await.unwrap(), expected);
            assert_eq!(fresh.open_log_records().await.unwrap(), expected);
            assert!(matches!(
                fresh.from_log_holding(fresh.next_seq(), u64::MAX).await,
                Err(SessionError::NotInMemory)
            ));
        }
        let reads = (backend.log_reads(), backend.checkpoint_frame_reads());
        let loaded = fresh
            .from_log_holding(fresh.next_seq(), u64::MAX)
            .await
            .unwrap();
        assert_eq!(loaded.checkpoint.covers_through, 0);
        assert_eq!(loaded.records, expected);
        assert_eq!(
            (backend.log_reads(), backend.checkpoint_frame_reads()),
            (reads.0, reads.1 + 1),
            "one read of the checkpoint frame, none of the log"
        );
    });
}

/// A sealed session's open log starts from a checkpoint that carries the sealed history's state.
/// A view serves that log whole from the records it kept and the checkpoint read back from the
/// log's first frame: one read of that frame and none of the log, giving the log a read from
/// storage gives.
fn a_sealed_views_open_log_reads_back_only_its_checkpoint_frame(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, _) = history(&backend, cipher).await;
        // The checkpoint the session was created with, before any history was sealed.
        let created = view(&backend, cipher)
            .await
            .from_log_holding(session.next_seq(), u64::MAX)
            .await
            .unwrap()
            .checkpoint
            .encode();
        session.seal(None, 1_000).await.unwrap();
        session.append(&[end("c")], true).await.unwrap();
        session
            .append(&[start("d"), input("d", "ask d")], true)
            .await
            .unwrap();
        let fresh = view(&backend, cipher).await;
        let seq = fresh.next_seq();
        let stored = fresh
            .stored_records(&backend)
            .from_log_holding(seq, u64::MAX)
            .await
            .unwrap();
        assert!(stored.checkpoint.covers_through > 0);
        assert!(stored.checkpoint.encode().len() > created.len());
        assert!(!stored.records.is_empty());
        let reads = (backend.log_reads(), backend.checkpoint_frame_reads());
        let loaded = fresh.from_log_holding(seq, u64::MAX).await.unwrap();
        assert_eq!(
            (backend.log_reads(), backend.checkpoint_frame_reads()),
            (reads.0, reads.1 + 1),
            "one read of the checkpoint frame, none of the log"
        );
        assert_eq!(loaded.checkpoint.encode(), stored.checkpoint.encode());
        assert_eq!(loaded.records, stored.records);
    });
}

/// After a seal the sealed history is read from storage: reading from memory only refuses it, and
/// the records kept before the seal are dropped, since the new log was copied, not appended. A run
/// started after the seal is kept again.
fn a_seal_drops_the_kept_records_and_memory_only_refuses_sealed_history(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, starts) = history(&backend, cipher).await;
        session.seal(Some(starts[2] + 1), 1_000).await.unwrap();
        assert_eq!(session.kept_records(), 0);
        assert_eq!(session.kept_run_start("c"), None);
        {
            let _memory = session.in_memory();
            assert!(matches!(
                session.entries(1, 1).await,
                Err(SessionError::NotInMemory)
            ));
            assert!(matches!(
                session.find_id(IdKind::RunId, "a").await,
                Err(SessionError::NotInMemory)
            ));
            // The window is in memory.
            let window = session.conversation.window_start();
            assert_eq!(session.entries(window, 1).await.unwrap().len(), 1);
        }
        // The guard is gone: storage is read again.
        assert_eq!(session.entries(1, 1).await.unwrap().len(), 1);

        session.append(&[end("c")], true).await.unwrap();
        let d = session.append(&[start("d")], true).await.unwrap();
        session.append(&[input("d", "ask d")], true).await.unwrap();
        assert_eq!(session.kept_run_start("d"), Some(d));
        let in_memory = {
            let _memory = session.in_memory();
            session.records_from(d).await.unwrap()
        };
        assert_eq!(
            in_memory,
            view(&backend, cipher).await.records_from(d).await.unwrap()
        );
        assert_eq!(in_memory.len(), 2);
    });
}

/// A batch appended without a barrier leaves the log unsynced until a sync writes a claim frame
/// with one; a second sync with nothing new writes nothing.
fn a_sync_makes_an_unsynced_tail_durable_once(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, _) = history(&backend, cipher).await;
        assert!(!session.unsynced());
        session.append(&[event("unsynced")], false).await.unwrap();
        assert!(session.unsynced());
        session.sync().await.unwrap();
        assert!(!session.unsynced());
        let (_, synced_len) = session.open_log();
        session.sync().await.unwrap();
        assert_eq!(
            session.open_log().1,
            synced_len,
            "nothing to sync, nothing written"
        );
        // The log still loads, with the record the sync made durable.
        let loaded = view(&backend, cipher).await;
        assert!(loaded
            .open_log_records()
            .await
            .unwrap()
            .iter()
            .any(|(_, record)| matches!(record, Record::Event(e) if e.name == "unsynced")));
    });
}

/// A sync that fails leaves the tail unsynced and the writer poisoned: nothing is answered from
/// the session as though it were durable.
fn a_failed_sync_leaves_the_tail_unsynced(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, _) = history(&backend, cipher).await;
        session.append(&[event("unsynced")], false).await.unwrap();
        backend.fail_once(0);
        assert!(session.sync().await.is_err());
        assert!(session.unsynced());
        assert!(matches!(session.sync().await, Err(SessionError::Poisoned)));
    });
}

/// A view loaded at a position reads the log only as far as the position: what was appended since
/// isn't in it. Once the head moves, as a seal moves it, the position refuses.
fn a_view_at_a_position_reads_only_that_far(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, starts) = history(&backend, cipher).await;
        let position = Session::stored_position(&&backend, &addr(), &deployment(cipher))
            .await
            .unwrap()
            .unwrap();
        let before = view(&backend, cipher).await.records_from(1).await.unwrap();
        session.append(&[event("after")], true).await.unwrap();
        let at = Session::view_at(&backend, addr(), &triple(), &deployment(cipher), &position)
            .await
            .unwrap();
        assert_eq!(at.records_from(1).await.unwrap(), before);
        let now = Session::stored_position(&&backend, &addr(), &deployment(cipher))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(now.version, position.version);
        assert!(now.len > position.len);

        session.seal(Some(starts[2] + 1), 1_000).await.unwrap();
        assert!(matches!(
            Session::view_at(&backend, addr(), &triple(), &deployment(cipher), &now).await,
            Err(SessionError::Moved)
        ));
    });
}

/// Whether the kept records' buffer holds little more room than records: what a buffer that
/// grows by a quarter of its length when full can hold, and far less than one that held a
/// longer run before.
fn snug(records: &[(u64, Record)], capacity: usize) -> bool {
    capacity <= records.len() + records.len() / 4 + 4
}

/// The kept records' buffer is sized for what it keeps. A writer loading a log whose latest run
/// is short, after a long one, keeps the short run's records in a buffer about their size, not
/// one that held the whole log; and a new run start drops the buffer that held a long run with
/// its records, rather than keep its room for the new run.
fn the_kept_buffer_is_sized_for_what_it_keeps(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let mut ordinal = 0;
        tool_run(&mut session, "long", 40, &mut ordinal, None, None, true).await;
        tool_run(&mut session, "short", 1, &mut ordinal, None, None, false).await;
        let logged = session.next_seq();
        drop(session);

        let mut loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        let kept = &loaded.kept.records;
        assert_eq!(
            kept.first().map(|(seq, _)| *seq),
            loaded.kept_run_start("short")
        );
        assert!(
            snug(kept, kept.capacity()),
            "after a load the buffer has room for {} records, it keeps {} of the log's {logged}",
            kept.capacity(),
            kept.len()
        );

        loaded.append(&[end("short")], true).await.unwrap();
        tool_run(&mut loaded, "again", 40, &mut ordinal, None, None, true).await;
        let long = loaded.kept.records.len();
        assert!(long > 400, "the long run is kept whole: {long} records");
        loaded.append(&[start("next")], true).await.unwrap();
        let kept = &loaded.kept.records;
        assert_eq!(kept.len(), 1);
        assert!(
            snug(kept, kept.capacity()),
            "after a run start the buffer has room for {} records, it keeps {}, its run before \
             held {long}",
            kept.capacity(),
            kept.len()
        );
    });
}

/// How many records id lookups on this thread read back from an open log, and how many reads
/// `backend` served of any open log.
fn reads(backend: &MemoryBackend) -> (u64, usize) {
    (idindex::open_record_reads(), backend.log_reads())
}

/// The open log's ids are found from memory for the latest run, and for every record not written
/// per tool call (a run's start and end, a send's records) in any run; a tool call's records
/// before the latest run are found by one read of the open log each, which reading from memory
/// only refuses. Each answer is the one a view, which keeps every record, gives.
fn an_id_lookup_reads_the_log_only_outside_the_latest_run(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        // r0 and r1 ended, r2 runs; r0 queued the send r1 started from, r1 the one r2 did.
        let (session, _) = tool_history(&backend, cipher, 3, 3).await;
        let fresh = view(&backend, cipher).await;
        let latest_call = call_key("r2", 2);
        let before = reads(&backend);
        for (kind, id) in [
            (IdKind::RunId, "r2"),
            (IdKind::RunId, "r0"),
            (IdKind::CallKey, latest_call.as_str()),
            (IdKind::SubmissionId, "sub-r1"),
            (IdKind::SubmissionId, "sub-r2"),
            (IdKind::SendId, "send-sub-r1"),
        ] {
            let found = {
                let _memory = session.in_memory();
                session.find_id(kind, id).await.unwrap()
            };
            assert!(!found.is_empty(), "{kind:?} {id} names records");
            assert_eq!(
                found,
                fresh.find_id(kind, id).await.unwrap(),
                "{kind:?} {id}"
            );
        }
        for run in ["r0", "r1", "r2"] {
            let start = {
                let _memory = session.in_memory();
                session.find_run_start(run).await.unwrap()
            };
            assert!(start.is_some(), "{run} started");
            assert_eq!(start, fresh.find_run_start(run).await.unwrap(), "{run}");
        }
        assert_eq!(
            reads(&backend),
            before,
            "the latest run's ids, every run's records and a send's records are read from memory"
        );

        // A call of an earlier run: its intent is read back from the open log, one record.
        let earlier_call = call_key("r0", 2);
        {
            let _memory = session.in_memory();
            assert!(matches!(
                session.find_id(IdKind::CallKey, &earlier_call).await,
                Err(SessionError::NotInMemory)
            ));
        }
        let before = reads(&backend);
        let found = session
            .find_id(IdKind::CallKey, &earlier_call)
            .await
            .unwrap();
        assert_eq!(reads(&backend), (before.0 + 1, before.1 + 1));
        assert_eq!(found.len(), 1);
        assert_eq!(
            found,
            fresh.find_id(IdKind::CallKey, &earlier_call).await.unwrap()
        );
        // An id no record names reads nothing.
        let before = reads(&backend);
        assert!(session
            .find_id(IdKind::CallKey, "missing")
            .await
            .unwrap()
            .is_empty());
        assert_eq!(reads(&backend), before);
    });
}

/// The records a poll of the send `sub-r1`'s status looks up, read from memory only: nothing is
/// read from storage, and the answer is a fresh view's.
async fn poll_reads_nothing(
    session: &Session<&MemoryBackend>,
    backend: &MemoryBackend,
    cipher: Cipher,
    when: &str,
) {
    let fresh = view(backend, cipher).await;
    let before = reads(backend);
    for (kind, id) in [
        (IdKind::SubmissionId, "sub-r1"),
        (IdKind::SendId, "send-sub-r1"),
    ] {
        let found = {
            let _memory = session.in_memory();
            session.find_id(kind, id).await
        };
        let found = found.unwrap_or_else(|error| panic!("{when}: {kind:?} {id}: {error:?}"));
        assert!(
            found
                .iter()
                .any(|(_, record)| matches!(record, Record::QueuedSend(_))),
            "{when}: {kind:?} {id} finds the queued send"
        );
        assert_eq!(
            found,
            fresh.find_id(kind, id).await.unwrap(),
            "{when}: {kind:?} {id}"
        );
    }
    assert_eq!(reads(backend), before, "{when}: read nothing from storage");
}

/// A send queued while a run runs has its records in that run. A poll of the send reads them
/// from memory only, without touching storage, during the run the send started, after a later
/// run started, and in a writer loaded after that.
fn a_queued_sends_records_stay_in_memory_after_its_run_starts(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        // r0 queued sub-r1, which started r1, still running.
        let (mut session, mut ordinal) = tool_history(&backend, cipher, 2, 2).await;
        poll_reads_nothing(&session, &backend, cipher, "during the run it started").await;
        session.append(&[end("r1")], true).await.unwrap();
        tool_run(&mut session, "r2", 1, &mut ordinal, None, None, false).await;
        poll_reads_nothing(&session, &backend, cipher, "after a later run started").await;
        drop(session);
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        poll_reads_nothing(&loaded, &backend, cipher, "in a writer loaded since").await;
    });
}
/// A run's start, sent as the queued send `submission` when one is given.
fn sent_start(run: &str, submission: Option<&str>) -> Record {
    let mut record = start(run);
    if let Record::RunStart(start) = &mut record {
        start.submission_id = submission.map(str::to_owned);
    }
    record
}

/// A send queued while a run runs, which the next run starts from.
fn queued(submission: &str) -> Record {
    Record::QueuedSend(QueuedSend {
        submission_id: submission.into(),
        send_id: Some(format!("send-{submission}")),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: format!("queued {submission}").into(),
            }],
        )],
        setup: Vec::new(),
        written_at_ms: None,
        ts_ms: None,
        request: None,
        receipt: None,
        delivery: None,
        media: Vec::new(),
        message_marks: None,
        named_speaker: None,
        kept_notice: None,
    })
}

/// The entry facts of the message at `ordinal`.
fn facts(ordinal: u64) -> Record {
    Record::EntryFacts(std::sync::Arc::new(EntryFacts {
        send_id: None,
        named_speaker: None,
        kept_notice: None,
        client_message_id: None,
        sender: None,
        marks: None,
        no_display_facts: None,
        reasoning_summaries: Vec::new(),
        rich_ui: None,
        runner_tag_base: Some(ordinal),
        legacy_view_seq: None,
        ordinal,
        written_at_ms: Some(ordinal),
        tag_base: ordinal,
        notice: None,
        usage: None,
        generated_under: None,
        native: true,
        render_tags: false,
        clean_reply: false,
        mid: format!("mfeedface.{}", ordinal - 1),
        run_id: None,
        created_at_ms: None,
        delivery_mode: None,
        delivery_tool_call_id: None,
        notices: Vec::new(),
    }))
}

fn assistant(run: &str, step: u64, content: Vec<ContentBlock>) -> Record {
    Record::Assistant(Assistant {
        run_id: run.into(),
        step_id: step,
        provider_message_id: MessageId(format!("{run}-m{step}")),
        message: std::sync::Arc::new(Message::assistant_from(content, model())),
        usage: None,
        written_at_ms: None,
        last_content_at_ms: None,
        budget_finalize: None,
        tags_v1: None,
        pruned: Vec::new(),
    })
}

fn step_start(run: &str, step: u64) -> Record {
    Record::StepStart(StepStart {
        run_id: run.into(),
        step_id: step,
        mid: None,
    })
}

fn step_end(run: &str, step: u64, calls: u32) -> Record {
    Record::StepEnd(StepEnd {
        run_id: run.into(),
        step_id: step,
        usage: None,
        finish_reason: None,
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
        call_count: calls,
        fallback: None,
        hooks: None,
        budget_finalize: None,
        total_usage: None,
        stop_met: None,
        limiting_stop_met: None,
        by_model: Vec::new(),
    })
}

/// The call key of run `run`'s call in step `step`.
fn call_key(run: &str, step: u64) -> String {
    format!("key-{run}-{step}")
}

/// One tool step of run `run`: the step, the assistant's call with its facts, the batch with the
/// call's intent and outcome, and the call's reply.
fn tool_step(run: &str, step: u64, ordinal: &mut u64) -> Vec<Vec<Record>> {
    let call = format!("{run}-c{step}");
    let batch = format!("{run}-b{step}");
    *ordinal += 1;
    let assistant_ordinal = *ordinal;
    // The tool-results message takes the next ordinal; it carries no facts of its own here.
    *ordinal += 1;
    let tool_call: ContentBlock = serde_json::from_value(serde_json::json!({
        "type": "tool_call", "tool_call_id": call, "tool_name": "read",
        "input": {"path": format!("/src/{call}.rs"), "limit": 200}
    }))
    .unwrap();
    let reply = Record::ToolReply(ToolReply {
        run_id: run.into(),
        batch_id: Some(batch.clone()),
        result: serde_json::from_value(serde_json::json!({
            "tool_call_id": call,
            "output": {"kind": "text", "text": format!("{call}: {}", "x".repeat(1500))}
        }))
        .unwrap(),
        finished_at_ms: None,
        tags_v1: None,
        media: Vec::new(),
    });
    vec![
        vec![
            step_start(run, step),
            event(&format!("{run}-started-{step}")),
        ],
        vec![
            assistant(run, step, vec![tool_call]),
            facts(assistant_ordinal),
            step_end(run, step, 1),
        ],
        vec![
            Record::BatchStart(BatchStart {
                run_id: run.into(),
                batch_id: batch.clone(),
                step_id: step,
                planned: vec![ToolCallId(call.clone())],
                mid: None,
                shown_tags: Vec::new(),
            }),
            Record::ToolIntent(ToolIntent {
                run_id: run.into(),
                batch_id: batch.clone(),
                tool_call_id: ToolCallId(call.clone()),
                tool_name: Some("read".into()),
                module_id: Some("tools".into()),
                args_digest: Digest([7; 32]),
                args: None,
                call_key: Some(call_key(run, step)),
                approval_digest: None,
                dispatched_at_ms: Some(step),
                schema_pin: None,
                hooks: None,
                key_derived: None,
            }),
            event(&format!("{run}-dispatched-{step}")),
        ],
        vec![
            Record::ToolOutcome(ToolOutcome {
                yielded_by: None,
                run_id: run.into(),
                batch_id: batch.clone(),
                tool_call_id: ToolCallId(call),
                outcome: CallOutcome::Completed,
                result_digest: Digest([9; 32]),
                is_error: false,
                reason: None,
                finished_at_ms: Some(step),
                detail: None,
                hooks: None,
                call_key: None,
            }),
            reply,
            Record::BatchEnd(BatchEnd {
                run_id: run.into(),
                batch_id: batch,
            }),
            event(&format!("{run}-progress-{step}")),
        ],
    ]
}

/// Appends run `run` of `steps` tool steps and an answer: started from the queued send
/// `submission` when one is given, queueing `queue` after its first step, and ended when `end`.
/// `ordinal` is the last message placed so far.
async fn tool_run<B: SessionBackend>(
    session: &mut Session<B>,
    run: &str,
    steps: u64,
    ordinal: &mut u64,
    submission: Option<&str>,
    queue: Option<&str>,
    end: bool,
) {
    session
        .append(&[sent_start(run, submission)], true)
        .await
        .unwrap();
    *ordinal += 1;
    session
        .append(&[input(run, &format!("ask {run}")), facts(*ordinal)], true)
        .await
        .unwrap();
    for step in 1..=steps {
        for batch in tool_step(run, step, ordinal) {
            session.append(&batch, true).await.unwrap();
        }
        if step == 1 {
            if let Some(queue) = queue {
                session.append(&[queued(queue)], true).await.unwrap();
            }
        }
    }
    let answer = steps + 1;
    *ordinal += 1;
    session
        .append(&[step_start(run, answer)], true)
        .await
        .unwrap();
    session
        .append(
            &[
                assistant(
                    run,
                    answer,
                    vec![ContentBlock::Text {
                        text: format!("answer {run}").into(),
                    }],
                ),
                facts(*ordinal),
                step_end(run, answer, 0),
            ],
            true,
        )
        .await
        .unwrap();
    if end {
        session.append(&[end_of(run)], true).await.unwrap();
    }
}

fn end_of(run: &str) -> Record {
    end(run)
}

/// A session of `runs` runs of `steps` tool steps each, every run after the first started from a
/// send queued during the run before it, the last one still running. Returns the session and the
/// messages it placed.
async fn tool_history<B: SessionBackend>(
    backend: B,
    cipher: Cipher,
    runs: usize,
    steps: u64,
) -> (Session<B>, u64) {
    let mut session = Session::create(backend, addr(), triple(), &deployment(cipher), false)
        .await
        .unwrap();
    let mut ordinal = 0;
    for index in 0..runs {
        let run = format!("r{index}");
        let submission = (index > 0).then(|| format!("sub-r{index}"));
        let queue = (index + 1 < runs).then(|| format!("sub-r{}", index + 1));
        tool_run(
            &mut session,
            &run,
            steps,
            &mut ordinal,
            submission.as_deref(),
            queue.as_deref(),
            index + 1 < runs,
        )
        .await;
    }
    (session, ordinal)
}

/// What loading a session holds and allocates, measured with the test binary's counting
/// allocator on a session of about 2,000 messages and 12,000 records in ten runs. It prints its
/// numbers and checks nothing: run it by name with `--run-ignored only` to compare two builds.
#[test]
#[ignore = "a measurement, run by name"]
fn memory_of_a_loaded_session() {
    use broca_test_support::alloc::Counting;
    let backend = MemoryBackend::default();
    block_on(async {
        let (session, messages) = tool_history(&backend, Cipher::XChaCha20Poly1305, 10, 100).await;
        let records = session.next_seq();
        let log_bytes = session.open_log().1;
        drop(session);
        let counting = Counting::start();
        let mut loaded = Session::load(
            &backend,
            addr(),
            &triple(),
            &deployment(Cipher::XChaCha20Poly1305),
        )
        .await
        .unwrap();
        let counts = counting.read();
        let (held, during, peak) = (counts.held, counts.allocated, counts.peak);
        // What dropping a part frees: the bytes it held.
        let freed = |free: &mut dyn FnMut()| -> isize {
            let counting = Counting::start();
            free();
            -counting.read().held
        };
        let crate::idindex::OpenIdsParts {
            ids,
            held: memory,
            in_kept,
            places,
        } = loaded.open_ids.parts();
        let filed = ids.len();
        let ids_bytes = freed(&mut || drop(std::mem::take(ids)));
        let placed = places.len();
        let places_bytes = freed(&mut || drop(std::mem::take(places)));
        let kept_for_index = in_kept.len();
        let in_kept_bytes = freed(&mut || drop(std::mem::take(in_kept)));
        // The records held in memory, by kind: each kind's bytes are what removing its records
        // frees.
        let mut kinds: std::collections::BTreeMap<String, Vec<u64>> = Default::default();
        for (seq, record) in memory.iter() {
            let name = format!("{record:?}");
            let name = name.split('(').next().unwrap_or_default().to_owned();
            kinds.entry(name).or_default().push(*seq);
        }
        let mut by_kind = Vec::new();
        let mut kinds_bytes = 0;
        for (name, seqs) in &kinds {
            let bytes = freed(&mut || {
                for seq in seqs {
                    memory.remove(seq);
                }
            });
            kinds_bytes += bytes;
            by_kind.push(format!("{name} {} records, {bytes} B", seqs.len()));
        }
        let held_bytes = freed(&mut || drop(std::mem::take(memory)));
        let index = ids_bytes + places_bytes + in_kept_bytes + kinds_bytes + held_bytes;
        let kept_records = loaded.kept.len();
        let kept_capacity = loaded.kept.records.capacity();
        let kept = freed(&mut || drop(std::mem::take(&mut loaded.kept)));
        println!(
            "messages {messages}, records {records}, log bytes {log_bytes}\n\
             load: held {held} B, allocated {during} B, peak {peak} B\n\
             id index: {index} B: {filed} ids filed, {ids_bytes} B; {placed} places, \
             {places_bytes} B; {kept_for_index} records left with the kept records, \
             {in_kept_bytes} B; held in memory: {}\n\
             kept records: {kept_records} records in {kept_capacity} slots, {kept} B",
            by_kind.join("; ")
        );
    });
}
