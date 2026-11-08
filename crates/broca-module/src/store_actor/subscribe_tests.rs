//! `session.subscribe` on a session kept in the new storage, through the store actor: what a
//! subscriber sees live, catching up and resuming at every unit, each refusal of a resume
//! cursor, and resuming across a seal and a revert.

use std::sync::Arc;
use std::time::Duration;

use broca_core::Cursor;
use broca_protocol::caller::ControlUnit;
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_store::backend::MemoryBackend;
use broca_session_store::record::Record;
use broca_session_store::session::{Carry, Session};
use tokio::sync::{mpsc, oneshot};

use super::tests::{actor_with, executor, idle, read_step, reopen, send, text_step};
use crate::actor::{LineageCmd, SubscribeOutcome};
use crate::fanout::{ControlEvent, Delivery, FromCursor, ResumeCursorProof};

type Commands = mpsc::UnboundedSender<LineageCmd>;

async fn subscribe(commands: &Commands, from: FromCursor, live: bool) -> mpsc::Receiver<Delivery> {
    let (sink, deliveries) = mpsc::channel(4096);
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Subscribe {
            from,
            live,
            sink,
            reply,
        })
        .unwrap();
    assert_eq!(answer.await.unwrap(), SubscribeOutcome::Attached);
    deliveries
}

/// A resume from `cursor` as a client sends it back: its wire form, parsed again.
fn resume(cursor: Cursor) -> FromCursor {
    let wire = broca_wire::CursorWire::from(cursor);
    resume_wire(&wire)
}

fn resume_wire(wire: &broca_wire::CursorWire) -> FromCursor {
    FromCursor::Resume {
        cursor: Cursor::new(wire.wal_seq, wire.sub_index),
        proof: ResumeCursorProof::new(
            wire.incarnation
                .as_deref()
                .expect("a store cursor has an incarnation"),
            wire.digest.as_deref().expect("a store cursor has a digest"),
        )
        .expect("a minted cursor parses"),
    }
}

/// Every non-display delivery until the stream stays quiet or ends.
async fn quiet(deliveries: &mut mpsc::Receiver<Delivery>) -> Vec<Delivery> {
    let mut out = Vec::new();
    while let Ok(Some(delivery)) =
        tokio::time::timeout(Duration::from_millis(300), deliveries.recv()).await
    {
        if !matches!(delivery, Delivery::Display(_)) {
            out.push(delivery);
        }
    }
    out
}

/// The control units of `deliveries`, refusing anything else.
fn units(deliveries: Vec<Delivery>) -> Vec<Arc<ControlEvent>> {
    deliveries
        .into_iter()
        .map(|delivery| match delivery {
            Delivery::Control(event) => event,
            other => panic!("expected only control units, got {other:?}"),
        })
        .collect()
}

/// A unit as a client compares it: its whole wire cursor, its unit and its lifecycle event.
fn wire(event: &ControlEvent) -> serde_json::Value {
    serde_json::json!({
        "cursor": broca_wire::CursorWire::from(event.0),
        "unit": event.1,
        "event": event.2,
    })
}

fn wires(events: &[Arc<ControlEvent>]) -> Vec<serde_json::Value> {
    events.iter().map(|event| wire(event)).collect()
}

fn finished_runs(events: &[Arc<ControlEvent>]) -> usize {
    events
        .iter()
        .filter(|event| matches!(event.1, ControlUnit::RunFinished { .. }))
        .count()
}

/// Two runs, the first calling a tool, then a third run's answer for a later send.
fn script() -> Vec<Vec<broca_protocol::NormalizedEvent>> {
    vec![
        read_step("a1"),
        text_step("a2", "done"),
        text_step("a3", "second"),
        text_step("a4", "third"),
    ]
}

/// The session after two runs, with a subscriber that attached before the first.
async fn two_runs() -> (
    Commands,
    tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
    &'static MemoryBackend,
    SharedParts,
    Vec<Arc<ControlEvent>>,
) {
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let (commands, served, backend) = actor_with(executor(script(), None), parts.clone()).await;
    let mut live = subscribe(&commands, FromCursor::Start, false).await;
    send(&commands, "r1", "one").await;
    idle(&commands).await;
    send(&commands, "r2", "two").await;
    idle(&commands).await;
    let live = units(quiet(&mut live).await);
    assert_eq!(finished_runs(&live), 2, "{:#?}", wires(&live));
    (commands, served, backend, parts, live)
}

/// Every resume cursor in `stream` resumes with exactly the units after it.
async fn resumes_at_every_unit(commands: &Commands, stream: &[Arc<ControlEvent>]) {
    for (at, event) in stream.iter().enumerate() {
        let mut resumed = subscribe(commands, resume(event.0), false).await;
        assert_eq!(
            wires(&units(quiet(&mut resumed).await)),
            wires(&stream[at + 1..]),
            "resuming after unit {at}"
        );
    }
}

/// Every unit a subscriber from the session's start gets resumes with exactly the units after
/// it, for the other scenarios that drive a store actor.
pub(super) async fn every_unit_resumes(commands: &Commands) {
    let mut from_start = subscribe(commands, FromCursor::Start, false).await;
    let stream = units(quiet(&mut from_start).await);
    assert!(!stream.is_empty(), "the session's stream has units");
    resumes_at_every_unit(commands, &stream).await;
}

/// The stream a subscriber sees live is the one a catch-up from the start delivers, and a
/// resume at any unit delivers exactly the units after it.
#[tokio::test]
async fn the_live_stream_is_the_caught_up_one_and_resumes_at_every_unit() {
    let (commands, _served, _backend, _parts, live) = two_runs().await;
    let mut from_start = subscribe(&commands, FromCursor::Start, false).await;
    assert_eq!(
        wires(&units(quiet(&mut from_start).await)),
        wires(&live),
        "a catch-up from the start delivers the live stream"
    );
    resumes_at_every_unit(&commands, &live).await;
    // Units after the session's last record arrive live, after the last one caught up.
    let mut after = subscribe(&commands, resume(live.last().unwrap().0), false).await;
    let mut future = subscribe(&commands, FromCursor::Start, true).await;
    send(&commands, "r3", "three").await;
    idle(&commands).await;
    let after = units(quiet(&mut after).await);
    assert_eq!(finished_runs(&after), 1);
    assert_eq!(wires(&units(quiet(&mut future).await)), wires(&after));
    assert!(after[0].0 > live.last().unwrap().0);
}

/// A position-only cursor resumes after its position, unchecked, as today.
#[tokio::test]
async fn a_position_only_cursor_resumes_after_its_position() {
    let (commands, _served, _backend, _parts, live) = two_runs().await;
    let at = live.len() / 2;
    let position = Cursor::new(live[at].0.wal_seq, live[at].0.sub_index);
    let mut resumed = subscribe(&commands, FromCursor::After(position), false).await;
    assert_eq!(
        wires(&units(quiet(&mut resumed).await)),
        wires(&live[at + 1..])
    );
}

async fn expired(commands: &Commands, from: FromCursor) -> Vec<Delivery> {
    let mut deliveries = subscribe(commands, from, false).await;
    quiet(&mut deliveries).await
}

fn is_expired(deliveries: &[Delivery]) -> bool {
    matches!(deliveries, [Delivery::CursorExpired])
}

/// A resume cursor is refused `cursor_expired` when it names another incarnation, a seq past
/// the durable end, a record with another digest, or a unit its record doesn't yield.
#[tokio::test]
async fn a_resume_cursor_is_refused_unless_it_names_a_unit_as_recorded() {
    let (commands, _served, _backend, _parts, live) = two_runs().await;
    let named = live[live.len() / 2].0;
    let good = broca_wire::CursorWire::from(named);
    assert!(!is_expired(&expired(&commands, resume_wire(&good)).await));

    let mut other_incarnation = good.clone();
    other_incarnation.incarnation = Some("0".repeat(32));
    assert!(is_expired(
        &expired(&commands, resume_wire(&other_incarnation)).await
    ));

    let mut past_the_end = good.clone();
    past_the_end.wal_seq = live.last().unwrap().0.wal_seq + 100;
    assert!(is_expired(
        &expired(&commands, resume_wire(&past_the_end)).await
    ));

    let mut other_digest = good.clone();
    let digest = u64::from_str_radix(good.digest.as_deref().unwrap(), 16).unwrap();
    other_digest.digest = Some(format!("{:016x}", digest ^ 1));
    assert!(is_expired(
        &expired(&commands, resume_wire(&other_digest)).await
    ));

    let yielded = live
        .iter()
        .filter(|event| event.0.wal_seq == named.wal_seq)
        .count() as u32;
    let mut no_such_unit = good.clone();
    no_such_unit.sub_index = yielded + 5;
    assert!(is_expired(
        &expired(&commands, resume_wire(&no_such_unit)).await
    ));
}

/// A seal moves the history into sealed logs: every cursor minted before it still resumes with
/// the same units, caught up from the sealed log holding it.
#[tokio::test]
async fn cursors_resume_across_a_seal() {
    let (commands, served, backend, parts, live) = two_runs().await;
    drop(commands);
    let mut session = served.await.unwrap();
    // The second run's messages stay in the new open log: a seal at its input.
    let second_input = session.conversation.placed - 1;
    session
        .seal_at_entry(
            second_input,
            &Carry {
                records: &[],
                window_repetition: None,
            },
            1,
        )
        .await
        .unwrap();
    session.close().await.unwrap();
    let (commands, _served) = reopen(backend, Arc::new(executor(Vec::new(), None)), &parts).await;
    let mut from_start = subscribe(&commands, FromCursor::Start, false).await;
    assert_eq!(wires(&units(quiet(&mut from_start).await)), wires(&live));
    resumes_at_every_unit(&commands, &live).await;
}

/// A revert ends the branch before a message: a cursor naming a unit of a record the revert
/// removed is refused, and one before the cut resumes with the units the session holds now.
#[tokio::test]
async fn cursors_resume_across_a_revert() {
    let (commands, served, backend, parts, live) = two_runs().await;
    drop(commands);
    let mut session = served.await.unwrap();
    let records = session.open_log_records().await.unwrap();
    // The second run's start, input and the input's facts, resent as a third run.
    let second_start = records
        .iter()
        .find_map(|(_, record)| match record {
            Record::RunStart(start) if start.run_id == "r2" => Some(start.clone()),
            _ => None,
        })
        .unwrap();
    let input_facts = records
        .iter()
        .skip_while(
            |(_, record)| !matches!(record, Record::RunStart(start) if start.run_id == "r2"),
        )
        .find_map(|(seq, record)| match record {
            Record::EntryFacts(facts) => Some((*seq, facts.clone())),
            _ => None,
        })
        .unwrap();
    let input = records
        .iter()
        .find_map(|(_, record)| match record {
            Record::Input(input) if input.run_id == "r2" => Some(input.clone()),
            _ => None,
        })
        .unwrap();
    let mut start = second_start;
    start.run_id = "r3".into();
    start.send_id = Some("k3".into());
    let mut resent_input = input;
    resent_input.run_id = "r3".into();
    let mut facts = input_facts.1.clone();
    facts.ordinal = 0;
    facts.mid = String::new();
    session
        .revert(
            &input_facts.1.mid,
            vec![
                Record::RunStart(start),
                Record::Input(resent_input),
                Record::EntryFacts(facts),
            ],
            None,
            2,
        )
        .await
        .unwrap();
    session.close().await.unwrap();
    let (commands, _served) = reopen(
        backend,
        Arc::new(executor(vec![text_step("a5", "again")], None)),
        &parts,
    )
    .await;
    idle(&commands).await;
    let mut from_start = subscribe(&commands, FromCursor::Start, false).await;
    let now = units(quiet(&mut from_start).await);
    // The first run's units are kept; resuming at each of them gives what the session holds.
    let kept = now
        .iter()
        .zip(&live)
        .take_while(|(now, then)| wire(now) == wire(then))
        .count();
    assert!(kept > 0 && kept < live.len(), "{kept} of {}", live.len());
    resumes_at_every_unit(&commands, &now).await;
    // A unit on the removed input's facts names a record the revert removed.
    let removed = live
        .iter()
        .find(|event| event.0.wal_seq == input_facts.0)
        .expect("the removed input's facts yield units");
    assert!(is_expired(&expired(&commands, resume(removed.0)).await));
}

/// `session.read` serves the cursor at its durable end as `head` (`store_fanout::last_cursor`
/// over the same view the page is read from): a subscribe from it, after another run appended
/// more, delivers exactly the units after what the read covered, the first of them first.
#[tokio::test]
async fn a_subscribe_from_a_reads_head_misses_and_repeats_nothing() {
    use super::tests::{deployment, triple, ADDR};
    let (commands, _served, backend, _parts, live) = two_runs().await;
    let view = Session::view(
        backend,
        broca_session_store::backend::SessionAddr(ADDR.into()),
        &triple(),
        &deployment(),
    )
    .await
    .unwrap();
    let head = crate::store_fanout::last_cursor(&view)
        .await
        .unwrap()
        .expect("the read covers units");
    assert_eq!(
        broca_wire::CursorWire::from(head),
        broca_wire::CursorWire::from(live.last().unwrap().0)
    );
    // Another run appends before the subscribe arrives.
    send(&commands, "r3", "three").await;
    idle(&commands).await;
    let mut from_head = subscribe(&commands, resume(head), false).await;
    let after = units(quiet(&mut from_head).await);
    let mut from_start = subscribe(&commands, FromCursor::Start, false).await;
    let all = units(quiet(&mut from_start).await);
    assert_eq!(finished_runs(&after), 1);
    assert_eq!(wires(&after), wires(&all[live.len()..]));
}

/// A session whose first run's tool result carries two notes, as a run that delivered sends
/// with that result records them: one from the module `occi` under the send id `ride-1`, and
/// one recorded without its send, as a log converted from today's WAL storage holds one (a WAL
/// ride written without its envelope names no send id). Built from the records a store actor
/// wrote for [`script`]'s two runs, the notes put after the tool batch's end, written to a
/// session of its own. Returns that session's backend, the seq of the note with its send (the
/// other follows it), and the session.
async fn ridden_session() -> (&'static MemoryBackend, u64, Session<&'static MemoryBackend>) {
    use super::tests::{deployment, triple, ADDR};
    use broca_session_store::record::{Notice, ResultAppend};
    let (commands, served, _backend, _parts, _live) = two_runs().await;
    drop(commands);
    let ran = served.await.unwrap();
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let mut session = Session::create(
        backend,
        broca_session_store::backend::SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    // Both sessions were created alike, so the new one already holds what creation wrote.
    let mut records: Vec<Record> = ran
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter(|(seq, _)| *seq >= session.next_seq())
        .map(|(_, record)| record)
        .collect();
    let batch_end = records
        .iter()
        .position(|record| matches!(record, Record::BatchEnd(_)))
        .expect("the first run's tool batch ends");
    let call = records
        .iter()
        .find_map(|record| match record {
            Record::ToolOutcome(outcome) => Some(outcome.tool_call_id.clone()),
            _ => None,
        })
        .expect("the first run's call has an outcome");
    let note = |text: &str, send_id: Option<&str>| {
        Record::ResultAppend(ResultAppend {
            run_id: "r1".into(),
            tool_call_id: call.clone(),
            text: format!("\n\n{text}"),
            notice: Some(Notice {
                sender: "occi".into(),
                template: None,
            }),
            written_at_ms: Some(1),
            send_id: send_id.map(str::to_owned),
        })
    };
    records.insert(batch_end + 1, note("unsent", None));
    records.insert(batch_end + 1, note("sent", Some("ride-1")));
    let first = session.append(&records, true).await.unwrap();
    (backend, first + batch_end as u64 + 1, session)
}

/// A note riding a tool result is a notice naming the send it came with and its sender; a note
/// recorded without its send is left out of the stream, yet a cursor naming its unit still
/// resumes, since a resume checks a cursor against every unit its record yields before notice
/// visibility filters any. Every delivered unit resumes.
#[tokio::test]
async fn a_ride_is_a_notice_and_one_without_its_send_still_resumes() {
    use cortexkit_role_llm_runner::live::LiveEvent;
    let (backend, sent_seq, session) = ridden_session().await;
    let unsent_seq = sent_seq + 1;
    let identity = crate::store_fanout::CursorIdentity::of(&session);
    let records = session.open_log_records().await.unwrap();
    let unsent = &records
        .iter()
        .find(|(seq, _)| *seq == unsent_seq)
        .expect("the session holds the unsent note")
        .1;
    let unsent_cursor = Cursor::new(unsent_seq, 0)
        .with_store_identity(identity.incarnation, identity.key.digest(unsent));
    let actor = super::StoreActor::new(
        super::tests::bind(),
        session,
        Arc::new(executor(Vec::new(), None)),
        Box::new(MemoryParts::default()),
    )
    .with_readers(Arc::new(move || backend));
    let (commands, receiver) = mpsc::unbounded_channel();
    let _served = tokio::spawn(actor.serve(receiver));

    let mut from_start = subscribe(&commands, FromCursor::Start, false).await;
    let stream = units(quiet(&mut from_start).await);
    let notices: Vec<(String, String, u64)> = stream
        .iter()
        .filter_map(|event| match &event.2 {
            LiveEvent::NoticeDelivered(notice) => Some((
                notice.send_id.clone(),
                notice.sender.clone(),
                event.0.wal_seq,
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        notices,
        [("ride-1".to_owned(), "occi".to_owned(), sent_seq)],
        "{:#?}",
        wires(&stream)
    );
    resumes_at_every_unit(&commands, &stream).await;
    let mut resumed = subscribe(&commands, resume(unsent_cursor), false).await;
    let after: Vec<_> = stream
        .iter()
        .filter(|event| event.0.wal_seq > unsent_seq)
        .cloned()
        .collect();
    assert_eq!(
        wires(&units(quiet(&mut resumed).await)),
        wires(&after),
        "a resume at the note recorded without its send"
    );
}

/// Asks the actor to read `request` from what it holds.
async fn read(
    commands: &Commands,
    request: crate::store_reads::StoreReadRequest,
) -> crate::store_reads::StoreReadOutcome {
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::StoreRead {
            request: Box::new(request),
            reply,
        })
        .unwrap();
    answer.await.unwrap()
}

/// `session.read`'s `head` on a page the actor serves from memory is the head a fresh view of
/// the session computes from storage, and the last unit a subscriber gets. While the live
/// projection is still seeding, the actor leaves such a read to a view instead of waiting; once
/// seeded, it serves the head from memory.
#[tokio::test]
async fn an_actor_served_reads_head_is_the_head_a_fresh_view_computes() {
    use super::tests::{bind, deployment, triple, ADDR};
    use crate::store_reads::{PageRequest, StoreReadOutcome, StoreReadReply, StoreReadRequest};
    use broca_session_store::backend::SessionAddr;
    let gate = Arc::new(tokio::sync::Notify::new());
    // Shared with the view read below, which reads the runs' setup parts the actor stored.
    let parts = broca_session_loop::parts::SharedParts::new(Box::new(MemoryParts::default()));
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let actor = super::StoreActor::new(
        bind(),
        session,
        Arc::new(executor(script(), None)),
        Box::new(parts.clone()),
    )
    .with_readers_held(Arc::new(move || backend), Arc::clone(&gate));
    let (commands, receiver) = mpsc::unbounded_channel();
    let _served = tokio::spawn(actor.serve(receiver));
    send(&commands, "r1", "one").await;
    idle(&commands).await;
    send(&commands, "r2", "two").await;
    idle(&commands).await;

    let view = Session::view(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
        .await
        .unwrap();
    let fresh = crate::store_fanout::last_cursor(&view)
        .await
        .unwrap()
        .expect("the session's stream has units");
    let mut from_start = subscribe(&commands, FromCursor::Start, false).await;
    let stream = units(quiet(&mut from_start).await);
    assert_eq!(wire(stream.last().unwrap())["cursor"], json_of(fresh));

    let page = || StoreReadRequest::Page(PageRequest::default());
    let StoreReadOutcome::Stored { position, .. } = read(&commands, page()).await else {
        panic!("before the projection is seeded, a page is left to a view");
    };
    // The view the module reads at the position the actor names carries the same head.
    let at = Session::view_at(
        backend,
        SessionAddr(ADDR.into()),
        &triple(),
        &deployment(),
        &position,
    )
    .await
    .unwrap();
    let context = crate::store_reads::ReadContext {
        parts: &parts,
        held: true,
        open: true,
        settled: None,
        session_name: "s",
        live_step: None,
        stream_head: None,
    };
    let answer = match crate::store_reads::execute(&at, &context, &page()).await {
        StoreReadReply::Page(Ok(answer)) => answer,
        StoreReadReply::Page(Err(error)) => panic!("a view refuses the page: {error:?}"),
        _ => panic!("a view answers a page read with a page"),
    };
    assert_eq!(
        answer.response.head,
        Some(broca_wire::CursorWire::from(fresh))
    );
    gate.notify_one();
    let served = broca_test_support::wait_until("the actor serving a page from memory", || {
        let commands = commands.clone();
        async move {
            match read(&commands, page()).await {
                StoreReadOutcome::Served(reply) => Ok(reply),
                _ => Err("the actor left the page to storage".to_owned()),
            }
        }
    })
    .await;
    let StoreReadReply::Page(Ok(answer)) = *served else {
        panic!("the actor answers with a page");
    };
    assert_eq!(
        answer.response.head,
        Some(broca_wire::CursorWire::from(fresh))
    );
}

fn json_of(cursor: Cursor) -> serde_json::Value {
    serde_json::to_value(broca_wire::CursorWire::from(cursor)).unwrap()
}
