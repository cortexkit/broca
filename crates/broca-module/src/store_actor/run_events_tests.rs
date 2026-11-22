//! The run events a session in the session store hands its run-event sink.

use std::sync::{Arc, Mutex};

use broca_protocol::run_events::{RunEvent, RunEventDetail, StructuredOutputStatus};
use broca_protocol::{BindIdentity, RunFinishReason, RunId};
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::session::Session;
use tokio::sync::{mpsc, Notify};

use super::tests::{
    auth_failure, bind, deployment, executor, idle, request, send, send_as, text_step, triple,
    Commands, LoopExecutor, ADDR,
};
use super::StoreActor;
use crate::actor::{SendKey, SendOutcome};
use crate::store_run_events::RunEventSink;
use broca_store::bus_events::RunEventStore as _;

/// One call of the sink: the events of one durable batch.
struct Handed {
    epoch: String,
    through: u64,
    events: Vec<RunEvent>,
}

#[derive(Default)]
struct Recorded(Mutex<Vec<Handed>>);

impl RunEventSink for Recorded {
    fn durable(&self, session: &BindIdentity, epoch: &str, through: u64, events: Vec<RunEvent>) {
        assert_eq!(session, &bind());
        self.0.lock().unwrap().push(Handed {
            epoch: epoch.to_owned(),
            through,
            events,
        });
    }
}

/// A store actor over a new session in memory, handing its run events to `sink`.
async fn actor(
    executor: LoopExecutor,
    sink: Arc<dyn RunEventSink>,
) -> (
    Commands,
    tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
) {
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
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let actor = StoreActor::new(bind(), session, Arc::new(executor), Box::new(parts))
        .with_readers(Arc::new(move || backend))
        .with_run_events(sink);
    let (commands, receiver) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    (Commands::over(commands, backend), served)
}

/// The kind of the record at each of `seqs`, as the session stores it.
async fn kinds(session: &Session<&'static MemoryBackend>, seqs: &[u64]) -> Vec<&'static str> {
    let last = session.next_seq() - 1;
    let log = session.from_log_holding(1, last).await.unwrap();
    seqs.iter()
        .map(|seq| {
            log.records
                .iter()
                .find(|(at, _)| at == seq)
                .map(|(_, record)| record.name())
                .unwrap_or("absent")
        })
        .collect()
}

/// A run with a steer hands, once each record is durable, `run.started` at its start,
/// `run.steer_consumed` at the step that places the steer, and `run.ended` at its end, each
/// with the record's own seq and the session's incarnation as its epoch.
#[tokio::test]
async fn a_run_hands_its_events_once_their_records_are_durable() {
    let gate = Arc::new(Notify::new());
    let sink = Arc::new(Recorded::default());
    let (commands, served) = actor(
        executor(
            vec![text_step("a1", "hello"), text_step("a2", "again")],
            Some(Arc::clone(&gate)),
        ),
        Arc::clone(&sink) as Arc<dyn RunEventSink>,
    )
    .await;
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    assert_eq!(
        send_as(
            &commands,
            "r-steer",
            request("steered"),
            SendKey::SendId("steer-1".into()),
            broca_protocol::Delivery::Steer
        )
        .await,
        SendOutcome::Active {
            run_id: RunId("r1".into()),
            delivered: None
        }
    );
    gate.notify_one();
    gate.notify_one();
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();

    let handed = std::mem::take(&mut *sink.0.lock().unwrap());
    let epoch: String = session
        .head()
        .incarnation
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let mut events = Vec::new();
    let mut through = 0;
    for batch in &handed {
        assert_eq!(
            batch.epoch, epoch,
            "every batch names the session's incarnation"
        );
        assert!(batch.through >= through, "batches arrive in seq order");
        through = batch.through;
        for event in &batch.events {
            assert!(
                event.seq <= batch.through,
                "an event is handed only once its record is durable"
            );
            events.push(event.clone());
        }
    }
    assert_eq!(
        through,
        session.next_seq() - 1,
        "the last batch reaches the end"
    );
    let details: Vec<RunEventDetail> = events.iter().map(|e| e.detail.clone()).collect();
    assert_eq!(
        details,
        [
            RunEventDetail::Started {},
            RunEventDetail::SteerConsumed {
                step: 2,
                consumed: 1,
                send_ids: vec!["steer-1".into()],
                truncated: false,
            },
            RunEventDetail::Ended {
                terminal: RunFinishReason::Completed,
                error_class: None,
                structured_output: Some(StructuredOutputStatus::Absent),
            },
        ]
    );
    let session_ref = broca_core::session_refs::agent_session_ref(&bind());
    for event in &events {
        assert_eq!(event.run_id, "r1");
        assert_eq!(event.session_ref, session_ref);
        assert!(event.at_ms > 0);
    }
    let seqs: Vec<u64> = events.iter().map(|event| event.seq).collect();
    assert_eq!(
        kinds(&session, &seqs).await,
        ["RunStart", "StepStart", "RunEnd"],
        "each event carries the seq of the record it reports"
    );
}

/// A run paused for sign-in hands `run.started` and then `run.paused`, naming the pause.
#[tokio::test]
async fn a_run_paused_for_sign_in_hands_run_paused() {
    let sink = Arc::new(Recorded::default());
    let (commands, served) = actor(
        executor(vec![auth_failure()], None),
        Arc::clone(&sink) as Arc<dyn RunEventSink>,
    )
    .await;
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    drop(commands);
    let _session = served.await.unwrap();
    let details: Vec<RunEventDetail> = sink
        .0
        .lock()
        .unwrap()
        .iter()
        .flat_map(|batch| batch.events.iter().map(|event| event.detail.clone()))
        .collect();
    assert_eq!(
        details,
        [
            RunEventDetail::Started {},
            RunEventDetail::Paused {
                pause_reason: broca_protocol::PauseReason::AuthRequired
            },
        ]
    );
}

/// Records run events in a run index's outbox, as the module does.
struct Outbox(Arc<broca_store::SqliteStore>);

impl RunEventSink for Outbox {
    fn durable(&self, session: &BindIdentity, epoch: &str, _: u64, mut events: Vec<RunEvent>) {
        assert!(self
            .0
            .record_run_events(session, epoch, &mut events, 0)
            .unwrap());
    }
}

/// Writes the publications the runs above produce to `$BROCA_SAMPLE_DIR`, one JSON file per
/// event, for readers to check before the module publishes them: each run's events go through
/// the module's own outbox and are read back as the publisher sends them.
#[tokio::test]
#[ignore = "writes reader samples; run with BROCA_SAMPLE_DIR set"]
async fn write_run_event_samples() {
    let dir = std::path::PathBuf::from(std::env::var("BROCA_SAMPLE_DIR").unwrap());
    std::fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
    store.enable_bus_events("box_sample").unwrap();
    let sink: Arc<dyn RunEventSink> = Arc::new(Outbox(Arc::clone(&store)));
    let gate = Arc::new(Notify::new());
    let (commands, served) = actor(
        executor(
            vec![text_step("a1", "hello"), text_step("a2", "again")],
            Some(Arc::clone(&gate)),
        ),
        Arc::clone(&sink),
    )
    .await;
    send(&commands, "r1", "first").await;
    send_as(
        &commands,
        "r-steer",
        request("steered"),
        SendKey::SendId("steer-1".into()),
        broca_protocol::Delivery::Steer,
    )
    .await;
    gate.notify_one();
    gate.notify_one();
    idle(&commands).await;
    drop(commands);
    served.await.unwrap();
    let (commands, served) = actor(executor(vec![auth_failure()], None), sink).await;
    send(&commands, "r2", "second").await;
    idle(&commands).await;
    drop(commands);
    served.await.unwrap();
    let mut n = 0;
    while let Some(publication) = store.pending_bus_event().unwrap() {
        n += 1;
        let sample = serde_json::json!({
            "subject": publication.subject,
            "nats_msg_id": publication.event_key,
            "headers": publication.headers,
            "body": String::from_utf8(publication.body.clone()).unwrap(),
        });
        let name = format!("{n:02}-{}.json", publication.headers["kind"]);
        std::fs::write(dir.join(name), serde_json::to_vec_pretty(&sample).unwrap()).unwrap();
        store
            .acknowledge_bus_event(
                &publication,
                cortexkit_bus_trait::PublishReceipt::new(n, false),
                1_794_790_000_000,
            )
            .unwrap();
    }
    assert_eq!(
        n, 5,
        "started, steer_consumed and ended, then started and paused"
    );
}

/// The selection a fresh run freezes in the tests below: routing chose its account and admitted
/// it as `adm-1`.
fn routed() -> broca_records::record::AuthSelection {
    serde_json::from_value(serde_json::json!({
        "credential_id": "credential-a", "account_ref": "a", "auth_method": "oauth",
        "account_decision": {"path": "routed", "admission_id": "adm-1", "decision_id": "decision-a"}
    }))
    .unwrap()
}

/// Collects the ends of routed runs a session hands its sink.
#[derive(Default)]
struct Ends(Mutex<Vec<crate::store_run_events::RoutedRunEnd>>);

impl RunEventSink for Ends {
    fn durable(&self, _: &BindIdentity, _: &str, _: u64, _: Vec<RunEvent>) {}

    fn routed_ends(
        &self,
        session: &BindIdentity,
        ends: Vec<crate::store_run_events::RoutedRunEnd>,
    ) {
        assert_eq!(session, &bind());
        self.0.lock().unwrap().extend(ends);
    }
}

/// The end `session`'s last run hands its sink: the session id and the end record's seq name it.
async fn expected_end(
    session: &Session<&'static MemoryBackend>,
    reason: RunFinishReason,
) -> crate::store_run_events::RoutedRunEnd {
    let records = session.open_log_records().await.unwrap();
    let end_seq = records
        .iter()
        .rev()
        .find(|(_, record)| matches!(record, broca_session_store::record::Record::RunEnd(_)))
        .map(|(seq, _)| *seq)
        .expect("the run ended");
    let session_id: String = session
        .head()
        .session_id
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    crate::store_run_events::RoutedRunEnd {
        event_id: format!("{session_id}:{end_seq}"),
        admission_id: "adm-1".into(),
        run_id: "r1".into(),
        reason,
    }
}

/// A run routing admitted hands its end once the end is durable, so the `run_finished` report
/// routing is owed is stored with it. A run whose account routing didn't choose hands none.
#[tokio::test]
async fn a_routed_run_hands_its_end_and_another_run_none() {
    for routed_run in [true, false] {
        let sink = Arc::new(Ends::default());
        let mut executor = executor(vec![text_step("a1", "hello")], None);
        executor.auth_selection = routed_run.then(routed);
        let (commands, served) = actor(executor, Arc::clone(&sink) as Arc<dyn RunEventSink>).await;
        assert!(matches!(
            send(&commands, "r1", "first").await,
            SendOutcome::Active { .. }
        ));
        idle(&commands).await;
        drop(commands);
        let session = served.await.unwrap();
        let ends = std::mem::take(&mut *sink.0.lock().unwrap());
        if routed_run {
            assert_eq!(
                ends,
                [expected_end(&session, RunFinishReason::Completed).await]
            );
        } else {
            assert!(ends.is_empty(), "{ends:?}");
        }
    }
}

/// A routed run cancelled while paused ends outside any run task, so no reporter is left to
/// tell routing; its end is still handed over, for the stored report.
#[tokio::test]
async fn a_routed_run_cancelled_while_paused_hands_its_end() {
    let sink = Arc::new(Ends::default());
    let mut executor = executor(vec![auth_failure()], None);
    executor.auth_selection = Some(routed());
    let (commands, served) = actor(executor, Arc::clone(&sink) as Arc<dyn RunEventSink>).await;
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    assert!(
        sink.0.lock().unwrap().is_empty(),
        "a paused run hasn't ended"
    );
    let (reply, cancelled) = tokio::sync::oneshot::channel();
    commands
        .send(crate::actor::LineageCmd::Cancel {
            run_id: RunId("r1".into()),
            at: broca_records::record::RunCancelAt::StepBoundary,
            reply,
        })
        .unwrap();
    cancelled.await.unwrap();
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let ends = std::mem::take(&mut *sink.0.lock().unwrap());
    assert_eq!(
        ends,
        [expected_end(&session, RunFinishReason::Cancelled).await]
    );
}

/// A routed run paused when the module stopped, and cancelled by the next one, hands its end
/// too: the reopened session's projector takes the open run's admission from the run the
/// session folded, since it never saw the run start.
#[tokio::test]
async fn a_routed_run_paused_across_a_restart_hands_its_end() {
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
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let mut paused_by = executor(vec![auth_failure(), auth_failure()], None);
    paused_by.auth_selection = Some(routed());
    let executor = Arc::new(paused_by);
    let actor = StoreActor::new(bind(), session, executor.clone(), Box::new(parts.clone()))
        .with_readers(Arc::new(move || backend));
    let (commands, receiver) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    let commands = Commands::over(commands, backend);
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    drop(commands);
    drop(served.await.unwrap());

    let sink = Arc::new(Ends::default());
    let session = Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
        .await
        .unwrap();
    let actor = StoreActor::new(bind(), session, executor, Box::new(parts))
        .with_readers(Arc::new(move || backend))
        .with_run_events(Arc::clone(&sink) as Arc<dyn RunEventSink>);
    let (commands, receiver) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    let commands = Commands::over(commands, backend);
    let (reply, cancelled) = tokio::sync::oneshot::channel();
    commands
        .send(crate::actor::LineageCmd::Cancel {
            run_id: RunId("r1".into()),
            at: broca_records::record::RunCancelAt::StepBoundary,
            reply,
        })
        .unwrap();
    cancelled.await.unwrap();
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let ends = std::mem::take(&mut *sink.0.lock().unwrap());
    assert_eq!(
        ends,
        [expected_end(&session, RunFinishReason::Cancelled).await]
    );
}
