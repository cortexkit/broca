//! The store subscription's parts on their own: the barrier on the live stream and the hook
//! that ends streams when read access is taken away.

use std::sync::Arc;

use broca_protocol::caller::ControlUnit;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::Session;
use tokio::sync::mpsc;

use super::*;

async fn session() -> Session<&'static MemoryBackend> {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    Session::create(
        backend,
        SessionAddr("0123456789abcdef".into()),
        SessionTriple {
            project_root: "/project".into(),
            harness: "store-fanout".into(),
            session: "s1".into(),
        },
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap()
}

fn fanout(session: &mut Session<&'static MemoryBackend>) -> StoreFanout<&'static MemoryBackend> {
    let backend = *session.backend();
    StoreFanout::new(session, Arc::new(move || backend), Default::default())
}

/// A unit as the live projection publishes it, at `seq`.
fn unit(seq: u64) -> LiveItem {
    LiveItem::Unit(Arc::new((
        Cursor::new(seq, 0),
        ControlUnit::StepStarted {
            step_id: broca_protocol::StepId(seq),
        },
        None,
    )))
}

fn publish(fanout: &StoreFanout<&'static MemoryBackend>, item: LiveItem) {
    fanout.publisher.lock().unwrap().ring.send(item);
}

/// The control deliveries `deliveries` holds until it ends or stays quiet, display left out.
async fn controls(deliveries: &mut mpsc::Receiver<Delivery>) -> Vec<Delivery> {
    let mut out = Vec::new();
    while let Ok(Some(delivery)) =
        tokio::time::timeout(std::time::Duration::from_millis(500), deliveries.recv()).await
    {
        if !matches!(delivery, Delivery::Display(_)) {
            out.push(delivery);
        }
    }
    out
}

fn seqs(deliveries: &[Delivery]) -> Vec<u64> {
    deliveries
        .iter()
        .filter_map(|delivery| match delivery {
            Delivery::Control(event) => Some(event.0.wal_seq),
            _ => None,
        })
        .collect()
}

/// A unit published live after a subscriber attached, but recorded at or before its barrier,
/// came with its catch-up: it isn't delivered again. One recorded after the barrier is.
#[tokio::test]
async fn a_live_unit_at_or_before_the_barrier_is_not_delivered_again() {
    let mut session = session().await;
    let mut fanout = fanout(&mut session);
    let head = session.next_seq() - 1;
    let (sink, mut deliveries) = mpsc::channel(64);
    fanout.attach(&session, AttachFrom::Live, sink);
    publish(&fanout, unit(head));
    publish(&fanout, unit(head + 1));
    assert_eq!(seqs(&controls(&mut deliveries).await), [head + 1]);
}

/// The read-access hook ends every stream where it is called: the units published before it
/// are delivered, then the refusal, and nothing published after it.
#[tokio::test]
async fn ending_read_access_ends_the_stream_with_the_refusal_and_nothing_after() {
    let mut session = session().await;
    let mut fanout = fanout(&mut session);
    let head = session.next_seq() - 1;
    let (sink, mut deliveries) = mpsc::channel(64);
    fanout.attach(&session, AttachFrom::Live, sink);
    let refusal = broca_core::ScopeRefusal::OwnerMismatch(Box::new(
        broca_core::OwnerMismatch::FirstPrincipal {
            first: broca_protocol::BindPrincipal::Direct,
            presented_principal: Some(broca_protocol::BindPrincipal::Unverified),
        },
    ));
    publish(&fanout, unit(head + 1));
    fanout.end_read_access(refusal.clone());
    publish(&fanout, unit(head + 2));
    let delivered = controls(&mut deliveries).await;
    assert_eq!(seqs(&delivered), [head + 1]);
    assert!(
        matches!(delivered.last(), Some(Delivery::AccessEnded(ended)) if **ended == refusal),
        "{delivered:?}"
    );
    assert!(deliveries.recv().await.is_none(), "the stream ended");
}

#[tokio::test]
async fn queued_access_changes_precede_ready_display_deltas() {
    let mut session = session().await;
    let mut fanout = fanout(&mut session);
    let (sink, mut deliveries) = mpsc::channel(8);
    fanout.attach(&session, AttachFrom::Live, sink);
    broca_test_support::wait_for(
        "fanout attached",
        async {
            let Some(Delivery::Display(event)) = deliveries.recv().await else {
                panic!("expected the attach display reset");
            };
            assert!(matches!(
                *event,
                broca_protocol::display::DisplayEvent::DisplayReset
            ));
        },
        || "waiting for display reset".into(),
    )
    .await;

    // Queue all three before letting this task yield to the forwarder.
    fanout
        .display()
        .0
        .send(Arc::new(
            broca_protocol::display::DisplayEvent::DisplayGap { dropped: 1 },
        ))
        .unwrap();
    fanout.recheck_read_access();
    fanout.end_read_access(broca_core::ScopeRefusal::SenderNotOwner);
    broca_test_support::wait_for(
        "fanout ends before displaying another delta",
        async {
            assert!(matches!(
                deliveries.recv().await,
                Some(Delivery::AdmissionChanged)
            ));
            assert!(matches!(
                deliveries.recv().await,
                Some(Delivery::AccessEnded(_))
            ));
            assert!(deliveries.recv().await.is_none());
        },
        || "waiting for access change and close".into(),
    )
    .await;
}

async fn before_reset(deliveries: &mut mpsc::Receiver<Delivery>) -> Vec<Arc<ControlEvent>> {
    let mut controls = Vec::new();
    loop {
        let delivery = broca_test_support::wait_until("live attach snapshot", || {
            std::future::ready(deliveries.try_recv().map_err(|error| format!("{error:?}")))
        })
        .await;
        match delivery {
            Delivery::Control(event) => controls.push(event),
            Delivery::Display(event)
                if matches!(*event, broca_protocol::DisplayEvent::DisplayReset) =>
            {
                return controls
            }
            other => panic!("unexpected attach delivery: {other:?}"),
        }
    }
}

fn step_start(step: u64) -> Record {
    Record::StepStart(broca_session_store::record::StepStart {
        run_id: "r1".into(),
        step_id: step,
        mid: Some(format!("m{step}")),
    })
}

#[tokio::test]
async fn catch_up_reads_a_long_open_log_once_after_sealed_history() {
    let mut session = session().await;
    let steps = 40 * CATCH_UP_CHUNK;
    let split = 4 * CATCH_UP_CHUNK;
    let sealed: Vec<_> = (1..=split).map(step_start).collect();
    let first = session.append(&sealed, true).await.unwrap();
    session.seal(None, 0).await.unwrap();
    // One record per append, as a session's writer appends them: a read of part of the open log
    // reads whole batches, so the smallest part it reads is one batch.
    let open_first = session.next_seq();
    for step in split + 1..=steps {
        session.append(&[step_start(step)], true).await.unwrap();
    }
    let head = session.next_seq() - 1;
    let open_len = session.position().len as usize;
    let backend = session.backend().snapshot();
    let before = backend.log_bytes_read();
    let runtime = tokio::runtime::Handle::current();
    let reader = session.stored_records(&backend);
    let identity = CursorIdentity::of(&session);
    let (tx, mut rx) = mpsc::channel(1);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            catch_up(
                &runtime,
                &reader,
                first,
                head,
                AttachFrom::Cursor(FromCursor::Start),
                &identity,
                Default::default(),
                &tx,
            );
        });
        let actual = futures::executor::block_on(async {
            let mut actual = Vec::new();
            loop {
                match rx.recv().await.unwrap() {
                    CatchUp::Units(units) => {
                        actual.extend(units.into_iter().map(|unit| unit.0.wal_seq))
                    }
                    CatchUp::Done => break,
                    CatchUp::CursorExpired => panic!("a start attach has no cursor to expire"),
                    CatchUp::Failed(why) => panic!("catch-up failed: {why}"),
                }
            }
            actual
        });
        let expected: Vec<_> = (first..first + split).chain(open_first..=head).collect();
        assert_eq!(actual, expected);
    });
    // The open log is read whole once, to learn where its records lie, and then each chunk reads
    // only the bytes holding it: not the whole log once per chunk.
    let read = backend.log_bytes_read() - before;
    assert!(
        read <= 2 * open_len + open_len / 4,
        "the catch-up read {read} log bytes of an open log of {open_len}"
    );
}

#[tokio::test]
async fn live_attaches_between_steps_and_after_a_boundary_keep_no_previous_controls() {
    let mut session = session().await;
    let mut fanout = fanout(&mut session);
    session.append(&[step_start(1)], true).await.unwrap();
    let (sink, mut deliveries) = mpsc::channel(64);
    fanout.attach(&session, AttachFrom::Live, sink);
    let first = before_reset(&mut deliveries).await;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].0.wal_seq, session.next_seq() - 1);
    assert_eq!(
        first[0].1,
        ControlUnit::StepStarted {
            step_id: broca_protocol::StepId(1)
        }
    );

    session
        .append(
            &[Record::BatchEnd(broca_session_store::record::BatchEnd {
                run_id: "r1".into(),
                batch_id: "b1".into(),
            })],
            true,
        )
        .await
        .unwrap();
    let (sink, mut deliveries) = mpsc::channel(64);
    fanout.attach(&session, AttachFrom::Live, sink);
    assert!(before_reset(&mut deliveries).await.is_empty());

    session.append(&[step_start(2)], true).await.unwrap();
    let (sink, mut deliveries) = mpsc::channel(64);
    fanout.attach(&session, AttachFrom::Live, sink);
    let next = before_reset(&mut deliveries).await;
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].0.wal_seq, session.next_seq() - 1);
    assert_eq!(
        next[0].1,
        ControlUnit::StepStarted {
            step_id: broca_protocol::StepId(2)
        }
    );
    assert_ne!(next[0].0, first[0].0);
}

#[tokio::test]
async fn queued_live_attaches_fail_when_seeding_fails_or_the_session_closes() {
    for close in [false, true] {
        let mut session = session().await;
        let backend = *session.backend();
        let gate = Arc::new(tokio::sync::Notify::new());
        let mut fanout = StoreFanout::held(
            &mut session,
            Arc::new(move || backend),
            Arc::clone(&gate),
            Default::default(),
        );
        let (sink, mut deliveries) = mpsc::channel(64);
        fanout.attach(&session, AttachFrom::Live, sink);
        if close {
            drop(fanout);
        } else {
            fanout.publisher.lock().unwrap().seeded(1, None);
        }
        let delivered = controls(&mut deliveries).await;
        assert!(
            matches!(delivered.as_slice(), [Delivery::Error(error)] if error == SEED_FAILED),
            "{delivered:?}"
        );
        assert!(deliveries.recv().await.is_none());
        gate.notify_one();
    }
}
