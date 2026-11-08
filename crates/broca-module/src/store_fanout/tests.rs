//! The store subscription's parts on their own: the barrier on the live stream and the hook
//! that ends streams when read access is taken away.

use std::sync::Arc;

use broca_protocol::caller::ControlUnit;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::Session;
use cortexkit_role_llm_runner::live::LiveEvent;
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
    StoreFanout::new(session, Arc::new(move || backend))
}

/// A unit as the live projection publishes it, at `seq`.
fn unit(seq: u64) -> LiveItem {
    LiveItem::Unit(Arc::new((
        Cursor::new(seq, 0),
        ControlUnit::StepStarted {
            step_id: broca_protocol::StepId(seq),
        },
        LiveEvent::DisplayReset,
    )))
}

fn publish(fanout: &StoreFanout<&'static MemoryBackend>, item: LiveItem) {
    let _ = fanout.publisher.lock().unwrap().ring.send(item);
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
