//! A subscriber keeps an idle session in the session store open, as on today's storage: its actor
//! doesn't close at an idle deadline or an idle tick while the subscriber holds its stream, and
//! closes at the first idle deadline after the subscriber has gone. A module stop still closes
//! it and ends the subscription.

use std::time::Duration;

use broca_protocol::caller::ControlUnit;
use tokio::sync::mpsc;

use super::new_heads_tests::{module, send, Module, IDLE};
use super::tests::{request, text_step};
use crate::actor::{LineageCmd, SendOutcome, SubscribeOutcome};
use crate::fanout::{Delivery, FromCursor};
use crate::registry::HEAD_SESSION_CLASS;

/// A subscription to the module's head from the start of its stream, through the registry.
async fn subscribed(module: &Module) -> mpsc::Receiver<Delivery> {
    let (sink, deliveries) = mpsc::channel(4096);
    let attached = module
        .registry
        .route_subscribe(&module.key, FromCursor::Start, sink)
        .await
        .expect("the head's actor opens");
    assert_eq!(attached, SubscribeOutcome::Attached);
    deliveries
}

/// Reads the subscription until run `run` has finished on it, which it must before it ends.
async fn finished(deliveries: &mut mpsc::Receiver<Delivery>, run: &str) {
    broca_test_support::wait_for(
        &format!("run {run} to finish on the subscription"),
        async {
            loop {
                match deliveries.recv().await {
                    None => panic!("the subscription ended before run {run} finished"),
                    Some(Delivery::Control(event)) => {
                        if matches!(
                            &event.1,
                            ControlUnit::RunFinished { run_id: Some(id), .. } if id.0 == run
                        ) {
                            return;
                        }
                    }
                    Some(_) => {}
                }
            }
        },
        String::new,
    )
    .await;
}

/// Watches the head for `deadlines` idle deadlines: its actor stays open all along, and the
/// subscription with it.
async fn stays_open(module: &Module, deliveries: &mut mpsc::Receiver<Delivery>, deadlines: u32) {
    let until = tokio::time::Instant::now() + IDLE * deadlines;
    while tokio::time::Instant::now() < until {
        assert_eq!(
            module.registry.slot_state(&module.key),
            Some("open"),
            "the subscribed head closed idle"
        );
        assert!(
            !matches!(
                deliveries.try_recv(),
                Err(mpsc::error::TryRecvError::Disconnected)
            ),
            "the subscription ended"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// An idle head with a subscriber stays open across several idle deadlines and an idle tick, and
/// the registry hears that a subscriber holds it, which is what `session.restore_from_backup`
/// tells an operator. The subscriber gets the next run's units on the stream it already holds.
/// Once it drops its stream, the head closes at an idle deadline.
#[tokio::test]
async fn a_subscriber_holds_an_idle_head_open_until_it_leaves() {
    let module = module(
        vec![text_step("a1", "hello"), text_step("a2", "again")],
        None,
    )
    .await;
    let first = send(
        &module,
        "r1",
        request("first"),
        Some(HEAD_SESSION_CLASS),
        None,
        None,
    )
    .await
    .unwrap();
    assert!(matches!(first, SendOutcome::Active { .. }), "{first:?}");
    let mut deliveries = subscribed(&module).await;
    finished(&mut deliveries, "r1").await;
    broca_test_support::wait_until("the registry to hear the subscriber holds the head", || {
        let holds = module.registry.idle_holds(&module.key);
        std::future::ready(if holds == ["live_subscriber"] {
            Ok(())
        } else {
            Err(format!("{holds:?}"))
        })
    })
    .await;
    stays_open(&module, &mut deliveries, 3).await;
    let tick = module
        .registry
        .open_handle(&module.key)
        .expect("the head is open")
        .send(LineageCmd::IdleTick)
        .await;
    assert!(tick.is_ok(), "the actor takes the tick");
    stays_open(&module, &mut deliveries, 3).await;

    let mut later = request("second");
    later.tools = None;
    let second = send(&module, "r2", later, Some(HEAD_SESSION_CLASS), None, None)
        .await
        .unwrap();
    assert!(matches!(second, SendOutcome::Active { .. }), "{second:?}");
    finished(&mut deliveries, "r2").await;

    drop(deliveries);
    module.closed().await;
}

/// A subscriber to a session no send has created yet waits for its first send, then follows it
/// from its start: the first run's units reach it on the stream it already holds. A subscriber
/// that names a cursor can't wait: a cursor only names a session that exists.
#[tokio::test]
async fn a_subscriber_before_the_first_send_follows_the_session_once_it_exists() {
    let module = module(vec![text_step("a1", "hello")], None).await;
    let mut deliveries = subscribed(&module).await;
    let (cursor_sink, _cursor_deliveries) = mpsc::channel(16);
    let refused = module
        .registry
        .route_subscribe(
            &module.key,
            FromCursor::After(broca_core::Cursor::new(1, 0)),
            cursor_sink,
        )
        .await;
    assert!(
        matches!(refused, Err(crate::registry::OpenError::UnknownSession)),
        "{refused:?}"
    );
    assert_eq!(
        module
            .registry
            .store_holding(&module.key)
            .unwrap()
            .map(|_| ()),
        None,
        "a waiting subscriber creates nothing"
    );
    let first = send(
        &module,
        "r1",
        request("first"),
        Some(HEAD_SESSION_CLASS),
        None,
        None,
    )
    .await
    .unwrap();
    assert!(matches!(first, SendOutcome::Active { .. }), "{first:?}");
    finished(&mut deliveries, "r1").await;
    drop(deliveries);
    module.closed().await;
}

/// A module stop closes a head a subscriber holds open, and ends the subscription.
#[tokio::test]
async fn a_module_stop_ends_a_subscription_holding_a_head_open() {
    let module = module(vec![text_step("a1", "hello")], None).await;
    let first = send(
        &module,
        "r1",
        request("first"),
        Some(HEAD_SESSION_CLASS),
        None,
        None,
    )
    .await
    .unwrap();
    assert!(matches!(first, SendOutcome::Active { .. }), "{first:?}");
    let mut deliveries = subscribed(&module).await;
    finished(&mut deliveries, "r1").await;
    stays_open(&module, &mut deliveries, 2).await;

    assert!(
        module.registry.stop_and_seal().await,
        "the stop closes every actor"
    );
    assert_eq!(module.registry.slot_state(&module.key), None);
    broca_test_support::wait_for(
        "the subscription to end",
        async { while deliveries.recv().await.is_some() {} },
        String::new,
    )
    .await;
}
